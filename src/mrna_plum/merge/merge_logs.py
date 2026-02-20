from __future__ import annotations

import json
import re
import hashlib
import pyarrow as pa
import pyarrow.parquet as pq
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

from mrna_plum.io.csv_read import detect_csv_dialect, iter_csv_rows_streaming, pick_time_column_index
from mrna_plum.store.duckdb_store import (
    EventRawRow,
    create_stage_table,
    insert_stage_rows,
    merge_stage_into_events_raw,
    export_course_to_csv,
    export_course_to_parquet,
)

_LOG_NAME_RE = re.compile(r"^logs_(?P<course>.+?)_(?P<ts>\d{8}-\d{4})\.csv$", re.IGNORECASE)


_TIME_FORMATS = (
    # ISO / quasi-ISO
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M",
    # PL
    "%d.%m.%Y %H:%M:%S",
    "%d.%m.%Y %H:%M",
)

def merge_logs_to_parquet(
    input_files: list[Path],
    parquet_out: Path,
    *,
    dedup_per_file: bool = True,
    row_group_size: int = 50_000,
) -> int:
    """
    Streamingowy zapis do Parquet:
    - nie używa pandas
    - nie trzyma wszystkich danych w RAM
    - dedup per plik (opcjonalnie)
    """
    parquet_out.parent.mkdir(parents=True, exist_ok=True)

    writer: Optional[pq.ParquetWriter] = None
    total_rows = 0

    try:
        for fp in input_files:
            dialect = detect_csv_dialect(fp)

            # dedup tylko w obrębie tego pliku (set hashy resetowany per plik)
            seen: set[str] = set() if dedup_per_file else set()

            header_current: Optional[list[str]] = None
            batch_cols: dict[str, list[str]] = {}
            batch_cols["_source_file"] = []

            for header, row in iter_csv_rows_streaming(fp, dialect=dialect):
                if header_current is None:
                    header_current = header
                    # przygotuj kolumny na ten header
                    for h in header_current:
                        batch_cols.setdefault(h, [])

                # hash całego wiersza po trim (reader już trimuje)
                if dedup_per_file:
                    key = "\x1f".join(row)
                    if key in seen:
                        continue
                    seen.add(key)

                # dopisz wartości (jeśli row krótszy -> "")
                for i, h in enumerate(header):
                    batch_cols[h].append(row[i] if i < len(row) else "")
                batch_cols["_source_file"].append(str(fp))

                # flush row-group
                if len(batch_cols["_source_file"]) >= row_group_size:
                    table = pa.table(batch_cols)
                    if writer is None:
                        writer = pq.ParquetWriter(parquet_out, table.schema)
                    writer.write_table(table)
                    total_rows += table.num_rows
                    # reset batch
                    batch_cols = {k: [] for k in table.schema.names}

            # flush reszty dla pliku
            if header_current is not None and len(batch_cols["_source_file"]) > 0:
                table = pa.table(batch_cols)
                if writer is None:
                    writer = pq.ParquetWriter(parquet_out, table.schema)
                writer.write_table(table)
                total_rows += table.num_rows

        # jeśli nie było żadnych danych -> pusty parquet
        if writer is None:
            empty = pa.table({})
            pq.write_table(empty, parquet_out)

        return total_rows

    finally:
        if writer is not None:
            writer.close()

def _parse_time_to_iso(value: str) -> Optional[str]:
    v = value.strip()
    if not v:
        return None

    # szybka ścieżka: datetime.fromisoformat
    try:
        dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
        # przechowaj jako ISO bez utraty
        return dt.isoformat()
    except Exception:
        pass

    for fmt in _TIME_FORMATS:
        try:
            dt = datetime.strptime(v, fmt)
            return dt.isoformat(sep="T")
        except Exception:
            continue
    return None


def _normalize_fields_key(fields: List[str]) -> str:
    """
    Dedup tylko gdy CAŁY wiersz identyczny po Trim, bez BOM, bez CR.
    BOM/CR załatwiamy na wejściu (encoding + newline=""), Trim robimy w readerze.
    Tu robimy stabilny "key" niezależny od delimitera.
    """
    joined = "\x1f".join(fields)  # separator niewystępujący w CSV
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def iter_log_files(root: Path) -> Iterator[Path]:
    for p in root.rglob("*.csv"):
        if p.is_file():
            yield p


def group_logs_by_course(root: Path) -> Dict[str, List[Path]]:
    grouped: Dict[str, List[Path]] = {}
    for p in iter_log_files(root):
        m = _LOG_NAME_RE.match(p.name)
        if not m:
            continue
        course = m.group("course")
        grouped.setdefault(course, []).append(p)

    # stabilnie posortuj listy plików po nazwie (timestamp w nazwie)
    for course in list(grouped.keys()):
        grouped[course].sort(key=lambda x: x.name)
    return grouped


@dataclass(frozen=True)
class MergeLogsResult:
    courses: int
    files: int
    inserted_rows: int


def merge_logs_into_duckdb(
    *,
    root: Path,
    con,  # duckdb connection
    export_mode: str = "duckdb",  # "duckdb" | "parquet" | "csv"
    export_dir: Optional[Path] = None,
    chunk_size: int = 2000,
) -> MergeLogsResult:
    """
    Główna funkcja:
    - rekurencyjnie znajduje logs_<KURS>_<YYYYMMDD-HHMM>.csv
    - grupuje po KURS
    - streaming read + stage insert
    - dedup po payload_json+row_key (czyli cały wiersz po trim)
    - sortowanie malejąco po czasie realizowane przy eksporcie (ORDER BY)
    """
    try:
        grouped = group_logs_by_course(root)
        total_files = sum(len(v) for v in grouped.values())
        total_inserted = 0

        for course, files in grouped.items():
            create_stage_table(con)

            buf: List[EventRawRow] = []

            for fpath in files:
                dialect = detect_csv_dialect(fpath)

                # czytamy header z pierwszego niepustego wiersza; potem payload zawsze jako dict(header->value)
                time_idx: Optional[int] = None
                header_ref: Optional[List[str]] = None

                for header, row in iter_csv_rows_streaming(fpath, dialect=dialect):
                    if header_ref is None:
                        header_ref = header
                        time_idx = pick_time_column_index(header_ref)

                    # jeśli header się różni między plikami — nie wywalamy procesu,
                    # tylko mapujemy po indeksach do aktualnego headera z tej iteracji.
                    payload = {header[i]: (row[i] if i < len(row) else "") for i in range(len(header))}
                    payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

                    time_text = None
                    time_iso = None
                    if time_idx is not None and time_idx < len(row):
                        time_text = row[time_idx]
                        time_iso = _parse_time_to_iso(time_text)

                    row_key = _normalize_fields_key(row)

                    buf.append(
                        EventRawRow(
                            course=course,
                            time_text=time_text,
                            time_ts_iso=time_iso,
                            row_key=row_key,
                            payload_json=payload_json,
                            source_file=str(fpath),
                        )
                    )

                    if len(buf) >= chunk_size:
                        insert_stage_rows(con, buf)
                        buf.clear()

            if buf:
                insert_stage_rows(con, buf)
                buf.clear()

            inserted = merge_stage_into_events_raw(con)
            total_inserted += inserted

            # opcjonalny eksport per kurs
            if export_mode in ("csv", "parquet"):
                if export_dir is None:
                    raise ValueError("export_dir is required for export_mode=csv/parquet")
                if export_mode == "csv":
                    out_csv = export_dir / f"{course}_full_log.csv"
                    export_course_to_csv(con, course=course, out_csv=out_csv)
                else:
                    out_pq = export_dir / f"{course}_full_log.parquet"
                    export_course_to_parquet(con, course=course, out_parquet=out_pq)

        return MergeLogsResult(courses=len(grouped), files=total_files, inserted_rows=total_inserted)
    except UnicodeDecodeError as e:
        # tu masz ładny komunikat: który plik/kurs poleciał
        raise RuntimeError(f"UnicodeDecodeError while reading CSV: course={course}, file={fpath}") from e
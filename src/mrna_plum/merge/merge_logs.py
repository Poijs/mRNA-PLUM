from __future__ import annotations

import json
import re
import hashlib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterator, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

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

    # Moodle/PLUM (częsty eksport): 6-11-25, 15:34:53
    "%d-%m-%y, %H:%M:%S",
    "%d-%m-%y, %H:%M",
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

            # dedup tylko w obrębie tego pliku (set resetowany per plik)
            seen: set[str] = set() if dedup_per_file else set()

            header_current: Optional[list[str]] = None
            batch_cols: dict[str, list[str]] = {}
            batch_cols["_source_file"] = []

            for header, row in iter_csv_rows_streaming(fp, dialect=dialect):
                if header_current is None:
                    header_current = header
                    for h in header_current:
                        batch_cols.setdefault(h, [])

                if dedup_per_file:
                    key = "\x1f".join(row)
                    if key in seen:
                        continue
                    seen.add(key)

                for i, h in enumerate(header):
                    batch_cols[h].append(row[i] if i < len(row) else "")
                batch_cols["_source_file"].append(str(fp))

                if len(batch_cols["_source_file"]) >= row_group_size:
                    table = pa.table(batch_cols)
                    if writer is None:
                        writer = pq.ParquetWriter(parquet_out, table.schema)
                    writer.write_table(table)
                    total_rows += table.num_rows
                    batch_cols = {k: [] for k in table.schema.names}

            if header_current is not None and len(batch_cols["_source_file"]) > 0:
                table = pa.table(batch_cols)
                if writer is None:
                    writer = pq.ParquetWriter(parquet_out, table.schema)
                writer.write_table(table)
                total_rows += table.num_rows

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

    try:
        dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
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
    Dedup: CAŁY wiersz identyczny po Trim (reader trimuje).
    Stabilny hash niezależny od delimitera.
    """
    joined = "\x1f".join(fields)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def iter_log_files(root: Path, pattern: str = "*.csv") -> Iterator[Path]:
    for p in root.rglob(pattern):
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

    # stabilne sortowanie po nazwie (timestamp jest w nazwie)
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
    Publiczny entrypoint zgodny z testami/CLI.

    - root: folder z logami logs_<KURS>_<YYYYMMDD-HHMM>.csv (rekurencyjnie)
    - export_mode: "duckdb" (domyślnie) lub "csv"/"parquet"
    - export_dir: wymagany dla "csv"/"parquet"
    - chunk_size: batch insert do stage
    """
    export_mode = (export_mode or "duckdb").lower().strip()
    if export_mode not in ("duckdb", "csv", "parquet"):
        raise ValueError(f"export_mode must be 'duckdb'|'csv'|'parquet', got: {export_mode}")

    if export_mode in ("csv", "parquet") and export_dir is None:
        raise ValueError("export_dir is required for export_mode=csv/parquet")

    return _merge_logs_into_duckdb_impl(
        root=root,
        con=con,
        export_mode=export_mode,
        export_dir=export_dir,
        chunk_size=chunk_size,
    )

_COURSE_ID_PATTERNS = [
    # najczęstsze w Moodle logs (EN)
    re.compile(r"course with id\s+'(\d+)'", re.IGNORECASE),
    re.compile(r"course with id\s+(\d+)", re.IGNORECASE),

    # czasem bez apostrofów
    re.compile(r"\bcourse id\b\s*[:=]?\s*(\d+)", re.IGNORECASE),

    # PL warianty (na wszelki wypadek)
    re.compile(r"\bid kursu\b\s*[:=]?\s*(\d+)", re.IGNORECASE),
]


def _extract_course_id_from_payload(payload: Dict[str, str]) -> Optional[int]:
    """
    Szukamy course_id głównie w polu 'Opis' (czasem 'Description'),
    ale dla bezpieczeństwa przeszukujemy też cały payload.
    """
    candidates: List[str] = []

    # preferowane pola
    for k in ("Opis", "Description", "Event description", "Nazwa zdarzenia", "Kontekst zdarzenia"):
        v = payload.get(k)
        if v:
            candidates.append(v)

    # fallback: cały payload (join wartości)
    if not candidates:
        candidates.append(" | ".join([v for v in payload.values() if v]))

    text = " \n ".join(candidates)

    for rx in _COURSE_ID_PATTERNS:
        m = rx.search(text)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                return None
    return None

def _merge_logs_into_duckdb_impl(
    *,
    root: Path,
    con,
    export_mode: str,
    export_dir: Optional[Path],
    chunk_size: int,
) -> MergeLogsResult:
    """
    Rdzeń logiki scalania.
    """
    try:
        grouped = group_logs_by_course(root)
        if not grouped:
            raise RuntimeError(
                f"No log files matched pattern logs_<COURSE>_<YYYYMMDD-HHMM>.csv under root={root}"
            )
        total_files = sum(len(v) for v in grouped.values())
        total_inserted = 0

        for course, files in grouped.items():
            create_stage_table(con)
            buf: List[EventRawRow] = []

            for fpath in files:
                dialect = detect_csv_dialect(fpath)

                time_idx: Optional[int] = None
                header_ref: Optional[List[str]] = None

                for header, row in iter_csv_rows_streaming(fpath, dialect=dialect):
                    if header_ref is None:
                        header_ref = header
                        time_idx = pick_time_column_index(header_ref)

                    payload = {header[i]: (row[i] if i < len(row) else "") for i in range(len(header))}
                    payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
                    course_id = _extract_course_id_from_payload(payload)

                    time_text = None
                    time_iso = None
                    if time_idx is not None and time_idx < len(row):
                        time_text = row[time_idx]
                        time_iso = _parse_time_to_iso(time_text)

                    row_key = _normalize_fields_key(row)

                    buf.append(
                        EventRawRow(
                            course=course,
                            course_id=course_id,
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

            if export_mode in ("csv", "parquet"):
                assert export_dir is not None
                export_dir.mkdir(parents=True, exist_ok=True)

                if export_mode == "csv":
                    out_csv = export_dir / f"{course}_full_log.csv"
                    export_course_to_csv(con, course=course, out_csv=out_csv)
                else:
                    out_pq = export_dir / f"{course}_full_log.parquet"
                    export_course_to_parquet(con, course=course, out_parquet=out_pq)

        return MergeLogsResult(courses=len(grouped), files=total_files, inserted_rows=total_inserted)

    except UnicodeDecodeError as e:
        # Bezpieczny komunikat (bez ryzyka UnboundLocalError)
        raise RuntimeError(f"UnicodeDecodeError while reading CSV under root={root}") from e


__all__ = ["merge_logs_into_duckdb", "MergeLogsResult", "merge_logs_to_parquet"]
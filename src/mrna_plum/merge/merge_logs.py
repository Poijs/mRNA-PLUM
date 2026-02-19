from __future__ import annotations
from pathlib import Path
import hashlib
import pandas as pd

from ..io.csv_read import read_csv_safely

def _row_hash(row: list[str]) -> str:
    s = "\u241F".join("" if v is None else str(v) for v in row)
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()

def merge_logs_to_parquet(
    input_files: list[Path],
    parquet_out: Path,
    *,
    dedup_per_file: bool = True,
) -> int:
    parquet_out.parent.mkdir(parents=True, exist_ok=True)

    all_frames: list[pd.DataFrame] = []
    total_rows = 0

    for fp in input_files:
        df = read_csv_safely(fp)
        df["_source_file"] = str(fp)

        if dedup_per_file and len(df) > 0:
            # hash całego wiersza -> dedup tylko wewnątrz tego pliku
            hashes = []
            for row in df.astype(str).values.tolist():
                hashes.append(_row_hash(row))
            df["_row_hash"] = hashes
            df = df.drop_duplicates(subset=["_row_hash"]).drop(columns=["_row_hash"])

        total_rows += len(df)
        all_frames.append(df)

    if not all_frames:
        # utwórz pusty parquet dla konsekwencji pipeline
        empty = pd.DataFrame()
        empty.to_parquet(parquet_out, index=False)
        return 0

    merged = pd.concat(all_frames, ignore_index=True)
    merged.to_parquet(parquet_out, index=False)
    return total_rows
from __future__ import annotations

import json
import re
import hashlib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

from rna_plum.io.csv_read import detect_csv_dialect, iter_csv_rows_streaming, pick_time_column_index
from rna_plum.store.duckdb_store import (
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

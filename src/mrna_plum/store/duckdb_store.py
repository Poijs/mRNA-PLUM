from __future__ import annotations
from pathlib import Path
import duckdb


import json
import hashlib
from dataclasses import dataclass
from typing import Iterable, Optional

class DuckDbStore:
    def __init__(self, db_path: Path):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path

    def connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.db_path))

    def init_schema(self) -> None:
        with self.connect() as con:
            con.execute("""
                CREATE TABLE IF NOT EXISTS raw_logs (
                    _source_file VARCHAR,
                    "Czas" VARCHAR,
                    "Kontekst zdarzenia" VARCHAR,
                    "Opis" VARCHAR,
                    "Składnik" VARCHAR,
                    "Nazwa zdarzenia" VARCHAR,
                    course_code VARCHAR,
                    course_id BIGINT,
                    period VARCHAR,
                    tech_key VARCHAR,
                    activity VARCHAR,
                    operation VARCHAR,
                    count_to_report BOOLEAN,
                    teacher_id VARCHAR,
                    object_id VARCHAR,
                    rule_priority INTEGER
                );
            """)
            con.execute("""
                CREATE TABLE IF NOT EXISTS stats_agg (
                    period VARCHAR,
                    course_code VARCHAR,
                    teacher_id VARCHAR,
                    tech_key VARCHAR,
                    cnt_events BIGINT,
                    cnt_objects BIGINT,
                    is_invalidated BOOLEAN
                );
            """)

    def load_parquet_to_raw(self, parquet_path: Path) -> None:
        with self.connect() as con:
            # prosty append; w praktyce możesz TRUNCATE per-run
            con.execute("INSERT INTO raw_logs SELECT * FROM read_parquet(?)", [str(parquet_path)])



@dataclass(frozen=True)
class EventRawRow:
    course: str
    course_id: Optional[int]
    time_text: Optional[str]
    time_ts_iso: Optional[str]  # ISO string (UTC/naive) lub None
    row_key: str                # sha256(normalized_fields_join)
    payload_json: str           # JSON dict: header->value
    source_file: str            # pełna ścieżka lub nazwa


def _connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    # bezpieczne ustawienia, brak wymogów
    return con


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS events_raw (
            course       TEXT NOT NULL,
            time_text    TEXT,
            time_ts      TIMESTAMP,
            row_key      TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            source_file  TEXT NOT NULL,
            inserted_at  TIMESTAMP DEFAULT now()
        );
        """
    )
    # indeksy opcjonalnie (DuckDB "CREATE INDEX" działa w nowszych wersjach)
    try:
        con.execute("CREATE INDEX IF NOT EXISTS idx_events_raw_course ON events_raw(course);")
    except Exception:
        pass
    try:
        con.execute("CREATE INDEX IF NOT EXISTS idx_events_raw_rowkey ON events_raw(row_key);")
    except Exception:
        pass


def create_stage_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("DROP TABLE IF EXISTS _events_raw_stage;")
    con.execute(
        """
        CREATE TEMP TABLE _events_raw_stage (
            course       TEXT NOT NULL,
            time_text    TEXT,
            time_ts      TIMESTAMP,
            row_key      TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            source_file  TEXT NOT NULL
        );
        """
    )


def insert_stage_rows(con: duckdb.DuckDBPyConnection, rows: list[EventRawRow]) -> None:
    if not rows:
        return
    params = [
        (
            r.course,
            r.time_text,
            r.time_ts_iso,  # DuckDB potrafi zrzucić ISO->TIMESTAMP
            r.row_key,
            r.payload_json,
            r.source_file,
        )
        for r in rows
    ]
    con.executemany(
        """
        INSERT INTO _events_raw_stage(course, time_text, time_ts, row_key, payload_json, source_file)
        VALUES (?, ?, ?, ?, ?, ?);
        """,
        params,
    )


def merge_stage_into_events_raw(con: duckdb.DuckDBPyConnection) -> int:
    """
    Dedup: tylko jeśli CAŁY wiersz identyczny (po Trim, bez BOM, bez CR).
    My realizujemy to przez:
      - payload_json z trimowanymi wartościami
      - row_key = sha256(normalized_fields_join)
    Wstawiamy tylko jeśli (course,row_key,payload_json) nie istnieje.
    """
    res = con.execute(
        """
        INSERT INTO events_raw(course, time_text, time_ts, row_key, payload_json, source_file)
        SELECT s.course, s.time_text, s.time_ts, s.row_key, s.payload_json, s.source_file
        FROM _events_raw_stage s
        LEFT JOIN events_raw e
          ON e.course = s.course
         AND e.row_key = s.row_key
         AND e.payload_json = s.payload_json
        WHERE e.course IS NULL;
        """
    )
    # DuckDB python: rowcount by cursor.rowcount is not always reliable; use changes()
    try:
        return con.execute("SELECT COUNT(*) FROM _events_raw_stage").fetchone()[0]
    except Exception:
        return 0


def open_store(db_path: Path) -> duckdb.DuckDBPyConnection:
    con = _connect(db_path)
    ensure_schema(con)
    return con


def export_course_to_csv(
    con: duckdb.DuckDBPyConnection,
    *,
    course: str,
    out_csv: Path,
) -> None:
    """
    CSV-compat: zapis *_full_log.csv posortowany po time_ts malejąco (jeśli jest),
    w przeciwnym razie stabilnie po inserted_at.
    Zapisujemy same payload_json jako jedną kolumnę? – NIE: eksportujemy jako CSV z kolumną payload_json.
    (Jeśli potrzebujesz 1:1 zgodności z VBA-headerami, da się to rozwinąć później na etapie parse.)
    """
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"""
        COPY (
            SELECT course, time_text, time_ts, payload_json, source_file
            FROM events_raw
            WHERE course = ?
            ORDER BY
              CASE WHEN time_ts IS NULL THEN 1 ELSE 0 END,
              time_ts DESC,
              inserted_at DESC
        )
        TO ?
        (HEADER, DELIMITER ';', QUOTE '"', ESCAPE '"');
        """,
        [course, str(out_csv)],
    )


def export_course_to_parquet(
    con: duckdb.DuckDBPyConnection,
    *,
    course: str,
    out_parquet: Path,
) -> None:
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        """
        COPY (
            SELECT course, time_text, time_ts, payload_json, source_file, inserted_at
            FROM events_raw
            WHERE course = ?
            ORDER BY
              CASE WHEN time_ts IS NULL THEN 1 ELSE 0 END,
              time_ts DESC,
              inserted_at DESC
        )
        TO ?
        (FORMAT PARQUET);
        """,
        [course, str(out_parquet)],    )

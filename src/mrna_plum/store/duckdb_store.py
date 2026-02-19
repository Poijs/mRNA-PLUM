from __future__ import annotations
from pathlib import Path
import duckdb

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

# src/mrna_plum/store/database.py

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd


class EventStore:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self.db_path = Path(cfg["paths"]["db_path"])
        self.parquet_root = Path(cfg["paths"]["parquet_root"]) if cfg.get("paths", {}).get("parquet_root") else None

    def _connect(self):
        import duckdb
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(str(self.db_path))

    def ensure_schema(self) -> None:
        con = self._connect()
        try:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS events_canonical_raw (
                    row_key VARCHAR,
                    course VARCHAR,
                    course_code VARCHAR,
                    wydzial_code VARCHAR,
                    kierunek_code VARCHAR,
                    track_code VARCHAR,
                    semester_code VARCHAR,
                    course_name VARCHAR,
                    ay VARCHAR,
                    term VARCHAR,
                    ts_utc TIMESTAMP,
                    teacher_id BIGINT,
                    operation VARCHAR,
                    tech_key VARCHAR,
                    activity_label VARCHAR,
                    object_id BIGINT,
                    count_mode VARCHAR,
                    raw_line_hash VARCHAR,
                    source_file VARCHAR,
                    payload_json VARCHAR
                );
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS events_conflicts (
                    row_key VARCHAR,
                    course_code VARCHAR,
                    teacher_id BIGINT,
                    tech_key VARCHAR,
                    operation VARCHAR,
                    object_id BIGINT,
                    note VARCHAR
                );
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS events_canonical (
                    -- finalna tabela do statystyk
                    row_key VARCHAR,
                    course VARCHAR,
                    course_code VARCHAR,
                    wydzial_code VARCHAR,
                    kierunek_code VARCHAR,
                    track_code VARCHAR,
                    semester_code VARCHAR,
                    course_name VARCHAR,
                    ay VARCHAR,
                    term VARCHAR,
                    ts_utc TIMESTAMP,
                    teacher_id BIGINT,
                    operation VARCHAR,
                    tech_key VARCHAR,
                    activity_label VARCHAR,
                    object_id BIGINT,
                    count_mode VARCHAR,
                    counted BOOLEAN,
                    raw_line_hash VARCHAR,
                    source_file VARCHAR
                );
                """
            )
            # indeksy logiczne / dedup incremental
            con.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_events_canonical_raw_rowkey ON events_canonical_raw(row_key);")
            con.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_events_canonical_rowkey ON events_canonical(row_key);")
        finally:
            con.close()

    def insert_raw_batch(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        con = self._connect()
        try:
            df = pd.DataFrame(rows)
            con.register("df_batch", df)
            # INSERT OR IGNORE po unique index (duckdb: użyj anti-join)
            con.execute(
                """
                INSERT INTO events_canonical_raw
                SELECT b.*
                FROM df_batch b
                LEFT JOIN events_canonical_raw e ON e.row_key = b.row_key
                WHERE e.row_key IS NULL;
                """
            )
            return len(rows)
        finally:
            con.close()

    def insert_conflicts_batch(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        con = self._connect()
        try:
            df = pd.DataFrame(rows)
            con.register("df_conf", df)
            con.execute("INSERT INTO events_conflicts SELECT * FROM df_conf;")
            return len(rows)
        finally:
            con.close()

    def finalize_canonical(self) -> int:
        """
        Zasada:
        - count_mode TAK_FLAG + TAK dla tego samego (course_code, teacher_id, tech_key, object_id)
          -> counted = false dla tych zdarzeń (unieważnienie)
        - jeśli object_id IS NULL -> liczymy event-based: counted=true jeśli count_mode='TAK'
        """
        con = self._connect()
        try:
            # przetwarzaj tylko nowe row_key
            con.execute(
                """
                INSERT INTO events_canonical
                WITH base AS (
                    SELECT r.*
                    FROM events_canonical_raw r
                    LEFT JOIN events_canonical c ON c.row_key = r.row_key
                    WHERE c.row_key IS NULL
                ),
                has_both AS (
                    SELECT
                        course_code,
                        teacher_id,
                        tech_key,
                        object_id,
                        MAX(CASE WHEN upper(count_mode)='TAK' THEN 1 ELSE 0 END) AS has_tak,
                        MAX(CASE WHEN upper(count_mode)='TAK_FLAG' THEN 1 ELSE 0 END) AS has_flag
                    FROM base
                    WHERE object_id IS NOT NULL
                    GROUP BY 1,2,3,4
                )
                SELECT
                    b.row_key,
                    b.course,
                    b.course_code,
                    b.wydzial_code,
                    b.kierunek_code,
                    b.track_code,
                    b.semester_code,
                    b.course_name,
                    b.ay,
                    b.term,
                    b.ts_utc,
                    b.teacher_id,
                    b.operation,
                    b.tech_key,
                    b.activity_label,
                    b.object_id,
                    b.count_mode,
                    CASE
                        WHEN b.object_id IS NULL THEN (upper(b.count_mode)='TAK')
                        ELSE (
                            NOT EXISTS (
                                SELECT 1 FROM has_both hb
                                WHERE hb.course_code=b.course_code
                                  AND hb.teacher_id=b.teacher_id
                                  AND hb.tech_key=b.tech_key
                                  AND hb.object_id=b.object_id
                                  AND hb.has_tak=1 AND hb.has_flag=1
                            )
                            AND (upper(b.count_mode)='TAK')
                        )
                    END AS counted,
                    b.raw_line_hash,
                    b.source_file
                FROM base b;
                """
            )

            # duckdb nie zwraca rowcount wprost stabilnie; policzmy różnicę
            res = con.execute("SELECT COUNT(*) FROM events_canonical;").fetchone()
            return int(res[0]) if res else 0
        finally:
            con.close()

    def export_parquet(self) -> Optional[Path]:
        if not self.parquet_root:
            return None
        out = self.parquet_root / "events_canonical.parquet"
        self.parquet_root.mkdir(parents=True, exist_ok=True)
        con = self._connect()
        try:
            con.execute(f"COPY (SELECT * FROM events_canonical) TO '{str(out).replace('\\\\', '/')}' (FORMAT PARQUET);")
            return out
        finally:
            con.close()
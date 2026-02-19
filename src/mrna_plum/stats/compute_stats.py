from __future__ import annotations
from pathlib import Path
from ..store.duckdb_store import DuckDbStore

def compute_stats(store: DuckDbStore, period: str | None) -> None:
    """
    Zasady (starter):
    - jeśli object_id jest NULL -> liczymy event-based (cnt_events)
    - jeśli object_id jest nie-NULL -> cnt_objects = COUNT(DISTINCT object_id)
    - unieważnienie: dla tego samego course_code+teacher_id+tech_key+object_id
      jeśli są jednocześnie TAK i TAK_FLAG -> is_invalidated = True (i to nie liczymy finalnie w VBA)
    """
    store.init_schema()

    with store.connect() as con:
        # filtr okresu jeśli znany
        period_filter = ""
        params = []
        if period:
            period_filter = "WHERE period = ?"
            params = [period]

        con.execute("DELETE FROM stats_agg")  # per-run

        con.execute(f"""
            INSERT INTO stats_agg
            WITH base AS (
                SELECT
                    period,
                    course_code,
                    teacher_id,
                    tech_key,
                    object_id,
                    operation
                FROM raw_logs
                {period_filter}
                WHERE count_to_report = TRUE
                  AND tech_key IS NOT NULL
            ),
            invalids AS (
                SELECT
                    period, course_code, teacher_id, tech_key, object_id,
                    MAX(CASE WHEN operation='TAK' THEN 1 ELSE 0 END) AS has_tak,
                    MAX(CASE WHEN operation='TAK_FLAG' THEN 1 ELSE 0 END) AS has_flag
                FROM base
                GROUP BY 1,2,3,4,5
            ),
            roll AS (
                SELECT
                    b.period, b.course_code, b.teacher_id, b.tech_key,
                    COUNT(*) AS cnt_events,
                    COUNT(DISTINCT b.object_id) AS cnt_objects,
                    MAX(CASE WHEN i.has_tak=1 AND i.has_flag=1 THEN 1 ELSE 0 END) AS is_invalidated
                FROM base b
                LEFT JOIN invalids i
                  ON i.period=b.period
                 AND i.course_code=b.course_code
                 AND i.teacher_id=b.teacher_id
                 AND i.tech_key=b.tech_key
                 AND ( (i.object_id IS NULL AND b.object_id IS NULL) OR i.object_id=b.object_id )
                GROUP BY 1,2,3,4
            )
            SELECT
                period,
                course_code,
                teacher_id,
                tech_key,
                cnt_events,
                cnt_objects,
                is_invalidated=1 AS is_invalidated
            FROM roll
        """, params)

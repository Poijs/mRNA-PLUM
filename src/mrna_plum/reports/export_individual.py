from __future__ import annotations
from pathlib import Path
import pandas as pd
from ..store.duckdb_store import DuckDbStore

def export_individual_packages(store: DuckDbStore, out_dir: Path) -> int:
    """
    Starter: generuje pliki per (course_code, teacher_id) jako CSV.
    VBA potem robi PDF.
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    with store.connect() as con:
        pairs = con.execute("""
            SELECT DISTINCT course_code, teacher_id
            FROM stats_agg
            WHERE teacher_id IS NOT NULL AND course_code IS NOT NULL
            ORDER BY course_code, teacher_id
        """).fetchall()

        n = 0
        for course_code, teacher_id in pairs:
            df = con.execute("""
                SELECT *
                FROM stats_agg
                WHERE course_code = ? AND teacher_id = ?
                ORDER BY tech_key
            """, [course_code, teacher_id]).df()

            safe_course = str(course_code).replace("/", "_").replace("\\", "_").replace(":", "_")
            safe_teacher = str(teacher_id).replace("/", "_").replace("\\", "_").replace(":", "_")
            out_path = out_dir / f"{safe_course}__{safe_teacher}.csv"
            df.to_csv(out_path, index=False, encoding="utf-8-sig")
            n += 1

        return n

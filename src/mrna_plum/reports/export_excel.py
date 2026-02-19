from __future__ import annotations
from pathlib import Path
import pandas as pd
from ..store.duckdb_store import DuckDbStore

def export_excel_aggregates(store: DuckDbStore, out_xlsx: Path) -> None:
    out_xlsx.parent.mkdir(parents=True, exist_ok=True)
    with store.connect() as con:
        df = con.execute("""
            SELECT * FROM stats_agg
            ORDER BY period, course_code, teacher_id, tech_key
        """).df()

    # Minimalnie: jedna zakładka
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as xw:
        df.to_excel(xw, index=False, sheet_name="AGG")

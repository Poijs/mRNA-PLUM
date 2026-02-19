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

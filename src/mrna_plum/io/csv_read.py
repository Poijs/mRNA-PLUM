from __future__ import annotations
from pathlib import Path
import pandas as pd

def read_csv_safely(path: Path) -> pd.DataFrame:
    # logi mają polskie znaki → utf-8-sig / cp1250 bywa w praktyce
    # starter: próbujemy kilka kodowań
    encodings = ["utf-8-sig", "utf-8", "cp1250", "latin2"]
    last_err: Exception | None = None

    for enc in encodings:
        try:
            return pd.read_csv(path, encoding=enc, dtype=str, low_memory=False)
        except Exception as e:
            last_err = e

    raise last_err  # type: ignore

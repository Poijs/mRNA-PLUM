from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Optional
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

@dataclass(frozen=True)
class CsvDialectInfo:
    delimiter: str
    encoding: str  # "utf-8-sig" lub "cp1250"


_TIME_COL_CANDIDATES = ("Czas", "Time", "Date", "TimeCreated")


def detect_encoding(path: Path) -> str:
    """
    Wymagania:
    - wykrywaj utf-8 z BOM albo windows-1250 (cp1250)
    """
    raw = path.read_bytes()[:8192]
    if raw.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"

    # Spróbuj utf-8 (bez BOM). Jeśli nie da się zdekodować -> cp1250.
    try:
        raw.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        return "cp1250"


def detect_delimiter(sample_text: str) -> str:
    """
    Wymagania: wykrywaj delimiter (TAB / ; / ,)
    Prosta i stabilna heurystyka: policz wystąpienia w pierwszych liniach.
    """
    lines = [ln for ln in sample_text.splitlines() if ln.strip()]
    head = "\n".join(lines[:20]) if lines else sample_text

    candidates = ["\t", ";", ","]
    counts = {c: head.count(c) for c in candidates}
    # Jeśli wszystko 0 -> domyślnie ';' (częste w PL)
    best = max(counts, key=lambda k: counts[k])
    return best if counts[best] > 0 else ";"


def detect_csv_dialect(path: Path) -> CsvDialectInfo:
    enc = detect_encoding(path)
    # czytaj próbkę tekstu do wykrycia delimitera
    with path.open("r", encoding=enc, errors="strict", newline="") as f:
        sample = f.read(16384)
    delim = detect_delimiter(sample)
    # jeśli enc="utf-8" bez BOM, OK; jeśli BOM był, to utf-8-sig
    if enc == "utf-8" and path.read_bytes().startswith(b"\xef\xbb\xbf"):
        enc = "utf-8-sig"
    return CsvDialectInfo(delimiter=delim, encoding=enc)


def iter_csv_rows_streaming(
    path: Path,
    *,
    dialect: Optional[CsvDialectInfo] = None,
) -> Iterator[tuple[list[str], list[str]]]:
    """
    Streamingowy reader CSV:
    - nie ładuje całości do RAM
    - usuwa CR (newline="" + normalizacja)
    - BOM obsługuje encoding utf-8-sig
    Zwraca iterator (header, row_fields).
    """
    d = dialect or detect_csv_dialect(path)

    with path.open("r", encoding=d.encoding, errors="strict", newline="") as f:
        reader = csv.reader(f, delimiter=d.delimiter)
        header: Optional[list[str]] = None

        for row in reader:
            # normalizacja: trim każdej komórki
            row = [c.strip() for c in row]
            # pomijaj puste wiersze
            if not any(row):
                continue
            if header is None:
                header = row
                continue
            assert header is not None
            yield header, row


def pick_time_column_index(header: list[str]) -> Optional[int]:
    """
    Rozpoznaj: "Czas" (preferowane), ale też Time/Date/TimeCreated
    """
    lowered = [h.strip().lower() for h in header]
    for cand in _TIME_COL_CANDIDATES:
        c = cand.lower()
        if c in lowered:
            return lowered.index(c)
    return None
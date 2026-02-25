from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple

from mrna_plum.io.csv_read import read_csv_safely


class InputValidationError(RuntimeError):
    pass


def _norm(s: str) -> str:
    # Normalizacja "odporna": małe litery, bez cudzysłowów, spacje->underscore
    return (
        str(s)
        .strip()
        .strip('"')
        .strip("'")
        .lower()
        .replace("\ufeff", "")      # BOM
        .replace("  ", " ")
        .replace(" ", "_")
        .replace("-", "_")
    )


def _build_colmap(df_cols) -> Dict[str, str]:
    # norm -> oryginalna
    m: Dict[str, str] = {}
    for c in df_cols:
        m[_norm(c)] = str(c)
    return m


def _require_cols(colmap: Dict[str, str], required_norm: Dict[str, str], file_name: str) -> Dict[str, str]:
    """
    required_norm: canonical -> normalized_expected
    returns: canonical -> original_column_name
    """
    missing = []
    out: Dict[str, str] = {}
    for canonical, expected_norm in required_norm.items():
        if expected_norm not in colmap:
            missing.append((canonical, expected_norm))
        else:
            out[canonical] = colmap[expected_norm]

    if missing:
        msg = "; ".join([f"{canon} expects '{exp}'" for canon, exp in missing])
        raise InputValidationError(f"{file_name}: missing required columns: {msg}")

    return out


def validate_and_map_teachers_hr(csv_path: Path) -> Dict[str, str]:
    """
    Twoje HR nagłówki:
    id,Pełna nazwa,E-mail,"ID bazus","Wydział jednostki zatrudnienia","Jednostka podlegajaca rozliczeniu"
    """
    df = read_csv_safely(csv_path)
    colmap = _build_colmap(df.columns)

    required = {
        "teacher_id": "id",
        "full_name": "pełna_nazwa",
        "email": "e_mail",   # bo 'E-mail' -> e_mail
    }
    mapped = _require_cols(colmap, required, csv_path.name)

    # opcjonalne:
    optional = {
        "bazus_id": "id_bazus",
        "wydzial": "wydział_jednostki_zatrudnienia",
        "jednostka": "jednostka_podlegajaca_rozliczeniu",
    }
    for canon, exp in optional.items():
        if exp in colmap:
            mapped[canon] = colmap[exp]

    return mapped


def validate_and_map_roster(csv_path: Path) -> Dict[str, str]:
    """
    Raport uczestników nagłówki:
    ID kursu,Nazwa kursu,Użytkownicy,Studenci,Nauczyciele,...,Studenci zapisani,...
    """
    df = read_csv_safely(csv_path)
    colmap = _build_colmap(df.columns)

    required = {
        "course_id": "id_kursu",
        "students_enrolled": "studenci_zapisani",
    }
    mapped = _require_cols(colmap, required, csv_path.name)

    # opcjonalne:
    optional = {
        "course_name": "nazwa_kursu",
        "students_total": "studenci",
        "teachers_total": "nauczyciele",
    }
    for canon, exp in optional.items():
        if exp in colmap:
            mapped[canon] = colmap[exp]

    return mapped


def validate_and_map_snapshot(csv_path: Path) -> Dict[str, str]:
    """
    Raport zawartości nagłówki:
    Nazwa kursu,ID kursu,Nazwa aktywności,Format aktywności,Link do aktywności,ID aktywności,...
    """
    df = read_csv_safely(csv_path)
    colmap = _build_colmap(df.columns)

    required = {
        "course_id": "id_kursu",
        "activity_id": "id_aktywności",
        "activity_name": "nazwa_aktywności",
        "activity_type": "format_aktywności",
    }
    mapped = _require_cols(colmap, required, csv_path.name)

    # opcjonalne:
    optional = {
        "course_name": "nazwa_kursu",
        "activity_link": "link_do_aktywności",
        "section_title": "tytuł_sekcji",
        "subsection_1": "tytuł_podsekcji_1",
        "subsection_2": "tytuł_podsekcji_2",
    }
    for canon, exp in optional.items():
        if exp in colmap:
            mapped[canon] = colmap[exp]

    return mapped
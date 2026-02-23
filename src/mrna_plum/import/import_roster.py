from __future__ import annotations

import csv
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import duckdb

from mrna_plum.io.csv_read import detect_csv_dialect, iter_csv_rows_streaming  # masz już w projekcie


# ======================================================================================
# Helpers
# ======================================================================================

def _cfg_get(cfg: Any, path: str, default: Any = None) -> Any:
    cur = cfg
    for part in path.split("."):
        if cur is None:
            return default
        if isinstance(cur, dict):
            cur = cur.get(part, None)
        else:
            cur = getattr(cur, part, None)
    return default if cur is None else cur


def _now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _norm_int(x: Any) -> int:
    if x is None:
        return 0
    s = str(x).strip()
    if s == "":
        return 0
    # czasem CSV ma spacje albo "1 234"
    s = s.replace(" ", "").replace("\u00a0", "")
    try:
        return int(float(s))
    except Exception:
        return 0


def _pick(payload: Dict[str, str], *keys: str) -> str:
    for k in keys:
        if k in payload:
            return payload.get(k, "") or ""
    return ""


# ======================================================================================
# Schema
# ======================================================================================

def ensure_roster_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS stage;")
    con.execute("CREATE SCHEMA IF NOT EXISTS dim;")
    con.execute("CREATE SCHEMA IF NOT EXISTS mart;")

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS stage.course_roster_raw (
            course_id                    VARCHAR,
            course_name                  VARCHAR,

            users_total                  BIGINT,
            students_total               BIGINT,
            teachers_total               BIGINT,
            teachers_no_edit             BIGINT,
            teachers_responsible         BIGINT,

            students_enrolled            BIGINT,
            students_completed           BIGINT,
            students_in_progress         BIGINT,
            students_before_start        BIGINT,

            source_file                  VARCHAR,
            loaded_at                    TIMESTAMP DEFAULT now(),
            row_key                      VARCHAR,
            payload_json                 VARCHAR
        );
        """
    )

    # 1 rekord per course_id (ostatni import wygrywa)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS dim.course_roster (
            course_id                    VARCHAR PRIMARY KEY,
            course_name                  VARCHAR,

            users_total                  BIGINT,
            students_total               BIGINT,
            teachers_total               BIGINT,
            teachers_no_edit             BIGINT,
            teachers_responsible         BIGINT,

            students_enrolled            BIGINT,
            students_completed           BIGINT,
            students_in_progress         BIGINT,
            students_before_start        BIGINT,

            source_file                  VARCHAR,
            loaded_at                    TIMESTAMP
        );
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS mart.roster_import_qa (
            source_file      VARCHAR,
            status           VARCHAR,   -- OK / ERROR
            message          VARCHAR,
            rows_inserted    BIGINT,
            imported_at      TIMESTAMP DEFAULT now()
        );
        """
    )


def _row_key(course_id: str, course_name: str, students_enrolled: int, teachers_total: int) -> str:
    # proste i deterministyczne
    return f"{course_id}|{course_name}|{students_enrolled}|{teachers_total}"


# ======================================================================================
# Main import
# ======================================================================================

def import_course_roster_csv(
    con: duckdb.DuckDBPyConnection,
    roster_csv: Path,
) -> Tuple[int, int]:
    """
    Import CSV -> stage.course_roster_raw, then merge into dim.course_roster.
    Returns: (rows_raw_inserted, rows_dim_merged)
    """
    roster_csv = Path(roster_csv)
    if not roster_csv.exists():
        raise FileNotFoundError(roster_csv)

    ensure_roster_tables(con)

    dialect = detect_csv_dialect(roster_csv)
    rows_to_insert: List[Tuple] = []

    # oczekiwane polskie nagłówki
    for header, row in iter_csv_rows_streaming(roster_csv, dialect=dialect):
        payload = {header[i]: (row[i] if i < len(row) else "") for i in range(len(header))}

        course_id = _pick(payload, "ID kursu", "ID kursu ", "course_id").strip()
        course_name = _pick(payload, "Nazwa kursu", "course_name").strip()

        users_total = _norm_int(_pick(payload, "Użytkownicy"))
        students_total = _norm_int(_pick(payload, "Studenci"))
        teachers_total = _norm_int(_pick(payload, "Nauczyciele"))
        teachers_no_edit = _norm_int(_pick(payload, "Nauczyciele bez praw edycji"))
        teachers_responsible = _norm_int(_pick(payload, "Nauczyciele odpowiedzialny", "Nauczyciele odpowiedzialni"))

        students_enrolled = _norm_int(_pick(payload, "Studenci zapisani"))
        students_completed = _norm_int(_pick(payload, "Studenci po ukończeniu"))
        students_in_progress = _norm_int(_pick(payload, "Studenci w trakcie"))
        students_before_start = _norm_int(_pick(payload, "Studenci przed rozpocząciem", "Studenci przed rozpoczęciem"))

        # minimalny wymóg: course_id
        if course_id == "":
            continue

        rk = _row_key(course_id, course_name, students_enrolled, teachers_total)
        payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

        rows_to_insert.append(
            (
                course_id,
                course_name,
                users_total,
                students_total,
                teachers_total,
                teachers_no_edit,
                teachers_responsible,
                students_enrolled,
                students_completed,
                students_in_progress,
                students_before_start,
                str(roster_csv),
                rk,
                payload_json,
            )
        )

    # insert raw
    con.executemany(
        """
        INSERT INTO stage.course_roster_raw (
            course_id, course_name,
            users_total, students_total, teachers_total, teachers_no_edit, teachers_responsible,
            students_enrolled, students_completed, students_in_progress, students_before_start,
            source_file, row_key, payload_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        rows_to_insert,
    )
    rows_raw = len(rows_to_insert)

    # merge -> dim (last loaded_at wins)
    # bierzemy ostatni rekord z raw per course_id wg loaded_at (i ewentualnie rowid)
    con.execute(
        """
        INSERT OR REPLACE INTO dim.course_roster
        SELECT
            course_id,
            any_value(course_name) AS course_name,

            any_value(users_total) AS users_total,
            any_value(students_total) AS students_total,
            any_value(teachers_total) AS teachers_total,
            any_value(teachers_no_edit) AS teachers_no_edit,
            any_value(teachers_responsible) AS teachers_responsible,

            any_value(students_enrolled) AS students_enrolled,
            any_value(students_completed) AS students_completed,
            any_value(students_in_progress) AS students_in_progress,
            any_value(students_before_start) AS students_before_start,

            any_value(source_file) AS source_file,
            max(loaded_at) AS loaded_at
        FROM stage.course_roster_raw
        WHERE source_file = ?
        GROUP BY course_id;
        """,
        [str(roster_csv)],
    )

    # ile w dim “dotknięto” – przybliżenie: distinct course_id z tego pliku
    rows_dim = con.execute(
        "SELECT COUNT(DISTINCT course_id) FROM stage.course_roster_raw WHERE source_file = ?;",
        [str(roster_csv)],
    ).fetchone()[0]

    con.execute(
        "INSERT INTO mart.roster_import_qa(source_file, status, message, rows_inserted) VALUES (?, 'OK', ?, ?);",
        [str(roster_csv), f"Imported roster: {rows_raw} raw rows, {rows_dim} courses", rows_raw],
    )

    return int(rows_raw), int(rows_dim)


def build_course_facts_views(con: duckdb.DuckDBPyConnection) -> None:
    """
    Creates/updates views:
      - mart.course_roster_mapped  (course_id -> course_key + roster counts)
      - mart.course_teachers_active (teachers_active per course_key)
      - mart.course_facts (course-level facts for reporting)
    """
    con.execute("CREATE SCHEMA IF NOT EXISTS mart;")
    con.execute("CREATE SCHEMA IF NOT EXISTS dim;")

    # IMPORTANT: musisz mieć gdzieś mapę course_id -> course_key.
    # Najczęściej w pipeline to jest dim.courses(course_id, course_key, course_name) albo podobnie.
    # Tu zakładamy istnienie: dim.courses(course_id, course_key, course_name).
    con.execute(
        """
        CREATE OR REPLACE VIEW mart.course_roster_mapped AS
        SELECT
            r.course_id,
            c.course_key,
            COALESCE(c.course_name, r.course_name) AS course_name,

            COALESCE(r.students_enrolled, 0) AS students_enrolled,
            COALESCE(r.teachers_total, 0) AS teachers_enrolled,

            COALESCE(r.users_total, 0) AS users_total,
            COALESCE(r.students_total, 0) AS students_total,
            COALESCE(r.teachers_no_edit, 0) AS teachers_no_edit,
            COALESCE(r.teachers_responsible, 0) AS teachers_responsible,

            COALESCE(r.students_completed, 0) AS students_completed,
            COALESCE(r.students_in_progress, 0) AS students_in_progress,
            COALESCE(r.students_before_start, 0) AS students_before_start
        FROM dim.course_roster r
        LEFT JOIN dim.courses c
          ON c.course_id::VARCHAR = r.course_id::VARCHAR;
        """
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW mart.course_teachers_active AS
        SELECT
            course_key,
            COUNT(DISTINCT teacher_id) AS teachers_active
        FROM mart.metrics_long
        WHERE visible_active = TRUE
          AND count_value > 0
          AND course_key IS NOT NULL
          AND TRIM(course_key) <> ''
        GROUP BY course_key;
        """
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW mart.course_facts AS
        SELECT
            rm.course_key,
            rm.course_id,
            rm.course_name,
            rm.students_enrolled,
            rm.teachers_enrolled,
            COALESCE(ta.teachers_active, 0) AS teachers_active
        FROM mart.course_roster_mapped rm
        LEFT JOIN mart.course_teachers_active ta
          ON ta.course_key = rm.course_key;
        """
    )
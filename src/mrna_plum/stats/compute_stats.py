from __future__ import annotations

import json
from dataclasses import dataclass
from logging import root
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Sequence

import duckdb
import pandas as pd
import yaml

from mrna_plum import paths
from mrna_plum.errors import ConfigError
from mrna_plum.paths import ProjectPaths

# ----------------------------
# Helpers / config
# ----------------------------

@dataclass(frozen=True)
class StatsConfig:
    duckdb_path: Path
    run_dir: Path
    include_deleted_in_percent: bool
    rebuild_full: bool

    # mapping sources
    map_teacher_id_email_path: Optional[Path]
    map_email_hr_path: Optional[Path]

    # rounding
    pct_round_decimals: int

    # period
    ay: Optional[str]
    term: Optional[str]


def _load_config(root: Path) -> dict:
    cfg_path = root / "config.yaml"
    if not cfg_path.exists():
        raise FileNotFoundError(f"Brak config.yaml pod: {cfg_path}")
    return yaml.safe_load(cfg_path.read_text(encoding="utf-8"))


def _resolve_path(root: Path, p: Optional[str]) -> Optional[Path]:
    if not p:
        return None
    pp = Path(p)
    return pp if pp.is_absolute() else (root / pp)


def _ensure_run_artifacts(run_dir: Path) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)


def _log_progress(run_dir: Path, payload: dict) -> None:
    p = run_dir / "progress.jsonl"
    payload2 = dict(payload)
    payload2["ts"] = datetime.now(timezone.utc).isoformat()
    with p.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload2, ensure_ascii=False) + "\n")


def _write_ok(run_dir: Path) -> None:
    (run_dir / "compute-stats.ok").write_text(
        datetime.now(timezone.utc).isoformat(),
        encoding="utf-8",
    )


def _read_mapping_teacher_email(path: Optional[Path]) -> pd.DataFrame:
    """
    Oczekiwane kolumny: teacher_id (lub id), email (lub e-mail / mail)
    """
    if path is None:
        return pd.DataFrame(columns=["teacher_id", "email"])
    if not path.exists():
        raise FileNotFoundError(f"Brak pliku mapowania teacher_id→email: {path}")

    if path.suffix.lower() in [".xlsx", ".xls"]:
        df = pd.read_excel(path, dtype=str)
    else:
        df = pd.read_csv(path, dtype=str)

    df = df.rename(columns={c: c.strip() for c in df.columns})
    # normalizacja nazw (tolerancyjnie) — klucze lowercase bez spacji
    cols = {c.lower().strip(): c for c in df.columns}

    tid = (cols.get("teacher_id") or cols.get("id") or cols.get("userid"))

    # POPRAWKA C: dodane "e-mail" — kolumna w dane_do_raportu.csv
    eml = (cols.get("email") or cols.get("mail") or cols.get("e-mail"))

    if not tid or not eml:
        raise ValueError(
            f"Plik {path} musi mieć kolumny teacher_id/id oraz email/e-mail. "
            f"Dostępne kolumny: {list(df.columns)}"
        )

    out = df[[tid, eml]].copy()
    out.columns = ["teacher_id", "email"]
    out["teacher_id"] = out["teacher_id"].astype(str).str.strip()
    out["email"] = out["email"].astype(str).str.strip().str.lower()
    out = out.dropna().drop_duplicates()
    return out


def read_hr_table(hr_file: Path, sheet: str | None,
                  email_col: str, full_name_col: str | None,
                  wydzial_col: str | None, jednostka_col: str | None,
                  passthrough_cols: list[str] | None = None) -> pd.DataFrame:
    if not hr_file.exists():
        raise FileNotFoundError(f"Brak pliku HR: {hr_file}")

    if hr_file.suffix.lower() in [".xlsx", ".xls"]:
        df = pd.read_excel(hr_file, sheet_name=sheet or 0, dtype=str)
    else:
        df = pd.read_csv(hr_file, sep=None, engine="python", dtype=str)

    df = df.rename(columns={c: str(c).strip() for c in df.columns})

    def pick(colname: str | None) -> pd.Series:
        if not colname:
            return pd.Series([""] * len(df))
        if colname not in df.columns:
            raise ValueError(f"HR: brak kolumny '{colname}'. Dostępne: {list(df.columns)}")
        return df[colname].astype(str)

    out = pd.DataFrame()
    out["email"] = pick(email_col).str.strip().str.lower()
    out["full_name"] = pick(full_name_col).str.strip() if full_name_col else ""
    out["wydzial"] = pick(wydzial_col).str.strip() if wydzial_col else ""
    out["jednostka"] = pick(jednostka_col).str.strip() if jednostka_col else ""

    if passthrough_cols:
        for c in passthrough_cols:
            if c not in df.columns:
                raise ValueError(f"HR passthrough: brak kolumny '{c}'")
            out[c] = df[c].astype(str).str.strip()

    out = out[out["email"].notna() & (out["email"] != "")]
    out = out.drop_duplicates(subset=["email"])
    return out


def _read_mapping_email_hr(path: Optional[Path]) -> pd.DataFrame:
    """
    Oczekiwane minimum: email, full_name, wydzial, jednostka.
    
    POPRAWKA C: dodane polskie nazwy kolumn z dane_do_raportu.csv:
      - "Pełna nazwa" / "pelna nazwa"
      - "Wydział jednostki zatrudnienia"
      - "Jednostka podlegajaca rozliczeniu"
    """
    if path is None:
        return pd.DataFrame(columns=["email", "full_name", "wydzial", "jednostka"])
    if not path.exists():
        raise FileNotFoundError(f"Brak pliku mapowania email→HR: {path}")

    if path.suffix.lower() in [".xlsx", ".xls"]:
        df = pd.read_excel(path, dtype=str)
    else:
        df = pd.read_csv(path, dtype=str)

    df = df.rename(columns={c: c.strip() for c in df.columns})
    # klucze lowercase do tolerancyjnego dopasowania
    cols = {c.lower().strip(): c for c in df.columns}

    # email
    email = (cols.get("email") or cols.get("mail") or cols.get("e-mail"))

    # POPRAWKA C: dodane "pełna nazwa" i "pelna nazwa"
    full_name = (
        cols.get("full_name") or cols.get("imie_nazwisko") or
        cols.get("name") or cols.get("nazwiskoimie") or
        cols.get("pełna nazwa") or cols.get("pelna nazwa")
    )

    # POPRAWKA C: dodane "wydział jednostki zatrudnienia"
    wydzial = (
        cols.get("wydzial") or cols.get("wydział") or
        cols.get("wydzial jednostki zatrudnienia") or
        cols.get("wydział jednostki zatrudnienia")
    )

    # POPRAWKA C: dodane "jednostka podlegajaca rozliczeniu"
    jednostka = (
        cols.get("jednostka") or cols.get("unit") or cols.get("katedra") or
        cols.get("jednostka podlegajaca rozliczeniu") or
        cols.get("jednostka podlegająca rozliczeniu")
    )

    if not email:
        raise ValueError(
            f"Plik HR {path} musi mieć kolumnę email/mail/e-mail. "
            f"Dostępne kolumny: {list(df.columns)}"
        )

    out = pd.DataFrame()
    out["email"] = df[email].astype(str).str.strip().str.lower()
    out["full_name"] = df[full_name].astype(str).str.strip() if full_name else ""
    out["wydzial"] = df[wydzial].astype(str).str.strip() if wydzial else ""
    out["jednostka"] = df[jednostka].astype(str).str.strip() if jednostka else ""

    out = out.dropna().drop_duplicates(subset=["email"])
    return out


# ----------------------------
# Main compute
# ----------------------------

def compute_stats(root: Path, ay: Optional[str] = None, term: Optional[str] = None) -> None:
    cfg = _load_config(root)

    paths_obj = ProjectPaths(root=root)

    # POPRAWKA A: szukamy db_path w config, fallback do ProjectPaths
    # Hierarchia: cfg["paths"]["db_path"] → cfg["duckdb_path"] → ProjectPaths
    db_path_str = None
    paths_cfg = cfg.get("paths") or {}
    if isinstance(paths_cfg, dict):
        db_path_str = paths_cfg.get("db_path")
    if not db_path_str:
        db_path_str = cfg.get("duckdb_path")

    duckdb_path = _resolve_path(root, db_path_str) if db_path_str else paths_obj.duckdb_path

    run_dir = _resolve_path(root, cfg.get("run_dir") or "_run") or (root / "_run")

    aggregation = cfg.get("aggregation", {}) or {}
    include_deleted_in_percent = bool(aggregation.get("include_deleted_in_percent", False))
    rebuild_full = bool(cfg.get("rebuild_full", False))

    pct_round_decimals = int(cfg.get("stats", {}).get("pct_round_decimals", 4))

    map_teacher_id_email_path = _resolve_path(root, cfg.get("mapping", {}).get("teacher_id_email"))
    map_email_hr_path = _resolve_path(root, cfg.get("mapping", {}).get("email_hr"))

    # okres: CLI ma pierwszeństwo, potem config
    ay_eff = ay or cfg.get("period", {}).get("ay")
    term_eff = term or cfg.get("period", {}).get("term")
    if not rebuild_full and (not ay_eff or not term_eff):
        raise ConfigError(
            "Brak ay/term. Ustaw period.ay + period.term w config.yaml albo podaj w CLI, albo włącz rebuild_full=true."
        )

    sc = StatsConfig(
        duckdb_path=duckdb_path,
        run_dir=run_dir,
        include_deleted_in_percent=include_deleted_in_percent,
        rebuild_full=rebuild_full,
        map_teacher_id_email_path=map_teacher_id_email_path,
        map_email_hr_path=map_email_hr_path,
        pct_round_decimals=pct_round_decimals,
        ay=ay_eff,
        term=term_eff,
    )

    _ensure_run_artifacts(sc.run_dir)
    _log_progress(sc.run_dir, {"step": "start", "duckdb_path": str(sc.duckdb_path)})

    df_tid_email = _read_mapping_teacher_email(sc.map_teacher_id_email_path)
    df_email_hr = _read_mapping_email_hr(sc.map_email_hr_path)

    con = duckdb.connect(str(sc.duckdb_path))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS mart;")

        con.register("map_tid_email_df", df_tid_email)
        con.register("map_email_hr_df", df_email_hr)

        con.execute("""
            CREATE OR REPLACE TEMP VIEW map_tid_email AS
            SELECT DISTINCT
                teacher_id,
                lower(trim(email)) AS email
            FROM map_tid_email_df
            WHERE teacher_id IS NOT NULL AND teacher_id <> ''
              AND email IS NOT NULL AND email <> '';
        """)

        con.execute("""
            CREATE OR REPLACE TEMP VIEW map_email_hr AS
            SELECT DISTINCT
                lower(trim(email)) AS email,
                nullif(trim(full_name), '') AS full_name,
                nullif(trim(wydzial), '') AS wydzial,
                nullif(trim(jednostka), '') AS jednostka
            FROM map_email_hr_df
            WHERE email IS NOT NULL AND email <> '';
        """)

        period_where = ""
        if not sc.rebuild_full:
            period_where = "AND ay = ? AND term = ?"

        _log_progress(sc.run_dir, {"step": "prepare_events_period", "rebuild_full": sc.rebuild_full, "ay": sc.ay, "term": sc.term})

        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW events_period AS
            SELECT
                course_code,
                ay,
                term,
                wydzial_code,
                kierunek_code,
                track_code,
                semester_code,
                ts_utc,
                teacher_id,
                operation,
                tech_key,
                activity_label,
                object_id,
                count_mode
            FROM events_canonical
            WHERE counted = true
            {period_where};
        """, ([] if sc.rebuild_full else [sc.ay, sc.term]))

        _log_progress(sc.run_dir, {"step": "join_activities_state"})

        con.execute("""
            CREATE OR REPLACE TEMP VIEW joined_state AS
            SELECT
                e.*,
                s.status_final,
                s.deleted_at,
                s.visible_last,
                s.confidence_deleted,
                CASE
                    WHEN s.activity_id IS NULL THEN 1 ELSE 0
                END AS qa_missing_state
            FROM events_period e
            LEFT JOIN mart.activities_state s
              ON s.course_code = e.course_code
             AND s.activity_id = e.object_id;
        """)

        _log_progress(sc.run_dir, {"step": "qa_teacher_mapping"})

        con.execute("""
            CREATE OR REPLACE TEMP VIEW teacher_enriched AS
            SELECT
                j.*,
                m.email,
                h.full_name,
                h.wydzial AS hr_wydzial,
                h.jednostka AS hr_jednostka,
                CASE WHEN m.email ILIKE '%@student.umw.edu.pl' THEN 1 ELSE 0 END AS is_student,
                CASE WHEN m.email IS NULL THEN 1 ELSE 0 END AS qa_missing_email,
                CASE WHEN m.email IS NOT NULL AND h.email IS NULL THEN 1 ELSE 0 END AS qa_missing_hr
            FROM joined_state j
            LEFT JOIN map_tid_email m
              ON m.teacher_id = j.teacher_id
            LEFT JOIN map_email_hr h
              ON h.email = m.email;
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS mart.metrics_qa (
                ay VARCHAR,
                term VARCHAR,
                qa_type VARCHAR,
                teacher_id VARCHAR,
                course_code VARCHAR,
                tech_key VARCHAR,
                object_id VARCHAR,
                details VARCHAR,
                created_at TIMESTAMP
            );
        """)

        if not sc.rebuild_full:
            con.execute("DELETE FROM mart.metrics_qa WHERE ay = ? AND term = ?;", [sc.ay, sc.term])

        con.execute("""
            INSERT INTO mart.metrics_qa
            SELECT ay, term, 'STUDENT_IGNORED' AS qa_type,
                teacher_id, course_code, tech_key, object_id,
                'email=' || coalesce(email,'') AS details, now() AS created_at
            FROM teacher_enriched WHERE is_student = 1;
        """)

        con.execute("""
            INSERT INTO mart.metrics_qa
            SELECT ay, term, 'MISSING_EMAIL_MAPPING' AS qa_type,
                teacher_id, course_code, tech_key, object_id,
                'teacher_id has no email mapping' AS details, now() AS created_at
            FROM teacher_enriched WHERE is_student = 0 AND qa_missing_email = 1;
        """)

        con.execute("""
            INSERT INTO mart.metrics_qa
            SELECT ay, term, 'MISSING_HR_MAPPING' AS qa_type,
                teacher_id, course_code, tech_key, object_id,
                'email=' || coalesce(email,'') AS details, now() AS created_at
            FROM teacher_enriched WHERE is_student = 0 AND qa_missing_email = 0 AND qa_missing_hr = 1;
        """)

        con.execute("""
            INSERT INTO mart.metrics_qa
            SELECT ay, term, 'EVENT_WITHOUT_ACTIVITY_STATE' AS qa_type,
                teacher_id, course_code, tech_key, object_id,
                'no activities_state row for object_id' AS details, now() AS created_at
            FROM teacher_enriched WHERE qa_missing_state = 1;
        """)

        con.execute("""
            INSERT INTO mart.metrics_qa
            SELECT ay, term, 'CONFIDENCE_LT_1' AS qa_type,
                teacher_id, course_code, tech_key, object_id,
                'confidence_deleted=' || cast(confidence_deleted AS VARCHAR) AS details, now() AS created_at
            FROM teacher_enriched
            WHERE confidence_deleted IS NOT NULL AND confidence_deleted < 1;
        """)

        con.execute("""
            CREATE OR REPLACE TEMP VIEW teacher_ok AS
            SELECT * FROM teacher_enriched
            WHERE is_student = 0 AND qa_missing_email = 0 AND qa_missing_hr = 0;
        """)

        con.execute("""
            CREATE OR REPLACE TEMP VIEW visible_ok AS
            SELECT * FROM teacher_ok WHERE status_final = 'visible_active';
        """)

        con.execute("""
            CREATE OR REPLACE TEMP VIEW qa_counts AS
            SELECT ay, term, teacher_id, course_code, tech_key,
                SUM(CASE WHEN status_final = 'deleted' THEN 1 ELSE 0 END) AS deleted_count,
                SUM(CASE WHEN status_final = 'hidden' THEN 1 ELSE 0 END) AS hidden_count,
                SUM(CASE WHEN status_final IS NULL OR status_final = 'unknown' THEN 1 ELSE 0 END) AS unknown_count,
                MAX(CASE WHEN confidence_deleted IS NOT NULL AND confidence_deleted < 1 THEN 1 ELSE 0 END) AS confidence_flag
            FROM teacher_ok GROUP BY 1,2,3,4,5;
        """)

        con.execute("""
            CREATE OR REPLACE TEMP VIEW counts_visible AS
            SELECT ay, term, teacher_id, course_code, wydzial_code, kierunek_code, tech_key,
                any_value(activity_label) AS activity_label,
                CASE
                    WHEN max(count_mode) = 'object-based' THEN COUNT(DISTINCT object_id)
                    ELSE COUNT(*)
                END AS count_value,
                CASE WHEN min(count_mode) <> max(count_mode) THEN 1 ELSE 0 END AS qa_mixed_count_mode
            FROM visible_ok GROUP BY 1,2,3,4,5,6,7;
        """)

        con.execute("""
            INSERT INTO mart.metrics_qa
            SELECT ay, term, 'MIXED_COUNT_MODE' AS qa_type,
                teacher_id, course_code, tech_key, NULL AS object_id,
                'min!=max count_mode in group' AS details, now() AS created_at
            FROM counts_visible WHERE qa_mixed_count_mode = 1;
        """)

        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW metrics_core AS
            SELECT
                c.ay, c.term, c.teacher_id, c.course_code, c.wydzial_code, c.kierunek_code,
                c.tech_key, c.activity_label, c.count_value,
                ROUND(c.count_value / NULLIF(SUM(c.count_value) OVER (PARTITION BY c.ay, c.term, c.course_code, c.tech_key), 0), {sc.pct_round_decimals}) AS pct_course,
                ROUND(c.count_value / NULLIF(SUM(c.count_value) OVER (PARTITION BY c.ay, c.term, c.kierunek_code, c.tech_key), 0), {sc.pct_round_decimals}) AS pct_kierunek,
                ROUND(c.count_value / NULLIF(SUM(c.count_value) OVER (PARTITION BY c.ay, c.term, c.wydzial_code, c.tech_key), 0), {sc.pct_round_decimals}) AS pct_wydzial,
                ROUND(c.count_value / NULLIF(SUM(c.count_value) OVER (PARTITION BY c.ay, c.term, c.tech_key), 0), {sc.pct_round_decimals}) AS pct_uczelnia
            FROM counts_visible c;
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS mart.metrics_long (
                ay VARCHAR, term VARCHAR, teacher_id VARCHAR,
                full_name VARCHAR, email VARCHAR, wydzial VARCHAR, jednostka VARCHAR,
                course_code VARCHAR, tech_key VARCHAR, activity_label VARCHAR,
                count_value BIGINT, pct_course DOUBLE, pct_kierunek DOUBLE,
                pct_wydzial DOUBLE, pct_uczelnia DOUBLE,
                deleted_count BIGINT, hidden_count BIGINT, unknown_count BIGINT,
                confidence_flag BOOLEAN
            );
        """)

        if not sc.rebuild_full:
            _log_progress(sc.run_dir, {"step": "incremental_delete_long", "ay": sc.ay, "term": sc.term})
            con.execute("DELETE FROM mart.metrics_long WHERE ay = ? AND term = ?;", [sc.ay, sc.term])
        else:
            _log_progress(sc.run_dir, {"step": "rebuild_full_long"})
            con.execute("DELETE FROM mart.metrics_long;")

        _log_progress(sc.run_dir, {"step": "insert_metrics_long"})

        con.execute("""
            INSERT INTO mart.metrics_long
            SELECT
                mc.ay, mc.term, mc.teacher_id,
                coalesce(h.full_name, '') AS full_name,
                coalesce(m.email, '') AS email,
                coalesce(h.wydzial, '') AS wydzial,
                coalesce(h.jednostka, '') AS jednostka,
                mc.course_code, mc.tech_key, mc.activity_label,
                mc.count_value, mc.pct_course, mc.pct_kierunek, mc.pct_wydzial, mc.pct_uczelnia,
                coalesce(qa.deleted_count, 0) AS deleted_count,
                coalesce(qa.hidden_count, 0) AS hidden_count,
                coalesce(qa.unknown_count, 0) AS unknown_count,
                coalesce(cast(qa.confidence_flag AS BOOLEAN), false) AS confidence_flag
            FROM metrics_core mc
            LEFT JOIN map_tid_email m ON m.teacher_id = mc.teacher_id
            LEFT JOIN map_email_hr h ON h.email = m.email
            LEFT JOIN qa_counts qa
              ON qa.teacher_id = mc.teacher_id
             AND qa.course_code = mc.course_code
             AND qa.tech_key = mc.tech_key
             AND qa.ay = mc.ay AND qa.term = mc.term;
        """)

        # metrics_wide
        tech_keys_rows = con.execute("SELECT DISTINCT tech_key FROM mart.metrics_long WHERE tech_key IS NOT NULL ORDER BY tech_key;").fetchall()
        tech_keys = [r[0] for r in tech_keys_rows]

        con.execute("""
            CREATE TABLE IF NOT EXISTS mart.metrics_wide (
                ay VARCHAR, term VARCHAR, teacher_id VARCHAR, course_code VARCHAR
            );
        """)

        if not sc.rebuild_full:
            con.execute("DELETE FROM mart.metrics_wide WHERE ay = ? AND term = ?;", [sc.ay, sc.term])
        else:
            con.execute("DELETE FROM mart.metrics_wide;")

        if tech_keys:
            def safe_col(s: str) -> str:
                return "".join(ch if ch.isalnum() else "_" for ch in s)

            select_cols = ["ay", "term", "teacher_id", "course_code"]
            for tk in tech_keys:
                c = safe_col(tk)
                select_cols.append(f"MAX(CASE WHEN tech_key='{tk}' THEN count_value END) AS count_{c}")
                select_cols.append(f"MAX(CASE WHEN tech_key='{tk}' THEN pct_course END) AS pct_course_{c}")
                select_cols.append(f"MAX(CASE WHEN tech_key='{tk}' THEN pct_kierunek END) AS pct_kierunek_{c}")
                select_cols.append(f"MAX(CASE WHEN tech_key='{tk}' THEN pct_wydzial END) AS pct_wydzial_{c}")
                select_cols.append(f"MAX(CASE WHEN tech_key='{tk}' THEN pct_uczelnia END) AS pct_uczelnia_{c}")

            wide_sql = f"""
                INSERT INTO mart.metrics_wide
                SELECT {", ".join(select_cols)}
                FROM mart.metrics_long
                {"WHERE ay = ? AND term = ?" if not sc.rebuild_full else ""}
                GROUP BY ay, term, teacher_id, course_code;
            """

            if not sc.rebuild_full:
                con.execute(wide_sql, [sc.ay, sc.term])
            else:
                con.execute(wide_sql)

        _log_progress(sc.run_dir, {"step": "done"})
        _write_ok(sc.run_dir)

    finally:
        con.close()

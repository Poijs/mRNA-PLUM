from __future__ import annotations

import json
from logging import root
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

import duckdb
import xlsxwriter
from concurrent.futures import ThreadPoolExecutor, as_completed


# ======================================================================================
# Config helpers (tolerant: cfg can be dict-like or attr-like)
# ======================================================================================

def _cfg_get(cfg: Any, path: str, default: Any = None) -> Any:
    """
    Read cfg value using dotted path, supports both dict-like and attribute-like objects.
    Example: _cfg_get(cfg, "reports.max_workers", 4)
    """
    cur = cfg
    for part in path.split("."):
        if cur is None:
            return default
        if isinstance(cur, dict):
            cur = cur.get(part, None)
        else:
            cur = getattr(cur, part, None)
    return default if cur is None else cur


# ======================================================================================
# Logging & artifacts
# ======================================================================================

@dataclass(frozen=True)
class RunArtifacts:
    run_dir: Path
    run_log: Path
    progress_jsonl: Path
    ok_file: Path


def _ensure_artifacts(root: Path) -> RunArtifacts:
    run_dir = root / "_run"
    run_dir.mkdir(parents=True, exist_ok=True)
    run_log = run_dir / "run.log"
    progress_jsonl = run_dir / "progress.jsonl"
    ok_file = run_dir / "export-individual.ok"
    return RunArtifacts(run_dir, run_log, progress_jsonl, ok_file)


def _log(line: str, run_log: Path) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    msg = f"{ts} [export-individual] {line}"
    print(msg)
    with run_log.open("a", encoding="utf-8") as f:
        f.write(msg + "\n")


def _progress(evt: Dict[str, Any], progress_jsonl: Path) -> None:
    evt = dict(evt)
    evt.setdefault("ts", time.strftime("%Y-%m-%d %H:%M:%S"))
    with progress_jsonl.open("a", encoding="utf-8") as f:
        f.write(json.dumps(evt, ensure_ascii=False) + "\n")


# ======================================================================================
# Filename sanitization
# ======================================================================================

_WIN_ILLEGAL = r'<>:"/\|?*'
_WIN_ILLEGAL_RE = re.compile(rf"[{re.escape(_WIN_ILLEGAL)}]")


def sanitize_filename(name: str, max_len: int = 120) -> str:
    """
    - replace illegal Windows filename chars with '_'
    - collapse whitespace
    - trim
    - limit length
    """
    if not name:
        return ""
    s = str(name).strip()
    s = _WIN_ILLEGAL_RE.sub("_", s)
    s = re.sub(r"\s+", " ", s)
    s = s.strip(" .")  # Windows hates trailing dot/space
    if len(s) > max_len:
        s = s[:max_len].rstrip(" .")
    return s


# ======================================================================================
# DuckDB SQL
# ======================================================================================

def _ensure_qa_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS mart;")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS mart.individual_export_qa (
            teacher_id      VARCHAR,
            status          VARCHAR,  -- OK / SKIPPED_* / ERROR
            message         VARCHAR,
            output_file     VARCHAR,
            rows_exported   BIGINT,
            exported_at     TIMESTAMP DEFAULT now()
        );
        """
    )


def _qa_insert(
    con: duckdb.DuckDBPyConnection,
    teacher_id: str,
    status: str,
    message: str,
    output_file: Optional[str],
    rows_exported: int,
) -> None:
    con.execute(
        """
        INSERT INTO mart.individual_export_qa
            (teacher_id, status, message, output_file, rows_exported)
        VALUES (?, ?, ?, ?, ?)
        """,
        [teacher_id, status, message, output_file, rows_exported],
    )


def _detect_hr_columns(con: duckdb.DuckDBPyConnection) -> List[str]:
    """
    Prefer HR embedded in mart.metrics_long as dynamic columns hr_*.
    Return list of column names present in metrics_long matching hr_*.
    """
    rows = con.execute("DESCRIBE mart.metrics_long;").fetchall()
    col_names = [r[0] for r in rows]
    return [c for c in col_names if c.lower().startswith("hr_")]


def _list_teachers(con: duckdb.DuckDBPyConnection) -> List[Tuple[str, str, str, str]]:
    """
    Teachers to export (metrics_long already HR-whitelisted).
    EXPORT RULE:
      - require teacher_id AND email (non-empty); otherwise teacher is not included here.
      - require id_bazus for filename (we still include them here; missing bazus handled in worker).
    Deterministic order: teacher_id asc.
    Returns: (teacher_id, full_name, email, id_bazus)
    """
    rows = con.execute(
        """
        SELECT
            teacher_id::VARCHAR AS teacher_id,
            COALESCE(NULLIF(TRIM(full_name), ''), '') AS full_name,
            COALESCE(NULLIF(TRIM(email), ''), '') AS email,
            COALESCE(NULLIF(TRIM(id_bazus), ''), '') AS id_bazus
        FROM mart.metrics_long
        WHERE visible_active = TRUE
        GROUP BY 1, 2, 3, 4
        HAVING teacher_id IS NOT NULL
           AND TRIM(teacher_id) <> ''
           AND email IS NOT NULL
           AND TRIM(email) <> ''
        ORDER BY teacher_id ASC
        """
    ).fetchall()
    return [(str(tid), str(fn), str(em), str(bz)) for tid, fn, em, bz in rows]


def _fetch_teacher_rows(
    con: duckdb.DuckDBPyConnection,
    teacher_id: str,
) -> Iterator[Tuple[Any, ...]]:
    """
    Rows for DANE_KURSY:
      course_name, activity_label, count_value,
      pct_course/100.0, pct_kierunek/100.0, pct_wydzial/100.0, pct_uczelnia/100.0

    Deterministic sort, count_value>0 only.
    """
    cur = con.execute(
        """
        SELECT
            course_name,
            activity_label,
            count_value,
            (pct_course / 100.0)   AS pct_course_xlsx,
            (pct_kierunek / 100.0) AS pct_kierunek_xlsx,
            (pct_wydzial / 100.0)  AS pct_wydzial_xlsx,
            (pct_uczelnia / 100.0) AS pct_uczelnia_xlsx
        FROM mart.metrics_long
        WHERE visible_active = TRUE
          AND teacher_id::VARCHAR = ?
          AND count_value > 0
        ORDER BY
            course_name ASC,
            activity_label ASC
        """,
        [teacher_id],
    )
    while True:
        batch = cur.fetchmany(10_000)
        if not batch:
            break
        for row in batch:
            yield row


def _fetch_teacher_pers(
    con: duckdb.DuckDBPyConnection,
    teacher_id: str,
    hr_cols: List[str],
) -> Dict[str, Any]:
    """
    One row with "metrics_long embedded HR" preference.
    We take MAX(...) as safe collapse (same per teacher).
    Assumes email + id_bazus exist in schema (per your rules).
    """
    select_parts = [
        "teacher_id::VARCHAR AS teacher_id",
        "MAX(COALESCE(NULLIF(TRIM(full_name), ''), '')) AS full_name",
        "MAX(COALESCE(NULLIF(TRIM(email), ''), '')) AS email",
        "MAX(COALESCE(NULLIF(TRIM(id_bazus), ''), '')) AS id_bazus",
    ]

    for c in hr_cols:
        select_parts.append(f"MAX(COALESCE(NULLIF(TRIM({c}), ''), '')) AS {c}")

    sql = f"""
        SELECT {", ".join(select_parts)}
        FROM mart.metrics_long
        WHERE visible_active = TRUE
          AND teacher_id::VARCHAR = ?
        GROUP BY teacher_id
    """
    row = con.execute(sql, [teacher_id]).fetchone()
    if row is None:
        base = {"teacher_id": teacher_id, "full_name": "", "email": "", "id_bazus": ""}
        for c in hr_cols:
            base[c] = ""
        return base

    keys = ["teacher_id", "full_name", "email", "id_bazus"] + hr_cols
    return dict(zip(keys, row))


def _hr_human_label(col: str) -> str:
    """
    Map hr_* columns to human-friendly labels for DANE_PERS.
    Extend as you standardize HR fields.
    """
    m = {
        "hr_wydzial": "Wydział",
        "hr_jednostka": "Jednostka",
        "hr_katedra": "Katedra",
        "hr_zaklad": "Zakład",
        "hr_stanowisko": "Stanowisko",
        "hr_tytul": "Tytuł / Stopień",
        "hr_umowa": "Rodzaj umowy",
    }
    low = col.lower()
    return m.get(low, col)  # fallback to raw column name


# ======================================================================================
# XLSX writing
# ======================================================================================

_COURSES_HEADERS = ["Kurs", "Aktywność", "Liczba", "% kurs", "% kierunek", "% wydział", "% uczelnia"]


def _write_teacher_xlsx(
    out_file: Path,
    teacher_id: str,
    full_name: str,
    rows_iter: Iterator[Tuple[Any, ...]],
    pers: Dict[str, Any],
    hr_cols: List[str],
) -> int:
    """
    Returns rows_exported (DANE_KURSY count).
    """
    out_file.parent.mkdir(parents=True, exist_ok=True)

    wb = xlsxwriter.Workbook(out_file.as_posix(), {"constant_memory": True})
    try:
        fmt_header = wb.add_format({"bold": True})
        fmt_pct = wb.add_format({"num_format": "0.0%"})
        fmt_int = wb.add_format({"num_format": "0"})  # count_value

        # Sheet 1: DANE_KURSY
        ws1 = wb.add_worksheet("DANE_KURSY")
        for c, h in enumerate(_COURSES_HEADERS):
            ws1.write(0, c, h, fmt_header)

        r = 1
        for (
            course_name,
            activity_label,
            count_value,
            pct_course_x,
            pct_kierunek_x,
            pct_wydzial_x,
            pct_uczelnia_x,
        ) in rows_iter:
            ws1.write(r, 0, course_name if course_name is not None else "")
            ws1.write(r, 1, activity_label if activity_label is not None else "")

            try:
                ws1.write_number(r, 2, float(count_value), fmt_int)
            except Exception:
                ws1.write(r, 2, count_value)

            for j, v in enumerate([pct_course_x, pct_kierunek_x, pct_wydzial_x, pct_uczelnia_x], start=3):
                if v is None:
                    ws1.write_blank(r, j, None)
                else:
                    ws1.write_number(r, j, float(v), fmt_pct)

            r += 1

        rows_exported = r - 1

        # Sheet 2: DANE_PERS (vertical key -> value)
        ws2 = wb.add_worksheet("DANE_PERS")
        ws2.write(0, 0, "Pole", fmt_header)
        ws2.write(0, 1, "Wartość", fmt_header)

        # Required minimum fields + your "Imię i nazwisko"
        name_val = pers.get("full_name", full_name) or full_name or ""
        kv: List[Tuple[str, Any]] = [
            ("ID_PLUM", teacher_id),
            ("Imię i nazwisko", name_val),
            ("E-mail", pers.get("email", "") or ""),
            ("ID bazus", pers.get("id_bazus", "") or ""),
        ]

        # HR -> human labels (dynamic)
        for c in hr_cols:
            kv.append((_hr_human_label(c), pers.get(c, "") or ""))

        # Ensure at least Wydział/Jednostka rows exist even if hr cols absent
        if not any(_hr_human_label(c) == "Wydział" for c in hr_cols):
            kv.append(("Wydział", ""))
        if not any(_hr_human_label(c) == "Jednostka" for c in hr_cols):
            kv.append(("Jednostka", ""))

        for i, (k, v) in enumerate(kv, start=1):
            ws2.write(i, 0, k)
            ws2.write(i, 1, v)

        return rows_exported
    finally:
        wb.close()


# ======================================================================================
# Public API
# ======================================================================================

def export_individual_reports(
    con: duckdb.DuckDBPyConnection,
    cfg: Any,
) -> Tuple[int, str]:
    """
    Public entrypoint:
      export_individual_reports(con, cfg) -> (exit_code, out_dir)

    Rules applied:
      - SQL-first (DuckDB)
      - only visible_active
      - exclude count_value=0 from export
      - require teacher_id + email (non-empty) or SKIP
      - filename: <NazwiskoImie>_<BAZUS ID>.xlsx (sanitized)
      - pct columns: DB 0-100 -> XLSX pct/100.0 with format 0.0%
      - deterministic sort: course_name ASC, activity_label ASC
      - idempotent overwrite
    """
    root = Path(_cfg_get(cfg, "root", ".")).resolve()
    arts = _ensure_artifacts(root)

    out_rel = _cfg_get(cfg, "reports.individual_dir", "_out/indywidualne")
    out_rel_p = Path(out_rel)
    out_dir = (out_rel_p if out_rel_p.is_absolute() else (root / out_rel_p)).resolve()
    max_workers = int(_cfg_get(cfg, "reports.max_workers", 4))
    batch_teachers = int(_cfg_get(cfg, "reports.batch_teachers", 50))

    _log(f"root={root}", arts.run_log)
    _log(f"out_dir={out_dir}", arts.run_log)
    _log(f"max_workers={max_workers} batch_teachers={batch_teachers}", arts.run_log)

    _ensure_qa_table(con)
    hr_cols = _detect_hr_columns(con)
    _log(f"Detected HR columns in mart.metrics_long: {hr_cols}", arts.run_log)

    teachers = _list_teachers(con)  # (teacher_id, full_name, email, id_bazus) with email required
    _log(f"Teachers to export (email required): {len(teachers)}", arts.run_log)

    # For parallel: DuckDB connection is not safely shared across threads.
    # We'll open a separate connection per worker using cfg.paths.db_path.
    db_path = _cfg_get(cfg, "paths.db_path", None)

    def _worker(teacher_id: str, full_name: str, email: str, id_bazus: str) -> Tuple[str, str, str, Optional[str], int]:
        """
        Returns: (teacher_id, status, message, output_file, rows_exported)
        """
        local_con = con
        must_close = False
        try:
            # hard guards (your rule)
            if not teacher_id or not str(teacher_id).strip():
                return (str(teacher_id), "SKIPPED_NO_ID", "Missing teacher_id", None, 0)
            if not email or not str(email).strip():
                return (str(teacher_id), "SKIPPED_NO_EMAIL", "Missing email", None, 0)
            if not id_bazus or not str(id_bazus).strip():
                return (str(teacher_id), "SKIPPED_NO_BAZUS", "Missing BAZUS ID for filename", None, 0)

            if max_workers and max_workers > 1:
                if not db_path:
                    # no db_path -> do not parallelize safely
                    local_con = con
                else:
                    local_con = duckdb.connect(str(db_path))
                    must_close = True

            safe_name = sanitize_filename(full_name, max_len=120) or "UNKNOWN"
            safe_bazus = sanitize_filename(str(id_bazus), max_len=60) or "BAZUS_UNKNOWN"

            filename = f"{safe_name}_{safe_bazus}.xlsx"
            if len(filename) > 180:
                safe_name2 = sanitize_filename(safe_name, max_len=120)
                filename = f"{safe_name2}_{safe_bazus}.xlsx"

            out_file = out_dir / filename

            # fetch rows (streaming)
            rows_iter = _fetch_teacher_rows(local_con, teacher_id)

            # Need to know if any rows exist without consuming iterator -> buffer first item
            buffered: List[Tuple[Any, ...]] = []
            try:
                buffered.append(next(rows_iter))
            except StopIteration:
                buffered = []

            if not buffered:
                return (teacher_id, "SKIPPED_NO_DATA", "No rows with count_value>0", None, 0)

            def _chain() -> Iterator[Tuple[Any, ...]]:
                for x in buffered:
                    yield x
                for x in rows_iter:
                    yield x

            pers = _fetch_teacher_pers(local_con, teacher_id, hr_cols)

            # idempotent overwrite
            if out_file.exists():
                out_file.unlink()

            rows_exported = _write_teacher_xlsx(
                out_file=out_file,
                teacher_id=teacher_id,
                full_name=full_name,
                rows_iter=_chain(),
                pers=pers,
                hr_cols=hr_cols,
            )

            if rows_exported == 0:
                if out_file.exists():
                    out_file.unlink()
                return (teacher_id, "SKIPPED_NO_DATA", "No rows with count_value>0", None, 0)

            return (teacher_id, "OK", "Exported", str(out_file), int(rows_exported))

        except Exception as e:
            return (teacher_id, "ERROR", f"{type(e).__name__}: {e}", None, 0)
        finally:
            if must_close:
                try:
                    local_con.close()
                except Exception:
                    pass

    exported_ok = 0
    exported_err = 0
    exported_skip = 0

    def _batched(seq: Sequence[Tuple[str, str, str, str]], n: int) -> Iterator[List[Tuple[str, str, str, str]]]:
        for i in range(0, len(seq), n):
            yield list(seq[i : i + n])

    for batch in _batched(teachers, batch_teachers):
        if max_workers and max_workers > 1 and db_path:
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futs = [ex.submit(_worker, tid, fn, em, bz) for (tid, fn, em, bz) in batch]
                for fut in as_completed(futs):
                    tid, status, msg, out_file, rows_exported = fut.result()
                    _qa_insert(con, tid, status, msg, out_file, rows_exported)
                    _progress(
                        {
                            "teacher_id": tid,
                            "status": status,
                            "message": msg,
                            "output_file": out_file,
                            "rows_exported": rows_exported,
                        },
                        arts.progress_jsonl,
                    )
                    if status == "OK":
                        exported_ok += 1
                    elif status.startswith("SKIPPED"):
                        exported_skip += 1
                    else:
                        exported_err += 1
        else:
            for (tid, fn, em, bz) in batch:
                tid, status, msg, out_file, rows_exported = _worker(tid, fn, em, bz)
                _qa_insert(con, tid, status, msg, out_file, rows_exported)
                _progress(
                    {
                        "teacher_id": tid,
                        "status": status,
                        "message": msg,
                        "output_file": out_file,
                        "rows_exported": rows_exported,
                    },
                    arts.progress_jsonl,
                )
                if status == "OK":
                    exported_ok += 1
                elif status.startswith("SKIPPED"):
                    exported_skip += 1
                else:
                    exported_err += 1

    _log(f"Done. OK={exported_ok} SKIPPED={exported_skip} ERROR={exported_err}", arts.run_log)

    if exported_err == 0:
        arts.ok_file.write_text("OK\n", encoding="utf-8")
        return (0, str(out_dir))
    return (2, str(out_dir))


# ======================================================================================
# Optional CLI wrapper (adapt to your cli.py / Typer)
# ======================================================================================

def cli_export_individual(root: str, config: Any) -> int:
    """
    Example CLI wrapper (adapt to your existing cli.py).
    - root: pipeline root
    - config: loaded config object/dict
    """
    if isinstance(config, dict):
        config = dict(config)
        config["root"] = root
    else:
        setattr(config, "root", root)

    db_path = _cfg_get(config, "paths.db_path", None)
    if not db_path:
        raise RuntimeError("cfg.paths.db_path is required for export-individual")

    con = duckdb.connect(str(db_path))
    try:
        code, _out = export_individual_reports(con, config)
        return int(code)
    finally:
        con.close()
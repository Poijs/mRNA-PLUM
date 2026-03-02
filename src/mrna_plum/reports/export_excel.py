from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

# xlsxwriter is used directly (fast, pyinstaller-friendly)
import xlsxwriter


# ===== Exit codes (align with project) =====
EXIT_OK = 0
EXIT_OVERFLOW = 30


@dataclass(frozen=True)
class ExportExcelConfig:
    max_rows_excel: int = 1_000_000
    overflow_strategy: str = "error"  # error | split | skip
    activity_column: str = "activity_label"  # NOW default
    course_column: str = "course_name"       # NOW default

    include_hr_cols: bool = True
    exclude_zero_counts: bool = True
    percent_excel_format: bool = True

class ExportExcelError(RuntimeError):
    pass


class ExportOverflowError(ExportExcelError):
    """Raised when row-count exceeds Excel limit and strategy=error."""
    pass


def _ensure_dirs(root: Path) -> Tuple[Path, Path]:
    run_dir = root / "_run"
    out_dir = root / "_out"
    run_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    return run_dir, out_dir


def _setup_logger(run_dir: Path) -> logging.Logger:
    logger = logging.getLogger("mrna_plum.export_excel")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fh = logging.FileHandler(run_dir / "run.log", encoding="utf-8")
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # optional console handler (kept minimal)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    return logger


def _progress_append(run_dir: Path, event: Dict[str, Any]) -> None:
    p = run_dir / "progress.jsonl"
    event = dict(event)
    event["ts"] = datetime.now(timezone.utc).isoformat()
    with p.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")


def _duckdb_has_column(con, table_fqn: str, col: str) -> bool:
    # Works for DuckDB: INFORMATION_SCHEMA.COLUMNS
    sql = """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = ? AND table_name = ? AND column_name = ?
        LIMIT 1
    """
    if "." in table_fqn:
        schema, name = table_fqn.split(".", 1)
    else:
        schema, name = "main", table_fqn
    row = con.execute(sql, [schema, name, col]).fetchone()
    return row is not None


def _select_metrics_long_sql(con, activity_col: str, course_col: str,
                            include_hr_cols: bool, exclude_zero_counts: bool,
                            percent_excel_format: bool) -> str:
    table = "mart.metrics_long"

    # activity_label fallback
    if activity_col == "activity_label" and not _duckdb_has_column(con, table, "activity_label"):
        activity_col = "tech_key"

    # course_name fallback
    if course_col == "course_name" and not _duckdb_has_column(con, table, "course_name"):
        course_col = "course_code"

    pct_course = "pct_course" if _duckdb_has_column(con, table, "pct_course") else "NULL"
    pct_program = "pct_program" if _duckdb_has_column(con, table, "pct_program") else "NULL"
    pct_faculty = "pct_faculty" if _duckdb_has_column(con, table, "pct_faculty") else "NULL"
    pct_university = "pct_university" if _duckdb_has_column(con, table, "pct_university") else "NULL"

    # Excel %: zapisujemy ułamek (12.3 -> 0.123)
    def pct_expr(expr: str) -> str:
        if expr == "NULL":
            return "NULL"
        return f"({expr} / 100.0)" if percent_excel_format else expr

    pct_course_e = pct_expr(pct_course)
    pct_program_e = pct_expr(pct_program)
    pct_faculty_e = pct_expr(pct_faculty)
    pct_university_e = pct_expr(pct_university)

    order_course = "course_code" if _duckdb_has_column(con, table, "course_code") else course_col
    order_tech = "tech_key" if _duckdb_has_column(con, table, "tech_key") else activity_col

    where_parts = []
    if _duckdb_has_column(con, table, "visible_active"):
        where_parts.append("visible_active")
    if exclude_zero_counts and _duckdb_has_column(con, table, "count_value"):
        where_parts.append("count_value <> 0")

    where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    # HR columns (optional, only if present)
    hr_cols = []
    if include_hr_cols:
        for col in ("hr_faculty", "hr_unit", "hr_department", "hr_org"):
            if _duckdb_has_column(con, table, col):
                hr_cols.append(f"{col}::VARCHAR AS {col}")

    hr_select = (",\n            " + ",\n            ".join(hr_cols)) if hr_cols else ""

    sql = f"""
        SELECT
            full_name::VARCHAR AS full_name,
            teacher_id::VARCHAR AS teacher_id{hr_select},
            {course_col}::VARCHAR AS course_value,
            {activity_col}::VARCHAR AS activity_value,
            count_value::BIGINT AS count_value,
            {pct_course_e}::DOUBLE AS pct_course,
            {pct_program_e}::DOUBLE AS pct_program,
            {pct_faculty_e}::DOUBLE AS pct_faculty,
            {pct_university_e}::DOUBLE AS pct_university,
            {order_tech}::VARCHAR AS _order_tech
        FROM {table}
        {where_clause}
        ORDER BY
            full_name ASC,
            {order_course} ASC,
            {order_tech} ASC
    """
    return sql


def _count_rows(con, base_sql: str) -> int:
    # Wrap in subquery; DuckDB handles it well.
    sql = f"SELECT COUNT(*)::BIGINT FROM ({base_sql}) t"
    return int(con.execute(sql).fetchone()[0])


def _select_metrics_qa_sql(con) -> str:
    table = "mart.metrics_qa"
    # Minimal required columns; if missing -> NULL
    type_col = "type" if _duckdb_has_column(con, table, "type") else "NULL"
    teacher_id = "teacher_id" if _duckdb_has_column(con, table, "teacher_id") else "NULL"
    course_code = "course_code" if _duckdb_has_column(con, table, "course_code") else "NULL"
    tech_key = "tech_key" if _duckdb_has_column(con, table, "tech_key") else "NULL"
    description = "description" if _duckdb_has_column(con, table, "description") else "NULL"

    sql = f"""
        SELECT
            {type_col}::VARCHAR AS type,
            {teacher_id}::VARCHAR AS teacher_id,
            {course_code}::VARCHAR AS course_code,
            {tech_key}::VARCHAR AS tech_key,
            {description}::VARCHAR AS description
        FROM {table}
        ORDER BY type ASC, teacher_id ASC, course_code ASC, tech_key ASC
    """
    return sql


def _write_sheet_header(ws, header_fmt, headers: Sequence[str]) -> None:
    ws.write_row(0, 0, list(headers), header_fmt)
    ws.freeze_panes(1, 0)


def _iter_cursor_rows(cur, batch_size: int) -> Iterable[Tuple[Any, ...]]:
    while True:
        rows = cur.fetchmany(batch_size)
        if not rows:
            break
        for r in rows:
            yield r


def _write_metrics_long_split_streaming(
    workbook: xlsxwriter.Workbook,
    con,
    sql: str,
    max_rows: int,
    overflow_strategy: str,
    main_sheet_base: str,
    logger: logging.Logger,
) -> Tuple[int, int]:
    """
    Stream rows from DuckDB and write to one or multiple sheets.
    Returns: (written_rows, sheets_count) excluding header row.
    """
    headers = [
        "Użytkownik",
        "ID",
        "Kurs",
        "Aktywność",
        "Liczba",
        "% kurs",
        "% kierunek",
        "% wydział",
        "% uczelnia",
    ]

    header_fmt = workbook.add_format({"bold": True, "border": 1})
    # Percent format: number with 1 decimal place, NOT Excel percentage
    pct_fmt = workbook.add_format({"num_format": "0.0"})
    int_fmt = workbook.add_format({"num_format": "0"})
    text_fmt = workbook.add_format({})  # default

    # column widths (optional but helpful)
    col_widths = [28, 12, 18, 26, 10, 10, 12, 12, 12]

    def make_sheet(idx: int):
        name = main_sheet_base if idx == 1 else f"{main_sheet_base}_{idx}"
        ws = workbook.add_worksheet(name[:31])
        for i, w in enumerate(col_widths):
            ws.set_column(i, i, w)
        _write_sheet_header(ws, header_fmt, headers)
        return ws

    ws = make_sheet(1)
    sheet_idx = 1
    row_in_sheet = 1  # start after header
    total_written = 0

    cur = con.execute(sql)
    colnames = [d[0] for d in cur.description]
    hr_present = [c for c in colnames if c.startswith("hr_")]

    for rec in _iter_cursor_rows(cur, batch_size=10_000):
        # If current sheet is full:
        if row_in_sheet > max_rows:
            if overflow_strategy == "split":
                sheet_idx += 1
                ws = make_sheet(sheet_idx)
                row_in_sheet = 1
            else:
                # overflow_strategy 'skip' shouldn't land here (handled earlier),
                # and 'error' should be prevented by pre-count logic.
                break

        # rec order:
        # full_name, teacher_id, course_value, activity_value, count_value,
        # pct_course, pct_program, pct_faculty, pct_university, _order_tech
        full_name, teacher_id, course_value, activity_value, count_value, p1, p2, p3, p4, _ = rec

        # Write row in one call (fast), with formats for numeric columns only
        ws.write(row_in_sheet, 0, full_name, text_fmt)
        ws.write(row_in_sheet, 1, teacher_id, text_fmt)
        ws.write(row_in_sheet, 2, course_value, text_fmt)
        ws.write(row_in_sheet, 3, activity_value, text_fmt)

        # count (int)
        ws.write_number(row_in_sheet, 4, float(count_value or 0), int_fmt)

        # percents (numbers with 1 decimal place display)
        # Note: ROUND is already done upstream; we only format.
        def write_pct(col: int, val: Any):
            if val is None:
                ws.write_blank(row_in_sheet, col, None)
            else:
                ws.write_number(row_in_sheet, col, float(val), pct_fmt)

        write_pct(5, p1)
        write_pct(6, p2)
        write_pct(7, p3)
        write_pct(8, p4)

        row_in_sheet += 1
        total_written += 1

    logger.info("Wrote %s rows into %s sheet(s).", total_written, sheet_idx)
    return total_written, sheet_idx


def _write_qa_sheet(workbook: xlsxwriter.Workbook, con, logger: logging.Logger) -> int:
    ws = workbook.add_worksheet("QA")
    header_fmt = workbook.add_format({"bold": True, "border": 1})
    text_fmt = workbook.add_format({})

    headers = ["type", "teacher_id", "course_code", "tech_key", "description"]
    ws.set_column(0, 0, 22)
    ws.set_column(1, 3, 16)
    ws.set_column(4, 4, 80)

    _write_sheet_header(ws, header_fmt, headers)

    sql = _select_metrics_qa_sql(con)
    cur = con.execute(sql)

    r = 1
    for row in _iter_cursor_rows(cur, batch_size=10_000):
        ws.write_row(r, 0, list(row), text_fmt)
        r += 1

    logger.info("QA rows: %s", r - 1)
    return r - 1


def _write_info_sheet(
    workbook: xlsxwriter.Workbook,
    con,
    base_sql_metrics_long: str,
    ay: str,
    term: str,
    generated_at: datetime,
    logger: logging.Logger,
) -> None:
    ws = workbook.add_worksheet("INFO")
    bold = workbook.add_format({"bold": True})
    ws.set_column(0, 0, 22)
    ws.set_column(1, 1, 40)

    # counts from SQL (no pandas aggregation)
    total = int(con.execute(f"SELECT COUNT(*)::BIGINT FROM ({base_sql_metrics_long}) t").fetchone()[0])

    teachers = int(
        con.execute(f"SELECT COUNT(DISTINCT teacher_id)::BIGINT FROM ({base_sql_metrics_long}) t").fetchone()[0]
    )

    courses = int(
        con.execute(f"SELECT COUNT(DISTINCT course_value)::BIGINT FROM ({base_sql_metrics_long}) t").fetchone()[0]
    )

    rows = [
        ("ay", ay),
        ("term", term),
        ("data_wygenerowania_utc", generated_at.isoformat()),
        ("liczba_nauczycieli", teachers),
        ("liczba_kursow", courses),
        ("liczba_rekordow", total),
    ]

    ws.write(0, 0, "pole", bold)
    ws.write(0, 1, "wartosc", bold)
    for i, (k, v) in enumerate(rows, start=1):
        ws.write(i, 0, k)
        ws.write(i, 1, v)

    logger.info("INFO: teachers=%s courses=%s records=%s", teachers, courses, total)


def _export_skip_strategy_sql(con, activity_col: str) -> str:
    """
    "skip" is ambiguous in prompt. We implement safe minimal output:
    aggregate per teacher + activity only, without course. Percents set NULL.
    """
    table = "mart.metrics_long"
    if activity_col == "activity_label" and not _duckdb_has_column(con, table, "activity_label"):
        activity_col = "tech_key"

    where_clause = "WHERE visible_active" if _duckdb_has_column(con, table, "visible_active") else ""
    sql = f"""
        SELECT
            full_name::VARCHAR AS full_name,
            teacher_id::VARCHAR AS teacher_id,
            NULL::VARCHAR AS course_value,
            {activity_col}::VARCHAR AS activity_value,
            SUM(count_value)::BIGINT AS count_value,
            NULL::DOUBLE AS pct_course,
            NULL::DOUBLE AS pct_program,
            NULL::DOUBLE AS pct_faculty,
            NULL::DOUBLE AS pct_university,
            {activity_col}::VARCHAR AS _order_tech
        FROM {table}
        {where_clause}
        GROUP BY 1,2,3,4,10
        ORDER BY full_name ASC, _order_tech ASC
    """
    return sql


def export_summary_excel(con, cfg: Dict[str, Any]) -> Tuple[int, Path]:
    """
    Main entry point. Returns (exit_code, output_path).
    Requires:
      cfg["paths"]["root"] or cfg["root"] (depending on your config shape)
      cfg["report"]["ay"], cfg["report"]["term"] (or equivalents)
      cfg["export"]["max_rows_excel"], cfg["export"]["overflow_strategy"] (optional)
    """
    # --- Resolve config fields (keep tolerant to shape) ---
    root = Path(cfg.get("root") or cfg.get("paths", {}).get("root") or cfg.get("paths", {}).get("output_root", "."))
    ay = str(cfg.get("report", {}).get("ay") or cfg.get("ay") or "")
    term = str(cfg.get("report", {}).get("term") or cfg.get("term") or "")

    export_cfg = ExportExcelConfig(
        max_rows_excel=int(cfg.get("export", {}).get("max_rows_excel", 1_000_000)),
        overflow_strategy=str(cfg.get("export", {}).get("overflow_strategy", "error")),
        activity_column=str(cfg.get("export", {}).get("activity_column", "activity_label")),
        course_column=str(cfg.get("export", {}).get("course_column", "course_name")),
        include_hr_cols=bool(cfg.get("export", {}).get("include_hr_cols", True)),
        exclude_zero_counts=bool(cfg.get("export", {}).get("exclude_zero_counts", True)),
        percent_excel_format=bool(cfg.get("export", {}).get("percent_excel_format", True)),
     )

    run_dir, out_dir = _ensure_dirs(root)
    logger = _setup_logger(run_dir)

    _progress_append(run_dir, {"step": "export-excel", "status": "start", "ay": ay, "term": term})

    out_path = out_dir / f"Raport_Zbiorczy_NA_{ay}_{term}.xlsx"
    ok_flag = run_dir / "export-excel.ok"

    logger.info("Exporting XLSX to: %s", out_path)
    logger.info("Export config: %s", export_cfg)

    # Build base SQL (ordered) for main sheet
    base_sql = _select_metrics_long_sql(
    con,
    export_cfg.activity_column,
    export_cfg.course_column,
    export_cfg.include_hr_cols,
    export_cfg.exclude_zero_counts,
    export_cfg.percent_excel_format,
    )

    # Pre-count rows to enforce overflow strategy deterministically
    total_rows = _count_rows(con, base_sql)
    logger.info("metrics_long rows to export: %s", total_rows)

    # Decide strategy
    strategy = export_cfg.overflow_strategy.lower().strip()
    max_rows = export_cfg.max_rows_excel

    if total_rows > max_rows and strategy == "error":
        _progress_append(
            run_dir,
            {
                "step": "export-excel",
                "status": "error",
                "reason": "overflow",
                "rows": total_rows,
                "max_rows_excel": max_rows,
                "strategy": strategy,
            },
        )
        raise ExportOverflowError(f"Too many rows for Excel: {total_rows} > {max_rows}")

    if total_rows > max_rows and strategy == "skip":
        logger.warning("Overflow with strategy=skip; exporting aggregated per teacher (course=NULL).")
        base_sql = _export_skip_strategy_sql(con, export_cfg.activity_column)
        total_rows = _count_rows(con, base_sql)

    # Create workbook (overwrite, idempotent)
    generated_at = datetime.now(timezone.utc)

    workbook = xlsxwriter.Workbook(
        out_path.as_posix(),
        {
            "constant_memory": True,  # good for large datasets
            "strings_to_numbers": False,
            "strings_to_formulas": False,
            "strings_to_urls": False,
        },
    )

    try:
        main_sheet_base = "ZLICZENIE_AKTYWNOSCI_NA"

        # Main data
        _progress_append(run_dir, {"step": "export-excel", "status": "writing_main", "rows": total_rows})
        written_rows, sheets_count = _write_metrics_long_split_streaming(
            workbook=workbook,
            con=con,
            sql=base_sql,
            max_rows=max_rows,
            overflow_strategy=("split" if strategy == "split" else "error"),
            main_sheet_base=main_sheet_base,
            logger=logger,
        )

        # QA
        _progress_append(run_dir, {"step": "export-excel", "status": "writing_qa"})
        qa_rows = _write_qa_sheet(workbook, con, logger)

        # INFO
        _progress_append(run_dir, {"step": "export-excel", "status": "writing_info"})
        _write_info_sheet(workbook, con, base_sql, ay, term, generated_at, logger)

    finally:
        workbook.close()

    ok_flag.write_text("OK\n", encoding="utf-8")
    _progress_append(
        run_dir,
        {
            "step": "export-excel",
            "status": "done",
            "output": str(out_path),
            "main_rows": written_rows,
            "qa_rows": qa_rows,
            "main_sheets": sheets_count,
        },
    )

    logger.info("DONE: %s", out_path)
    return EXIT_OK, out_path
from __future__ import annotations

from pathlib import Path
from typing import Optional, Callable, Any

import typer

from mrna_plum.paths import ProjectPaths
from mrna_plum.config import load_config
from mrna_plum.logging_run import setup_file_logger
from mrna_plum.ui_bridge import ProgressWriter
from mrna_plum.errors import ConfigError, InputDataError, MixedPeriodsError, ProcessingError

# NEW: autodetekcja INPUTS_DIR
from .inputs.autodetect import find_inputs, InputValidationError

app = typer.Typer(add_completion=False)

# Exit codes (wg ustaleń)
EC_OK = 0
EC_CONFIG = 2
EC_INPUT = 10
EC_MIXED = 20
EC_PROC = 30
EC_INTERNAL = 40


# -------------------------
# Helpers
# -------------------------
def _resolve_root(root: str) -> Path:
    return Path(root).resolve()


def _resolve_config(root: Path, config: str | None) -> Path:
    return Path(config).resolve() if config else (root / "config.yaml").resolve()


def _ensure_dirs(paths: ProjectPaths) -> None:
    paths.run_dir.mkdir(parents=True, exist_ok=True)
    paths.data_dir.mkdir(parents=True, exist_ok=True)
    paths.parquet_dir.mkdir(parents=True, exist_ok=True)
    # opcjonalnie: out dir jeśli masz w ProjectPaths
    try:
        paths.out_dir.mkdir(parents=True, exist_ok=True)  # type: ignore[attr-defined]
    except Exception:
        (paths.root / "_out").mkdir(parents=True, exist_ok=True)


def _write_marker(paths: ProjectPaths, step: str) -> None:
    # marker: {step}.ok w _run
    paths.marker_path(step).write_text("ok", encoding="utf-8")


def _collect_input_files(root: Path, input_glob: str) -> list[Path]:
    return sorted([p for p in root.glob(input_glob) if p.is_file()])


def _main_guard(fn: Callable[..., Any]) -> Callable[..., Any]:
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except ConfigError as e:
            typer.echo(str(e), err=True)
            raise typer.Exit(code=EC_CONFIG)
        except InputDataError as e:
            typer.echo(str(e), err=True)
            raise typer.Exit(code=EC_INPUT)
        except MixedPeriodsError as e:
            typer.echo(str(e), err=True)
            raise typer.Exit(code=EC_MIXED)
        except ProcessingError as e:
            typer.echo(str(e), err=True)
            raise typer.Exit(code=EC_PROC)
        except typer.Exit:
            raise
        except Exception as e:
            typer.echo(f"Internal error: {e}", err=True)
            raise typer.Exit(code=EC_INTERNAL)

    return wrapper


# -------- NEW: INPUTS_DIR helpers --------
def _resolve_inputs_dir(root: Path, cfg: Any, inputs_dir_opt: str | None) -> Path | None:
    """
    Priorytet:
    1) CLI --inputs-dir
    2) config.yaml: inputs.inputs_dir (jeśli istnieje)
    """
    if inputs_dir_opt:
        return Path(inputs_dir_opt).expanduser().resolve()

    v = None
    try:
        if isinstance(cfg, dict):
            v = (cfg.get("inputs") or {}).get("inputs_dir")
        else:
            # jeśli kiedyś inputs będzie atrybutem
            v = getattr(getattr(cfg, "inputs", None), "inputs_dir", None)
            # Twoja konfiguracja używa często cfg._data jako słownik
            if v is None and hasattr(cfg, "_data"):
                inputs_sec = cfg._data.get("inputs") or {}  # type: ignore[attr-defined]
                if isinstance(inputs_sec, dict):
                    v = inputs_sec.get("inputs_dir")
    except Exception:
        v = None

    if not v:
        return None

    p = Path(str(v)).expanduser()
    if not p.is_absolute():
        p = (root / p).resolve()
    else:
        p = p.resolve()
    return p


def _emit_inputs_detected(progress: ProgressWriter, inputs: Any) -> None:
    progress.emit(
        "inputs",
        "detected",
        "Inputs autodetected",
        extra={
            "inputs_dir": str(inputs.inputs_dir),
            "teachers_csv": str(inputs.teachers_csv) if getattr(inputs, "teachers_csv", None) else None,
            "roster_csv": str(inputs.roster_csv) if getattr(inputs, "roster_csv", None) else None,
            "snapshot_csv": str(inputs.snapshot_csv) if getattr(inputs, "snapshot_csv", None) else None,
            # opcjonalne mapowania (jeśli find_inputs je zwraca)
            "teachers_map": getattr(inputs, "teachers_map", None),
            "roster_map": getattr(inputs, "roster_map", None),
            "snapshot_map": getattr(inputs, "snapshot_map", None),
        },
    )


def _inject_inputs_into_cfg(cfg: Any, teachers_csv: str | None, roster_csv: str | None, snapshot_csv: str | None = None) -> None:
    """
    Wstrzykuje wykryte pliki do cfg:
      cfg.inputs.teachers_csv / cfg.inputs.roster_csv / cfg.inputs.snapshot_csv
    Obsługuje cfg jako dict lub obiekt z _data.
    """
    if isinstance(cfg, dict):
        cfg.setdefault("inputs", {})
        if not isinstance(cfg["inputs"], dict):
            cfg["inputs"] = {}
        cfg["inputs"]["teachers_csv"] = teachers_csv
        cfg["inputs"]["roster_csv"] = roster_csv
        cfg["inputs"]["snapshot_csv"] = snapshot_csv
        return

    if hasattr(cfg, "_data"):
        cfg._data.setdefault("inputs", {})  # type: ignore[attr-defined]
        if not isinstance(cfg._data["inputs"], dict):  # type: ignore[attr-defined]
            cfg._data["inputs"] = {}  # type: ignore[attr-defined]
        cfg._data["inputs"]["teachers_csv"] = teachers_csv  # type: ignore[attr-defined]
        cfg._data["inputs"]["roster_csv"] = roster_csv  # type: ignore[attr-defined]
        cfg._data["inputs"]["snapshot_csv"] = snapshot_csv  # type: ignore[attr-defined]
        return

    # fallback: dynamic attrs (ostatnia deska ratunku)
    setattr(cfg, "inputs_teachers_csv", teachers_csv)
    setattr(cfg, "inputs_roster_csv", roster_csv)
    setattr(cfg, "inputs_snapshot_csv", snapshot_csv)


# -------------------------
# Commands
# -------------------------

@app.command("init")
@_main_guard
def cmd_init(
    root: str = typer.Option(..., "--root", help="Root folder projektu (np. ThisWorkbook.Path)"),
):
    """
    Tworzy podstawową strukturę folderów projektu.
    """
    root_p = _resolve_root(root)

    # lazy import - bez ryzyka cykli
    from .init_project import init_project

    created = init_project(root_p)
    typer.echo(f"Created {len(created)} folders")
    raise typer.Exit(code=EC_OK)


@app.command("merge-logs")
@_main_guard
def cmd_merge_logs(
    root: str = typer.Option(..., "--root", help="Root folder passed from VBA (ThisWorkbook.Path)"),
    config: str | None = typer.Option(None, "--config", help="Config path; default {root}/config.yaml"),
    mode: str = typer.Option(
        "duckdb",
        "--mode",
        help="duckdb (pipeline B) | parquet (pipeline A → merged_raw.parquet)",
        case_sensitive=False,
    ),
):
    """
    Merge logów CSV z Moodle/PLUM.

    mode=parquet:
        CSV → merged_raw.parquet (pipeline A)

    mode=duckdb:
        CSV → DuckDB raw (pipeline B / staging)
    """
    root_p = _resolve_root(root)
    cfg_p = _resolve_config(root_p, config)
    paths = ProjectPaths(root=root_p)
    _ensure_dirs(paths)

    logger = setup_file_logger(paths.run_dir / "run.log")
    progress = ProgressWriter(paths.run_dir / "progress.jsonl")

    cfg = load_config(cfg_p)

    mode = mode.lower().strip()
    if mode not in ("parquet", "duckdb"):
        raise typer.BadParameter("--mode must be one of: duckdb, parquet")

    input_files = _collect_input_files(root_p, cfg.input_glob)
    if not input_files:
        raise InputDataError(f"No input files found under {root_p} with glob {cfg.input_glob}")

    progress.emit(
        "merge",
        "start",
        f"Starting merge ({mode})",
        current=0,
        total=len(input_files),
        extra={"root": str(root_p), "mode": mode},
    )
    logger.info("[merge] start mode=%s files=%s", mode, len(input_files))

    if mode == "parquet":
        # pipeline A
        from .merge import merge_logs_to_parquet

        merged_parquet = paths.parquet_dir / "merged_raw.parquet"
        total_rows = merge_logs_to_parquet(input_files, merged_parquet, dedup_per_file=True)

        progress.emit(
            "merge",
            "done",
            "Merge finished",
            current=len(input_files),
            total=len(input_files),
            extra={"rows": total_rows, "parquet": str(merged_parquet)},
        )
        _write_marker(paths, "merge")
        logger.info("[merge] done rows=%s parquet=%s", total_rows, merged_parquet)
        raise typer.Exit(code=EC_OK)

    # mode == duckdb → pipeline B
    from .store.duckdb_store import open_store
    from .merge.merge_logs import merge_logs_into_duckdb

    db_path = paths.duckdb_path
    con = open_store(db_path)
    try:
        res = merge_logs_into_duckdb(
            root=root_p,
            con=con,
            export_mode="duckdb",
            export_dir=None,
            chunk_size=int(getattr(cfg, "chunk_rows", 2000)),  # fallback
        )
    finally:
        con.close()

    progress.emit(
        "merge",
        "done",
        "Merge finished (duckdb)",
        current=len(input_files),
        total=len(input_files),
        extra={
            "db": str(db_path),
            "courses": getattr(res, "courses", None),
            "inserted_rows": getattr(res, "inserted_rows", None),
        },
    )
    _write_marker(paths, "merge")
    logger.info("[merge] done duckdb db=%s res=%s", db_path, res)
    raise typer.Exit(code=EC_OK)


@app.command("build-db")
@_main_guard
def cmd_build_db(
    root: str = typer.Option(..., "--root"),
    config: str | None = typer.Option(None, "--config"),
):
    """
    Pipeline A: merged_raw.parquet → parsed.parquet → DuckDB(raw_logs)
    """
    root_p = _resolve_root(root)
    cfg_p = _resolve_config(root_p, config)
    paths = ProjectPaths(root=root_p)
    _ensure_dirs(paths)

    logger = setup_file_logger(paths.run_dir / "run.log")
    progress = ProgressWriter(paths.run_dir / "progress.jsonl")

    cfg = load_config(cfg_p)

    # KEYS → rules
    from .io.excel_keys import load_keys_sheet
    from .rules.engine import compile_rules

    keys_wb = (root_p / cfg.keys_workbook).resolve()
    keys_df = load_keys_sheet(keys_wb, cfg.keys_sheet)
    rules = compile_rules(keys_df)

    merged_parquet = paths.parquet_dir / "merged_raw.parquet"
    if not merged_parquet.exists():
        raise InputDataError(
            f"Missing merged parquet. Run: mrna_plum merge-logs --mode parquet --root ...  Expected: {merged_parquet}"
        )

    progress.emit("parse", "start", "Parsing merged logs & applying rules")
    logger.info("[parse] start rules=%s", len(rules))

    from .parse import parse_merged_parquet

    parsed_parquet = paths.parquet_dir / "parsed.parquet"
    n_rows, run_period = parse_merged_parquet(merged_parquet, parsed_parquet, cfg, rules)

    # DuckDB load
    from .store import DuckDbStore

    store = DuckDbStore(paths.duckdb_path)
    store.init_schema()

    progress.emit("db", "start", "Loading parsed parquet into DuckDB", extra={"db": str(paths.duckdb_path)})
    store.load_parquet_to_raw(parsed_parquet)

    progress.emit("db", "done", "DB built", extra={"rows": n_rows, "period": run_period})
    _write_marker(paths, "build_db")
    logger.info("[db] done rows=%s period=%s db=%s", n_rows, run_period, paths.duckdb_path)
    raise typer.Exit(code=EC_OK)


@app.command("parse-events")
@_main_guard
def cmd_parse_events(
    root: str = typer.Option(..., "--root", help="Root projektu"),
    config: str | None = typer.Option(None, "--config", help="config.yaml; default {root}/config.yaml"),
    keys_xlsx: str | None = typer.Option(None, "--keys-xlsx", help="Override ścieżki KEYS.xlsx/KEYS workbook"),
):
    """
    Pipeline B: raw (DuckDB) → events_canonical (DuckDB)
    """
    root_p = _resolve_root(root)
    cfg_p = _resolve_config(root_p, config)
    paths = ProjectPaths(root=root_p)
    _ensure_dirs(paths)

    logger = setup_file_logger(paths.run_dir / "run.log")
    progress = ProgressWriter(paths.run_dir / "progress.jsonl")

    cfg = load_config(cfg_p)

    progress.emit(
        "parse_events",
        "start",
        "Parsing events into events_canonical",
        extra={"db": str(paths.duckdb_path), "keys_xlsx": keys_xlsx},
    )
    logger.info("[parse_events] start db=%s", paths.duckdb_path)

    from .parse.parse_events import run_parse_events

    exit_code = run_parse_events(cfg, root=str(root_p), keys_xlsx_override=keys_xlsx)
    if exit_code != 0:
        raise ProcessingError(f"parse-events failed with exit code {exit_code}")

    progress.emit("parse_events", "done", "Events parsed")
    _write_marker(paths, "parse_events")
    logger.info("[parse_events] done")
    raise typer.Exit(code=EC_OK)


@app.command("build-activities-state")
@_main_guard
def cmd_build_activities_state(
    root: str = typer.Option(..., "--root", help="Root projektu"),
    config: str | None = typer.Option(None, "--config", help="config.yaml; default {root}/config.yaml"),
    # NEW: INPUTS_DIR
    inputs_dir: str | None = typer.Option(
        None,
        "--inputs-dir",
        help="Folder INPUTS_DIR (autodetekcja plików HR/roster/snapshot)",
    ),
    # CHANGED: snapshot-file optional (override)
    snapshot_file: str | None = typer.Option(
        None,
        "--snapshot-file",
        help="Override: CSV '*_zawartosc_kursow.csv' (jeśli nie używasz inputs-dir)",
    ),
):
    """
    Pipeline B: snapshot CSV → raw.activities_snapshot → mart.activities_state
    """
    root_p = _resolve_root(root)
    cfg_p = _resolve_config(root_p, config)
    paths = ProjectPaths(root=root_p)
    _ensure_dirs(paths)

    logger = setup_file_logger(paths.run_dir / "run.log")
    progress = ProgressWriter(paths.run_dir / "progress.jsonl")

    cfg = load_config(cfg_p)

    # --- resolve snapshot path (override -> autodetect) ---
    snap_path: Path | None = None

    if snapshot_file:
        snap_path = Path(snapshot_file).expanduser().resolve()
    else:
        in_dir = _resolve_inputs_dir(root_p, cfg, inputs_dir)
        if in_dir:
            try:
                inputs = find_inputs(in_dir)
            except InputValidationError as e:
                progress.emit("inputs", "error", "Inputs autodetect failed", extra={"error": str(e)})
                raise InputDataError(str(e))

            _emit_inputs_detected(progress, inputs)

            # snapshot krytyczny dla tego kroku
            snap_path = inputs.snapshot_csv

            # HR/roster opcjonalne: loguj warningi
            if not inputs.teachers_csv:
                progress.emit("inputs", "warning", "HR file missing (dane_do_raportu.csv) - HR fields will be '-'")
            if not inputs.roster_csv:
                progress.emit("inputs", "warning", "Roster missing (*_raport_uczestnikow.csv) - students_enrolled will be '-'")

            # wstrzyknij do cfg (żeby dalsze kroki mogły korzystać)
            _inject_inputs_into_cfg(
                cfg,
                teachers_csv=str(inputs.teachers_csv) if inputs.teachers_csv else None,
                roster_csv=str(inputs.roster_csv) if inputs.roster_csv else None,
                snapshot_csv=str(inputs.snapshot_csv) if inputs.snapshot_csv else None,
            )

    if not snap_path or not snap_path.exists():
        msg = "Missing snapshot CSV. Provide --snapshot-file or --inputs-dir with a file '*_zawartosc_kursow.csv'."
        progress.emit("inputs", "error", msg, extra={"snapshot_file": str(snap_path) if snap_path else None})
        raise InputDataError(msg)

    from .store import DuckDbStore
    from .activities.activities_state import (
        build_activities_state,
        BuildConfig,
        DeletionConfig,
        MappingConfig,
        IncrementalConfig,
    )

    store = DuckDbStore(paths.duckdb_path)
    store.init_schema()

    progress.emit(
        "activities_state",
        "start",
        "Loading snapshots & building activities_state",
        extra={"snapshot_file": str(snap_path), "db": str(paths.duckdb_path)},
    )
    logger.info("[activities_state] start snapshot=%s", snap_path)

    from .activities.snapshots_load import load_plum_snapshot_file_into_duckdb

    # KONFIG: minimalny default
    build_cfg = BuildConfig(
        deletion=DeletionConfig(
            delete_operations=["DELETE"],
            delete_tech_keys=[],
            delete_activity_labels_regex=[],
            disappearance_grace_period_days=14,
            min_missing_snapshots_to_confirm=2,
            deleted_at_policy="first_missing",
        ),
        mapping=MappingConfig(
            use_activity_id_map_table=True,
            allow_fuzzy_name_type_match=False,
        ),
        incremental=IncrementalConfig(
            checkpoint_table="raw.pipeline_checkpoints",
            checkpoint_key="build_activities_state",
            process_only_new_snapshots=True,
            process_only_new_events=True,
        ),
    )

    with store.connect() as con:
        load_stats = load_plum_snapshot_file_into_duckdb(con, snap_path)
        progress.emit("activities_state", "snapshots_loaded", "Snapshots loaded", extra=load_stats)

        stats = build_activities_state(con, build_cfg)

    progress.emit("activities_state", "done", "Activities state built", extra=stats)
    _write_marker(paths, "activities_state")
    logger.info("[activities_state] done %s", stats)
    raise typer.Exit(code=EC_OK)


@app.command("compute-stats")
@_main_guard
def cmd_compute_stats(
    root: str = typer.Option(..., "--root"),
    ay: str | None = typer.Option(None, "--ay"),
    term: str | None = typer.Option(None, "--term"),
    config: str | None = typer.Option(None, "--config"),
):
    """
    Ujednolicone compute-stats: wywołuje stats.compute_stats(root, ay, term).
    """
    root_p = _resolve_root(root)
    _ = _resolve_config(root_p, config)  # na przyszłość
    paths = ProjectPaths(root=root_p)
    _ensure_dirs(paths)

    logger = setup_file_logger(paths.run_dir / "run.log")
    progress = ProgressWriter(paths.run_dir / "progress.jsonl")

    progress.emit("stats", "start", "Computing stats", extra={"ay": ay, "term": term})
    logger.info("[stats] start ay=%s term=%s", ay, term)

    from .stats.compute_stats import compute_stats

    compute_stats(root=root_p, ay=ay, term=term)

    progress.emit("stats", "done", "Stats computed", extra={"ay": ay, "term": term})
    _write_marker(paths, "stats")
    logger.info("[stats] done")
    raise typer.Exit(code=EC_OK)


@app.command("export-excel")
@_main_guard
def cmd_export_excel(
    root: str = typer.Option(..., "--root"),
    db_path: str | None = typer.Option(None, "--db-path", help="Override ścieżki do DuckDB; default z ProjectPaths"),
    config: str | None = typer.Option(None, "--config"),
    # NEW: INPUTS_DIR (opcjonalne, ale przydatne do metryczki/roster w raportach)
    inputs_dir: str | None = typer.Option(None, "--inputs-dir", help="Folder INPUTS_DIR (autodetekcja HR/roster)"),
):
    """
    Eksport agregatów do Excela (docelowo: export_summary_excel).
    """
    root_p = _resolve_root(root)
    cfg_p = _resolve_config(root_p, config)
    paths = ProjectPaths(root=root_p)
    _ensure_dirs(paths)

    logger = setup_file_logger(paths.run_dir / "run.log")
    progress = ProgressWriter(paths.run_dir / "progress.jsonl")

    cfg = load_config(cfg_p)

    # NEW: autodetekcja HR/roster (opcjonalne)
    in_dir = _resolve_inputs_dir(root_p, cfg, inputs_dir)
    if in_dir:
        try:
            inputs = find_inputs(in_dir)
        except InputValidationError as e:
            progress.emit("inputs", "error", "Inputs autodetect failed", extra={"error": str(e)})
            raise InputDataError(str(e))

        _emit_inputs_detected(progress, inputs)

        if not inputs.teachers_csv:
            progress.emit("inputs", "warning", "HR file missing (dane_do_raportu.csv) - HR fields will be '-'")
        if not inputs.roster_csv:
            progress.emit("inputs", "warning", "Roster missing (*_raport_uczestnikow.csv) - students_enrolled will be '-'")

        _inject_inputs_into_cfg(
            cfg,
            teachers_csv=str(inputs.teachers_csv) if inputs.teachers_csv else None,
            roster_csv=str(inputs.roster_csv) if inputs.roster_csv else None,
            snapshot_csv=str(inputs.snapshot_csv) if inputs.snapshot_csv else None,
        )

    db = Path(db_path).resolve() if db_path else paths.duckdb_path

    progress.emit("export_excel", "start", "Exporting Excel report", extra={"db": str(db)})
    logger.info("[export_excel] start db=%s", db)

    import duckdb
    from .reports.export_excel import export_summary_excel, ExportOverflowError, EXIT_OVERFLOW

    con = duckdb.connect(str(db))
    try:
        code, out_path = export_summary_excel(con, cfg)  # cfg w Twoim repo jest dict-like
    except ExportOverflowError:
        raise typer.Exit(code=EXIT_OVERFLOW)
    finally:
        con.close()

    progress.emit("export_excel", "done", "Excel exported", extra={"out": str(out_path)})
    _write_marker(paths, "export_excel")
    logger.info("[export_excel] done out=%s", out_path)
    raise typer.Exit(code=code)


@app.command("export-individual")
@_main_guard
def cmd_export_individual(
    root: str = typer.Option(..., "--root"),
    config: str | None = typer.Option(None, "--config"),
    out_dir: str | None = typer.Option(None, "--out-dir", help="Override folderu wyjściowego na XLSX"),
    # NEW: INPUTS_DIR
    inputs_dir: str | None = typer.Option(None, "--inputs-dir", help="Folder INPUTS_DIR (autodetekcja HR/roster/snapshot)"),
):
    """
    Eksport paczek indywidualnych.
    """
    root_p = _resolve_root(root)
    cfg_p = _resolve_config(root_p, config)
    paths = ProjectPaths(root=root_p)
    _ensure_dirs(paths)

    # Ustal out_dir_p i upewnij się że istnieje
    if out_dir:
        out_dir_p = Path(out_dir).expanduser().resolve()
    else:
        out_dir_p = paths.out_dir / "indywidualne"
    out_dir_p.mkdir(parents=True, exist_ok=True)

    logger = setup_file_logger(paths.run_dir / "run.log")
    progress = ProgressWriter(paths.run_dir / "progress.jsonl")

    cfg = load_config(cfg_p)

    # NEW: autodetekcja HR/roster (opcjonalne)
    in_dir = _resolve_inputs_dir(root_p, cfg, inputs_dir)
    if in_dir:
        try:
            inputs = find_inputs(in_dir)
        except InputValidationError as e:
            progress.emit("inputs", "error", "Inputs autodetect failed", extra={"error": str(e)})
            raise InputDataError(str(e))

        _emit_inputs_detected(progress, inputs)

        if not inputs.teachers_csv:
            progress.emit("inputs", "warning", "HR file missing (dane_do_raportu.csv) - HR fields will be '-'")
        if not inputs.roster_csv:
            progress.emit("inputs", "warning", "Roster missing (*_raport_uczestnikow.csv) - students_enrolled will be '-'")

        _inject_inputs_into_cfg(
            cfg,
            teachers_csv=str(inputs.teachers_csv) if inputs.teachers_csv else None,
            roster_csv=str(inputs.roster_csv) if inputs.roster_csv else None,
            snapshot_csv=str(inputs.snapshot_csv) if inputs.snapshot_csv else None,
        )

    # Wstrzyknij out_dir do cfg (tak jak było)
    if isinstance(cfg, dict):
        cfg.setdefault("reports", {})
        cfg["reports"]["individual_dir"] = str(out_dir_p)
    else:
        cfg._data.setdefault("reports", {})  # type: ignore[attr-defined]
        if not isinstance(cfg._data["reports"], dict):  # type: ignore[attr-defined]
            cfg._data["reports"] = {}  # type: ignore[attr-defined]
        cfg._data["reports"]["individual_dir"] = str(out_dir_p)  # type: ignore[attr-defined]

    progress.emit("export_individual", "start", "Exporting individual reports", extra={"out_dir": str(out_dir_p)})
    logger.info("[export_individual] start out_dir=%s", out_dir_p)

    import duckdb as _duckdb
    db = paths.duckdb_path
    con = _duckdb.connect(str(db))
    try:
        from .reports.export_individual import export_individual_reports
        code, result_dir = export_individual_reports(con, cfg)
    finally:
        con.close()

    progress.emit("export_individual", "done", "Individual reports exported", extra={"out_dir": str(result_dir)})
    _write_marker(paths, "export_individual")
    logger.info("[export_individual] done out_dir=%s", result_dir)
    raise typer.Exit(code=code)
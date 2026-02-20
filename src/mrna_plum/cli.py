from __future__ import annotations
import sys
from pathlib import Path
import typer

from .paths import ProjectPaths
from .config import load_config
from .logging_run import setup_file_logger
from .ui_bridge import ProgressWriter
from .errors import ConfigError, InputDataError, MixedPeriodsError, ProcessingError

from .io.excel_keys import load_keys_sheet
from .rules.engine import compile_rules
from .merge import merge_logs_to_parquet
from .parse import parse_merged_parquet
from .store import DuckDbStore
from .stats import compute_stats
from .reports import export_excel_aggregates, export_individual_packages
from mrna_plum.store.duckdb_store import open_store
from mrna_plum.merge.merge_logs import merge_logs_into_duckdb
from mrna_plum.parse.parse_events import run_parse_events

from .activities.snapshots_load import load_snapshots_into_duckdb  # albo loader PL
from .activities.activities_state import build_activities_state, BuildConfig, DeletionConfig, MappingConfig, IncrementalConfig

app = typer.Typer(add_completion=False)

# Exit codes
EC_OK = 0
EC_CONFIG = 2
EC_INPUT = 10
EC_MIXED = 20
EC_PROC = 30
EC_INTERNAL = 40

def _resolve_root(root: str) -> Path:
    p = Path(root).resolve()
    return p

def _resolve_config(root: Path, config: str | None) -> Path:
    if config:
        return Path(config).resolve()
    return (root / "config.yaml").resolve()

def _ensure_dirs(paths: ProjectPaths) -> None:
    paths.run_dir.mkdir(parents=True, exist_ok=True)
    paths.data_dir.mkdir(parents=True, exist_ok=True)
    paths.parquet_dir.mkdir(parents=True, exist_ok=True)

def _write_marker(paths: ProjectPaths, step: str) -> None:
    paths.marker_path(step).write_text("ok", encoding="utf-8")

def _collect_input_files(root: Path, input_glob: str) -> list[Path]:
    files = sorted([p for p in root.glob(input_glob) if p.is_file()])
    return files

def _main_guard(fn):
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
        except Exception as e:
            typer.echo(f"Internal error: {e}", err=True)
            raise typer.Exit(code=EC_INTERNAL)
    return wrapper

@app.command("merge-logs")
@_main_guard
def cmd_merge_logs(
    root: str = typer.Option(..., "--root", help="Root folder passed from VBA (ThisWorkbook.Path)"),
    config: str | None = typer.Option(None, "--config", help="Config path; default {root}/config.yaml"),
):
    root_p = _resolve_root(root)
    cfg_p = _resolve_config(root_p, config)
    paths = ProjectPaths(root=root_p)
    _ensure_dirs(paths)

    logger = setup_file_logger(paths.run_dir / "run.log")
    progress = ProgressWriter(paths.run_dir / "progress.jsonl")

    cfg = load_config(cfg_p)

    input_files = _collect_input_files(root_p, cfg.input_glob)
    if not input_files:
        raise InputDataError(f"No input files found under {root_p} with glob {cfg.input_glob}")

    progress.emit("merge", "start", "Starting merge", current=0, total=len(input_files), extra={"root": str(root_p)})
    logger.info("[merge] files=%s", len(input_files))

    merged_parquet = paths.parquet_dir / "merged_raw.parquet"
    total_rows = merge_logs_to_parquet(input_files, merged_parquet, dedup_per_file=True)

    progress.emit("merge", "done", "Merge finished", current=len(input_files), total=len(input_files), extra={"rows": total_rows, "parquet": str(merged_parquet)})
    _write_marker(paths, "merge")
    logger.info("[merge] done rows=%s parquet=%s", total_rows, merged_parquet)


@app.command("build-db")
@_main_guard
def cmd_build_db(
    root: str = typer.Option(..., "--root"),
    config: str | None = typer.Option(None, "--config"),
):
    root_p = _resolve_root(root)
    cfg_p = _resolve_config(root_p, config)
    paths = ProjectPaths(root=root_p)
    _ensure_dirs(paths)

    logger = setup_file_logger(paths.run_dir / "run.log")
    progress = ProgressWriter(paths.run_dir / "progress.jsonl")

    cfg = load_config(cfg_p)

    # KEYS
    keys_wb = (root_p / cfg.keys_workbook).resolve()
    keys_df = load_keys_sheet(keys_wb, cfg.keys_sheet)
    rules = compile_rules(keys_df)

    merged_parquet = paths.parquet_dir / "merged_raw.parquet"
    if not merged_parquet.exists():
        raise InputDataError(f"Missing merged parquet. Run: mrna_plum merge-logs --root ...  Expected: {merged_parquet}")

    progress.emit("parse", "start", "Parsing merged logs & applying rules")
    logger.info("[parse] start rules=%s", len(rules))

    parsed_parquet = paths.parquet_dir / "parsed.parquet"
    n_rows, run_period = parse_merged_parquet(merged_parquet, parsed_parquet, cfg, rules)

    # DuckDB
    store = DuckDbStore(paths.duckdb_path)
    store.init_schema()

    progress.emit("db", "start", "Loading parsed parquet into DuckDB", extra={"db": str(paths.duckdb_path)})
    store.load_parquet_to_raw(parsed_parquet)

    progress.emit("db", "done", "DB built", extra={"rows": n_rows, "period": run_period})
    _write_marker(paths, "build_db")
    logger.info("[db] done rows=%s period=%s db=%s", n_rows, run_period, paths.duckdb_path)


@app.command("compute-stats")
@_main_guard
def cmd_compute_stats(
    root: str = typer.Option(..., "--root"),
    config: str | None = typer.Option(None, "--config"),
):
    root_p = _resolve_root(root)
    cfg_p = _resolve_config(root_p, config)
    paths = ProjectPaths(root=root_p)
    _ensure_dirs(paths)

    logger = setup_file_logger(paths.run_dir / "run.log")
    progress = ProgressWriter(paths.run_dir / "progress.jsonl")
    cfg = load_config(cfg_p)

    store = DuckDbStore(paths.duckdb_path)
    store.init_schema()

    # opcjonalnie: period z danych (weź pierwszy nie-null z raw_logs)
    with store.connect() as con:
        row = con.execute("SELECT period FROM raw_logs WHERE period IS NOT NULL LIMIT 1").fetchone()
        period = row[0] if row else None

    progress.emit("stats", "start", "Computing stats", extra={"period": period})
    compute_stats(store, period)
    progress.emit("stats", "done", "Stats computed", extra={"period": period})
    _write_marker(paths, "stats")
    logger.info("[stats] done period=%s", period)


@app.command("export-excel")
@_main_guard
def cmd_export_excel(
    root: str = typer.Option(..., "--root"),
    out: str | None = typer.Option(None, "--out", help="Output xlsx; default {root}/_out/aggregates.xlsx"),
    config: str | None = typer.Option(None, "--config"),
):
    root_p = _resolve_root(root)
    cfg_p = _resolve_config(root_p, config)
    paths = ProjectPaths(root=root_p)
    _ensure_dirs(paths)

    logger = setup_file_logger(paths.run_dir / "run.log")
    progress = ProgressWriter(paths.run_dir / "progress.jsonl")
    _ = load_config(cfg_p)

    store = DuckDbStore(paths.duckdb_path)

    out_xlsx = Path(out).resolve() if out else (root_p / "_out" / "aggregates.xlsx").resolve()
    progress.emit("export_excel", "start", "Exporting Excel aggregates", extra={"out": str(out_xlsx)})
    export_excel_aggregates(store, out_xlsx)
    progress.emit("export_excel", "done", "Excel exported", extra={"out": str(out_xlsx)})
    _write_marker(paths, "export_excel")
    logger.info("[export_excel] out=%s", out_xlsx)


@app.command("export-individual")
@_main_guard
def cmd_export_individual(
    root: str = typer.Option(..., "--root"),
    out_dir: str | None = typer.Option(None, "--out-dir", help="Output folder; default {root}/_out/individual"),
    config: str | None = typer.Option(None, "--config"),
):
    root_p = _resolve_root(root)
    cfg_p = _resolve_config(root_p, config)
    paths = ProjectPaths(root=root_p)
    _ensure_dirs(paths)

    logger = setup_file_logger(paths.run_dir / "run.log")
    progress = ProgressWriter(paths.run_dir / "progress.jsonl")
    _ = load_config(cfg_p)

    store = DuckDbStore(paths.duckdb_path)
    out_folder = Path(out_dir).resolve() if out_dir else (root_p / "_out" / "individual").resolve()

    progress.emit("export_individual", "start", "Exporting individual packages", extra={"out_dir": str(out_folder)})
    n = export_individual_packages(store, out_folder)
    progress.emit("export_individual", "done", "Individual packages exported", extra={"count": n, "out_dir": str(out_folder)})
    _write_marker(paths, "export_individual")
    logger.info("[export_individual] count=%s out_dir=%s", n, out_folder)

from mrna_plum.init_project import init_project

def cmd_init(args):
    created = init_project(args.root)
    print(f"Created {len(created)} folders")

@app.command("merge-logs")
def merge_logs_cmd(
    logs_root: Path = typer.Option(..., "--logs-root", exists=True, file_okay=False, dir_okay=True),
    db_path: Path = typer.Option(..., "--db-path", help="Ścieżka do DuckDB (np. E:/RNA/_db/mrna_plum.duckdb)"),
    export_mode: str = typer.Option(
        "duckdb",
        "--export-mode",
        help="duckdb (tylko zapis do DB) | parquet (eksport per-kurs) | csv (eksport per-kurs *_full_log.csv)",
    ),
    export_dir: Path | None = typer.Option(
        None,
        "--export-dir",
        help="Wymagane dla export-mode=csv/parquet. Folder wyjściowy per kurs.",
    ),
    chunk_size: int = typer.Option(2000, "--chunk-size", min=100, help="Batch insert do DuckDB"),
):
    export_mode = export_mode.lower().strip()
    if export_mode not in ("duckdb", "parquet", "csv"):
        raise typer.BadParameter("export-mode must be one of: duckdb, parquet, csv")

    if export_mode in ("csv", "parquet") and export_dir is None:
        raise typer.BadParameter("--export-dir is required for export-mode=csv/parquet")

    con = open_store(db_path)
    try:
        res = merge_logs_into_duckdb(
            root=logs_root,
            con=con,
            export_mode=export_mode,
            export_dir=export_dir,
            chunk_size=chunk_size,
        )
    finally:
        con.close()

    typer.echo(f"OK: courses={res.courses}, files={res.files}, inserted_rows={res.inserted_rows}")

@app.command("parse-events")
def parse_events_cmd(
    root: str,
    config: str,
    keys_xlsx: str = None,
):
    """
    Parse raw Moodle/PLUM CSV logs into canonical events table (DuckDB + optional Parquet).
    """
    from mrna_plum.config import load_config
    cfg = load_config(config)

    exit_code = run_parse_events(
        cfg,
        root=root,
        keys_xlsx_override=keys_xlsx,
    )
    raise SystemExit(exit_code)

@app.command("build-activities-state")
@_main_guard
def cmd_build_activities_state(
    root: str = typer.Option(..., "--root"),
    config: str | None = typer.Option(None, "--config"),
    snapshot_file: str = typer.Option(..., "--snapshot-file", help="CSV 'zawartość kursów' wybrany w VBA"),
):
    root_p = _resolve_root(root)
    cfg_p = _resolve_config(root_p, config)
    paths = ProjectPaths(root=root_p)
    _ensure_dirs(paths)

    logger = setup_file_logger(paths.run_dir / "run.log")
    progress = ProgressWriter(paths.run_dir / "progress.jsonl")

    cfg = load_config(cfg_p)

    snap_path = Path(snapshot_file).resolve()
    if not snap_path.exists():
        raise InputDataError(f"Snapshot file not found: {snap_path}")

    # DB
    store = DuckDbStore(paths.duckdb_path)
    store.init_schema()

    progress.emit("activities_state", "start", "Loading snapshots & building activities_state",
                  extra={"snapshot_file": str(snap_path), "db": str(paths.duckdb_path)})
    logger.info("[activities_state] start snapshot=%s", snap_path)

    with store.connect() as con:
        # 1) load snapshot do raw.activities_snapshot (idempotent)
        #    UWAGA: tu wywołaj loader dopasowany do formatu PL (Nazwa kursu/ID aktywności/...)
        load_stats = load_snapshots_into_duckdb(con, snap_path.parent, glob=snap_path.name)
        progress.emit("activities_state", "snapshots_loaded", "Snapshots loaded", extra=load_stats)

        # 2) build state (MERGE do mart.activities_state + QA + view)
        bcfg = cfg.build_activities_state  # zależy jak masz config model; jeśli to dict -> cfg["build_activities_state"]
        build_cfg = BuildConfig(
            deletion=DeletionConfig(
                delete_operations=["DELETE"],  # bo u Ciebie zawsze DELETE
                delete_tech_keys=[],
                delete_activity_labels_regex=[],
                disappearance_grace_period_days=int(bcfg.deletion.disappearance_grace_period_days),
                min_missing_snapshots_to_confirm=int(bcfg.deletion.min_missing_snapshots_to_confirm),
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

        stats = build_activities_state(con, build_cfg)

    progress.emit("activities_state", "done", "Activities state built", extra=stats)
    _write_marker(paths, "activities_state")
    logger.info("[activities_state] done %s", stats)
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
        raise InputDataError(f"Missing merged parquet. Run: rna_plum merge-logs --root ...  Expected: {merged_parquet}")

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

from rna_plum.init_project import init_project

def cmd_init(args):
    created = init_project(args.root)
    print(f"Created {len(created)} folders")

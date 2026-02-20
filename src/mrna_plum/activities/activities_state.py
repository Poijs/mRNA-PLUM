from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional
from typing import List

import duckdb


def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", errors="replace")).hexdigest()


def _parse_bool(val: str) -> Optional[bool]:
    if val is None:
        return None
    v = str(val).strip().lower()
    if v in ("1", "true", "t", "tak", "yes", "y"):
        return True
    if v in ("0", "false", "f", "nie", "no", "n"):
        return False
    return None


def _parse_ts(val: str) -> datetime:
    # Zakładamy ISO-8601 w snapshotach; jeśli nie – dopisz formaty w razie potrzeby.
    # Przykład: 2026-02-20T10:15:00Z / 2026-02-20 10:15:00
    v = val.strip()
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    return datetime.fromisoformat(v)


@dataclass(frozen=True)
class SnapshotRow:
    course_code: str
    activity_id: str
    name: str
    type: str
    visible_to_students: Optional[bool]
    captured_at: datetime
    source_file: str
    row_key: str


def iter_snapshot_csv(path: Path) -> Iterable[SnapshotRow]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        required = {"course_code", "activity_id", "name", "type", "visible_to_students", "captured_at"}
        missing = required - set((reader.fieldnames or []))
        if missing:
            raise ValueError(f"Snapshot CSV missing columns: {sorted(missing)} in {path}")

        for r in reader:
            course_code = (r.get("course_code") or "").strip()
            activity_id = (r.get("activity_id") or "").strip()
            if not course_code or not activity_id:
                continue

            name = (r.get("name") or "").strip()
            typ = (r.get("type") or "").strip()
            vis = _parse_bool(r.get("visible_to_students") or "")
            cap = _parse_ts(r.get("captured_at") or "")
            payload = json.dumps(
                {
                    "course_code": course_code,
                    "activity_id": activity_id,
                    "name": name,
                    "type": typ,
                    "visible_to_students": vis,
                    "captured_at": cap.isoformat(),
                },
                ensure_ascii=False,
                separators=(",", ":"),
            )
            row_key = _sha1(payload)

            yield SnapshotRow(
                course_code=course_code,
                activity_id=activity_id,
                name=name,
                type=typ,
                visible_to_students=vis,
                captured_at=cap,
                source_file=str(path),
                row_key=row_key,
            )


def load_snapshots_into_duckdb(
    con: duckdb.DuckDBPyConnection,
    snapshots_dir: Path,
    glob: str = "*.csv",
) -> dict:
    con.execute("create schema if not exists raw;")
    con.execute(
        """
        create table if not exists raw.activities_snapshot (
          course_code varchar not null,
          activity_id varchar not null,
          name varchar,
          type varchar,
          visible_to_students boolean,
          captured_at timestamp not null,
          source_file varchar,
          row_key varchar not null,
          inserted_at timestamp default now()
        );
        """
    )
    con.execute(
        "create unique index if not exists ux_activities_snapshot_rowkey on raw.activities_snapshot(row_key);"
    )

    files = sorted(snapshots_dir.glob(glob))
    inserted = 0
    scanned = 0
    max_captured_at: Optional[datetime] = None

    for fp in files:
        for row in iter_snapshot_csv(fp):
            scanned += 1
            # INSERT OR IGNORE (duckdb wspiera ON CONFLICT w nowych wersjach; jeśli nie, użyj anti-join)
            con.execute(
                """
                insert into raw.activities_snapshot
                (course_code, activity_id, name, type, visible_to_students, captured_at, source_file, row_key)
                select ?,?,?,?,?,?,?,?
                where not exists (select 1 from raw.activities_snapshot where row_key = ?)
                """,
                [
                    row.course_code,
                    row.activity_id,
                    row.name,
                    row.type,
                    row.visible_to_students,
                    row.captured_at,
                    row.source_file,
                    row.row_key,
                    row.row_key,
                ],
            )
            # duckdb python: rowcount by changes() nie zawsze; policzmy inaczej:
            # użyjemy changes() (działa w duckdb)
            ch = con.execute("select changes()").fetchone()[0]
            inserted += int(ch)

            if max_captured_at is None or row.captured_at > max_captured_at:
                max_captured_at = row.captured_at

    return {
        "files": len(files),
        "scanned_rows": scanned,
        "inserted_rows": inserted,
        "max_captured_at": max_captured_at.isoformat() if max_captured_at else None,
    }

# --- compat exports for tests/CLI ---
# Jeżeli w pliku są już takie dataclassy wyżej, to ten blok nic nie psuje.
# Jeśli ich nie ma / są pod inną nazwą, to zapewnia kompatybilne nazwy do importu.

from dataclasses import dataclass
from typing import List

if "DeletionConfig" not in globals():
    @dataclass(frozen=True)
    class DeletionConfig:
        delete_operations: List[str]
        delete_tech_keys: List[str]
        delete_activity_labels_regex: List[str]
        disappearance_grace_period_days: int
        min_missing_snapshots_to_confirm: int
        deleted_at_policy: str  # "first_missing" | "last_seen"

if "MappingConfig" not in globals():
    @dataclass(frozen=True)
    class MappingConfig:
        use_activity_id_map_table: bool
        allow_fuzzy_name_type_match: bool

if "IncrementalConfig" not in globals():
    @dataclass(frozen=True)
    class IncrementalConfig:
        checkpoint_table: str
        checkpoint_key: str
        process_only_new_snapshots: bool
        process_only_new_events: bool

if "BuildConfig" not in globals():
    @dataclass(frozen=True)
    class BuildConfig:
        deletion: "DeletionConfig"
        mapping: "MappingConfig"
        incremental: "IncrementalConfig"

def build_activities_state(con, cfg):
    """
    Public API expected by tests/CLI.

    con: duckdb connection
    cfg: BuildConfig (lub kompatybilny obiekt z polami .deletion/.mapping/.incremental)
    """
    # public API expected by tests
    build_activities_state = run_activities_state

__all__ = [
    "DeletionConfig",
    "MappingConfig",
    "IncrementalConfig",
    "BuildConfig",
    "build_activities_state",
]
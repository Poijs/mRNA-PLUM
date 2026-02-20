from __future__ import annotations

import csv
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

import duckdb


def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", errors="replace")).hexdigest()


def _captured_at_from_filename(path: Path) -> datetime:
    # np. 20260206-0826_2526z_zawartosc_kursow.csv
    m = re.search(r"(\d{8})-(\d{4})", path.name)
    if not m:
        return datetime.fromtimestamp(path.stat().st_mtime)
    ymd = m.group(1)  # YYYYMMDD
    hm = m.group(2)   # HHMM
    return datetime(
        int(ymd[0:4]), int(ymd[4:6]), int(ymd[6:8]),
        int(hm[0:2]), int(hm[2:4]),
    )


@dataclass(frozen=True)
class SnapshotRowPL:
    course_code: str
    activity_id: str
    name: str
    type: str
    visible_to_students: bool
    captured_at: datetime
    source_file: str
    row_key: str


def iter_snapshot_csv_plum_visible(path: Path) -> Iterable[SnapshotRowPL]:
    captured_at = _captured_at_from_filename(path)

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        req = {"Nazwa kursu", "ID aktywności", "Nazwa aktywności", "Format aktywności"}
        missing = req - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Snapshot(PL) missing columns {sorted(missing)} in {path}")

        for r in reader:
            course_code = (r.get("Nazwa kursu") or "").strip()
            activity_id = (r.get("ID aktywności") or "").strip()
            if not course_code or not activity_id:
                continue

            name = (r.get("Nazwa aktywności") or "").strip()
            typ = (r.get("Format aktywności") or "").strip()

            payload = json.dumps(
                {
                    "course_code": course_code,
                    "activity_id": activity_id,
                    "name": name,
                    "type": typ,
                    "visible_to_students": True,
                    "captured_at": captured_at.isoformat(),
                },
                ensure_ascii=False,
                separators=(",", ":"),
            )
            row_key = _sha1(payload)

            yield SnapshotRowPL(
                course_code=course_code,
                activity_id=activity_id,
                name=name,
                type=typ,
                visible_to_students=True,
                captured_at=captured_at,
                source_file=str(path),
                row_key=row_key,
            )


def ensure_snapshot_table(con: duckdb.DuckDBPyConnection) -> None:
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


def load_plum_snapshot_file_into_duckdb(
    con: duckdb.DuckDBPyConnection,
    snapshot_file: Path,
) -> dict:
    ensure_snapshot_table(con)

    inserted = 0
    scanned = 0
    max_captured_at: Optional[datetime] = None

    for row in iter_snapshot_csv_plum_visible(snapshot_file):
        scanned += 1
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
        inserted += int(con.execute("select changes()").fetchone()[0])

        if max_captured_at is None or row.captured_at > max_captured_at:
            max_captured_at = row.captured_at

    return {
        "snapshot_file": str(snapshot_file),
        "scanned_rows": scanned,
        "inserted_rows": inserted,
        "captured_at": max_captured_at.isoformat() if max_captured_at else None,
    }
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Any, Dict, Optional
import json
import uuid

import duckdb


# --- Config dataclasses expected by tests ---

@dataclass(frozen=True)
class DeletionConfig:
    delete_operations: List[str]
    delete_tech_keys: List[str]
    delete_activity_labels_regex: List[str]
    disappearance_grace_period_days: int
    min_missing_snapshots_to_confirm: int
    deleted_at_policy: str  # "first_missing" | "last_seen"


@dataclass(frozen=True)
class MappingConfig:
    use_activity_id_map_table: bool
    allow_fuzzy_name_type_match: bool


@dataclass(frozen=True)
class IncrementalConfig:
    checkpoint_table: str
    checkpoint_key: str
    process_only_new_snapshots: bool
    process_only_new_events: bool


@dataclass(frozen=True)
class BuildConfig:
    deletion: DeletionConfig
    mapping: MappingConfig
    incremental: IncrementalConfig


def _ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("create schema if not exists raw;")
    con.execute("create schema if not exists mart;")

    con.execute("""
    create table if not exists mart.activities_state (
      course_code varchar not null,
      ay varchar,
      term varchar,
      wydzial_code varchar,
      kierunek_code varchar,
      track_code varchar,
      semester_code varchar,

      activity_id varchar not null,
      type varchar,
      name_last varchar,

      first_seen_at timestamp,
      last_seen_at timestamp,

      last_snapshot_at timestamp,
      last_event_at timestamp,

      visible_last boolean,
      deleted_at timestamp,

      status_final varchar not null,
      evidence_deleted varchar not null,
      confidence_deleted double not null,

      notes varchar,
      updated_at timestamp default now(),

      primary key(course_code, activity_id)
    );
    """)

    con.execute("""
    create table if not exists mart.activities_qa (
      qa_id varchar,
      qa_type varchar not null,
      course_code varchar,
      activity_id varchar,
      object_id varchar,
      details_json varchar,
      created_at timestamp default now()
    );
    """)


def _qa(con: duckdb.DuckDBPyConnection, qa_type: str,
        course_code: Optional[str], activity_id: Optional[str],
        object_id: Optional[str], details: Dict[str, Any]) -> None:
    con.execute(
        """
        insert into mart.activities_qa(qa_id, qa_type, course_code, activity_id, object_id, details_json)
        values (?,?,?,?,?,?)
        """,
        [str(uuid.uuid4()), qa_type, course_code, activity_id, object_id, json.dumps(details, ensure_ascii=False)],
    )


def build_activities_state(con: duckdb.DuckDBPyConnection, cfg: BuildConfig) -> dict:
    """
    Public API expected by tests.
    Minimal implementation to satisfy:
    - delete from logs
    - disappearance from snapshots
    - hidden
    - conflict QA
    - missing mapping QA
    """
    _ensure_tables(con)

    # Universe: wszystkie (course_code, activity_id) z snapshotów + eventów gdzie object_id nie null
    con.execute("""
    create temp table tmp_universe as
    select distinct course_code, activity_id
    from raw.activities_snapshot
    union
    select distinct course_code, cast(object_id as varchar) as activity_id
    from events_canonical
    where object_id is not null;
    """)

    # Snapshot last
    con.execute("""
    create temp table tmp_snap_last as
    select s.*
    from raw.activities_snapshot s
    join (
      select course_code, activity_id, max(captured_at) as mx
      from raw.activities_snapshot
      group by 1,2
    ) t
    on s.course_code=t.course_code and s.activity_id=t.activity_id and s.captured_at=t.mx;
    """)

    # Snapshot bounds per activity
    con.execute("""
    create temp table tmp_snap_bounds as
    select course_code, activity_id, min(captured_at) as first_snap, max(captured_at) as last_snap
    from raw.activities_snapshot
    group by 1,2;
    """)

    # Event bounds per object_id (map 1:1 -> activity_id)
    con.execute("""
    create temp table tmp_evt_bounds as
    select course_code, cast(object_id as varchar) as activity_id,
           min(ts_utc) as first_evt,
           max(ts_utc) as last_evt
    from events_canonical
    where counted = true and object_id is not null
    group by 1,2;
    """)

    # Delete from logs (operation == DELETE)
    con.execute("""
    create temp table tmp_evt_delete as
    select course_code, cast(object_id as varchar) as activity_id, min(ts_utc) as deleted_at_log
    from events_canonical
    where counted = true
      and object_id is not null
      and upper(operation) = 'DELETE'
    group by 1,2;
    """)

    # Disappearance:
    # - last_seen_snap_at = max(captured_at) for activity
    # - count snapshots of course after last_seen_snap_at (>=min_missing)
    # - last_course_snapshot_at >= first_missing + grace
    con.execute("""
    create temp table tmp_course_snaps as
    select course_code, captured_at
    from raw.activities_snapshot
    group by 1,2;
    """)

    con.execute("""
    create temp table tmp_last_seen as
    select course_code, activity_id,
           max(captured_at) as last_seen_snap_at,
           min(captured_at) as first_seen_snap_at
    from raw.activities_snapshot
    group by 1,2;
    """)

    con.execute("""
    create temp table tmp_missing as
    select
      a.course_code,
      a.activity_id,
      a.last_seen_snap_at,
      min(cs.captured_at) as first_missing_snapshot_at,
      count(*) as missing_count,
      max(cs.captured_at) as last_course_snapshot_at
    from tmp_last_seen a
    join tmp_course_snaps cs
      on cs.course_code=a.course_code and cs.captured_at > a.last_seen_snap_at
    group by 1,2,3;
    """)

    con.execute("""
    create temp table tmp_disappearance as
    select
      course_code,
      activity_id,
      case
        when ? = 'first_missing' then first_missing_snapshot_at
        else last_seen_snap_at
      end as deleted_at_snap,
      first_missing_snapshot_at,
      last_course_snapshot_at,
      missing_count
    from tmp_missing
    where missing_count >= ?
      and last_course_snapshot_at >= (first_missing_snapshot_at + (? || ' days')::interval);
    """, [cfg.deletion.deleted_at_policy, cfg.deletion.min_missing_snapshots_to_confirm, cfg.deletion.disappearance_grace_period_days])

    # Course meta (z eventów)
    con.execute("""
    create temp table tmp_course_meta as
    select course_code,
           any_value(ay) as ay,
           any_value(term) as term,
           any_value(wydzial_code) as wydzial_code,
           any_value(kierunek_code) as kierunek_code,
           any_value(track_code) as track_code,
           any_value(semester_code) as semester_code
    from events_canonical
    group by 1;
    """)

    # Final stage
    con.execute("""
    create temp table tmp_final as
    select
      u.course_code,
      m.ay, m.term, m.wydzial_code, m.kierunek_code, m.track_code, m.semester_code,
      u.activity_id,
      sl.type,
      sl.name as name_last,
      least(sb.first_snap, eb.first_evt) as first_seen_at,
      greatest(sb.last_snap, eb.last_evt) as last_seen_at,
      sb.last_snap as last_snapshot_at,
      eb.last_evt as last_event_at,
      sl.visible_to_students as visible_last,
      dlog.deleted_at_log,
      ds.deleted_at_snap,
      case
        when dlog.deleted_at_log is not null and ds.deleted_at_snap is not null then least(dlog.deleted_at_log, ds.deleted_at_snap)
        when dlog.deleted_at_log is not null then dlog.deleted_at_log
        when ds.deleted_at_snap is not null then ds.deleted_at_snap
        else null
      end as deleted_at,
      case
        when dlog.deleted_at_log is not null and ds.deleted_at_snap is not null then 'both'
        when dlog.deleted_at_log is not null then 'log_delete_event'
        when ds.deleted_at_snap is not null then 'snapshot_disappearance'
        else 'none'
      end as evidence_deleted,
      case
        when dlog.deleted_at_log is not null and ds.deleted_at_snap is not null then 0.95
        when dlog.deleted_at_log is not null then 0.80
        when ds.deleted_at_snap is not null then 0.70
        else 0.0
      end as confidence_deleted
    from tmp_universe u
    left join tmp_course_meta m using(course_code)
    left join tmp_snap_last sl using(course_code, activity_id)
    left join tmp_snap_bounds sb using(course_code, activity_id)
    left join tmp_evt_bounds eb using(course_code, activity_id)
    left join tmp_evt_delete dlog using(course_code, activity_id)
    left join tmp_disappearance ds using(course_code, activity_id);
    """)

    # QA: conflict log delete but snapshot visible after delete
    rows = con.execute("""
    select course_code, activity_id, deleted_at_log, last_snapshot_at
    from tmp_final
    where deleted_at_log is not null
      and last_snapshot_at is not null
      and last_snapshot_at > deleted_at_log
      and visible_last = true;
    """).fetchall()
    for course_code, activity_id, deleted_at_log, last_snapshot_at in rows:
        _qa(con, "conflict_log_delete_but_visible_in_snapshot", course_code, activity_id, None, {
            "deleted_at_log": str(deleted_at_log),
            "last_snapshot_at": str(last_snapshot_at),
        })

    # QA: activity without mapping (snapshot exists but no events for it)
    rows = con.execute("""
    select s.course_code, s.activity_id
    from (select distinct course_code, activity_id from raw.activities_snapshot) s
    left join (select distinct course_code, cast(object_id as varchar) as activity_id from events_canonical where object_id is not null) e
      on e.course_code=s.course_code and e.activity_id=s.activity_id
    where e.activity_id is null;
    """).fetchall()
    for course_code, activity_id in rows:
        _qa(con, "activity_without_object_id_mapping", course_code, activity_id, None, {})

    # status_final
    con.execute("""
    create temp table tmp_final2 as
    select *,
      case
        when deleted_at is not null then 'visible_deleted'
        when visible_last = true then 'visible_active'
        when visible_last = false then 'hidden'
        else 'unknown'
      end as status_final,
      cast(null as varchar) as notes
    from tmp_final;
    """)

    # Upsert (delete+insert for simplicity in tests)
    con.execute("delete from mart.activities_state;")
    con.execute("""
    insert into mart.activities_state(
      course_code, ay, term, wydzial_code, kierunek_code, track_code, semester_code,
      activity_id, type, name_last,
      first_seen_at, last_seen_at,
      last_snapshot_at, last_event_at,
      visible_last,
      deleted_at,
      status_final, evidence_deleted, confidence_deleted,
      notes
    )
    select
      course_code, ay, term, wydzial_code, kierunek_code, track_code, semester_code,
      activity_id, type, name_last,
      first_seen_at, last_seen_at,
      last_snapshot_at, last_event_at,
      visible_last,
      deleted_at,
      status_final, evidence_deleted, confidence_deleted,
      notes
    from tmp_final2;
    """)

    return {"ok": True}
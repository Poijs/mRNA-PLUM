from __future__ import annotations

from datetime import datetime
import duckdb
import pytest

from mrna_plum.activities.activities_state import (
    BuildConfig, DeletionConfig, MappingConfig, IncrementalConfig, build_activities_state
)

def _cfg(**kw):
    return BuildConfig(
        deletion=DeletionConfig(
            delete_operations=kw.get("delete_operations", ["DELETE"]),
            delete_tech_keys=kw.get("delete_tech_keys", []),
            delete_activity_labels_regex=[],
            disappearance_grace_period_days=kw.get("grace", 14),
            min_missing_snapshots_to_confirm=kw.get("min_missing", 2),
            deleted_at_policy=kw.get("policy", "first_missing"),
        ),
        mapping=MappingConfig(
            use_activity_id_map_table=True,
            allow_fuzzy_name_type_match=False,
        ),
        incremental=IncrementalConfig(
            checkpoint_table="raw.pipeline_checkpoints",
            checkpoint_key="build_activities_state",
            process_only_new_snapshots=False,
            process_only_new_events=False,
        ),
    )

def setup_base(con):
    con.execute("create schema if not exists raw;")
    con.execute("create schema if not exists mart;")

    con.execute("""
      create table events_canonical (
        course_code varchar,
        ay varchar,
        term varchar,
        wydzial_code varchar,
        kierunek_code varchar,
        track_code varchar,
        semester_code varchar,
        ts_utc timestamp,
        teacher_id varchar,
        operation varchar,
        tech_key varchar,
        activity_label varchar,
        object_id varchar,
        count_mode varchar,
        row_key varchar,
        source_file varchar,
        counted boolean
      );
    """)
    con.execute("""
      create table raw.activities_snapshot(
        course_code varchar,
        activity_id varchar,
        name varchar,
        type varchar,
        visible_to_students boolean,
        captured_at timestamp,
        source_file varchar,
        row_key varchar
      );
    """)
    con.execute("""
      create table raw.activity_id_map(
        course_code varchar,
        activity_id varchar,
        object_id varchar,
        map_method varchar,
        confidence double,
        first_seen_at timestamp,
        last_seen_at timestamp,
        primary key(course_code, activity_id)
      );
    """)

def test_delete_from_logs():
    con = duckdb.connect(":memory:")
    setup_base(con)

    con.execute("""
      insert into raw.activities_snapshot values
      ('C1','101','Quiz 1','quiz', true, '2026-02-01 10:00:00','f.csv','rk1'),
      ('C1','101','Quiz 1','quiz', true, '2026-02-10 10:00:00','f.csv','rk2')
    """)

    con.execute("""
      insert into events_canonical values
      ('C1','2025/26','Z','W1','K1','T1','S1','2026-02-05 12:00:00','U1','DELETE','tk_del','', '101','object-based','e1','e.csv', true)
    """)

    stats = build_activities_state(con, _cfg(delete_operations=["DELETE"]))
    row = con.execute("select status_final, evidence_deleted, deleted_at from mart.activities_state where course_code='C1' and activity_id='101'").fetchone()
    assert row[0] == "visible_deleted"
    assert row[1] in ("log_delete_event", "both")
    assert row[2] is not None

def test_disappearance_from_snapshots():
    con = duckdb.connect(":memory:")
    setup_base(con)

    # snapshoty kursu: aktywność znika po 2026-02-01, potem mamy 2 snapshoty bez niej i grace spełniony
    con.execute("""
      insert into raw.activities_snapshot values
      ('C1','200','Page A','page', true, '2026-02-01 10:00:00','f.csv','a1'),
      ('C1','201','Other','page', true, '2026-02-08 10:00:00','f.csv','a2'),
      ('C1','201','Other','page', true, '2026-02-20 10:00:00','f.csv','a3')
    """)
    # brak eventów

    stats = build_activities_state(con, _cfg(grace=7, min_missing=2, policy="first_missing"))
    row = con.execute("select status_final, evidence_deleted from mart.activities_state where course_code='C1' and activity_id='200'").fetchone()
    assert row[1] == "snapshot_disappearance"
    assert row[0] == "visible_deleted"

def test_hidden():
    con = duckdb.connect(":memory:")
    setup_base(con)

    con.execute("""
      insert into raw.activities_snapshot values
      ('C1','300','Forum','forum', false, '2026-02-20 10:00:00','f.csv','h1')
    """)
    stats = build_activities_state(con, _cfg())
    row = con.execute("select status_final from mart.activities_state where course_code='C1' and activity_id='300'").fetchone()
    assert row[0] == "hidden"

def test_conflict_log_delete_but_snapshot_visible():
    con = duckdb.connect(":memory:")
    setup_base(con)

    con.execute("""
      insert into raw.activities_snapshot values
      ('C1','400','H5P','h5p', true, '2026-02-20 10:00:00','f.csv','c1')
    """)
    con.execute("""
      insert into events_canonical values
      ('C1','2025/26','Z','W1','K1','T1','S1','2026-02-10 12:00:00','U1','DELETE','tk_del','', '400','object-based','e1','e.csv', true)
    """)
    build_activities_state(con, _cfg(delete_operations=["DELETE"]))
    qa = con.execute("select count(*) from mart.activities_qa where qa_type='conflict_log_delete_but_visible_in_snapshot'").fetchone()[0]
    assert qa >= 1

def test_missing_mapping_activity_to_object():
    con = duckdb.connect(":memory:")
    setup_base(con)

    # snapshot istnieje, ale w events brak object_id/akcji
    con.execute("""
      insert into raw.activities_snapshot values
      ('C1','500','URL','url', true, '2026-02-20 10:00:00','f.csv','m1')
    """)
    build_activities_state(con, _cfg())
    qa = con.execute("select count(*) from mart.activities_qa where qa_type='activity_without_object_id_mapping'").fetchone()[0]
    assert qa >= 1
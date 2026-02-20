from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict, List, Optional, Tuple

import duckdb


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


def ensure_tables(con: duckdb.DuckDBPyConnection, checkpoint_table: str) -> None:
    con.execute("create schema if not exists raw;")
    con.execute("create schema if not exists mart;")

    con.execute(
        f"""
        create table if not exists {checkpoint_table} (
          pipeline_key varchar primary key,
          last_snapshot_captured_at timestamp,
          last_event_ts_utc timestamp,
          updated_at timestamp default now()
        );
        """
    )

    con.execute(
        """
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
        """
    )

    con.execute(
        """
        create table if not exists mart.activities_qa (
          qa_id varchar,
          qa_type varchar not null,
          course_code varchar,
          activity_id varchar,
          object_id varchar,
          details_json varchar,
          created_at timestamp default now()
        );
        """
    )

    con.execute(
        """
        create or replace view mart.v_activities_state_for_reporting as
        select
          s.*,
          case when status_final='visible_active' then 1 else 0 end as is_visible_active,
          case when status_final='hidden' then 1 else 0 end as is_hidden,
          case when status_final='visible_deleted' then 1 else 0 end as is_visible_deleted
        from mart.activities_state s;
        """
    )


def _write_qa(
    con: duckdb.DuckDBPyConnection,
    qa_type: str,
    course_code: Optional[str],
    activity_id: Optional[str],
    object_id: Optional[str],
    details: Dict[str, Any],
) -> None:
    con.execute(
        """
        insert into mart.activities_qa
        (qa_id, qa_type, course_code, activity_id, object_id, details_json)
        values (?,?,?,?,?,?)
        """,
        [str(uuid.uuid4()), qa_type, course_code, activity_id, object_id, json.dumps(details, ensure_ascii=False)],
    )


def _compile_regexes(patterns: List[str]) -> List[re.Pattern]:
    out: List[re.Pattern] = []
    for p in patterns:
        out.append(re.compile(p, flags=re.IGNORECASE))
    return out


def build_activities_state(
    con: duckdb.DuckDBPyConnection,
    cfg: BuildConfig,
) -> dict:
    ensure_tables(con, cfg.incremental.checkpoint_table)

    # checkpoint
    cp = con.execute(
        f"select last_snapshot_captured_at, last_event_ts_utc from {cfg.incremental.checkpoint_table} where pipeline_key=?",
        [cfg.incremental.checkpoint_key],
    ).fetchone()
    last_snap_cp = cp[0] if cp else None
    last_evt_cp = cp[1] if cp else None

    # wybór zakresu danych incremental
    snap_filter = ""
    evt_filter = ""
    params: List[Any] = []
    if cfg.incremental.process_only_new_snapshots and last_snap_cp is not None:
        snap_filter = "where captured_at > ?"
        params.append(last_snap_cp)
    if cfg.incremental.process_only_new_events and last_evt_cp is not None:
        evt_filter = "and ts_utc > ?" if "where" in "where counted = true" else "where ts_utc > ?"
        # (bez kombinowania: zbudujemy osobno)
        pass

    # --- 1) zbuduj “snapshot_latest_per_activity” oraz historię widoczności ---
    # Bierzemy WSZYSTKIE snapshoty dla kursów/aktywności, bo disappearance wymaga historii.
    # Incremental robimy poprzez ograniczenie kursów dotkniętych nowymi snapshotami/eventami.

    # Kursy dotknięte zmianą:
    changed_courses = set()

    if cfg.incremental.process_only_new_snapshots and last_snap_cp is not None:
        rows = con.execute(
            "select distinct course_code from raw.activities_snapshot where captured_at > ?",
            [last_snap_cp],
        ).fetchall()
        changed_courses |= {r[0] for r in rows}
    else:
        # jeśli nie incremental – wszystko
        rows = con.execute("select distinct course_code from raw.activities_snapshot").fetchall()
        changed_courses |= {r[0] for r in rows}

    if cfg.incremental.process_only_new_events and last_evt_cp is not None:
        rows = con.execute(
            "select distinct course_code from events_canonical where ts_utc > ?",
            [last_evt_cp],
        ).fetchall()
        changed_courses |= {r[0] for r in rows}
    else:
        rows = con.execute("select distinct course_code from events_canonical").fetchall()
        changed_courses |= {r[0] for r in rows}

    if not changed_courses:
        return {"changed_courses": 0, "upserted": 0, "qa": 0, "checkpoint_updated": False}

    con.execute("create temp table tmp_changed_courses(course_code varchar);")
    con.executemany("insert into tmp_changed_courses values (?)", [(c,) for c in sorted(changed_courses)])

    # --- 2) mapowanie activity_id <-> object_id ---
    # a) direct map: object_id == activity_id
    # b) raw.activity_id_map (jeśli włączone)
    # c) (opcjonalnie) name+type match

    con.execute(
        """
        create temp table tmp_map_direct as
        select distinct
          e.course_code,
          cast(e.object_id as varchar) as object_id,
          cast(e.object_id as varchar) as activity_id,
          'direct' as map_method,
          1.0::double as confidence
        from events_canonical e
        join tmp_changed_courses c using(course_code)
        where e.object_id is not null
        """
    )

    if cfg.mapping.use_activity_id_map_table:
        # tabela mapowań (manual/previous runs)
        con.execute(
            """
            create temp table tmp_map_table as
            select
              m.course_code,
              m.object_id,
              m.activity_id,
              m.map_method,
              coalesce(m.confidence, 0.8) as confidence
            from raw.activity_id_map m
            join tmp_changed_courses c using(course_code)
            """
        )
    else:
        con.execute("create temp table tmp_map_table as select * from tmp_map_direct where 1=0;")

    # final map: prefer raw.activity_id_map, potem direct
    con.execute(
        """
        create temp table tmp_activity_object_map as
        select * from (
          select course_code, activity_id, object_id, map_method, confidence,
                 1 as prio
          from tmp_map_table
          union all
          select course_code, activity_id, object_id, map_method, confidence,
                 2 as prio
          from tmp_map_direct
        )
        qualify row_number() over(partition by course_code, activity_id order by prio asc, confidence desc) = 1
        """
    )

    # Konflikty: to samo activity_id -> różne object_id (w danych źródłowych)
    conflicts = con.execute(
        """
        select course_code, activity_id, count(distinct object_id) as n
        from (
          select course_code, activity_id, object_id from tmp_map_table
          union all
          select course_code, activity_id, object_id from tmp_map_direct
        )
        group by 1,2
        having count(distinct object_id) > 1
        """
    ).fetchall()
    qa_count = 0
    for course_code, activity_id, n in conflicts:
        _write_qa(
            con,
            "mapping_conflict_activity_to_object",
            course_code,
            activity_id,
            None,
            {"distinct_object_ids": int(n)},
        )
        qa_count += 1

    # --- 3) agregaty snapshotów ---
    con.execute(
        """
        create temp table tmp_snap_hist as
        select
          s.course_code,
          s.activity_id,
          max(s.captured_at) as last_snapshot_at,
          min(s.captured_at) as first_snapshot_at
        from raw.activities_snapshot s
        join tmp_changed_courses c using(course_code)
        group by 1,2
        """
    )

    con.execute(
        """
        create temp table tmp_snap_last as
        select
          s.course_code,
          s.activity_id,
          s.type,
          s.name as name_last,
          s.visible_to_students as visible_last,
          s.captured_at as last_snapshot_at
        from raw.activities_snapshot s
        join (
          select course_code, activity_id, max(captured_at) as mx
          from raw.activities_snapshot
          join tmp_changed_courses c using(course_code)
          group by 1,2
        ) t
        on s.course_code=t.course_code and s.activity_id=t.activity_id and s.captured_at=t.mx
        """
    )

    # --- 4) agregaty eventów (last_event_at, first_event_at, delete_events) ---
    del_ops = [x.upper() for x in cfg.deletion.delete_operations]
    del_tech = [x.lower() for x in cfg.deletion.delete_tech_keys]
    del_lbl_rx = _compile_regexes(cfg.deletion.delete_activity_labels_regex)

    # W SQL nie wstrzykniemy regex python; więc:
    # - opieramy się głównie o operation i tech_key (które są już deterministyczne z KEYS),
    # - a label regex można zrobić w python jako dodatkowe QA/override w przyszłości.
    # Na teraz: label regex pomijamy w logice core (lub robimy dodatkową flagę przez duckdb regexp_matches jeśli chcesz).
    con.execute(
        """
        create temp table tmp_evt_aggr as
        select
          e.course_code,
          cast(e.object_id as varchar) as object_id,
          max(e.ts_utc) as last_event_at,
          min(e.ts_utc) as first_event_at
        from events_canonical e
        join tmp_changed_courses c using(course_code)
        where e.counted = true
        group by 1,2
        """
    )

    # delete events: operation OR tech_key
    # tech_key w events_canonical jest sterowane przez KEYS – zakładamy, że ma sensowną klasyfikację
    con.execute(
        """
        create temp table tmp_evt_delete as
        select
          e.course_code,
          cast(e.object_id as varchar) as object_id,
          min(e.ts_utc) as deleted_at_log,
          count(*) as delete_events_n
        from events_canonical e
        join tmp_changed_courses c using(course_code)
        where e.counted = true
          and e.object_id is not null
        group by 1,2
        """
    )

    # ogranicz wiersze tmp_evt_delete do tych, które spełniają warunek delete wg config
    # (robimy to w python: filtr listą operacji i tech_key, bo to mała agregacja)
    # Pobierz kandydatów: course_code, object_id, min(ts), n + sample tech/operation? – weźmy min_by
    del_rows = con.execute(
        """
        select
          e.course_code,
          cast(e.object_id as varchar) as object_id,
          min(e.ts_utc) as deleted_at_log,
          any_value(e.operation) as any_operation,
          any_value(e.tech_key) as any_tech_key,
          count(*) as n
        from events_canonical e
        join tmp_changed_courses c using(course_code)
        where e.counted = true
          and e.object_id is not null
        group by 1,2
        """
    ).fetchall()

    # zbuduj temp table z faktycznymi delete
    con.execute("create temp table tmp_evt_delete_filtered(course_code varchar, object_id varchar, deleted_at_log timestamp);")
    for course_code, object_id, deleted_at_log, any_operation, any_tech_key, n in del_rows:
        op_ok = (str(any_operation or "").upper() in del_ops) if del_ops else False
        tk_ok = (str(any_tech_key or "").lower() in del_tech) if del_tech else False
        if op_ok or tk_ok:
            con.execute(
                "insert into tmp_evt_delete_filtered values (?,?,?)",
                [course_code, object_id, deleted_at_log],
            )

    # --- 5) zniknięcia w snapshotach (disappearance) ---
    # Definicja: aktywność była kiedyś, ale w ostatnich N snapshotach kursu już jej nie ma
    # i minęło >= grace_days od pierwszego braku.

    grace_days = cfg.deletion.disappearance_grace_period_days
    min_missing = cfg.deletion.min_missing_snapshots_to_confirm

    # Dla każdego kursu budujemy listę snapshotów (captured_at), a potem sprawdzamy aktywność:
    # - last_seen_at = max(captured_at gdzie activity występuje)
    # - first_missing_after_last_seen = min(captured_at snapshotu kursu > last_seen_at gdzie activity nie występuje) -> to będzie last_snapshot kursu po last_seen
    # Uwaga: “min_missing_snapshots_to_confirm” liczymy jako liczba snapshotów kursu po last_seen_at.
    con.execute(
        """
        create temp table tmp_course_snapshots as
        select
          course_code,
          captured_at
        from raw.activities_snapshot
        join tmp_changed_courses c using(course_code)
        group by 1,2
        """
    )

    # last_seen_at per activity (snapshot)
    con.execute(
        """
        create temp table tmp_last_seen_snap as
        select
          course_code,
          activity_id,
          max(captured_at) as last_seen_snap_at,
          min(captured_at) as first_seen_snap_at
        from raw.activities_snapshot
        join tmp_changed_courses c using(course_code)
        group by 1,2
        """
    )

    # ile snapshotów kursu jest po last_seen_snap_at?
    con.execute(
        """
        create temp table tmp_missing_stats as
        select
          a.course_code,
          a.activity_id,
          a.last_seen_snap_at,
          min(cs.captured_at) as first_missing_snapshot_at,
          count(*) as missing_snapshots_count,
          max(cs.captured_at) as last_course_snapshot_at
        from tmp_last_seen_snap a
        join tmp_course_snapshots cs
          on cs.course_code=a.course_code and cs.captured_at > a.last_seen_snap_at
        group by 1,2,3
        """
    )

    # potwierdzone disappearance wg progów
    con.execute(
        """
        create temp table tmp_snap_disappearance as
        select
          course_code,
          activity_id,
          case
            when ? = 'first_missing' then first_missing_snapshot_at
            else last_seen_snap_at
          end as deleted_at_snap,
          first_missing_snapshot_at,
          last_seen_snap_at,
          missing_snapshots_count,
          last_course_snapshot_at
        from tmp_missing_stats
        where missing_snapshots_count >= ?
          and last_course_snapshot_at >= (first_missing_snapshot_at + (? || ' days')::interval)
        """,
        [cfg.deletion.deleted_at_policy, min_missing, grace_days],
    )

    # --- 6) Złóż “activity universe” = snapshoty + eventy (po mapowaniu) ---
    # Universe po activity_id (snapshot) + eventy po object_id->activity_id (map)
    con.execute(
        """
        create temp table tmp_universe as
        select
          s.course_code,
          s.activity_id
        from tmp_snap_hist s
        union
        select
          m.course_code,
          m.activity_id
        from tmp_activity_object_map m
        """
    )

    # --- 7) Zaciągnij atrybuty kursu z events_canonical (ay/term/wydzial/...) ---
    # Zakładamy, że events_canonical ma spójne meta per course_code.
    con.execute(
        """
        create temp table tmp_course_meta as
        select
          e.course_code,
          any_value(e.ay) as ay,
          any_value(e.term) as term,
          any_value(e.wydzial_code) as wydzial_code,
          any_value(e.kierunek_code) as kierunek_code,
          any_value(e.track_code) as track_code,
          any_value(e.semester_code) as semester_code
        from events_canonical e
        join tmp_changed_courses c using(course_code)
        group by 1
        """
    )

    # --- 8) Połącz wszystko do staging “tmp_state_stage” ---
    con.execute(
        """
        create temp table tmp_state_stage as
        select
          u.course_code,
          cm.ay, cm.term, cm.wydzial_code, cm.kierunek_code, cm.track_code, cm.semester_code,

          u.activity_id,
          sl.type,
          sl.name_last,

          -- first/last seen: min/max z snapshotów i eventów
          least(coalesce(ls.first_seen_snap_at, timestamp '9999-12-31'), coalesce(ea.first_event_at, timestamp '9999-12-31')) as first_seen_at_tmp,
          greatest(coalesce(ls.last_seen_snap_at, timestamp '0001-01-01'), coalesce(ea.last_event_at, timestamp '0001-01-01')) as last_seen_at_tmp,

          sl.last_snapshot_at,
          ea.last_event_at,

          sl.visible_last,

          dlog.deleted_at_log,
          ds.deleted_at_snap,

          ds.missing_snapshots_count

        from tmp_universe u
        left join tmp_course_meta cm using(course_code)
        left join tmp_snap_last sl using(course_code, activity_id)
        left join tmp_last_seen_snap ls using(course_code, activity_id)

        left join tmp_activity_object_map map using(course_code, activity_id)
        left join tmp_evt_aggr ea
          on ea.course_code=u.course_code and ea.object_id=map.object_id

        left join tmp_evt_delete_filtered dlog
          on dlog.course_code=u.course_code and dlog.object_id=map.object_id

        left join tmp_snap_disappearance ds using(course_code, activity_id)
        """
    )

    # clean first/last (zamiana sentinel na null)
    con.execute(
        """
        create temp table tmp_state_stage2 as
        select
          *,
          case when first_seen_at_tmp = timestamp '9999-12-31' then null else first_seen_at_tmp end as first_seen_at,
          case when last_seen_at_tmp = timestamp '0001-01-01' then null else last_seen_at_tmp end as last_seen_at
        from tmp_state_stage
        """
    )

    # --- 9) Konflikty dowodów + wyliczenie status_final, evidence_deleted, confidence ---
    # deleted_at: min z (log, snap) jeśli oba istnieją? – domyślnie min
    con.execute(
        """
        create temp table tmp_state_final as
        select
          course_code, ay, term, wydzial_code, kierunek_code, track_code, semester_code,
          activity_id, type, name_last,
          first_seen_at, last_seen_at,
          last_snapshot_at, last_event_at,
          visible_last,

          case
            when deleted_at_log is not null and deleted_at_snap is not null then least(deleted_at_log, deleted_at_snap)
            when deleted_at_log is not null then deleted_at_log
            when deleted_at_snap is not null then deleted_at_snap
            else null
          end as deleted_at,

          case
            when deleted_at_log is not null and deleted_at_snap is not null then 'both'
            when deleted_at_log is not null then 'log_delete_event'
            when deleted_at_snap is not null then 'snapshot_disappearance'
            else 'none'
          end as evidence_deleted,

          -- confidence: prosta heurystyka (możesz rozbudować)
          case
            when deleted_at_log is not null and deleted_at_snap is not null then 0.95
            when deleted_at_log is not null and deleted_at_snap is null then 0.80
            when deleted_at_log is null and deleted_at_snap is not null then 0.70
            else 0.0
          end as confidence_deleted,

          -- status_final
          case
            when deleted_at is not null then 'visible_deleted'
            when visible_last = true then 'visible_active'
            when visible_last = false then 'hidden'
            else 'unknown'
          end as status_final,

          cast(null as varchar) as notes
        from tmp_state_stage2
        """
    )

    # Konflikt: log delete, ale snapshot nadal widzi (visible_last=true i last_snapshot_at > deleted_at_log)
    bad = con.execute(
        """
        select course_code, activity_id, deleted_at_log, last_snapshot_at, visible_last
        from tmp_state_stage2
        where deleted_at_log is not null
          and last_snapshot_at is not null
          and last_snapshot_at > deleted_at_log
          and visible_last = true
        """
    ).fetchall()
    for course_code, activity_id, deleted_at_log, last_snapshot_at, visible_last in bad:
        _write_qa(
            con,
            "conflict_log_delete_but_visible_in_snapshot",
            course_code,
            activity_id,
            None,
            {
                "deleted_at_log": str(deleted_at_log),
                "last_snapshot_at": str(last_snapshot_at),
                "visible_last": bool(visible_last),
                "hint": "possible id mismatch object_id!=activity_id or delayed snapshot",
            },
        )
        qa_count += 1

    # Kursy bez snapshotów, ale z eventami
    no_snaps = con.execute(
        """
        select distinct e.course_code
        from events_canonical e
        join tmp_changed_courses c using(course_code)
        where not exists (
          select 1 from raw.activities_snapshot s where s.course_code = e.course_code
        )
        """
    ).fetchall()
    for (course_code,) in no_snaps:
        _write_qa(con, "course_without_snapshots", course_code, None, None, {})
        qa_count += 1

    # Snapshoty kursów nieznanych w events (brak meta)
    unknown_course = con.execute(
        """
        select distinct s.course_code
        from raw.activities_snapshot s
        join tmp_changed_courses c using(course_code)
        where not exists (select 1 from events_canonical e where e.course_code=s.course_code)
        """
    ).fetchall()
    for (course_code,) in unknown_course:
        _write_qa(con, "snapshot_course_not_in_events", course_code, None, None, {})
        qa_count += 1

    # Aktywności bez mapowania (snapshot jest, ale nie ma object_id; jeśli to ma być QA)
    unmapped = con.execute(
        """
        select u.course_code, u.activity_id
        from tmp_universe u
        left join tmp_activity_object_map m using(course_code, activity_id)
        where m.object_id is null
        """
    ).fetchall()
    for course_code, activity_id in unmapped:
        _write_qa(
            con,
            "activity_without_object_id_mapping",
            course_code,
            activity_id,
            None,
            {"hint": "no matching events or missing activity_id_map; ok if activity never logged"},
        )
        qa_count += 1

    # --- 10) UPSERT/MERGE do mart.activities_state ---
    # DuckDB MERGE: działa w nowszych wersjach. Jeśli nie – fallback: delete+insert per changed_courses.
    # Robimy MERGE po (course_code, activity_id).
    con.execute(
        """
        merge into mart.activities_state as tgt
        using tmp_state_final as src
        on tgt.course_code = src.course_code and tgt.activity_id = src.activity_id
        when matched then update set
          ay=src.ay,
          term=src.term,
          wydzial_code=src.wydzial_code,
          kierunek_code=src.kierunek_code,
          track_code=src.track_code,
          semester_code=src.semester_code,
          type=src.type,
          name_last=src.name_last,
          first_seen_at=src.first_seen_at,
          last_seen_at=src.last_seen_at,
          last_snapshot_at=src.last_snapshot_at,
          last_event_at=src.last_event_at,
          visible_last=src.visible_last,
          deleted_at=src.deleted_at,
          status_final=src.status_final,
          evidence_deleted=src.evidence_deleted,
          confidence_deleted=src.confidence_deleted,
          notes=src.notes,
          updated_at=now()
        when not matched then insert (
          course_code, ay, term, wydzial_code, kierunek_code, track_code, semester_code,
          activity_id, type, name_last,
          first_seen_at, last_seen_at,
          last_snapshot_at, last_event_at,
          visible_last,
          deleted_at,
          status_final, evidence_deleted, confidence_deleted,
          notes
        )
        values (
          src.course_code, src.ay, src.term, src.wydzial_code, src.kierunek_code, src.track_code, src.semester_code,
          src.activity_id, src.type, src.name_last,
          src.first_seen_at, src.last_seen_at,
          src.last_snapshot_at, src.last_event_at,
          src.visible_last,
          src.deleted_at,
          src.status_final, src.evidence_deleted, src.confidence_deleted,
          src.notes
        )
        """
    )
    upserted = con.execute("select changes()").fetchone()[0]

    # --- 11) checkpoint update ---
    max_snap = con.execute(
        """
        select max(captured_at)
        from raw.activities_snapshot
        join tmp_changed_courses c using(course_code)
        """
    ).fetchone()[0]
    max_evt = con.execute(
        """
        select max(ts_utc)
        from events_canonical
        join tmp_changed_courses c using(course_code)
        """
    ).fetchone()[0]

    con.execute(
        f"""
        insert into {cfg.incremental.checkpoint_table}(pipeline_key, last_snapshot_captured_at, last_event_ts_utc, updated_at)
        values (?,?,?,now())
        on conflict(pipeline_key) do update set
          last_snapshot_captured_at=excluded.last_snapshot_captured_at,
          last_event_ts_utc=excluded.last_event_ts_utc,
          updated_at=now()
        """,
        [cfg.incremental.checkpoint_key, max_snap, max_evt],
    )

    return {
        "changed_courses": len(changed_courses),
        "upserted": int(upserted),
        "qa_written": int(qa_count),
        "checkpoint": {"last_snapshot_captured_at": str(max_snap), "last_event_ts_utc": str(max_evt)},
    }
# src/mrna_plum/parse/parse_events.py

from __future__ import annotations

import json
import os
import re
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

from mrna_plum.rules.activity_rules import ActivityRuleEngine, load_keys_rules
from mrna_plum.store.database import EventStore


COURSE_CTX_RX = re.compile(
    r"Kurs:\s*(?P<course_code>[A-Z]{1,3}/[A-Za-z]{1,6}/[A-Za-z0-9_]+/(?P<semester>\d+sem)-(?P<name>.+?)-(?P<ay>\d{4}/\d{2})(?P<term>[zl]))",
    re.IGNORECASE,
)

# fallback, jeśli czas nieparsowalny
def parse_ts_to_utc(s: str) -> Optional[datetime]:
    if not s:
        return None
    s = str(s).strip()
    if not s:
        return None
    # epoch?
    if s.isdigit():
        try:
            x = int(s)
            if x > 10_000_000_000:
                x //= 1000
            return datetime.fromtimestamp(x, tz=timezone.utc)
        except Exception:
            return None
    try:
        ts = pd.to_datetime(s, utc=True, errors="coerce")
        if pd.isna(ts):
            return None
        return ts.to_pydatetime()
    except Exception:
        return None


def parse_course_context(kontekst: str) -> Optional[Dict[str, str]]:
    if not kontekst:
        return None
    m = COURSE_CTX_RX.search(kontekst)
    if not m:
        return None
    gd = m.groupdict()
    course_code = gd["course_code"]
    parts = course_code.split("/")
    # WF/An/stj/5sem-...
    wydzial = parts[0] if len(parts) > 0 else ""
    kierunek = parts[1] if len(parts) > 1 else ""
    track = parts[2] if len(parts) > 2 else ""

    return {
        "course_code": course_code,
        "wydzial_code": wydzial,
        "kierunek_code": kierunek,
        "track_code": track,
        "semester_code": gd.get("semester", ""),
        "course_name": gd.get("name", "").strip(),
        "ay": gd.get("ay", ""),
        "term": gd.get("term", "").lower(),
    }


def ensure_run_paths(root: Path) -> Dict[str, Path]:
    run_dir = root / "_run"
    run_dir.mkdir(parents=True, exist_ok=True)
    return {
        "run_dir": run_dir,
        "run_log": run_dir / "run.log",
        "progress": run_dir / "progress.jsonl",
        "ok": run_dir / "parse.ok",
    }


class RunLogger:
    def __init__(self, run_log: Path, progress: Path):
        self.run_log = run_log
        self.progress = progress

    def log(self, msg: str) -> None:
        line = f"{datetime.now().isoformat(timespec='seconds')} {msg}\n"
        self.run_log.open("a", encoding="utf-8").write(line)

    def progress_event(self, obj: Dict[str, Any]) -> None:
        self.progress.open("a", encoding="utf-8").write(json.dumps(obj, ensure_ascii=False) + "\n")


def _cfg_filters(cfg: Dict[str, Any]) -> Dict[str, Any]:
    f = cfg.get("filters", {}) or {}
    parse_cfg = cfg.get("parse_events", {}) or {}
    # dopuszczamy override w parse_events.*
    return {**f, **(parse_cfg.get("filters") or {})}


def _is_student_email(payload: Dict[str, Any], student_domain: str) -> bool:
    # payload może mieć różne kolumny; przeszukaj wszystkie wartości z '@'
    domain = student_domain.lower().strip()
    if not domain:
        return False
    blob = " ".join([str(v) for v in payload.values() if v is not None])
    return domain in blob.lower()


def _source_allowed(payload: Dict[str, Any], allowed_sources: List[str], blocked_sources: List[str]) -> bool:
    src = str(payload.get("Źródło") or payload.get("Zrodlo") or payload.get("Source") or "").strip()
    if blocked_sources and any(b.lower() in src.lower() for b in blocked_sources):
        return False
    if allowed_sources:
        return any(a.lower() in src.lower() for a in allowed_sources)
    return True


def _techkey_allowed(tech_key: str, wl: List[str], bl: List[str]) -> bool:
    if bl and tech_key in bl:
        return False
    if wl and tech_key not in wl:
        return False
    return True


def _date_allowed(ts: datetime, date_from: Optional[str], date_to: Optional[str]) -> bool:
    if date_from:
        d1 = pd.to_datetime(date_from, utc=True, errors="coerce")
        if not pd.isna(d1) and ts < d1.to_pydatetime():
            return False
    if date_to:
        d2 = pd.to_datetime(date_to, utc=True, errors="coerce")
        if not pd.isna(d2) and ts > d2.to_pydatetime():
            return False
    return True


def run_parse_events(
    cfg: Dict[str, Any],
    root: str,
    keys_xlsx_override: Optional[str] = None,
) -> int:
    """
    Exit codes:
      0 = OK
      2 = mixed periods
      1 = other error
    """
    root_p = Path(root)
    paths = ensure_run_paths(root_p)
    logger = RunLogger(paths["run_log"], paths["progress"])

    try:
        logger.log("[PARSE] start parse-events")
        _cfg_with_root = dict(cfg._data) if hasattr(cfg, "_data") else dict(cfg)
        _cfg_with_root["_root"] = root
        store = EventStore(_cfg_with_root)
        store.ensure_schema()

        # KEYS
        keys_cfg = cfg.get("parse_events", {}) or {}

        # 1️ priorytet: CLI override
        keys_xlsx = keys_xlsx_override

        # 2️ fallback: config.yaml
        if not keys_xlsx:
            keys_xlsx = keys_cfg.get("keys_xlsx")

        if not keys_xlsx:
            raise ValueError(
                "Brak KEYS: podaj --keys-xlsx lub ustaw parse_events.keys_xlsx w config.yaml"
            )

        # jeśli w config używasz {root}
        if "{root}" in keys_xlsx:
            keys_xlsx = keys_xlsx.replace("{root}", root)

        keys_sheet = keys_cfg.get("keys_sheet", "KEYS")
        rules = load_keys_rules(keys_xlsx, sheet_name=keys_sheet)
        engine = ActivityRuleEngine(rules, drop_mode_nie=True)
        logger.log(f"[PARSE] loaded KEYS rules: {len(rules)}")

        # filtry
        fil = _cfg_filters(cfg)
        student_domain = (fil.get("student_email_domain") or "@student.umw.edu.pl").lower()
        tech_wl = fil.get("tech_key_whitelist") or []
        tech_bl = fil.get("tech_key_blacklist") or []
        allowed_sources = fil.get("source_whitelist") or []
        blocked_sources = fil.get("source_blacklist") or []
        date_from = fil.get("date_from")
        date_to = fil.get("date_to")

        # DuckDB streaming z events_raw
        import duckdb
        con = duckdb.connect(str((Path(root_p) / cfg["paths"]["db_path"]).resolve()))
        con.execute("PRAGMA enable_progress_bar=false;")

        # incremental: bierz tylko te, których row_key nie ma w canonical_raw
        query = """
            SELECT course, time_text, time_ts_iso, row_key, payload_json, source_file
            FROM events_raw r
            WHERE NOT EXISTS (SELECT 1 FROM events_canonical_raw c WHERE c.row_key = r.row_key)
        """
        cur = con.execute(query)

        batch: List[Dict[str, Any]] = []
        conf_batch: List[Dict[str, Any]] = []

        seen_period: Optional[Tuple[str, str]] = None  # (ay, term)
        mixed_period = False

        total_read = 0
        total_matched = 0
        total_inserted = 0

        FETCH = int(keys_cfg.get("fetch_size", 5000))
        INSERT_BATCH = int(keys_cfg.get("insert_batch_size", 20000))

        while True:
            rows = cur.fetchmany(FETCH)
            if not rows:
                break

            for course, time_text, time_ts_iso, row_key, payload_json, source_file in rows:
                total_read += 1

                # payload
                try:
                    payload = json.loads(payload_json)
                except Exception:
                    continue

                # źródło filter
                if not _source_allowed(payload, allowed_sources, blocked_sources):
                    continue

                # student filter
                if student_domain and _is_student_email(payload, student_domain):
                    continue

                # czas
                ts = None
                if time_ts_iso:
                    try:
                        ts = pd.to_datetime(time_ts_iso, utc=True, errors="coerce")
                        ts = None if pd.isna(ts) else ts.to_pydatetime()
                    except Exception:
                        ts = None
                if ts is None:
                    ts = parse_ts_to_utc(str(payload.get("Czas") or payload.get("Time") or payload.get("Date") or time_text or ""))
                if ts is None:
                    continue

                if not _date_allowed(ts, date_from, date_to):
                    continue

                # kontekst kursu
                kontekst = str(payload.get("Kontekst zdarzenia") or payload.get("Event context") or "")
                ctx = parse_course_context(kontekst)
                if not ctx:
                    continue

                # okres: ay+term
                period = (ctx["ay"], ctx["term"])
                if seen_period is None:
                    seen_period = period
                elif period != seen_period:
                    mixed_period = True
                    # logujemy i kończymy po batchu
                    logger.log(f"[PARSE][ERR] mixed periods: first={seen_period} next={period} row_key={row_key}")
                    break

                # opis (KEYS dopasowanie)
                opis = str(payload.get("Opis") or payload.get("Description") or "")
                m = engine.match(opis)
                if not m:
                    continue  # aktywności spoza KEYS → pomijamy

                # whitelist/blacklist tech_key
                if not _techkey_allowed(m.tech_key, tech_wl, tech_bl):
                    continue

                total_matched += 1

                if m.conflict:
                    conf_batch.append(
                        {
                            "row_key": row_key,
                            "course_code": ctx["course_code"],
                            "teacher_id": m.teacher_id,
                            "tech_key": m.tech_key,
                            "operation": m.operation,
                            "object_id": m.object_id,
                            "note": f"KEYS conflict: multiple matches with same priority={m.priority}",
                        }
                    )

                row = {
                    "row_key": row_key,
                    "course": course,
                    "course_code": ctx["course_code"],
                    "wydzial_code": ctx["wydzial_code"],
                    "kierunek_code": ctx["kierunek_code"],
                    "track_code": ctx["track_code"],
                    "semester_code": ctx["semester_code"],
                    "course_name": ctx["course_name"],
                    "ay": ctx["ay"],
                    "term": ctx["term"],
                    "ts_utc": ts,
                    "teacher_id": m.teacher_id,
                    "operation": m.operation,
                    "tech_key": m.tech_key,
                    "activity_label": m.activity_label,
                    "object_id": m.object_id,
                    "count_mode": m.count_mode,
                    "raw_line_hash": row_key,  # row_key już jest hashem całego wiersza po trim
                    "source_file": source_file,
                    "payload_json": payload_json,
                }
                batch.append(row)

                if len(batch) >= INSERT_BATCH:
                    total_inserted += store.insert_raw_batch(batch)
                    batch.clear()
                if len(conf_batch) >= 2000:
                    store.insert_conflicts_batch(conf_batch)
                    conf_batch.clear()

            if mixed_period:
                break

            # progress co chunk
            if total_read % (FETCH * 2) == 0:
                logger.progress_event(
                    {
                        "stage": "parse-events",
                        "read": total_read,
                        "matched": total_matched,
                        "inserted_raw": total_inserted,
                        "period": {"ay": seen_period[0], "term": seen_period[1]} if seen_period else None,
                    }
                )

        if batch:
            total_inserted += store.insert_raw_batch(batch)
            batch.clear()
        if conf_batch:
            store.insert_conflicts_batch(conf_batch)
            conf_batch.clear()

        con.close()

        if mixed_period:
            logger.log("[PARSE][ERR] mixed periods -> abort")
            return 2

        # finalize counted/unieważnienia
        store.finalize_canonical()
        pq = store.export_parquet() if (cfg.get("parse_events", {}) or {}).get("export_parquet", False) else None

        paths["ok"].write_text("OK\n", encoding="utf-8")
        logger.log(f"[PARSE] OK read={total_read} matched={total_matched} inserted_raw={total_inserted} parquet={pq}")
        logger.progress_event(
            {
                "stage": "parse-events",
                "status": "ok",
                "read": total_read,
                "matched": total_matched,
                "inserted_raw": total_inserted,
                "period": {"ay": seen_period[0], "term": seen_period[1]} if seen_period else None,
            }
        )
        return 0

    except Exception as e:
        logger.log(f"[PARSE][ERR] {type(e).__name__}: {e}")
        return 1
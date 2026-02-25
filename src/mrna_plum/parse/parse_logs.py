from __future__ import annotations
from pathlib import Path
import pandas as pd

from mrna_plum.config import AppConfig
from mrna_plum.errors import MixedPeriodsError, ProcessingError
from mrna_plum.rules.engine import match_best_rule
from .context import parse_context

def parse_merged_parquet(
    parquet_in: Path,
    parquet_out: Path,
    config: AppConfig,
    rules: list,
) -> tuple[int, str | None]:
    df = pd.read_parquet(parquet_in)
    if df.empty:
        parquet_out.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(parquet_out, index=False)
        return 0, None

    # wymagane kolumny
    need = [config.col_time, config.col_context, config.col_desc, config.col_component, config.col_event_name]
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise ProcessingError(f"Missing required CSV columns: {missing}")

    # wyciągnij kontekst (kurs, okres)
    ctx = df[config.col_context].astype(str).apply(lambda x: parse_context(x, config.course_regex, config.period_regex))
    df["course_code"] = ctx.apply(lambda c: c.course_code)
    df["period"] = ctx.apply(lambda c: c.period)

    # mixed periods check (ignorujemy None)
    periods = sorted({p for p in df["period"].dropna().unique().tolist() if str(p).strip() != ""})
    if len(periods) > 1:
        raise MixedPeriodsError(f"mixed periods detected: {periods}")
    run_period = periods[0] if periods else None

    # dopasuj regułę po opisie
    tech_keys = []
    activities = []
    operations = []
    count_flags = []
    teacher_ids = []
    object_ids = []
    matched_prio = []

    for desc in df[config.col_desc].astype(str).tolist():
        m = match_best_rule(desc, rules)
        if m is None:
            tech_keys.append(None)
            activities.append(None)
            operations.append(None)
            count_flags.append(False)
            teacher_ids.append(None)
            object_ids.append(None)
            matched_prio.append(None)
        else:
            tech_keys.append(m.tech_key)
            activities.append(m.activity)
            operations.append(m.operation)
            count_flags.append(bool(m.count_to_report))
            teacher_ids.append(m.teacher_id)
            object_ids.append(m.object_id)
            matched_prio.append(m.priority)

    df["tech_key"] = tech_keys
    df["activity"] = activities
    df["operation"] = operations
    df["count_to_report"] = count_flags
    df["teacher_id"] = teacher_ids
    df["object_id"] = object_ids
    df["rule_priority"] = matched_prio

    parquet_out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(parquet_out, index=False)
    return len(df), run_period

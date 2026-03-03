from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
import re
import pandas as pd

from .models import Rule

@dataclass(frozen=True)
class MatchResult:
    tech_key: str
    activity: str
    operation: str
    count_to_report: bool
    teacher_id: Optional[str]
    object_id: Optional[str]
    priority: int

def compile_rules(keys_df: pd.DataFrame) -> list[Rule]:
    rules: list[Rule] = []

    for _, r in keys_df.iterrows():
        def _p(s: object) -> str:
            return "" if s is None else str(s)

        match_rx = re.compile(_p(r["REGEX_DOPASOWANIA_(Opis)"]))

        user_rx_s = _p(r["REGEX_USER_ID_(Opis)"]).strip()
        user_rx = re.compile(user_rx_s) if user_rx_s else None

        # zamiast kompilować regex, traktujemy to jako nazwę grupy
        obj_group = _p(r["REGEX_OBIEKT_ID_(z dopasowania)"]).strip() or None

        count_flag = _p(r["LICZYC_DO_RAPORTU"]).upper() in ("TAK", "1", "TRUE", "YES")

        rules.append(
            Rule(
                activity=_p(r["AKTYWNOSC"]).strip(),
                tech_key=_p(r["KLUCZ_TECHNICZNY"]).strip(),
                operation=_p(r["OPERACJA"]).strip(),
                count_to_report=count_flag,
                regex_match_desc=match_rx,
                regex_user_id=user_rx,
                regex_object_id=obj_group,  # ← teraz to string z nazwą grupy
                priority=int(r["PRIORYTET"]),
            )
        )

    rules.sort(key=lambda x: x.priority, reverse=True)
    return rules

def match_best_rule(description: str, rules: list[Rule]) -> Optional[MatchResult]:
    for rule in rules:
        m = rule.regex_match_desc.search(description or "")
        if not m:
            continue

        teacher_id = None
        object_id = None

        if rule.regex_user_id:
            um = rule.regex_user_id.search(description or "")
            if um and um.groups():
                teacher_id = um.group(1)
            elif um:
                teacher_id = um.group(0)

        # object_id z osobnego regexa lub pierwszej grupy
        if rule.regex_object_id:
            try:
                obj_m = re.search(rule.regex_object_id, description or "")
                if obj_m and obj_m.groups():
                    object_id = obj_m.group(1)
                elif obj_m:
                    object_id = obj_m.group(0)
            except Exception:
                object_id = None
        else:
            try:
                if m.groups():
                    object_id = m.group(1)
            except Exception:
                object_id = None

        return MatchResult(
            tech_key=rule.tech_key,
            activity=rule.activity,
            operation=rule.operation,
            count_to_report=rule.count_to_report,
            teacher_id=teacher_id,
            object_id=object_id,
            priority=rule.priority,
        )
    return None

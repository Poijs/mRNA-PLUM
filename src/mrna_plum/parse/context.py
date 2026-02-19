from __future__ import annotations
import re
from dataclasses import dataclass
from typing import Optional

@dataclass(frozen=True)
class ContextInfo:
    course_code: Optional[str]
    period: Optional[str]

def parse_context(context: str, course_regex: str, period_regex: str) -> ContextInfo:
    course = None
    period = None

    cm = re.search(course_regex, context or "")
    if cm:
        course = cm.group(1)

    pm = re.search(period_regex, context or "")
    if pm:
        period = pm.group(1)

    return ContextInfo(course_code=course, period=period)

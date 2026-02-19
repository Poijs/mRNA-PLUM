from __future__ import annotations
from dataclasses import dataclass
import re
from typing import Optional

@dataclass(frozen=True)
class Rule:
    activity: str
    tech_key: str
    operation: str          # np. TAK / TAK_FLAG / NIE / etc.
    count_to_report: bool
    regex_match_desc: re.Pattern
    regex_user_id: Optional[re.Pattern]
    regex_object_id: Optional[re.Pattern]
    priority: int

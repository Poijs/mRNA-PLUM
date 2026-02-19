from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import json
from pathlib import Path
from typing import Any, Optional

WARSAW = ZoneInfo("Europe/Warsaw")

@dataclass
class ProgressWriter:
    path: Path

    def emit(
        self,
        step: str,
        status: str,
        message: str,
        current: Optional[int] = None,
        total: Optional[int] = None,
        extra: Optional[dict[str, Any]] = None,
    ) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "ts": datetime.now(WARSAW).isoformat(),
            "step": step,
            "status": status,     # start|progress|done|error
            "message": message,
            "current": current,
            "total": total,
            "extra": extra or {},
        }
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

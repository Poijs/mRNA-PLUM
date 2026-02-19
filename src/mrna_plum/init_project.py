from __future__ import annotations
from pathlib import Path

DEFAULT_DIRS = [
    "IN/logs",
    "IN/activities",
    "OUT/logs",
    "OUT/db",
    "OUT/merged",
    "OUT/excel",
    "OUT/individual",
    "OUT/pdf",
]

def init_project(root: str | Path) -> list[Path]:
    root = Path(root).resolve()
    created: list[Path] = []
    for rel in DEFAULT_DIRS:
        p = root / rel
        if not p.exists():
            p.mkdir(parents=True, exist_ok=True)
            created.append(p)
    return created

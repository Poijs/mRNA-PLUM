from __future__ import annotations
from pathlib import Path
from typing import Any

def cfg_get(cfg: dict, key: str, default: Any = None) -> Any:
    cur: Any = cfg
    for part in key.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur

def cfg_str(cfg: dict, key: str, default: str | None = None) -> str | None:
    v = cfg_get(cfg, key, default)
    return None if v is None else str(v)

def cfg_int(cfg: dict, key: str, default: int | None = None) -> int | None:
    v = cfg_get(cfg, key, default)
    return None if v is None else int(v)

def cfg_bool(cfg: dict, key: str, default: bool = False) -> bool:
    v = cfg_get(cfg, key, default)
    return bool(v)

def cfg_path(root: Path, cfg: dict, key: str, default_rel: str | None = None) -> Path | None:
    v = cfg_get(cfg, key, default_rel)
    if v is None or str(v).strip() == "":
        return None
    p = Path(str(v))
    return p if p.is_absolute() else (root / p).resolve()
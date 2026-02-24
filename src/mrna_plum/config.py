from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

import yaml

from .errors import ConfigError


# Jeśli AppConfig już masz w tym pliku, NIE dubluj tej klasy.
# Ten blok zostaw tylko jeśli AppConfig nie istnieje.
@dataclass(frozen=True)
class AppConfig:
    input_glob: str = "**/*.csv"
    keys_workbook: str = "mRNA_PLUM_Form.xlsx"
    keys_sheet: str = "KEYS"
    col_time: str = "Czas"
    col_context: str = "Kontekst zdarzenia"
    col_desc: str = "Opis"
    col_component: str = "Składnik"
    col_event_name: str = "Nazwa zdarzenia"
    course_regex: str = r"Kurs:\s*([^\s]+)"
    period_regex: str = r"(\d{4}/\d{2}[zl])"
    chunk_rows: int = 200_000


def load_config_dict(path: Path) -> Dict[str, Any]:
    """
    Jedno źródło prawdy: YAML -> dict.
    Reszta projektu (stats/reports/activities_state) używa dict.
    """
    p = Path(path).resolve()
    if not p.exists():
        raise ConfigError(f"Config file not found: {p}")

    try:
        text = p.read_text(encoding="utf-8")
    except Exception as e:
        raise ConfigError(f"Cannot read config file: {p}. {e}") from e

    try:
        data = yaml.safe_load(text) or {}
    except Exception as e:
        raise ConfigError(f"Invalid YAML in config: {p}. {e}") from e

    if not isinstance(data, dict):
        raise ConfigError(f"Config root must be a mapping (dict). Got: {type(data).__name__}")

    return data


def load_app_config(path: Path) -> AppConfig:
    """
    CSV layer: merge/parse używa AppConfig.
    Bierzemy dane z sekcji `csv:` w YAML (albo fallback: root dict dla kompatybilności).
    """
    cfg = load_config_dict(path)

    # preferuj sekcję `csv:`, ale pozwól na kompatybilność wstecz
    csv_section = cfg.get("csv")
    if csv_section is None:
        csv_section = cfg
    if not isinstance(csv_section, dict):
        raise ConfigError("Config key `csv` must be a mapping (dict).")

    try:
        return AppConfig(**csv_section)
    except TypeError as e:
        raise ConfigError(f"Invalid `csv` section fields for AppConfig: {e}") from e
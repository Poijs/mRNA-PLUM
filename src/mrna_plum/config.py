# src/mrna_plum/config.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from mrna_plum.errors import ConfigError


# ---------------------------------------------------------------------------
# AppConfig — używany przez merge/parse (pipeline A, CSV layer)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Config — wrapper dict + dostęp atrybutowy; używany przez CLI i moduły B
# ---------------------------------------------------------------------------

class Config:
    """
    Jednolity obiekt konfiguracyjny dla całego pipeline'u B (DuckDB).

    Obsługuje dwa style dostępu:
      cfg.input_glob          — atrybuty z AppConfig (CSV layer)
      cfg.get("parse_events") — dict-style dla sekcji YAML
      cfg["hr"]               — item access (opcjonalnie)

    Używaj load_config(path) zamiast tworzyć bezpośrednio.
    """

    def __init__(self, data: Dict[str, Any]) -> None:
        self._data: Dict[str, Any] = data

        # Płaskie pola CSV-layer (kompatybilność z AppConfig)
        _csv: Dict[str, Any] = data.get("csv", data) or data
        if not isinstance(_csv, dict):
            _csv = data

        self.input_glob: str       = str(_csv.get("input_glob", "**/*.csv"))
        self.keys_workbook: str    = str(_csv.get("keys_workbook", "mRNA_PLUM_Form.xlsx"))
        self.keys_sheet: str       = str(_csv.get("keys_sheet", "KEYS"))
        self.col_time: str         = str(_csv.get("col_time", "Czas"))
        self.col_context: str      = str(_csv.get("col_context", "Kontekst zdarzenia"))
        self.col_desc: str         = str(_csv.get("col_desc", "Opis"))
        self.col_component: str    = str(_csv.get("col_component", "Składnik"))
        self.col_event_name: str   = str(_csv.get("col_event_name", "Nazwa zdarzenia"))
        self.course_regex: str     = str(_csv.get("course_regex", r"Kurs:\s*([^\s]+)"))
        self.period_regex: str     = str(_csv.get("period_regex", r"(\d{4}/\d{2}[zl])"))
        self.chunk_rows: int       = int(_csv.get("chunk_rows", 200_000))

    # ------------------------------------------------------------------
    # Dict-style API (używane przez compute_stats, export_excel itp.)
    # ------------------------------------------------------------------

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __contains__(self, key: str) -> bool:
        return key in self._data

    # ------------------------------------------------------------------
    # Fallback atrybutowy: cfg.parse_events -> self._data["parse_events"]
    # Nie nadpisuje jawnie zdefiniowanych atrybutów powyżej.
    # ------------------------------------------------------------------

    def __getattr__(self, name: str) -> Any:
        # Wywoływane tylko gdy normalny lookup zawiedzie.
        # Chronić przed rekursją na _data.
        if name == "_data":
            raise AttributeError(name)
        try:
            return self._data[name]
        except KeyError:
            raise AttributeError(
                f"'Config' object has no attribute '{name}' "
                f"(also not found in config dict)"
            )

    def __repr__(self) -> str:  # czytelny debug
        keys = list(self._data.keys())
        return f"<Config keys={keys}>"


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def load_config_dict(path: Path) -> Dict[str, Any]:
    """
    Surowy YAML -> dict. Używany gdy potrzebujesz czystego słownika.
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


def load_config(path: Path) -> Config:
    """
    Główny loader dla CLI i pipeline'u B.
    Zwraca Config — obsługuje zarówno dostęp atrybutowy jak i dict-style.
    """
    return Config(load_config_dict(path))


def load_app_config(path: Path) -> AppConfig:
    """
    Loader dla pipeline'u A (merge/parse, CSV layer).
    Wymaga sekcji `csv:` w YAML (lub root dict dla kompatybilności wstecznej).
    """
    cfg = load_config_dict(path)
    csv_section = cfg.get("csv") or cfg
    if not isinstance(csv_section, dict):
        raise ConfigError("Config key `csv` must be a mapping (dict).")
    try:
        return AppConfig(**{
            k: v for k, v in csv_section.items()
            if k in AppConfig.__dataclass_fields__
        })
    except TypeError as e:
        raise ConfigError(f"Invalid `csv` section fields for AppConfig: {e}") from e
from dataclasses import dataclass
from pathlib import Path
import yaml
from .errors import ConfigError

@dataclass(frozen=True)
class AppConfig:
    # gdzie szukać logów wejściowych (rekurencyjnie)
    input_glob: str = "**/*.csv"

    # excel z arkuszem KEYS (jeśli pusty -> będzie szukane w root)
    keys_workbook: str = "mRNA_PLUM_Form.xlsx"
    keys_sheet: str = "KEYS"

    # nazwy kolumn w CSV (jeśli różne — dostosuj w config.yaml)
    col_time: str = "Czas"
    col_context: str = "Kontekst zdarzenia"
    col_desc: str = "Opis"
    col_component: str = "Składnik"
    col_event_name: str = "Nazwa zdarzenia"

    # regex kursu/okresu z kontekstu (starter)
    # Kurs: WF/An/stj/5sem-Nazwa-2025/26z
    course_regex: str = r"Kurs:\s*([^\s]+)"
    period_regex: str = r"(\d{4}/\d{2}[zl])"   # np. 2025/26z lub 2025/26l

    # limity / performance
    chunk_rows: int = 200_000  # na przyszłość, gdy będziesz czytał w chunkach

def load_config(config_path: Path) -> AppConfig:
    if not config_path.exists():
        raise ConfigError(f"Config not found: {config_path}")

    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    # pozwalamy na brak kluczy (defaulty z dataclass)
    try:
        return AppConfig(**data)
    except TypeError as e:
        raise ConfigError(f"Invalid config schema: {e}") from e

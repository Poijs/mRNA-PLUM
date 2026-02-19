from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class ProjectPaths:
    root: Path

    @property
    def run_dir(self) -> Path:
        return self.root / "_run"

    @property
    def data_dir(self) -> Path:
        return self.root / "_data"

    @property
    def parquet_dir(self) -> Path:
        return self.data_dir / "parquet"

    @property
    def duckdb_path(self) -> Path:
        return self.data_dir / "mrna_plum.duckdb"

    @property
    def markers_dir(self) -> Path:
        return self.run_dir

    def marker_path(self, step: str) -> Path:
        return self.markers_dir / f"{step}.ok"

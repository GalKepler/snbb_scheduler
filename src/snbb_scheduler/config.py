from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class SchedulerConfig:
    """All path conventions and settings in one place."""

    # Root directories
    dicom_root: Path = field(default_factory=lambda: Path("/data/snbb/dicom"))
    bids_root: Path = field(default_factory=lambda: Path("/data/snbb/bids"))
    derivatives_root: Path = field(default_factory=lambda: Path("/data/snbb/derivatives"))

    # Derivative subdirectories
    qsiprep_dir: str = "qsiprep"
    freesurfer_dir: str = "freesurfer"

    # Slurm settings
    slurm_partition: str = "normal"
    slurm_account: str = "snbb"

    # State tracking
    state_file: Path = field(default_factory=lambda: Path("/data/snbb/.scheduler_state.parquet"))

    @property
    def qsiprep_root(self) -> Path:
        return self.derivatives_root / self.qsiprep_dir

    @property
    def freesurfer_root(self) -> Path:
        return self.derivatives_root / self.freesurfer_dir

    @classmethod
    def from_yaml(cls, path: str | Path) -> "SchedulerConfig":
        """Load config from a YAML file, overriding defaults."""
        with open(path) as f:
            data = yaml.safe_load(f) or {}

        path_fields = {"dicom_root", "bids_root", "derivatives_root", "state_file"}
        for key in path_fields:
            if key in data:
                data[key] = Path(data[key])

        return cls(**data)

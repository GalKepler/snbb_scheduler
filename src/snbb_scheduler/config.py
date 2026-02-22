from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import yaml


@dataclass
class Procedure:
    """Declaration of a single processing procedure."""

    name: str
    output_dir: str  # subdirectory under derivatives_root; empty string for bids (uses bids_root)
    script: str  # sbatch script filename
    scope: Literal["session", "subject"] = "session"
    depends_on: list[str] = field(default_factory=list)
    completion_marker: str | None = None
    # completion_marker semantics:
    #   None          → output directory must exist (non-empty)
    #   "path/file"   → that specific file must exist inside the output dir
    #   "**/*.nii.gz" → at least one file matching the glob must exist


DEFAULT_PROCEDURES: list[Procedure] = [
    Procedure(
        name="bids",
        output_dir="",  # output root is bids_root, not derivatives_root
        script="snbb_run_bids.sh",
        scope="session",
        depends_on=[],
        completion_marker="**/*.nii.gz",
    ),
    Procedure(
        name="qsiprep",
        output_dir="qsiprep",
        script="snbb_run_qsiprep.sh",
        scope="session",
        depends_on=["bids"],
        completion_marker=None,
    ),
    Procedure(
        name="freesurfer",
        output_dir="freesurfer",
        script="snbb_run_freesurfer.sh",
        scope="subject",
        depends_on=["bids"],
        completion_marker="scripts/recon-all.done",
    ),
]


@dataclass
class SchedulerConfig:
    """All path conventions and settings in one place."""

    # Root directories
    dicom_root: Path = field(default_factory=lambda: Path("/data/snbb/dicom"))
    bids_root: Path = field(default_factory=lambda: Path("/data/snbb/bids"))
    derivatives_root: Path = field(default_factory=lambda: Path("/data/snbb/derivatives"))

    # Slurm settings
    slurm_partition: str = "normal"
    slurm_account: str = "snbb"

    # State tracking
    state_file: Path = field(default_factory=lambda: Path("/data/snbb/.scheduler_state.parquet"))

    # Procedure registry — add new procedures here or via YAML
    procedures: list[Procedure] = field(default_factory=lambda: list(DEFAULT_PROCEDURES))

    def get_procedure_root(self, proc: Procedure) -> Path:
        """Return the base output root for a procedure."""
        if proc.name == "bids":
            return self.bids_root
        return self.derivatives_root / proc.output_dir

    def get_procedure(self, name: str) -> Procedure:
        """Look up a procedure by name."""
        for proc in self.procedures:
            if proc.name == name:
                return proc
        raise KeyError(f"Unknown procedure: {name!r}")

    @classmethod
    def from_yaml(cls, path: str | Path) -> "SchedulerConfig":
        """Load config from a YAML file, overriding defaults."""
        with open(path) as f:
            data = yaml.safe_load(f) or {}

        path_fields = {"dicom_root", "bids_root", "derivatives_root", "state_file"}
        for key in path_fields:
            if key in data:
                data[key] = Path(data[key])

        if "procedures" in data:
            data["procedures"] = [Procedure(**p) for p in data["procedures"]]

        return cls(**data)

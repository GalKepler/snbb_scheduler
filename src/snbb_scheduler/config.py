from __future__ import annotations

__all__ = ["Procedure", "DEFAULT_PROCEDURES", "SchedulerConfig"]

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
    completion_marker: str | list[str] | None = None
    # completion_marker semantics:
    #   None          → output directory must exist (non-empty)
    #   "path/file"   → that specific file must exist inside the output dir
    #   "**/*.nii.gz" → at least one file matching the glob must exist
    #   ["pat1", ...] → ALL patterns must match at least one file


DEFAULT_PROCEDURES: list[Procedure] = [
    Procedure(
        name="bids",
        output_dir="",  # output root is bids_root, not derivatives_root
        script="snbb_run_bids.sh",
        scope="session",
        depends_on=[],
        completion_marker=[
            "anat/*_T1w.nii.gz",
            "dwi/*dir-AP*_dwi.nii.gz",
            "dwi/*dir-AP*_dwi.bvec",
            "dwi/*dir-AP*_dwi.bval",
            "fmap/*acq-dwi_dir-AP*epi.nii.gz",
            "fmap/*acq-func_dir-AP*epi.nii.gz",
            "fmap/*acq-func_dir-PA*epi.nii.gz",
            "func/*task-rest_bold.nii.gz",
        ],
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
    slurm_partition: str = "debug"
    slurm_account: str = "snbb"
    slurm_mem: str | None = None           # e.g. "32G"; omitted from sbatch if None
    slurm_cpus_per_task: int | None = None  # e.g. 8; omitted from sbatch if None

    # State tracking
    state_file: Path = field(default_factory=lambda: Path("/data/snbb/.scheduler_state.parquet"))

    # Optional CSV for session discovery (subject_code, session_id, ScanID).
    # When set, filesystem scanning is skipped.
    sessions_file: Path | None = field(default=None)

    # Procedure registry — add new procedures here or via YAML
    procedures: list[Procedure] = field(default_factory=lambda: list(DEFAULT_PROCEDURES))

    def __post_init__(self) -> None:
        """Validate that all ``depends_on`` entries reference known procedures.

        Raises
        ------
        ValueError
            If any procedure's ``depends_on`` list contains a name that does
            not match another procedure in this config.
        """
        known = {p.name for p in self.procedures}
        for proc in self.procedures:
            for dep in proc.depends_on:
                if dep not in known:
                    raise ValueError(
                        f"Procedure {proc.name!r} depends on {dep!r}, which is not "
                        f"in the procedures list. Known procedures: {sorted(known)}"
                    )

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
        """Load config from a YAML file, overriding defaults.

        Raises
        ------
        ValueError
            If the file contains invalid YAML syntax.
        FileNotFoundError
            If *path* does not exist.
        """
        with open(path) as f:
            try:
                data = yaml.safe_load(f) or {}
            except yaml.YAMLError as exc:
                raise ValueError(f"Invalid YAML in {path}: {exc}") from exc

        path_fields = {"dicom_root", "bids_root", "derivatives_root", "state_file", "sessions_file"}
        for key in path_fields:
            if data.get(key) is not None:
                data[key] = Path(data[key])

        if "procedures" in data:
            data["procedures"] = [Procedure(**p) for p in data["procedures"]]

        return cls(**data)

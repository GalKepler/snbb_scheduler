# snbb_scheduler

A lightweight, rule-based scheduler for the Stichting Netherlands Brain Bank (SNBB) neuroimaging processing pipeline. It performs a daily sweep of all sessions, evaluates which processing steps are needed, and submits jobs to Slurm.

## Overview

The brain bank receives MRI sessions that need to pass through a sequence of processing steps: BIDS conversion, qsiprep, freesurfer, and potentially others. Each step has preconditions (e.g., DICOM data must exist) and produces outputs that can be checked for completeness.

`snbb_scheduler` runs as a periodic job (e.g., daily via cron). On each run it:

1. Scans the filesystem to build a DataFrame of all sessions/subjects
2. Evaluates a set of declarative rules to determine which (session, procedure) pairs need processing
3. Produces a task manifest (a table of what needs to run)
4. Submits the tasks to Slurm (or prints a dry-run report)

## Directory Structure

```
snbb_scheduler/
├── pyproject.toml
├── README.md
├── src/
│   └── snbb_scheduler/
│       ├── __init__.py
│       ├── config.py          # Configuration and path conventions
│       ├── sessions.py        # Scan filesystem, build session DataFrame
│       ├── checks.py          # Completion check functions
│       ├── rules.py           # Rule definitions per procedure
│       ├── manifest.py        # Build task manifest from rules
│       ├── submit.py          # Slurm submission logic
│       └── cli.py             # CLI entry point
└── tests/
    ├── conftest.py            # Shared fixtures (fake directory trees)
    ├── test_sessions.py
    ├── test_checks.py
    ├── test_rules.py
    └── test_manifest.py
```

## Configuration (`config.py`)

A single configuration dataclass or dictionary that defines all path conventions. This is the only place paths are defined. Example:

```python
from dataclasses import dataclass, field
from pathlib import Path

@dataclass
class SchedulerConfig:
    """All path conventions and settings in one place."""
    # Root directories
    dicom_root: Path = Path("/data/snbb/dicom")
    bids_root: Path = Path("/data/snbb/bids")
    derivatives_root: Path = Path("/data/snbb/derivatives")

    # Derivative subdirectories
    qsiprep_dir: str = "qsiprep"
    freesurfer_dir: str = "freesurfer"

    # Slurm settings
    slurm_partition: str = "debug"
    slurm_account: str = "snbb"

    # State tracking
    state_file: Path = Path("/data/snbb/.scheduler_state.parquet")

    @property
    def qsiprep_root(self) -> Path:
        return self.derivatives_root / self.qsiprep_dir

    @property
    def freesurfer_root(self) -> Path:
        return self.derivatives_root / self.freesurfer_dir
```

The config should be loadable from a YAML file as well, so users can override defaults without editing code. Use a simple `from_yaml(path)` classmethod.

## Session Discovery (`sessions.py`)

Scans the DICOM root directory to discover all available sessions. The SNBB directory layout is:

```
dicom_root/
├── sub-0001/
│   ├── ses-01/
│   │   └── *.dcm (or subdirectories with DICOMs)
│   └── ses-02/
│       └── ...
├── sub-0002/
│   └── ses-01/
│       └── ...
```

The module should:

- Walk `dicom_root` and identify subject/session pairs
- Return a pandas DataFrame with columns: `subject`, `session`, `dicom_path`
- Enrich the DataFrame with paths to expected outputs:
  - `bids_path`: `bids_root / subject / session`
  - `qsiprep_path`: `qsiprep_root / subject / session`
  - `freesurfer_path`: `freesurfer_root / subject`  (freesurfer is per-subject, not per-session)
- Add boolean columns for existence of each path (e.g., `dicom_exists`, `bids_exists`, etc.)

```python
def discover_sessions(config: SchedulerConfig) -> pd.DataFrame:
    """Scan filesystem and return DataFrame of all sessions with path info."""
    ...
```

## Completion Checks (`checks.py`)

Functions that determine whether a processing step has been completed successfully for a given session. Each check returns a boolean.

```python
def bids_complete(bids_path: Path) -> bool:
    """
    Check if BIDS conversion is complete.
    Returns True if the BIDS directory exists and contains
    the expected modality subdirectories (anat/, dwi/, func/ etc.)
    with at least one NIfTI file each.
    """
    ...

def qsiprep_complete(qsiprep_path: Path) -> bool:
    """
    Check if qsiprep has completed successfully.
    Look for expected output files (e.g., preprocessed DWI,
    or a known completion marker file).
    """
    ...

def freesurfer_complete(freesurfer_path: Path) -> bool:
    """
    Check if FreeSurfer recon-all has completed.
    Check for the presence of the 'scripts/recon-all.done' file
    in the subject's FreeSurfer directory.
    """
    ...
```

These checks should be conservative — if in doubt, report incomplete so the procedure gets re-run. Each check should handle missing directories gracefully (missing = incomplete).

## Rules (`rules.py`)

Each rule is a function that takes a session row (a pandas Series or a dataclass) and returns `True` if that procedure should be run for that session.

Rules encode the dependency logic:

```python
from typing import Callable

# Type alias for a rule function
Rule = Callable[[pd.Series], bool]

def needs_bids(row: pd.Series) -> bool:
    """BIDS conversion needed if DICOMs exist but BIDS is not complete."""
    return row["dicom_exists"] and not bids_complete(row["bids_path"])

def needs_qsiprep(row: pd.Series) -> bool:
    """qsiprep needed if BIDS is complete but qsiprep is not."""
    return bids_complete(row["bids_path"]) and not qsiprep_complete(row["qsiprep_path"])

def needs_freesurfer(row: pd.Series) -> bool:
    """FreeSurfer needed if BIDS is complete but FreeSurfer is not."""
    return bids_complete(row["bids_path"]) and not freesurfer_complete(row["freesurfer_path"])

# Registry of all rules, in dependency order
RULES: dict[str, Rule] = {
    "bids": needs_bids,
    "qsiprep": needs_qsiprep,
    "freesurfer": needs_freesurfer,
}
```

New procedures are added by writing a check function and a rule function, then adding to the `RULES` dict. Nothing else needs to change.

## Task Manifest (`manifest.py`)

Applies all rules to the session DataFrame to produce a task manifest — a DataFrame of `(subject, session, procedure)` tuples that need to be executed.

```python
def build_manifest(sessions: pd.DataFrame, rules: dict[str, Rule] | None = None) -> pd.DataFrame:
    """
    Evaluate rules against all sessions.

    Returns a DataFrame with columns:
        subject, session, procedure, dicom_path, priority
    """
    ...
```

The manifest should also respect **in-flight tracking**: if a job is already running or pending for a (session, procedure) pair, it should be excluded. This is managed via a state file (parquet) with columns: `subject, session, procedure, status, submitted_at, job_id`.

Statuses: `pending`, `running`, `complete`, `failed`.

```python
def load_state(config: SchedulerConfig) -> pd.DataFrame:
    """Load the state file. Returns empty DataFrame if it doesn't exist."""
    ...

def save_state(state: pd.DataFrame, config: SchedulerConfig) -> None:
    """Save updated state."""
    ...

def filter_in_flight(manifest: pd.DataFrame, state: pd.DataFrame) -> pd.DataFrame:
    """Remove tasks that are already pending or running."""
    ...
```

## Slurm Submission (`submit.py`)

Takes the filtered task manifest and submits each task as a Slurm job. Each procedure maps to a specific Slurm command/script.

```python
import subprocess

# Map procedure names to their Slurm submission commands/scripts
PROCEDURE_COMMANDS: dict[str, str] = {
    "bids": "snbb_run_bids.sh",
    "qsiprep": "snbb_run_qsiprep.sh",
    "freesurfer": "snbb_run_freesurfer.sh",
}

def submit_task(row: pd.Series, config: SchedulerConfig, dry_run: bool = False) -> str | None:
    """
    Submit a single task to Slurm via sbatch.
    Returns the Slurm job ID, or None if dry_run.
    """
    cmd = [
        "sbatch",
        f"--partition={config.slurm_partition}",
        f"--account={config.slurm_account}",
        f"--job-name={row['procedure']}_{row['subject']}_{row['session']}",
        PROCEDURE_COMMANDS[row["procedure"]],
        row["subject"],
        row["session"],
    ]

    if dry_run:
        print(f"[DRY RUN] Would submit: {' '.join(cmd)}")
        return None

    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    # Parse job ID from sbatch output: "Submitted batch job 12345"
    job_id = result.stdout.strip().split()[-1]
    return job_id

def submit_manifest(manifest: pd.DataFrame, config: SchedulerConfig, dry_run: bool = False) -> pd.DataFrame:
    """
    Submit all tasks in the manifest.
    Returns updated state DataFrame with new job entries.
    """
    ...
```

The module should NOT define the actual processing scripts (e.g., `snbb_run_bids.sh`). Those are external and already exist or will be written separately. This module only handles submission.

## CLI (`cli.py`)

Uses `click` or `argparse` for the CLI. Exposed as `snbb-scheduler` via pyproject.toml entry point.

### Commands

```bash
# Full run: discover sessions, evaluate rules, submit to Slurm
snbb-scheduler run

# Dry run: same as above but only print what would be submitted
snbb-scheduler run --dry-run

# Show the current task manifest without submitting
snbb-scheduler manifest

# Show current state (pending/running/complete/failed jobs)
snbb-scheduler status

# Reset failed jobs so they can be retried on next run
snbb-scheduler retry --procedure bids --subject sub-0001

# Use a custom config file
snbb-scheduler run --config /path/to/config.yaml
```

### Entry point in `pyproject.toml`

```toml
[project.scripts]
snbb-scheduler = "snbb_scheduler.cli:main"
```

## pyproject.toml

```toml
[build-system]
requires = ["setuptools>=68.0"]
build-backend = "setuptools.backends._legacy:_Backend"

[project]
name = "snbb-scheduler"
version = "0.1.0"
description = "Rule-based scheduler for SNBB neuroimaging processing pipeline"
requires-python = ">=3.10"
dependencies = [
    "pandas>=2.0",
    "click>=8.0",
    "pyyaml>=6.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.0",
    "pytest-cov",
]

[project.scripts]
snbb-scheduler = "snbb_scheduler.cli:main"

[tool.setuptools.packages.find]
where = ["src"]
```

## Testing Strategy

Tests should use `tmp_path` fixtures to create fake directory trees and validate that:

- `sessions.py` correctly discovers subjects/sessions from a directory structure
- `checks.py` returns correct True/False for complete/incomplete directories
- `rules.py` correctly identifies what needs processing
- `manifest.py` produces the right task list and filters in-flight jobs
- `submit.py` constructs correct sbatch commands (mock `subprocess.run`)

Example fixture in `conftest.py`:

```python
import pytest
from pathlib import Path

@pytest.fixture
def fake_data_dir(tmp_path):
    """Create a minimal fake SNBB directory tree."""
    # DICOM exists for sub-0001/ses-01
    dicom = tmp_path / "dicom" / "sub-0001" / "ses-01"
    dicom.mkdir(parents=True)
    (dicom / "file.dcm").touch()

    # BIDS complete for sub-0001/ses-01
    bids = tmp_path / "bids" / "sub-0001" / "ses-01" / "anat"
    bids.mkdir(parents=True)
    (bids / "sub-0001_ses-01_T1w.nii.gz").touch()

    # DICOM exists for sub-0002/ses-01 but no BIDS
    dicom2 = tmp_path / "dicom" / "sub-0002" / "ses-01"
    dicom2.mkdir(parents=True)
    (dicom2 / "file.dcm").touch()

    return tmp_path
```

## Design Principles

1. **Declarative rules**: Adding a new procedure means writing one check function and one rule function. No other code changes needed.
2. **Filesystem as source of truth**: No database required. The directory structure and state file are the only persistent state.
3. **Safe by default**: Dry-run mode is always available. The scheduler never deletes data. In-flight tracking prevents duplicate submissions.
4. **Minimal dependencies**: Only pandas, click, and pyyaml. No heavy frameworks.
5. **Idempotent**: Running the scheduler twice in a row should produce the same result (second run submits nothing if first run's jobs are tracked).

## Future Extensions (not in scope for v1)

- Slurm job dependency chaining (`--dependency=afterok:JOBID`) so BIDS → qsiprep can happen in the same day
- Email/Slack notifications on failures
- Web dashboard showing pipeline status
- Integration with SNBB's existing database systems

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`snbb_scheduler` is a rule-based scheduler for the Stichting Netherlands Brain Bank (SNBB) neuroimaging pipeline. It performs a daily sweep of all MRI sessions, evaluates which processing steps are needed, and submits jobs to Slurm. The full specification is in `snbb_scheduler_spec.md`.

## Commands

```bash
# Install in editable mode with dev dependencies
pip install -e ".[dev]"

# Run the CLI
snbb-scheduler run
snbb-scheduler run --dry-run
snbb-scheduler run --skip-monitor        # skip sacct state refresh
snbb-scheduler manifest
snbb-scheduler status
snbb-scheduler monitor                   # poll sacct and refresh in-flight job states
snbb-scheduler retry --procedure bids --subject sub-0001
snbb-scheduler run --config /path/to/config.yaml
snbb-scheduler run --slurm-log-dir /data/logs/slurm

# Run all tests
pytest

# Run a single test file
pytest tests/test_sessions.py

# Run with coverage
pytest --cov=snbb_scheduler
```

## Architecture

The pipeline is a linear data flow: **discover → monitor → evaluate → filter → submit → audit**.

```
sessions.py  →  rules.py  →  manifest.py  →  submit.py
(DataFrame)     (Rule fns)   (task table)    (sbatch)
                                  ↑
                            monitor.py        audit.py
                           (sacct poll)    (JSONL log)
```

### Key design constraints

- **`config.py` is the only place paths are defined.** All modules receive a `SchedulerConfig` instance; never hardcode paths elsewhere.
- **Rules are declarative.** Adding a new procedure requires only: (1) a check function in `checks.py`, (2) a rule function in `rules.py`, (3) an entry in `RULES` dict, and (4) a command in `submit.py`'s `PROCEDURE_COMMANDS`. Nothing else changes.
- **Filesystem is source of truth.** No database. State is tracked in a single parquet file (`state_file` in config) with columns: `subject, session, procedure, status, submitted_at, job_id`. Statuses: `pending`, `running`, `complete`, `failed`.
- **In-flight deduplication**: `manifest.py`'s `filter_in_flight()` removes tasks already `pending` or `running` from the state file before submission.
- **Checks are conservative**: if in doubt, report incomplete so the procedure gets re-run.
- **Scripts call real tools directly.** All `scripts/snbb_run_*.sh` invoke the actual neuroimaging tools (heudiconv, QSIPrep, QSIRecon via Apptainer; recon-all via `snbb_recon_all_helper.py`). No voxelops dependency.

### FreeSurfer is per-subject, not per-session

All other derivatives (`bids_path`, `qsiprep_path`) are keyed on `(subject, session)`. FreeSurfer (`freesurfer_path`) is keyed only on `subject`. This asymmetry must be preserved throughout the DataFrame columns.

### `checks.py` completion criteria

- `bids_complete`: directory exists **and** contains modality subdirs with at least one `.nii.gz`
- `qsiprep_complete`: check for expected output files or a known completion marker
- `freesurfer_complete`: presence of `scripts/recon-all.done` inside the subject's FreeSurfer dir

### `audit.py` — JSONL audit log

`AuditLogger` appends one JSON object per line to the audit log file. Events:
- `submitted` — sbatch accepted the job
- `error` — sbatch raised `CalledProcessError`
- `dry_run` — would-submit event in dry-run mode
- `retry_cleared` — failed state row removed by `retry` command
- `status_change` — sacct reported a new state for an in-flight job

`get_logger(config)` returns an `AuditLogger` pointing at `config.log_file` (or `<state_dir>/scheduler_audit.jsonl`).

### `monitor.py` — Slurm job state polling

`poll_jobs(job_ids)` calls `sacct --parsable2 --noheader` and maps sacct states to scheduler statuses. Gracefully returns `{}` if sacct is unavailable.

`update_state_from_sacct(state, audit)` finds all `pending`/`running` rows, polls sacct, and updates changed rows in a copy of the DataFrame.

### Testing approach

Tests use `tmp_path` fixtures (see `conftest.py`'s `fake_data_dir`) to create minimal fake directory trees. `submit.py` tests mock `subprocess.run` to validate sbatch command construction without a real Slurm cluster. `monitor.py` tests mock both `subprocess.run` (for sacct) and `poll_jobs` directly.

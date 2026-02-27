# Concepts: Architecture & Data Flow

## The pipeline

`snbb-scheduler` implements a linear data flow with five stages:

```
discover → evaluate → filter → submit → monitor
```

### 1. Discover

`sessions.py` scans the filesystem (or reads a CSV) and produces a **sessions DataFrame** — one row per `(subject, session)` pair. Each row contains:

- `subject` / `session` — BIDS labels (`sub-0001`, `ses-202411010600`)
- `dicom_path` / `dicom_exists` — location of raw DICOMs
- `<proc>_path` / `<proc>_exists` — output path and existence flag for every configured procedure

### 2. Evaluate

`rules.py` applies rule functions to each session row. A rule function for a procedure returns `True` if that procedure should run for that session. Rules check:

- whether the procedure's dependencies are already complete (`<dep>_exists` is True)
- whether the procedure's own output does **not** yet exist (unless `--force` is used)

### 3. Filter

`manifest.py` builds the task table, then removes tasks that are already `pending` or `running` in the state file. This prevents duplicate submission when the scheduler runs twice.

### 4. Submit

`submit.py` constructs the `sbatch` command for each task and runs it. On success, the task is recorded in the state file with `status=pending`.

### 5. Monitor

`monitor.py` polls `sacct` to update job statuses (`pending → running → complete/failed`). `manifest.py`'s `reconcile_with_filesystem` provides a secondary check: if a job is no longer tracked by sacct but its output exists on disk, it is marked `complete`.

---

## Module map

```
src/snbb_scheduler/
├── config.py      # Procedure dataclass, DEFAULT_PROCEDURES, SchedulerConfig
├── sessions.py    # discover_sessions(), sanitize_subject_code/session_id, load_sessions
├── checks.py      # is_complete() + specialized checks for freesurfer, qsiprep, qsirecon
├── rules.py       # Rule type, build_rules()
├── manifest.py    # build_manifest(), load_state(), save_state(),
│                  # filter_in_flight(), reconcile_with_filesystem()
├── submit.py      # submit_task(), submit_manifest(), _build_job_name()
├── monitor.py     # poll_jobs(), update_state_from_sacct(), _SLURM_STATE_MAP
├── audit.py       # AuditLogger, get_logger()
└── cli.py         # Click CLI: run, manifest, status, monitor, retry
```

---

## Key design constraints

**`config.py` is the only place paths are defined.** All modules receive a `SchedulerConfig` instance; paths are never hardcoded elsewhere.

**Rules are declarative.** Adding a new procedure requires only a YAML entry (or a `Procedure` dataclass). No code changes to `rules.py`, `manifest.py`, or `submit.py`.

**Filesystem is the source of truth.** No database. All state is in a single Parquet file.

**FreeSurfer and QSIPrep are subject-scoped.** Their output paths are `derivatives_root/<name>/sub-XXXX` — one directory per subject, not per session. All other procedures use `derivatives_root/<name>/sub-XXXX/ses-YY`. This asymmetry is tracked in the `scope` field of each `Procedure`.

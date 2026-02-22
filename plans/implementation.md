# Implementation Plan: snbb_scheduler

## Context

The `snbb_scheduler` package needs to be built from scratch based on the detailed spec in `snbb_scheduler_spec.md`. No code exists yet — only the spec and a `CLAUDE.md`. The spec is prescriptive (it includes exact function signatures, data structures, and CLI commands), so the plan follows the spec closely and focuses on sequencing work so each module can be tested before dependent modules are built.

## Implementation Order

The modules have a clear dependency chain: `config` → `sessions` → `checks` → `rules` → `manifest` → `submit` → `cli`. We'll build bottom-up, writing tests alongside each module.

---

### Step 1: Project scaffolding

Create the directory structure and `pyproject.toml`.

**Files to create:**
- `pyproject.toml` — as specified in the spec (setuptools build, pandas/click/pyyaml deps, pytest dev deps, `snbb-scheduler` entry point)
- `src/snbb_scheduler/__init__.py` — empty or version only
- `tests/` directory

**Verification:** `pip install -e ".[dev]"` succeeds.

---

### Step 2: `config.py` — Configuration dataclass

**File:** `src/snbb_scheduler/config.py`

Implement:
- `SchedulerConfig` dataclass with all fields from spec (dicom_root, bids_root, derivatives_root, qsiprep_dir, freesurfer_dir, slurm_partition, slurm_account, state_file)
- `@property` methods for `qsiprep_root` and `freesurfer_root`
- `from_yaml(path)` classmethod that loads a YAML file and overrides defaults (use `yaml.safe_load`, convert path strings to `Path` objects)

**No separate test file** — config is exercised by every other test. But we can add a small smoke test if desired.

---

### Step 3: `sessions.py` + `tests/test_sessions.py`

**File:** `src/snbb_scheduler/sessions.py`

Implement:
- `discover_sessions(config: SchedulerConfig) -> pd.DataFrame`
  - Walk `config.dicom_root`, find directories matching `sub-*/ses-*` pattern
  - Build DataFrame with columns: `subject`, `session`, `dicom_path`
  - Enrich with computed paths: `bids_path`, `qsiprep_path`, `freesurfer_path` (note: freesurfer is per-subject only)
  - Add boolean existence columns: `dicom_exists`, `bids_exists`, `qsiprep_exists`, `freesurfer_exists`

**File:** `tests/conftest.py` — shared `fake_data_dir` fixture as described in spec

**File:** `tests/test_sessions.py`
- Test correct discovery of subject/session pairs
- Test enrichment paths are computed correctly
- Test existence booleans reflect actual filesystem state
- Test empty dicom_root returns empty DataFrame

**Verification:** `pytest tests/test_sessions.py` passes.

---

### Step 4: `checks.py` + `tests/test_checks.py`

**File:** `src/snbb_scheduler/checks.py`

Implement:
- `bids_complete(bids_path: Path) -> bool` — directory exists AND contains at least one modality subdir (anat/, dwi/, func/) with at least one `.nii.gz` file
- `qsiprep_complete(qsiprep_path: Path) -> bool` — check for expected output files or completion marker
- `freesurfer_complete(freesurfer_path: Path) -> bool` — check for `scripts/recon-all.done`
- All return `False` for missing/nonexistent directories

**File:** `tests/test_checks.py`
- Test each check with complete directory → True
- Test each check with incomplete directory → False
- Test each check with nonexistent path → False

**Verification:** `pytest tests/test_checks.py` passes.

---

### Step 5: `rules.py` + `tests/test_rules.py`

**File:** `src/snbb_scheduler/rules.py`

Implement:
- `Rule = Callable[[pd.Series], bool]` type alias
- `needs_bids(row)` — dicom_exists and not bids_complete
- `needs_qsiprep(row)` — bids_complete and not qsiprep_complete
- `needs_freesurfer(row)` — bids_complete and not freesurfer_complete
- `RULES: dict[str, Rule]` registry in dependency order

**File:** `tests/test_rules.py`
- Test each rule against rows representing various filesystem states
- Test that rules respect dependency ordering (e.g., qsiprep not needed if BIDS incomplete)

**Verification:** `pytest tests/test_rules.py` passes.

---

### Step 6: `manifest.py` + `tests/test_manifest.py`

**File:** `src/snbb_scheduler/manifest.py`

Implement:
- `build_manifest(sessions: pd.DataFrame, rules: dict[str, Rule] | None = None) -> pd.DataFrame` — evaluate all rules, return DataFrame with columns: `subject, session, procedure, dicom_path, priority`
- `load_state(config) -> pd.DataFrame` — load parquet state file, return empty DataFrame with correct schema if missing
- `save_state(state, config)` — write parquet
- `filter_in_flight(manifest, state) -> pd.DataFrame` — anti-join on (subject, session, procedure) where status is `pending` or `running`

**File:** `tests/test_manifest.py`
- Test manifest builds correct task list from session DataFrame
- Test filter_in_flight removes pending/running tasks
- Test filter_in_flight keeps failed/complete tasks (they don't block re-runs — though "complete" should also not need re-running since the rule wouldn't fire)
- Test load_state with nonexistent file returns empty DataFrame
- Test round-trip save_state → load_state

**Verification:** `pytest tests/test_manifest.py` passes.

---

### Step 7: `submit.py` + `tests/test_submit.py`

**File:** `src/snbb_scheduler/submit.py`

Implement:
- `PROCEDURE_COMMANDS` dict mapping procedure names to shell scripts
- `submit_task(row, config, dry_run=False) -> str | None` — build sbatch command, run or print
- `submit_manifest(manifest, config, dry_run=False) -> pd.DataFrame` — iterate manifest, submit each task, return updated state entries

**File:** `tests/test_submit.py`
- Mock `subprocess.run` to test sbatch command construction
- Test dry_run prints but doesn't call subprocess
- Test job ID parsing from sbatch output

**Verification:** `pytest tests/test_submit.py` passes.

---

### Step 8: `cli.py`

**File:** `src/snbb_scheduler/cli.py`

Implement using Click:
- `@click.group` main group with `--config` option
- `run` command (with `--dry-run` flag) — discover → build manifest → filter in-flight → submit → save state
- `manifest` command — discover → build manifest → print table
- `status` command — load state → print table
- `retry` command (with `--procedure` and `--subject` options) — load state → set matching failed entries to allow re-run → save state

**Verification:** `snbb-scheduler --help`, `snbb-scheduler run --dry-run` (against a fake or empty data dir), `snbb-scheduler status`.

---

### Step 9: Final integration test + cleanup

- Run the full test suite: `pytest --cov=snbb_scheduler`
- Verify `pip install -e .` and `snbb-scheduler --help` work
- Ensure all modules have `__all__` or clean public APIs

---

## Summary of all files to create

| File | Purpose |
|------|---------|
| `pyproject.toml` | Package metadata, deps, entry point |
| `src/snbb_scheduler/__init__.py` | Package init |
| `src/snbb_scheduler/config.py` | `SchedulerConfig` dataclass + YAML loading |
| `src/snbb_scheduler/sessions.py` | `discover_sessions()` |
| `src/snbb_scheduler/checks.py` | `bids_complete()`, `qsiprep_complete()`, `freesurfer_complete()` |
| `src/snbb_scheduler/rules.py` | Rule functions + `RULES` registry |
| `src/snbb_scheduler/manifest.py` | `build_manifest()`, state load/save, `filter_in_flight()` |
| `src/snbb_scheduler/submit.py` | `submit_task()`, `submit_manifest()` |
| `src/snbb_scheduler/cli.py` | Click CLI: run, manifest, status, retry |
| `tests/conftest.py` | `fake_data_dir` fixture |
| `tests/test_sessions.py` | Session discovery tests |
| `tests/test_checks.py` | Completion check tests |
| `tests/test_rules.py` | Rule logic tests |
| `tests/test_manifest.py` | Manifest build + state tracking tests |
| `tests/test_submit.py` | Slurm submission tests (mocked) |

## Verification

After all steps: `pytest --cov=snbb_scheduler` — all tests pass with good coverage. `pip install -e .` works. `snbb-scheduler --help` shows all commands.

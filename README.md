# snbb_scheduler

A rule-based scheduler for the SNBB neuroimaging pipeline. Runs as a daily job: scans the filesystem, evaluates which processing steps are needed, and submits them to Slurm — automatically.

```
discover → evaluate → filter → submit
```

---

## Installation

```bash
git clone https://github.com/GalKepler/snbb_scheduler.git
cd snbb_scheduler
pip install -e ".[dev]"
```

---

## Quick start

```bash
# See what would be submitted (safe, no actual jobs)
snbb-scheduler --config /path/to/config.yaml run --dry-run

# Submit real jobs
snbb-scheduler --config /path/to/config.yaml run

# Check what's in the queue
snbb-scheduler --config /path/to/config.yaml status

# Show the full pending task table
snbb-scheduler --config /path/to/config.yaml manifest
```

---

## Configuration

All site-specific paths and procedures live in a single YAML file. The scheduler has built-in defaults; override only what differs on your system.

### Minimal config

```yaml
# /etc/snbb/config.yaml
dicom_root: /data/snbb/dicom
bids_root:  /data/snbb/bids
derivatives_root: /data/snbb/derivatives
state_file: /data/snbb/.scheduler_state.parquet

slurm_partition: normal
slurm_account:   snbb
```

With no `procedures` key the built-in defaults run: **bids → qsiprep** and **bids → freesurfer**.

### Full config with all options

```yaml
dicom_root:        /data/snbb/dicom
bids_root:         /data/snbb/bids
derivatives_root:  /data/snbb/derivatives
state_file:        /data/snbb/.scheduler_state.parquet

slurm_partition: normal
slurm_account:   snbb

procedures:
  - name: bids
    output_dir: ""          # special: lives in bids_root, not derivatives_root
    script: snbb_run_bids.sh
    scope: session
    depends_on: []
    completion_marker: "**/*.nii.gz"

  - name: qsiprep
    output_dir: qsiprep
    script: snbb_run_qsiprep.sh
    scope: session
    depends_on: [bids]
    completion_marker: null  # non-empty directory = complete

  - name: freesurfer
    output_dir: freesurfer
    script: snbb_run_freesurfer.sh
    scope: subject           # one run per subject, not per session
    depends_on: [bids]
    completion_marker: "scripts/recon-all.done"
```

### `completion_marker` values

| Value | Meaning |
|---|---|
| `null` | Output directory must exist and be non-empty |
| `"scripts/recon-all.done"` | That specific file must exist inside the output directory |
| `"**/*.nii.gz"` | At least one file matching the glob must exist (recursive) |

### `scope` values

| Value | Output path |
|---|---|
| `session` | `derivatives_root/<name>/sub-XXXX/ses-YY` |
| `subject` | `derivatives_root/<name>/sub-XXXX` (shared across sessions) |

---

## Adding a new procedure

No code changes required. Two options:

### Option A — YAML only (recommended for site deployments)

Add the procedure to your `procedures` list in `config.yaml`:

```yaml
procedures:
  # ... existing procedures ...

  - name: qsirecon
    output_dir: qsirecon
    script: snbb_run_qsirecon.sh
    scope: session
    depends_on: [qsiprep]       # won't run until qsiprep is complete
    completion_marker: null

  - name: fmriprep
    output_dir: fmriprep
    script: snbb_run_fmriprep.sh
    scope: session
    depends_on: [bids]
    completion_marker: "**/*.html"  # fmriprep writes an HTML report when done
```

That's it. On the next run, `qsirecon_path`, `qsirecon_exists`, `fmriprep_path`, and `fmriprep_exists` columns appear automatically in the session DataFrame, rules are generated automatically, and the sbatch command uses `proc.script`.

### Option B — Python API (for programmatic or multi-site setups)

```python
from snbb_scheduler.config import DEFAULT_PROCEDURES, Procedure, SchedulerConfig

qsirecon = Procedure(
    name="qsirecon",
    output_dir="qsirecon",
    script="snbb_run_qsirecon.sh",
    scope="session",
    depends_on=["qsiprep"],
    completion_marker=None,
)

cfg = SchedulerConfig.from_yaml("/etc/snbb/config.yaml")
cfg.procedures.append(qsirecon)

# cfg now schedules bids → qsiprep → qsirecon (and bids → freesurfer)
```

---

## CLI reference

### `run`

Runs the full pipeline: discover sessions → evaluate rules → filter in-flight → submit to Slurm → save state.

```bash
snbb-scheduler --config config.yaml run
snbb-scheduler --config config.yaml run --dry-run   # print only, no submission
```

Example dry-run output:
```
Discovering sessions…
  Found 47 session(s).
  12 task(s) need processing.
  10 task(s) after filtering in-flight jobs.
[DRY RUN] Would submit: sbatch --partition=normal --account=snbb --job-name=qsiprep_sub-0031_ses-01 snbb_run_qsiprep.sh sub-0031 ses-01
[DRY RUN] Would submit: sbatch --partition=normal --account=snbb --job-name=qsiprep_sub-0044_ses-01 snbb_run_qsiprep.sh sub-0044 ses-01
...
[DRY RUN] Would submit 10 job(s).
```

### `manifest`

Shows the pending task table without submitting anything.

```bash
snbb-scheduler --config config.yaml manifest
```

```
    subject  session  procedure  priority
   sub-0001   ses-01       bids         0
   sub-0002   ses-01       bids         0
   sub-0003   ses-01    qsiprep         1
   sub-0003   ses-01  freesurfer        2
```

### `status`

Shows the full state file — all submitted jobs and their current status.

```bash
snbb-scheduler --config config.yaml status
```

```
    subject  session   procedure    status           submitted_at  job_id
   sub-0001   ses-01        bids  complete  2024-11-01 06:00:00   10234
   sub-0001   ses-01     qsiprep   running  2024-11-02 06:00:00   10891
   sub-0002   ses-01        bids    failed  2024-11-01 06:00:00   10235
```

### `retry`

Clears failed entries from the state file so they are re-submitted on the next run. Accepts optional `--procedure` and `--subject` filters.

```bash
# Retry all failed jobs
snbb-scheduler --config config.yaml retry

# Retry only failed bids jobs
snbb-scheduler --config config.yaml retry --procedure bids

# Retry a specific subject
snbb-scheduler --config config.yaml retry --subject sub-0002

# Both filters combined
snbb-scheduler --config config.yaml retry --procedure bids --subject sub-0002
```

---

## Running as a cron job

```cron
# /etc/cron.d/snbb-scheduler
# Run every day at 6 AM
0 6 * * * snbb-user snbb-scheduler --config /etc/snbb/config.yaml run >> /var/log/snbb_scheduler.log 2>&1
```

---

## Python API

Use the modules directly when you need custom logic, scripting, or integration with other tools.

### Inspect the pending manifest

```python
from snbb_scheduler.config import SchedulerConfig
from snbb_scheduler.sessions import discover_sessions
from snbb_scheduler.manifest import build_manifest

cfg = SchedulerConfig.from_yaml("/etc/snbb/config.yaml")
sessions = discover_sessions(cfg)
manifest = build_manifest(sessions, cfg)

print(manifest.groupby("procedure").size())
# procedure
# bids          3
# freesurfer    8
# qsiprep       5
# dtype: int64
```

### Inspect the state file

```python
from snbb_scheduler.config import SchedulerConfig
from snbb_scheduler.manifest import load_state

cfg = SchedulerConfig.from_yaml("/etc/snbb/config.yaml")
state = load_state(cfg)

# Failed jobs
failed = state[state["status"] == "failed"]
print(failed[["subject", "session", "procedure", "job_id"]])

# Running jobs
running = state[state["status"] == "running"]
print(f"{len(running)} jobs currently running")
```

### Run the full pipeline programmatically

```python
from snbb_scheduler.config import SchedulerConfig
from snbb_scheduler.manifest import build_manifest, filter_in_flight, load_state, save_state
from snbb_scheduler.sessions import discover_sessions
from snbb_scheduler.submit import submit_manifest
import pandas as pd

cfg = SchedulerConfig.from_yaml("/etc/snbb/config.yaml")

sessions  = discover_sessions(cfg)
manifest  = build_manifest(sessions, cfg)
state     = load_state(cfg)
manifest  = filter_in_flight(manifest, state)

new_rows  = submit_manifest(manifest, cfg, dry_run=False)

# Merge with existing state and save
parts = [df for df in (state, new_rows) if not df.empty]
if parts:
    save_state(pd.concat(parts, ignore_index=True), cfg)
```

### Check completion for a specific session

```python
from snbb_scheduler.config import SchedulerConfig
from snbb_scheduler.checks import is_complete

cfg  = SchedulerConfig.from_yaml("/etc/snbb/config.yaml")
proc = cfg.get_procedure("freesurfer")
path = cfg.get_procedure_root(proc) / "sub-0001"

print(is_complete(proc, path))  # True / False
```

---

## Directory layout expected on disk

```
dicom_root/
├── sub-0001/
│   ├── ses-01/
│   │   └── *.dcm
│   └── ses-02/
│       └── *.dcm
└── sub-0002/
    └── ses-01/
        └── *.dcm

bids_root/
└── sub-0001/
    └── ses-01/
        └── anat/
            └── sub-0001_ses-01_T1w.nii.gz   ← marks bids complete

derivatives_root/
├── qsiprep/
│   └── sub-0001/
│       └── ses-01/
│           └── *.nii.gz                     ← non-empty marks qsiprep complete
└── freesurfer/
    └── sub-0001/
        └── scripts/
            └── recon-all.done               ← marker file for freesurfer
```

---

## Design principles

- **Filesystem is the source of truth.** No database. Completion is checked by looking at actual output files.
- **Declarative rules.** Adding a procedure is a data declaration, not code.
- **Conservative checks.** If in doubt, report incomplete so the procedure gets re-run.
- **Safe by default.** `--dry-run` is always available. The scheduler never deletes data.
- **Idempotent.** Running twice in a row submits nothing if in-flight jobs are tracked.

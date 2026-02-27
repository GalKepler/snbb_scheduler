# `snbb_scheduler.submit`

sbatch command construction and submission.

```python
from snbb_scheduler.submit import submit_task, submit_manifest
```

---

## `submit_task(row, config, dry_run=False, audit=None)`

Submit a single task to Slurm via `sbatch`.

```python
job_id = submit_task(row, cfg, dry_run=False)
```

**Parameters:**
- `row` — manifest row (`pd.Series`) with at least `procedure`, `subject`, `session`
- `config` — `SchedulerConfig` instance supplying Slurm settings
- `dry_run` — if `True`, print the command and return `None` without calling sbatch
- `audit` — optional `AuditLogger`; logs `submitted`, `error`, or `dry_run` events

**Returns:** Slurm job ID string on success, or `None` for dry runs.

**Raises:**
- `subprocess.CalledProcessError` — if sbatch exits non-zero
- `RuntimeError` — if sbatch output doesn't match `"Submitted batch job <ID>"`

### sbatch command construction

The command is built from:

```bash
sbatch
  [--partition=<partition>]        # omitted if config.slurm_partition is empty
  --account=<account>
  --job-name=<job_name>
  [--mem=<mem>]                    # omitted if config.slurm_mem is None
  [--cpus-per-task=<cpus>]         # omitted if config.slurm_cpus_per_task is None
  [--output=<log_dir>/<name>_%j.out]  # added if config.slurm_log_dir is set
  [--error=<log_dir>/<name>_%j.err]   # added if config.slurm_log_dir is set
  <script>
  <subject>
  [<session> <dicom_path>]        # added for session-scoped procedures
```

### Job naming

| Scope | Job name |
|---|---|
| `subject` | `<procedure>_<subject>` |
| `session` | `<procedure>_<subject>_<session>` |

### Example

```python
import pandas as pd
from snbb_scheduler.config import SchedulerConfig
from snbb_scheduler.submit import submit_task

cfg = SchedulerConfig.from_yaml("/etc/snbb/config.yaml")

row = pd.Series({
    "subject": "sub-0001",
    "session": "ses-202407110849",
    "procedure": "bids",
    "dicom_path": "/data/snbb/dicom/sub-0001/ses-202407110849",
})

# Dry run
submit_task(row, cfg, dry_run=True)
# [DRY RUN] Would submit: sbatch ...

# Real submission
job_id = submit_task(row, cfg, dry_run=False)
print(job_id)  # "12345"
```

---

## `submit_manifest(manifest, config, dry_run=False, audit=None)`

Submit all tasks in the manifest.

```python
new_state = submit_manifest(manifest, cfg, dry_run=False, audit=audit)
```

**Parameters:**
- `manifest` — DataFrame from `build_manifest` (after `filter_in_flight`)
- `config` — `SchedulerConfig` instance
- `dry_run` — if `True`, print commands without submitting
- `audit` — optional `AuditLogger`

**Returns:** `pd.DataFrame` of new state rows with columns:
`subject`, `session`, `procedure`, `status`, `submitted_at`, `job_id`

All new rows get `status="pending"` and `submitted_at=now(UTC)`.

Returns an empty DataFrame with the correct schema if `manifest` is empty.

### Example

```python
new_rows = submit_manifest(manifest, cfg, dry_run=False, audit=audit)
print(f"Submitted {len(new_rows)} jobs")
# Merge with existing state and save
parts = [df for df in (state, new_rows) if not df.empty]
if parts:
    import pandas as pd
    save_state(pd.concat(parts, ignore_index=True), cfg)
```

---

## `_build_job_name(row, proc_scope)`

Internal helper that builds the Slurm job name string.

```python
from snbb_scheduler.submit import _build_job_name

name = _build_job_name(row, "subject")  # "qsiprep_sub-0001"
name = _build_job_name(row, "session")  # "bids_sub-0001_ses-202407110849"
```

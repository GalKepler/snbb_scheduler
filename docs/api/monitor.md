# `snbb_scheduler.monitor`

sacct polling and job status updates.

```python
from snbb_scheduler.monitor import poll_jobs, update_state_from_sacct
```

---

## `poll_jobs(job_ids)`

Query `sacct` for the current state of each job ID.

```python
states = poll_jobs(["12345", "12346", "12347"])
# {"12345": "complete", "12346": "running", "12347": "failed"}
```

**Parameters:**
- `job_ids` — list of Slurm job ID strings

**Returns:** `dict[str, str]` mapping job_id → scheduler status string. Returns `{}` on error (sacct not found, non-zero exit, etc.).

**Slurm state mapping:**

| Slurm state | Scheduler status |
|---|---|
| `PENDING` | `pending` |
| `RUNNING` | `running` |
| `COMPLETED` | `complete` |
| `FAILED` | `failed` |
| `TIMEOUT` | `failed` |
| `CANCELLED` | `failed` |
| `OUT_OF_MEMORY` | `failed` |
| `NODE_FAIL` | `failed` |

Sub-step job IDs (containing `.`) are skipped. State suffixes like `"CANCELLED by user"` are normalized to the base state.

---

## `update_state_from_sacct(state, audit=None)`

Poll sacct for in-flight jobs and update their statuses.

```python
updated = update_state_from_sacct(state, audit=audit)
```

**Parameters:**
- `state` — current scheduler state DataFrame
- `audit` — optional `AuditLogger`; logs `status_change` events

**Returns:** Modified copy of `state` with updated statuses. The original is unchanged.

Only jobs with `status=pending` or `status=running` are queried. If sacct returns no results (unavailable or empty), the original state is returned unchanged.

### Example

```python
from snbb_scheduler.config import SchedulerConfig
from snbb_scheduler.manifest import load_state, save_state
from snbb_scheduler.monitor import update_state_from_sacct
from snbb_scheduler.audit import get_logger

cfg = SchedulerConfig.from_yaml("/etc/snbb/config.yaml")
audit = get_logger(cfg)
state = load_state(cfg)

updated = update_state_from_sacct(state, audit)

if not updated.equals(state):
    save_state(updated, cfg)
    print("State updated from sacct.")
```

---

## `_SLURM_STATE_MAP`

The mapping from Slurm state strings to scheduler status strings:

```python
_SLURM_STATE_MAP = {
    "PENDING": "pending",
    "RUNNING": "running",
    "COMPLETED": "complete",
    "FAILED": "failed",
    "TIMEOUT": "failed",
    "CANCELLED": "failed",
    "OUT_OF_MEMORY": "failed",
    "NODE_FAIL": "failed",
}
```

Unknown Slurm states are silently ignored (job status is not updated).

---

## Error handling

- If `sacct` is not found on `$PATH`, a warning is logged and `{}` is returned
- If `sacct` exits non-zero, a warning is logged and `{}` is returned
- Neither case raises an exception — the scheduler continues with unchanged statuses
- After sacct polling, `reconcile_with_filesystem` (in `manifest.py`) provides a second check based on actual output files

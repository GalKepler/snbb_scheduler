# State File

The scheduler tracks every submitted job in a single [Apache Parquet](https://parquet.apache.org/) file. No external database is required.

## Location

Set in `config.yaml`:

```yaml
state_file: /data/snbb/.scheduler_state.parquet
```

The file is created automatically on first run if it does not exist.

## Schema

| Column | dtype | Description |
|---|---|---|
| `subject` | string | BIDS subject label, e.g. `sub-0001` |
| `session` | string | BIDS session label, e.g. `ses-202411010600`; empty string `""` for subject-scoped procedures |
| `procedure` | string | Procedure name, e.g. `bids`, `qsiprep`, `freesurfer` |
| `status` | string | One of `pending`, `running`, `complete`, `failed` |
| `submitted_at` | datetime64[ns] UTC | Timestamp when the job was submitted |
| `job_id` | string | Slurm job ID returned by `sbatch`; `None` for dry-run entries |

## Status lifecycle

```
pending → running → complete
               ↘ failed
```

| Status | Set when |
|---|---|
| `pending` | `sbatch` returns successfully |
| `running` | sacct reports the job is active (`RUNNING`) |
| `complete` | sacct reports `COMPLETED`, or filesystem reconciliation finds output |
| `failed` | sacct reports `FAILED`, `TIMEOUT`, `CANCELLED`, `OUT_OF_MEMORY`, or `NODE_FAIL` |

## Reading with pandas

```python
import pandas as pd

state = pd.read_parquet("/data/snbb/.scheduler_state.parquet")

# All failed jobs
failed = state[state["status"] == "failed"]
print(failed[["subject", "session", "procedure", "job_id"]])

# Count by procedure and status
print(state.groupby(["procedure", "status"]).size().unstack(fill_value=0))

# Jobs submitted today
from datetime import date
today = pd.Timestamp(date.today(), tz="UTC")
today_jobs = state[state["submitted_at"] >= today]

# Running qsiprep jobs
running_qsiprep = state[(state["procedure"] == "qsiprep") & (state["status"] == "running")]
```

## Writing with pandas

If you need to manually edit the state file, always read-modify-write to preserve schema:

```python
import pandas as pd

path = "/data/snbb/.scheduler_state.parquet"
state = pd.read_parquet(path)

# Example: remove a stuck pending entry
mask = (state["subject"] == "sub-0003") & (state["procedure"] == "freesurfer")
state = state[~mask].reset_index(drop=True)

state.to_parquet(path, index=False)
```

## Backing up the state file

```bash
cp /data/snbb/.scheduler_state.parquet \
   /data/snbb/.scheduler_state.parquet.$(date +%Y%m%d)
```

Since the filesystem is the source of truth, losing the state file only means losing job history. Run `monitor` after restoring from backup to reconcile with the actual filesystem state.

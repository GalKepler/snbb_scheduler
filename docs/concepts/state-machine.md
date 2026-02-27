# State Machine

## Status lifecycle

Every submitted job transitions through a defined set of statuses:

```
pending → running → complete
               ↘ failed
```

| Status | Meaning |
|---|---|
| `pending` | Submitted to Slurm, not yet confirmed running |
| `running` | Slurm reports the job is active |
| `complete` | Output verified complete by the completion marker |
| `failed` | Slurm reported failure, or the job timed out / ran out of memory |

### How transitions happen

1. **`pending`** — set immediately when `sbatch` returns a job ID
2. **`running`** / **`complete`** / **`failed`** — updated by `snbb-scheduler monitor` (or automatically at the start of `snbb-scheduler run`)
3. **`complete` (filesystem path)** — `reconcile_with_filesystem` catches jobs that sacct no longer tracks but whose outputs exist on disk
4. **Clearing `failed`** — `snbb-scheduler retry` removes failed entries so they are re-submitted on the next `run`

---

## The state file

The scheduler tracks every submitted job in a single [Apache Parquet](https://parquet.apache.org/) file configured via `state_file` in `config.yaml`. No external database is required.

### Schema

| Column | Type | Description |
|---|---|---|
| `subject` | string | BIDS subject label, e.g. `sub-0001` |
| `session` | string | BIDS session label, e.g. `ses-202411010600`; empty string for subject-scoped procedures |
| `procedure` | string | Procedure name, e.g. `bids`, `qsiprep` |
| `status` | string | One of `pending`, `running`, `complete`, `failed` |
| `submitted_at` | datetime (UTC) | Timestamp of initial submission |
| `job_id` | string | Slurm job ID returned by `sbatch` |

### In-flight deduplication

Before submission, `filter_in_flight` removes from the manifest any task that already has `status=pending` or `status=running` in the state file. This prevents a second submission of the same job when the scheduler runs twice in quick succession.

### Reading the state file directly

```python
import pandas as pd

state = pd.read_parquet("/data/snbb/.scheduler_state.parquet")

# Show all failed jobs
print(state[state["status"] == "failed"])

# Count by procedure and status
print(state.groupby(["procedure", "status"]).size())
```

---

## State file location

Set `state_file` in `config.yaml`:

```yaml
state_file: /data/snbb/.scheduler_state.parquet
```

If the file does not exist on first run, it is created automatically.

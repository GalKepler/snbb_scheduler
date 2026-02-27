# Audit Log

Every significant scheduler event is appended to a JSONL (newline-delimited JSON) file. One record per event, human-readable, easily processed with standard tools.

## Location

Defaults to `<state_file_parent>/scheduler_audit.jsonl`. Override in config:

```yaml
log_file: /data/snbb/scheduler_audit.jsonl
```

## Record format

Every record has these fields:

| Field | Type | Description |
|---|---|---|
| `timestamp` | ISO 8601 datetime (UTC) | When the event occurred |
| `event` | string | Event type — see table below |
| `subject` | string | BIDS subject label, or `""` |
| `session` | string | BIDS session label, or `""` |
| `procedure` | string | Procedure name, or `""` |
| `job_id` | string | Slurm job ID (when applicable) |
| `old_status` | string | Previous status (for `status_change` events) |
| `new_status` | string | New status (for `status_change` events) |
| `detail` | string | Extra context (for `error` and `dry_run` events) |

## Event types

| Event | Triggered by | Key fields |
|---|---|---|
| `submitted` | Job submitted to Slurm | `job_id` |
| `status_change` | sacct poll or filesystem reconciliation updates a status | `job_id`, `old_status`, `new_status` |
| `error` | `sbatch` exits non-zero | `detail` (error message) |
| `dry_run` | `run --dry-run` | `detail` (full sbatch command) |
| `retry_cleared` | `retry` removes a failed entry | `job_id`, `old_status` |

## Example records

```json
{"timestamp": "2024-11-01T06:00:12.345678+00:00", "event": "submitted", "subject": "sub-0001", "session": "ses-202407110849", "procedure": "bids", "job_id": "10234"}
{"timestamp": "2024-11-01T06:00:12.567890+00:00", "event": "submitted", "subject": "sub-0002", "session": "ses-202407110849", "procedure": "bids", "job_id": "10235"}
{"timestamp": "2024-11-01T07:30:44.112233+00:00", "event": "status_change", "subject": "sub-0001", "session": "ses-202407110849", "procedure": "bids", "job_id": "10234", "old_status": "pending", "new_status": "complete"}
{"timestamp": "2024-11-01T07:30:44.223344+00:00", "event": "status_change", "subject": "sub-0002", "session": "ses-202407110849", "procedure": "bids", "job_id": "10235", "old_status": "pending", "new_status": "failed"}
{"timestamp": "2024-11-02T06:00:05.000000+00:00", "event": "retry_cleared", "subject": "sub-0002", "session": "ses-202407110849", "procedure": "bids", "job_id": "10235", "old_status": "failed"}
```

## Tailing the log

```bash
tail -f /data/snbb/scheduler_audit.jsonl
```

Pretty-print with Python:

```bash
tail -f /data/snbb/scheduler_audit.jsonl | python3 -c "
import sys, json
for line in sys.stdin:
    r = json.loads(line)
    print(r['timestamp'][:19], f'{r[\"event\"]:20s}', r.get('procedure',''), r.get('subject',''), r.get('new_status',''))
"
```

## Querying with Python

```python
import json
import pandas as pd

records = []
with open("/data/snbb/scheduler_audit.jsonl") as f:
    for line in f:
        records.append(json.loads(line))

df = pd.DataFrame(records)
df["timestamp"] = pd.to_datetime(df["timestamp"])

# All failures
failures = df[df["new_status"] == "failed"]
print(failures[["timestamp", "procedure", "subject", "job_id"]])

# Submissions per day
submissions = df[df["event"] == "submitted"].copy()
submissions["date"] = submissions["timestamp"].dt.date
print(submissions.groupby("date").size())
```

## Querying with grep

```bash
# All failed status changes
grep '"new_status": "failed"' /data/snbb/scheduler_audit.jsonl

# All events for sub-0003
grep '"subject": "sub-0003"' /data/snbb/scheduler_audit.jsonl

# All retry events
grep '"event": "retry_cleared"' /data/snbb/scheduler_audit.jsonl
```

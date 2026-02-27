# Monitoring Jobs

The scheduler tracks job status through three complementary mechanisms.

---

## 1. sacct polling (`monitor` command)

`snbb-scheduler monitor` queries `sacct` for the current Slurm state of every in-flight job and updates the state file:

```bash
snbb-scheduler --config config.yaml monitor
```

Output:

```
Updated 4 job status(es).
    procedure    status  count
         bids  complete     12
     bids_post  complete      9
     bids_post   running      3
      qsiprep   running      2
   freesurfer    failed       1
```

The monitor runs automatically at the start of `snbb-scheduler run` (unless `--skip-monitor` is passed), so you usually don't need to call it separately unless you want status updates without triggering a new submission pass.

---

## 2. Filesystem reconciliation

`reconcile_with_filesystem` is called after every sacct poll. It scans the output directories for every job still marked `pending` or `running` and marks it `complete` if the completion check passes.

This is especially useful when:
- sacct no longer tracks a job (outside the retention window)
- the cluster was rescheduled and job IDs changed
- you manually ran a procedure outside the scheduler

Filesystem reconciliation runs automatically as part of both `monitor` and `run`. You can also call it directly in Python:

```python
from snbb_scheduler.config import SchedulerConfig
from snbb_scheduler.manifest import load_state, reconcile_with_filesystem, save_state

cfg = SchedulerConfig.from_yaml("/etc/snbb/config.yaml")
state = load_state(cfg)
updated = reconcile_with_filesystem(state, cfg)
if not updated.equals(state):
    save_state(updated, cfg)
    print("State updated.")
```

---

## 3. Audit log

Every status change is appended to a JSONL audit log. To watch it in real time:

```bash
tail -f /data/snbb/scheduler_audit.jsonl | python3 -c "
import sys, json
for line in sys.stdin:
    r = json.loads(line)
    print(r['timestamp'], r['event'], r.get('procedure',''), r.get('subject',''), r.get('new_status',''))
"
```

To find all failed events:

```bash
grep '"event": "status_change"' /data/snbb/scheduler_audit.jsonl \
  | python3 -c "
import sys, json
for line in sys.stdin:
    r = json.loads(line)
    if r.get('new_status') == 'failed':
        print(r['timestamp'], r['procedure'], r['subject'], r.get('job_id',''))
"
```

See [Audit Log reference](../reference/audit-log.md) for the full event schema.

---

## Recommended monitoring workflow

### For daily cron use

Run monitor + run together. The `run` command handles both:

```cron
0 6 * * * snbb-user snbb-scheduler --config /etc/snbb/config.yaml run
```

This automatically polls sacct, reconciles the filesystem, and submits new jobs in one pass.

### For interactive use

```bash
# Update statuses
snbb-scheduler --config config.yaml monitor

# Check results
snbb-scheduler --config config.yaml status

# Retry failures
snbb-scheduler --config config.yaml retry

# Submit new jobs
snbb-scheduler --config config.yaml run
```

---

## Notes

- `monitor` is safe to run at any time — it never submits jobs
- If `sacct` is not installed, monitor falls back to filesystem reconciliation only
- The scheduler does not set up Slurm job epilog scripts or callbacks — status is always polled, not pushed

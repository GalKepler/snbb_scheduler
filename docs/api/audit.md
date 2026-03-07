# `snbb_scheduler.audit`

JSONL audit logging for all scheduler events, with optional HTML report generation.

```python
from snbb_scheduler.audit import AuditLogger, get_logger
```

---

## `get_logger(config)`

Return an `AuditLogger` for the given config.

```python
from snbb_scheduler.audit import get_logger

audit = get_logger(cfg)
```

- Uses `config.log_file` if set; otherwise defaults to `<state_file_parent>/scheduler_audit.jsonl`.
- Passes `config.audit.report_dir` to `AuditLogger` so that HTML reports are written automatically whenever `report_dir` is configured.

---

## `AuditLogger`

Appends JSONL records to a log file and, optionally, keeps an HTML report up to date.

```python
from pathlib import Path
from snbb_scheduler.audit import AuditLogger

# JSONL only
audit = AuditLogger(Path("/data/snbb/scheduler_audit.jsonl"))

# JSONL + HTML report
audit = AuditLogger(
    Path("/data/snbb/scheduler_audit.jsonl"),
    report_dir=Path("/data/snbb/audit_reports"),
)
```

### Constructor parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `log_file` | `Path` | — | Path to the JSONL log file (created on first write) |
| `report_dir` | `Path` or `None` | `None` | Directory where `audit_report.html` is written after every event. `None` disables HTML output. |

### `audit.log(event, *, subject, session, procedure, job_id, old_status, new_status, detail, **extra)`

Append a single JSONL record and refresh the HTML report (if `report_dir` is set).

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` | `str` | — | Event type (see table below) |
| `subject` | `str` | `""` | BIDS subject label |
| `session` | `str` | `""` | BIDS session label |
| `procedure` | `str` | `""` | Procedure name |
| `job_id` | `str` or `None` | `None` | Slurm job ID (omitted if `None`) |
| `old_status` | `str` or `None` | `None` | Previous status (omitted if `None`) |
| `new_status` | `str` or `None` | `None` | New status (omitted if `None`) |
| `detail` | `str` | `""` | Extra context string (omitted if empty) |
| `**extra` | — | — | Any additional key-value pairs to include in the record |

### Event types

| Event | When | Key fields |
|---|---|---|
| `submitted` | Job submitted to Slurm | `job_id` |
| `status_change` | sacct or filesystem updates a status | `job_id`, `old_status`, `new_status` |
| `error` | sbatch exits non-zero | `detail` (error message) |
| `dry_run` | `run --dry-run` | `detail` (full sbatch command) |
| `retry_cleared` | `retry` removes a failed entry | `job_id`, `old_status` |

### Example

```python
from pathlib import Path
from snbb_scheduler.audit import AuditLogger

audit = AuditLogger(
    Path("/data/snbb/scheduler_audit.jsonl"),
    report_dir=Path("/data/snbb/audit_reports"),
)

# Log a submission — also updates audit_report.html
audit.log(
    "submitted",
    subject="sub-0001",
    session="ses-202407110849",
    procedure="bids",
    job_id="12345",
)

# Log a status change
audit.log(
    "status_change",
    subject="sub-0001",
    session="ses-202407110849",
    procedure="bids",
    job_id="12345",
    old_status="pending",
    new_status="complete",
)
```

### Output record (JSONL)

```json
{
  "timestamp": "2024-11-01T06:00:12.345678+00:00",
  "event": "submitted",
  "subject": "sub-0001",
  "session": "ses-202407110849",
  "procedure": "bids",
  "job_id": "12345"
}
```

---

## HTML report

When `report_dir` is set, every call to `log()` regenerates `<report_dir>/audit_report.html`. The report is a self-contained HTML page with a styled table of all events recorded in the JSONL log, colour-coded by event type.

The file is **overwritten** on each update — it always reflects the full history of the JSONL log.

Configure via YAML:

```yaml
audit:
  report_dir: /data/snbb/audit_reports
```

The file is then available at `/data/snbb/audit_reports/audit_report.html` after the first scheduler event.

---

## Notes

- Both `log_file` and `report_dir` parent directories are created automatically if they don't exist.
- JSONL records are appended (never overwritten) — the file grows indefinitely.
- Set up log rotation if the scheduler runs daily (see [Cron Setup](../guides/cron-setup.md)).
- See [Audit Log reference](../reference/audit-log.md) for querying and tailing the JSONL log.

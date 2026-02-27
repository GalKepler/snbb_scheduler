# `snbb_scheduler.manifest`

Task manifest and state file management.

```python
from snbb_scheduler.manifest import (
    build_manifest,
    load_state,
    save_state,
    filter_in_flight,
    reconcile_with_filesystem,
)
```

---

## `build_manifest(sessions, config, force=False, force_procedures=None)`

Evaluate rules against all sessions and return a task manifest.

```python
manifest = build_manifest(sessions, cfg)
```

**Parameters:**
- `sessions` — DataFrame from `discover_sessions`
- `config` — `SchedulerConfig` instance
- `force` — skip completion check for all (or selected) procedures
- `force_procedures` — list of procedure names to force-requeue

**Returns:** `pd.DataFrame` with columns `subject`, `session`, `procedure`, `dicom_path`, `priority`

The `priority` column reflects the position of the procedure in `config.procedures` (lower = submitted first).

Subject-scoped procedures appear once per subject (deduplicated across sessions), with `session=""`.

### Example

```python
from snbb_scheduler.config import SchedulerConfig
from snbb_scheduler.sessions import discover_sessions
from snbb_scheduler.manifest import build_manifest

cfg = SchedulerConfig.from_yaml("/etc/snbb/config.yaml")
sessions = discover_sessions(cfg)
manifest = build_manifest(sessions, cfg)

print(manifest[["subject", "session", "procedure", "priority"]])
```

---

## `load_state(config)`

Load the state parquet file.

```python
state = load_state(cfg)
```

Returns an empty DataFrame with the correct schema if the file does not exist (never raises `FileNotFoundError`).

**Schema:** `subject`, `session`, `procedure`, `status`, `submitted_at`, `job_id`

---

## `save_state(state, config)`

Persist the state DataFrame to the parquet state file.

```python
save_state(state, cfg)
```

Creates parent directories if needed.

---

## `filter_in_flight(manifest, state)`

Remove tasks that are already `pending` or `running` in the state file.

```python
filtered = filter_in_flight(manifest, state)
```

Compares on `(subject, session, procedure)`. Returns `manifest` unchanged if either DataFrame is empty.

### Example

```python
manifest = build_manifest(sessions, cfg)
state = load_state(cfg)
manifest = filter_in_flight(manifest, state)  # remove already-submitted tasks
```

---

## `reconcile_with_filesystem(state, config, audit=None)`

Mark pending/running tasks as `complete` when their output exists on disk.

```python
updated = reconcile_with_filesystem(state, cfg)
```

Handles the case where sacct no longer tracks a completed job (job purged from retention, or sacct unavailable). For each in-flight task, runs `is_complete` against the actual output directory.

**Parameters:**
- `state` — current state DataFrame
- `config` — `SchedulerConfig` instance
- `audit` — optional `AuditLogger`; logs `status_change` events for each transition

**Returns:** Modified copy of `state` with updated statuses. The original is unchanged.

### Example

```python
from snbb_scheduler.manifest import load_state, reconcile_with_filesystem, save_state
from snbb_scheduler.audit import get_logger

cfg = SchedulerConfig.from_yaml("/etc/snbb/config.yaml")
audit = get_logger(cfg)
state = load_state(cfg)
updated = reconcile_with_filesystem(state, cfg, audit=audit)
if not updated.equals(state):
    save_state(updated, cfg)
```

---

## Full pipeline example

```python
import pandas as pd
from snbb_scheduler.config import SchedulerConfig
from snbb_scheduler.sessions import discover_sessions
from snbb_scheduler.manifest import (
    build_manifest, filter_in_flight,
    load_state, save_state, reconcile_with_filesystem,
)
from snbb_scheduler.monitor import update_state_from_sacct
from snbb_scheduler.submit import submit_manifest
from snbb_scheduler.audit import get_logger

cfg = SchedulerConfig.from_yaml("/etc/snbb/config.yaml")
audit = get_logger(cfg)

# Discover and evaluate
sessions = discover_sessions(cfg)
manifest = build_manifest(sessions, cfg)

# Update state from sacct + filesystem
state = load_state(cfg)
state = update_state_from_sacct(state, audit)
state = reconcile_with_filesystem(state, cfg, audit)
save_state(state, cfg)

# Filter and submit
manifest = filter_in_flight(manifest, state)
new_rows = submit_manifest(manifest, cfg, dry_run=False, audit=audit)

# Save combined state
parts = [df for df in (state, new_rows) if not df.empty]
if parts:
    save_state(pd.concat(parts, ignore_index=True), cfg)
```

# Python API Overview

Use the `snbb_scheduler` modules directly when you need custom logic, scripting, or integration with other tools.

## When to use the Python API

- Inspecting the manifest or state file in a notebook or script
- Building configs programmatically (multi-site setups)
- Adding procedures dynamically based on external logic
- Running the pipeline from within another Python process
- Writing custom reports or dashboards

## Module map

| Module | Key exports | Description |
|---|---|---|
| [`config`](config.md) | `SchedulerConfig`, `Procedure`, `DEFAULT_PROCEDURES` | All path conventions and procedure declarations |
| [`sessions`](sessions.md) | `discover_sessions`, `load_sessions`, `sanitize_*` | Session discovery from filesystem or CSV |
| [`checks`](checks.md) | `is_complete` | Completion checking for any procedure |
| [`rules`](rules.md) | `build_rules`, `Rule` | Rule evaluation logic |
| [`manifest`](manifest.md) | `build_manifest`, `load_state`, `save_state`, `filter_in_flight`, `reconcile_with_filesystem` | Task manifest and state file management |
| [`submit`](submit.md) | `submit_task`, `submit_manifest` | sbatch command construction and submission |
| [`monitor`](monitor.md) | `poll_jobs`, `update_state_from_sacct` | sacct polling and status updates |
| [`audit`](audit.md) | `AuditLogger`, `get_logger` | JSONL audit logging |

## Quick examples

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
# bids_post     2
# qsiprep       1
# dtype: int64
```

### Run the full pipeline

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

print(is_complete(proc, path, bids_root=cfg.bids_root, subject="sub-0001"))
```

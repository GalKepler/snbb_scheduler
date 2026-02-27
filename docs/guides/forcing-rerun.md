# Forcing a Rerun

Sometimes you need to re-run a procedure that the scheduler considers already complete, or override the in-flight filter. There are three levels of force, from least to most invasive.

---

## Level 1 — Force a single procedure (recommended)

Use `--force --procedure` to re-queue one procedure across all subjects/sessions, bypassing both the completion check and the in-flight filter:

```bash
snbb-scheduler --config config.yaml run --force --procedure qsiprep
```

This submits a new `qsiprep` job for every subject, even those already marked `complete` or `running`. Use `--dry-run` first to preview:

```bash
snbb-scheduler --config config.yaml run --dry-run --force --procedure qsiprep
```

---

## Level 2 — Force all procedures

Use `--force` alone to re-queue all procedures:

```bash
snbb-scheduler --config config.yaml run --force
```

!!! warning
    This submits jobs for every session and every procedure regardless of status. Only use this after a major environment change (e.g. upgrading a container, wiping derivatives).

---

## Level 3 — Manual state surgery

For precise control — e.g., force re-run of one specific subject for one procedure — edit the state file directly with pandas:

```python
import pandas as pd

state_path = "/data/snbb/.scheduler_state.parquet"
state = pd.read_parquet(state_path)

# Remove sub-0003's freesurfer entry so it gets re-submitted
mask = (state["subject"] == "sub-0003") & (state["procedure"] == "freesurfer")
state = state[~mask].reset_index(drop=True)

state.to_parquet(state_path, index=False)
```

After saving, run the scheduler normally:

```bash
snbb-scheduler --config config.yaml run
```

---

## Common scenarios

### "qsiprep ran with wrong parameters — re-run for all subjects"

```bash
# 1. Preview
snbb-scheduler --config config.yaml run --dry-run --force --procedure qsiprep

# 2. Submit
snbb-scheduler --config config.yaml run --force --procedure qsiprep
```

### "New T1w session added — freesurfer needs to re-run for sub-0001"

The freesurfer completion check detects the new session automatically (it compares the number of T1w inputs used vs. available). The next unforced `run` will pick it up.

But if you want to force it immediately:

```python
import pandas as pd

state_path = "/data/snbb/.scheduler_state.parquet"
state = pd.read_parquet(state_path)
mask = (state["subject"] == "sub-0001") & (state["procedure"] == "freesurfer")
state = state[~mask].reset_index(drop=True)
state.to_parquet(state_path, index=False)
```

### "All jobs vanished from Slurm — state file still shows pending/running"

Run `monitor` first — it will use filesystem reconciliation to mark complete jobs:

```bash
snbb-scheduler --config config.yaml monitor
```

For any that are still stuck as `pending` after monitoring, clear them:

```python
import pandas as pd

state_path = "/data/snbb/.scheduler_state.parquet"
state = pd.read_parquet(state_path)

# Clear all stuck pending/running (will be re-submitted on next run)
stuck = state["status"].isin(["pending", "running"])
state = state[~stuck].reset_index(drop=True)
state.to_parquet(state_path, index=False)
```

---

## Notes

- `--force` does not delete existing output. If you want a clean re-run, manually remove the output directory first.
- After forced submission, use `monitor` regularly to track the new jobs.
- The audit log records all `dry_run` events so you can review what was attempted.

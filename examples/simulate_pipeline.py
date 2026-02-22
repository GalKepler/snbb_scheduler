"""simulate_pipeline.py — simulate a full multi-day pipeline run locally.

Creates a fake filesystem in a temp directory and walks through four days
of the scheduler, printing what happens at each stage. No Slurm required:
jobs are "completed" by creating output directories between runs.

Run with:
    python examples/simulate_pipeline.py
"""

import tempfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from snbb_scheduler.config import SchedulerConfig
from snbb_scheduler.manifest import build_manifest, filter_in_flight, load_state, save_state
from snbb_scheduler.sessions import discover_sessions
from snbb_scheduler.submit import submit_manifest

_JOB_COUNTER = 0


def next_job_id() -> str:
    global _JOB_COUNTER
    _JOB_COUNTER += 1
    return str(10000 + _JOB_COUNTER)


def mock_sbatch(_cmd, **_kw):
    m = type("R", (), {"stdout": f"Submitted batch job {next_job_id()}\n"})()
    return m


def scheduler_run(cfg: SchedulerConfig, day: int) -> None:
    """One scheduler cycle: discover → manifest → filter → submit → save."""
    print(f"\n{'='*60}")
    print(f"  DAY {day}")
    print(f"{'='*60}")

    sessions = discover_sessions(cfg)
    print(f"  Sessions found: {len(sessions)}")

    manifest = build_manifest(sessions, cfg)
    state = load_state(cfg)
    filtered = filter_in_flight(manifest, state)

    print(f"  Tasks in manifest: {len(manifest)}")
    print(f"  After in-flight filter: {len(filtered)}")

    if filtered.empty:
        print("  → Nothing to submit.")
        return

    with patch("subprocess.run", side_effect=mock_sbatch):
        new_rows = submit_manifest(filtered, cfg)

    parts = [df for df in (state, new_rows) if not df.empty]
    save_state(pd.concat(parts, ignore_index=True), cfg)

    print("  Submitted:")
    for _, row in new_rows.iterrows():
        print(f"    job {row['job_id']:>6}  {row['subject']}  {row['session']}  {row['procedure']}")


def mark_job_complete(cfg: SchedulerConfig, job_id: str) -> None:
    """Simulate a Slurm job finishing: create its output and update state."""
    state = load_state(cfg)
    row = state[state["job_id"] == job_id].iloc[0]
    subject, session, procedure = row["subject"], row["session"], row["procedure"]

    proc = cfg.get_procedure(procedure)
    root = cfg.get_procedure_root(proc)

    # Create minimal output so completion checks pass
    if procedure == "bids":
        out = root / subject / session / "anat"
        out.mkdir(parents=True, exist_ok=True)
        (out / f"{subject}_{session}_T1w.nii.gz").touch()
    elif procedure == "freesurfer":
        out = root / subject / "scripts"
        out.mkdir(parents=True, exist_ok=True)
        (out / "recon-all.done").touch()
    else:
        out = root / subject / session
        out.mkdir(parents=True, exist_ok=True)
        (out / "output.nii.gz").touch()

    state.loc[state["job_id"] == job_id, "status"] = "complete"
    save_state(state, cfg)
    print(f"  ✓ Job {job_id} complete: {procedure} for {subject}/{session}")


def print_state(cfg: SchedulerConfig) -> None:
    state = load_state(cfg)
    if state.empty:
        print("  (no state yet)")
        return
    summary = state.groupby("status").size()
    for status, count in summary.items():
        print(f"  {status:<10} {count}")


# ---------------------------------------------------------------------------
# Simulation
# ---------------------------------------------------------------------------

with tempfile.TemporaryDirectory() as tmp:
    tmp = Path(tmp)

    # Set up fake DICOM data for 3 subjects × 1 session
    subjects = ["sub-0001", "sub-0002", "sub-0003"]
    for sub in subjects:
        d = tmp / "dicom" / sub / "ses-01"
        d.mkdir(parents=True)
        (d / "file.dcm").touch()

    cfg = SchedulerConfig(
        dicom_root=tmp / "dicom",
        bids_root=tmp / "bids",
        derivatives_root=tmp / "derivatives",
        state_file=tmp / "state.parquet",
    )

    # --- Day 1: BIDS jobs submitted for all 3 subjects ---
    scheduler_run(cfg, day=1)

    # Overnight: bids jobs for sub-0001 and sub-0002 finish
    print("\n  [overnight] bids jobs complete for sub-0001 and sub-0002")
    state = load_state(cfg)
    for job_id in state[state["procedure"] == "bids"]["job_id"].tolist()[:2]:
        mark_job_complete(cfg, job_id)

    # --- Day 2: qsiprep + freesurfer submitted for completed subjects ---
    scheduler_run(cfg, day=2)

    # Overnight: bids for sub-0003 finishes; qsiprep for sub-0001 finishes
    print("\n  [overnight] bids complete for sub-0003; qsiprep complete for sub-0001")
    state = load_state(cfg)
    bids_0003 = state[(state["procedure"] == "bids") & (state["subject"] == "sub-0003")]["job_id"].iloc[0]
    mark_job_complete(cfg, bids_0003)
    qsiprep_0001 = state[(state["procedure"] == "qsiprep") & (state["subject"] == "sub-0001")]["job_id"].iloc[0]
    mark_job_complete(cfg, qsiprep_0001)

    # --- Day 3: sub-0003 gets downstream jobs; sub-0001 qsiprep already done ---
    scheduler_run(cfg, day=3)

    # Overnight: all remaining jobs finish
    print("\n  [overnight] all remaining jobs complete")
    state = load_state(cfg)
    for job_id in state[state["status"] == "pending"]["job_id"].tolist():
        mark_job_complete(cfg, job_id)
    for job_id in state[state["status"] == "running"]["job_id"].tolist():
        mark_job_complete(cfg, job_id)

    # --- Day 4: nothing to do ---
    scheduler_run(cfg, day=4)

    print(f"\n{'='*60}")
    print("  FINAL STATE")
    print(f"{'='*60}")
    print_state(cfg)
    print()
    final_state = load_state(cfg)
    print(final_state[["subject", "session", "procedure", "status", "job_id"]].to_string(index=False))

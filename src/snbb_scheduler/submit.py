from __future__ import annotations

__all__ = ["submit_task", "submit_manifest"]

import subprocess
from datetime import datetime, timezone

import pandas as pd

from snbb_scheduler.config import SchedulerConfig


def submit_task(row: pd.Series, config: SchedulerConfig, dry_run: bool = False) -> str | None:
    """Submit a single task to Slurm via sbatch.

    Returns the Slurm job ID on success, or None for dry runs.
    """
    proc = config.get_procedure(row["procedure"])
    cmd = [
        "sbatch",
        # f"--partition={config.slurm_partition}",
        f"--account={config.slurm_account}",
        f"--job-name={row['procedure']}_{row['subject']}_{row['session']}",
        proc.script,
        row["subject"],
        row["session"],
    ]

    if dry_run:
        print(f"[DRY RUN] Would submit: {' '.join(cmd)}")
        return None

    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    # sbatch output: "Submitted batch job 12345"
    return result.stdout.strip().split()[-1]


def submit_manifest(
    manifest: pd.DataFrame,
    config: SchedulerConfig,
    dry_run: bool = False,
) -> pd.DataFrame:
    """Submit all tasks in the manifest.

    Returns a DataFrame of new state rows (one per submitted task) with
    columns: subject, session, procedure, status, submitted_at, job_id.
    """
    new_rows = []
    now = datetime.now(tz=timezone.utc)

    for _, row in manifest.iterrows():
        job_id = submit_task(row, config, dry_run=dry_run)
        new_rows.append({
            "subject": row["subject"],
            "session": row["session"],
            "procedure": row["procedure"],
            "status": "pending",
            "submitted_at": now,
            "job_id": job_id,
        })

    if not new_rows:
        return pd.DataFrame(
            columns=["subject", "session", "procedure", "status", "submitted_at", "job_id"]
        )

    return pd.DataFrame(new_rows)

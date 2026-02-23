from __future__ import annotations

__all__ = ["submit_task", "submit_manifest"]

import subprocess
from datetime import datetime, timezone

import pandas as pd

from snbb_scheduler.config import SchedulerConfig


def submit_task(row: pd.Series, config: SchedulerConfig, dry_run: bool = False) -> str | None:
    """Submit a single task to Slurm via sbatch.

    Builds the sbatch command from the procedure's script and the row's
    subject/session values. The ``--partition`` flag is included only when
    ``config.slurm_partition`` is non-empty, so sites that do not use
    Slurm partitions can leave the field blank.

    Parameters
    ----------
    row:
        A manifest row with at least ``procedure``, ``subject``, and
        ``session`` keys.
    config:
        Scheduler configuration supplying Slurm settings and the procedure
        registry.
    dry_run:
        When *True*, prints the command that would be run and returns *None*
        without calling sbatch.

    Returns
    -------
    str or None
        The Slurm job ID string on success, or *None* for dry runs.

    Raises
    ------
    RuntimeError
        If sbatch exits successfully but its stdout does not match the
        expected ``"Submitted batch job <ID>"`` format.
    subprocess.CalledProcessError
        If sbatch exits with a non-zero status.
    """
    proc = config.get_procedure(row["procedure"])
    cmd = ["sbatch"]
    if config.slurm_partition:
        cmd.append(f"--partition={config.slurm_partition}")
    cmd.append(f"--account={config.slurm_account}")
    cmd.append(f"--job-name={row['procedure']}_{row['subject']}_{row['session']}")
    if config.slurm_mem:
        cmd.append(f"--mem={config.slurm_mem}")
    if config.slurm_cpus_per_task:
        cmd.append(f"--cpus-per-task={config.slurm_cpus_per_task}")
    cmd += [
        proc.script,
        row["subject"],
        row["session"],
    ]

    if dry_run:
        print(f"[DRY RUN] Would submit: {' '.join(cmd)}")
        return None

    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    # sbatch stdout: "Submitted batch job 12345"
    output = result.stdout.strip()
    if not output.startswith("Submitted batch job "):
        raise RuntimeError(
            f"Unexpected sbatch output: {output!r}. "
            "Expected format: 'Submitted batch job <ID>'"
        )
    return output.split()[-1]


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

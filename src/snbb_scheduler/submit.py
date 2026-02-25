from __future__ import annotations

__all__ = ["submit_task", "submit_manifest"]

import logging
import subprocess
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pandas as pd

from snbb_scheduler.config import SchedulerConfig

if TYPE_CHECKING:
    from snbb_scheduler.audit import AuditLogger

logger = logging.getLogger(__name__)


def _build_job_name(row: pd.Series, proc_scope: str) -> str:
    """Return the Slurm job name for a manifest row."""
    if proc_scope == "subject":
        return f"{row['procedure']}_{row['subject']}"
    return f"{row['procedure']}_{row['subject']}_{row['session']}"


def submit_task(
    row: pd.Series,
    config: SchedulerConfig,
    dry_run: bool = False,
    audit: AuditLogger | None = None,
) -> str | None:
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
    job_name = _build_job_name(row, proc.scope)
    cmd = ["sbatch"]
    if config.slurm_partition:
        cmd.append(f"--partition={config.slurm_partition}")
    cmd.append(f"--account={config.slurm_account}")
    cmd.append(f"--job-name={job_name}")
    if config.slurm_mem:
        cmd.append(f"--mem={config.slurm_mem}")
    if config.slurm_cpus_per_task:
        cmd.append(f"--cpus-per-task={config.slurm_cpus_per_task}")
    if config.slurm_log_dir is not None:
        log_subdir = config.slurm_log_dir / row["procedure"]
        log_subdir.mkdir(parents=True, exist_ok=True)
        cmd.append(f"--output={log_subdir}/{job_name}_%j.out")
        cmd.append(f"--error={log_subdir}/{job_name}_%j.err")
    cmd.append(proc.script)
    cmd.append(row["subject"])
    if proc.scope != "subject":
        cmd.append(row["session"])
        dicom_path = row.get("dicom_path")
        if dicom_path is not None and not (isinstance(dicom_path, float) and pd.isna(dicom_path)):
            cmd.append(str(dicom_path))

    if dry_run:
        logger.info("[DRY RUN] Would submit: %s", " ".join(cmd))
        print(f"[DRY RUN] Would submit: {' '.join(cmd)}")
        if audit is not None:
            audit.log(
                "dry_run",
                subject=row["subject"],
                session=row["session"],
                procedure=row["procedure"],
                detail=" ".join(cmd),
            )
        return None
    logger.info("Submitting: %s", " ".join(cmd))
    print(f"Submitting: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        if audit is not None:
            audit.log(
                "error",
                subject=row["subject"],
                session=row["session"],
                procedure=row["procedure"],
                detail=str(e),
            )
        raise
    # sbatch stdout: "Submitted batch job 12345"
    output = result.stdout.strip()
    if not output.startswith("Submitted batch job "):
        raise RuntimeError(
            f"Unexpected sbatch output: {output!r}. "
            "Expected format: 'Submitted batch job <ID>'"
        )
    job_id = output.split()[-1]
    if audit is not None:
        audit.log(
            "submitted",
            subject=row["subject"],
            session=row["session"],
            procedure=row["procedure"],
            job_id=job_id,
        )
    return job_id


def submit_manifest(
    manifest: pd.DataFrame,
    config: SchedulerConfig,
    dry_run: bool = False,
    audit: AuditLogger | None = None,
) -> pd.DataFrame:
    """Submit all tasks in the manifest.

    Returns a DataFrame of new state rows (one per submitted task) with
    columns: subject, session, procedure, status, submitted_at, job_id.
    """
    new_rows = []
    now = datetime.now(tz=timezone.utc)

    for _, row in manifest.iterrows():
        job_id = submit_task(row, config, dry_run=dry_run, audit=audit)
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

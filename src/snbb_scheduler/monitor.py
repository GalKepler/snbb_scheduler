from __future__ import annotations

__all__ = ["poll_jobs", "update_state_from_sacct"]

import logging
import subprocess
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from snbb_scheduler.audit import AuditLogger

logger = logging.getLogger(__name__)

_SLURM_STATE_MAP: dict[str, str] = {
    "PENDING": "pending",
    "RUNNING": "running",
    "COMPLETED": "complete",
    "FAILED": "failed",
    "TIMEOUT": "failed",
    "CANCELLED": "failed",
    "OUT_OF_MEMORY": "failed",
    "NODE_FAIL": "failed",
}


def poll_jobs(job_ids: list[str]) -> dict[str, str]:
    """Query sacct for the current state of each job ID.

    Parameters
    ----------
    job_ids:
        List of Slurm job ID strings to query.

    Returns
    -------
    dict mapping job_id → scheduler status string, or ``{}`` on error.
    """
    if not job_ids:
        return {}

    ids_str = ",".join(job_ids)
    cmd = [
        "sacct",
        "-j", ids_str,
        "--format=JobID,State",
        "--noheader",
        "--parsable2",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    except FileNotFoundError:
        logger.warning("sacct not found; skipping job status update.")
        return {}
    except subprocess.CalledProcessError as e:
        logger.warning("sacct failed: %s", e)
        return {}

    states: dict[str, str] = {}
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split("|")
        if len(parts) < 2:
            continue
        job_id, raw_state = parts[0], parts[1]
        # Skip sub-steps (job IDs containing ".")
        if "." in job_id:
            continue
        # Normalize state suffix (e.g. "CANCELLED by user" → "CANCELLED")
        slurm_state = raw_state.split()[0]
        scheduler_status = _SLURM_STATE_MAP.get(slurm_state)
        if scheduler_status is not None:
            states[job_id] = scheduler_status

    return states


def update_state_from_sacct(
    state: pd.DataFrame,
    audit: AuditLogger | None = None,
) -> pd.DataFrame:
    """Poll sacct for in-flight jobs and update their statuses.

    Parameters
    ----------
    state:
        Current scheduler state DataFrame.
    audit:
        Optional audit logger for recording status transitions.

    Returns
    -------
    A modified copy of *state* with updated statuses. The original is unchanged.
    """
    if state.empty:
        return state.copy()

    in_flight_mask = state["status"].isin(["pending", "running"])
    in_flight = state[in_flight_mask]

    if in_flight.empty:
        return state.copy()

    job_ids = [
        str(jid) for jid in in_flight["job_id"].dropna().unique()
        if str(jid) not in ("", "None")
    ]

    if not job_ids:
        return state.copy()

    new_states = poll_jobs(job_ids)
    if not new_states:
        return state.copy()

    updated = state.copy()
    for idx in in_flight.index:
        job_id = str(state.at[idx, "job_id"])
        if job_id not in new_states:
            continue
        new_status = new_states[job_id]
        old_status = state.at[idx, "status"]
        if new_status == old_status:
            continue
        updated.at[idx, "status"] = new_status
        if audit is not None:
            audit.log(
                "status_change",
                subject=state.at[idx, "subject"],
                session=state.at[idx, "session"],
                procedure=state.at[idx, "procedure"],
                job_id=job_id,
                old_status=old_status,
                new_status=new_status,
            )

    return updated

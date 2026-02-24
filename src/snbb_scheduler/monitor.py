"""monitor.py — Slurm job state polling via sacct.

Provides helpers to query ``sacct`` for in-flight jobs and update the
scheduler state parquet file accordingly.

Typical usage::

    from snbb_scheduler.monitor import update_state_from_sacct
    from snbb_scheduler.manifest import load_state, save_state
    from snbb_scheduler.audit import get_logger

    state = load_state(config)
    state = update_state_from_sacct(state, audit=get_logger(config))
    save_state(state, config)
"""
from __future__ import annotations

__all__ = ["poll_jobs", "update_state_from_sacct"]

import logging
import subprocess
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from snbb_scheduler.audit import AuditLogger

logger = logging.getLogger(__name__)

# Mapping from sacct state strings to scheduler status strings.
_SACCT_TO_STATUS: dict[str, str] = {
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
    """Query sacct and return a mapping of job_id → scheduler status.

    Only top-level job steps are returned (sub-steps with "." in the ID are
    skipped).  Unknown sacct states are silently ignored so new Slurm state
    strings don't break the scheduler.

    Parameters
    ----------
    job_ids:
        List of Slurm job ID strings to query.

    Returns
    -------
    dict[str, str]
        Mapping ``{job_id: status}`` where *status* is one of
        ``pending``, ``running``, ``complete``, or ``failed``.
    """
    if not job_ids:
        return {}

    ids_arg = ",".join(str(j) for j in job_ids)
    try:
        result = subprocess.run(
            [
                "sacct",
                "-j", ids_arg,
                "--format=JobID,State",
                "--noheader",
                "--parsable2",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        logger.warning("sacct call failed: %s", exc)
        return {}

    statuses: dict[str, str] = {}
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split("|")
        if len(parts) < 2:
            continue
        job_id, sacct_state = parts[0], parts[1]
        # Skip sub-steps (e.g. "12345.batch", "12345.0")
        if "." in job_id:
            continue
        # Strip trailing state qualifiers like "+", " by user"
        sacct_state = sacct_state.split()[0].rstrip("+")
        scheduler_status = _SACCT_TO_STATUS.get(sacct_state)
        if scheduler_status is not None:
            statuses[job_id] = scheduler_status

    return statuses


def update_state_from_sacct(
    state: pd.DataFrame,
    audit: "AuditLogger | None" = None,
) -> pd.DataFrame:
    """Refresh in-flight rows by polling sacct.

    Finds all rows with ``status`` of ``pending`` or ``running``, calls
    :func:`poll_jobs`, and updates any rows whose status has changed.

    Parameters
    ----------
    state:
        Scheduler state DataFrame with columns including ``job_id`` and
        ``status``.
    audit:
        Optional :class:`~snbb_scheduler.audit.AuditLogger` to record
        ``status_change`` events.

    Returns
    -------
    pd.DataFrame
        Updated state DataFrame (a copy — the original is not mutated).
    """
    if state.empty:
        return state

    in_flight_mask = state["status"].isin({"pending", "running"})
    if not in_flight_mask.any():
        return state

    job_ids = (
        state.loc[in_flight_mask, "job_id"]
        .dropna()
        .astype(str)
        .tolist()
    )
    if not job_ids:
        return state

    polled = poll_jobs(job_ids)
    if not polled:
        return state

    state = state.copy()
    for idx in state.index[in_flight_mask]:
        job_id = str(state.at[idx, "job_id"])
        new_status = polled.get(job_id)
        if new_status is None:
            continue
        old_status = state.at[idx, "status"]
        if new_status == old_status:
            continue
        logger.info(
            "job %s (%s/%s/%s): %s → %s",
            job_id,
            state.at[idx, "subject"],
            state.at[idx, "session"],
            state.at[idx, "procedure"],
            old_status,
            new_status,
        )
        state.at[idx, "status"] = new_status
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

    return state

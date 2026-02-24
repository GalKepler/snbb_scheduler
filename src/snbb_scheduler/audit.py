"""audit.py — JSONL audit logger for snbb_scheduler.

Each submitted, skipped, or status-changed event is appended as a single
JSON object (one line) to the audit log file.  The file is created (with
parent directories) on the first write if it does not already exist.

Typical usage::

    from snbb_scheduler.audit import get_logger

    audit = get_logger(config)
    audit.log("submitted", subject="sub-0001", session="ses-01",
               procedure="bids", job_id="12345")
"""
from __future__ import annotations

__all__ = ["AuditLogger", "get_logger"]

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from snbb_scheduler.config import SchedulerConfig

logger = logging.getLogger(__name__)

#: Valid event names for the audit log.
AUDIT_EVENTS = frozenset(
    {"submitted", "status_change", "skipped", "error", "dry_run", "retry_cleared"}
)


class AuditLogger:
    """Appends structured JSON Lines entries to an audit log file.

    Parameters
    ----------
    log_file:
        Path to the JSONL audit file.  Parent directories are created
        automatically on the first write.
    """

    def __init__(self, log_file: Path) -> None:
        self.log_file = log_file

    def log(
        self,
        event: str,
        *,
        subject: str = "",
        session: str = "",
        procedure: str = "",
        job_id: str | None = None,
        detail: str = "",
        old_status: str = "",
        new_status: str = "",
        **extra: Any,
    ) -> None:
        """Append a single audit event as a JSON line.

        Parameters
        ----------
        event:
            One of ``submitted``, ``status_change``, ``skipped``,
            ``error``, ``dry_run``, ``retry_cleared``.
        subject:
            Subject label (e.g. ``sub-0001``).
        session:
            Session label (e.g. ``ses-01``).  Empty for subject-scoped procedures.
        procedure:
            Procedure name (e.g. ``bids``).
        job_id:
            Slurm job ID string, or ``None`` for dry-run / non-submitted events.
        detail:
            Free-text detail message.
        old_status / new_status:
            Used for ``status_change`` events.
        **extra:
            Any additional key-value pairs to include in the log entry.
        """
        entry: dict[str, Any] = {
            "ts": datetime.now(tz=timezone.utc).isoformat(),
            "event": event,
            "subject": subject,
            "session": session,
            "procedure": procedure,
            "job_id": job_id,
            "detail": detail,
            "old_status": old_status,
            "new_status": new_status,
        }
        entry.update(extra)

        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        with self.log_file.open("a") as fh:
            fh.write(json.dumps(entry) + "\n")

        logger.debug("audit %s: %s/%s/%s job_id=%s", event, subject, session, procedure, job_id)


def get_logger(config: SchedulerConfig) -> AuditLogger:
    """Return an :class:`AuditLogger` for *config*.

    Uses ``config.log_file`` when set; otherwise defaults to
    ``<state_file parent>/scheduler_audit.jsonl``.
    """
    if config.log_file is not None:
        log_file = config.log_file
    else:
        log_file = config.state_file.parent / "scheduler_audit.jsonl"
    return AuditLogger(log_file)

from __future__ import annotations

__all__ = ["AuditLogger", "get_logger"]

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from snbb_scheduler.config import SchedulerConfig

logger = logging.getLogger(__name__)


class AuditLogger:
    """Appends JSONL records to a log file. One record per event."""

    def __init__(self, log_file: Path) -> None:
        self._log_file = log_file

    def log(
        self,
        event: str,
        *,
        subject: str = "",
        session: str = "",
        procedure: str = "",
        job_id: str | None = None,
        old_status: str | None = None,
        new_status: str | None = None,
        detail: str = "",
        **extra,
    ) -> None:
        """Append a single JSONL record."""
        record: dict = {
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "event": event,
            "subject": subject,
            "session": session,
            "procedure": procedure,
        }
        if job_id is not None:
            record["job_id"] = job_id
        if old_status is not None:
            record["old_status"] = old_status
        if new_status is not None:
            record["new_status"] = new_status
        if detail:
            record["detail"] = detail
        record.update(extra)

        self._log_file.parent.mkdir(parents=True, exist_ok=True)
        with self._log_file.open("a") as f:
            f.write(json.dumps(record) + "\n")


def get_logger(config: SchedulerConfig) -> AuditLogger:
    """Return an AuditLogger for the given config.

    Uses ``config.log_file`` if set; otherwise defaults to
    ``<state_file parent>/scheduler_audit.jsonl``.
    """
    log_file = config.log_file or (config.state_file.parent / "scheduler_audit.jsonl")
    return AuditLogger(log_file)

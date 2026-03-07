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

_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>SNBB Scheduler Audit Report</title>
<style>
  body {{ font-family: sans-serif; margin: 2rem; color: #222; }}
  h1 {{ font-size: 1.4rem; margin-bottom: 0.25rem; }}
  p.meta {{ color: #666; font-size: 0.85rem; margin-top: 0; }}
  table {{ border-collapse: collapse; width: 100%; font-size: 0.875rem; }}
  th {{ background: #2c3e50; color: #fff; padding: 0.5rem 0.75rem; text-align: left; }}
  td {{ padding: 0.4rem 0.75rem; border-bottom: 1px solid #e0e0e0; vertical-align: top; }}
  tr:nth-child(even) {{ background: #f7f7f7; }}
  .badge {{ display: inline-block; border-radius: 3px; padding: 1px 6px;
            font-size: 0.78rem; font-weight: bold; }}
  .badge-submitted  {{ background:#d4edda; color:#155724; }}
  .badge-status_change {{ background:#cce5ff; color:#004085; }}
  .badge-error      {{ background:#f8d7da; color:#721c24; }}
  .badge-dry_run    {{ background:#fff3cd; color:#856404; }}
  .badge-retry_cleared {{ background:#d1ecf1; color:#0c5460; }}
  .badge-default    {{ background:#e2e3e5; color:#383d41; }}
</style>
</head>
<body>
<h1>SNBB Scheduler Audit Report</h1>
<p class="meta">Generated: {generated_at} &mdash; {n_records} event(s)</p>
<table>
<thead>
<tr>
  <th>Timestamp</th><th>Event</th><th>Subject</th><th>Session</th>
  <th>Procedure</th><th>Job ID</th><th>Status change</th><th>Detail</th>
</tr>
</thead>
<tbody>
{rows}
</tbody>
</table>
</body>
</html>
"""

_ROW_TEMPLATE = (
    "<tr>"
    "<td>{timestamp}</td>"
    "<td><span class=\"badge badge-{badge_class}\">{event}</span></td>"
    "<td>{subject}</td>"
    "<td>{session}</td>"
    "<td>{procedure}</td>"
    "<td>{job_id}</td>"
    "<td>{status_change}</td>"
    "<td>{detail}</td>"
    "</tr>"
)


def _badge_class(event: str) -> str:
    known = {"submitted", "status_change", "error", "dry_run", "retry_cleared"}
    return event if event in known else "default"


def _render_html(records: list[dict]) -> str:
    rows = []
    for r in records:
        status_change = ""
        if "old_status" in r or "new_status" in r:
            status_change = f"{r.get('old_status', '')} → {r.get('new_status', '')}"
        rows.append(
            _ROW_TEMPLATE.format(
                timestamp=r.get("timestamp", ""),
                event=r.get("event", ""),
                badge_class=_badge_class(r.get("event", "")),
                subject=r.get("subject", ""),
                session=r.get("session", ""),
                procedure=r.get("procedure", ""),
                job_id=r.get("job_id", ""),
                status_change=status_change,
                detail=r.get("detail", ""),
            )
        )
    return _HTML_TEMPLATE.format(
        generated_at=datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        n_records=len(records),
        rows="\n".join(rows),
    )


class AuditLogger:
    """Appends JSONL records to a log file and keeps an HTML report up to date."""

    def __init__(self, log_file: Path, report_dir: Path | None = None) -> None:
        self._log_file = log_file
        self._report_dir = report_dir

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
        """Append a single JSONL record and refresh the HTML report."""
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

        if self._report_dir is not None:
            self._write_html_report()

    def _write_html_report(self) -> None:
        """Regenerate audit_report.html in report_dir from the current JSONL log."""
        records: list[dict] = []
        if self._log_file.exists():
            for line in self._log_file.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass

        self._report_dir.mkdir(parents=True, exist_ok=True)  # type: ignore[union-attr]
        report_path = self._report_dir / "audit_report.html"
        report_path.write_text(_render_html(records), encoding="utf-8")


def get_logger(config: SchedulerConfig) -> AuditLogger:
    """Return an AuditLogger for the given config.

    Uses ``config.log_file`` if set; otherwise defaults to
    ``<state_file parent>/scheduler_audit.jsonl``.
    The HTML report is written to ``config.audit.report_dir`` when set.
    """
    log_file = config.log_file or (config.state_file.parent / "scheduler_audit.jsonl")
    return AuditLogger(log_file, report_dir=config.audit.report_dir)

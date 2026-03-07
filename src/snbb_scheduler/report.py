from __future__ import annotations

__all__ = [
    "render_markdown",
    "render_html",
    "render_json",
    "save_report",
    "send_report_email",
    "load_previous_report",
    "compare_reports",
]

import dataclasses
import json
import re
import smtplib
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from snbb_scheduler.auditor import AuditReport, SessionAuditResult


# ---------------------------------------------------------------------------
# Serialization helpers
# ---------------------------------------------------------------------------

def _report_to_dict(report: AuditReport) -> dict:
    """Convert an AuditReport to a JSON-serialisable dict."""
    return dataclasses.asdict(report)


def _dict_to_report(d: dict) -> AuditReport:
    """Reconstruct an AuditReport from a previously serialised dict."""
    from snbb_scheduler.auditor import (
        DicomAuditResult,
        ProcedureAuditResult,
        ProcedureSummary,
        SessionAuditResult,
    )
    from snbb_scheduler.checks import FileCheckResult
    from snbb_scheduler.log_analyzer import LogFinding

    session_results = []
    for sr in d.get("session_results", []):
        dicom = DicomAuditResult(**sr["dicom"])
        procs = {}
        for pname, pr in sr.get("procedures", {}).items():
            file_checks = [FileCheckResult(**fc) for fc in pr.pop("file_checks", [])]
            log_findings = [LogFinding(**lf) for lf in pr.pop("log_findings", [])]
            procs[pname] = ProcedureAuditResult(
                **pr, file_checks=file_checks, log_findings=log_findings
            )
        session_results.append(
            SessionAuditResult(
                subject=sr["subject"],
                session=sr["session"],
                dicom=dicom,
                procedures=procs,
                health_score=sr["health_score"],
            )
        )

    summaries = []
    for s in d.get("procedure_summaries", []):
        summaries.append(
            ProcedureSummary(
                procedure=s["procedure"],
                total_sessions=s["total_sessions"],
                complete=s["complete"],
                incomplete=s["incomplete"],
                failed=s["failed"],
                not_started=s["not_started"],
                stale=s["stale"],
                common_errors=[tuple(e) for e in s.get("common_errors", [])],
            )
        )

    return AuditReport(
        timestamp=d["timestamp"],
        config_summary=d.get("config_summary", {}),
        session_results=session_results,
        procedure_summaries=summaries,
    )


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

_STATUS_ICON = {
    "complete": "✓",
    "incomplete": "~",
    "failed": "✗",
    "not_started": "-",
    "pending": "…",
    "running": "↻",
}


def _status_icon(status: str) -> str:
    return _STATUS_ICON.get(status, "?")


def render_markdown(report: AuditReport) -> str:
    """Render an AuditReport as a Markdown document."""
    lines: list[str] = []

    lines.append(f"# SNBB Scheduler Audit Report")
    lines.append(f"\nGenerated: {report.timestamp}\n")

    # -- Executive Summary --------------------------------------------------
    n_sessions = len(report.session_results)
    healthy = sum(1 for s in report.session_results if s.health_score >= 0.8)
    degraded = sum(1 for s in report.session_results if 0.4 <= s.health_score < 0.8)
    critical = sum(1 for s in report.session_results if s.health_score < 0.4)

    lines.append("## Executive Summary\n")
    lines.append(f"| Metric | Value |")
    lines.append(f"|--------|-------|")
    lines.append(f"| Total sessions | {n_sessions} |")
    lines.append(f"| Healthy (≥80%) | {healthy} |")
    lines.append(f"| Degraded (40-80%) | {degraded} |")
    lines.append(f"| Critical (<40%) | {critical} |")
    lines.append("")

    # -- DICOM Issues -------------------------------------------------------
    dicom_issues = [
        s for s in report.session_results if not s.dicom.exists or s.dicom.is_suspicious
    ]
    if dicom_issues:
        lines.append("## DICOM Source Issues\n")
        lines.append("| Subject | Session | Issue |")
        lines.append("|---------|---------|-------|")
        for s in dicom_issues:
            lines.append(f"| {s.subject} | {s.session} | {s.dicom.detail} |")
        lines.append("")

    # -- Stale Jobs ---------------------------------------------------------
    stale_entries: list[tuple[str, str, str, float]] = []
    for s in report.session_results:
        for proc_name, pr in s.procedures.items():
            if pr.is_stale and pr.job_age_hours is not None:
                stale_entries.append((s.subject, s.session, proc_name, pr.job_age_hours))

    if stale_entries:
        lines.append("## Stale Jobs\n")
        lines.append("| Subject | Session | Procedure | Age (hours) |")
        lines.append("|---------|---------|-----------|-------------|")
        for subj, ses, proc, age in stale_entries:
            lines.append(f"| {subj} | {ses} | {proc} | {age:.1f} |")
        lines.append("")

    # -- Procedure Status Matrix --------------------------------------------
    if report.session_results:
        proc_names = list(report.session_results[0].procedures.keys()) if report.session_results else []
        if proc_names:
            lines.append("## Procedure Status Matrix\n")
            header = "| Subject | Session | " + " | ".join(proc_names) + " |"
            sep = "|---------|---------|" + "|".join("-" * max(len(p) + 2, 4) for p in proc_names) + "|"
            lines.append(header)
            lines.append(sep)
            for s in report.session_results:
                icons = [_status_icon(s.procedures.get(p, type("_", (), {"status": "not_started"})()).status) for p in proc_names]
                lines.append(f"| {s.subject} | {s.session} | " + " | ".join(icons) + " |")
            lines.append("")

    # -- Procedure Summaries -----------------------------------------------
    if report.procedure_summaries:
        lines.append("## Procedure Summaries\n")
        lines.append("| Procedure | Total | Complete | Incomplete | Failed | Not Started | Stale |")
        lines.append("|-----------|-------|----------|------------|--------|-------------|-------|")
        for ps in report.procedure_summaries:
            lines.append(
                f"| {ps.procedure} | {ps.total_sessions} | {ps.complete} | "
                f"{ps.incomplete} | {ps.failed} | {ps.not_started} | {ps.stale} |"
            )
        lines.append("")

        # Common errors per procedure
        for ps in report.procedure_summaries:
            if ps.common_errors:
                lines.append(f"### {ps.procedure} — Common Errors\n")
                for pattern, count in ps.common_errors:
                    lines.append(f"- `{pattern}`: {count} occurrence(s)")
                lines.append("")

    # -- Log Analysis -------------------------------------------------------
    all_findings: list[tuple[str, str, str, str, int, str]] = []
    for s in report.session_results:
        for proc_name, pr in s.procedures.items():
            for f in pr.log_findings:
                all_findings.append(
                    (s.subject, s.session, proc_name, f.pattern_name, f.line_number, f.line_text[:80])
                )

    if all_findings:
        lines.append("## Log Analysis\n")
        lines.append("| Subject | Session | Procedure | Pattern | Line | Text |")
        lines.append("|---------|---------|-----------|---------|------|------|")
        for subj, ses, proc, pat, lineno, text in all_findings[:50]:
            lines.append(f"| {subj} | {ses} | {proc} | `{pat}` | {lineno} | `{text}` |")
        if len(all_findings) > 50:
            lines.append(f"\n_...and {len(all_findings) - 50} more findings._")
        lines.append("")

    return "\n".join(lines)


def render_html(report: AuditReport) -> str:
    """Render an AuditReport as an HTML document (Markdown wrapped in HTML)."""
    md = render_markdown(report)
    # Simple conversion: wrap in minimal HTML
    body = _md_to_html_basic(md)
    return (
        "<!DOCTYPE html>\n"
        "<html>\n"
        "<head>\n"
        '  <meta charset="utf-8">\n'
        "  <title>SNBB Audit Report</title>\n"
        "  <style>\n"
        "    body { font-family: sans-serif; max-width: 1100px; margin: 2em auto; }\n"
        "    table { border-collapse: collapse; width: 100%; }\n"
        "    th, td { border: 1px solid #ccc; padding: 4px 8px; text-align: left; }\n"
        "    th { background: #f0f0f0; }\n"
        "    code { background: #f8f8f8; padding: 0 3px; }\n"
        "    pre { background: #f8f8f8; padding: 1em; overflow: auto; }\n"
        "  </style>\n"
        "</head>\n"
        "<body>\n"
        f"{body}\n"
        "</body>\n"
        "</html>\n"
    )


def _md_to_html_basic(md: str) -> str:
    """Very minimal Markdown → HTML conversion (headers, tables, code, bold)."""
    lines = md.split("\n")
    html_lines = []
    in_table = False
    table_header_done = False

    for line in lines:
        # Headers
        m = re.match(r"^(#{1,6})\s+(.*)", line)
        if m:
            if in_table:
                html_lines.append("</table>")
                in_table = False
                table_header_done = False
            level = len(m.group(1))
            html_lines.append(f"<h{level}>{_inline_md(m.group(2))}</h{level}>")
            continue

        # Table rows
        if line.startswith("|") and line.endswith("|"):
            # separator row
            if re.match(r"^\|[-| ]+\|$", line):
                if not in_table:
                    pass
                elif not table_header_done:
                    table_header_done = True
                continue

            cells = [c.strip() for c in line.strip("|").split("|")]
            if not in_table:
                html_lines.append("<table>")
                in_table = True
                table_header_done = False
                tag = "th"
            elif not table_header_done:
                tag = "th"
            else:
                tag = "td"

            row_html = "".join(f"<{tag}>{_inline_md(c)}</{tag}>" for c in cells)
            html_lines.append(f"<tr>{row_html}</tr>")
            continue

        if in_table:
            html_lines.append("</table>")
            in_table = False
            table_header_done = False

        # List items
        m = re.match(r"^- (.*)", line)
        if m:
            html_lines.append(f"<li>{_inline_md(m.group(1))}</li>")
            continue

        # Blank line
        if not line.strip():
            html_lines.append("<br>")
            continue

        html_lines.append(f"<p>{_inline_md(line)}</p>")

    if in_table:
        html_lines.append("</table>")

    return "\n".join(html_lines)


def _inline_md(text: str) -> str:
    """Convert inline Markdown (code, bold, italic) to HTML."""
    text = re.sub(r"`([^`]+)`", r"<code>\1</code>", text)
    text = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", text)
    text = re.sub(r"\*([^*]+)\*", r"<em>\1</em>", text)
    text = re.sub(r"_([^_]+)_", r"<em>\1</em>", text)
    return text


def render_json(report: AuditReport) -> str:
    """Render an AuditReport as a JSON string."""
    return json.dumps(_report_to_dict(report), indent=2, default=str)


# ---------------------------------------------------------------------------
# File I/O
# ---------------------------------------------------------------------------

def save_report(
    report: AuditReport,
    output_dir: Path,
    fmt: str = "markdown",
) -> Path:
    """Save a report to a timestamped file.

    Parameters
    ----------
    report:
        The audit report to save.
    output_dir:
        Directory to write into (created if needed).
    fmt:
        One of ``"markdown"``, ``"html"``, ``"json"``.

    Returns
    -------
    Path
        Path of the written file.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    ext = {"markdown": "md", "html": "html", "json": "json"}.get(fmt, "md")
    out_path = output_dir / f"audit_{ts}.{ext}"

    if fmt == "html":
        content = render_html(report)
    elif fmt == "json":
        content = render_json(report)
    else:
        content = render_markdown(report)

    out_path.write_text(content, encoding="utf-8")
    return out_path


# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------

def send_report_email(
    report: AuditReport,
    recipients: list[str],
    from_address: str = "snbb-scheduler@localhost",
) -> None:
    """Send the audit report as an HTML email via localhost SMTP.

    Requires a local MTA (sendmail/postfix) listening on port 25.
    No authentication is performed.

    Parameters
    ----------
    report:
        Report to send.
    recipients:
        List of email addresses to send to.
    from_address:
        Sender address.

    Raises
    ------
    smtplib.SMTPException
        On SMTP errors.
    OSError
        If the local MTA is not reachable.
    """
    html_body = render_html(report)
    text_body = render_markdown(report)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"SNBB Audit Report — {report.timestamp}"
    msg["From"] = from_address
    msg["To"] = ", ".join(recipients)

    msg.attach(MIMEText(text_body, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    with smtplib.SMTP("localhost", 25) as smtp:
        smtp.sendmail(from_address, recipients, msg.as_string())


# ---------------------------------------------------------------------------
# History
# ---------------------------------------------------------------------------

def load_previous_report(report_dir: Path) -> AuditReport | None:
    """Load the most recent saved JSON audit report from *report_dir*.

    Returns ``None`` if no reports exist or the directory is absent.
    """
    report_dir = Path(report_dir)
    if not report_dir.exists():
        return None

    candidates = sorted(report_dir.glob("audit_*.json"), reverse=True)
    if not candidates:
        return None

    try:
        data = json.loads(candidates[0].read_text(encoding="utf-8"))
        return _dict_to_report(data)
    except Exception:  # noqa: BLE001
        return None


def compare_reports(current: AuditReport, previous: AuditReport) -> dict:
    """Compute deltas between two audit reports.

    Returns a dict with keys:
    - ``new_completions``: list of (subject, session, procedure) newly complete
    - ``new_failures``: list of (subject, session, procedure) newly failed
    - ``health_trend``: average health score change (positive = improvement)
    - ``sessions_added``: count of sessions in current but not previous
    - ``sessions_removed``: count of sessions in previous but not current
    """
    def _session_key(s: SessionAuditResult) -> tuple[str, str]:
        return (s.subject, s.session)

    def _proc_statuses(results: list[SessionAuditResult]) -> dict[tuple, str]:
        statuses: dict[tuple, str] = {}
        for s in results:
            for pname, pr in s.procedures.items():
                statuses[(s.subject, s.session, pname)] = pr.status
        return statuses

    prev_keys = {_session_key(s) for s in previous.session_results}
    curr_keys = {_session_key(s) for s in current.session_results}

    sessions_added = len(curr_keys - prev_keys)
    sessions_removed = len(prev_keys - curr_keys)

    prev_statuses = _proc_statuses(previous.session_results)
    curr_statuses = _proc_statuses(current.session_results)

    new_completions = []
    new_failures = []

    for key, curr_status in curr_statuses.items():
        prev_status = prev_statuses.get(key)
        if prev_status != "complete" and curr_status == "complete":
            new_completions.append(key)
        if prev_status != "failed" and curr_status == "failed":
            new_failures.append(key)

    prev_avg = (
        sum(s.health_score for s in previous.session_results) / len(previous.session_results)
        if previous.session_results else 0.0
    )
    curr_avg = (
        sum(s.health_score for s in current.session_results) / len(current.session_results)
        if current.session_results else 0.0
    )

    return {
        "new_completions": new_completions,
        "new_failures": new_failures,
        "health_trend": curr_avg - prev_avg,
        "sessions_added": sessions_added,
        "sessions_removed": sessions_removed,
    }

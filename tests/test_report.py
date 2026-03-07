"""Tests for report.py — rendering, email, history."""
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from snbb_scheduler.auditor import (
    AuditReport,
    DicomAuditResult,
    ProcedureAuditResult,
    ProcedureSummary,
    SessionAuditResult,
)
from snbb_scheduler.checks import FileCheckResult
from snbb_scheduler.log_analyzer import LogFinding
from snbb_scheduler.report import (
    _report_to_dict,
    compare_reports,
    load_previous_report,
    render_html,
    render_json,
    render_markdown,
    save_report,
    send_report_email,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_dicom(subject="sub-0001", session="ses-01", exists=True, suspicious=False):
    return DicomAuditResult(
        subject=subject,
        session=session,
        dicom_path=f"/data/dicom/{subject}/{session}",
        exists=exists,
        file_count=20 if exists else 0,
        has_expected_structure=exists,
        is_suspicious=suspicious,
        detail="20 file(s)" if exists else "DICOM directory does not exist",
    )


def _make_proc_result(procedure="bids", status="complete", subject="sub-0001", session="ses-01"):
    return ProcedureAuditResult(
        procedure=procedure,
        subject=subject,
        session=session,
        status=status,
        file_checks=[
            FileCheckResult(pattern="anat/*.nii.gz", found=True, matched_files=["/data/bids/anat/t1.nii.gz"]),
        ],
        completeness_ratio=1.0 if status == "complete" else 0.0,
        state_status=status,
        is_stale=False,
        job_age_hours=None,
        log_findings=[],
    )


def _make_session(subject="sub-0001", session="ses-01", proc_status="complete"):
    return SessionAuditResult(
        subject=subject,
        session=session,
        dicom=_make_dicom(subject, session),
        procedures={"bids": _make_proc_result(status=proc_status, subject=subject, session=session)},
        health_score=1.0 if proc_status == "complete" else 0.0,
    )


def _make_report(n_sessions=2, proc_status="complete"):
    sessions = [_make_session(f"sub-{i:04d}", "ses-01", proc_status) for i in range(1, n_sessions + 1)]
    summaries = [
        ProcedureSummary(
            procedure="bids",
            total_sessions=n_sessions,
            complete=n_sessions if proc_status == "complete" else 0,
            incomplete=0,
            failed=0,
            not_started=n_sessions if proc_status != "complete" else 0,
            stale=0,
            common_errors=[],
        )
    ]
    return AuditReport(
        timestamp=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        config_summary={"dicom_root": "/data/dicom", "procedures": ["bids"]},
        session_results=sessions,
        procedure_summaries=summaries,
    )


# ---------------------------------------------------------------------------
# render_markdown
# ---------------------------------------------------------------------------


def test_render_markdown_returns_string():
    report = _make_report()
    md = render_markdown(report)
    assert isinstance(md, str)
    assert len(md) > 0


def test_render_markdown_contains_sections(tmp_path):
    report = _make_report()
    md = render_markdown(report)
    assert "Executive Summary" in md
    assert "Procedure Summaries" in md


def test_render_markdown_shows_session_subjects():
    report = _make_report(n_sessions=2)
    md = render_markdown(report)
    assert "sub-0001" in md
    assert "sub-0002" in md


def test_render_markdown_dicom_issues_shown():
    report = _make_report(n_sessions=1)
    report.session_results[0].dicom.exists = False
    report.session_results[0].dicom.is_suspicious = True
    report.session_results[0].dicom.detail = "DICOM directory does not exist"
    md = render_markdown(report)
    assert "DICOM Source Issues" in md


def test_render_markdown_stale_jobs_section():
    report = _make_report(n_sessions=1)
    report.session_results[0].procedures["bids"].is_stale = True
    report.session_results[0].procedures["bids"].job_age_hours = 200.0
    md = render_markdown(report)
    assert "Stale Jobs" in md


def test_render_markdown_log_findings():
    report = _make_report(n_sessions=1)
    report.session_results[0].procedures["bids"].log_findings = [
        LogFinding(
            pattern_name="oom",
            severity="error",
            line_number=5,
            line_text="out of memory",
            log_file="/logs/job.out",
        )
    ]
    md = render_markdown(report)
    assert "Log Analysis" in md
    assert "oom" in md


def test_render_markdown_empty_report():
    report = AuditReport(
        timestamp="2024-01-01T00:00:00Z",
        config_summary={},
        session_results=[],
        procedure_summaries=[],
    )
    md = render_markdown(report)
    assert "Executive Summary" in md


# ---------------------------------------------------------------------------
# render_html
# ---------------------------------------------------------------------------


def test_render_html_is_valid_html():
    report = _make_report()
    html = render_html(report)
    assert "<!DOCTYPE html>" in html
    assert "<html>" in html
    assert "</html>" in html
    assert "<body>" in html


def test_render_html_contains_content():
    report = _make_report(n_sessions=1)
    html = render_html(report)
    assert "sub-0001" in html
    assert "bids" in html


# ---------------------------------------------------------------------------
# render_json
# ---------------------------------------------------------------------------


def test_render_json_is_valid_json():
    report = _make_report()
    j = render_json(report)
    parsed = json.loads(j)
    assert "timestamp" in parsed
    assert "session_results" in parsed
    assert "procedure_summaries" in parsed


def test_render_json_round_trip():
    report = _make_report(n_sessions=1)
    j = render_json(report)
    d = json.loads(j)
    assert d["session_results"][0]["subject"] == "sub-0001"
    assert d["procedure_summaries"][0]["procedure"] == "bids"


# ---------------------------------------------------------------------------
# save_report
# ---------------------------------------------------------------------------


def test_save_report_markdown(tmp_path):
    report = _make_report()
    path = save_report(report, tmp_path / "reports", fmt="markdown")
    assert path.exists()
    assert path.suffix == ".md"
    content = path.read_text()
    assert "Executive Summary" in content


def test_save_report_html(tmp_path):
    report = _make_report()
    path = save_report(report, tmp_path / "reports", fmt="html")
    assert path.exists()
    assert path.suffix == ".html"
    assert "<!DOCTYPE html>" in path.read_text()


def test_save_report_json(tmp_path):
    report = _make_report()
    path = save_report(report, tmp_path / "reports", fmt="json")
    assert path.exists()
    assert path.suffix == ".json"
    d = json.loads(path.read_text())
    assert "timestamp" in d


def test_save_report_creates_dir(tmp_path):
    report = _make_report()
    out_dir = tmp_path / "nested" / "audit_reports"
    path = save_report(report, out_dir)
    assert path.exists()


def test_save_report_timestamped_filename(tmp_path):
    report = _make_report()
    path = save_report(report, tmp_path)
    assert path.name.startswith("audit_")


# ---------------------------------------------------------------------------
# send_report_email
# ---------------------------------------------------------------------------


def test_send_report_email_calls_smtp(tmp_path):
    report = _make_report()
    with patch("smtplib.SMTP") as mock_smtp_cls:
        mock_smtp = MagicMock()
        mock_smtp_cls.return_value.__enter__ = MagicMock(return_value=mock_smtp)
        mock_smtp_cls.return_value.__exit__ = MagicMock(return_value=False)

        send_report_email(report, ["admin@example.com"])

        mock_smtp_cls.assert_called_once_with("localhost", 25)
        mock_smtp.sendmail.assert_called_once()
        args = mock_smtp.sendmail.call_args[0]
        assert args[0] == "snbb-scheduler@localhost"
        assert "admin@example.com" in args[1]


def test_send_report_email_custom_from(tmp_path):
    report = _make_report()
    with patch("smtplib.SMTP") as mock_smtp_cls:
        mock_smtp = MagicMock()
        mock_smtp_cls.return_value.__enter__ = MagicMock(return_value=mock_smtp)
        mock_smtp_cls.return_value.__exit__ = MagicMock(return_value=False)

        send_report_email(report, ["pi@example.com"], from_address="scheduler@hpc")

        args = mock_smtp.sendmail.call_args[0]
        assert args[0] == "scheduler@hpc"


def test_send_report_email_multiple_recipients(tmp_path):
    report = _make_report()
    with patch("smtplib.SMTP") as mock_smtp_cls:
        mock_smtp = MagicMock()
        mock_smtp_cls.return_value.__enter__ = MagicMock(return_value=mock_smtp)
        mock_smtp_cls.return_value.__exit__ = MagicMock(return_value=False)

        send_report_email(report, ["a@x.com", "b@x.com"])

        args = mock_smtp.sendmail.call_args[0]
        assert "a@x.com" in args[1]
        assert "b@x.com" in args[1]


# ---------------------------------------------------------------------------
# load_previous_report & compare_reports
# ---------------------------------------------------------------------------


def test_load_previous_report_no_dir(tmp_path):
    result = load_previous_report(tmp_path / "nonexistent")
    assert result is None


def test_load_previous_report_empty_dir(tmp_path):
    result = load_previous_report(tmp_path)
    assert result is None


def test_load_previous_report_returns_most_recent(tmp_path):
    report = _make_report(n_sessions=1)
    # Save two reports
    p1 = save_report(report, tmp_path, fmt="json")
    # Rename to ensure ordering
    import time
    time.sleep(0.01)
    p2 = save_report(report, tmp_path, fmt="json")

    loaded = load_previous_report(tmp_path)
    assert loaded is not None
    assert loaded.timestamp == report.timestamp


def test_load_previous_report_corrupt_file(tmp_path):
    (tmp_path / "audit_20240101_000000.json").write_text("not json")
    result = load_previous_report(tmp_path)
    assert result is None


def test_compare_reports_new_completions():
    prev = _make_report(n_sessions=1, proc_status="not_started")
    curr = _make_report(n_sessions=1, proc_status="complete")
    delta = compare_reports(curr, prev)
    assert len(delta["new_completions"]) == 1


def test_compare_reports_new_failures():
    prev = _make_report(n_sessions=1, proc_status="running")
    curr = _make_report(n_sessions=1, proc_status="failed")
    delta = compare_reports(curr, prev)
    assert len(delta["new_failures"]) == 1


def test_compare_reports_health_trend_positive():
    prev = _make_report(n_sessions=2, proc_status="not_started")
    curr = _make_report(n_sessions=2, proc_status="complete")
    delta = compare_reports(curr, prev)
    assert delta["health_trend"] > 0


def test_compare_reports_health_trend_negative():
    prev = _make_report(n_sessions=2, proc_status="complete")
    curr = _make_report(n_sessions=2, proc_status="failed")
    delta = compare_reports(curr, prev)
    assert delta["health_trend"] <= 0


def test_compare_reports_sessions_added():
    prev = _make_report(n_sessions=1, proc_status="complete")
    curr = _make_report(n_sessions=3, proc_status="complete")
    delta = compare_reports(curr, prev)
    assert delta["sessions_added"] == 2


def test_compare_reports_sessions_removed():
    prev = _make_report(n_sessions=3, proc_status="complete")
    curr = _make_report(n_sessions=1, proc_status="complete")
    delta = compare_reports(curr, prev)
    assert delta["sessions_removed"] == 2


def test_compare_reports_no_change():
    report = _make_report(n_sessions=2, proc_status="complete")
    delta = compare_reports(report, report)
    assert delta["new_completions"] == []
    assert delta["new_failures"] == []
    assert delta["health_trend"] == pytest.approx(0.0)

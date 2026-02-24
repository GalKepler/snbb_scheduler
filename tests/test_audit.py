"""Tests for audit.py — AuditLogger and get_logger()."""
import json
from pathlib import Path

import pytest

from snbb_scheduler.audit import AuditLogger, get_logger
from snbb_scheduler.config import SchedulerConfig


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def log_file(tmp_path):
    return tmp_path / "audit.jsonl"


@pytest.fixture
def audit(log_file):
    return AuditLogger(log_file)


# ---------------------------------------------------------------------------
# AuditLogger — file creation
# ---------------------------------------------------------------------------


def test_log_creates_file(audit, log_file):
    audit.log("submitted", subject="sub-0001", session="ses-01", procedure="bids", job_id="1")
    assert log_file.exists()


def test_log_creates_parent_dirs(tmp_path):
    deep = tmp_path / "a" / "b" / "c" / "audit.jsonl"
    al = AuditLogger(deep)
    al.log("submitted", subject="sub-0001", session="ses-01", procedure="bids", job_id="1")
    assert deep.exists()


# ---------------------------------------------------------------------------
# AuditLogger — JSONL structure
# ---------------------------------------------------------------------------


def test_log_writes_valid_json(audit, log_file):
    audit.log("submitted", subject="sub-0001", session="ses-01", procedure="bids", job_id="42")
    lines = log_file.read_text().splitlines()
    assert len(lines) == 1
    entry = json.loads(lines[0])
    assert isinstance(entry, dict)


def test_log_entry_has_required_fields(audit, log_file):
    audit.log("submitted", subject="sub-0001", session="ses-01", procedure="bids", job_id="42")
    entry = json.loads(log_file.read_text())
    for field in ("ts", "event", "subject", "session", "procedure", "job_id"):
        assert field in entry, f"Missing field: {field}"


def test_log_entry_event_field(audit, log_file):
    audit.log("error", subject="sub-0001", session="ses-01", procedure="bids", detail="boom")
    entry = json.loads(log_file.read_text())
    assert entry["event"] == "error"


def test_log_entry_subject_and_procedure(audit, log_file):
    audit.log("submitted", subject="sub-0042", session="ses-02", procedure="qsiprep", job_id="7")
    entry = json.loads(log_file.read_text())
    assert entry["subject"] == "sub-0042"
    assert entry["procedure"] == "qsiprep"
    assert entry["session"] == "ses-02"


def test_log_entry_job_id(audit, log_file):
    audit.log("submitted", subject="sub-0001", session="ses-01", procedure="bids", job_id="99")
    entry = json.loads(log_file.read_text())
    assert entry["job_id"] == "99"


def test_log_entry_job_id_none_for_dry_run(audit, log_file):
    audit.log("dry_run", subject="sub-0001", session="ses-01", procedure="bids")
    entry = json.loads(log_file.read_text())
    assert entry["job_id"] is None


def test_log_entry_status_change_fields(audit, log_file):
    audit.log(
        "status_change",
        subject="sub-0001",
        session="ses-01",
        procedure="bids",
        job_id="5",
        old_status="pending",
        new_status="complete",
    )
    entry = json.loads(log_file.read_text())
    assert entry["old_status"] == "pending"
    assert entry["new_status"] == "complete"


def test_log_entry_extra_kwargs(audit, log_file):
    audit.log("submitted", subject="sub-0001", session="ses-01", procedure="bids",
               job_id="1", custom_key="custom_value")
    entry = json.loads(log_file.read_text())
    assert entry["custom_key"] == "custom_value"


def test_log_appends_multiple_entries(audit, log_file):
    audit.log("submitted", subject="sub-0001", session="ses-01", procedure="bids", job_id="1")
    audit.log("submitted", subject="sub-0002", session="ses-01", procedure="bids", job_id="2")
    lines = log_file.read_text().splitlines()
    assert len(lines) == 2
    entries = [json.loads(l) for l in lines]
    assert entries[0]["subject"] == "sub-0001"
    assert entries[1]["subject"] == "sub-0002"


def test_log_entries_are_one_line_each(audit, log_file):
    """Each entry must be a complete JSON object on a single line (JSONL format)."""
    audit.log("submitted", subject="sub-0001", session="ses-01", procedure="bids", job_id="1")
    raw = log_file.read_text()
    assert raw.count("\n") == 1  # exactly one newline per entry


def test_log_timestamp_is_iso_format(audit, log_file):
    audit.log("submitted", subject="sub-0001", session="ses-01", procedure="bids", job_id="1")
    entry = json.loads(log_file.read_text())
    from datetime import datetime
    # Should parse without raising
    datetime.fromisoformat(entry["ts"])


# ---------------------------------------------------------------------------
# get_logger
# ---------------------------------------------------------------------------


def test_get_logger_returns_audit_logger(tmp_path):
    cfg = SchedulerConfig(state_file=tmp_path / "state.parquet")
    al = get_logger(cfg)
    assert isinstance(al, AuditLogger)


def test_get_logger_uses_log_file_when_set(tmp_path):
    log_path = tmp_path / "custom_audit.jsonl"
    cfg = SchedulerConfig(
        state_file=tmp_path / "state.parquet",
        log_file=log_path,
    )
    al = get_logger(cfg)
    assert al.log_file == log_path


def test_get_logger_defaults_to_state_dir(tmp_path):
    cfg = SchedulerConfig(state_file=tmp_path / "state.parquet")
    al = get_logger(cfg)
    assert al.log_file == tmp_path / "scheduler_audit.jsonl"


def test_get_logger_default_path_is_jsonl(tmp_path):
    cfg = SchedulerConfig(state_file=tmp_path / "sub" / "state.parquet")
    al = get_logger(cfg)
    assert al.log_file.suffix == ".jsonl"


# ---------------------------------------------------------------------------
# AuditLogger integration with submit_task (error event)
# ---------------------------------------------------------------------------


def test_submit_task_logs_error_event_to_audit(tmp_path):
    """An sbatch CalledProcessError triggers an 'error' audit entry."""
    import subprocess
    from unittest.mock import patch
    import pandas as pd
    from snbb_scheduler.submit import submit_task

    log_path = tmp_path / "audit.jsonl"
    al = AuditLogger(log_path)
    cfg = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
    )
    row = pd.Series({"subject": "sub-0001", "session": "ses-01", "procedure": "bids"})

    with patch("subprocess.run", side_effect=subprocess.CalledProcessError(1, "sbatch")):
        with pytest.raises(subprocess.CalledProcessError):
            submit_task(row, cfg, audit=al)

    entries = [json.loads(l) for l in log_path.read_text().splitlines()]
    assert any(e["event"] == "error" for e in entries)


def test_submit_task_logs_submitted_event_to_audit(tmp_path):
    """A successful sbatch call triggers a 'submitted' audit entry."""
    from unittest.mock import MagicMock, patch
    import pandas as pd
    from snbb_scheduler.submit import submit_task

    log_path = tmp_path / "audit.jsonl"
    al = AuditLogger(log_path)
    cfg = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
    )
    row = pd.Series({"subject": "sub-0001", "session": "ses-01", "procedure": "bids"})
    mock_result = MagicMock()
    mock_result.stdout = "Submitted batch job 123\n"

    with patch("subprocess.run", return_value=mock_result):
        submit_task(row, cfg, audit=al)

    entries = [json.loads(l) for l in log_path.read_text().splitlines()]
    assert any(e["event"] == "submitted" and e["job_id"] == "123" for e in entries)


def test_submit_task_logs_dry_run_event_to_audit(tmp_path):
    """A dry-run submit triggers a 'dry_run' audit entry."""
    import pandas as pd
    from snbb_scheduler.submit import submit_task

    log_path = tmp_path / "audit.jsonl"
    al = AuditLogger(log_path)
    cfg = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
    )
    row = pd.Series({"subject": "sub-0001", "session": "ses-01", "procedure": "bids"})
    submit_task(row, cfg, dry_run=True, audit=al)

    entries = [json.loads(l) for l in log_path.read_text().splitlines()]
    assert any(e["event"] == "dry_run" for e in entries)

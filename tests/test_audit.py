"""Tests for audit.py."""
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from snbb_scheduler.audit import AuditLogger, get_logger
from snbb_scheduler.config import SchedulerConfig


@pytest.fixture
def log_file(tmp_path):
    return tmp_path / "audit.jsonl"


@pytest.fixture
def audit(log_file):
    return AuditLogger(log_file)


# ---------------------------------------------------------------------------
# Basic write / parse
# ---------------------------------------------------------------------------

def test_log_writes_valid_jsonl(audit, log_file):
    audit.log("submitted", subject="sub-0001", session="ses-01", procedure="bids")
    lines = log_file.read_text().splitlines()
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["event"] == "submitted"


def test_log_includes_required_fields(audit, log_file):
    audit.log("submitted", subject="sub-0001", session="ses-01", procedure="bids")
    record = json.loads(log_file.read_text())
    assert "timestamp" in record
    assert record["subject"] == "sub-0001"
    assert record["session"] == "ses-01"
    assert record["procedure"] == "bids"


def test_log_optional_job_id(audit, log_file):
    audit.log("submitted", subject="sub-0001", session="ses-01", procedure="bids", job_id="42")
    record = json.loads(log_file.read_text())
    assert record["job_id"] == "42"


def test_log_optional_fields_omitted_when_not_provided(audit, log_file):
    audit.log("submitted", subject="sub-0001", session="ses-01", procedure="bids")
    record = json.loads(log_file.read_text())
    assert "job_id" not in record
    assert "old_status" not in record
    assert "new_status" not in record
    assert "detail" not in record


def test_log_old_and_new_status(audit, log_file):
    audit.log(
        "status_change",
        subject="sub-0001", session="ses-01", procedure="bids",
        job_id="7", old_status="pending", new_status="complete",
    )
    record = json.loads(log_file.read_text())
    assert record["old_status"] == "pending"
    assert record["new_status"] == "complete"


def test_log_detail_field(audit, log_file):
    audit.log("dry_run", subject="s", session="s", procedure="bids", detail="sbatch bids")
    record = json.loads(log_file.read_text())
    assert record["detail"] == "sbatch bids"


def test_log_appends_multiple_lines(audit, log_file):
    audit.log("submitted", subject="sub-0001", session="ses-01", procedure="bids")
    audit.log("submitted", subject="sub-0002", session="ses-01", procedure="bids")
    lines = log_file.read_text().splitlines()
    assert len(lines) == 2
    for line in lines:
        json.loads(line)  # must be valid JSON


def test_log_creates_parent_directory(tmp_path):
    log_file = tmp_path / "subdir" / "audit.jsonl"
    a = AuditLogger(log_file)
    a.log("submitted")
    assert log_file.exists()


def test_log_extra_kwargs_in_record(audit, log_file):
    audit.log("submitted", subject="sub-0001", session="ses-01", procedure="bids", extra_key="x")
    record = json.loads(log_file.read_text())
    assert record["extra_key"] == "x"


# ---------------------------------------------------------------------------
# get_logger
# ---------------------------------------------------------------------------

def test_get_logger_uses_log_file_when_set(tmp_path):
    log_path = tmp_path / "custom_audit.jsonl"
    cfg = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
        log_file=log_path,
    )
    a = get_logger(cfg)
    a.log("submitted")
    assert log_path.exists()


def test_get_logger_defaults_to_state_file_parent(tmp_path):
    cfg = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
        log_file=None,
    )
    a = get_logger(cfg)
    a.log("submitted")
    assert (tmp_path / "scheduler_audit.jsonl").exists()

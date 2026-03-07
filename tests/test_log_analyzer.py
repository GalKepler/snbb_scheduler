"""Tests for log_analyzer.py — Slurm log parsing."""
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from snbb_scheduler.config import SchedulerConfig
from snbb_scheduler.log_analyzer import (
    DEFAULT_LOG_PATTERNS,
    LogFinding,
    LogPattern,
    analyze_log_file,
    analyze_task_logs,
    find_logs_for_task,
)


# ---------------------------------------------------------------------------
# analyze_log_file
# ---------------------------------------------------------------------------


def test_analyze_log_file_no_match(tmp_path):
    log = tmp_path / "job_123.out"
    log.write_text("Everything is fine\nJob completed successfully\n")
    findings = analyze_log_file(log)
    assert findings == []


def test_analyze_log_file_oom(tmp_path):
    log = tmp_path / "job.out"
    log.write_text("Starting job\nKilled process 1234 (out of memory)\nDone\n")
    findings = analyze_log_file(log)
    names = [f.pattern_name for f in findings]
    assert "oom" in names


def test_analyze_log_file_timeout(tmp_path):
    log = tmp_path / "job.err"
    log.write_text("slurmstepd: error: *** JOB 99 ON node1 CANCELLED AT 2024-01-01 DUE TO TIME LIMIT ***\n")
    findings = analyze_log_file(log)
    assert any(f.pattern_name == "timeout" for f in findings)


def test_analyze_log_file_segfault(tmp_path):
    log = tmp_path / "job.out"
    log.write_text("Segmentation fault (core dumped)\n")
    findings = analyze_log_file(log)
    assert any(f.pattern_name == "segfault" for f in findings)


def test_analyze_log_file_python_traceback(tmp_path):
    log = tmp_path / "job.out"
    log.write_text("Traceback (most recent call last):\n  File \"foo.py\", line 1\nValueError: oops\n")
    findings = analyze_log_file(log)
    assert any(f.pattern_name == "python_traceback" for f in findings)


def test_analyze_log_file_missing_file(tmp_path):
    log = tmp_path / "job.out"
    log.write_text("FileNotFoundError: /data/missing.nii\n")
    findings = analyze_log_file(log)
    assert any(f.pattern_name == "missing_file" for f in findings)


def test_analyze_log_file_freesurfer_error(tmp_path):
    log = tmp_path / "job.err"
    log.write_text("ERROR: recon-all exited with errors\n")
    findings = analyze_log_file(log)
    assert any(f.pattern_name == "freesurfer_error" for f in findings)


def test_analyze_log_file_missing_file_returns_empty(tmp_path):
    """Non-existent log file returns empty list (no crash)."""
    findings = analyze_log_file(tmp_path / "ghost.out")
    assert findings == []


def test_analyze_log_file_finding_attributes(tmp_path):
    log = tmp_path / "job.out"
    log.write_text("line1\nout of memory error\nline3\n")
    findings = analyze_log_file(log)
    assert len(findings) >= 1
    f = findings[0]
    assert f.pattern_name == "oom"
    assert f.severity == "error"
    assert f.line_number == 2
    assert "out of memory" in f.line_text.lower()
    assert str(log) == f.log_file


def test_analyze_log_file_custom_patterns(tmp_path):
    log = tmp_path / "job.out"
    log.write_text("CUSTOM_ERROR: something went wrong\n")
    custom = [LogPattern(name="custom", regex=r"CUSTOM_ERROR", severity="error", description="Custom")]
    findings = analyze_log_file(log, patterns=custom)
    assert len(findings) == 1
    assert findings[0].pattern_name == "custom"


def test_analyze_log_file_warning_severity(tmp_path):
    log = tmp_path / "job.out"
    log.write_text("qsiprep WARNING: some warning\n")
    findings = analyze_log_file(log)
    warnings = [f for f in findings if f.severity == "warning"]
    assert len(warnings) >= 1


def test_default_log_patterns_have_required_fields():
    for p in DEFAULT_LOG_PATTERNS:
        assert p.name
        assert p.regex
        assert p.severity in ("error", "warning")
        assert p.description


# ---------------------------------------------------------------------------
# find_logs_for_task
# ---------------------------------------------------------------------------


def _make_config(tmp_path, slurm_log_dir=None):
    return SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
        slurm_log_dir=slurm_log_dir,
    )


def test_find_logs_no_slurm_log_dir(tmp_path):
    config = _make_config(tmp_path)
    state = pd.DataFrame()
    found = find_logs_for_task("sub-0001", "ses-01", "bids", config, state)
    assert found == []


def test_find_logs_nonexistent_proc_dir(tmp_path):
    config = _make_config(tmp_path, slurm_log_dir=tmp_path / "logs")
    state = pd.DataFrame()
    found = find_logs_for_task("sub-0001", "ses-01", "bids", config, state)
    assert found == []


def test_find_logs_by_job_id(tmp_path):
    log_dir = tmp_path / "logs" / "bids"
    log_dir.mkdir(parents=True)
    out_file = log_dir / "bids_sub-0001_ses-01_999.out"
    out_file.touch()
    config = _make_config(tmp_path, slurm_log_dir=tmp_path / "logs")

    state = pd.DataFrame([{
        "subject": "sub-0001",
        "session": "ses-01",
        "procedure": "bids",
        "status": "complete",
        "submitted_at": None,
        "job_id": "999",
    }])
    found = find_logs_for_task("sub-0001", "ses-01", "bids", config, state)
    assert out_file in found


def test_find_logs_fallback_glob(tmp_path):
    log_dir = tmp_path / "logs" / "bids"
    log_dir.mkdir(parents=True)
    out_file = log_dir / "bids_sub-0001_ses-01_UNKNOWN.out"
    out_file.touch()
    config = _make_config(tmp_path, slurm_log_dir=tmp_path / "logs")
    state = pd.DataFrame()
    found = find_logs_for_task("sub-0001", "ses-01", "bids", config, state)
    assert out_file in found


# ---------------------------------------------------------------------------
# analyze_task_logs
# ---------------------------------------------------------------------------


def test_analyze_task_logs_no_config_log_dir(tmp_path):
    config = _make_config(tmp_path)
    state = pd.DataFrame()
    findings = analyze_task_logs("sub-0001", "ses-01", "bids", config, state)
    assert findings == []


def test_analyze_task_logs_with_errors(tmp_path):
    log_dir = tmp_path / "logs" / "bids"
    log_dir.mkdir(parents=True)
    log_file = log_dir / "bids_sub-0001_ses-01_42.out"
    log_file.write_text("Segmentation fault (core dumped)\n")
    config = _make_config(tmp_path, slurm_log_dir=tmp_path / "logs")

    state = pd.DataFrame([{
        "subject": "sub-0001",
        "session": "ses-01",
        "procedure": "bids",
        "status": "failed",
        "submitted_at": None,
        "job_id": "42",
    }])
    findings = analyze_task_logs("sub-0001", "ses-01", "bids", config, state)
    assert any(f.pattern_name == "segfault" for f in findings)

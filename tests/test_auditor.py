"""Tests for auditor.py — core audit engine."""
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from snbb_scheduler.auditor import (
    AuditReport,
    DicomAuditResult,
    ProcedureAuditResult,
    ProcedureSummary,
    SessionAuditResult,
    audit_dicom,
    audit_procedure,
    audit_session,
    run_full_audit,
)
from snbb_scheduler.config import AuditConfig, SchedulerConfig
from snbb_scheduler.manifest import save_state


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def cfg(tmp_path):
    return SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
        audit=AuditConfig(dicom_min_files=2),
    )


@pytest.fixture
def sessions_df(cfg):
    """DataFrame with two sessions."""
    (cfg.dicom_root / "sub-0001" / "ses-01").mkdir(parents=True)
    (cfg.dicom_root / "sub-0002" / "ses-01").mkdir(parents=True)

    return pd.DataFrame([
        {"subject": "sub-0001", "session": "ses-01", "dicom_path": str(cfg.dicom_root / "sub-0001" / "ses-01")},
        {"subject": "sub-0002", "session": "ses-01", "dicom_path": str(cfg.dicom_root / "sub-0002" / "ses-01")},
    ])


@pytest.fixture
def empty_state():
    return pd.DataFrame(columns=["subject", "session", "procedure", "status", "submitted_at", "job_id"])


# ---------------------------------------------------------------------------
# audit_dicom
# ---------------------------------------------------------------------------


def test_audit_dicom_missing_dir(cfg, sessions_df, empty_state):
    # No DICOM directory exists for sub-0003
    result = audit_dicom("sub-0003", "ses-01", sessions_df, cfg)
    assert not result.exists
    assert result.file_count == 0
    assert result.is_suspicious


def test_audit_dicom_empty_dir(cfg, sessions_df, empty_state):
    # Directory exists but has no files (below threshold)
    result = audit_dicom("sub-0001", "ses-01", sessions_df, cfg)
    assert result.exists
    assert result.file_count == 0
    assert result.is_suspicious


def test_audit_dicom_enough_files(cfg, sessions_df, empty_state):
    dicom_dir = cfg.dicom_root / "sub-0001" / "ses-01"
    for i in range(5):
        (dicom_dir / f"file{i}.dcm").touch()

    result = audit_dicom("sub-0001", "ses-01", sessions_df, cfg)
    assert result.exists
    assert result.file_count == 5
    assert not result.is_suspicious  # threshold is 2


def test_audit_dicom_has_subdirs(cfg, sessions_df):
    dicom_dir = cfg.dicom_root / "sub-0001" / "ses-01"
    subdir = dicom_dir / "series001"
    subdir.mkdir()
    (subdir / "img.dcm").touch()
    (subdir / "img2.dcm").touch()
    (subdir / "img3.dcm").touch()

    result = audit_dicom("sub-0001", "ses-01", sessions_df, cfg)
    assert result.has_expected_structure
    assert result.file_count == 3


def test_audit_dicom_flat_dir_no_subdirs(cfg, sessions_df):
    dicom_dir = cfg.dicom_root / "sub-0002" / "ses-01"
    for i in range(3):
        (dicom_dir / f"file{i}.dcm").touch()

    result = audit_dicom("sub-0002", "ses-01", sessions_df, cfg)
    assert not result.has_expected_structure


def test_audit_dicom_path_from_sessions_df(cfg, tmp_path):
    # When dicom_path in sessions_df differs from dicom_root
    custom_path = tmp_path / "custom_dicom"
    custom_path.mkdir()
    for i in range(3):
        (custom_path / f"f{i}.dcm").touch()

    df = pd.DataFrame([{
        "subject": "sub-0001",
        "session": "ses-01",
        "dicom_path": str(custom_path),
    }])
    result = audit_dicom("sub-0001", "ses-01", df, cfg)
    assert result.exists
    assert result.file_count == 3


# ---------------------------------------------------------------------------
# audit_session
# ---------------------------------------------------------------------------


def test_audit_session_returns_session_result(cfg, sessions_df, empty_state):
    result = audit_session("sub-0001", "ses-01", cfg, sessions_df, empty_state)
    assert isinstance(result, SessionAuditResult)
    assert result.subject == "sub-0001"
    assert result.session == "ses-01"
    assert isinstance(result.dicom, DicomAuditResult)
    assert isinstance(result.procedures, dict)


def test_audit_session_has_all_procedures(cfg, sessions_df, empty_state):
    result = audit_session("sub-0001", "ses-01", cfg, sessions_df, empty_state)
    proc_names = {p.name for p in cfg.procedures}
    assert set(result.procedures.keys()) == proc_names


def test_audit_session_health_score_zero_no_output(cfg, sessions_df, empty_state):
    """With no outputs on disk, health score should be 0."""
    result = audit_session("sub-0001", "ses-01", cfg, sessions_df, empty_state)
    assert result.health_score == 0.0


def test_audit_session_health_score_with_complete_procedure(cfg, sessions_df, empty_state, tmp_path):
    """Health score improves when at least one procedure is complete."""
    # Create a complete BIDS session
    bids_dir = cfg.bids_root / "sub-0001" / "ses-01"
    for subdir, name in [
        ("anat", "sub_T1w.nii.gz"),
        ("dwi", "sub_dir-AP_dwi.nii.gz"),
        ("dwi", "sub_dir-AP_dwi.bvec"),
        ("dwi", "sub_dir-AP_dwi.bval"),
        ("dwi", "sub_dir-PA_dwi.nii.gz"),
        ("fmap", "sub_acq-func_dir-AP_epi.nii.gz"),
        ("fmap", "sub_acq-func_dir-PA_epi.nii.gz"),
        ("func", "sub_task-rest_bold.nii.gz"),
    ]:
        d = bids_dir / subdir
        d.mkdir(parents=True, exist_ok=True)
        (d / name).touch()

    result = audit_session("sub-0001", "ses-01", cfg, sessions_df, empty_state)
    assert result.health_score > 0.0


def test_audit_session_procedure_result_attributes(cfg, sessions_df, empty_state):
    result = audit_session("sub-0001", "ses-01", cfg, sessions_df, empty_state)
    pr = result.procedures["bids"]
    assert isinstance(pr, ProcedureAuditResult)
    assert pr.procedure == "bids"
    assert pr.subject == "sub-0001"
    assert pr.session == "ses-01"
    assert 0.0 <= pr.completeness_ratio <= 1.0


# ---------------------------------------------------------------------------
# Stale job detection
# ---------------------------------------------------------------------------


def test_stale_job_detected(cfg, sessions_df):
    now = datetime.now(timezone.utc)
    old_time = now - timedelta(hours=200)  # beyond default 168h threshold

    state = pd.DataFrame([{
        "subject": "sub-0001",
        "session": "ses-01",
        "procedure": "bids",
        "status": "running",
        "submitted_at": pd.Timestamp(old_time),
        "job_id": "777",
    }])

    result = audit_session("sub-0001", "ses-01", cfg, sessions_df, state)
    bids_result = result.procedures["bids"]
    assert bids_result.is_stale
    assert bids_result.job_age_hours is not None
    assert bids_result.job_age_hours > 168


def test_non_stale_job(cfg, sessions_df):
    now = datetime.now(timezone.utc)
    recent = now - timedelta(hours=10)

    state = pd.DataFrame([{
        "subject": "sub-0001",
        "session": "ses-01",
        "procedure": "bids",
        "status": "running",
        "submitted_at": pd.Timestamp(recent),
        "job_id": "888",
    }])

    result = audit_session("sub-0001", "ses-01", cfg, sessions_df, state)
    assert not result.procedures["bids"].is_stale


def test_completed_job_not_stale(cfg, sessions_df):
    """Completed jobs should not be flagged as stale even if old."""
    old_time = datetime.now(timezone.utc) - timedelta(hours=500)

    state = pd.DataFrame([{
        "subject": "sub-0001",
        "session": "ses-01",
        "procedure": "bids",
        "status": "complete",
        "submitted_at": pd.Timestamp(old_time),
        "job_id": "100",
    }])

    result = audit_session("sub-0001", "ses-01", cfg, sessions_df, state)
    assert not result.procedures["bids"].is_stale


# ---------------------------------------------------------------------------
# audit_procedure
# ---------------------------------------------------------------------------


def test_audit_procedure_returns_summary(cfg, sessions_df, empty_state):
    summary = audit_procedure("bids", cfg, sessions_df, empty_state)
    assert isinstance(summary, ProcedureSummary)
    assert summary.procedure == "bids"
    assert summary.total_sessions >= 0


def test_audit_procedure_counts_no_completions(cfg, sessions_df, empty_state):
    summary = audit_procedure("bids", cfg, sessions_df, empty_state)
    assert summary.complete == 0
    # Sessions with no output are counted as incomplete (markers exist but none found)
    assert summary.incomplete + summary.not_started == summary.total_sessions


def test_audit_procedure_counts_complete(cfg, sessions_df, empty_state):
    # Create BIDS output for sub-0001
    bids_dir = cfg.bids_root / "sub-0001" / "ses-01"
    for subdir, name in [
        ("anat", "sub_T1w.nii.gz"),
        ("dwi", "sub_dir-AP_dwi.nii.gz"),
        ("dwi", "sub_dir-AP_dwi.bvec"),
        ("dwi", "sub_dir-AP_dwi.bval"),
        ("dwi", "sub_dir-PA_dwi.nii.gz"),
        ("fmap", "sub_acq-func_dir-AP_epi.nii.gz"),
        ("fmap", "sub_acq-func_dir-PA_epi.nii.gz"),
        ("func", "sub_task-rest_bold.nii.gz"),
    ]:
        d = bids_dir / subdir
        d.mkdir(parents=True, exist_ok=True)
        (d / name).touch()

    summary = audit_procedure("bids", cfg, sessions_df, empty_state)
    assert summary.complete >= 1
    assert summary.total_sessions == 2


def test_audit_procedure_stale_count(cfg, sessions_df):
    old_time = datetime.now(timezone.utc) - timedelta(hours=200)
    state = pd.DataFrame([{
        "subject": "sub-0001",
        "session": "ses-01",
        "procedure": "bids",
        "status": "running",
        "submitted_at": pd.Timestamp(old_time),
        "job_id": "55",
    }])
    summary = audit_procedure("bids", cfg, sessions_df, state)
    assert summary.stale >= 1


def test_audit_procedure_unknown_name_raises(cfg, sessions_df, empty_state):
    with pytest.raises(KeyError):
        audit_procedure("nonexistent", cfg, sessions_df, empty_state)


# ---------------------------------------------------------------------------
# run_full_audit
# ---------------------------------------------------------------------------


def test_run_full_audit_empty_dicom(cfg):
    report = run_full_audit(cfg)
    assert isinstance(report, AuditReport)
    assert report.timestamp
    assert isinstance(report.session_results, list)
    assert isinstance(report.procedure_summaries, list)


def test_run_full_audit_with_sessions(cfg, tmp_path):
    (cfg.dicom_root / "sub-0001" / "ses-01").mkdir(parents=True)
    (cfg.dicom_root / "sub-0001" / "ses-02").mkdir(parents=True)

    report = run_full_audit(cfg)
    assert len(report.session_results) == 2
    subjects = {s.subject for s in report.session_results}
    assert "sub-0001" in subjects


def test_run_full_audit_procedure_summaries(cfg, tmp_path):
    (cfg.dicom_root / "sub-0001" / "ses-01").mkdir(parents=True)

    report = run_full_audit(cfg)
    proc_names = {ps.procedure for ps in report.procedure_summaries}
    expected = {p.name for p in cfg.procedures}
    assert proc_names == expected


def test_run_full_audit_config_summary(cfg, tmp_path):
    report = run_full_audit(cfg)
    assert "dicom_root" in report.config_summary
    assert "procedures" in report.config_summary

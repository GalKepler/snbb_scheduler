"""End-to-end integration tests.

These tests exercise the full pipeline — discover → manifest → filter →
submit → save state — against a real fake filesystem, with only
subprocess.run mocked to avoid needing a Slurm cluster.
"""
from unittest.mock import patch

import pandas as pd
import pytest

from snbb_scheduler.config import SchedulerConfig
from snbb_scheduler.manifest import (
    build_manifest,
    filter_in_flight,
    load_state,
    save_state,
)
from snbb_scheduler.sessions import discover_sessions
from snbb_scheduler.submit import submit_manifest


# ---------------------------------------------------------------------------
# Shared setup helpers
# ---------------------------------------------------------------------------

def make_config(tmp_path) -> SchedulerConfig:
    return SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
    )


def add_dicom(tmp_path, subject, session):
    d = tmp_path / "dicom" / subject / session
    d.mkdir(parents=True, exist_ok=True)
    (d / "file.dcm").touch()


def add_bids(tmp_path, subject, session):
    """Create all 8 required BIDS modality files for the session."""
    bids_dir = tmp_path / "bids" / subject / session
    files = {
        "anat": [f"{subject}_{session}_T1w.nii.gz"],
        "dwi": [
            f"{subject}_{session}_dir-AP_dwi.nii.gz",
            f"{subject}_{session}_dir-AP_dwi.bvec",
            f"{subject}_{session}_dir-AP_dwi.bval",
        ],
        "fmap": [
            f"{subject}_{session}_acq-dwi_dir-AP_epi.nii.gz",
            f"{subject}_{session}_acq-func_dir-AP_epi.nii.gz",
            f"{subject}_{session}_acq-func_dir-PA_epi.nii.gz",
        ],
        "func": [f"{subject}_{session}_task-rest_bold.nii.gz"],
    }
    for subdir, names in files.items():
        d = bids_dir / subdir
        d.mkdir(parents=True, exist_ok=True)
        for name in names:
            (d / name).touch()


def add_qsiprep(tmp_path, subject, session):
    out = tmp_path / "derivatives" / "qsiprep" / subject / session
    out.mkdir(parents=True, exist_ok=True)
    (out / "dwi.nii.gz").touch()


def add_freesurfer(tmp_path, subject):
    """Create recon-all.done with CMDARGS matching T1w files in BIDS."""
    scripts = tmp_path / "derivatives" / "freesurfer" / subject / "scripts"
    scripts.mkdir(parents=True, exist_ok=True)
    subject_bids = tmp_path / "bids" / subject
    t1w_count = len(list(subject_bids.glob("ses-*/anat/*_T1w.nii.gz")))
    i_flags = " ".join(f"-i /fake/T1w_{k}.nii.gz" for k in range(t1w_count))
    (scripts / "recon-all.done").write_text(
        f"#CMDARGS -subject {subject} -all {i_flags}\n"
    )


def mock_sbatch(job_id="1"):
    m = __import__("unittest.mock", fromlist=["MagicMock"]).MagicMock()
    m.stdout = f"Submitted batch job {job_id}\n"
    return m


# ---------------------------------------------------------------------------
# Full pipeline: fresh run
# ---------------------------------------------------------------------------

def test_full_run_only_bids_submitted_initially(tmp_path):
    cfg = make_config(tmp_path)
    add_dicom(tmp_path, "sub-0001", "ses-01")
    add_dicom(tmp_path, "sub-0002", "ses-01")

    sessions = discover_sessions(cfg)
    manifest = build_manifest(sessions, cfg)
    state = load_state(cfg)
    manifest = filter_in_flight(manifest, state)

    with patch("subprocess.run", return_value=mock_sbatch()) as mock_run:
        new_state = submit_manifest(manifest, cfg)

    assert mock_run.call_count == 2  # one bids job per subject
    assert set(new_state["procedure"]) == {"bids"}
    assert (new_state["status"] == "pending").all()


def test_state_saved_and_reloaded(tmp_path):
    cfg = make_config(tmp_path)
    add_dicom(tmp_path, "sub-0001", "ses-01")

    sessions = discover_sessions(cfg)
    manifest = build_manifest(sessions, cfg)

    with patch("subprocess.run", return_value=mock_sbatch("42")):
        new_state = submit_manifest(manifest, cfg)
    save_state(new_state, cfg)

    loaded = load_state(cfg)
    assert len(loaded) == 1
    assert loaded.iloc[0]["job_id"] == "42"
    assert loaded.iloc[0]["procedure"] == "bids"


# ---------------------------------------------------------------------------
# In-flight deduplication
# ---------------------------------------------------------------------------

def test_in_flight_prevents_duplicate_submission(tmp_path):
    cfg = make_config(tmp_path)
    add_dicom(tmp_path, "sub-0001", "ses-01")

    # Simulate a bids job already pending
    existing = pd.DataFrame([{
        "subject": "sub-0001", "session": "ses-01", "procedure": "bids",
        "status": "pending", "submitted_at": pd.Timestamp("2024-01-01"), "job_id": "99",
    }])
    save_state(existing, cfg)

    sessions = discover_sessions(cfg)
    manifest = build_manifest(sessions, cfg)
    state = load_state(cfg)
    manifest = filter_in_flight(manifest, state)

    with patch("subprocess.run") as mock_run:
        submit_manifest(manifest, cfg)
    mock_run.assert_not_called()


def test_failed_job_is_resubmitted(tmp_path):
    cfg = make_config(tmp_path)
    add_dicom(tmp_path, "sub-0001", "ses-01")

    # A previous bids job failed
    existing = pd.DataFrame([{
        "subject": "sub-0001", "session": "ses-01", "procedure": "bids",
        "status": "failed", "submitted_at": pd.Timestamp("2024-01-01"), "job_id": "99",
    }])
    save_state(existing, cfg)

    sessions = discover_sessions(cfg)
    manifest = build_manifest(sessions, cfg)
    state = load_state(cfg)
    manifest = filter_in_flight(manifest, state)

    with patch("subprocess.run", return_value=mock_sbatch("100")) as mock_run:
        submit_manifest(manifest, cfg)
    mock_run.assert_called_once()


# ---------------------------------------------------------------------------
# Multi-step pipeline progression
# ---------------------------------------------------------------------------

def test_pipeline_advances_after_bids_complete(tmp_path):
    cfg = make_config(tmp_path)
    add_dicom(tmp_path, "sub-0001", "ses-01")
    add_bids(tmp_path, "sub-0001", "ses-01")  # BIDS already done

    sessions = discover_sessions(cfg)
    manifest = build_manifest(sessions, cfg)

    procedures = set(manifest["procedure"])
    assert "bids" not in procedures
    assert "qsiprep" in procedures
    assert "freesurfer" in procedures


def test_nothing_submitted_when_all_complete(tmp_path):
    cfg = make_config(tmp_path)
    add_dicom(tmp_path, "sub-0001", "ses-01")
    add_bids(tmp_path, "sub-0001", "ses-01")
    add_qsiprep(tmp_path, "sub-0001", "ses-01")
    add_freesurfer(tmp_path, "sub-0001")

    sessions = discover_sessions(cfg)
    manifest = build_manifest(sessions, cfg)
    assert manifest.empty


def test_two_sessions_same_subject_share_freesurfer_path(tmp_path):
    """FreeSurfer is subject-scoped: completing it covers both sessions."""
    cfg = make_config(tmp_path)
    add_dicom(tmp_path, "sub-0001", "ses-01")
    add_dicom(tmp_path, "sub-0001", "ses-02")
    add_bids(tmp_path, "sub-0001", "ses-01")
    add_bids(tmp_path, "sub-0001", "ses-02")
    add_freesurfer(tmp_path, "sub-0001")

    sessions = discover_sessions(cfg)
    manifest = build_manifest(sessions, cfg)

    # freesurfer should not appear — already done for this subject
    assert "freesurfer" not in set(manifest["procedure"])
    # qsiprep should appear for both sessions
    assert len(manifest[manifest["procedure"] == "qsiprep"]) == 2


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------

def test_second_run_submits_nothing_when_all_in_flight(tmp_path):
    """Running the scheduler twice should not double-submit."""
    cfg = make_config(tmp_path)
    add_dicom(tmp_path, "sub-0001", "ses-01")
    add_dicom(tmp_path, "sub-0002", "ses-01")

    # First run
    sessions = discover_sessions(cfg)
    manifest = build_manifest(sessions, cfg)
    state = load_state(cfg)
    manifest = filter_in_flight(manifest, state)
    with patch("subprocess.run", return_value=mock_sbatch()):
        new_state = submit_manifest(manifest, cfg)
    save_state(new_state, cfg)

    # Second run — jobs are still pending
    sessions = discover_sessions(cfg)
    manifest = build_manifest(sessions, cfg)
    state = load_state(cfg)
    manifest = filter_in_flight(manifest, state)

    with patch("subprocess.run") as mock_run:
        submit_manifest(manifest, cfg)
    mock_run.assert_not_called()

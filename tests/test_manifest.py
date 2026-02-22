"""Tests for manifest.py."""
from pathlib import Path

import pandas as pd
import pytest

from snbb_scheduler.config import DEFAULT_PROCEDURES, Procedure, SchedulerConfig
from snbb_scheduler.manifest import (
    build_manifest,
    filter_in_flight,
    load_state,
    save_state,
)


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
    )


def make_sessions(cfg: SchedulerConfig, tmp_path: Path) -> pd.DataFrame:
    """Return a two-row sessions DataFrame from fake_data_dir layout."""
    from snbb_scheduler.sessions import discover_sessions

    (tmp_path / "dicom" / "sub-0001" / "ses-01").mkdir(parents=True, exist_ok=True)
    (tmp_path / "dicom" / "sub-0002" / "ses-01").mkdir(parents=True, exist_ok=True)
    return discover_sessions(cfg)


def mark_bids_complete(tmp_path: Path, subject: str, session: str) -> None:
    anat = tmp_path / "bids" / subject / session / "anat"
    anat.mkdir(parents=True, exist_ok=True)
    (anat / "T1w.nii.gz").touch()


def make_state_row(subject, session, procedure, status, job_id="12345") -> dict:
    return {
        "subject": subject,
        "session": session,
        "procedure": procedure,
        "status": status,
        "submitted_at": pd.Timestamp("2024-01-01"),
        "job_id": job_id,
    }


# ---------------------------------------------------------------------------
# build_manifest — basic behaviour
# ---------------------------------------------------------------------------

def test_build_manifest_returns_dataframe(cfg, tmp_path):
    sessions = make_sessions(cfg, tmp_path)
    manifest = build_manifest(sessions, cfg)
    assert isinstance(manifest, pd.DataFrame)


def test_build_manifest_columns(cfg, tmp_path):
    sessions = make_sessions(cfg, tmp_path)
    manifest = build_manifest(sessions, cfg)
    for col in ("subject", "session", "procedure", "dicom_path", "priority"):
        assert col in manifest.columns


def test_build_manifest_empty_sessions(cfg):
    from snbb_scheduler.sessions import discover_sessions
    sessions = discover_sessions(cfg)  # dicom_root doesn't exist → empty
    manifest = build_manifest(sessions, cfg)
    assert manifest.empty


def test_build_manifest_only_bids_without_dicom_output(cfg, tmp_path):
    """With only DICOM present, only bids should be in the manifest."""
    sessions = make_sessions(cfg, tmp_path)
    manifest = build_manifest(sessions, cfg)
    assert set(manifest["procedure"]) == {"bids"}


def test_build_manifest_both_subjects_need_bids(cfg, tmp_path):
    sessions = make_sessions(cfg, tmp_path)
    manifest = build_manifest(sessions, cfg)
    assert set(manifest["subject"]) == {"sub-0001", "sub-0002"}


def test_build_manifest_downstream_after_bids(cfg, tmp_path):
    """Once BIDS is done for sub-0001, qsiprep and freesurfer should appear."""
    sessions = make_sessions(cfg, tmp_path)
    mark_bids_complete(tmp_path, "sub-0001", "ses-01")
    sessions = make_sessions(cfg, tmp_path)  # re-discover with updated FS
    manifest = build_manifest(sessions, cfg)
    sub01 = manifest[manifest["subject"] == "sub-0001"]["procedure"].tolist()
    assert "qsiprep" in sub01
    assert "freesurfer" in sub01
    assert "bids" not in sub01


def test_build_manifest_sorted_by_priority(cfg, tmp_path):
    sessions = make_sessions(cfg, tmp_path)
    mark_bids_complete(tmp_path, "sub-0001", "ses-01")
    sessions = make_sessions(cfg, tmp_path)
    manifest = build_manifest(sessions, cfg)
    assert list(manifest["priority"]) == sorted(manifest["priority"].tolist())


def test_build_manifest_no_tasks_when_all_complete(cfg, tmp_path):
    sessions = make_sessions(cfg, tmp_path)
    mark_bids_complete(tmp_path, "sub-0001", "ses-01")
    mark_bids_complete(tmp_path, "sub-0002", "ses-01")
    # Also create qsiprep and freesurfer outputs
    for sub in ("sub-0001", "sub-0002"):
        qp = tmp_path / "derivatives" / "qsiprep" / sub / "ses-01"
        qp.mkdir(parents=True)
        (qp / "out.nii.gz").touch()
        fs = tmp_path / "derivatives" / "freesurfer" / sub / "scripts"
        fs.mkdir(parents=True)
        (fs / "recon-all.done").touch()
    sessions = make_sessions(cfg, tmp_path)
    manifest = build_manifest(sessions, cfg)
    assert manifest.empty


# ---------------------------------------------------------------------------
# load_state / save_state
# ---------------------------------------------------------------------------

def test_load_state_missing_file_returns_empty(cfg):
    state = load_state(cfg)
    assert isinstance(state, pd.DataFrame)
    assert state.empty


def test_load_state_missing_file_has_correct_columns(cfg):
    state = load_state(cfg)
    for col in ("subject", "session", "procedure", "status", "submitted_at", "job_id"):
        assert col in state.columns


def test_save_and_load_state_roundtrip(cfg):
    rows = [
        make_state_row("sub-0001", "ses-01", "bids", "complete"),
        make_state_row("sub-0001", "ses-01", "qsiprep", "running"),
    ]
    state = pd.DataFrame(rows)
    save_state(state, cfg)
    loaded = load_state(cfg)
    assert len(loaded) == 2
    assert set(loaded["status"]) == {"complete", "running"}


def test_save_state_creates_parent_dirs(tmp_path):
    cfg = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "deep" / "nested" / "state.parquet",
    )
    state = pd.DataFrame([make_state_row("sub-0001", "ses-01", "bids", "complete")])
    save_state(state, cfg)
    assert cfg.state_file.exists()


def test_load_state_preserves_values(cfg):
    rows = [make_state_row("sub-0001", "ses-01", "bids", "failed", job_id="99")]
    save_state(pd.DataFrame(rows), cfg)
    loaded = load_state(cfg)
    assert loaded.iloc[0]["job_id"] == "99"
    assert loaded.iloc[0]["status"] == "failed"


# ---------------------------------------------------------------------------
# filter_in_flight
# ---------------------------------------------------------------------------

def make_manifest_row(subject, session, procedure, priority=0):
    return {
        "subject": subject,
        "session": session,
        "procedure": procedure,
        "dicom_path": Path(f"/fake/{subject}/{session}"),
        "priority": priority,
    }


def test_filter_in_flight_removes_pending(cfg):
    manifest = pd.DataFrame([make_manifest_row("sub-0001", "ses-01", "bids")])
    state = pd.DataFrame([make_state_row("sub-0001", "ses-01", "bids", "pending")])
    result = filter_in_flight(manifest, state)
    assert result.empty


def test_filter_in_flight_removes_running(cfg):
    manifest = pd.DataFrame([make_manifest_row("sub-0001", "ses-01", "qsiprep")])
    state = pd.DataFrame([make_state_row("sub-0001", "ses-01", "qsiprep", "running")])
    result = filter_in_flight(manifest, state)
    assert result.empty


def test_filter_in_flight_keeps_failed(cfg):
    manifest = pd.DataFrame([make_manifest_row("sub-0001", "ses-01", "bids")])
    state = pd.DataFrame([make_state_row("sub-0001", "ses-01", "bids", "failed")])
    result = filter_in_flight(manifest, state)
    assert len(result) == 1


def test_filter_in_flight_keeps_complete(cfg):
    # complete tasks should not appear in the manifest at all (rule won't fire),
    # but filter_in_flight should also not strip them if somehow present
    manifest = pd.DataFrame([make_manifest_row("sub-0001", "ses-01", "bids")])
    state = pd.DataFrame([make_state_row("sub-0001", "ses-01", "bids", "complete")])
    result = filter_in_flight(manifest, state)
    assert len(result) == 1


def test_filter_in_flight_empty_state(cfg):
    manifest = pd.DataFrame([make_manifest_row("sub-0001", "ses-01", "bids")])
    state = load_state(cfg)  # empty
    result = filter_in_flight(manifest, state)
    assert len(result) == 1


def test_filter_in_flight_empty_manifest(cfg):
    manifest = pd.DataFrame(columns=["subject", "session", "procedure", "dicom_path", "priority"])
    state = pd.DataFrame([make_state_row("sub-0001", "ses-01", "bids", "running")])
    result = filter_in_flight(manifest, state)
    assert result.empty


def test_filter_in_flight_partial_removal(cfg):
    manifest = pd.DataFrame([
        make_manifest_row("sub-0001", "ses-01", "bids"),
        make_manifest_row("sub-0002", "ses-01", "bids"),
    ])
    state = pd.DataFrame([make_state_row("sub-0001", "ses-01", "bids", "pending")])
    result = filter_in_flight(manifest, state)
    assert len(result) == 1
    assert result.iloc[0]["subject"] == "sub-0002"


def test_filter_in_flight_different_procedures_not_removed(cfg):
    manifest = pd.DataFrame([
        make_manifest_row("sub-0001", "ses-01", "qsiprep"),
    ])
    state = pd.DataFrame([make_state_row("sub-0001", "ses-01", "bids", "pending")])
    result = filter_in_flight(manifest, state)
    assert len(result) == 1

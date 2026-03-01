"""Tests for manifest.py."""
from pathlib import Path

import pandas as pd
import pytest

from snbb_scheduler.config import DEFAULT_PROCEDURES, Procedure, SchedulerConfig
from snbb_scheduler.manifest import (
    build_manifest,
    filter_in_flight,
    load_state,
    reconcile_with_filesystem,
    save_state,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_sessions(cfg: SchedulerConfig, tmp_path: Path) -> pd.DataFrame:
    """Return a two-row sessions DataFrame from fake_data_dir layout."""
    from snbb_scheduler.sessions import discover_sessions

    (tmp_path / "dicom" / "sub-0001" / "ses-01").mkdir(parents=True, exist_ok=True)
    (tmp_path / "dicom" / "sub-0002" / "ses-01").mkdir(parents=True, exist_ok=True)
    return discover_sessions(cfg)


def mark_bids_complete(tmp_path: Path, subject: str, session: str) -> None:
    """Create BIDS modality files matching the bids completion_marker."""
    bids_dir = tmp_path / "bids" / subject / session
    files = {
        "anat": ["sub_T1w.nii.gz"],
        "dwi": [
            "sub_dir-AP_dwi.nii.gz",
            "sub_dir-AP_dwi.bvec",
            "sub_dir-AP_dwi.bval",
            "sub_dir-PA_dwi.nii.gz",
        ],
        "fmap": [
            "sub_acq-func_dir-AP_epi.nii.gz",
            "sub_acq-func_dir-PA_epi.nii.gz",
        ],
        "func": ["sub_task-rest_bold.nii.gz"],
    }
    for subdir, names in files.items():
        d = bids_dir / subdir
        d.mkdir(parents=True, exist_ok=True)
        for name in names:
            (d / name).touch()


def mark_bids_post_complete(tmp_path: Path, subject: str, session: str) -> None:
    """Create the derived DWI EPI fieldmap that marks bids_post as complete."""
    fmap_dir = tmp_path / "bids" / subject / session / "fmap"
    fmap_dir.mkdir(parents=True, exist_ok=True)
    (fmap_dir / "sub_acq-dwi_dir-PA_epi.nii.gz").touch()


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
    """Once BIDS and bids_post are done for sub-0001, qsiprep/freesurfer appear."""
    sessions = make_sessions(cfg, tmp_path)
    mark_bids_complete(tmp_path, "sub-0001", "ses-01")
    mark_bids_post_complete(tmp_path, "sub-0001", "ses-01")
    sessions = make_sessions(cfg, tmp_path)  # re-discover with updated FS
    manifest = build_manifest(sessions, cfg)
    sub01 = manifest[manifest["subject"] == "sub-0001"]["procedure"].tolist()
    assert "qsiprep" in sub01
    assert "freesurfer" in sub01
    assert "bids" not in sub01
    assert "bids_post" not in sub01


def test_build_manifest_sorted_by_priority(cfg, tmp_path):
    sessions = make_sessions(cfg, tmp_path)
    mark_bids_complete(tmp_path, "sub-0001", "ses-01")
    sessions = make_sessions(cfg, tmp_path)
    manifest = build_manifest(sessions, cfg)
    assert list(manifest["priority"]) == sorted(manifest["priority"].tolist())


def mark_defacing_complete(tmp_path: Path, subject: str, session: str) -> None:
    """Create an acq-defaced T1w file that marks defacing as complete."""
    anat_dir = tmp_path / "bids" / subject / session / "anat"
    anat_dir.mkdir(parents=True, exist_ok=True)
    (anat_dir / f"{subject}_{session}_acq-defaced_T1w.nii.gz").touch()


def mark_freesurfer_complete(
    tmp_path: Path, subject: str, session: str, sessions: list[str] | None = None
) -> None:
    """Create FreeSurfer longitudinal completion markers.

    For single-session subjects, creates ``<subject>/scripts/recon-all.done``.
    For multi-session subjects (when *sessions* has 2+ entries), creates all
    three sets of done files: cross-sectional, template, and longitudinal.
    """
    subjects_dir = tmp_path / "derivatives" / "freesurfer"
    if sessions is None:
        sessions = [session]

    if len(sessions) == 1:
        s = subjects_dir / subject / "scripts"
        s.mkdir(parents=True, exist_ok=True)
        (s / "recon-all.done").touch()
    else:
        for ses in sessions:
            s = subjects_dir / f"{subject}_{ses}" / "scripts"
            s.mkdir(parents=True, exist_ok=True)
            (s / "recon-all.done").touch()
        s = subjects_dir / subject / "scripts"
        s.mkdir(parents=True, exist_ok=True)
        (s / "recon-all.done").touch()
        for ses in sessions:
            s = subjects_dir / f"{subject}_{ses}.long.{subject}" / "scripts"
            s.mkdir(parents=True, exist_ok=True)
            (s / "recon-all.done").touch()


def test_build_manifest_no_tasks_when_all_complete(cfg, tmp_path):
    sessions = make_sessions(cfg, tmp_path)
    mark_bids_complete(tmp_path, "sub-0001", "ses-01")
    mark_bids_complete(tmp_path, "sub-0002", "ses-01")
    mark_bids_post_complete(tmp_path, "sub-0001", "ses-01")
    mark_bids_post_complete(tmp_path, "sub-0002", "ses-01")
    for sub in ("sub-0001", "sub-0002"):
        mark_defacing_complete(tmp_path, sub, "ses-01")
        # qsiprep: subject-scoped, session subdir matches BIDS DWI sessions
        qp = tmp_path / "derivatives" / "qsiprep" / sub / "ses-01"
        qp.mkdir(parents=True)
        (qp / "out.nii.gz").touch()
        _make_bids_t1w(tmp_path, sub, "ses-01")
        mark_freesurfer_complete(tmp_path, sub, "ses-01")
        # qsirecon: session subdir count must match qsiprep
        qr = tmp_path / "derivatives" / "qsirecon-MRtrix3_act-HSVS" / sub / "ses-01"
        qr.mkdir(parents=True)
        (qr / "report.html").touch()
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


# ---------------------------------------------------------------------------
# reconcile_with_filesystem
# ---------------------------------------------------------------------------

def test_reconcile_empty_state(cfg):
    state = pd.DataFrame(
        columns=["subject", "session", "procedure", "status", "submitted_at", "job_id"]
    )
    result = reconcile_with_filesystem(state, cfg)
    assert result.empty


def test_reconcile_no_in_flight(cfg):
    state = pd.DataFrame([make_state_row("sub-0001", "ses-01", "bids", "complete")])
    result = reconcile_with_filesystem(state, cfg)
    assert result.iloc[0]["status"] == "complete"


def test_reconcile_pending_output_missing(cfg, tmp_path):
    """Output does not exist on disk → stays pending."""
    state = pd.DataFrame([make_state_row("sub-0001", "ses-01", "bids", "pending")])
    result = reconcile_with_filesystem(state, cfg)
    assert result.iloc[0]["status"] == "pending"


def test_reconcile_pending_bids_output_present(cfg, tmp_path):
    """bids output exists on disk → flipped to complete."""
    mark_bids_complete(tmp_path, "sub-0001", "ses-01")
    state = pd.DataFrame([make_state_row("sub-0001", "ses-01", "bids", "pending")])
    result = reconcile_with_filesystem(state, cfg)
    assert result.iloc[0]["status"] == "complete"


def test_reconcile_running_bids_output_present(cfg, tmp_path):
    """running status also gets resolved when output exists."""
    mark_bids_complete(tmp_path, "sub-0001", "ses-01")
    state = pd.DataFrame([make_state_row("sub-0001", "ses-01", "bids", "running")])
    result = reconcile_with_filesystem(state, cfg)
    assert result.iloc[0]["status"] == "complete"


def test_reconcile_original_unchanged(cfg, tmp_path):
    """Original state DataFrame is not mutated."""
    mark_bids_complete(tmp_path, "sub-0001", "ses-01")
    state = pd.DataFrame([make_state_row("sub-0001", "ses-01", "bids", "pending")])
    reconcile_with_filesystem(state, cfg)
    assert state.iloc[0]["status"] == "pending"


def test_reconcile_unknown_procedure_skipped(cfg):
    """Rows with an unknown procedure name are skipped without error."""
    state = pd.DataFrame([make_state_row("sub-0001", "ses-01", "nonexistent", "pending")])
    result = reconcile_with_filesystem(state, cfg)
    assert result.iloc[0]["status"] == "pending"


def test_reconcile_logs_transition(cfg, tmp_path):
    from unittest.mock import MagicMock
    mark_bids_complete(tmp_path, "sub-0001", "ses-01")
    state = pd.DataFrame([make_state_row("sub-0001", "ses-01", "bids", "pending")])
    audit = MagicMock()
    reconcile_with_filesystem(state, cfg, audit=audit)
    audit.log.assert_called_once_with(
        "status_change",
        subject="sub-0001",
        session="ses-01",
        procedure="bids",
        job_id="12345",
        old_status="pending",
        new_status="complete",
    )


def test_reconcile_no_log_when_incomplete(cfg, tmp_path):
    from unittest.mock import MagicMock
    state = pd.DataFrame([make_state_row("sub-0001", "ses-01", "bids", "pending")])
    audit = MagicMock()
    reconcile_with_filesystem(state, cfg, audit=audit)
    audit.log.assert_not_called()


def test_reconcile_partial_resolution(cfg, tmp_path):
    """Only the session with output on disk is resolved."""
    mark_bids_complete(tmp_path, "sub-0001", "ses-01")
    state = pd.DataFrame([
        make_state_row("sub-0001", "ses-01", "bids", "pending"),
        make_state_row("sub-0002", "ses-01", "bids", "pending"),
    ])
    result = reconcile_with_filesystem(state, cfg)
    assert result.iloc[0]["status"] == "complete"
    assert result.iloc[1]["status"] == "pending"


# ---------------------------------------------------------------------------
# FreeSurfer longitudinal manifest tests
# ---------------------------------------------------------------------------


def make_two_session_df(cfg: SchedulerConfig, tmp_path: Path) -> "pd.DataFrame":
    """Create DICOM dirs for sub-0001/ses-01 and sub-0001/ses-02, return sessions df."""
    from snbb_scheduler.sessions import discover_sessions

    for session in ("ses-01", "ses-02"):
        (tmp_path / "dicom" / "sub-0001" / session).mkdir(parents=True, exist_ok=True)
    return discover_sessions(cfg)


def _make_bids_t1w(tmp_path: Path, subject: str, session: str) -> None:
    """Create a minimal BIDS T1w file so _count_bids_anat_sessions finds the session."""
    anat = tmp_path / "bids" / subject / session / "anat"
    anat.mkdir(parents=True, exist_ok=True)
    (anat / f"{subject}_{session}_T1w.nii.gz").touch()


def test_freesurfer_appears_when_bids_post_complete(cfg, tmp_path):
    """freesurfer appears in manifest once bids_post is complete for all sessions."""
    (tmp_path / "dicom" / "sub-0001" / "ses-01").mkdir(parents=True, exist_ok=True)
    mark_bids_complete(tmp_path, "sub-0001", "ses-01")
    mark_bids_post_complete(tmp_path, "sub-0001", "ses-01")
    _make_bids_t1w(tmp_path, "sub-0001", "ses-01")

    from snbb_scheduler.sessions import discover_sessions
    sessions_df = discover_sessions(cfg)
    manifest = build_manifest(sessions_df, cfg)
    assert "freesurfer" in set(manifest["procedure"])


def test_freesurfer_not_in_manifest_without_bids_post(cfg, tmp_path):
    """freesurfer must not appear until bids_post is complete."""
    (tmp_path / "dicom" / "sub-0001" / "ses-01").mkdir(parents=True, exist_ok=True)
    mark_bids_complete(tmp_path, "sub-0001", "ses-01")

    from snbb_scheduler.sessions import discover_sessions
    sessions_df = discover_sessions(cfg)
    manifest = build_manifest(sessions_df, cfg)
    assert "freesurfer" not in set(manifest["procedure"])


def test_freesurfer_deduplicated_in_manifest(cfg, tmp_path):
    """freesurfer appears only once per subject even with two sessions."""
    sessions_df = make_two_session_df(cfg, tmp_path)
    for session in ("ses-01", "ses-02"):
        mark_bids_complete(tmp_path, "sub-0001", session)
        mark_bids_post_complete(tmp_path, "sub-0001", session)
        _make_bids_t1w(tmp_path, "sub-0001", session)

    manifest = build_manifest(sessions_df, cfg)

    fs_rows = manifest[manifest["procedure"] == "freesurfer"]
    assert len(fs_rows) == 1
    assert fs_rows.iloc[0]["session"] == ""


def test_freesurfer_not_in_manifest_when_one_session_missing_bids_post(cfg, tmp_path):
    """freesurfer must not appear if any session's bids_post is still incomplete."""
    sessions_df = make_two_session_df(cfg, tmp_path)
    for session in ("ses-01", "ses-02"):
        mark_bids_complete(tmp_path, "sub-0001", session)
    # Only ses-01 has bids_post
    mark_bids_post_complete(tmp_path, "sub-0001", "ses-01")

    manifest = build_manifest(sessions_df, cfg)
    assert "freesurfer" not in set(manifest["procedure"])


def test_reconcile_freesurfer_completion_single_session(cfg, tmp_path):
    """reconcile_with_filesystem detects completed single-session freesurfer."""
    subject, session = "sub-0001", "ses-01"
    _make_bids_t1w(tmp_path, subject, session)
    mark_freesurfer_complete(tmp_path, subject, session, sessions=[session])

    state = pd.DataFrame([make_state_row(subject, "", "freesurfer", "pending")])
    result = reconcile_with_filesystem(state, cfg)
    assert result.iloc[0]["status"] == "complete"


def test_reconcile_freesurfer_completion_multi_session(cfg, tmp_path):
    """reconcile_with_filesystem detects completed multi-session freesurfer (all 3 steps)."""
    subject = "sub-0001"
    sessions = ["ses-01", "ses-02"]
    for ses in sessions:
        _make_bids_t1w(tmp_path, subject, ses)
    mark_freesurfer_complete(tmp_path, subject, sessions[0], sessions=sessions)

    state = pd.DataFrame([make_state_row(subject, "", "freesurfer", "pending")])
    result = reconcile_with_filesystem(state, cfg)
    assert result.iloc[0]["status"] == "complete"


def test_reconcile_freesurfer_incomplete_missing_longitudinal(cfg, tmp_path):
    """reconcile stays pending when longitudinal step is not yet done."""
    subject, session = "sub-0001", "ses-01"
    sessions = ["ses-01", "ses-02"]
    for ses in sessions:
        _make_bids_t1w(tmp_path, subject, ses)
    subjects_dir = tmp_path / "derivatives" / "freesurfer"
    # Create cross-sectional and template done files, but not longitudinal
    for ses in sessions:
        s = subjects_dir / f"{subject}_{ses}" / "scripts"
        s.mkdir(parents=True, exist_ok=True)
        (s / "recon-all.done").touch()
    s = subjects_dir / subject / "scripts"
    s.mkdir(parents=True, exist_ok=True)
    (s / "recon-all.done").touch()
    # Intentionally skip the longitudinal done files

    state = pd.DataFrame([make_state_row(subject, "", "freesurfer", "pending")])
    result = reconcile_with_filesystem(state, cfg)
    assert result.iloc[0]["status"] == "pending"

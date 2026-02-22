"""Tests for rules.py.

Row fixtures are built with real tmp_path directories so is_complete() can
check the filesystem. Helper functions create the minimal output structure
that marks a procedure as complete.
"""
from pathlib import Path

import pandas as pd
import pytest

from snbb_scheduler.config import DEFAULT_PROCEDURES, Procedure, SchedulerConfig
from snbb_scheduler.rules import build_rules


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def cfg(tmp_path):
    return SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
    )


def make_row(cfg: SchedulerConfig, subject: str = "sub-0001", session: str = "ses-01") -> dict:
    """Build a row dict with all path columns for the given config."""
    row: dict = {
        "subject": subject,
        "session": session,
        "dicom_path": cfg.dicom_root / subject / session,
        "dicom_exists": False,
    }
    for proc in cfg.procedures:
        root = cfg.get_procedure_root(proc)
        if proc.scope == "subject":
            path = root / subject
        else:
            path = root / subject / session
        row[f"{proc.name}_path"] = path
        row[f"{proc.name}_exists"] = path.exists()
    return row


def mark_dicom(row: dict) -> None:
    row["dicom_path"].mkdir(parents=True, exist_ok=True)
    (row["dicom_path"] / "file.dcm").touch()
    row["dicom_exists"] = True


def mark_bids_complete(row: dict) -> None:
    anat = row["bids_path"] / "anat"
    anat.mkdir(parents=True, exist_ok=True)
    (anat / "T1w.nii.gz").touch()


def mark_qsiprep_complete(row: dict) -> None:
    row["qsiprep_path"].mkdir(parents=True, exist_ok=True)
    (row["qsiprep_path"] / "dwi.nii.gz").touch()


def mark_freesurfer_complete(row: dict) -> None:
    scripts = row["freesurfer_path"] / "scripts"
    scripts.mkdir(parents=True, exist_ok=True)
    (scripts / "recon-all.done").touch()


# ---------------------------------------------------------------------------
# build_rules returns correct keys
# ---------------------------------------------------------------------------

def test_build_rules_keys_match_procedures(cfg):
    rules = build_rules(cfg)
    assert set(rules.keys()) == {p.name for p in cfg.procedures}


def test_rule_functions_are_callable(cfg):
    rules = build_rules(cfg)
    for rule in rules.values():
        assert callable(rule)


# ---------------------------------------------------------------------------
# dicom_exists gate — no rule fires without DICOM data
# ---------------------------------------------------------------------------

def test_no_rule_fires_without_dicom(cfg):
    row = pd.Series(make_row(cfg))  # dicom_exists=False
    rules = build_rules(cfg)
    for name, rule in rules.items():
        assert rule(row) is False, f"{name} fired without DICOM"


# ---------------------------------------------------------------------------
# bids rule
# ---------------------------------------------------------------------------

def test_bids_needed_when_dicom_present_bids_absent(cfg):
    row = make_row(cfg)
    mark_dicom(row)
    rules = build_rules(cfg)
    assert rules["bids"](pd.Series(row)) is True


def test_bids_not_needed_when_already_complete(cfg):
    row = make_row(cfg)
    mark_dicom(row)
    mark_bids_complete(row)
    rules = build_rules(cfg)
    assert rules["bids"](pd.Series(row)) is False


# ---------------------------------------------------------------------------
# qsiprep rule — depends on bids
# ---------------------------------------------------------------------------

def test_qsiprep_not_needed_when_bids_incomplete(cfg):
    row = make_row(cfg)
    mark_dicom(row)
    # bids NOT complete
    rules = build_rules(cfg)
    assert rules["qsiprep"](pd.Series(row)) is False


def test_qsiprep_needed_when_bids_complete_qsiprep_absent(cfg):
    row = make_row(cfg)
    mark_dicom(row)
    mark_bids_complete(row)
    rules = build_rules(cfg)
    assert rules["qsiprep"](pd.Series(row)) is True


def test_qsiprep_not_needed_when_already_complete(cfg):
    row = make_row(cfg)
    mark_dicom(row)
    mark_bids_complete(row)
    mark_qsiprep_complete(row)
    rules = build_rules(cfg)
    assert rules["qsiprep"](pd.Series(row)) is False


# ---------------------------------------------------------------------------
# freesurfer rule — depends on bids, subject-scoped
# ---------------------------------------------------------------------------

def test_freesurfer_not_needed_when_bids_incomplete(cfg):
    row = make_row(cfg)
    mark_dicom(row)
    rules = build_rules(cfg)
    assert rules["freesurfer"](pd.Series(row)) is False


def test_freesurfer_needed_when_bids_complete_fs_absent(cfg):
    row = make_row(cfg)
    mark_dicom(row)
    mark_bids_complete(row)
    rules = build_rules(cfg)
    assert rules["freesurfer"](pd.Series(row)) is True


def test_freesurfer_not_needed_when_already_complete(cfg):
    row = make_row(cfg)
    mark_dicom(row)
    mark_bids_complete(row)
    mark_freesurfer_complete(row)
    rules = build_rules(cfg)
    assert rules["freesurfer"](pd.Series(row)) is False


# ---------------------------------------------------------------------------
# bids and downstream can be needed simultaneously
# ---------------------------------------------------------------------------

def test_only_bids_fires_when_nothing_done(cfg):
    row = make_row(cfg)
    mark_dicom(row)
    rules = build_rules(cfg)
    assert rules["bids"](pd.Series(row)) is True
    assert rules["qsiprep"](pd.Series(row)) is False
    assert rules["freesurfer"](pd.Series(row)) is False


def test_downstream_fire_once_bids_done(cfg):
    row = make_row(cfg)
    mark_dicom(row)
    mark_bids_complete(row)
    rules = build_rules(cfg)
    assert rules["bids"](pd.Series(row)) is False
    assert rules["qsiprep"](pd.Series(row)) is True
    assert rules["freesurfer"](pd.Series(row)) is True


def test_nothing_fires_when_all_complete(cfg):
    row = make_row(cfg)
    mark_dicom(row)
    mark_bids_complete(row)
    mark_qsiprep_complete(row)
    mark_freesurfer_complete(row)
    rules = build_rules(cfg)
    for name, rule in rules.items():
        assert rule(pd.Series(row)) is False, f"{name} fired when already complete"


# ---------------------------------------------------------------------------
# Dynamic custom procedure
# ---------------------------------------------------------------------------

def test_custom_procedure_rule_generated(tmp_path):
    fmriprep = Procedure(
        name="fmriprep",
        output_dir="fmriprep",
        script="snbb_run_fmriprep.sh",
        depends_on=["bids"],
        completion_marker=None,
    )
    cfg = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
        procedures=[*DEFAULT_PROCEDURES, fmriprep],
    )
    rules = build_rules(cfg)
    assert "fmriprep" in rules


def test_custom_procedure_respects_dependency(tmp_path):
    fmriprep = Procedure(
        name="fmriprep",
        output_dir="fmriprep",
        script="snbb_run_fmriprep.sh",
        depends_on=["bids"],
        completion_marker=None,
    )
    cfg = SchedulerConfig(
        dicom_root=tmp_path / "dicom",
        bids_root=tmp_path / "bids",
        derivatives_root=tmp_path / "derivatives",
        state_file=tmp_path / "state.parquet",
        procedures=[*DEFAULT_PROCEDURES, fmriprep],
    )
    row = make_row(cfg)
    mark_dicom(row)
    rules = build_rules(cfg)
    # bids not yet done → fmriprep should not fire
    assert rules["fmriprep"](pd.Series(row)) is False

    mark_bids_complete(row)
    assert rules["fmriprep"](pd.Series(row)) is True

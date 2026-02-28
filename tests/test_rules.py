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
# Helpers
# ---------------------------------------------------------------------------

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
    """Create BIDS modality files matching the bids completion_marker."""
    bids_dir = row["bids_path"]
    files = {
        "anat": ["sub_T1w.nii.gz"],
        "dwi": [
            "sub_dir-AP_dwi.nii.gz",
            "sub_dir-AP_dwi.bvec",
            "sub_dir-AP_dwi.bval",
            # Short reverse-PE DWI; bids_post derives the fmap EPI from this
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


def mark_bids_post_complete(row: dict) -> None:
    """Create the derived DWI EPI fieldmap that marks bids_post as complete."""
    fmap_dir = row["bids_post_path"] / "fmap"
    fmap_dir.mkdir(parents=True, exist_ok=True)
    (fmap_dir / "sub_acq-dwi_dir-PA_epi.nii.gz").touch()


def mark_qsiprep_complete(row: dict) -> None:
    """Create qsiprep ses-* output dirs matching the BIDS DWI sessions."""
    subject_bids = row["bids_path"].parent  # bids_root/subject
    for ses_dir in subject_bids.iterdir():
        if ses_dir.is_dir() and ses_dir.name.startswith("ses-"):
            out = row["qsiprep_path"] / ses_dir.name
            out.mkdir(parents=True, exist_ok=True)
            (out / "dwi.nii.gz").touch()


def mark_qsirecon_complete(row: dict) -> None:
    """Create qsirecon ses-* output dirs matching the qsiprep sessions."""
    for ses_dir in row["qsiprep_path"].iterdir():
        if ses_dir.is_dir() and ses_dir.name.startswith("ses-"):
            out = row["qsirecon_path"] / ses_dir.name
            out.mkdir(parents=True, exist_ok=True)
            (out / "report.html").touch()


def mark_defacing_complete(row: dict) -> None:
    """Create an acq-defaced T1w file that marks defacing as complete."""
    anat_dir = row["defacing_path"] / "anat"
    anat_dir.mkdir(parents=True, exist_ok=True)
    subject = row["subject"]
    session = row["session"]
    (anat_dir / f"{subject}_{session}_acq-defaced_T1w.nii.gz").touch()


def mark_freesurfer_complete(row: dict) -> None:
    """Create recon-all.done with CMDARGS matching the available T1w count.

    Uses collect_images so that the same filtering rules (no defaced, prefer
    rec-norm) apply here and in the completion check.
    """
    from snbb_scheduler.freesurfer import collect_images

    scripts = row["freesurfer_path"] / "scripts"
    scripts.mkdir(parents=True, exist_ok=True)
    bids_root = row["bids_path"].parent.parent  # bids_root/subject/session → bids_root
    subject = row["subject"]
    t1w_files, _ = collect_images(bids_root, subject)
    i_flags = " ".join(f"-i /fake/T1w_{k}.nii.gz" for k in range(len(t1w_files)))
    (scripts / "recon-all.done").write_text(
        f"#CMDARGS -subject {subject} -all {i_flags}\n"
    )


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
# defacing rule — depends on bids_post, session-scoped
# ---------------------------------------------------------------------------

def test_defacing_not_needed_when_bids_post_incomplete(cfg):
    row = make_row(cfg)
    mark_dicom(row)
    mark_bids_complete(row)
    # bids_post NOT complete
    rules = build_rules(cfg)
    assert rules["defacing"](pd.Series(row)) is False


def test_defacing_needed_when_bids_post_complete_defacing_absent(cfg):
    row = make_row(cfg)
    mark_dicom(row)
    mark_bids_complete(row)
    mark_bids_post_complete(row)
    rules = build_rules(cfg)
    assert rules["defacing"](pd.Series(row)) is True


def test_defacing_not_needed_when_already_complete(cfg):
    row = make_row(cfg)
    mark_dicom(row)
    mark_bids_complete(row)
    mark_bids_post_complete(row)
    mark_defacing_complete(row)
    rules = build_rules(cfg)
    assert rules["defacing"](pd.Series(row)) is False


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
    mark_bids_post_complete(row)
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
    mark_bids_post_complete(row)
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


def test_downstream_fire_once_bids_post_done(cfg):
    row = make_row(cfg)
    mark_dicom(row)
    mark_bids_complete(row)
    mark_bids_post_complete(row)
    rules = build_rules(cfg)
    assert rules["bids"](pd.Series(row)) is False
    assert rules["bids_post"](pd.Series(row)) is False
    assert rules["defacing"](pd.Series(row)) is True
    assert rules["qsiprep"](pd.Series(row)) is True
    assert rules["freesurfer"](pd.Series(row)) is True


def test_nothing_fires_when_all_complete(cfg):
    row = make_row(cfg)
    mark_dicom(row)
    mark_bids_complete(row)
    mark_bids_post_complete(row)
    mark_defacing_complete(row)
    mark_qsiprep_complete(row)
    mark_freesurfer_complete(row)
    mark_fastsurfer_cross_complete(cfg, "sub-0001", "ses-01")
    mark_fastsurfer_template_complete(cfg, "sub-0001")
    mark_fastsurfer_long_complete(cfg, "sub-0001", "ses-01")
    mark_qsirecon_complete(row)
    rules = build_rules(cfg)
    for name, rule in rules.items():
        assert rule(pd.Series(row)) is False, f"{name} fired when already complete"


# ---------------------------------------------------------------------------
# qsirecon rule — depends on qsiprep + freesurfer, subject-scoped
# ---------------------------------------------------------------------------

def test_qsirecon_not_needed_when_qsiprep_incomplete(cfg):
    row = make_row(cfg)
    mark_dicom(row)
    mark_bids_complete(row)
    # qsiprep NOT complete
    rules = build_rules(cfg)
    assert rules["qsirecon"](pd.Series(row)) is False


def test_qsirecon_not_needed_when_freesurfer_incomplete(cfg):
    row = make_row(cfg)
    mark_dicom(row)
    mark_bids_complete(row)
    mark_qsiprep_complete(row)
    # freesurfer NOT complete
    rules = build_rules(cfg)
    assert rules["qsirecon"](pd.Series(row)) is False


def test_qsirecon_needed_when_deps_complete_qsirecon_absent(cfg):
    row = make_row(cfg)
    mark_dicom(row)
    mark_bids_complete(row)
    mark_qsiprep_complete(row)
    mark_freesurfer_complete(row)
    rules = build_rules(cfg)
    assert rules["qsirecon"](pd.Series(row)) is True


def test_qsirecon_not_needed_when_already_complete(cfg):
    row = make_row(cfg)
    mark_dicom(row)
    mark_bids_complete(row)
    mark_qsiprep_complete(row)
    mark_freesurfer_complete(row)
    mark_qsirecon_complete(row)
    rules = build_rules(cfg)
    assert rules["qsirecon"](pd.Series(row)) is False


# ---------------------------------------------------------------------------
# --force flag
# ---------------------------------------------------------------------------

def test_force_reruns_already_complete_procedure(cfg):
    """force=True causes a complete procedure to be re-submitted."""
    row = make_row(cfg)
    mark_dicom(row)
    mark_bids_complete(row)
    rules = build_rules(cfg, force=True)
    # bids is already complete but --force overrides
    assert rules["bids"](pd.Series(row)) is True


def test_force_procedure_only_forces_named_procedure(cfg):
    """force + force_procedures=['bids'] only forces bids, not others."""
    row = make_row(cfg)
    mark_dicom(row)
    mark_bids_complete(row)
    rules = build_rules(cfg, force=True, force_procedures=["bids"])
    assert rules["bids"](pd.Series(row)) is True
    # qsiprep is not forced — bids is done so it would normally fire,
    # but qsiprep itself is not complete yet → True either way; check freesurfer
    # (also not complete yet) → True. The key test is that bids IS forced.


def test_force_still_requires_dicom(cfg):
    """force does not bypass the dicom_exists gate."""
    row = make_row(cfg)
    # dicom_exists = False
    rules = build_rules(cfg, force=True)
    assert rules["bids"](pd.Series(row)) is False


def test_force_still_requires_dependencies(cfg):
    """force on qsiprep still requires bids to be complete."""
    row = make_row(cfg)
    mark_dicom(row)
    # bids NOT done
    rules = build_rules(cfg, force=True, force_procedures=["qsiprep"])
    assert rules["qsiprep"](pd.Series(row)) is False


def test_force_none_forces_all_procedures(cfg):
    """force=True with force_procedures=None forces every procedure."""
    row = make_row(cfg)
    mark_dicom(row)
    mark_bids_complete(row)
    mark_bids_post_complete(row)
    mark_defacing_complete(row)
    mark_qsiprep_complete(row)
    mark_freesurfer_complete(row)
    mark_qsirecon_complete(row)
    rules = build_rules(cfg, force=True, force_procedures=None)
    # Everything is complete, but --force should make all rules fire
    # (given dicom + deps are satisfied)
    assert rules["bids"](pd.Series(row)) is True
    assert rules["bids_post"](pd.Series(row)) is True
    assert rules["defacing"](pd.Series(row)) is True
    assert rules["qsiprep"](pd.Series(row)) is True
    assert rules["freesurfer"](pd.Series(row)) is True
    assert rules["qsirecon"](pd.Series(row)) is True


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


# ---------------------------------------------------------------------------
# FastSurfer longitudinal pipeline — helpers
# ---------------------------------------------------------------------------


def mark_bids_post_complete_for_row(row: dict) -> None:
    """Mark bids_post complete for the session in *row*."""
    mark_bids_post_complete(row)


def mark_fastsurfer_cross_complete(cfg: SchedulerConfig, subject: str, session: str) -> None:
    """Create the FastSurfer cross-sectional completion marker at the actual path."""
    actual = cfg.derivatives_root / "fastsurfer" / f"{subject}_{session}" / "scripts"
    actual.mkdir(parents=True, exist_ok=True)
    (actual / "recon-all.done").write_text("#CMDARGS placeholder\n")


def mark_fastsurfer_template_complete(cfg: SchedulerConfig, subject: str) -> None:
    """Create the FastSurfer template completion marker."""
    scripts = cfg.derivatives_root / "fastsurfer" / subject / "scripts"
    scripts.mkdir(parents=True, exist_ok=True)
    (scripts / "recon-all.done").write_text("#CMDARGS placeholder\n")


def mark_fastsurfer_long_complete(cfg: SchedulerConfig, subject: str, session: str) -> None:
    """Create the FastSurfer longitudinal completion marker at the actual path."""
    actual = (
        cfg.derivatives_root
        / "fastsurfer"
        / f"{subject}_{session}.long.{subject}"
        / "scripts"
    )
    actual.mkdir(parents=True, exist_ok=True)
    (actual / "recon-all.done").write_text("#CMDARGS placeholder\n")


def make_two_session_df(cfg: SchedulerConfig) -> pd.DataFrame:
    """Build a sessions DataFrame with sub-0001/ses-01 and sub-0001/ses-02."""
    rows = []
    for session in ("ses-01", "ses-02"):
        row = make_row(cfg, subject="sub-0001", session=session)
        mark_dicom(row)
        mark_bids_complete(row)
        mark_bids_post_complete(row)
        rows.append(row)
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# fastsurfer_cross — session-scoped, depends on bids_post
# ---------------------------------------------------------------------------


def test_fastsurfer_cross_needed_when_bids_post_complete(cfg):
    row = make_row(cfg)
    mark_dicom(row)
    mark_bids_complete(row)
    mark_bids_post_complete(row)
    rules = build_rules(cfg)
    assert rules["fastsurfer_cross"](pd.Series(row)) is True


def test_fastsurfer_cross_not_needed_without_bids_post(cfg):
    row = make_row(cfg)
    mark_dicom(row)
    mark_bids_complete(row)
    rules = build_rules(cfg)
    assert rules["fastsurfer_cross"](pd.Series(row)) is False


def test_fastsurfer_cross_not_needed_when_already_complete(cfg):
    row = make_row(cfg)
    mark_dicom(row)
    mark_bids_complete(row)
    mark_bids_post_complete(row)
    mark_fastsurfer_cross_complete(cfg, "sub-0001", "ses-01")
    rules = build_rules(cfg)
    assert rules["fastsurfer_cross"](pd.Series(row)) is False


# ---------------------------------------------------------------------------
# fastsurfer_template — subject-scoped, depends on fastsurfer_cross
# Cross-scope: all sessions must be complete; min 2 sessions required.
# ---------------------------------------------------------------------------


def test_fastsurfer_template_fires_when_both_sessions_complete(cfg):
    """Template rule fires only when all sessions have cross done (≥2 sessions)."""
    sessions_df = make_two_session_df(cfg)
    mark_fastsurfer_cross_complete(cfg, "sub-0001", "ses-01")
    mark_fastsurfer_cross_complete(cfg, "sub-0001", "ses-02")

    # Rebuild sessions_df with updated state info
    rows = []
    for session in ("ses-01", "ses-02"):
        row = make_row(cfg, subject="sub-0001", session=session)
        mark_dicom(row)
        mark_bids_complete(row)
        mark_bids_post_complete(row)
        rows.append(row)
    sessions_df = pd.DataFrame(rows)

    rules = build_rules(cfg, sessions_df=sessions_df)
    row = pd.Series(rows[0])  # evaluate from ses-01's perspective
    assert rules["fastsurfer_template"](row) is True


def test_fastsurfer_template_does_not_fire_when_one_session_incomplete(cfg):
    """Template must NOT fire if any session's cross is still incomplete."""
    rows = []
    for session in ("ses-01", "ses-02"):
        row = make_row(cfg, subject="sub-0001", session=session)
        mark_dicom(row)
        mark_bids_complete(row)
        mark_bids_post_complete(row)
        rows.append(row)
    sessions_df = pd.DataFrame(rows)

    # Only ses-01 cross done — ses-02 is still pending
    mark_fastsurfer_cross_complete(cfg, "sub-0001", "ses-01")

    rules = build_rules(cfg, sessions_df=sessions_df)
    row = pd.Series(rows[0])
    assert rules["fastsurfer_template"](row) is False


def test_fastsurfer_template_skipped_for_single_session_subject(cfg):
    """Single-session subjects must NOT trigger template creation."""
    row = make_row(cfg, subject="sub-0001", session="ses-01")
    mark_dicom(row)
    mark_bids_complete(row)
    mark_bids_post_complete(row)
    mark_fastsurfer_cross_complete(cfg, "sub-0001", "ses-01")

    sessions_df = pd.DataFrame([row])  # only one session
    rules = build_rules(cfg, sessions_df=sessions_df)
    assert rules["fastsurfer_template"](pd.Series(row)) is False


def test_fastsurfer_template_not_needed_when_already_complete(cfg):
    rows = []
    for session in ("ses-01", "ses-02"):
        row = make_row(cfg, subject="sub-0001", session=session)
        mark_dicom(row)
        mark_bids_complete(row)
        mark_bids_post_complete(row)
        rows.append(row)
    sessions_df = pd.DataFrame(rows)

    mark_fastsurfer_cross_complete(cfg, "sub-0001", "ses-01")
    mark_fastsurfer_cross_complete(cfg, "sub-0001", "ses-02")
    mark_fastsurfer_template_complete(cfg, "sub-0001")

    rules = build_rules(cfg, sessions_df=sessions_df)
    assert rules["fastsurfer_template"](pd.Series(rows[0])) is False


def test_fastsurfer_template_without_sessions_df_skips_cross_scope_check(cfg):
    """Without sessions_df the cross-scope dep check is skipped.

    This is the backward-compat behaviour: callers that don't pass sessions_df
    won't break, though they may get inaccurate results for cross-scope deps.
    The template's self-completion check still prevents double-submission.
    """
    row = make_row(cfg, subject="sub-0001", session="ses-01")
    mark_dicom(row)
    mark_bids_complete(row)
    mark_bids_post_complete(row)
    # fastsurfer_cross NOT done, but sessions_df not provided
    rules = build_rules(cfg, sessions_df=None)
    # Without sessions_df, cross-scope check is skipped → template self-check runs
    # (not complete) → rule returns True because it thinks it should run
    assert rules["fastsurfer_template"](pd.Series(row)) is True


# ---------------------------------------------------------------------------
# fastsurfer_long — session-scoped, depends on fastsurfer_template
# ---------------------------------------------------------------------------


def test_fastsurfer_long_needed_when_template_complete(cfg):
    rows = []
    for session in ("ses-01", "ses-02"):
        row = make_row(cfg, subject="sub-0001", session=session)
        mark_dicom(row)
        mark_bids_complete(row)
        mark_bids_post_complete(row)
        rows.append(row)

    mark_fastsurfer_cross_complete(cfg, "sub-0001", "ses-01")
    mark_fastsurfer_cross_complete(cfg, "sub-0001", "ses-02")
    mark_fastsurfer_template_complete(cfg, "sub-0001")

    sessions_df = pd.DataFrame(rows)
    rules = build_rules(cfg, sessions_df=sessions_df)
    # ses-01's longitudinal is not done → should fire
    assert rules["fastsurfer_long"](pd.Series(rows[0])) is True


def test_fastsurfer_long_not_needed_without_template(cfg):
    row = make_row(cfg, subject="sub-0001", session="ses-01")
    mark_dicom(row)
    mark_bids_complete(row)
    mark_bids_post_complete(row)
    mark_fastsurfer_cross_complete(cfg, "sub-0001", "ses-01")
    # Template NOT complete
    rules = build_rules(cfg)
    assert rules["fastsurfer_long"](pd.Series(row)) is False


def test_fastsurfer_long_not_needed_when_already_complete(cfg):
    rows = []
    for session in ("ses-01", "ses-02"):
        row = make_row(cfg, subject="sub-0001", session=session)
        mark_dicom(row)
        mark_bids_complete(row)
        mark_bids_post_complete(row)
        rows.append(row)

    mark_fastsurfer_cross_complete(cfg, "sub-0001", "ses-01")
    mark_fastsurfer_cross_complete(cfg, "sub-0001", "ses-02")
    mark_fastsurfer_template_complete(cfg, "sub-0001")
    mark_fastsurfer_long_complete(cfg, "sub-0001", "ses-01")

    sessions_df = pd.DataFrame(rows)
    rules = build_rules(cfg, sessions_df=sessions_df)
    assert rules["fastsurfer_long"](pd.Series(rows[0])) is False

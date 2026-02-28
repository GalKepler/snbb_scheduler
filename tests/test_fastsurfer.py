"""Tests for snbb_scheduler.fastsurfer — command builders and T1w collection."""
from pathlib import Path

import pytest

from snbb_scheduler.fastsurfer import (
    build_cross_apptainer_command,
    build_long_fastsurfer_command,
    collect_all_session_t1ws,
    collect_session_t1w,
    fastsurfer_long_sid,
    fastsurfer_sid,
)


# ---------------------------------------------------------------------------
# Naming helpers
# ---------------------------------------------------------------------------


def test_fastsurfer_sid():
    assert fastsurfer_sid("sub-0001", "ses-01") == "sub-0001_ses-01"


def test_fastsurfer_sid_different_subject():
    assert fastsurfer_sid("sub-9999", "ses-99") == "sub-9999_ses-99"


def test_fastsurfer_long_sid():
    assert fastsurfer_long_sid("sub-0001", "ses-01") == "sub-0001_ses-01.long.sub-0001"


def test_fastsurfer_long_sid_multi_session():
    assert fastsurfer_long_sid("sub-0002", "ses-02") == "sub-0002_ses-02.long.sub-0002"


# ---------------------------------------------------------------------------
# collect_session_t1w
# ---------------------------------------------------------------------------


def _make_anat(bids_dir: Path, subject: str, session: str, filename: str) -> Path:
    """Create a fake T1w NIfTI file and return its path."""
    anat = bids_dir / subject / session / "anat"
    anat.mkdir(parents=True, exist_ok=True)
    p = anat / filename
    p.touch()
    return p


def test_collect_session_t1w_returns_only_file(tmp_path):
    t1w = _make_anat(tmp_path, "sub-0001", "ses-01", "sub-0001_ses-01_T1w.nii.gz")
    result = collect_session_t1w(tmp_path, "sub-0001", "ses-01")
    assert result == t1w


def test_collect_session_t1w_no_images_returns_none(tmp_path):
    (tmp_path / "sub-0001" / "ses-01" / "anat").mkdir(parents=True)
    result = collect_session_t1w(tmp_path, "sub-0001", "ses-01")
    assert result is None


def test_collect_session_t1w_excludes_defaced(tmp_path):
    _make_anat(tmp_path, "sub-0001", "ses-01", "sub-0001_ses-01_acq-defaced_T1w.nii.gz")
    result = collect_session_t1w(tmp_path, "sub-0001", "ses-01")
    assert result is None


def test_collect_session_t1w_prefers_rec_norm(tmp_path):
    plain = _make_anat(tmp_path, "sub-0001", "ses-01", "sub-0001_ses-01_T1w.nii.gz")
    rec_norm = _make_anat(tmp_path, "sub-0001", "ses-01", "sub-0001_ses-01_rec-norm_T1w.nii.gz")
    result = collect_session_t1w(tmp_path, "sub-0001", "ses-01")
    assert result == rec_norm


def test_collect_session_t1w_returns_first_when_multiple(tmp_path):
    """When multiple T1w images exist (no rec-norm), return first sorted."""
    a = _make_anat(tmp_path, "sub-0001", "ses-01", "sub-0001_ses-01_run-01_T1w.nii.gz")
    _make_anat(tmp_path, "sub-0001", "ses-01", "sub-0001_ses-01_run-02_T1w.nii.gz")
    result = collect_session_t1w(tmp_path, "sub-0001", "ses-01")
    assert result == a


def test_collect_session_t1w_ignores_other_sessions(tmp_path):
    """T1w images from ses-02 must not appear when querying ses-01."""
    _make_anat(tmp_path, "sub-0001", "ses-02", "sub-0001_ses-02_T1w.nii.gz")
    result = collect_session_t1w(tmp_path, "sub-0001", "ses-01")
    assert result is None


def test_collect_session_t1w_excludes_defaced_rec_norm(tmp_path):
    """rec-norm that is also defaced must be excluded."""
    _make_anat(
        tmp_path, "sub-0001", "ses-01",
        "sub-0001_ses-01_acq-defaced_rec-norm_T1w.nii.gz",
    )
    result = collect_session_t1w(tmp_path, "sub-0001", "ses-01")
    assert result is None


# ---------------------------------------------------------------------------
# collect_all_session_t1ws
# ---------------------------------------------------------------------------


def test_collect_all_session_t1ws_single_session(tmp_path):
    t1w = _make_anat(tmp_path, "sub-0001", "ses-01", "sub-0001_ses-01_T1w.nii.gz")
    result = collect_all_session_t1ws(tmp_path, "sub-0001")
    assert result == {"ses-01": t1w}


def test_collect_all_session_t1ws_multi_session(tmp_path):
    t1 = _make_anat(tmp_path, "sub-0001", "ses-01", "sub-0001_ses-01_T1w.nii.gz")
    t2 = _make_anat(tmp_path, "sub-0001", "ses-02", "sub-0001_ses-02_T1w.nii.gz")
    result = collect_all_session_t1ws(tmp_path, "sub-0001")
    assert result == {"ses-01": t1, "ses-02": t2}


def test_collect_all_session_t1ws_skips_sessions_without_t1w(tmp_path):
    """Sessions without a suitable T1w image are omitted from the result."""
    t1 = _make_anat(tmp_path, "sub-0001", "ses-01", "sub-0001_ses-01_T1w.nii.gz")
    # ses-02 has only a defaced T1w → should be skipped
    _make_anat(tmp_path, "sub-0001", "ses-02", "sub-0001_ses-02_acq-defaced_T1w.nii.gz")
    result = collect_all_session_t1ws(tmp_path, "sub-0001")
    assert result == {"ses-01": t1}


def test_collect_all_session_t1ws_empty_when_no_sessions(tmp_path):
    """Returns an empty dict when the subject directory has no sessions."""
    (tmp_path / "sub-0001").mkdir()
    result = collect_all_session_t1ws(tmp_path, "sub-0001")
    assert result == {}


def test_collect_all_session_t1ws_empty_when_subject_absent(tmp_path):
    result = collect_all_session_t1ws(tmp_path, "sub-9999")
    assert result == {}


def test_collect_all_session_t1ws_sorted_by_session(tmp_path):
    """Sessions are returned in sorted order."""
    _make_anat(tmp_path, "sub-0001", "ses-03", "sub-0001_ses-03_T1w.nii.gz")
    _make_anat(tmp_path, "sub-0001", "ses-01", "sub-0001_ses-01_T1w.nii.gz")
    _make_anat(tmp_path, "sub-0001", "ses-02", "sub-0001_ses-02_T1w.nii.gz")
    result = collect_all_session_t1ws(tmp_path, "sub-0001")
    assert list(result.keys()) == ["ses-01", "ses-02", "ses-03"]


# ---------------------------------------------------------------------------
# build_cross_apptainer_command
# ---------------------------------------------------------------------------

SIF = Path("/containers/fastsurfer.sif")
FS_LICENSE = Path("/misc/freesurfer/license.txt")
BIDS_DIR = Path("/data/bids")
OUTPUT_DIR = Path("/data/derivatives/fastsurfer")
T1W = Path("/data/bids/sub-0001/ses-01/anat/sub-0001_ses-01_T1w.nii.gz")


def test_cross_command_starts_with_apptainer_run():
    cmd = build_cross_apptainer_command(SIF, FS_LICENSE, BIDS_DIR, OUTPUT_DIR,
                                        "sub-0001", "ses-01", T1W, 8)
    assert cmd[:3] == ["apptainer", "run", "--cleanenv"]


def test_cross_command_binds_bids_readonly():
    cmd = build_cross_apptainer_command(SIF, FS_LICENSE, BIDS_DIR, OUTPUT_DIR,
                                        "sub-0001", "ses-01", T1W, 8)
    assert f"{BIDS_DIR}:/data:ro" in cmd


def test_cross_command_binds_output_readwrite():
    cmd = build_cross_apptainer_command(SIF, FS_LICENSE, BIDS_DIR, OUTPUT_DIR,
                                        "sub-0001", "ses-01", T1W, 8)
    assert f"{OUTPUT_DIR}:/output" in cmd


def test_cross_command_sid_is_subject_session():
    cmd = build_cross_apptainer_command(SIF, FS_LICENSE, BIDS_DIR, OUTPUT_DIR,
                                        "sub-0001", "ses-01", T1W, 8)
    idx = cmd.index("--sid")
    assert cmd[idx + 1] == "sub-0001_ses-01"


def test_cross_command_sd_is_container_output():
    cmd = build_cross_apptainer_command(SIF, FS_LICENSE, BIDS_DIR, OUTPUT_DIR,
                                        "sub-0001", "ses-01", T1W, 8)
    idx = cmd.index("--sd")
    assert cmd[idx + 1] == "/output"


def test_cross_command_t1_remapped_to_container_path():
    cmd = build_cross_apptainer_command(SIF, FS_LICENSE, BIDS_DIR, OUTPUT_DIR,
                                        "sub-0001", "ses-01", T1W, 8)
    idx = cmd.index("--t1")
    expected = "/data/sub-0001/ses-01/anat/sub-0001_ses-01_T1w.nii.gz"
    assert cmd[idx + 1] == expected


def test_cross_command_threads():
    cmd = build_cross_apptainer_command(SIF, FS_LICENSE, BIDS_DIR, OUTPUT_DIR,
                                        "sub-0001", "ses-01", T1W, 16)
    idx = cmd.index("--threads")
    assert cmd[idx + 1] == "16"


def test_cross_command_includes_3T_flag():
    cmd = build_cross_apptainer_command(SIF, FS_LICENSE, BIDS_DIR, OUTPUT_DIR,
                                        "sub-0001", "ses-01", T1W, 8)
    assert "--3T" in cmd


# ---------------------------------------------------------------------------
# build_long_fastsurfer_command
# ---------------------------------------------------------------------------

SESSIONS_T1WS = {
    "ses-01": Path("/data/bids/sub-0001/ses-01/anat/sub-0001_ses-01_T1w.nii.gz"),
    "ses-02": Path("/data/bids/sub-0001/ses-02/anat/sub-0001_ses-02_T1w.nii.gz"),
}


def test_long_fastsurfer_command_starts_with_apptainer_run():
    cmd = build_long_fastsurfer_command(
        SIF, FS_LICENSE, BIDS_DIR, OUTPUT_DIR, "sub-0001", SESSIONS_T1WS, 8
    )
    assert cmd[:3] == ["apptainer", "run", "--cleanenv"]


def test_long_fastsurfer_command_binds_bids_readonly():
    cmd = build_long_fastsurfer_command(
        SIF, FS_LICENSE, BIDS_DIR, OUTPUT_DIR, "sub-0001", SESSIONS_T1WS, 8
    )
    assert f"{BIDS_DIR}:/data:ro" in cmd


def test_long_fastsurfer_command_binds_output_readwrite():
    cmd = build_long_fastsurfer_command(
        SIF, FS_LICENSE, BIDS_DIR, OUTPUT_DIR, "sub-0001", SESSIONS_T1WS, 8
    )
    assert f"{OUTPUT_DIR}:/output" in cmd


def test_long_fastsurfer_command_invokes_long_fastsurfer_sh():
    cmd = build_long_fastsurfer_command(
        SIF, FS_LICENSE, BIDS_DIR, OUTPUT_DIR, "sub-0001", SESSIONS_T1WS, 8
    )
    assert "long_fastsurfer.sh" in cmd


def test_long_fastsurfer_command_tid_is_subject():
    cmd = build_long_fastsurfer_command(
        SIF, FS_LICENSE, BIDS_DIR, OUTPUT_DIR, "sub-0001", SESSIONS_T1WS, 8
    )
    idx = cmd.index("--tid")
    assert cmd[idx + 1] == "sub-0001"


def test_long_fastsurfer_command_sd_is_container_output():
    cmd = build_long_fastsurfer_command(
        SIF, FS_LICENSE, BIDS_DIR, OUTPUT_DIR, "sub-0001", SESSIONS_T1WS, 8
    )
    idx = cmd.index("--sd")
    assert cmd[idx + 1] == "/output"


def test_long_fastsurfer_command_includes_3T_flag():
    cmd = build_long_fastsurfer_command(
        SIF, FS_LICENSE, BIDS_DIR, OUTPUT_DIR, "sub-0001", SESSIONS_T1WS, 8
    )
    assert "--3T" in cmd


def test_long_fastsurfer_command_includes_parallel_surf():
    cmd = build_long_fastsurfer_command(
        SIF, FS_LICENSE, BIDS_DIR, OUTPUT_DIR, "sub-0001", SESSIONS_T1WS, 8
    )
    assert "--parallel_surf" in cmd


def test_long_fastsurfer_command_t1s_remapped_to_container():
    cmd = build_long_fastsurfer_command(
        SIF, FS_LICENSE, BIDS_DIR, OUTPUT_DIR, "sub-0001", SESSIONS_T1WS, 8
    )
    t1s_idx = cmd.index("--t1s")
    t1s = cmd[t1s_idx + 1 : t1s_idx + 3]
    assert t1s == [
        "/data/sub-0001/ses-01/anat/sub-0001_ses-01_T1w.nii.gz",
        "/data/sub-0001/ses-02/anat/sub-0001_ses-02_T1w.nii.gz",
    ]


def test_long_fastsurfer_command_tpids_are_fastsurfer_sids():
    cmd = build_long_fastsurfer_command(
        SIF, FS_LICENSE, BIDS_DIR, OUTPUT_DIR, "sub-0001", SESSIONS_T1WS, 8
    )
    tpids_idx = cmd.index("--tpids")
    tpids = cmd[tpids_idx + 1 : tpids_idx + 3]
    assert tpids == ["sub-0001_ses-01", "sub-0001_ses-02"]


def test_long_fastsurfer_command_threads():
    cmd = build_long_fastsurfer_command(
        SIF, FS_LICENSE, BIDS_DIR, OUTPUT_DIR, "sub-0001", SESSIONS_T1WS, 16
    )
    idx = cmd.index("--threads")
    assert cmd[idx + 1] == "16"

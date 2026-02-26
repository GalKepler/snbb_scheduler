"""Tests for snbb_scheduler.freesurfer — collect_images and command builders."""
from pathlib import Path

import pytest

from snbb_scheduler.freesurfer import (
    _remap,
    build_apptainer_command,
    build_native_command,
    collect_images,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_t1w(root: Path, subject: str, session: str, suffix: str = "") -> Path:
    anat = root / subject / session / "anat"
    anat.mkdir(parents=True, exist_ok=True)
    name = f"{subject}_{session}{('_' + suffix) if suffix else ''}_T1w.nii.gz"
    p = anat / name
    p.touch()
    return p


def _make_t2w(root: Path, subject: str, session: str, suffix: str = "") -> Path:
    anat = root / subject / session / "anat"
    anat.mkdir(parents=True, exist_ok=True)
    name = f"{subject}_{session}{('_' + suffix) if suffix else ''}_T2w.nii.gz"
    p = anat / name
    p.touch()
    return p


# ---------------------------------------------------------------------------
# collect_images — basic cases
# ---------------------------------------------------------------------------

def test_collect_images_no_sessions(tmp_path):
    t1w, t2w = collect_images(tmp_path, "sub-0001")
    assert t1w == []
    assert t2w == []


def test_collect_images_single_session(tmp_path):
    _make_t1w(tmp_path, "sub-0001", "ses-01")
    t1w, t2w = collect_images(tmp_path, "sub-0001")
    assert len(t1w) == 1
    assert t2w == []


def test_collect_images_two_sessions(tmp_path):
    _make_t1w(tmp_path, "sub-0001", "ses-01")
    _make_t1w(tmp_path, "sub-0001", "ses-02")
    t1w, _ = collect_images(tmp_path, "sub-0001")
    assert len(t1w) == 2


# ---------------------------------------------------------------------------
# collect_images — defaced exclusion
# ---------------------------------------------------------------------------

def test_collect_images_excludes_defaced_t1w(tmp_path):
    _make_t1w(tmp_path, "sub-0001", "ses-01")
    _make_t1w(tmp_path, "sub-0001", "ses-01", suffix="acq-defaced")
    t1w, _ = collect_images(tmp_path, "sub-0001")
    assert len(t1w) == 1
    assert all("defaced" not in f.name for f in t1w)


def test_collect_images_excludes_defaced_t2w(tmp_path):
    _make_t2w(tmp_path, "sub-0001", "ses-01")
    _make_t2w(tmp_path, "sub-0001", "ses-01", suffix="acq-defaced")
    _, t2w = collect_images(tmp_path, "sub-0001")
    assert len(t2w) == 1
    assert all("defaced" not in f.name for f in t2w)


# ---------------------------------------------------------------------------
# collect_images — rec-norm preference
# ---------------------------------------------------------------------------

def test_collect_images_prefers_rec_norm_t1w(tmp_path):
    """When rec-norm variants exist, only those are returned."""
    _make_t1w(tmp_path, "sub-0001", "ses-01")                       # plain T1w
    _make_t1w(tmp_path, "sub-0001", "ses-01", suffix="rec-norm")    # rec-norm T1w
    t1w, _ = collect_images(tmp_path, "sub-0001")
    assert len(t1w) == 1
    assert "rec-norm" in t1w[0].name


def test_collect_images_falls_back_when_no_rec_norm(tmp_path):
    """Without rec-norm variants, all non-defaced T1w files are returned."""
    _make_t1w(tmp_path, "sub-0001", "ses-01")
    _make_t1w(tmp_path, "sub-0001", "ses-02")
    t1w, _ = collect_images(tmp_path, "sub-0001")
    assert len(t1w) == 2


def test_collect_images_prefers_rec_norm_t2w(tmp_path):
    _make_t2w(tmp_path, "sub-0001", "ses-01")
    _make_t2w(tmp_path, "sub-0001", "ses-01", suffix="rec-norm")
    _, t2w = collect_images(tmp_path, "sub-0001")
    assert len(t2w) == 1
    assert "rec-norm" in t2w[0].name


def test_collect_images_rec_norm_and_defaced_combined(tmp_path):
    """Defaced files are always excluded, even if rec-norm is present."""
    _make_t1w(tmp_path, "sub-0001", "ses-01")
    _make_t1w(tmp_path, "sub-0001", "ses-01", suffix="rec-norm")
    _make_t1w(tmp_path, "sub-0001", "ses-01", suffix="acq-defaced")
    t1w, _ = collect_images(tmp_path, "sub-0001")
    assert len(t1w) == 1
    assert "rec-norm" in t1w[0].name
    assert "defaced" not in t1w[0].name


# ---------------------------------------------------------------------------
# _remap
# ---------------------------------------------------------------------------

def test_remap_replaces_prefix(tmp_path):
    host_root = tmp_path / "bids"
    path = host_root / "sub-0001" / "ses-01" / "anat" / "T1w.nii.gz"
    result = _remap(path, host_root, "/data")
    assert result == "/data/sub-0001/ses-01/anat/T1w.nii.gz"


# ---------------------------------------------------------------------------
# build_native_command
# ---------------------------------------------------------------------------

def test_build_native_command_basic(tmp_path):
    t1w = [tmp_path / "T1w_01.nii.gz"]
    cmd = build_native_command(
        subject="sub-0001",
        output_dir=tmp_path / "output",
        t1w_files=t1w,
        t2w_files=[],
        threads=4,
    )
    assert cmd[0] == "recon-all"
    assert "-i" in cmd
    assert str(t1w[0]) in cmd
    assert "-T2" not in cmd


def test_build_native_command_with_t2w(tmp_path):
    t1w = [tmp_path / "T1w.nii.gz"]
    t2w = [tmp_path / "T2w.nii.gz"]
    cmd = build_native_command("sub-0001", tmp_path, t1w, t2w, threads=8)
    assert "-T2" in cmd
    assert "-T2pial" in cmd


def test_build_native_command_multiple_t1w(tmp_path):
    t1w = [tmp_path / "T1w_01.nii.gz", tmp_path / "T1w_02.nii.gz"]
    cmd = build_native_command("sub-0001", tmp_path, t1w, [], threads=8)
    assert cmd.count("-i") == 2


# ---------------------------------------------------------------------------
# build_apptainer_command
# ---------------------------------------------------------------------------

def test_build_apptainer_command_contains_bind_mounts(tmp_path):
    bids_dir = tmp_path / "bids"
    output_dir = tmp_path / "output"
    sif = tmp_path / "freesurfer.sif"
    fs_license = tmp_path / "license.txt"
    t1w = [bids_dir / "sub-0001" / "ses-01" / "anat" / "T1w.nii.gz"]

    cmd = build_apptainer_command(
        sif=sif,
        fs_license=fs_license,
        bids_dir=bids_dir,
        output_dir=output_dir,
        subject="sub-0001",
        t1w_files=t1w,
        t2w_files=[],
        threads=8,
    )

    cmd_str = " ".join(cmd)
    assert "apptainer" in cmd_str
    assert "/data" in cmd_str
    assert "/output" in cmd_str
    assert "recon-all" in cmd_str


def test_build_apptainer_command_remaps_t1w_paths(tmp_path):
    bids_dir = tmp_path / "bids"
    t1w = [bids_dir / "sub-0001" / "ses-01" / "anat" / "T1w.nii.gz"]
    cmd = build_apptainer_command(
        sif=tmp_path / "fs.sif",
        fs_license=tmp_path / "license.txt",
        bids_dir=bids_dir,
        output_dir=tmp_path / "out",
        subject="sub-0001",
        t1w_files=t1w,
        t2w_files=[],
        threads=8,
    )
    # The T1w path should be remapped to /data/...
    assert "/data/sub-0001/ses-01/anat/T1w.nii.gz" in cmd

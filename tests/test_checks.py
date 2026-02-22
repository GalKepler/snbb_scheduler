from pathlib import Path

import pytest

from snbb_scheduler.checks import is_complete
from snbb_scheduler.config import Procedure


# ---------------------------------------------------------------------------
# Helpers to build minimal Procedure instances for each completion strategy
# ---------------------------------------------------------------------------

def proc_nonempty(name="test"):
    """completion_marker=None → directory must be non-empty."""
    return Procedure(name=name, output_dir=name, script=f"{name}.sh", completion_marker=None)


def proc_marker(name="test", marker="done.txt"):
    """completion_marker is a plain file path."""
    return Procedure(name=name, output_dir=name, script=f"{name}.sh", completion_marker=marker)


def proc_glob(name="test", pattern="**/*.nii.gz"):
    """completion_marker is a glob pattern."""
    return Procedure(name=name, output_dir=name, script=f"{name}.sh", completion_marker=pattern)


# ---------------------------------------------------------------------------
# Nonexistent path → always False
# ---------------------------------------------------------------------------

def test_nonexistent_path_nonempty_strategy(tmp_path):
    assert is_complete(proc_nonempty(), tmp_path / "missing") is False


def test_nonexistent_path_marker_strategy(tmp_path):
    assert is_complete(proc_marker(), tmp_path / "missing") is False


def test_nonexistent_path_glob_strategy(tmp_path):
    assert is_complete(proc_glob(), tmp_path / "missing") is False


# ---------------------------------------------------------------------------
# completion_marker=None — directory must be non-empty
# ---------------------------------------------------------------------------

def test_nonempty_strategy_empty_dir(tmp_path):
    d = tmp_path / "out"
    d.mkdir()
    assert is_complete(proc_nonempty(), d) is False


def test_nonempty_strategy_populated_dir(tmp_path):
    d = tmp_path / "out"
    d.mkdir()
    (d / "somefile.txt").touch()
    assert is_complete(proc_nonempty(), d) is True


def test_nonempty_strategy_subdir_counts(tmp_path):
    d = tmp_path / "out"
    (d / "subdir").mkdir(parents=True)
    assert is_complete(proc_nonempty(), d) is True


# ---------------------------------------------------------------------------
# completion_marker is a plain file path
# ---------------------------------------------------------------------------

def test_marker_strategy_file_present(tmp_path):
    d = tmp_path / "out"
    (d / "scripts").mkdir(parents=True)
    (d / "scripts" / "recon-all.done").touch()
    assert is_complete(proc_marker(marker="scripts/recon-all.done"), d) is True


def test_marker_strategy_file_absent(tmp_path):
    d = tmp_path / "out"
    d.mkdir()
    assert is_complete(proc_marker(marker="scripts/recon-all.done"), d) is False


def test_marker_strategy_wrong_file(tmp_path):
    d = tmp_path / "out"
    d.mkdir()
    (d / "other.done").touch()
    assert is_complete(proc_marker(marker="scripts/recon-all.done"), d) is False


# ---------------------------------------------------------------------------
# completion_marker is a glob pattern
# ---------------------------------------------------------------------------

def test_glob_strategy_match_present(tmp_path):
    d = tmp_path / "out"
    anat = d / "anat"
    anat.mkdir(parents=True)
    (anat / "T1w.nii.gz").touch()
    assert is_complete(proc_glob(pattern="**/*.nii.gz"), d) is True


def test_glob_strategy_no_match(tmp_path):
    d = tmp_path / "out"
    anat = d / "anat"
    anat.mkdir(parents=True)
    (anat / "T1w.nii").touch()  # no .gz
    assert is_complete(proc_glob(pattern="**/*.nii.gz"), d) is False


def test_glob_strategy_nested_match(tmp_path):
    d = tmp_path / "out"
    deep = d / "sub-0001" / "ses-01" / "dwi"
    deep.mkdir(parents=True)
    (deep / "dwi.nii.gz").touch()
    assert is_complete(proc_glob(pattern="**/*.nii.gz"), d) is True


def test_glob_strategy_flat_pattern(tmp_path):
    d = tmp_path / "out"
    d.mkdir()
    (d / "report.html").touch()
    assert is_complete(proc_glob(pattern="*.html"), d) is True


def test_glob_strategy_empty_dir(tmp_path):
    d = tmp_path / "out"
    d.mkdir()
    assert is_complete(proc_glob(pattern="**/*.nii.gz"), d) is False


# ---------------------------------------------------------------------------
# Realistic procedure configurations from DEFAULT_PROCEDURES
# ---------------------------------------------------------------------------

def test_bids_complete_with_nifti(tmp_path):
    """bids uses completion_marker='**/*.nii.gz'."""
    from snbb_scheduler.config import DEFAULT_PROCEDURES
    bids = next(p for p in DEFAULT_PROCEDURES if p.name == "bids")

    bids_session = tmp_path / "bids" / "sub-0001" / "ses-01"
    anat = bids_session / "anat"
    anat.mkdir(parents=True)
    (anat / "sub-0001_ses-01_T1w.nii.gz").touch()

    assert is_complete(bids, bids_session) is True


def test_bids_incomplete_no_nifti(tmp_path):
    from snbb_scheduler.config import DEFAULT_PROCEDURES
    bids = next(p for p in DEFAULT_PROCEDURES if p.name == "bids")

    bids_session = tmp_path / "bids" / "sub-0001" / "ses-01"
    bids_session.mkdir(parents=True)  # dir exists but empty

    assert is_complete(bids, bids_session) is False


def test_freesurfer_complete_with_marker(tmp_path):
    """freesurfer uses completion_marker='scripts/recon-all.done'."""
    from snbb_scheduler.config import DEFAULT_PROCEDURES
    fs = next(p for p in DEFAULT_PROCEDURES if p.name == "freesurfer")

    fs_subject = tmp_path / "freesurfer" / "sub-0001"
    (fs_subject / "scripts").mkdir(parents=True)
    (fs_subject / "scripts" / "recon-all.done").touch()

    assert is_complete(fs, fs_subject) is True


def test_freesurfer_incomplete_no_marker(tmp_path):
    from snbb_scheduler.config import DEFAULT_PROCEDURES
    fs = next(p for p in DEFAULT_PROCEDURES if p.name == "freesurfer")

    fs_subject = tmp_path / "freesurfer" / "sub-0001"
    fs_subject.mkdir(parents=True)  # dir exists but no marker

    assert is_complete(fs, fs_subject) is False


def test_qsiprep_complete_nonempty(tmp_path):
    """qsiprep uses completion_marker=None → non-empty directory."""
    from snbb_scheduler.config import DEFAULT_PROCEDURES
    qsiprep = next(p for p in DEFAULT_PROCEDURES if p.name == "qsiprep")

    qsiprep_session = tmp_path / "qsiprep" / "sub-0001" / "ses-01"
    qsiprep_session.mkdir(parents=True)
    (qsiprep_session / "dwi.nii.gz").touch()

    assert is_complete(qsiprep, qsiprep_session) is True


def test_qsiprep_incomplete_empty_dir(tmp_path):
    from snbb_scheduler.config import DEFAULT_PROCEDURES
    qsiprep = next(p for p in DEFAULT_PROCEDURES if p.name == "qsiprep")

    qsiprep_session = tmp_path / "qsiprep" / "sub-0001" / "ses-01"
    qsiprep_session.mkdir(parents=True)

    assert is_complete(qsiprep, qsiprep_session) is False

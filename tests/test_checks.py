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
# completion_marker is a list of glob patterns (all must match)
# ---------------------------------------------------------------------------


def proc_list(name="test", patterns=None):
    """completion_marker is a list of glob patterns."""
    if patterns is None:
        patterns = ["anat/*.nii.gz", "dwi/*.bvec"]
    return Procedure(name=name, output_dir=name, script=f"{name}.sh", completion_marker=patterns)


def test_list_marker_all_present(tmp_path):
    d = tmp_path / "out"
    (d / "anat").mkdir(parents=True)
    (d / "anat" / "T1w.nii.gz").touch()
    (d / "dwi").mkdir()
    (d / "dwi" / "dwi.bvec").touch()
    assert is_complete(proc_list(), d) is True


def test_list_marker_one_missing(tmp_path):
    d = tmp_path / "out"
    (d / "anat").mkdir(parents=True)
    (d / "anat" / "T1w.nii.gz").touch()
    # dwi directory not created — second pattern won't match
    assert is_complete(proc_list(), d) is False


def test_list_marker_empty_list(tmp_path):
    """An empty list is vacuously true (all() of nothing is True)."""
    d = tmp_path / "out"
    d.mkdir()
    assert is_complete(proc_list(patterns=[]), d) is True


def test_list_marker_nonexistent_path(tmp_path):
    assert is_complete(proc_list(), tmp_path / "missing") is False


# ---------------------------------------------------------------------------
# Realistic procedure configurations from DEFAULT_PROCEDURES
# ---------------------------------------------------------------------------

def _create_bids_session_files(bids_session_dir) -> None:
    """Create all 8 required BIDS modality files."""
    files = {
        "anat": ["sub_T1w.nii.gz"],
        "dwi": ["sub_dir-AP_dwi.nii.gz", "sub_dir-AP_dwi.bvec", "sub_dir-AP_dwi.bval"],
        "fmap": [
            "sub_acq-dwi_dir-AP_epi.nii.gz",
            "sub_acq-func_dir-AP_epi.nii.gz",
            "sub_acq-func_dir-PA_epi.nii.gz",
        ],
        "func": ["sub_task-rest_bold.nii.gz"],
    }
    for subdir, names in files.items():
        d = bids_session_dir / subdir
        d.mkdir(parents=True, exist_ok=True)
        for name in names:
            (d / name).touch()


def test_bids_complete_all_patterns_present(tmp_path):
    """bids is complete when all 8 modality patterns are satisfied."""
    from snbb_scheduler.config import DEFAULT_PROCEDURES
    bids = next(p for p in DEFAULT_PROCEDURES if p.name == "bids")

    bids_session = tmp_path / "bids" / "sub-0001" / "ses-01"
    _create_bids_session_files(bids_session)

    assert is_complete(bids, bids_session) is True


def test_bids_incomplete_missing_one_pattern(tmp_path):
    """bids is incomplete when any one of the 8 patterns is missing."""
    from snbb_scheduler.config import DEFAULT_PROCEDURES
    bids = next(p for p in DEFAULT_PROCEDURES if p.name == "bids")

    bids_session = tmp_path / "bids" / "sub-0001" / "ses-01"
    _create_bids_session_files(bids_session)
    # Remove the func file to make one pattern fail
    (bids_session / "func" / "sub_task-rest_bold.nii.gz").unlink()

    assert is_complete(bids, bids_session) is False


def test_bids_incomplete_no_files(tmp_path):
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

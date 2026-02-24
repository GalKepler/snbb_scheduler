from pathlib import Path

import pytest

from snbb_scheduler.checks import (
    _count_available_t1w,
    _count_bids_dwi_sessions,
    _count_recon_all_inputs,
    _count_subject_ses_dirs,
    is_complete,
)
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


def _write_recon_all_done(scripts_dir, subject, n_t1w):
    """Write a realistic recon-all.done file with *n_t1w* -i flags."""
    scripts_dir.mkdir(parents=True, exist_ok=True)
    i_flags = " ".join(f"-i /fake/T1w_{k}.nii.gz" for k in range(n_t1w))
    (scripts_dir / "recon-all.done").write_text(
        f"#CMDARGS -subject {subject} -all {i_flags}\n"
    )


# ---------------------------------------------------------------------------
# FreeSurfer — fallback (no kwargs)
# ---------------------------------------------------------------------------

def test_freesurfer_complete_with_marker_no_kwargs(tmp_path):
    """Without bids_root/subject kwargs the fallback is: done file exists → True."""
    from snbb_scheduler.config import DEFAULT_PROCEDURES
    fs = next(p for p in DEFAULT_PROCEDURES if p.name == "freesurfer")

    fs_subject = tmp_path / "freesurfer" / "sub-0001"
    _write_recon_all_done(fs_subject / "scripts", "sub-0001", n_t1w=1)

    assert is_complete(fs, fs_subject) is True


def test_freesurfer_incomplete_no_marker(tmp_path):
    from snbb_scheduler.config import DEFAULT_PROCEDURES
    fs = next(p for p in DEFAULT_PROCEDURES if p.name == "freesurfer")

    fs_subject = tmp_path / "freesurfer" / "sub-0001"
    fs_subject.mkdir(parents=True)  # dir exists but no done file

    assert is_complete(fs, fs_subject) is False


# ---------------------------------------------------------------------------
# FreeSurfer — T1w count matching (with kwargs)
# ---------------------------------------------------------------------------

def test_freesurfer_complete_when_t1w_count_matches(tmp_path):
    """T1w count in done file matches available → complete."""
    from snbb_scheduler.config import DEFAULT_PROCEDURES
    fs = next(p for p in DEFAULT_PROCEDURES if p.name == "freesurfer")

    subject = "sub-0001"
    fs_subject = tmp_path / "freesurfer" / subject
    _write_recon_all_done(fs_subject / "scripts", subject, n_t1w=1)

    bids_root = tmp_path / "bids"
    anat = bids_root / subject / "ses-01" / "anat"
    anat.mkdir(parents=True)
    (anat / f"{subject}_ses-01_T1w.nii.gz").touch()

    assert is_complete(fs, fs_subject, bids_root=bids_root, subject=subject) is True


def test_freesurfer_incomplete_when_new_t1w_added(tmp_path):
    """More T1w files available than used in done file → needs re-run."""
    from snbb_scheduler.config import DEFAULT_PROCEDURES
    fs = next(p for p in DEFAULT_PROCEDURES if p.name == "freesurfer")

    subject = "sub-0001"
    fs_subject = tmp_path / "freesurfer" / subject
    # Done file only used 1 T1w
    _write_recon_all_done(fs_subject / "scripts", subject, n_t1w=1)

    bids_root = tmp_path / "bids"
    for ses in ("ses-01", "ses-02"):  # 2 T1w now available
        anat = bids_root / subject / ses / "anat"
        anat.mkdir(parents=True)
        (anat / f"{subject}_{ses}_T1w.nii.gz").touch()

    assert is_complete(fs, fs_subject, bids_root=bids_root, subject=subject) is False


def test_freesurfer_incomplete_no_done_file_with_kwargs(tmp_path):
    """Done file missing → incomplete even with kwargs provided."""
    from snbb_scheduler.config import DEFAULT_PROCEDURES
    fs = next(p for p in DEFAULT_PROCEDURES if p.name == "freesurfer")

    subject = "sub-0001"
    fs_subject = tmp_path / "freesurfer" / subject
    fs_subject.mkdir(parents=True)

    assert is_complete(fs, fs_subject, bids_root=tmp_path / "bids", subject=subject) is False


# ---------------------------------------------------------------------------
# _count_recon_all_inputs helper
# ---------------------------------------------------------------------------

def test_count_recon_all_inputs_one_flag(tmp_path):
    done = tmp_path / "recon-all.done"
    done.write_text("#CMDARGS -subject sub-0001 -all -i /data/T1w.nii.gz\n")
    assert _count_recon_all_inputs(done) == 1


def test_count_recon_all_inputs_multiple_flags(tmp_path):
    done = tmp_path / "recon-all.done"
    done.write_text(
        "#CMDARGS -subject sub-0001 -all -i /data/ses-01/T1w.nii.gz -i /data/ses-02/T1w.nii.gz\n"
    )
    assert _count_recon_all_inputs(done) == 2


def test_count_recon_all_inputs_no_cmdargs_line(tmp_path):
    done = tmp_path / "recon-all.done"
    done.write_text("some other content\n")
    assert _count_recon_all_inputs(done) == 0


# ---------------------------------------------------------------------------
# _count_available_t1w helper
# ---------------------------------------------------------------------------

def test_count_available_t1w_two_sessions(tmp_path):
    subject = "sub-0001"
    for ses in ("ses-01", "ses-02"):
        anat = tmp_path / subject / ses / "anat"
        anat.mkdir(parents=True)
        (anat / f"{subject}_{ses}_T1w.nii.gz").touch()
    assert _count_available_t1w(tmp_path, subject) == 2


def test_count_available_t1w_subject_missing(tmp_path):
    assert _count_available_t1w(tmp_path, "sub-9999") == 0


# ---------------------------------------------------------------------------
# _count_subject_ses_dirs helper
# ---------------------------------------------------------------------------

def test_count_subject_ses_dirs_two_sessions(tmp_path):
    subject_dir = tmp_path / "sub-0001"
    (subject_dir / "ses-01").mkdir(parents=True)
    (subject_dir / "ses-02").mkdir()
    assert _count_subject_ses_dirs(subject_dir) == 2


def test_count_subject_ses_dirs_missing_dir(tmp_path):
    assert _count_subject_ses_dirs(tmp_path / "nonexistent") == 0


# ---------------------------------------------------------------------------
# _count_bids_dwi_sessions helper
# ---------------------------------------------------------------------------

def test_count_bids_dwi_sessions_two_sessions(tmp_path):
    subject = "sub-0001"
    for ses in ("ses-01", "ses-02"):
        dwi = tmp_path / subject / ses / "dwi"
        dwi.mkdir(parents=True)
        (dwi / f"{subject}_{ses}_dir-AP_dwi.nii.gz").touch()
    assert _count_bids_dwi_sessions(tmp_path, subject) == 2


def test_count_bids_dwi_sessions_subject_missing(tmp_path):
    assert _count_bids_dwi_sessions(tmp_path, "sub-9999") == 0


# ---------------------------------------------------------------------------
# QSIPrep specialized check
# ---------------------------------------------------------------------------

def test_qsiprep_complete_session_count_matches(tmp_path):
    """QSIPrep complete when ses-* dirs match BIDS DWI session count."""
    from snbb_scheduler.config import DEFAULT_PROCEDURES
    qsiprep = next(p for p in DEFAULT_PROCEDURES if p.name == "qsiprep")

    subject = "sub-0001"
    bids_root = tmp_path / "bids"
    dwi = bids_root / subject / "ses-01" / "dwi"
    dwi.mkdir(parents=True)
    (dwi / f"{subject}_ses-01_dir-AP_dwi.nii.gz").touch()

    qsiprep_subject = tmp_path / "derivatives" / "qsiprep" / subject
    (qsiprep_subject / "ses-01").mkdir(parents=True)
    (qsiprep_subject / "ses-01" / "dwi.nii.gz").touch()

    assert is_complete(qsiprep, qsiprep_subject, bids_root=bids_root, subject=subject) is True


def test_qsiprep_incomplete_missing_session(tmp_path):
    """QSIPrep incomplete when fewer ses-* dirs than BIDS DWI sessions."""
    from snbb_scheduler.config import DEFAULT_PROCEDURES
    qsiprep = next(p for p in DEFAULT_PROCEDURES if p.name == "qsiprep")

    subject = "sub-0001"
    bids_root = tmp_path / "bids"
    for ses in ("ses-01", "ses-02"):
        dwi = bids_root / subject / ses / "dwi"
        dwi.mkdir(parents=True)
        (dwi / f"{subject}_{ses}_dir-AP_dwi.nii.gz").touch()

    qsiprep_subject = tmp_path / "derivatives" / "qsiprep" / subject
    (qsiprep_subject / "ses-01").mkdir(parents=True)
    (qsiprep_subject / "ses-01" / "dwi.nii.gz").touch()
    # ses-02 not yet processed

    assert is_complete(qsiprep, qsiprep_subject, bids_root=bids_root, subject=subject) is False


def test_qsiprep_fallback_nonempty(tmp_path):
    """Without kwargs, qsiprep falls back to dir-nonempty check."""
    from snbb_scheduler.config import DEFAULT_PROCEDURES
    qsiprep = next(p for p in DEFAULT_PROCEDURES if p.name == "qsiprep")

    subject_dir = tmp_path / "qsiprep" / "sub-0001"
    (subject_dir / "ses-01").mkdir(parents=True)
    (subject_dir / "ses-01" / "dwi.nii.gz").touch()

    assert is_complete(qsiprep, subject_dir) is True


# ---------------------------------------------------------------------------
# QSIRecon specialized check
# ---------------------------------------------------------------------------

def test_qsirecon_complete_session_count_matches(tmp_path):
    """QSIRecon complete when ses-* dirs match QSIPrep session count."""
    from snbb_scheduler.config import DEFAULT_PROCEDURES
    qsirecon = next(p for p in DEFAULT_PROCEDURES if p.name == "qsirecon")

    subject = "sub-0001"
    derivatives_root = tmp_path / "derivatives"
    qsiprep_subject = derivatives_root / "qsiprep" / subject
    (qsiprep_subject / "ses-01").mkdir(parents=True)

    qsirecon_subject = derivatives_root / "qsirecon-MRtrix3_act-HSVS" / subject
    (qsirecon_subject / "ses-01").mkdir(parents=True)
    (qsirecon_subject / "ses-01" / "report.html").touch()

    assert is_complete(qsirecon, qsirecon_subject, derivatives_root=derivatives_root, subject=subject) is True


def test_qsirecon_incomplete_missing_session(tmp_path):
    """QSIRecon incomplete when fewer ses-* dirs than QSIPrep sessions."""
    from snbb_scheduler.config import DEFAULT_PROCEDURES
    qsirecon = next(p for p in DEFAULT_PROCEDURES if p.name == "qsirecon")

    subject = "sub-0001"
    derivatives_root = tmp_path / "derivatives"
    for ses in ("ses-01", "ses-02"):
        (derivatives_root / "qsiprep" / subject / ses).mkdir(parents=True)

    qsirecon_subject = derivatives_root / "qsirecon-MRtrix3_act-HSVS" / subject
    (qsirecon_subject / "ses-01").mkdir(parents=True)
    (qsirecon_subject / "ses-01" / "report.html").touch()

    assert is_complete(qsirecon, qsirecon_subject, derivatives_root=derivatives_root, subject=subject) is False


def test_qsirecon_fallback_nonempty(tmp_path):
    """Without kwargs, qsirecon falls back to dir-nonempty check."""
    from snbb_scheduler.config import DEFAULT_PROCEDURES
    qsirecon = next(p for p in DEFAULT_PROCEDURES if p.name == "qsirecon")

    subject_dir = tmp_path / "qsirecon-MRtrix3_act-HSVS" / "sub-0001"
    (subject_dir / "ses-01").mkdir(parents=True)
    (subject_dir / "ses-01" / "report.html").touch()

    assert is_complete(qsirecon, subject_dir) is True


# ---------------------------------------------------------------------------
# Legacy qsiprep/qsirecon nonempty tests (kept for backward compat coverage)
# ---------------------------------------------------------------------------

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

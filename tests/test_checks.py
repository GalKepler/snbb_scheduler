from snbb_scheduler.checks import (
    FileCheckResult,
    _count_available_t1w,
    _count_bids_dwi_sessions,
    _count_recon_all_inputs,
    _count_subject_ses_dirs,
    check_detailed,
    is_complete,
)
from snbb_scheduler.config import Procedure


# ---------------------------------------------------------------------------
# Helpers to build minimal Procedure instances for each completion strategy
# ---------------------------------------------------------------------------


def proc_nonempty(name="test"):
    """completion_marker=None → directory must be non-empty."""
    return Procedure(
        name=name, output_dir=name, script=f"{name}.sh", completion_marker=None
    )


def proc_marker(name="test", marker="done.txt"):
    """completion_marker is a plain file path."""
    return Procedure(
        name=name, output_dir=name, script=f"{name}.sh", completion_marker=marker
    )


def proc_glob(name="test", pattern="**/*.nii.gz"):
    """completion_marker is a glob pattern."""
    return Procedure(
        name=name, output_dir=name, script=f"{name}.sh", completion_marker=pattern
    )


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
    return Procedure(
        name=name, output_dir=name, script=f"{name}.sh", completion_marker=patterns
    )


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
    """Create all required BIDS modality files matching the bids completion_marker."""
    files = {
        "anat": ["sub_T1w.nii.gz"],
        "dwi": [
            "sub_dir-AP_dwi.nii.gz",
            "sub_dir-AP_dwi.bvec",
            "sub_dir-AP_dwi.bval",
            # Short reverse-PE DWI lives in dwi/ (bids_post derives the fmap from it)
            "sub_dir-PA_dwi.nii.gz",
        ],
        "fmap": [
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


def _write_recon_all_done(scripts_dir, subject, n_t1w, success=True):
    """Write a realistic recon-all.done file with *n_t1w* -i flags.

    When *success* is True (default), writes the multi-line metadata format
    that FreeSurfer produces on a successful run.  When False, writes just
    the exit code ``1`` (matching the real failure format).
    """
    scripts_dir.mkdir(parents=True, exist_ok=True)
    if not success:
        (scripts_dir / "recon-all.done").write_text("1\n")
        return
    i_flags = " ".join(f"-i /fake/T1w_{k}.nii.gz" for k in range(n_t1w))
    (scripts_dir / "recon-all.done").write_text(
        f"------------------------------\n"
        f"SUBJECT {subject}\n"
        f"CMDARGS -subject {subject} -all {i_flags}\n"
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
# FreeSurfer — longitudinal completion check (with kwargs)
# ---------------------------------------------------------------------------


def _get_freesurfer_proc():
    """Retrieve the freesurfer Procedure from DEFAULT_PROCEDURES."""
    from snbb_scheduler.config import DEFAULT_PROCEDURES
    return next(p for p in DEFAULT_PROCEDURES if p.name == "freesurfer")


def _make_bids_t1w(bids_root, subject, session):
    """Create a minimal BIDS T1w NIfTI so _count_bids_anat_sessions finds the session."""
    anat = bids_root / subject / session / "anat"
    anat.mkdir(parents=True, exist_ok=True)
    (anat / f"{subject}_{session}_T1w.nii.gz").touch()


def _touch_done(subjects_dir, subject_id):
    """Create scripts/recon-all.done for a FreeSurfer subject ID."""
    scripts = subjects_dir / subject_id / "scripts"
    scripts.mkdir(parents=True, exist_ok=True)
    (scripts / "recon-all.done").write_text(
        f"------------------------------\nSUBJECT {subject_id}\n"
    )


# ── single-session (cross-sectional only) ─────────────────────────────────────


def test_freesurfer_single_session_complete(tmp_path):
    """Single-session: check <output_path>/scripts/recon-all.done."""
    proc = _get_freesurfer_proc()
    subject, session = "sub-0001", "ses-01"
    bids_root = tmp_path / "bids"
    subjects_dir = tmp_path / "derivatives" / "freesurfer"

    _make_bids_t1w(bids_root, subject, session)
    # output at <subject>/ (cross-sectional naming for single session)
    output_path = subjects_dir / subject
    _touch_done(subjects_dir, subject)

    assert is_complete(proc, output_path, bids_root=bids_root, subject=subject) is True


def test_freesurfer_single_session_incomplete_no_done(tmp_path):
    """Single-session: returns False when recon-all.done is absent."""
    proc = _get_freesurfer_proc()
    subject, session = "sub-0001", "ses-01"
    bids_root = tmp_path / "bids"
    subjects_dir = tmp_path / "derivatives" / "freesurfer"

    _make_bids_t1w(bids_root, subject, session)
    output_path = subjects_dir / subject
    output_path.mkdir(parents=True)  # dir exists but no done file

    assert is_complete(proc, output_path, bids_root=bids_root, subject=subject) is False


def test_freesurfer_single_session_incomplete_dir_absent(tmp_path):
    """Single-session: returns False when the subject directory does not exist."""
    proc = _get_freesurfer_proc()
    subject, session = "sub-0001", "ses-01"
    bids_root = tmp_path / "bids"

    _make_bids_t1w(bids_root, subject, session)
    output_path = tmp_path / "derivatives" / "freesurfer" / subject

    assert is_complete(proc, output_path, bids_root=bids_root, subject=subject) is False


# ── multi-session (longitudinal: cross + template + long) ─────────────────────


def _setup_multi_session_complete(tmp_path, subject, sessions):
    """Create all done files needed for a complete longitudinal FreeSurfer run."""
    bids_root = tmp_path / "bids"
    subjects_dir = tmp_path / "derivatives" / "freesurfer"

    for ses in sessions:
        _make_bids_t1w(bids_root, subject, ses)
        # Step 1: cross-sectional
        _touch_done(subjects_dir, f"{subject}_{ses}")

    # Step 2: template
    _touch_done(subjects_dir, subject)

    # Step 3: longitudinal
    for ses in sessions:
        _touch_done(subjects_dir, f"{subject}_{ses}.long.{subject}")

    return bids_root, subjects_dir / subject


def test_freesurfer_multi_session_complete(tmp_path):
    """Multi-session: complete when cross, template, and longitudinal done files all exist."""
    proc = _get_freesurfer_proc()
    subject = "sub-0001"
    sessions = ["ses-01", "ses-02"]

    bids_root, output_path = _setup_multi_session_complete(tmp_path, subject, sessions)

    assert is_complete(proc, output_path, bids_root=bids_root, subject=subject) is True


def test_freesurfer_multi_session_incomplete_cross_missing(tmp_path):
    """Multi-session: returns False when a cross-sectional done file is absent."""
    proc = _get_freesurfer_proc()
    subject = "sub-0001"
    sessions = ["ses-01", "ses-02"]

    bids_root, output_path = _setup_multi_session_complete(tmp_path, subject, sessions)

    # Remove one cross-sectional done file
    subjects_dir = output_path.parent
    (subjects_dir / f"{subject}_ses-02" / "scripts" / "recon-all.done").unlink()

    assert is_complete(proc, output_path, bids_root=bids_root, subject=subject) is False


def test_freesurfer_multi_session_incomplete_template_missing(tmp_path):
    """Multi-session: returns False when the template done file is absent."""
    proc = _get_freesurfer_proc()
    subject = "sub-0001"
    sessions = ["ses-01", "ses-02"]

    bids_root, output_path = _setup_multi_session_complete(tmp_path, subject, sessions)

    # Remove the template done file
    (output_path / "scripts" / "recon-all.done").unlink()

    assert is_complete(proc, output_path, bids_root=bids_root, subject=subject) is False


def test_freesurfer_multi_session_incomplete_long_missing(tmp_path):
    """Multi-session: returns False when a longitudinal done file is absent."""
    proc = _get_freesurfer_proc()
    subject = "sub-0001"
    sessions = ["ses-01", "ses-02"]

    bids_root, output_path = _setup_multi_session_complete(tmp_path, subject, sessions)

    # Remove one longitudinal done file
    subjects_dir = output_path.parent
    (subjects_dir / f"{subject}_ses-01.long.{subject}" / "scripts" / "recon-all.done").unlink()

    assert is_complete(proc, output_path, bids_root=bids_root, subject=subject) is False


def test_freesurfer_multi_session_three_sessions_complete(tmp_path):
    """Multi-session: works correctly with three sessions."""
    proc = _get_freesurfer_proc()
    subject = "sub-0001"
    sessions = ["ses-01", "ses-02", "ses-03"]

    bids_root, output_path = _setup_multi_session_complete(tmp_path, subject, sessions)

    assert is_complete(proc, output_path, bids_root=bids_root, subject=subject) is True


# ── no BIDS sessions / fallback ───────────────────────────────────────────────


def test_freesurfer_no_bids_sessions_returns_false(tmp_path):
    """Returns False when no T1w sessions are found in BIDS."""
    proc = _get_freesurfer_proc()
    subject = "sub-0001"
    bids_root = tmp_path / "bids"

    output_path = tmp_path / "derivatives" / "freesurfer" / subject
    assert is_complete(proc, output_path, bids_root=bids_root, subject=subject) is False


def test_freesurfer_incomplete_no_done_file_with_kwargs(tmp_path):
    """Done file missing → incomplete even with kwargs provided (single session)."""
    proc = _get_freesurfer_proc()
    subject, session = "sub-0001", "ses-01"
    bids_root = tmp_path / "bids"

    _make_bids_t1w(bids_root, subject, session)
    output_path = tmp_path / "derivatives" / "freesurfer" / subject
    output_path.mkdir(parents=True)  # dir exists but no done file

    assert is_complete(proc, output_path, bids_root=bids_root, subject=subject) is False


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


def test_count_available_t1w_excludes_defaced(tmp_path):
    subject = "sub-0001"
    anat = tmp_path / subject / "ses-01" / "anat"
    anat.mkdir(parents=True)
    (anat / f"{subject}_ses-01_T1w.nii.gz").touch()
    (anat / f"{subject}_ses-01_acq-defaced_T1w.nii.gz").touch()
    assert _count_available_t1w(tmp_path, subject) == 1


def test_count_available_t1w_prefers_rec_norm(tmp_path):
    """When rec-norm variants exist, only those count."""
    subject = "sub-0001"
    anat = tmp_path / subject / "ses-01" / "anat"
    anat.mkdir(parents=True)
    (anat / f"{subject}_ses-01_T1w.nii.gz").touch()
    (anat / f"{subject}_ses-01_rec-norm_T1w.nii.gz").touch()
    assert _count_available_t1w(tmp_path, subject) == 1


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
# QSIPrep completion check (session-scoped, list-marker)
# ---------------------------------------------------------------------------


def _create_qsiprep_session_files(session_dir, subject, session):
    """Create all expected QSIPrep session-level output files."""
    session_dir.mkdir(parents=True, exist_ok=True)
    (session_dir / f"{subject}_{session}.html").touch()
    dwi = session_dir / "dwi"
    dwi.mkdir(exist_ok=True)
    stem = f"{subject}_{session}_dwi_preproc"
    (dwi / f"{stem}.nii.gz").touch()
    (dwi / f"{stem}.bvec").touch()
    (dwi / f"{stem}.bval").touch()
    (dwi / f"{subject}_{session}_desc-image_qc.tsv").touch()


def test_qsiprep_complete_with_html_and_dwi(tmp_path):
    """QSIPrep complete when HTML + all DWI preproc files present at session level."""
    from snbb_scheduler.config import DEFAULT_PROCEDURES

    qsiprep = next(p for p in DEFAULT_PROCEDURES if p.name == "qsiprep")

    subject, session = "sub-0001", "ses-01"
    session_dir = tmp_path / "derivatives" / "qsiprep" / subject / session
    _create_qsiprep_session_files(session_dir, subject, session)

    assert is_complete(qsiprep, session_dir) is True


def test_qsiprep_incomplete_missing_html(tmp_path):
    """QSIPrep incomplete when HTML report is absent even if DWI files exist."""
    from snbb_scheduler.config import DEFAULT_PROCEDURES

    qsiprep = next(p for p in DEFAULT_PROCEDURES if p.name == "qsiprep")

    subject, session = "sub-0001", "ses-01"
    session_dir = tmp_path / "derivatives" / "qsiprep" / subject / session
    _create_qsiprep_session_files(session_dir, subject, session)
    (session_dir / f"{subject}_{session}.html").unlink()

    assert is_complete(qsiprep, session_dir) is False


def test_qsiprep_incomplete_missing_dwi_file(tmp_path):
    """QSIPrep incomplete when one DWI preproc file is absent."""
    from snbb_scheduler.config import DEFAULT_PROCEDURES

    qsiprep = next(p for p in DEFAULT_PROCEDURES if p.name == "qsiprep")

    subject, session = "sub-0001", "ses-01"
    session_dir = tmp_path / "derivatives" / "qsiprep" / subject / session
    _create_qsiprep_session_files(session_dir, subject, session)
    (session_dir / "dwi" / f"{subject}_{session}_dwi_preproc.bval").unlink()

    assert is_complete(qsiprep, session_dir) is False


# ---------------------------------------------------------------------------
# QSIRecon specialized check
# ---------------------------------------------------------------------------


def test_qsirecon_complete_wildcard_fallback(tmp_path):
    """QSIRecon complete via wildcard when no recon_spec given but HTML exists."""
    from snbb_scheduler.config import DEFAULT_PROCEDURES

    qsirecon = next(p for p in DEFAULT_PROCEDURES if p.name == "qsirecon")

    subject, session = "sub-0001", "ses-01"
    derivatives_root = tmp_path / "derivatives"

    pipeline_dir = derivatives_root / "qsirecon" / "derivatives" / "qsirecon-MRtrix3_act-HSVS"
    pipeline_dir.mkdir(parents=True)
    (pipeline_dir / f"{subject}_{session}.html").touch()

    qsirecon_subject = derivatives_root / "qsirecon" / subject
    assert (
        is_complete(
            qsirecon,
            qsirecon_subject,
            derivatives_root=derivatives_root,
            subject=subject,
            session=session,
        )
        is True
    )


def test_qsirecon_incomplete_missing_session_html(tmp_path):
    """QSIRecon incomplete when HTML report is absent for the requested session."""
    from snbb_scheduler.config import DEFAULT_PROCEDURES

    qsirecon = next(p for p in DEFAULT_PROCEDURES if p.name == "qsirecon")

    subject, session = "sub-0001", "ses-02"
    derivatives_root = tmp_path / "derivatives"

    # Only ses-01 HTML created; ses-02 missing
    pipeline_dir = derivatives_root / "qsirecon" / "derivatives" / "qsirecon-MRtrix3_act-HSVS"
    pipeline_dir.mkdir(parents=True)
    (pipeline_dir / f"{subject}_ses-01.html").touch()

    qsirecon_subject = derivatives_root / "qsirecon" / subject
    assert (
        is_complete(
            qsirecon,
            qsirecon_subject,
            derivatives_root=derivatives_root,
            subject=subject,
            session=session,
        )
        is False
    )


def test_qsirecon_fallback_nonempty(tmp_path):
    """Without kwargs, qsirecon falls back to dir-nonempty check."""
    from snbb_scheduler.config import DEFAULT_PROCEDURES

    qsirecon = next(p for p in DEFAULT_PROCEDURES if p.name == "qsirecon")

    subject_dir = tmp_path / "qsirecon" / "sub-0001"
    subject_dir.mkdir(parents=True)
    (subject_dir / "something.txt").touch()

    assert is_complete(qsirecon, subject_dir) is True


# ---------------------------------------------------------------------------
# Defacing completion check — glob-based (acq-defaced T1w)
# ---------------------------------------------------------------------------


def test_defacing_complete_when_acq_defaced_present(tmp_path):
    """Defacing complete when anat/*acq-defaced*_T1w.nii.gz exists."""
    from snbb_scheduler.config import DEFAULT_PROCEDURES

    defacing = next(p for p in DEFAULT_PROCEDURES if p.name == "defacing")

    bids_session = tmp_path / "bids" / "sub-0001" / "ses-01"
    anat = bids_session / "anat"
    anat.mkdir(parents=True)
    (anat / "sub-0001_ses-01_acq-defaced_T1w.nii.gz").touch()

    assert is_complete(defacing, bids_session) is True


def test_defacing_incomplete_when_no_acq_defaced(tmp_path):
    """Defacing incomplete when only the original (non-defaced) T1w exists."""
    from snbb_scheduler.config import DEFAULT_PROCEDURES

    defacing = next(p for p in DEFAULT_PROCEDURES if p.name == "defacing")

    bids_session = tmp_path / "bids" / "sub-0001" / "ses-01"
    anat = bids_session / "anat"
    anat.mkdir(parents=True)
    (anat / "sub-0001_ses-01_T1w.nii.gz").touch()  # no acq-defaced entity

    assert is_complete(defacing, bids_session) is False


def test_defacing_incomplete_when_no_anat_dir(tmp_path):
    """Defacing incomplete when the anat directory does not exist."""
    from snbb_scheduler.config import DEFAULT_PROCEDURES

    defacing = next(p for p in DEFAULT_PROCEDURES if p.name == "defacing")

    bids_session = tmp_path / "bids" / "sub-0001" / "ses-01"
    bids_session.mkdir(parents=True)  # session dir exists but no anat subdir

    assert is_complete(defacing, bids_session) is False


def test_defacing_incomplete_when_session_dir_missing(tmp_path):
    """Defacing incomplete when the session directory itself does not exist."""
    from snbb_scheduler.config import DEFAULT_PROCEDURES

    defacing = next(p for p in DEFAULT_PROCEDURES if p.name == "defacing")

    assert is_complete(defacing, tmp_path / "bids" / "sub-0001" / "ses-01") is False




# ---------------------------------------------------------------------------
# QSIRecon with recon_spec (per-suffix HTML verification)
# ---------------------------------------------------------------------------


def test_qsirecon_complete_with_recon_spec(tmp_path):
    """QSIRecon complete when HTML exists for every suffix in the spec."""
    import yaml as _yaml
    from snbb_scheduler.config import DEFAULT_PROCEDURES

    qsirecon = next(p for p in DEFAULT_PROCEDURES if p.name == "qsirecon")

    subject, session = "sub-0001", "ses-01"
    derivatives_root = tmp_path / "derivatives"
    suffixes = ["DIPYDKI", "MRtrix3_act-HSVS"]

    spec = tmp_path / "spec.yaml"
    spec.write_text(_yaml.dump({"nodes": [{"qsirecon_suffix": s} for s in suffixes]}))

    qsirecon_root = derivatives_root / "qsirecon"
    for s in suffixes:
        d = qsirecon_root / "derivatives" / f"qsirecon-{s}"
        d.mkdir(parents=True)
        (d / f"{subject}_{session}.html").touch()

    qsirecon_subject = qsirecon_root / subject
    assert (
        is_complete(
            qsirecon,
            qsirecon_subject,
            derivatives_root=derivatives_root,
            subject=subject,
            session=session,
            recon_spec=spec,
        )
        is True
    )


def test_qsirecon_incomplete_missing_one_suffix_html(tmp_path):
    """QSIRecon incomplete when one suffix HTML is absent."""
    import yaml as _yaml
    from snbb_scheduler.config import DEFAULT_PROCEDURES

    qsirecon = next(p for p in DEFAULT_PROCEDURES if p.name == "qsirecon")

    subject, session = "sub-0001", "ses-01"
    derivatives_root = tmp_path / "derivatives"
    suffixes = ["DIPYDKI", "MRtrix3_act-HSVS"]

    spec = tmp_path / "spec.yaml"
    spec.write_text(_yaml.dump({"nodes": [{"qsirecon_suffix": s} for s in suffixes]}))

    # Only create HTML for the first suffix; second is missing
    qsirecon_root = derivatives_root / "qsirecon"
    d = qsirecon_root / "derivatives" / f"qsirecon-{suffixes[0]}"
    d.mkdir(parents=True)
    (d / f"{subject}_{session}.html").touch()

    qsirecon_subject = qsirecon_root / subject
    assert (
        is_complete(
            qsirecon,
            qsirecon_subject,
            derivatives_root=derivatives_root,
            subject=subject,
            session=session,
            recon_spec=spec,
        )
        is False
    )


def test_qsirecon_recon_spec_empty_falls_back_to_wildcard(tmp_path):
    """When spec has no qsirecon_suffix nodes, falls back to wildcard HTML check."""
    import yaml as _yaml
    from snbb_scheduler.config import DEFAULT_PROCEDURES

    qsirecon = next(p for p in DEFAULT_PROCEDURES if p.name == "qsirecon")

    subject, session = "sub-0001", "ses-01"
    derivatives_root = tmp_path / "derivatives"

    spec = tmp_path / "spec.yaml"
    spec.write_text(_yaml.dump({"nodes": [{"action": "some_action"}]}))  # no suffixes

    qsirecon_root = derivatives_root / "qsirecon"
    d = qsirecon_root / "derivatives" / "qsirecon-SomePipeline"
    d.mkdir(parents=True)
    (d / f"{subject}_{session}.html").touch()

    qsirecon_subject = qsirecon_root / subject
    assert (
        is_complete(
            qsirecon,
            qsirecon_subject,
            derivatives_root=derivatives_root,
            subject=subject,
            session=session,
            recon_spec=spec,
        )
        is True
    )


# ---------------------------------------------------------------------------
# Legacy qsiprep session-level marker tests
# ---------------------------------------------------------------------------


def test_qsiprep_complete_session_files(tmp_path):
    """QSIPrep complete when HTML + all DWI preproc outputs are present."""
    from snbb_scheduler.config import DEFAULT_PROCEDURES

    qsiprep = next(p for p in DEFAULT_PROCEDURES if p.name == "qsiprep")

    subject, session = "sub-0001", "ses-01"
    qsiprep_session = tmp_path / "qsiprep" / subject / session
    _create_qsiprep_session_files(qsiprep_session, subject, session)

    assert is_complete(qsiprep, qsiprep_session) is True


def test_qsiprep_incomplete_empty_dir(tmp_path):
    from snbb_scheduler.config import DEFAULT_PROCEDURES

    qsiprep = next(p for p in DEFAULT_PROCEDURES if p.name == "qsiprep")

    qsiprep_session = tmp_path / "qsiprep" / "sub-0001" / "ses-01"
    qsiprep_session.mkdir(parents=True)

    assert is_complete(qsiprep, qsiprep_session) is False


# (FastSurfer checks removed — procedure replaced by FreeSurfer longitudinal pipeline)


# ---------------------------------------------------------------------------
# FileCheckResult dataclass
# ---------------------------------------------------------------------------


def test_file_check_result_fields():
    fc = FileCheckResult(pattern="anat/*.nii.gz", found=True, matched_files=["/a/b.nii.gz"])
    assert fc.pattern == "anat/*.nii.gz"
    assert fc.found is True
    assert fc.matched_files == ["/a/b.nii.gz"]


def test_file_check_result_default_matched_files():
    fc = FileCheckResult(pattern="done.txt", found=False)
    assert fc.matched_files == []


# ---------------------------------------------------------------------------
# check_detailed — nonexistent path
# ---------------------------------------------------------------------------


def test_check_detailed_nonexistent_none_marker(tmp_path):
    results = check_detailed(proc_nonempty(), tmp_path / "missing")
    assert len(results) == 1
    assert not results[0].found
    assert results[0].pattern == "<directory>"


def test_check_detailed_nonexistent_single_marker(tmp_path):
    results = check_detailed(proc_marker(), tmp_path / "missing")
    assert len(results) == 1
    assert not results[0].found
    assert results[0].pattern == "done.txt"


def test_check_detailed_nonexistent_glob(tmp_path):
    results = check_detailed(proc_glob(), tmp_path / "missing")
    assert len(results) == 1
    assert not results[0].found


def test_check_detailed_nonexistent_list_marker(tmp_path):
    proc = Procedure(
        name="test", output_dir="test", script="t.sh",
        completion_marker=["anat/*.nii.gz", "dwi/*.nii.gz"]
    )
    results = check_detailed(proc, tmp_path / "missing")
    assert len(results) == 2
    assert all(not r.found for r in results)


# ---------------------------------------------------------------------------
# check_detailed — none marker (directory non-empty)
# ---------------------------------------------------------------------------


def test_check_detailed_none_marker_empty_dir(tmp_path):
    d = tmp_path / "out"
    d.mkdir()
    results = check_detailed(proc_nonempty(), d)
    assert len(results) == 1
    assert not results[0].found


def test_check_detailed_none_marker_populated_dir(tmp_path):
    d = tmp_path / "out"
    d.mkdir()
    (d / "file.txt").touch()
    results = check_detailed(proc_nonempty(), d)
    assert len(results) == 1
    assert results[0].found


# ---------------------------------------------------------------------------
# check_detailed — single file marker
# ---------------------------------------------------------------------------


def test_check_detailed_single_marker_missing(tmp_path):
    d = tmp_path / "out"
    d.mkdir()
    results = check_detailed(proc_marker(), d)
    assert not results[0].found
    assert results[0].matched_files == []


def test_check_detailed_single_marker_present(tmp_path):
    d = tmp_path / "out"
    d.mkdir()
    (d / "done.txt").touch()
    results = check_detailed(proc_marker(), d)
    assert results[0].found
    assert len(results[0].matched_files) == 1


# ---------------------------------------------------------------------------
# check_detailed — glob marker
# ---------------------------------------------------------------------------


def test_check_detailed_glob_no_matches(tmp_path):
    d = tmp_path / "out"
    d.mkdir()
    results = check_detailed(proc_glob(), d)
    assert not results[0].found
    assert results[0].matched_files == []


def test_check_detailed_glob_with_matches(tmp_path):
    d = tmp_path / "out"
    (d / "anat").mkdir(parents=True)
    (d / "anat" / "T1w.nii.gz").touch()
    results = check_detailed(proc_glob(), d)
    assert results[0].found
    assert len(results[0].matched_files) >= 1


# ---------------------------------------------------------------------------
# check_detailed — list marker
# ---------------------------------------------------------------------------


def test_check_detailed_list_marker_all_found(tmp_path):
    proc = Procedure(
        name="test", output_dir="test", script="t.sh",
        completion_marker=["anat/*.nii.gz", "dwi/*.bvec"]
    )
    d = tmp_path / "out"
    (d / "anat").mkdir(parents=True)
    (d / "anat" / "T1w.nii.gz").touch()
    (d / "dwi").mkdir()
    (d / "dwi" / "run.bvec").touch()
    results = check_detailed(proc, d)
    assert len(results) == 2
    assert all(r.found for r in results)


def test_check_detailed_list_marker_partial(tmp_path):
    proc = Procedure(
        name="test", output_dir="test", script="t.sh",
        completion_marker=["anat/*.nii.gz", "dwi/*.bvec"]
    )
    d = tmp_path / "out"
    (d / "anat").mkdir(parents=True)
    (d / "anat" / "T1w.nii.gz").touch()
    # dwi missing
    results = check_detailed(proc, d)
    assert results[0].found
    assert not results[1].found


def test_check_detailed_list_returns_one_entry_per_pattern(tmp_path):
    proc = Procedure(
        name="test", output_dir="test", script="t.sh",
        completion_marker=["a/*.txt", "b/*.txt", "c/*.txt"]
    )
    d = tmp_path / "out"
    d.mkdir()
    results = check_detailed(proc, d)
    assert len(results) == 3


# ---------------------------------------------------------------------------
# check_detailed — specialized checks (freesurfer, qsirecon)
# ---------------------------------------------------------------------------


def test_check_detailed_specialized_freesurfer_returns_single_entry(tmp_path):
    proc = Procedure(
        name="freesurfer", output_dir="freesurfer", script="s.sh",
        completion_marker=None
    )
    results = check_detailed(proc, tmp_path / "freesurfer" / "sub-0001")
    assert len(results) == 1
    assert results[0].pattern == "freesurfer"


def test_check_detailed_specialized_qsirecon_returns_single_entry(tmp_path):
    proc = Procedure(
        name="qsirecon", output_dir="qsirecon", script="s.sh",
        completion_marker=None
    )
    results = check_detailed(proc, tmp_path / "qsirecon" / "sub-0001" / "ses-01")
    assert len(results) == 1
    assert results[0].pattern == "qsirecon"

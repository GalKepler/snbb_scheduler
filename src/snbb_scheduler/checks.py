from __future__ import annotations

__all__ = ["is_complete"]

from pathlib import Path
from typing import Callable

from snbb_scheduler.config import Procedure


# ---------------------------------------------------------------------------
# Specialized check registry
# ---------------------------------------------------------------------------

# Maps procedure name → specialized completion function
# Signature: (proc, output_path, **kwargs) -> bool
_SPECIALIZED_CHECKS: dict[str, Callable] = {}


def _register_check(name: str):
    """Decorator to register a specialized completion check for a procedure."""

    def decorator(fn: Callable) -> Callable:
        _SPECIALIZED_CHECKS[name] = fn
        return fn

    return decorator


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def is_complete(proc: Procedure, output_path: Path, **kwargs) -> bool:
    """Return True if a procedure's output is considered complete.

    Completion is determined by proc.completion_marker:
      None          — output directory must exist and be non-empty
      "path/file"   — that specific file must exist inside output_path
      "**/*.nii.gz" — at least one file matching the glob must exist
      ["pat1", ...] — ALL patterns must match at least one file

    Procedures registered in ``_SPECIALIZED_CHECKS`` use a custom check
    function instead and are called **before** the ``output_path.exists()``
    guard, allowing them to remap paths (e.g. FreeSurfer's longitudinal
    SUBJECTS_DIR naming differs from the scheduler's path convention).

    Unknown keyword arguments are silently ignored.
    """
    if proc.name in _SPECIALIZED_CHECKS:
        return _SPECIALIZED_CHECKS[proc.name](proc, output_path, **kwargs)

    if not output_path.exists():
        return False

    marker = proc.completion_marker

    if marker is None:
        return _dir_nonempty(output_path)

    if isinstance(marker, list):
        return all(any(output_path.glob(pat)) for pat in marker)

    if _is_glob(marker):
        return any(output_path.glob(marker))

    return (output_path / marker).exists()


# ---------------------------------------------------------------------------
# Specialized checks
# ---------------------------------------------------------------------------


@_register_check("freesurfer")
def _freesurfer_check(proc: Procedure, output_path: Path, **kwargs) -> bool:
    """FreeSurfer longitudinal completion check.

    ``output_path`` is ``<derivatives>/freesurfer/<subject>`` — the subject-level
    directory that serves as the template output and the QSIRecon FS subjects dir.

    With ``bids_root`` and ``subject`` kwargs:

    * **Single-session**: checks ``<output_path>/scripts/recon-all.done``
      (cross-sectional output, located at ``<subject>/``).
    * **Multi-session** (2+ sessions): checks all three pipeline steps:

      1. Cross-sectional — ``<subjects_dir>/<subject>_<session>/scripts/recon-all.done``
         for every BIDS session.
      2. Template — ``<output_path>/scripts/recon-all.done``
         (i.e. ``<subject>/`` directory, same as single-session location).
      3. Longitudinal — ``<subjects_dir>/<subject>_<session>.long.<subject>/scripts/recon-all.done``
         for every BIDS session.

    Without kwargs: falls back to checking ``<output_path>/scripts/recon-all.done``.
    """
    bids_root = kwargs.get("bids_root")
    subject = kwargs.get("subject")

    if bids_root is None or subject is None:
        # Backward-compat fallback
        return (output_path / "scripts" / "recon-all.done").exists()

    sessions = _count_bids_anat_sessions(Path(bids_root), subject)
    if not sessions:
        return False

    # output_path = derivatives/freesurfer/<subject>
    subjects_dir = output_path.parent

    if len(sessions) == 1:
        # Single session: cross-sectional only, output at <subject>/
        return (output_path / "scripts" / "recon-all.done").exists()

    # Multi-session: verify all 3 pipeline steps
    # Step 1 — cross-sectional for each session
    for ses in sessions:
        cross_done = subjects_dir / f"{subject}_{ses}" / "scripts" / "recon-all.done"
        if not cross_done.exists():
            return False

    # Step 2 — template
    if not (output_path / "scripts" / "recon-all.done").exists():
        return False

    # Step 3 — longitudinal for each session
    for ses in sessions:
        long_done = (
            subjects_dir
            / f"{subject}_{ses}.long.{subject}"
            / "scripts"
            / "recon-all.done"
        )
        if not long_done.exists():
            return False

    return True


@_register_check("qsiprep")
def _qsiprep_check(proc: Procedure, output_path: Path, **kwargs) -> bool:
    """QSIPrep (subject-scoped) is complete when a processed ``ses-*`` subdirectory
    exists for every BIDS session that has DWI data.

    Falls back to ``_dir_nonempty`` when ``bids_root``/``subject`` are absent.
    """
    bids_root = kwargs.get("bids_root")
    subject = kwargs.get("subject")
    if bids_root is None or subject is None:
        return _dir_nonempty(output_path)

    qsiprep_sessions = _count_subject_ses_dirs(output_path)
    dwi_sessions = _count_bids_dwi_sessions(Path(bids_root), subject)
    return qsiprep_sessions > 0 and qsiprep_sessions == dwi_sessions


@_register_check("qsirecon")
def _qsirecon_check(proc: Procedure, output_path: Path, **kwargs) -> bool:
    """QSIRecon (subject-scoped) is complete when its ``ses-*`` subdirectory count
    matches the number of processed QSIPrep sessions for the same subject.

    Falls back to ``_dir_nonempty`` when ``derivatives_root``/``subject`` are absent.
    """
    derivatives_root = kwargs.get("derivatives_root")
    subject = kwargs.get("subject")
    if derivatives_root is None or subject is None:
        return _dir_nonempty(output_path)

    qsirecon_sessions = _count_subject_ses_dirs(output_path)
    qsiprep_sessions = _count_subject_ses_dirs(
        Path(derivatives_root) / "qsiprep" / subject
    )
    return qsirecon_sessions > 0 and qsirecon_sessions == qsiprep_sessions


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_glob(pattern: str) -> bool:
    """Return True if *pattern* contains any glob metacharacter (``*``, ``?``, ``[``)."""
    return "*" in pattern or "?" in pattern or "[" in pattern


def _dir_nonempty(path: Path) -> bool:
    """Return True if *path* is an existing directory that contains at least one entry."""
    try:
        next(path.iterdir())
        return True
    except (StopIteration, FileNotFoundError, NotADirectoryError):
        return False


def _count_recon_all_inputs(done_file: Path) -> int:
    """Count the number of ``-i`` input flags in the CMDARGS line of *done_file*.

    FreeSurfer writes a ``#CMDARGS`` line inside ``scripts/recon-all.done``
    containing all the arguments passed to ``recon-all``, including one
    ``-i <path>`` pair for each T1w input.  This function parses that line
    and returns the number of ``-i`` tokens found.
    """
    for line in done_file.read_text().splitlines():
        if "CMDARGS" in line:
            return sum(1 for token in line.split() if token == "-i")
    return 0


def _count_available_t1w(bids_root: Path, subject: str) -> int:
    """Count T1w NIfTI files that would be passed to ``recon-all`` for *subject*.

    Delegates to :func:`snbb_scheduler.freesurfer.collect_images` so that the
    same filtering rules (exclude defaced, prefer ``rec-norm``) are applied
    here and during actual job execution.
    """
    from snbb_scheduler.freesurfer import collect_images

    t1w, _ = collect_images(bids_root, subject)
    return len(t1w)


def _count_subject_ses_dirs(subject_dir: Path) -> int:
    """Count ``ses-*`` subdirectories inside *subject_dir*."""
    if not subject_dir.exists():
        return 0
    return sum(
        1 for d in subject_dir.iterdir() if d.is_dir() and d.name.startswith("ses-")
    )


def _count_bids_anat_sessions(bids_root: Path, subject: str) -> list[str]:
    """Return sorted list of session labels with at least one T1w NIfTI.

    A session qualifies when ``ses-*/anat/*_T1w.nii.gz`` matches inside
    ``<bids_root>/<subject>``.
    """
    subject_dir = bids_root / subject
    if not subject_dir.exists():
        return []
    return sorted(
        ses_dir.name
        for ses_dir in subject_dir.iterdir()
        if ses_dir.is_dir()
        and ses_dir.name.startswith("ses-")
        and any((ses_dir / "anat").glob("*_T1w.nii.gz"))
    )


def _count_bids_dwi_sessions(bids_root: Path, subject: str) -> int:
    """Count BIDS sessions for *subject* that contain at least one DWI NIfTI.

    A session qualifies when ``ses-*/dwi/*_dwi.nii.gz`` matches inside
    ``<bids_root>/<subject>``.
    """
    subject_dir = bids_root / subject
    if not subject_dir.exists():
        return 0
    return sum(
        1
        for ses_dir in subject_dir.iterdir()
        if ses_dir.is_dir()
        and ses_dir.name.startswith("ses-")
        and any((ses_dir / "dwi").glob("*_dwi.nii.gz"))
    )

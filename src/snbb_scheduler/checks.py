from __future__ import annotations

__all__ = ["is_complete", "check_detailed", "FileCheckResult"]

from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import yaml

from snbb_scheduler.config import Procedure


@dataclass
class FileCheckResult:
    """Result of a single completion-marker pattern check."""

    pattern: str
    found: bool
    matched_files: list[str] = field(default_factory=list)


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


def check_detailed(
    proc: Procedure, output_path: Path, **kwargs
) -> list[FileCheckResult]:
    """Per-pattern check results instead of a single boolean.

    For list completion_markers: checks each pattern individually.
    For specialized checks (freesurfer, qsirecon): returns a single entry
    with the overall is_complete() result.
    For None marker: returns a single entry checking if the directory is non-empty.
    For a single glob/file marker: returns a single entry for that pattern.

    Does NOT modify existing is_complete() behavior.
    """
    if proc.name in _SPECIALIZED_CHECKS:
        overall = _SPECIALIZED_CHECKS[proc.name](proc, output_path, **kwargs)
        return [FileCheckResult(pattern=proc.name, found=overall)]

    if not output_path.exists():
        marker = proc.completion_marker
        if marker is None:
            return [FileCheckResult(pattern="<directory>", found=False)]
        patterns = marker if isinstance(marker, list) else [marker]
        return [FileCheckResult(pattern=p, found=False) for p in patterns]

    marker = proc.completion_marker

    if marker is None:
        found = _dir_nonempty(output_path)
        files = [str(p) for p in output_path.iterdir()] if found else []
        return [FileCheckResult(pattern="<directory>", found=found, matched_files=files)]

    if isinstance(marker, list):
        results = []
        for pat in marker:
            matched = list(output_path.glob(pat))
            results.append(
                FileCheckResult(
                    pattern=pat,
                    found=bool(matched),
                    matched_files=[str(m) for m in matched],
                )
            )
        return results

    if _is_glob(marker):
        matched = list(output_path.glob(marker))
        return [
            FileCheckResult(
                pattern=marker,
                found=bool(matched),
                matched_files=[str(m) for m in matched],
            )
        ]

    target = output_path / marker
    return [
        FileCheckResult(
            pattern=marker,
            found=target.exists(),
            matched_files=[str(target)] if target.exists() else [],
        )
    ]


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
        return _recon_all_succeeded(output_path / "scripts" / "recon-all.done")

    sessions = _count_bids_anat_sessions(Path(bids_root), subject)
    if not sessions:
        return False

    # output_path = derivatives/freesurfer/<subject>
    subjects_dir = output_path.parent

    if len(sessions) == 1:
        # Single session: cross-sectional only, output at <subject>/
        return _recon_all_succeeded(output_path / "scripts" / "recon-all.done")

    # Multi-session: verify all 3 pipeline steps
    # Step 1 — cross-sectional for each session
    for ses in sessions:
        cross_done = subjects_dir / f"{subject}_{ses}" / "scripts" / "recon-all.done"
        if not _recon_all_succeeded(cross_done):
            return False

    # Step 2 — template
    if not _recon_all_succeeded(output_path / "scripts" / "recon-all.done"):
        return False

    # Step 3 — longitudinal for each session
    for ses in sessions:
        long_done = (
            subjects_dir
            / f"{subject}_{ses}.long.{subject}"
            / "scripts"
            / "recon-all.done"
        )
        if not _recon_all_succeeded(long_done):
            return False

    return True


# @_register_check("qsiprep")
# def _qsiprep_check(proc: Procedure, output_path: Path, **kwargs) -> bool:
#     """QSIPrep (subject-scoped) is complete when a processed ``ses-*`` subdirectory
#     exists for every BIDS session that has DWI data.

#     Falls back to ``_dir_nonempty`` when ``bids_root``/``subject`` are absent.
#     """
#     bids_root = kwargs.get("bids_root")
#     subject = kwargs.get("subject")
#     if bids_root is None or subject is None:
#         return _dir_nonempty(output_path)

#     qsiprep_sessions = _count_subject_ses_dirs(output_path)
#     dwi_sessions = _count_bids_dwi_sessions(Path(bids_root), subject)
#     return qsiprep_sessions > 0 and qsiprep_sessions == dwi_sessions


@_register_check("qsirecon")
def _qsirecon_check(proc: Procedure, output_path: Path, **kwargs) -> bool:
    """QSIRecon (session-scoped) is complete when an HTML report exists at
    ``<qsirecon_root>/derivatives/qsirecon-<suffix>/<subject>_<session>.html``
    for every workflow suffix listed in the QSIRecon spec YAML.

    Falls back to a wildcard HTML glob when no ``recon_spec`` is given, and to
    ``_dir_nonempty`` when ``derivatives_root``, ``subject``, or ``session``
    are absent.
    """
    derivatives_root = kwargs.get("derivatives_root")
    subject = kwargs.get("subject")
    session = kwargs.get("session")
    if derivatives_root is None or subject is None or session is None:
        return _dir_nonempty(output_path)

    qsirecon_root = Path(derivatives_root) / "qsirecon"
    recon_spec = kwargs.get("recon_spec")

    if recon_spec is not None:
        suffixes = _parse_qsirecon_suffixes(Path(recon_spec))
        if suffixes:
            for suffix in suffixes:
                html = (
                    qsirecon_root
                    / "derivatives"
                    / f"qsirecon-{suffix}"
                    / f"{subject}_{session}.html"
                )
                if not html.exists():
                    return False
            return True
        # spec missing/empty → fall through to wildcard

    # Fallback: any matching HTML under derivatives/
    return any(qsirecon_root.glob(f"derivatives/*/{subject}_{session}.html"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_qsirecon_suffixes(recon_spec: Path) -> list[str]:
    """Return unique ``qsirecon_suffix`` values from a QSIRecon workflow YAML.

    Reads *recon_spec*, iterates ``nodes``, and collects the ``qsirecon_suffix``
    field where present (order-preserving, duplicates removed).  Returns an
    empty list if the file is missing, unreadable, or contains no suffixes.
    """
    try:
        data = yaml.safe_load(recon_spec.read_text()) or {}
    except (OSError, yaml.YAMLError):
        return []

    seen: list[str] = []
    seen_set: set[str] = set()
    for node in data.get("nodes", []):
        suffix = node.get("qsirecon_suffix")
        if suffix and suffix not in seen_set:
            seen.append(suffix)
            seen_set.add(suffix)
    return seen


def _recon_all_succeeded(done_file: Path) -> bool:
    """Return True if *done_file* exists and indicates a successful run.

    On success, FreeSurfer writes a multi-line metadata block starting with
    ``-----...`` into ``scripts/recon-all.done``.  On failure, it writes just
    the numeric exit code (e.g. ``1``).  We consider the run successful when
    the file exists and its first line is *not* a bare integer.
    """
    if not done_file.exists():
        return False
    try:
        first_line = done_file.read_text().split("\n", 1)[0].strip()
        if not first_line:
            return False
        # A bare integer means recon-all exited with an error code
        try:
            int(first_line)
            return False
        except ValueError:
            return True
    except OSError:
        return False


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

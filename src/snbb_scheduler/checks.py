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
    function instead.  Unknown keyword arguments are silently ignored.
    """
    if not output_path.exists():
        return False

    if proc.name in _SPECIALIZED_CHECKS:
        return _SPECIALIZED_CHECKS[proc.name](proc, output_path, **kwargs)

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
    """FreeSurfer is complete when recon-all.done exists AND all available
    T1w images were used as inputs.

    If ``bids_root`` and ``subject`` are not provided, falls back to simply
    checking for the ``scripts/recon-all.done`` marker file.
    """
    done_file = output_path / "scripts" / "recon-all.done"
    if not done_file.exists():
        return False

    bids_root = kwargs.get("bids_root")
    subject = kwargs.get("subject")
    if bids_root is None or subject is None:
        # Backward-compat fallback: marker file presence is sufficient
        return True

    used = _count_recon_all_inputs(done_file)
    available = _count_available_t1w(Path(bids_root), subject)
    return used == available


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_glob(pattern: str) -> bool:
    """Return True if *pattern* contains any glob metacharacter (``*``, ``?``, ``[``)."""
    return "*" in pattern or "?" in pattern or "[" in pattern


def _dir_nonempty(path: Path) -> bool:
    """Return True if *path* is a directory that contains at least one entry."""
    try:
        next(path.iterdir())
        return True
    except StopIteration:
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
    """Count T1w NIfTI files across all sessions for *subject* in *bids_root*.

    Globs ``<bids_root>/<subject>/ses-*/anat/*_T1w.nii.gz``.
    """
    subject_dir = bids_root / subject
    if not subject_dir.exists():
        return 0
    return len(list(subject_dir.glob("ses-*/anat/*_T1w.nii.gz")))

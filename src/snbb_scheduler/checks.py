from __future__ import annotations

__all__ = ["is_complete"]

from pathlib import Path

from snbb_scheduler.config import Procedure


def is_complete(proc: Procedure, output_path: Path) -> bool:
    """Return True if a procedure's output is considered complete.

    Completion is determined by proc.completion_marker:
      None          — output directory must exist and be non-empty
      "path/file"   — that specific file must exist inside output_path
      "**/*.nii.gz" — at least one file matching the glob must exist
    """
    if not output_path.exists():
        return False

    marker = proc.completion_marker

    if marker is None:
        return _dir_nonempty(output_path)

    if _is_glob(marker):
        return any(output_path.glob(marker))

    return (output_path / marker).exists()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_glob(pattern: str) -> bool:
    return "*" in pattern or "?" in pattern or "[" in pattern


def _dir_nonempty(path: Path) -> bool:
    try:
        next(path.iterdir())
        return True
    except StopIteration:
        return False

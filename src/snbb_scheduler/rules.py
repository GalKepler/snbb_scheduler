from __future__ import annotations

__all__ = ["Rule", "build_rules"]

from typing import Callable

import pandas as pd

from snbb_scheduler.checks import is_complete
from snbb_scheduler.config import Procedure, SchedulerConfig

# Type alias for a rule function
Rule = Callable[[pd.Series], bool]


def build_rules(
    config: SchedulerConfig,
    force: bool = False,
    force_procedures: list[str] | None = None,
) -> dict[str, Rule]:
    """Generate a rule function for every procedure in config.

    Each rule returns True when all of the following hold:
      1. DICOM data exists for the session (dicom_exists)
      2. All upstream procedures listed in proc.depends_on are complete
      3. This procedure's own output is not yet complete
         (skipped when *force* is True and the procedure is in *force_procedures*,
         or when *force* is True and *force_procedures* is None)
    """
    return {
        proc.name: _make_rule(proc, config, force=force, force_procedures=force_procedures)
        for proc in config.procedures
    }


def _completion_kwargs(proc: Procedure, row: pd.Series, config: SchedulerConfig) -> dict:
    """Return extra keyword arguments for ``is_complete`` based on procedure name.

    Some specialized checks require additional context (e.g. the BIDS root
    and subject label) that cannot be derived from the output path alone.
    This helper centralises that mapping so that ``_make_rule`` stays clean.
    """
    subject = row["subject"]
    if proc.name in ("freesurfer", "qsiprep"):
        return {"bids_root": config.bids_root, "subject": subject}
    if proc.name == "qsirecon":
        return {"derivatives_root": config.derivatives_root, "subject": subject}
    return {}


def _make_rule(
    proc: Procedure,
    config: SchedulerConfig,
    force: bool = False,
    force_procedures: list[str] | None = None,
) -> Rule:
    """Create a rule closure that decides whether *proc* needs to run for a session.

    The returned callable accepts a session row (``pd.Series``) and returns
    ``True`` when **all** of the following hold:

    1. DICOM data exists for the session (``dicom_exists`` is ``True``).
    2. Every procedure in ``proc.depends_on`` is already complete on disk.
    3. This procedure's own output is **not** yet complete on disk.

    The closure captures *proc* and *config* by reference so that rule
    functions stay lightweight and can be regenerated cheaply.
    """
    def rule(row: pd.Series) -> bool:
        if not row["dicom_exists"]:
            return False
        for dep_name in proc.depends_on:
            dep_proc = config.get_procedure(dep_name)
            dep_kwargs = _completion_kwargs(dep_proc, row, config)
            if not is_complete(dep_proc, row[f"{dep_name}_path"], **dep_kwargs):
                return False
        should_force = force and (force_procedures is None or proc.name in force_procedures)
        if should_force:
            return True
        self_kwargs = _completion_kwargs(proc, row, config)
        return not is_complete(proc, row[f"{proc.name}_path"], **self_kwargs)

    rule.__name__ = f"needs_{proc.name}"
    return rule

from __future__ import annotations

__all__ = ["Rule", "build_rules"]

from typing import Callable

import pandas as pd

from snbb_scheduler.checks import is_complete
from snbb_scheduler.config import Procedure, SchedulerConfig

# Type alias for a rule function
Rule = Callable[[pd.Series], bool]


def build_rules(config: SchedulerConfig) -> dict[str, Rule]:
    """Generate a rule function for every procedure in config.

    Each rule returns True when all of the following hold:
      1. DICOM data exists for the session (dicom_exists)
      2. All upstream procedures listed in proc.depends_on are complete
      3. This procedure's own output is not yet complete
    """
    return {proc.name: _make_rule(proc, config) for proc in config.procedures}


def _make_rule(proc: Procedure, config: SchedulerConfig) -> Rule:
    def rule(row: pd.Series) -> bool:
        if not row["dicom_exists"]:
            return False
        for dep_name in proc.depends_on:
            dep_proc = config.get_procedure(dep_name)
            if not is_complete(dep_proc, row[f"{dep_name}_path"]):
                return False
        return not is_complete(proc, row[f"{proc.name}_path"])

    rule.__name__ = f"needs_{proc.name}"
    return rule

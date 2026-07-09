#!/usr/bin/env python3
"""snbb_recon_all_helper.py — thin entry-point wrapper.

The implementation lives in :mod:`snbb_scheduler.freesurfer`.  This script
exists only so that :file:`snbb_run_freesurfer.sh` can call it at a stable
path without requiring any changes to the shell script.
"""

import sys
from pathlib import Path

# Allow running without pip-installing the package: look for src/ next to scripts/
_src = Path(__file__).resolve().parent.parent / "src"
if _src.is_dir() and str(_src) not in sys.path:
    sys.path.insert(0, str(_src))

from snbb_scheduler.freesurfer import main

if __name__ == "__main__":
    sys.exit(main())

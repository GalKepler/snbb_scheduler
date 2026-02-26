#!/usr/bin/env python3
"""snbb_recon_all_helper.py — thin entry-point wrapper.

The implementation lives in :mod:`snbb_scheduler.freesurfer`.  This script
exists only so that :file:`snbb_run_freesurfer.sh` can call it at a stable
path without requiring any changes to the shell script.
"""

import sys

from snbb_scheduler.freesurfer import main

if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""snbb_fastsurfer_helper.py — thin entry-point wrapper for FastSurfer.

The implementation lives in :mod:`snbb_scheduler.fastsurfer`.  This script
exists only so that the FastSurfer shell scripts can call it at a stable
path without requiring any changes to the shell scripts when the package
is updated.

Usage::

    python3 snbb_fastsurfer_helper.py cross   --bids-dir ... --subject ... --session ...
    python3 snbb_fastsurfer_helper.py template --output-dir ... --subject ... --sessions ...
    python3 snbb_fastsurfer_helper.py long     --output-dir ... --subject ... --session ...

See :func:`snbb_scheduler.fastsurfer.main` for full argument documentation.
"""

import sys

from snbb_scheduler.fastsurfer import main

if __name__ == "__main__":
    sys.exit(main())

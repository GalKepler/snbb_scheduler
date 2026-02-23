#!/usr/bin/env bash
# snbb_run_qsiprep.sh — QSIPrep diffusion MRI preprocessing wrapper
# Called by the snbb_scheduler as:  sbatch ... snbb_run_qsiprep.sh sub-XXXX ses-YY
#
# ── Site configuration ────────────────────────────────────────────────────────
SNBB_BIDS_ROOT="${SNBB_BIDS_ROOT:-/data/snbb/bids}"
SNBB_DERIVATIVES="${SNBB_DERIVATIVES:-/data/snbb/derivatives}"
SNBB_FS_LICENSE="${SNBB_FS_LICENSE:-/data/snbb/freesurfer_license.txt}"
SNBB_LOG_DIR="${SNBB_LOG_DIR:-/data/snbb/logs/qsiprep}"
SNBB_RUNNERS_DIR="${SNBB_RUNNERS_DIR:-/opt/snbb_scheduler/examples/runners}"
# ─────────────────────────────────────────────────────────────────────────────

#SBATCH --time=12:00:00
#SBATCH --mem=32G
#SBATCH --cpus-per-task=8

set -euo pipefail

SUBJECT="$1"          # e.g. sub-0001  ($2 = session, not used — QSIPrep is subject-scoped)
PARTICIPANT="${SUBJECT#sub-}"

python "${SNBB_RUNNERS_DIR}/run_qsiprep.py" \
    --bids-dir     "${SNBB_BIDS_ROOT}" \
    --output-dir   "${SNBB_DERIVATIVES}" \
    --participants "${PARTICIPANT}" \
    --fs-license   "${SNBB_FS_LICENSE}" \
    --workers      1 \
    --log-dir      "${SNBB_LOG_DIR}"

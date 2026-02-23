#!/usr/bin/env bash
# snbb_run_freesurfer.sh — FreeSurfer recon-all wrapper
# Called by the snbb_scheduler as:  sbatch ... snbb_run_freesurfer.sh sub-XXXX ses-YY
#
# ── Site configuration ────────────────────────────────────────────────────────
SNBB_BIDS_ROOT="${SNBB_BIDS_ROOT:-/data/snbb/bids}"
SNBB_FS_OUTPUT="${SNBB_FS_OUTPUT:-/data/snbb/derivatives/freesurfer}"
SNBB_FS_LICENSE="${SNBB_FS_LICENSE:-/data/snbb/freesurfer_license.txt}"
SNBB_LOG_DIR="${SNBB_LOG_DIR:-/data/snbb/logs/freesurfer}"
SNBB_RUNNERS_DIR="${SNBB_RUNNERS_DIR:-/opt/snbb_scheduler/examples/runners}"
# ─────────────────────────────────────────────────────────────────────────────

#SBATCH --time=24:00:00
#SBATCH --mem=16G
#SBATCH --cpus-per-task=8

set -euo pipefail

SUBJECT="$1"          # e.g. sub-0001  ($2 = session, ignored — FreeSurfer is subject-scoped)
PARTICIPANT="${SUBJECT#sub-}"

python "${SNBB_RUNNERS_DIR}/run_freesurfer.py" \
    --bids-dir     "${SNBB_BIDS_ROOT}" \
    --output-dir   "${SNBB_FS_OUTPUT}" \
    --participants "${PARTICIPANT}" \
    --fs-license   "${SNBB_FS_LICENSE}" \
    --workers      1 \
    --log-dir      "${SNBB_LOG_DIR}"

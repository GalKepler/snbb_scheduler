#!/usr/bin/env bash
# snbb_run_bids_post.sh — BIDS fieldmap post-processing (Slurm wrapper)
# Called by the snbb_scheduler as:  sbatch ... snbb_run_bids_post.sh sub-XXXX ses-YY
#
# ── Site configuration ────────────────────────────────────────────────────────
# Edit the values below for your cluster, or set the env vars before submitting.
SNBB_BIDS_ROOT="${SNBB_BIDS_ROOT:-/media/storage/yalab-dev/snbb_scheduler/bids}"
# ─────────────────────────────────────────────────────────────────────────────

#SBATCH --time=0:30:00
#SBATCH --mem=2G
#SBATCH --cpus-per-task=1

set -euo pipefail

SUBJECT="$1"   # e.g. sub-0001
SESSION="$2"   # e.g. ses-202602161208

# SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_DIR=/home/galkepler/Projects/snbb_scheduler/scripts
python "${SCRIPT_DIR}/snbb_bids_post.py" \
    "${SUBJECT}" "${SESSION}" "${SNBB_BIDS_ROOT}"

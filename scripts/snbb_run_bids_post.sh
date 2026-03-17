#!/usr/bin/env bash
# snbb_run_bids_post.sh — BIDS fieldmap post-processing (Slurm wrapper)
# Called by the snbb_scheduler as:  sbatch ... snbb_run_bids_post.sh sub-XXXX ses-YY
#
# ── Site configuration ────────────────────────────────────────────────────────
# Edit the values below for your cluster, or set the env vars before submitting.
SNBB_BIDS_ROOT="${SNBB_BIDS_ROOT:-/media/storage/yalab-dev/snbb_scheduler/bids}"
SNBB_BIDS_POST_TMP="${SNBB_BIDS_POST_TMP:-${TMPDIR:-/tmp}/snbb_bids_post_${SLURM_JOB_ID:-$$}}"
# ─────────────────────────────────────────────────────────────────────────────

#SBATCH --time=0:30:00
#SBATCH --mem=2G
#SBATCH --cpus-per-task=1

set -euo pipefail

SUBJECT="$1"   # e.g. sub-0001
SESSION="$2"   # e.g. ses-202602161208

# SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_DIR=/home/galkepler/Projects/snbb_scheduler/scripts

# Rsync the session to a local temp dir so post-processing runs on files owned
# by the current user, avoiding Permission Denied on network-mounted BIDS roots.
SESSION_SRC="${SNBB_BIDS_ROOT}/${SUBJECT}/${SESSION}"
SESSION_TMP="${SNBB_BIDS_POST_TMP}/${SUBJECT}/${SESSION}"
mkdir -p "${SESSION_TMP}"
trap 'rm -rf "${SNBB_BIDS_POST_TMP}"' EXIT

rsync -a "${SESSION_SRC}/" "${SESSION_TMP}/"
chmod -R u+rw "${SESSION_TMP}"

python "${SCRIPT_DIR}/snbb_bids_post.py" \
    "${SUBJECT}" "${SESSION}" "${SNBB_BIDS_POST_TMP}"

rsync -a --no-perms --no-owner --no-group "${SESSION_TMP}/" "${SESSION_SRC}/"

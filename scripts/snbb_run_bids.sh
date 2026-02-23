#!/usr/bin/env bash
# snbb_run_bids.sh — DICOM → BIDS conversion wrapper
# Called by the snbb_scheduler as:  sbatch ... snbb_run_bids.sh sub-XXXX ses-YY
#
# ── Site configuration ────────────────────────────────────────────────────────
# Edit the values below for your cluster, or set the env vars before submitting.
SNBB_CSV="${SNBB_CSV:-/home/galkepler/Downloads/linked_sessions.csv}"
SNBB_BIDS_ROOT="${SNBB_BIDS_ROOT:-/media/storage/yalab-dev/snbb_scheduler/bids}"
SNBB_HEURISTIC="${SNBB_HEURISTIC:-/home/galkepler/Projects/yalab-devops/VoxelOps/heuristic.py}"
SNBB_LOG_DIR="${SNBB_LOG_DIR:-/media/storage/yalab-dev/snbb_scheduler/logs/bids}"
SNBB_RUNNERS_DIR="${SNBB_RUNNERS_DIR:-/home/galkepler/Projects/snbb_scheduler/examples/runners}"
SNBB_VENV="${SNBB_VENV:-/home/galkepler/Projects/snbb_scheduler/.venv}"
SNBB_DEBUG_LOG="${SNBB_DEBUG_LOG:-/media/storage/yalab-dev/snbb_scheduler/logs/bids/debug_submit.log}"
# ─────────────────────────────────────────────────────────────────────────────

#SBATCH --time=4:00:00
#SBATCH --mem=8G
#SBATCH --cpus-per-task=4

set -euo pipefail

SUBJECT="$1"          # e.g. sub-0001
PARTICIPANT="${SUBJECT#sub-}"   # strip prefix → 0001

# ── Diagnostics ──────────────────────────────────────────────────────────────
mkdir -p "$(dirname "${SNBB_DEBUG_LOG}")"
{
    echo "=== $(date -Iseconds) | Job ${SLURM_JOB_ID:-local} | ${SUBJECT} ==="
    echo "SNBB_VENV:        ${SNBB_VENV}"
    echo "SNBB_CSV:         ${SNBB_CSV}"
    echo "SNBB_HEURISTIC:   ${SNBB_HEURISTIC}"
    echo "SNBB_RUNNERS_DIR: ${SNBB_RUNNERS_DIR}"
    echo "python binary:    ${SNBB_VENV}/bin/python"
    echo "python exists:    $(test -x "${SNBB_VENV}/bin/python" && echo yes || echo NO)"
    echo "voxelops:         $("${SNBB_VENV}/bin/python" -c 'import voxelops; print(voxelops.__file__)' 2>&1)"
    echo "PATH:             ${PATH}"
} >> "${SNBB_DEBUG_LOG}" 2>&1
# ─────────────────────────────────────────────────────────────────────────────

. "${SNBB_VENV}/bin/activate"

"${SNBB_VENV}/bin/python" "${SNBB_RUNNERS_DIR}/run_dicom_to_bids.py" \
    --csv         "${SNBB_CSV}" \
    --output-dir  "${SNBB_BIDS_ROOT}" \
    --heuristic   "${SNBB_HEURISTIC}" \
    --participants "${PARTICIPANT}" \
    --workers      1 \
    --log-dir     "${SNBB_LOG_DIR}" \
    --overwrite

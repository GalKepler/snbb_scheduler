#!/usr/bin/env bash
# snbb_run_qsiprep.sh — QSIPrep diffusion MRI preprocessing wrapper
# Called by the snbb_scheduler as:  sbatch ... snbb_run_qsiprep.sh sub-XXXX ses-YY
#
# ── Site configuration ────────────────────────────────────────────────────────
# Edit the values below for your cluster, or set the env vars before submitting.
SNBB_BIDS_ROOT="${SNBB_BIDS_ROOT:-/media/storage/yalab-dev/snbb_scheduler/bids}"
SNBB_DERIVATIVES="${SNBB_DERIVATIVES:-/media/storage/yalab-dev/snbb_scheduler/derivatives}"
SNBB_FS_LICENSE="${SNBB_FS_LICENSE:-/home/galkepler/misc/freesurfer/license.txt}"
SNBB_LOG_DIR="${SNBB_LOG_DIR:-/media/storage/yalab-dev/snbb_scheduler/logs/qsiprep}"
SNBB_RUNNERS_DIR="${SNBB_RUNNERS_DIR:-/home/galkepler/Projects/snbb_scheduler/examples/runners}"
SNBB_VENV="${SNBB_VENV:-/home/galkepler/Projects/snbb_scheduler/.venv}"
SNBB_DEBUG_LOG="${SNBB_DEBUG_LOG:-/media/storage/yalab-dev/snbb_scheduler/logs/qsiprep/debug_submit.log}"
# ─────────────────────────────────────────────────────────────────────────────

#SBATCH --time=12:00:00
#SBATCH --mem=20G
#SBATCH --cpus-per-task=8

set -euo pipefail

SUBJECT="$1"          # e.g. sub-0001  ($2 = session, not used — QSIPrep is subject-scoped)
PARTICIPANT="${SUBJECT#sub-}"

# ── Diagnostics ──────────────────────────────────────────────────────────────
mkdir -p "$(dirname "${SNBB_DEBUG_LOG}")"
{
    echo "=== $(date -Iseconds) | Job ${SLURM_JOB_ID:-local} | ${SUBJECT} ==="
    echo "SNBB_VENV:        ${SNBB_VENV}"
    echo "SNBB_BIDS_ROOT:   ${SNBB_BIDS_ROOT}"
    echo "SNBB_DERIVATIVES: ${SNBB_DERIVATIVES}"
    echo "SNBB_FS_LICENSE:  ${SNBB_FS_LICENSE}"
    echo "SNBB_RUNNERS_DIR: ${SNBB_RUNNERS_DIR}"
    echo "python binary:    ${SNBB_VENV}/bin/python"
    echo "python exists:    $(test -x "${SNBB_VENV}/bin/python" && echo yes || echo NO)"
    echo "voxelops:         $("${SNBB_VENV}/bin/python" -c 'import voxelops; print(voxelops.__file__)' 2>&1)"
    echo "PATH:             ${PATH}"
} >> "${SNBB_DEBUG_LOG}" 2>&1
# ─────────────────────────────────────────────────────────────────────────────

. "${SNBB_VENV}/bin/activate"

"${SNBB_VENV}/bin/python" "${SNBB_RUNNERS_DIR}/run_qsiprep.py" \
    --bids-dir     "${SNBB_BIDS_ROOT}" \
    --output-dir   "${SNBB_DERIVATIVES}" \
    --participants "${PARTICIPANT}" \
    --fs-license   "${SNBB_FS_LICENSE}" \
    --workers      1 \
    --log-dir      "${SNBB_LOG_DIR}" \
    --force

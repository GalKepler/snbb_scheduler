#!/usr/bin/env bash
# snbb_run_qsiprep.sh — QSIPrep diffusion MRI preprocessing via Apptainer
# Called by the snbb_scheduler as:  sbatch ... snbb_run_qsiprep.sh sub-XXXX ses-YY
#
# ── Site configuration ────────────────────────────────────────────────────────
# Edit the values below for your cluster, or set the env vars before submitting.
SNBB_BIDS_ROOT="${SNBB_BIDS_ROOT:-/data/snbb/bids}"
SNBB_DERIVATIVES="${SNBB_DERIVATIVES:-/data/snbb/derivatives}"
SNBB_FS_LICENSE="${SNBB_FS_LICENSE:-/data/snbb/freesurfer/license.txt}"
SNBB_WORK_DIR="${SNBB_WORK_DIR:-/data/snbb/work/qsiprep}"
SNBB_QSIPREP_SIF="${SNBB_QSIPREP_SIF:-/data/containers/qsiprep.sif}"
SNBB_DEBUG_LOG="${SNBB_DEBUG_LOG:-/data/snbb/logs/qsiprep/debug_submit.log}"
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
    echo "SNBB_BIDS_ROOT:   ${SNBB_BIDS_ROOT}"
    echo "SNBB_DERIVATIVES: ${SNBB_DERIVATIVES}"
    echo "SNBB_FS_LICENSE:  ${SNBB_FS_LICENSE}"
    echo "SNBB_WORK_DIR:    ${SNBB_WORK_DIR}"
    echo "SNBB_QSIPREP_SIF: ${SNBB_QSIPREP_SIF}"
    echo "PATH:             ${PATH}"
} >> "${SNBB_DEBUG_LOG}" 2>&1
# ─────────────────────────────────────────────────────────────────────────────

mkdir -p "${SNBB_WORK_DIR}"

apptainer run --cleanenv \
    --bind "${SNBB_BIDS_ROOT}":"${SNBB_BIDS_ROOT}":ro \
    --bind "${SNBB_DERIVATIVES}":"${SNBB_DERIVATIVES}" \
    --bind "${SNBB_FS_LICENSE}":"${SNBB_FS_LICENSE}":ro \
    --bind "${SNBB_WORK_DIR}":"${SNBB_WORK_DIR}" \
    "${SNBB_QSIPREP_SIF}" \
    "${SNBB_BIDS_ROOT}" \
    "${SNBB_DERIVATIVES}/qsiprep" \
    participant \
    --participant-label "${PARTICIPANT}" \
    --fs-license-file "${SNBB_FS_LICENSE}" \
    --nthreads "${SLURM_CPUS_PER_TASK:-8}" \
    --mem_mb "$((${SLURM_MEM_PER_NODE:-20480}))" \
    --work-dir "${SNBB_WORK_DIR}" \
    --output-resolution 1.5

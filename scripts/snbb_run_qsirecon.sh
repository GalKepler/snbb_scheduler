#!/usr/bin/env bash
# snbb_run_qsirecon.sh — QSIRecon diffusion reconstruction via Apptainer
# Called by the snbb_scheduler as:  sbatch ... snbb_run_qsirecon.sh sub-XXXX ses-YY
#
# ── Site configuration ────────────────────────────────────────────────────────
# Edit the values below for your cluster, or set the env vars before submitting.
SNBB_QSIPREP_DIR="${SNBB_QSIPREP_DIR:-/data/snbb/derivatives/qsiprep}"
SNBB_DERIVATIVES="${SNBB_DERIVATIVES:-/data/snbb/derivatives}"
SNBB_FS_LICENSE="${SNBB_FS_LICENSE:-/data/snbb/freesurfer/license.txt}"
SNBB_FS_SUBJECTS_DIR="${SNBB_FS_SUBJECTS_DIR:-/data/snbb/derivatives/freesurfer}"
SNBB_RECON_SPEC="${SNBB_RECON_SPEC:-/data/snbb/recon_spec.yaml}"
SNBB_WORK_DIR="${SNBB_WORK_DIR:-/data/snbb/work/qsirecon}"
SNBB_QSIRECON_SIF="${SNBB_QSIRECON_SIF:-/data/containers/qsirecon.sif}"
SNBB_DEBUG_LOG="${SNBB_DEBUG_LOG:-/data/snbb/logs/qsirecon/debug_submit.log}"
# ─────────────────────────────────────────────────────────────────────────────

#SBATCH --time=12:00:00
#SBATCH --mem=32G
#SBATCH --cpus-per-task=8

set -euo pipefail

SUBJECT="$1"          # e.g. sub-0001
SESSION="$2"          # e.g. ses-01
PARTICIPANT="${SUBJECT#sub-}"
SESSION_ID="${SESSION#ses-}"

# ── Diagnostics ──────────────────────────────────────────────────────────────
mkdir -p "$(dirname "${SNBB_DEBUG_LOG}")"
{
    echo "=== $(date -Iseconds) | Job ${SLURM_JOB_ID:-local} | ${SUBJECT} ${SESSION} ==="
    echo "SNBB_QSIPREP_DIR:     ${SNBB_QSIPREP_DIR}"
    echo "SNBB_DERIVATIVES:     ${SNBB_DERIVATIVES}"
    echo "SNBB_FS_LICENSE:      ${SNBB_FS_LICENSE}"
    echo "SNBB_FS_SUBJECTS_DIR: ${SNBB_FS_SUBJECTS_DIR}"
    echo "SNBB_RECON_SPEC:      ${SNBB_RECON_SPEC}"
    echo "SNBB_WORK_DIR:        ${SNBB_WORK_DIR}"
    echo "SNBB_QSIRECON_SIF:    ${SNBB_QSIRECON_SIF}"
    echo "PATH:                 ${PATH}"
} >> "${SNBB_DEBUG_LOG}" 2>&1
# ─────────────────────────────────────────────────────────────────────────────

mkdir -p "${SNBB_WORK_DIR}"

apptainer run --cleanenv \
    --bind "${SNBB_QSIPREP_DIR}":"${SNBB_QSIPREP_DIR}":ro \
    --bind "${SNBB_DERIVATIVES}":"${SNBB_DERIVATIVES}" \
    --bind "${SNBB_FS_LICENSE}":"${SNBB_FS_LICENSE}":ro \
    --bind "${SNBB_FS_SUBJECTS_DIR}":"${SNBB_FS_SUBJECTS_DIR}":ro \
    --bind "${SNBB_RECON_SPEC}":"${SNBB_RECON_SPEC}":ro \
    --bind "${SNBB_WORK_DIR}":"${SNBB_WORK_DIR}" \
    "${SNBB_QSIRECON_SIF}" \
    "${SNBB_QSIPREP_DIR}" \
    "${SNBB_DERIVATIVES}/qsirecon" \
    participant \
    --participant-label "${PARTICIPANT}" \
    --session-id "${SESSION_ID}" \
    --recon-spec "${SNBB_RECON_SPEC}" \
    --fs-license-file "${SNBB_FS_LICENSE}" \
    --freesurfer-input "${SNBB_FS_SUBJECTS_DIR}" \
    --nthreads "${SLURM_CPUS_PER_TASK:-8}" \
    --mem_mb "$((${SLURM_MEM_PER_NODE:-32768}))" \
    --work-dir "${SNBB_WORK_DIR}"

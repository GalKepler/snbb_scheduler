#!/usr/bin/env bash
# snbb_run_qsiprep.sh — QSIPrep diffusion MRI preprocessing via Apptainer
# Called by the snbb_scheduler as:  sbatch ... snbb_run_qsiprep.sh sub-XXXX ses-YY
#
# QSIPrep writes its output to <SNBB_DERIVATIVES>/qsiprep/ — the tool creates
# that subdirectory automatically, so SNBB_DERIVATIVES is passed as the output
# root (not SNBB_DERIVATIVES/qsiprep).
#
# ── Site configuration ────────────────────────────────────────────────────────
# Edit the values below for your cluster, or set the env vars before submitting.
SNBB_BIDS_ROOT="${SNBB_BIDS_ROOT:-/media/storage/yalab-dev/snbb_scheduler/bids}"
SNBB_DERIVATIVES="${SNBB_DERIVATIVES:-/media/storage/yalab-dev/snbb_scheduler/derivatives/qsiprep}"
SNBB_FS_LICENSE="${SNBB_FS_LICENSE:-/home/galkepler/misc/freesurfer/license.txt}"
SNBB_WORK_DIR="${SNBB_WORK_DIR:-/media/storage/yalab-dev/snbb_scheduler/work/qsiprep}"
SNBB_QSIPREP_SIF="${SNBB_QSIPREP_SIF:-/media/storage/apptainer/images/qsiprep-1.1.1.sif}"
SNBB_DEBUG_LOG="${SNBB_DEBUG_LOG:-/media/storage/yalab-dev/snbb_scheduler/logs/qsiprep/debug_submit.log}"
# Anatomical template and reference (override if your site uses a different space)
SNBB_ANATOMICAL_TEMPLATE="${SNBB_ANATOMICAL_TEMPLATE:-MNI152NLin2009cAsym}"
SNBB_SUBJECT_ANAT_REF="${SNBB_SUBJECT_ANAT_REF:-unbiased}"
# Optional BIDS filter file — set to restrict which runs QSIPrep processes
SNBB_BIDS_FILTER_FILE="${SNBB_BIDS_FILTER_FILE:-/home/galkepler/Projects/snbb_scheduler/examples/bids_filters.json}"
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
    echo "SNBB_BIDS_ROOT:           ${SNBB_BIDS_ROOT}"
    echo "SNBB_DERIVATIVES:         ${SNBB_DERIVATIVES}"
    echo "SNBB_FS_LICENSE:          ${SNBB_FS_LICENSE}"
    echo "SNBB_WORK_DIR:            ${SNBB_WORK_DIR}"
    echo "SNBB_QSIPREP_SIF:         ${SNBB_QSIPREP_SIF}"
    echo "SNBB_ANATOMICAL_TEMPLATE: ${SNBB_ANATOMICAL_TEMPLATE}"
    echo "SNBB_SUBJECT_ANAT_REF:    ${SNBB_SUBJECT_ANAT_REF}"
    echo "SNBB_BIDS_FILTER_FILE:    ${SNBB_BIDS_FILTER_FILE}"
    echo "PATH:                     ${PATH}"
} >> "${SNBB_DEBUG_LOG}" 2>&1
# ─────────────────────────────────────────────────────────────────────────────

mkdir -p "${SNBB_WORK_DIR}"

# Build optional bind mounts and flags
EXTRA_BINDS=()
EXTRA_ARGS=()
if [[ -n "${SNBB_BIDS_FILTER_FILE}" ]]; then
    EXTRA_BINDS+=(--bind "${SNBB_BIDS_FILTER_FILE}":"${SNBB_BIDS_FILTER_FILE}":ro)
    EXTRA_ARGS+=(--bids-filter-file "${SNBB_BIDS_FILTER_FILE}")
fi

apptainer run --cleanenv \
    --bind "${SNBB_BIDS_ROOT}":"${SNBB_BIDS_ROOT}":ro \
    --bind "${SNBB_DERIVATIVES}":"${SNBB_DERIVATIVES}" \
    --bind "${SNBB_FS_LICENSE}":"${SNBB_FS_LICENSE}":ro \
    --bind "${SNBB_WORK_DIR}":"${SNBB_WORK_DIR}" \
    "${EXTRA_BINDS[@]}" \
    "${SNBB_QSIPREP_SIF}" \
    "${SNBB_BIDS_ROOT}" \
    "${SNBB_DERIVATIVES}" \
    participant \
    --participant-label "${PARTICIPANT}" \
    --fs-license-file "${SNBB_FS_LICENSE}" \
    --nprocs "${SLURM_CPUS_PER_TASK:-8}" \
    --mem-mb "${SLURM_MEM_PER_NODE:-16000}" \
    --work-dir "${SNBB_WORK_DIR}" \
    --output-resolution 1.6 \
    --anatomical-template "${SNBB_ANATOMICAL_TEMPLATE}" \
    --subject-anatomical-reference "${SNBB_SUBJECT_ANAT_REF}" \
    "${EXTRA_ARGS[@]}"

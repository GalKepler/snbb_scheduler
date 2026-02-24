#!/usr/bin/env bash
# snbb_run_qsirecon.sh — QSIRecon diffusion reconstruction via Apptainer
# Called by the snbb_scheduler as:  sbatch ... snbb_run_qsirecon.sh sub-XXXX ses-YY
#
# ── Site configuration ────────────────────────────────────────────────────────
# Edit the values below for your cluster, or set the env vars before submitting.
SNBB_QSIPREP_DIR="${SNBB_QSIPREP_DIR:-/data/snbb/derivatives/qsiprep}"
SNBB_QSIRECON_OUTPUT_DIR="${SNBB_QSIRECON_OUTPUT_DIR:-/data/snbb/derivatives/qsirecon}"
SNBB_FS_LICENSE="${SNBB_FS_LICENSE:-/data/snbb/freesurfer/license.txt}"
SNBB_FS_SUBJECTS_DIR="${SNBB_FS_SUBJECTS_DIR:-/data/snbb/derivatives/freesurfer}"
SNBB_RECON_SPEC="${SNBB_RECON_SPEC:-/data/snbb/recon_spec.yaml}"
SNBB_WORK_DIR="${SNBB_WORK_DIR:-/data/snbb/work/qsirecon}"
SNBB_QSIRECON_SIF="${SNBB_QSIRECON_SIF:-/data/containers/qsirecon.sif}"
SNBB_DEBUG_LOG="${SNBB_DEBUG_LOG:-/data/snbb/logs/qsirecon/debug_submit.log}"
# Optional: directory of pre-computed response functions (--recon-spec-aux-files)
SNBB_RESPONSES_DIR="${SNBB_RESPONSES_DIR:-}"
# Optional: atlas dataset directory and space-separated atlas names
# Example: SNBB_ATLASES_DIR=/data/atlases  SNBB_ATLASES="4S156Parcels Schaefer2018N100n7Tian2020S1"
SNBB_ATLASES_DIR="${SNBB_ATLASES_DIR:-}"
SNBB_ATLASES="${SNBB_ATLASES:-}"
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
    echo "SNBB_QSIPREP_DIR:       ${SNBB_QSIPREP_DIR}"
    echo "SNBB_QSIRECON_OUTPUT_DIR: ${SNBB_QSIRECON_OUTPUT_DIR}"
    echo "SNBB_FS_LICENSE:        ${SNBB_FS_LICENSE}"
    echo "SNBB_FS_SUBJECTS_DIR:   ${SNBB_FS_SUBJECTS_DIR}"
    echo "SNBB_RECON_SPEC:        ${SNBB_RECON_SPEC}"
    echo "SNBB_WORK_DIR:          ${SNBB_WORK_DIR}"
    echo "SNBB_QSIRECON_SIF:      ${SNBB_QSIRECON_SIF}"
    echo "SNBB_RESPONSES_DIR:     ${SNBB_RESPONSES_DIR}"
    echo "SNBB_ATLASES_DIR:       ${SNBB_ATLASES_DIR}"
    echo "SNBB_ATLASES:           ${SNBB_ATLASES}"
    echo "PATH:                   ${PATH}"
} >> "${SNBB_DEBUG_LOG}" 2>&1
# ─────────────────────────────────────────────────────────────────────────────

mkdir -p "${SNBB_WORK_DIR}" "${SNBB_QSIRECON_OUTPUT_DIR}"

# Build optional bind mounts and flags
EXTRA_BINDS=()
EXTRA_ARGS=()

if [[ -n "${SNBB_RESPONSES_DIR}" ]]; then
    EXTRA_BINDS+=(--bind "${SNBB_RESPONSES_DIR}":"${SNBB_RESPONSES_DIR}":ro)
    EXTRA_ARGS+=(--recon-spec-aux-files "${SNBB_RESPONSES_DIR}")
fi

if [[ -n "${SNBB_ATLASES_DIR}" ]]; then
    EXTRA_BINDS+=(--bind "${SNBB_ATLASES_DIR}":"${SNBB_ATLASES_DIR}":ro)
    EXTRA_ARGS+=(--datasets "atlases=${SNBB_ATLASES_DIR}")
fi

if [[ -n "${SNBB_ATLASES}" ]]; then
    # shellcheck disable=SC2206
    EXTRA_ARGS+=(--atlases ${SNBB_ATLASES})
fi

apptainer run --cleanenv \
    --bind "${SNBB_QSIPREP_DIR}":"${SNBB_QSIPREP_DIR}":ro \
    --bind "${SNBB_QSIRECON_OUTPUT_DIR}":"${SNBB_QSIRECON_OUTPUT_DIR}" \
    --bind "${SNBB_FS_LICENSE}":"${SNBB_FS_LICENSE}":ro \
    --bind "${SNBB_FS_SUBJECTS_DIR}":"${SNBB_FS_SUBJECTS_DIR}":ro \
    --bind "${SNBB_RECON_SPEC}":"${SNBB_RECON_SPEC}":ro \
    --bind "${SNBB_WORK_DIR}":"${SNBB_WORK_DIR}" \
    "${EXTRA_BINDS[@]}" \
    "${SNBB_QSIRECON_SIF}" \
    "${SNBB_QSIPREP_DIR}" \
    "${SNBB_QSIRECON_OUTPUT_DIR}" \
    participant \
    --participant-label "${PARTICIPANT}" \
    --session-id "${SESSION_ID}" \
    --recon-spec "${SNBB_RECON_SPEC}" \
    --fs-license-file "${SNBB_FS_LICENSE}" \
    --fs-subjects-dir "${SNBB_FS_SUBJECTS_DIR}" \
    --nprocs "${SLURM_CPUS_PER_TASK:-8}" \
    --mem-mb "${SLURM_MEM_PER_NODE:-32000}" \
    --work-dir "${SNBB_WORK_DIR}" \
    "${EXTRA_ARGS[@]}"

#!/usr/bin/env bash
# snbb_run_qsiparc.sh — QSIParc diffusion parcellation via Apptainer
# Called by the snbb_scheduler as:  sbatch ... snbb_run_qsiparc.sh sub-XXXX ses-YY
# (session-scoped: parcellates QSIRecon outputs for a single subject+session)
#
# ── Site configuration ────────────────────────────────────────────────────────
# Edit the values below for your cluster, or set the env vars before submitting.
SNBB_QSIRECON_DIR="${SNBB_QSIRECON_DIR:-/media/storage/yalab-dev/snbb_scheduler/derivatives/qsirecon}"
SNBB_TABULAR_OUTPUT_DIR="${SNBB_TABULAR_OUTPUT_DIR:-/media/storage/yalab-dev/snbb_scheduler/derivatives/tabular}"
SNBB_FS_LICENSE="${SNBB_FS_LICENSE:-/home/galkepler/misc/freesurfer/license.txt}"
SNBB_FS_SUBJECTS_DIR="${SNBB_FS_SUBJECTS_DIR:-/media/storage/yalab-dev/snbb_scheduler/derivatives/freesurfer}"
SNBB_QSIPARC_SIF="${SNBB_QSIPARC_SIF:-/media/storage/apptainer/images/qsiparc-0.1.1.sif}"
SNBB_DEBUG_LOG="${SNBB_DEBUG_LOG:-/media/storage/yalab-dev/snbb_scheduler/logs/qsiparc/debug_submit.log}"
# Space-separated atlas names to parcellate. Leave empty to use QSIParc defaults.
SNBB_ATLASES="${SNBB_ATLASES:-\
  4S456Parcels \
  Schaefer2018N400n7Tian2020S2 \
  Brainnetome246Ext Gordon333Ext \
  HCPex}"
# Optional: root of local scratch on compute nodes.
# When set, QSIRecon subject input is staged locally, output is written locally,
# then rsynced to the remote destination on success.
# Leave empty (default) to use remote paths directly.
SNBB_LOCAL_TMP_ROOT="${SNBB_LOCAL_TMP_ROOT:-}"
# ─────────────────────────────────────────────────────────────────────────────

#SBATCH --time=12:00:00
#SBATCH --mem=32G
#SBATCH --cpus-per-task=8

set -euo pipefail

SUBJECT="$1"          # e.g. sub-0001
PARTICIPANT="${SUBJECT#sub-}"
SESSION="$2"          # e.g. ses-01
SESSION_ID="${SESSION#ses-}"

# ── Diagnostics ──────────────────────────────────────────────────────────────
mkdir -p "$(dirname "${SNBB_DEBUG_LOG}")"
{
    echo "=== $(date -Iseconds) | Job ${SLURM_JOB_ID:-local} | ${SUBJECT} ${SESSION} ==="
    echo "SNBB_QSIRECON_DIR:       ${SNBB_QSIRECON_DIR}"
    echo "SNBB_TABULAR_OUTPUT_DIR: ${SNBB_TABULAR_OUTPUT_DIR}"
    echo "SNBB_FS_LICENSE:         ${SNBB_FS_LICENSE}"
    echo "SNBB_FS_SUBJECTS_DIR:    ${SNBB_FS_SUBJECTS_DIR}"
    echo "SNBB_QSIPARC_SIF:        ${SNBB_QSIPARC_SIF}"
    echo "SNBB_ATLASES:            ${SNBB_ATLASES:-<unset>}"
    echo "SNBB_LOCAL_TMP_ROOT:     ${SNBB_LOCAL_TMP_ROOT:-<unset>}"
    echo "SLURM_CPUS_PER_TASK:     ${SLURM_CPUS_PER_TASK:-<unset>}"
    echo "PATH:                    ${PATH}"
} >> "${SNBB_DEBUG_LOG}" 2>&1
# ─────────────────────────────────────────────────────────────────────────────

# Build optional atlas flags (shared between both code paths).
EXTRA_ARGS=()
if [[ -n "${SNBB_ATLASES}" ]]; then
    # shellcheck disable=SC2206
    for atlas in ${SNBB_ATLASES}; do
        EXTRA_ARGS+=("-a" "${atlas}")
    done
fi

if [[ -n "${SNBB_LOCAL_TMP_ROOT}" ]]; then
    # ── Local-scratch mode ────────────────────────────────────────────────────
    # Stage QSIRecon subject output on the compute node's local disk.
    # QSIParc output is written locally and rsynced back on success.
    # EXIT trap cleans up the local workdir on any exit (success/error/SIGTERM),
    # except when rsync-out fails — in that case local output is preserved.

    LOCAL_WORKDIR="${SNBB_LOCAL_TMP_ROOT}/snbb_${SLURM_JOB_ID:-$$}_${SUBJECT}_${SESSION}"
    LOCAL_QSIRECON="${LOCAL_WORKDIR}/qsirecon"
    LOCAL_OUTPUT="${LOCAL_WORKDIR}/output"
    CLEANUP_ON_EXIT=true

    _cleanup() {
        if [[ "${CLEANUP_ON_EXIT}" == "true" ]]; then
            echo "Cleaning up local workdir: ${LOCAL_WORKDIR}" >&2
            rm -rf "${LOCAL_WORKDIR}"
        else
            echo "Preserving local workdir for recovery: ${LOCAL_WORKDIR}" >&2
        fi
    }
    trap _cleanup EXIT

    # Create local directory structure
    mkdir -p "${LOCAL_QSIRECON}" "${LOCAL_OUTPUT}"

    # Copy QSIRecon subject output + dataset_description.json (required by QSIParc)
    rsync -a "${SNBB_QSIRECON_DIR}/${SUBJECT}/" "${LOCAL_QSIRECON}/${SUBJECT}/"
    [[ -e "${SNBB_QSIRECON_DIR}/dataset_description.json" ]] && \
        rsync -a "${SNBB_QSIRECON_DIR}/dataset_description.json" "${LOCAL_QSIRECON}/dataset_description.json"

    mkdir -p "${SNBB_TABULAR_OUTPUT_DIR}"

    apptainer run --cleanenv \
        --bind "${LOCAL_QSIRECON}":"${LOCAL_QSIRECON}":ro \
        --bind "${LOCAL_OUTPUT}":"${LOCAL_OUTPUT}" \
        --bind "${SNBB_FS_SUBJECTS_DIR}":"${SNBB_FS_SUBJECTS_DIR}":ro \
        --bind "${SNBB_FS_LICENSE}":"${SNBB_FS_LICENSE}":ro \
        "${SNBB_QSIPARC_SIF}" \
        "${LOCAL_QSIRECON}" \
        "${LOCAL_OUTPUT}" \
        -p "${PARTICIPANT}" \
        -s "${SESSION_ID}" \
        -n "${SLURM_CPUS_PER_TASK:-1}" \
        "${EXTRA_ARGS[@]}"

    # Rsync results to remote destination; if this fails, preserve local output
    CLEANUP_ON_EXIT=false
    rsync -av "${LOCAL_OUTPUT}/" "${SNBB_TABULAR_OUTPUT_DIR}/" || {
        echo "ERROR: rsync to remote destination failed. Local output preserved at ${LOCAL_OUTPUT}" >&2
        exit 1
    }
    CLEANUP_ON_EXIT=true
    # ─────────────────────────────────────────────────────────────────────────
else
    # ── Original behaviour (remote filesystem) ────────────────────────────────
    mkdir -p "${SNBB_TABULAR_OUTPUT_DIR}"

    apptainer run --cleanenv \
        --bind "${SNBB_QSIRECON_DIR}":"${SNBB_QSIRECON_DIR}":ro \
        --bind "${SNBB_TABULAR_OUTPUT_DIR}":"${SNBB_TABULAR_OUTPUT_DIR}" \
        --bind "${SNBB_FS_LICENSE}":"${SNBB_FS_LICENSE}":ro \
        --bind "${SNBB_FS_SUBJECTS_DIR}":"${SNBB_FS_SUBJECTS_DIR}":ro \
        "${SNBB_QSIPARC_SIF}" \
        "${SNBB_QSIRECON_DIR}" \
        "${SNBB_TABULAR_OUTPUT_DIR}" \
        -p "${PARTICIPANT}" \
        -s "${SESSION_ID}" \
        -n "${SLURM_CPUS_PER_TASK:-1}" \
        "${EXTRA_ARGS[@]}"
    # ─────────────────────────────────────────────────────────────────────────
fi

#!/usr/bin/env bash
# snbb_run_fsatlas.sh — FreeSurfer atlas extraction via Apptainer
# Called by the snbb_scheduler as:  sbatch ... snbb_run_fsatlas.sh sub-XXXX
# (subject-scoped: processes all sessions' FreeSurfer output for a subject)
#
# ── Site configuration ────────────────────────────────────────────────────────
# Edit the values below for your cluster, or set the env vars before submitting.
SNBB_TABULAR_OUTPUT_DIR="${SNBB_TABULAR_OUTPUT_DIR:-/media/storage/yalab-dev/snbb_scheduler/derivatives/tabular}"
SNBB_FS_LICENSE="${SNBB_FS_LICENSE:-/home/galkepler/misc/freesurfer/license.txt}"
SNBB_FS_SUBJECTS_DIR="${SNBB_FS_SUBJECTS_DIR:-/media/storage/yalab-dev/snbb_scheduler/derivatives/freesurfer}"
SNBB_FSATLAS_SIF="${SNBB_FSATLAS_SIF:-/media/storage/apptainer/images/fsatlas-0.1.0.sif}"
SNBB_DEBUG_LOG="${SNBB_DEBUG_LOG:-/media/storage/yalab-dev/snbb_scheduler/logs/fsatlas/debug_submit.log}"
# Space-separated atlas names to extract. Leave empty to use fsatlas defaults.
# Example: SNBB_ATLASES="schaefer400-7 tian-s2 BN_Atlas gordon333"
SNBB_ATLASES="${SNBB_ATLASES:-\
  schaefer400-7 tian-s2 \
  BN_Atlas BN_Atlas_subcortex gordon333 gordon333_subcortical \
  hcp-mmp hcpex_subcortical}"
# Optional: root of local scratch on compute nodes.
# When set, FreeSurfer subject data is staged locally, output is written locally,
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

# ── Diagnostics ──────────────────────────────────────────────────────────────
mkdir -p "$(dirname "${SNBB_DEBUG_LOG}")"
{
    echo "=== $(date -Iseconds) | Job ${SLURM_JOB_ID:-local} | ${SUBJECT} ==="
    echo "SNBB_FSATLAS_SIF:        ${SNBB_FSATLAS_SIF}"
    echo "SNBB_TABULAR_OUTPUT_DIR: ${SNBB_TABULAR_OUTPUT_DIR}"
    echo "SNBB_FS_LICENSE:         ${SNBB_FS_LICENSE}"
    echo "SNBB_FS_SUBJECTS_DIR:    ${SNBB_FS_SUBJECTS_DIR}"
    echo "SNBB_ATLASES:            ${SNBB_ATLASES:-<unset>}"
    echo "SNBB_LOCAL_TMP_ROOT:     ${SNBB_LOCAL_TMP_ROOT:-<unset>}"
    echo "PATH:                    ${PATH}"
} >> "${SNBB_DEBUG_LOG}" 2>&1
# ─────────────────────────────────────────────────────────────────────────────

# Build optional atlas flags (shared between both code paths).
EXTRA_ARGS=()
if [[ -n "${SNBB_ATLASES}" ]]; then
    # shellcheck disable=SC2206
    for atlas in ${SNBB_ATLASES}; do
        EXTRA_ARGS+=("--atlas" "${atlas}")
    done
fi

if [[ -n "${SNBB_LOCAL_TMP_ROOT}" ]]; then
    # ── Local-scratch mode ────────────────────────────────────────────────────
    # Stage FreeSurfer subject data on the compute node's local disk.
    # fsatlas output is written locally and rsynced back on success.
    # EXIT trap cleans up the local workdir on any exit (success/error/SIGTERM),
    # except when rsync-out fails — in that case local output is preserved.

    LOCAL_WORKDIR="${SNBB_LOCAL_TMP_ROOT}/snbb_${SLURM_JOB_ID:-$$}_${SUBJECT}"
    LOCAL_FS="${LOCAL_WORKDIR}/freesurfer"
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
    mkdir -p "${LOCAL_FS}" "${LOCAL_OUTPUT}"

    # Copy FreeSurfer subject directory
    rsync -a "${SNBB_FS_SUBJECTS_DIR}/${SUBJECT}/" "${LOCAL_FS}/${SUBJECT}/"

    mkdir -p "${SNBB_TABULAR_OUTPUT_DIR}"

    apptainer run --cleanenv \
        --bind "${LOCAL_FS}":"${LOCAL_FS}":ro \
        --bind "${LOCAL_OUTPUT}":"${LOCAL_OUTPUT}" \
        --bind "${SNBB_FS_LICENSE}":"${SNBB_FS_LICENSE}":ro \
        --env SUBJECTS_DIR="${LOCAL_FS}" \
        "${SNBB_FSATLAS_SIF}" \
        --freesurfer-license-file "${SNBB_FS_LICENSE}" \
        extract \
        -d "${LOCAL_FS}" \
        --output-dir "${LOCAL_OUTPUT}" \
        --subjects "${SUBJECT}" \
        --output-layout bids \
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
        --bind "${SNBB_TABULAR_OUTPUT_DIR}":"${SNBB_TABULAR_OUTPUT_DIR}" \
        --bind "${SNBB_FS_LICENSE}":"${SNBB_FS_LICENSE}":ro \
        --bind "${SNBB_FS_SUBJECTS_DIR}":"${SNBB_FS_SUBJECTS_DIR}":ro \
        --env SUBJECTS_DIR="${SNBB_FS_SUBJECTS_DIR}" \
        "${SNBB_FSATLAS_SIF}" \
        --freesurfer-license-file "${SNBB_FS_LICENSE}" \
        extract \
        -d "${SNBB_FS_SUBJECTS_DIR}" \
        --output-dir "${SNBB_TABULAR_OUTPUT_DIR}" \
        --subjects "${SUBJECT}" \
        --output-layout bids \
        "${EXTRA_ARGS[@]}"
    # ─────────────────────────────────────────────────────────────────────────
fi

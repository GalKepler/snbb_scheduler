#!/usr/bin/env bash
# snbb_run_qsirecon.sh — QSIRecon diffusion reconstruction via Apptainer
# Called by the snbb_scheduler as:  sbatch ... snbb_run_qsirecon.sh sub-XXXX ses-YY
#
# ── Site configuration ────────────────────────────────────────────────────────
# Edit the values below for your cluster, or set the env vars before submitting.
# SNBB_DERIVATIVES="${SNBB_DERIVATIVES:-/media/storage/yalab-dev/snbb_scheduler/derivatives/qsiprep}"
SNBB_QSIPREP_DIR="${SNBB_QSIPREP_DIR:-/media/storage/yalab-dev/snbb_scheduler/derivatives/qsiprep}"
SNBB_QSIRECON_OUTPUT_DIR="${SNBB_QSIRECON_OUTPUT_DIR:-/media/storage/yalab-dev/snbb_scheduler/derivatives/qsirecon}"
SNBB_FS_LICENSE="${SNBB_FS_LICENSE:-/home/galkepler/misc/freesurfer/license.txt}"
SNBB_FS_SUBJECTS_DIR="${SNBB_FS_SUBJECTS_DIR:-/media/storage/yalab-dev/snbb_scheduler/derivatives/freesurfer}"
SNBB_RECON_SPEC="${SNBB_RECON_SPEC:-/home/galkepler/Projects/snbb_scheduler/scripts/qsirecon_full_spec.yaml}"
SNBB_WORK_DIR="${SNBB_WORK_DIR:-/media/storage/yalab-dev/snbb_scheduler/work/qsirecon}"
SNBB_QSIRECON_SIF="${SNBB_QSIRECON_SIF:-/media/storage/apptainer/images/qsirecon-1.2.0.sif}"
SNBB_DEBUG_LOG="${SNBB_DEBUG_LOG:-/media/storage/yalab-dev/snbb_scheduler/logs/qsirecon/debug_submit.log}"
# Optional: directory of pre-computed response functions (--recon-spec-aux-files)
SNBB_RESPONSES_DIR="${SNBB_RESPONSES_DIR:-/media/storage/yalab-dev/qsiprep_test/derivatives/responses}"
SNBB_TEMPLATEFLOW_HOME="${SNBB_TEMPLATEFLOW_HOME:-/media/storage/yalab-dev/snbb_scheduler/templateflow}"

# Optional: atlas dataset directory and space-separated atlas names
# Example: SNBB_ATLASES_DIR=/data/atlases  SNBB_ATLASES="4S156Parcels Schaefer2018N100n7Tian2020S1"
SNBB_ATLASES_DIR="${SNBB_ATLASES_DIR:-/media/storage/yalab-dev/voxelops/Schaefer2018Tian2020_atlases}"
SNBB_ATLASES="${SNBB_ATLASES:-4S156Parcels Schaefer2018N100n7Tian2020S1}"
# Optional: root of local scratch on compute nodes.
# When set, QSIPrep and FreeSurfer subject inputs are staged locally and
# QSIRecon output is written locally, then rsynced to the remote destination.
# SNBB_RESPONSES_DIR and SNBB_ATLASES_DIR are always kept on remote (read-only,
# shared across jobs). Leave empty (default) to use remote paths directly.
SNBB_LOCAL_TMP_ROOT="${SNBB_LOCAL_TMP_ROOT:-}"
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
    echo "SNBB_QSIPREP_DIR:         ${SNBB_QSIPREP_DIR}"
    echo "SNBB_QSIRECON_OUTPUT_DIR: ${SNBB_QSIRECON_OUTPUT_DIR}"
    echo "SNBB_FS_LICENSE:          ${SNBB_FS_LICENSE}"
    echo "SNBB_FS_SUBJECTS_DIR:     ${SNBB_FS_SUBJECTS_DIR}"
    echo "SNBB_RECON_SPEC:          ${SNBB_RECON_SPEC}"
    echo "SNBB_WORK_DIR:            ${SNBB_WORK_DIR}"
    echo "SNBB_QSIRECON_SIF:        ${SNBB_QSIRECON_SIF}"
    echo "SNBB_RESPONSES_DIR:       ${SNBB_RESPONSES_DIR}"
    echo "SNBB_ATLASES_DIR:         ${SNBB_ATLASES_DIR}"
    echo "SNBB_ATLASES:             ${SNBB_ATLASES}"
    echo "SNBB_LOCAL_TMP_ROOT:      ${SNBB_LOCAL_TMP_ROOT:-<unset>}"
    echo "PATH:                     ${PATH}"
} >> "${SNBB_DEBUG_LOG}" 2>&1
# ─────────────────────────────────────────────────────────────────────────────

# Build optional bind mounts and flags (shared between both code paths).
# SNBB_RESPONSES_DIR and SNBB_ATLASES_DIR always bind from their remote paths
# — they are read-only shared datasets and do not need to be staged locally.
EXTRA_BINDS=()
EXTRA_ARGS=()

if [[ -n "${SNBB_RESPONSES_DIR}" ]]; then
    EXTRA_BINDS+=(--bind "${SNBB_RESPONSES_DIR}":/responses:ro)
    EXTRA_ARGS+=(--recon-spec-aux-files /responses)
fi

if [[ -n "${SNBB_ATLASES_DIR}" ]]; then
    EXTRA_BINDS+=(--bind "${SNBB_ATLASES_DIR}":"${SNBB_ATLASES_DIR}":ro)
    EXTRA_ARGS+=(--datasets "atlases=${SNBB_ATLASES_DIR}")
fi

if [[ -n "${SNBB_ATLASES}" ]]; then
    # shellcheck disable=SC2206
    EXTRA_ARGS+=(--atlases ${SNBB_ATLASES})
fi

if [[ -n "${SNBB_LOCAL_TMP_ROOT}" ]]; then
    # ── Local-scratch mode ────────────────────────────────────────────────────
    # Stage QSIPrep and FreeSurfer subject data on the compute node's local disk.
    # QSIRecon output is also written locally and rsynced back on success.
    # EXIT trap cleans up the local workdir on any exit (success/error/SIGTERM),
    # except when rsync-out fails — in that case local output is preserved.

    LOCAL_WORKDIR="${SNBB_LOCAL_TMP_ROOT}/snbb_${SLURM_JOB_ID:-$$}_${SUBJECT}"
    LOCAL_QSIPREP="${LOCAL_WORKDIR}/qsiprep"
    LOCAL_FS="${LOCAL_WORKDIR}/freesurfer"
    LOCAL_OUTPUT="${LOCAL_WORKDIR}/output"
    LOCAL_WORK="${LOCAL_WORKDIR}/work"
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
    mkdir -p "${LOCAL_QSIPREP}" "${LOCAL_FS}" "${LOCAL_OUTPUT}" "${LOCAL_WORK}"

    # Copy QSIPrep subject output + dataset_description.json (required by QSIRecon)
    rsync -a "${SNBB_QSIPREP_DIR}/${SUBJECT}/" "${LOCAL_QSIPREP}/${SUBJECT}/"
    [[ -e "${SNBB_QSIPREP_DIR}/dataset_description.json" ]] && \
        rsync -a "${SNBB_QSIPREP_DIR}/dataset_description.json" "${LOCAL_QSIPREP}/dataset_description.json"

    # Copy FreeSurfer subject output
    rsync -a "${SNBB_FS_SUBJECTS_DIR}/${SUBJECT}/" "${LOCAL_FS}/${SUBJECT}/"

    mkdir -p "${SNBB_QSIRECON_OUTPUT_DIR}"

    apptainer run --cleanenv \
        --bind "${LOCAL_QSIPREP}":"${LOCAL_QSIPREP}":ro \
        --bind "${LOCAL_OUTPUT}":"${LOCAL_OUTPUT}" \
        --bind "${LOCAL_FS}":"${LOCAL_FS}":ro \
        --bind "${SNBB_FS_LICENSE}":"${SNBB_FS_LICENSE}":ro \
        --bind "${SNBB_RECON_SPEC}":"${SNBB_RECON_SPEC}":ro \
        --bind "${LOCAL_WORK}":"${LOCAL_WORK}" \
        --bind "${SNBB_TEMPLATEFLOW_HOME}":"${SNBB_TEMPLATEFLOW_HOME}" \
        --env TEMPLATEFLOW_HOME="${SNBB_TEMPLATEFLOW_HOME}" \
        "${EXTRA_BINDS[@]}" \
        "${SNBB_QSIRECON_SIF}" \
        "${LOCAL_QSIPREP}" \
        "${LOCAL_OUTPUT}" \
        participant \
        --participant-label "${PARTICIPANT}" \
        --session-id "${SESSION_ID}" \
        --recon-spec "${SNBB_RECON_SPEC}" \
        --fs-license-file "${SNBB_FS_LICENSE}" \
        --fs-subjects-dir "${LOCAL_FS}" \
        --nprocs "${SLURM_CPUS_PER_TASK:-8}" \
        --mem-mb "${SLURM_MEM_PER_NODE:-32000}" \
        --work-dir "${LOCAL_WORK}" \
        "${EXTRA_ARGS[@]}"

    # Rsync results to remote destination; if this fails, preserve local output
    CLEANUP_ON_EXIT=false
    rsync -av "${LOCAL_OUTPUT}/" "${SNBB_QSIRECON_OUTPUT_DIR}/" || {
        echo "ERROR: rsync to remote destination failed. Local output preserved at ${LOCAL_OUTPUT}" >&2
        exit 1
    }
    CLEANUP_ON_EXIT=true
    # ─────────────────────────────────────────────────────────────────────────
else
    # ── Original behaviour (remote filesystem) ────────────────────────────────
    mkdir -p "${SNBB_WORK_DIR}" "${SNBB_QSIRECON_OUTPUT_DIR}"

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
    # ─────────────────────────────────────────────────────────────────────────
fi

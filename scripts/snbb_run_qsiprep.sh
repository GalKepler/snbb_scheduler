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
SNBB_TEMPLATEFLOW_HOME="${SNBB_TEMPLATEFLOW_HOME:-/media/storage/yalab-dev/snbb_scheduler/templateflow}"
# Optional: root of local scratch on compute nodes.
# When set, subject BIDS input and QSIPrep output are staged locally,
# then rsynced back to the remote destination on success.
# Leave empty (default) to use remote paths directly.
SNBB_LOCAL_TMP_ROOT="${SNBB_LOCAL_TMP_ROOT:-}"
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
    echo "SNBB_LOCAL_TMP_ROOT:      ${SNBB_LOCAL_TMP_ROOT:-<unset>}"
    echo "PATH:                     ${PATH}"
} >> "${SNBB_DEBUG_LOG}" 2>&1
# ─────────────────────────────────────────────────────────────────────────────

# Build optional bind mounts and flags (shared between both code paths)
EXTRA_BINDS=()
EXTRA_ARGS=()
if [[ -n "${SNBB_BIDS_FILTER_FILE}" ]]; then
    EXTRA_BINDS+=(--bind "${SNBB_BIDS_FILTER_FILE}":"${SNBB_BIDS_FILTER_FILE}":ro)
    EXTRA_ARGS+=(--bids-filter-file "${SNBB_BIDS_FILTER_FILE}")
fi

if [[ -n "${SNBB_LOCAL_TMP_ROOT}" ]]; then
    # ── Local-scratch mode ────────────────────────────────────────────────────
    # Stage BIDS input and QSIPrep output on the compute node's local disk.
    # On any exit (success, error, or SIGTERM) the local workdir is cleaned up,
    # except when rsync-out fails — in that case the local output is preserved
    # for manual recovery.

    LOCAL_WORKDIR="${SNBB_LOCAL_TMP_ROOT}/snbb_${SLURM_JOB_ID:-$$}_${SUBJECT}"
    LOCAL_BIDS="${LOCAL_WORKDIR}/bids"
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
    mkdir -p "${LOCAL_BIDS}" "${LOCAL_OUTPUT}" "${LOCAL_WORK}"

    # Copy subject BIDS data + BIDS root metadata files
    rsync -a "${SNBB_BIDS_ROOT}/${SUBJECT}/" "${LOCAL_BIDS}/${SUBJECT}/"
    for f in dataset_description.json README README.md .bidsignore participants.tsv participants.json; do
        [[ -e "${SNBB_BIDS_ROOT}/${f}" ]] && rsync -a "${SNBB_BIDS_ROOT}/${f}" "${LOCAL_BIDS}/${f}"
    done

    mkdir -p "${SNBB_DERIVATIVES}"

    apptainer run --no-home --writable-tmpfs --containall --cleanenv \
        --bind "${LOCAL_BIDS}":"${LOCAL_BIDS}":ro \
        --bind "${LOCAL_OUTPUT}":"${LOCAL_OUTPUT}" \
        --bind "${LOCAL_WORK}":"${LOCAL_WORK}" \
        --bind "${SNBB_FS_LICENSE}":"${SNBB_FS_LICENSE}":ro \
        --bind "${SNBB_TEMPLATEFLOW_HOME}":"${SNBB_TEMPLATEFLOW_HOME}" \
        --env TEMPLATEFLOW_HOME="${SNBB_TEMPLATEFLOW_HOME}" \
        "${EXTRA_BINDS[@]}" \
        "${SNBB_QSIPREP_SIF}" \
        "${LOCAL_BIDS}" \
        "${LOCAL_OUTPUT}" \
        participant \
        --participant-label "${PARTICIPANT}" \
        --fs-license-file "${SNBB_FS_LICENSE}" \
        --nprocs "${SLURM_CPUS_PER_TASK:-8}" \
        --mem-mb "${SLURM_MEM_PER_NODE:-16000}" \
        --work-dir "${LOCAL_WORK}" \
        --output-resolution 1.6 \
        --anatomical-template "${SNBB_ANATOMICAL_TEMPLATE}" \
        --subject-anatomical-reference "${SNBB_SUBJECT_ANAT_REF}" \
        "${EXTRA_ARGS[@]}"

    # Rsync results to remote destination; if this fails, preserve local output
    CLEANUP_ON_EXIT=false
    rsync -av "${LOCAL_OUTPUT}/" "${SNBB_DERIVATIVES}/" || {
        echo "ERROR: rsync to remote destination failed. Local output preserved at ${LOCAL_OUTPUT}" >&2
        exit 1
    }
    CLEANUP_ON_EXIT=true
    # ─────────────────────────────────────────────────────────────────────────
else
    # ── Original behaviour (remote filesystem) ────────────────────────────────
    mkdir -p "${SNBB_WORK_DIR}"

    apptainer run --no-home --writable-tmpfs --containall --cleanenv \
        --bind "${SNBB_BIDS_ROOT}":"${SNBB_BIDS_ROOT}":ro \
        --bind "${SNBB_DERIVATIVES}":"${SNBB_DERIVATIVES}" \
        --bind "${SNBB_FS_LICENSE}":"${SNBB_FS_LICENSE}":ro \
        --bind "${SNBB_WORK_DIR}":"${SNBB_WORK_DIR}" \
        --bind "${SNBB_TEMPLATEFLOW_HOME}":"${SNBB_TEMPLATEFLOW_HOME}" \
        --env TEMPLATEFLOW_HOME="${SNBB_TEMPLATEFLOW_HOME}" \
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
        "${EXTRA_ARGS[@]}" \
        # --subject-anatomical-reference "${SNBB_SUBJECT_ANAT_REF}" \
        
    # ─────────────────────────────────────────────────────────────────────────
fi

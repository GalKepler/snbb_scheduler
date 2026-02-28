#!/usr/bin/env bash
# snbb_run_fastsurfer_cross.sh — Cross-sectional FastSurfer via Apptainer
# Called by snbb_scheduler as:  sbatch ... snbb_run_fastsurfer_cross.sh sub-XXXX ses-YY
#
# Runs FastSurfer on a single session's T1w image.  Each session is
# processed independently; the results feed into the template-creation
# step (snbb_run_fastsurfer_template.sh).
#
# Output directory: SNBB_FASTSURFER_OUTPUT/<subject>_<session>/
#
# ── Site configuration ────────────────────────────────────────────────────────
# Edit the defaults below for your cluster, or export the env vars before
# submitting the job.
SNBB_BIDS_ROOT="${SNBB_BIDS_ROOT:-/media/storage/yalab-dev/snbb_scheduler/bids}"
SNBB_FASTSURFER_OUTPUT="${SNBB_FASTSURFER_OUTPUT:-/media/storage/yalab-dev/snbb_scheduler/derivatives/fastsurfer}"
SNBB_FS_LICENSE="${SNBB_FS_LICENSE:-/home/galkepler/misc/freesurfer/license.txt}"
SNBB_FASTSURFER_SIF="${SNBB_FASTSURFER_SIF:-/media/storage/apptainer/images/fastsurfer-latest.sif}"
SNBB_DEBUG_LOG="${SNBB_DEBUG_LOG:-/media/storage/yalab-dev/snbb_scheduler/logs/fastsurfer_cross/debug_submit.log}"
# Optional: root of local scratch on compute nodes.
# When set, BIDS input and FastSurfer output are staged locally,
# then rsynced back on success.  Leave empty to use remote paths directly.
SNBB_LOCAL_TMP_ROOT="${SNBB_LOCAL_TMP_ROOT:-}"
# ─────────────────────────────────────────────────────────────────────────────

#SBATCH --time=12:00:00
#SBATCH --mem=16G
#SBATCH --cpus-per-task=8

set -euo pipefail

SUBJECT="$1"   # e.g. sub-0001
SESSION="$2"   # e.g. ses-01

# ── Diagnostics ──────────────────────────────────────────────────────────────
mkdir -p "$(dirname "${SNBB_DEBUG_LOG}")"
{
    echo "=== $(date -Iseconds) | Job ${SLURM_JOB_ID:-local} | ${SUBJECT} ${SESSION} ==="
    echo "SNBB_BIDS_ROOT:         ${SNBB_BIDS_ROOT}"
    echo "SNBB_FASTSURFER_OUTPUT: ${SNBB_FASTSURFER_OUTPUT}"
    echo "SNBB_FS_LICENSE:        ${SNBB_FS_LICENSE}"
    echo "SNBB_FASTSURFER_SIF:    ${SNBB_FASTSURFER_SIF}"
    echo "SNBB_LOCAL_TMP_ROOT:    ${SNBB_LOCAL_TMP_ROOT:-<unset>}"
    echo "PATH:                   ${PATH}"
} >> "${SNBB_DEBUG_LOG}" 2>&1
# ─────────────────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ -n "${SNBB_LOCAL_TMP_ROOT}" ]]; then
    # ── Local-scratch mode ────────────────────────────────────────────────────
    LOCAL_WORKDIR="${SNBB_LOCAL_TMP_ROOT}/snbb_${SLURM_JOB_ID:-$$}_${SUBJECT}_${SESSION}"
    LOCAL_BIDS="${LOCAL_WORKDIR}/bids"
    LOCAL_FS_OUTPUT="${LOCAL_WORKDIR}/fastsurfer"
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

    mkdir -p "${LOCAL_BIDS}" "${LOCAL_FS_OUTPUT}"

    # Stage BIDS session data + root metadata files
    rsync -a "${SNBB_BIDS_ROOT}/${SUBJECT}/${SESSION}/" "${LOCAL_BIDS}/${SUBJECT}/${SESSION}/"
    for f in dataset_description.json README README.md .bidsignore participants.tsv participants.json; do
        [[ -e "${SNBB_BIDS_ROOT}/${f}" ]] && rsync -a "${SNBB_BIDS_ROOT}/${f}" "${LOCAL_BIDS}/${f}"
    done

    mkdir -p "${SNBB_FASTSURFER_OUTPUT}"
    python3 "${SCRIPT_DIR}/snbb_fastsurfer_helper.py" cross \
        --bids-dir    "${LOCAL_BIDS}" \
        --output-dir  "${LOCAL_FS_OUTPUT}" \
        --subject     "${SUBJECT}" \
        --session     "${SESSION}" \
        --sif         "${SNBB_FASTSURFER_SIF}" \
        --fs-license  "${SNBB_FS_LICENSE}" \
        --threads     "${SLURM_CPUS_PER_TASK:-8}"

    # Rsync subject_session output back; preserve local on failure
    SID="${SUBJECT}_${SESSION}"
    CLEANUP_ON_EXIT=false
    rsync -av "${LOCAL_FS_OUTPUT}/${SID}/" "${SNBB_FASTSURFER_OUTPUT}/${SID}/" || {
        echo "ERROR: rsync to remote destination failed. Local output preserved at ${LOCAL_FS_OUTPUT}/${SID}" >&2
        exit 1
    }
    CLEANUP_ON_EXIT=true
    # ─────────────────────────────────────────────────────────────────────────
else
    # ── Remote filesystem mode ────────────────────────────────────────────────
    mkdir -p "${SNBB_FASTSURFER_OUTPUT}"
    python3 "${SCRIPT_DIR}/snbb_fastsurfer_helper.py" cross \
        --bids-dir    "${SNBB_BIDS_ROOT}" \
        --output-dir  "${SNBB_FASTSURFER_OUTPUT}" \
        --subject     "${SUBJECT}" \
        --session     "${SESSION}" \
        --sif         "${SNBB_FASTSURFER_SIF}" \
        --fs-license  "${SNBB_FS_LICENSE}" \
        --threads     "${SLURM_CPUS_PER_TASK:-8}"
    # ─────────────────────────────────────────────────────────────────────────
fi

#!/usr/bin/env bash
# snbb_run_fastsurfer.sh — Unified FastSurfer pipeline via Apptainer
# Called by snbb_scheduler as:  sbatch ... snbb_run_fastsurfer.sh sub-XXXX
#
# Subject-scoped: one job per subject covers ALL sessions.
# The helper script auto-discovers sessions and runs either:
#   - cross-sectional FastSurfer  (1 session)
#   - long_fastsurfer.sh          (2+ sessions, full longitudinal pipeline)
#
# Output directory: SNBB_FASTSURFER_OUTPUT/<subject>/
# (SUBJECTS_DIR naming: ses-YY  or  ses-YY.long.sub-XXXX  inside the subject dir)
#
# ── Site configuration ────────────────────────────────────────────────────────
# Edit the defaults below for your cluster, or export the env vars before
# submitting the job.
SNBB_BIDS_ROOT="${SNBB_BIDS_ROOT:-/media/storage/yalab-dev/snbb_scheduler/bids}"
SNBB_FASTSURFER_OUTPUT="${SNBB_FASTSURFER_OUTPUT:-/media/storage/yalab-dev/snbb_scheduler/derivatives/fastsurfer}"
SNBB_FS_LICENSE="${SNBB_FS_LICENSE:-/home/galkepler/misc/freesurfer/license.txt}"
SNBB_FASTSURFER_SIF="${SNBB_FASTSURFER_SIF:-/media/storage/apptainer/images/fastsurfer-2.4.2.sif}"
SNBB_DEBUG_LOG="${SNBB_DEBUG_LOG:-/media/storage/yalab-dev/snbb_scheduler/logs/fastsurfer/debug_submit.log}"
# Optional: root of local scratch on compute nodes.
# When set, BIDS input and FastSurfer output are staged locally,
# then rsynced back on success.  Leave empty to use remote paths directly.
SNBB_LOCAL_TMP_ROOT="${SNBB_LOCAL_TMP_ROOT:-}"
# ─────────────────────────────────────────────────────────────────────────────

#SBATCH --time=24:00:00
#SBATCH --mem=16G
#SBATCH --cpus-per-task=8

set -euo pipefail

SUBJECT="$1"   # e.g. sub-0001

# ── Diagnostics ──────────────────────────────────────────────────────────────
mkdir -p "$(dirname "${SNBB_DEBUG_LOG}")"
{
    echo "=== $(date -Iseconds) | Job ${SLURM_JOB_ID:-local} | ${SUBJECT} ==="
    echo "SNBB_BIDS_ROOT:         ${SNBB_BIDS_ROOT}"
    echo "SNBB_FASTSURFER_OUTPUT: ${SNBB_FASTSURFER_OUTPUT}"
    echo "SNBB_FS_LICENSE:        ${SNBB_FS_LICENSE}"
    echo "SNBB_FASTSURFER_SIF:    ${SNBB_FASTSURFER_SIF}"
    echo "SNBB_LOCAL_TMP_ROOT:    ${SNBB_LOCAL_TMP_ROOT:-<unset>}"
    echo "PATH:                   ${PATH}"
} >> "${SNBB_DEBUG_LOG}" 2>&1
# ─────────────────────────────────────────────────────────────────────────────

# SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_DIR=/home/galkepler/Projects/snbb_scheduler/scripts

if [[ -n "${SNBB_LOCAL_TMP_ROOT}" ]]; then
    # ── Local-scratch mode ────────────────────────────────────────────────────
    LOCAL_WORKDIR="${SNBB_LOCAL_TMP_ROOT}/snbb_${SLURM_JOB_ID:-$$}_${SUBJECT}"
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

    # Stage ALL BIDS sessions for the subject + root metadata files
    rsync -a "${SNBB_BIDS_ROOT}/${SUBJECT}/" "${LOCAL_BIDS}/${SUBJECT}/"
    for f in dataset_description.json README README.md .bidsignore participants.tsv participants.json; do
        [[ -e "${SNBB_BIDS_ROOT}/${f}" ]] && rsync -a "${SNBB_BIDS_ROOT}/${f}" "${LOCAL_BIDS}/${f}"
    done

    mkdir -p "${SNBB_FASTSURFER_OUTPUT}"
    python3 "${SCRIPT_DIR}/snbb_fastsurfer_helper.py" \
        --bids-dir    "${LOCAL_BIDS}" \
        --output-dir  "${LOCAL_FS_OUTPUT}" \
        --subject     "${SUBJECT}" \
        --sif         "${SNBB_FASTSURFER_SIF}" \
        --fs-license  "${SNBB_FS_LICENSE}" \
        --threads     "${SLURM_CPUS_PER_TASK:-8}"

    # Rsync the subject subdirectory back as a single unit
    CLEANUP_ON_EXIT=false
    rsync -av "${LOCAL_FS_OUTPUT}/${SUBJECT}/" "${SNBB_FASTSURFER_OUTPUT}/${SUBJECT}/" || {
        echo "ERROR: rsync failed for ${SUBJECT}. Local output preserved at ${LOCAL_FS_OUTPUT}/${SUBJECT}" >&2
        exit 1
    }
    CLEANUP_ON_EXIT=true
    # ─────────────────────────────────────────────────────────────────────────
else
    # ── Remote filesystem mode ────────────────────────────────────────────────
    mkdir -p "${SNBB_FASTSURFER_OUTPUT}"
    python3 "${SCRIPT_DIR}/snbb_fastsurfer_helper.py" \
        --bids-dir    "${SNBB_BIDS_ROOT}" \
        --output-dir  "${SNBB_FASTSURFER_OUTPUT}" \
        --subject     "${SUBJECT}" \
        --sif         "${SNBB_FASTSURFER_SIF}" \
        --fs-license  "${SNBB_FS_LICENSE}" \
        --threads     "${SLURM_CPUS_PER_TASK:-8}"
    # ─────────────────────────────────────────────────────────────────────────
fi

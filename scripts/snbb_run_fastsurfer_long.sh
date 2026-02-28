#!/usr/bin/env bash
# snbb_run_fastsurfer_long.sh — Longitudinal FreeSurfer refinement via Apptainer
# Called by snbb_scheduler as:  sbatch ... snbb_run_fastsurfer_long.sh sub-XXXX ses-YY
#
# Applies FreeSurfer's recon-all -long to each timepoint using the
# within-subject template produced by snbb_run_fastsurfer_template.sh.
#
# Prerequisites: snbb_run_fastsurfer_template.sh must have completed for
# the subject before this script runs.
#
# Output directory: SNBB_FASTSURFER_OUTPUT/<subject>_<session>.long.<subject>/
#
# ── Site configuration ────────────────────────────────────────────────────────
SNBB_FASTSURFER_OUTPUT="${SNBB_FASTSURFER_OUTPUT:-/media/storage/yalab-dev/snbb_scheduler/derivatives/fastsurfer}"
SNBB_FS_LICENSE="${SNBB_FS_LICENSE:-/home/galkepler/misc/freesurfer/license.txt}"
SNBB_FASTSURFER_SIF="${SNBB_FASTSURFER_SIF:-/media/storage/apptainer/images/fastsurfer-latest.sif}"
SNBB_DEBUG_LOG="${SNBB_DEBUG_LOG:-/media/storage/yalab-dev/snbb_scheduler/logs/fastsurfer_long/debug_submit.log}"
SNBB_LOCAL_TMP_ROOT="${SNBB_LOCAL_TMP_ROOT:-}"
# ─────────────────────────────────────────────────────────────────────────────

#SBATCH --time=12:00:00
#SBATCH --mem=16G
#SBATCH --cpus-per-task=8

set -euo pipefail

SUBJECT="$1"   # e.g. sub-0001
SESSION="$2"   # e.g. ses-01

LONG_SID="${SUBJECT}_${SESSION}.long.${SUBJECT}"

# ── Diagnostics ──────────────────────────────────────────────────────────────
mkdir -p "$(dirname "${SNBB_DEBUG_LOG}")"
{
    echo "=== $(date -Iseconds) | Job ${SLURM_JOB_ID:-local} | ${SUBJECT} ${SESSION} ==="
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
    LOCAL_WORKDIR="${SNBB_LOCAL_TMP_ROOT}/snbb_${SLURM_JOB_ID:-$$}_${SUBJECT}_${SESSION}_long"
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

    mkdir -p "${LOCAL_FS_OUTPUT}"

    # Stage the cross-sectional output and the template
    SID="${SUBJECT}_${SESSION}"
    rsync -a "${SNBB_FASTSURFER_OUTPUT}/${SID}/" "${LOCAL_FS_OUTPUT}/${SID}/"
    rsync -a "${SNBB_FASTSURFER_OUTPUT}/${SUBJECT}/" "${LOCAL_FS_OUTPUT}/${SUBJECT}/"

    mkdir -p "${SNBB_FASTSURFER_OUTPUT}"
    python3 "${SCRIPT_DIR}/snbb_fastsurfer_helper.py" long \
        --output-dir  "${LOCAL_FS_OUTPUT}" \
        --subject     "${SUBJECT}" \
        --session     "${SESSION}" \
        --sif         "${SNBB_FASTSURFER_SIF}" \
        --fs-license  "${SNBB_FS_LICENSE}" \
        --threads     "${SLURM_CPUS_PER_TASK:-8}"

    CLEANUP_ON_EXIT=false
    rsync -av "${LOCAL_FS_OUTPUT}/${LONG_SID}/" "${SNBB_FASTSURFER_OUTPUT}/${LONG_SID}/" || {
        echo "ERROR: rsync to remote destination failed. Local output preserved at ${LOCAL_FS_OUTPUT}/${LONG_SID}" >&2
        exit 1
    }
    CLEANUP_ON_EXIT=true
    # ─────────────────────────────────────────────────────────────────────────
else
    # ── Remote filesystem mode ────────────────────────────────────────────────
    mkdir -p "${SNBB_FASTSURFER_OUTPUT}"
    python3 "${SCRIPT_DIR}/snbb_fastsurfer_helper.py" long \
        --output-dir  "${SNBB_FASTSURFER_OUTPUT}" \
        --subject     "${SUBJECT}" \
        --session     "${SESSION}" \
        --sif         "${SNBB_FASTSURFER_SIF}" \
        --fs-license  "${SNBB_FS_LICENSE}" \
        --threads     "${SLURM_CPUS_PER_TASK:-8}"
    # ─────────────────────────────────────────────────────────────────────────
fi

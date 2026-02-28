#!/usr/bin/env bash
# snbb_run_fastsurfer_template.sh — Within-subject template creation
# Called by snbb_scheduler as:  sbatch ... snbb_run_fastsurfer_template.sh sub-XXXX
#
# Builds a within-subject unbiased anatomical template from all completed
# cross-sectional FastSurfer runs for the subject using FreeSurfer's
# recon-all -base command (run inside the FastSurfer container).
#
# Prerequisites: snbb_run_fastsurfer_cross.sh must have completed for
# EVERY session of the subject before this script runs.
#
# Output directory: SNBB_FASTSURFER_OUTPUT/<subject>/
#
# ── Site configuration ────────────────────────────────────────────────────────
SNBB_FASTSURFER_OUTPUT="${SNBB_FASTSURFER_OUTPUT:-/media/storage/yalab-dev/snbb_scheduler/derivatives/fastsurfer}"
SNBB_FS_LICENSE="${SNBB_FS_LICENSE:-/home/galkepler/misc/freesurfer/license.txt}"
SNBB_FASTSURFER_SIF="${SNBB_FASTSURFER_SIF:-/media/storage/apptainer/images/fastsurfer-latest.sif}"
SNBB_DEBUG_LOG="${SNBB_DEBUG_LOG:-/media/storage/yalab-dev/snbb_scheduler/logs/fastsurfer_template/debug_submit.log}"
SNBB_LOCAL_TMP_ROOT="${SNBB_LOCAL_TMP_ROOT:-}"
# ─────────────────────────────────────────────────────────────────────────────

#SBATCH --time=12:00:00
#SBATCH --mem=16G
#SBATCH --cpus-per-task=8

set -euo pipefail

SUBJECT="$1"   # e.g. sub-0001

# ── Diagnostics ──────────────────────────────────────────────────────────────
mkdir -p "$(dirname "${SNBB_DEBUG_LOG}")"
{
    echo "=== $(date -Iseconds) | Job ${SLURM_JOB_ID:-local} | ${SUBJECT} ==="
    echo "SNBB_FASTSURFER_OUTPUT: ${SNBB_FASTSURFER_OUTPUT}"
    echo "SNBB_FS_LICENSE:        ${SNBB_FS_LICENSE}"
    echo "SNBB_FASTSURFER_SIF:    ${SNBB_FASTSURFER_SIF}"
    echo "SNBB_LOCAL_TMP_ROOT:    ${SNBB_LOCAL_TMP_ROOT:-<unset>}"
    echo "PATH:                   ${PATH}"
} >> "${SNBB_DEBUG_LOG}" 2>&1
# ─────────────────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Discover completed cross-sectional sessions ───────────────────────────────
# Glob for <subject>_ses-*/scripts/recon-all.done to find which sessions have
# finished their cross-sectional run.  Extract session labels from dir names.
mapfile -t SESSIONS < <(
    for done_file in "${SNBB_FASTSURFER_OUTPUT}/${SUBJECT}_ses-"*/scripts/recon-all.done; do
        [[ -f "${done_file}" ]] || continue
        dir="${done_file%/scripts/recon-all.done}"
        dir="${dir##*/}"          # basename: sub-0001_ses-01
        session="${dir#${SUBJECT}_}"   # strip subject prefix → ses-01
        echo "${session}"
    done | sort
)

if [[ ${#SESSIONS[@]} -lt 2 ]]; then
    echo "ERROR: Fewer than 2 completed cross-sectional sessions found for ${SUBJECT}." >&2
    echo "       Sessions found: ${SESSIONS[*]:-none}" >&2
    echo "       Template creation requires at least 2 timepoints." >&2
    exit 1
fi

echo "Building template for ${SUBJECT} from sessions: ${SESSIONS[*]}"

if [[ -n "${SNBB_LOCAL_TMP_ROOT}" ]]; then
    # ── Local-scratch mode ────────────────────────────────────────────────────
    LOCAL_WORKDIR="${SNBB_LOCAL_TMP_ROOT}/snbb_${SLURM_JOB_ID:-$$}_${SUBJECT}_template"
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

    # Stage cross-sectional outputs for all sessions
    for ses in "${SESSIONS[@]}"; do
        SID="${SUBJECT}_${ses}"
        rsync -a "${SNBB_FASTSURFER_OUTPUT}/${SID}/" "${LOCAL_FS_OUTPUT}/${SID}/"
    done

    mkdir -p "${SNBB_FASTSURFER_OUTPUT}"
    python3 "${SCRIPT_DIR}/snbb_fastsurfer_helper.py" template \
        --output-dir  "${LOCAL_FS_OUTPUT}" \
        --subject     "${SUBJECT}" \
        --sessions    "${SESSIONS[@]}" \
        --sif         "${SNBB_FASTSURFER_SIF}" \
        --fs-license  "${SNBB_FS_LICENSE}" \
        --threads     "${SLURM_CPUS_PER_TASK:-8}"

    CLEANUP_ON_EXIT=false
    rsync -av "${LOCAL_FS_OUTPUT}/${SUBJECT}/" "${SNBB_FASTSURFER_OUTPUT}/${SUBJECT}/" || {
        echo "ERROR: rsync to remote destination failed. Local output preserved at ${LOCAL_FS_OUTPUT}/${SUBJECT}" >&2
        exit 1
    }
    CLEANUP_ON_EXIT=true
    # ─────────────────────────────────────────────────────────────────────────
else
    # ── Remote filesystem mode ────────────────────────────────────────────────
    mkdir -p "${SNBB_FASTSURFER_OUTPUT}"
    python3 "${SCRIPT_DIR}/snbb_fastsurfer_helper.py" template \
        --output-dir  "${SNBB_FASTSURFER_OUTPUT}" \
        --subject     "${SUBJECT}" \
        --sessions    "${SESSIONS[@]}" \
        --sif         "${SNBB_FASTSURFER_SIF}" \
        --fs-license  "${SNBB_FS_LICENSE}" \
        --threads     "${SLURM_CPUS_PER_TASK:-8}"
    # ─────────────────────────────────────────────────────────────────────────
fi

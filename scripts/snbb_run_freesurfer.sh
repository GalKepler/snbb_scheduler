#!/usr/bin/env bash
# snbb_run_freesurfer.sh — FreeSurfer longitudinal pipeline via Apptainer container
# Called by the snbb_scheduler as:  sbatch ... snbb_run_freesurfer.sh sub-XXXX
# (subject-scoped: one job processes all sessions for a subject)
#
# For subjects with ONE session:
#   Runs a standard cross-sectional recon-all:
#   recon-all -s <subject> -i <T1w> [-T2 <T2w> -T2pial] -sd <SUBJECTS_DIR> -all
#
# For subjects with TWO OR MORE sessions (longitudinal pipeline):
#   Step 1 — cross-sectional per session:
#   recon-all -s <subject>_<session> -i <T1w> -sd <SUBJECTS_DIR> -all
#   Step 2 — unbiased template:
#   recon-all -base <subject> -tp <subject>_<ses1> -tp <subject>_<ses2> -sd <SUBJECTS_DIR> -all
#   Step 3 — longitudinal refinement per session:
#   recon-all -long <subject>_<session> <subject> -sd <SUBJECTS_DIR> -all
#
# Already-completed steps (scripts/recon-all.done) are skipped automatically
# so failed jobs can be resumed without reprocessing completed stages.
#
# ── Site configuration ────────────────────────────────────────────────────────
# Edit the values below for your cluster, or set the env vars before submitting.
SNBB_BIDS_ROOT="${SNBB_BIDS_ROOT:-/media/storage/yalab-dev/snbb_scheduler/bids}"
SNBB_FS_OUTPUT="${SNBB_FS_OUTPUT:-/media/storage/yalab-dev/snbb_scheduler/derivatives/freesurfer}"
SNBB_FS_LICENSE="${SNBB_FS_LICENSE:-/home/galkepler/misc/freesurfer/license.txt}"
SNBB_FREESURFER_SIF="${SNBB_FREESURFER_SIF:-/media/storage/apptainer/images/freesurfer-8.1.0.sif}"
SNBB_DEBUG_LOG="${SNBB_DEBUG_LOG:-/media/storage/yalab-dev/snbb_scheduler/logs/freesurfer/debug_submit.log}"
# Optional: root of local scratch on compute nodes.
# When set, subject BIDS input and FreeSurfer output are staged locally,
# then rsynced back to the remote destination on success.
# Leave empty (default) to use remote paths directly.
SNBB_LOCAL_TMP_ROOT="${SNBB_LOCAL_TMP_ROOT:-}"
# ─────────────────────────────────────────────────────────────────────────────

# The longitudinal pipeline (3 sequential recon-all runs per session) can take
# up to ~72 h for subjects with 2–4 sessions.
#SBATCH --time=72:00:00
#SBATCH --mem=32G
#SBATCH --cpus-per-task=8

set -euo pipefail

SUBJECT="$1"          # e.g. sub-0001

# ── Diagnostics ──────────────────────────────────────────────────────────────
mkdir -p "$(dirname "${SNBB_DEBUG_LOG}")"
{
    echo "=== $(date -Iseconds) | Job ${SLURM_JOB_ID:-local} | ${SUBJECT} ==="
    echo "SNBB_BIDS_ROOT:        ${SNBB_BIDS_ROOT}"
    echo "SNBB_FS_OUTPUT:        ${SNBB_FS_OUTPUT}"
    echo "SNBB_FS_LICENSE:       ${SNBB_FS_LICENSE}"
    echo "SNBB_FREESURFER_SIF:   ${SNBB_FREESURFER_SIF}"
    echo "SNBB_LOCAL_TMP_ROOT:   ${SNBB_LOCAL_TMP_ROOT:-<unset>}"
    echo "PATH:                  ${PATH}"
} >> "${SNBB_DEBUG_LOG}" 2>&1
# ─────────────────────────────────────────────────────────────────────────────

if [[ -n "${SNBB_LOCAL_TMP_ROOT}" ]]; then
    # ── Local-scratch mode ────────────────────────────────────────────────────
    # Stage BIDS input and FreeSurfer output on the compute node's local disk.
    # On any exit (success, error, or SIGTERM) the local workdir is cleaned up,
    # except when rsync-out fails — in that case the local output is preserved
    # for manual recovery.

    LOCAL_WORKDIR="${SNBB_LOCAL_TMP_ROOT}/snbb_${SLURM_JOB_ID:-$$}_${SUBJECT}"
    LOCAL_BIDS="${LOCAL_WORKDIR}/bids"
    LOCAL_FS_OUTPUT="${LOCAL_WORKDIR}/freesurfer"
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
    mkdir -p "${LOCAL_BIDS}" "${LOCAL_FS_OUTPUT}"

    # Copy subject BIDS data + BIDS root metadata files
    rsync -a "${SNBB_BIDS_ROOT}/${SUBJECT}/" "${LOCAL_BIDS}/${SUBJECT}/"
    for f in dataset_description.json README README.md .bidsignore participants.tsv participants.json; do
        [[ -e "${SNBB_BIDS_ROOT}/${f}" ]] && rsync -a "${SNBB_BIDS_ROOT}/${f}" "${LOCAL_BIDS}/${f}"
    done

    # Stage any existing FreeSurfer outputs for the subject (allows resume).
    # Sync all directories whose names start with the subject label.
    if [[ -d "${SNBB_FS_OUTPUT}" ]]; then
        find "${SNBB_FS_OUTPUT}" -maxdepth 1 -type d -name "${SUBJECT}*" | while read -r d; do
            name="$(basename "${d}")"
            rsync -a "${d}/" "${LOCAL_FS_OUTPUT}/${name}/"
        done
    fi

    # Run the FreeSurfer longitudinal helper against local paths
    mkdir -p "${SNBB_FS_OUTPUT}"
    python3 "/home/galkepler/Projects/snbb_scheduler/scripts/snbb_recon_all_helper.py" \
        --bids-dir    "${LOCAL_BIDS}" \
        --output-dir  "${LOCAL_FS_OUTPUT}" \
        --subject     "${SUBJECT}" \
        --threads     "${SLURM_CPUS_PER_TASK:-8}" \
        --sif         "${SNBB_FREESURFER_SIF}" \
        --fs-license  "${SNBB_FS_LICENSE}"

    # Rsync all subject-related output directories back to remote destination.
    # This captures the cross-sectional, template, and longitudinal directories.
    CLEANUP_ON_EXIT=false
    find "${LOCAL_FS_OUTPUT}" -maxdepth 1 -type d -name "${SUBJECT}*" | while read -r d; do
        name="$(basename "${d}")"
        rsync -av "${d}/" "${SNBB_FS_OUTPUT}/${name}/" || {
            echo "ERROR: rsync failed for ${name}. Local output preserved at ${d}" >&2
            exit 1
        }
    done
    CLEANUP_ON_EXIT=true
    # ─────────────────────────────────────────────────────────────────────────
else
    # ── Remote filesystem mode ────────────────────────────────────────────────
    mkdir -p "${SNBB_FS_OUTPUT}"

    python3 "/home/galkepler/Projects/snbb_scheduler/scripts/snbb_recon_all_helper.py" \
        --bids-dir    "${SNBB_BIDS_ROOT}" \
        --output-dir  "${SNBB_FS_OUTPUT}" \
        --subject     "${SUBJECT}" \
        --threads     "${SLURM_CPUS_PER_TASK:-8}" \
        --sif         "${SNBB_FREESURFER_SIF}" \
        --fs-license  "${SNBB_FS_LICENSE}"
    # ─────────────────────────────────────────────────────────────────────────
fi

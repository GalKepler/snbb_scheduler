#!/usr/bin/env bash
# snbb_run_freesurfer.sh — FreeSurfer recon-all via Apptainer container
# Called by the snbb_scheduler as:  sbatch ... snbb_run_freesurfer.sh sub-XXXX ses-YY
#
# Runs recon-all inside a FreeSurfer Apptainer container. The helper script
# snbb_recon_all_helper.py globbs all T1w (and T2w) NIfTI files for the
# subject across all BIDS sessions and builds the -i argument list.
#
# ── Site configuration ────────────────────────────────────────────────────────
# Edit the values below for your cluster, or set the env vars before submitting.
SNBB_BIDS_ROOT="${SNBB_BIDS_ROOT:-/media/storage/yalab-dev/snbb_scheduler/bids}"
SNBB_FS_OUTPUT="${SNBB_FS_OUTPUT:-/media/storage/yalab-dev/snbb_scheduler/derivatives/freesurfer}"
TMP_FS_OUTPUT="${SNBB_TMP_FS_OUTPUT:-/media/storage/yalab-dev/tmp/freesurfer}"  # FreeSurfer needs write access to SUBJECTS_DIR
SNBB_FS_LICENSE="${SNBB_FS_LICENSE:-/home/galkepler/misc/freesurfer/license.txt}"
SNBB_FREESURFER_SIF="${SNBB_FREESURFER_SIF:-/media/storage/apptainer/images/freesurfer-8.1.0.sif}"
SNBB_DEBUG_LOG="${SNBB_DEBUG_LOG:-/media/storage/yalab-dev/snbb_scheduler/logs/freesurfer/debug_submit.log}"
# Optional: root of local scratch on compute nodes.
# When set, subject BIDS input and FreeSurfer output are staged locally,
# then rsynced back to the remote destination on success.
# Leave empty (default) to use the existing TMP_FS_OUTPUT behaviour unchanged.
SNBB_LOCAL_TMP_ROOT="${SNBB_LOCAL_TMP_ROOT:-}"
# ─────────────────────────────────────────────────────────────────────────────

#SBATCH --time=24:00:00
#SBATCH --mem=20G
#SBATCH --cpus-per-task=8

set -euo pipefail

SUBJECT="$1"          # e.g. sub-0001  ($2 = session, ignored — FreeSurfer is subject-scoped)

# ── Diagnostics ──────────────────────────────────────────────────────────────
mkdir -p "$(dirname "${SNBB_DEBUG_LOG}")"
{
    echo "=== $(date -Iseconds) | Job ${SLURM_JOB_ID:-local} | ${SUBJECT} ==="
    echo "SNBB_BIDS_ROOT:        ${SNBB_BIDS_ROOT}"
    echo "SNBB_FS_OUTPUT:        ${SNBB_FS_OUTPUT}"
    echo "TMP_FS_OUTPUT:         ${TMP_FS_OUTPUT}"
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

    # Run FreeSurfer against local paths
    mkdir -p "${SNBB_FS_OUTPUT}"
    python3 "/home/galkepler/Projects/snbb_scheduler/scripts/snbb_recon_all_helper.py" \
        --bids-dir    "${LOCAL_BIDS}" \
        --output-dir  "${LOCAL_FS_OUTPUT}" \
        --subject     "${SUBJECT}" \
        --threads     "${SLURM_CPUS_PER_TASK:-8}" \
        --sif         "${SNBB_FREESURFER_SIF}" \
        --fs-license  "${SNBB_FS_LICENSE}"

    # Rsync results to remote destination; if this fails, preserve local output
    CLEANUP_ON_EXIT=false
    rsync -av "${LOCAL_FS_OUTPUT}/${SUBJECT}/" "${SNBB_FS_OUTPUT}/${SUBJECT}/" || {
        echo "ERROR: rsync to remote destination failed. Local output preserved at ${LOCAL_FS_OUTPUT}/${SUBJECT}" >&2
        exit 1
    }
    CLEANUP_ON_EXIT=true
    # ─────────────────────────────────────────────────────────────────────────
else
    # ── Original behaviour (remote filesystem) ────────────────────────────────
    mkdir -p "${SNBB_FS_OUTPUT}"
    mkdir -p "${TMP_FS_OUTPUT}"

    # run freesurfer to the tmp output dir (must be writable), then move the results to the final output
    # location (to avoid permission issues if the final output is on a read-only filesystem)
    # (move using rsync to preserve permissions and avoid issues if the source and destination are on different filesystems)

    python3 "/home/galkepler/Projects/snbb_scheduler/scripts/snbb_recon_all_helper.py" \
        --bids-dir    "${SNBB_BIDS_ROOT}" \
        --output-dir  "${TMP_FS_OUTPUT}" \
        --subject     "${SUBJECT}" \
        --threads     "${SLURM_CPUS_PER_TASK:-8}" \
        --sif         "${SNBB_FREESURFER_SIF}" \
        --fs-license  "${SNBB_FS_LICENSE}"
    rsync -av "${TMP_FS_OUTPUT}/${SUBJECT}/" "${SNBB_FS_OUTPUT}/${SUBJECT}/"

    # if the data transferred successfully, remove the temporary output to save space.
    # check for the sub-xx/scripts/recon-all.done file as a marker that the recon-all completed successfully before deleting.
    if [[ -f "${SNBB_FS_OUTPUT}/${SUBJECT}/scripts/recon-all.done" ]]; then
        rm -rf "${TMP_FS_OUTPUT}/${SUBJECT}"
    else
        echo "WARNING: recon-all completion marker not found. Temporary output not deleted: ${TMP_FS_OUTPUT}/${SUBJECT}" >&2
    fi
    # ─────────────────────────────────────────────────────────────────────────
fi

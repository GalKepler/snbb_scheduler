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
TMP_FS_OUTPUT="${SNBB_FS_OUTPUT:-/media/storage/yalab-dev/tmp/freesurfer}"  # FreeSurfer needs write access to SUBJECTS_DIR
SNBB_FS_LICENSE="${SNBB_FS_LICENSE:-/home/galkepler/misc/freesurfer/license.txt}"
SNBB_FREESURFER_SIF="${SNBB_FREESURFER_SIF:-/media/storage/apptainer/images/freesurfer-8.1.0.sif}"
SNBB_DEBUG_LOG="${SNBB_DEBUG_LOG:-/media/storage/yalab-dev/snbb_scheduler/logs/freesurfer/debug_submit.log}"
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
    echo "SNBB_BIDS_ROOT:      ${SNBB_BIDS_ROOT}"
    echo "SNBB_FS_OUTPUT:      ${SNBB_FS_OUTPUT}"
    echo "SNBB_FS_LICENSE:     ${SNBB_FS_LICENSE}"
    echo "SNBB_FREESURFER_SIF: ${SNBB_FREESURFER_SIF}"
    echo "PATH:                ${PATH}"
} >> "${SNBB_DEBUG_LOG}" 2>&1
# ─────────────────────────────────────────────────────────────────────────────

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
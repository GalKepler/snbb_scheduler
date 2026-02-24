#!/usr/bin/env bash
# snbb_run_bids.sh — DICOM → BIDS conversion via heudiconv (Apptainer)
# Called by the snbb_scheduler as:  sbatch ... snbb_run_bids.sh sub-XXXX ses-YY
#
# ── Site configuration ────────────────────────────────────────────────────────
# Edit the values below for your cluster, or set the env vars before submitting.
SNBB_DICOM_ROOT="${SNBB_DICOM_ROOT:-/data/snbb/dicom}"
SNBB_BIDS_ROOT="${SNBB_BIDS_ROOT:-/media/storage/yalab-dev/snbb_scheduler/bids}"
SNBB_HEURISTIC="${SNBB_HEURISTIC:-/home/galkepler/Projects/snbb_scheduler/scripts/heuristic.py}"
SNBB_HEUDICONV_SIF="${SNBB_HEUDICONV_SIF:-/media/storage/apptainer/images/heudiconv-1.3.4.sif}"
SNBB_DEBUG_LOG="${SNBB_DEBUG_LOG:-/media/storage/yalab-dev/snbb_scheduler/logs/bids/debug_submit.log}"
# ─────────────────────────────────────────────────────────────────────────────

#SBATCH --time=4:00:00
#SBATCH --mem=8G
#SBATCH --cpus-per-task=4

set -euo pipefail

SUBJECT="$1"                   # e.g. sub-0001
SESSION="$2"                   # e.g. ses-202602161208
DICOM_PATH_ARG="${3:-}"        # optional: explicit DICOM path from scheduler
PARTICIPANT="${SUBJECT#sub-}"  # strip prefix → 0001
SESSION_ID="${SESSION#ses-}"   # strip prefix → 202602161208

# Session-specific DICOM directory. Uses the explicit path passed by the
# scheduler (from the sessions CSV's dicom_path column) when provided;
# falls back to SNBB_DICOM_SESSION_DIR env var, then <dicom_root>/<session_id>.
if [[ -n "${DICOM_PATH_ARG}" ]]; then
    SNBB_DICOM_SESSION_DIR="${DICOM_PATH_ARG}"
else
    SNBB_DICOM_SESSION_DIR="${SNBB_DICOM_SESSION_DIR:-${SNBB_DICOM_ROOT}/${SESSION_ID}}"
fi

# ── Diagnostics ──────────────────────────────────────────────────────────────
mkdir -p "$(dirname "${SNBB_DEBUG_LOG}")"
{
    echo "=== $(date -Iseconds) | Job ${SLURM_JOB_ID:-local} | ${SUBJECT} ${SESSION} ==="
    echo "SNBB_DICOM_SESSION_DIR: ${SNBB_DICOM_SESSION_DIR}"
    echo "SNBB_BIDS_ROOT:         ${SNBB_BIDS_ROOT}"
    echo "SNBB_HEURISTIC:         ${SNBB_HEURISTIC}"
    echo "SNBB_HEUDICONV_SIF:     ${SNBB_HEUDICONV_SIF}"
    echo "PATH:                   ${PATH}"
} >> "${SNBB_DEBUG_LOG}" 2>&1
# ─────────────────────────────────────────────────────────────────────────────

apptainer run --cleanenv \
    --bind "${SNBB_DICOM_SESSION_DIR}":"${SNBB_DICOM_SESSION_DIR}":ro \
    --bind "${SNBB_BIDS_ROOT}":"${SNBB_BIDS_ROOT}" \
    --bind "${SNBB_HEURISTIC}":"${SNBB_HEURISTIC}":ro \
    "${SNBB_HEUDICONV_SIF}" \
    --files "${SNBB_DICOM_SESSION_DIR}" \
    --outdir "${SNBB_BIDS_ROOT}" \
    --heuristic "${SNBB_HEURISTIC}" \
    --subjects "${PARTICIPANT}" \
    --ses "${SESSION_ID}" \
    --converter dcm2niix \
    --bids notop \
    --grouping all \
    --overwrite

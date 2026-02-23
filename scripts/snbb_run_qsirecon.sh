#!/usr/bin/env bash
# snbb_run_qsirecon.sh — QSIRecon diffusion reconstruction wrapper
# Called by the snbb_scheduler as:  sbatch ... snbb_run_qsirecon.sh sub-XXXX ses-YY
#
# ── Site configuration ────────────────────────────────────────────────────────
# Edit the values below for your cluster, or set the env vars before submitting.
SNBB_QSIPREP_DIR="${SNBB_QSIPREP_DIR:-/media/storage/yalab-dev/snbb_scheduler/derivatives/qsiprep}"
SNBB_DERIVATIVES="${SNBB_DERIVATIVES:-/media/storage/yalab-dev/snbb_scheduler/derivatives/qsirecon}"
SNBB_FS_LICENSE="${SNBB_FS_LICENSE:-/home/galkepler/misc/freesurfer/license.txt}"
SNBB_FS_SUBJECTS_DIR="${SNBB_FS_SUBJECTS_DIR:-/media/storage/yalab-dev/snbb_scheduler/derivatives/freesurfer}"
SNBB_RECON_SPEC="${SNBB_RECON_SPEC:-/home/galkepler/Projects/yalab-devops/VoxelOps/mrtrix_tractography.yaml}"
SNBB_RECON_SPEC_AUX_FILES="${SNBB_RECON_SPEC_AUX_FILES:-}"
SNBB_LOG_DIR="${SNBB_LOG_DIR:-/media/storage/yalab-dev/snbb_scheduler/logs/qsirecon}"
SNBB_RUNNERS_DIR="${SNBB_RUNNERS_DIR:-/home/galkepler/Projects/snbb_scheduler/examples/runners}"
SNBB_VENV="${SNBB_VENV:-/home/galkepler/Projects/snbb_scheduler/.venv}"
SNBB_DEBUG_LOG="${SNBB_DEBUG_LOG:-/media/storage/yalab-dev/snbb_scheduler/logs/qsirecon/debug_submit.log}"
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
    echo "SNBB_VENV:              ${SNBB_VENV}"
    echo "SNBB_QSIPREP_DIR:       ${SNBB_QSIPREP_DIR}"
    echo "SNBB_DERIVATIVES:       ${SNBB_DERIVATIVES}"
    echo "SNBB_FS_LICENSE:        ${SNBB_FS_LICENSE}"
    echo "SNBB_FS_SUBJECTS_DIR:   ${SNBB_FS_SUBJECTS_DIR}"
    echo "SNBB_RECON_SPEC:        ${SNBB_RECON_SPEC}"
    echo "SNBB_RECON_SPEC_AUX_FILES: ${SNBB_RECON_SPEC_AUX_FILES}"
    echo "SNBB_RUNNERS_DIR:       ${SNBB_RUNNERS_DIR}"
    echo "python binary:          ${SNBB_VENV}/bin/python"
    echo "python exists:          $(test -x "${SNBB_VENV}/bin/python" && echo yes || echo NO)"
    echo "voxelops:               $("${SNBB_VENV}/bin/python" -c 'import voxelops; print(voxelops.__file__)' 2>&1)"
    echo "PATH:                   ${PATH}"
} >> "${SNBB_DEBUG_LOG}" 2>&1
# ─────────────────────────────────────────────────────────────────────────────

. "${SNBB_VENV}/bin/activate"

# Build optional arguments
EXTRA_ARGS=()
if [[ -n "${SNBB_RECON_SPEC_AUX_FILES}" ]]; then
    EXTRA_ARGS+=(--recon-spec-aux-files "${SNBB_RECON_SPEC_AUX_FILES}")
fi

"${SNBB_VENV}/bin/python" "${SNBB_RUNNERS_DIR}/run_qsirecon.py" \
    --qsiprep-dir      "${SNBB_QSIPREP_DIR}" \
    --output-dir       "${SNBB_DERIVATIVES}/qsirecon" \
    --participants     "${PARTICIPANT}" \
    --session          "${SESSION_ID}" \
    --recon-spec       "${SNBB_RECON_SPEC}" \
    --fs-license       "${SNBB_FS_LICENSE}" \
    --fs-subjects-dir  "${SNBB_FS_SUBJECTS_DIR}" \
    --workers          1 \
    --log-dir          "${SNBB_LOG_DIR}" \
    --force \
    "${EXTRA_ARGS[@]}"

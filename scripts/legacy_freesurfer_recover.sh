#!/usr/bin/env bash
# legacy_freesurfer_recover.sh — find legacy_freesurfer_run.sh jobs whose
# final rsync-to-remote failed (symlink ENOTSUP, now fixed with rsync -L),
# and finish the copy from the preserved local workdir.
#
# Usage: ./legacy_freesurfer_recover.sh [log_dir]
#   log_dir defaults to $SNBB_FS_LOG_DIR or
#   /media/storage/yalab-dev/snbb_scheduler/logs/freesurfer_legacy
#
# Scans *.err files for "Preserving local workdir for recovery: <path>",
# reruns "rsync -avL <path>/output/ $SNBB_FS_OUTPUT/", and on success
# (scripts/recon-all.done now present) removes the local workdir.

SNBB_FS_OUTPUT="${SNBB_FS_OUTPUT:-/media/storage/yalab-dev/snbb_scheduler/derivatives/freesurfer_legacy}"
SNBB_FS_LOG_DIR="${1:-${SNBB_FS_LOG_DIR:-/media/storage/yalab-dev/snbb_scheduler/logs/freesurfer_legacy}}"

set -euo pipefail

shopt -s nullglob
for err_file in "${SNBB_FS_LOG_DIR}"/*.err; do
    workdir="$(grep -o 'Preserving local workdir for recovery: .*' "${err_file}" | tail -1 | cut -d' ' -f6-)"
    [[ -z "${workdir}" ]] && continue
    [[ -d "${workdir}/output" ]] || { echo "SKIP: ${workdir}/output missing (${err_file})" >&2; continue; }

    subject="$(basename "${workdir}" | sed -E 's/^snbb_legacyfs_[0-9]+_//')"
    echo "RECOVER: ${subject}  (${workdir})"

    # fsaverage is a symlink to the container's shared atlas dir (not subject
    # output, dangling on the host) — exclude it, dereference the rest with -L.
    rsync -avL --exclude=fsaverage "${workdir}/output/" "${SNBB_FS_OUTPUT}/"

    if [[ -f "${SNBB_FS_OUTPUT}/${subject}/scripts/recon-all.done" ]]; then
        echo "OK: ${subject} recovered, removing ${workdir}"
        rm -rf "${workdir}"
    else
        echo "WARN: ${subject} rsync ran but recon-all.done still missing at ${SNBB_FS_OUTPUT}/${subject} — leaving ${workdir}" >&2
    fi
done

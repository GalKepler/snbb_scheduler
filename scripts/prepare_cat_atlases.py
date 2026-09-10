#!/usr/bin/env python3
"""prepare_cat_atlases.py — one-time conversion of the qsirecon atlas pack
into CAT12's `ownatlas` format (uncompressed .nii + `ROIid;ROIabbr;ROIname`
.csv, same basename), so snbb_run_cat.sh can feed them to CAT12's own
volumetric ROI extraction alongside its built-in atlases.

Run once (not per-session, not per-job — the source atlases are static):

    python3 scripts/prepare_cat_atlases.py
    python3 scripts/prepare_cat_atlases.py --atlas-pack-dir /path --out-dir /path

Then point snbb_run_cat.sh's SNBB_CAT_ATLAS_DIR at --out-dir (defaults match,
so no config needed if you used the defaults).

Source: the same atlas pack scripts/snbb_run_qsirecon.sh reads via
SNBB_ATLASES_DIR (default /media/storage/Projects/snbb-atlas-pack/qsirecon_ext),
BIDS-Atlas layout: atlas-<NAME>/atlas-<NAME>_space-MNI152NLin2009cAsym_res-01_dseg.nii.gz
+ atlas-<NAME>_dseg.tsv (columns: index, label, name, ...).

ATLASES below is the subset of SNBB_ATLASES (scripts/snbb_run_qsirecon.sh)
that has a local volumetric file in the pack — 45 of the 59 entries there.
The other 14 (4S*Parcels, AAL116, AICHA384Ext, Brainnetome246Ext, Gordon333Ext)
are QSIRecon's own built-in/TemplateFlow atlases, not files in this pack —
not included here. Keep this list in sync with SNBB_ATLASES if that changes.

CAT12 resamples the ROI atlas onto its own internal grid at extraction time
regardless of source voxel size, so the MNI152NLin2009cAsym affine space
here (not CAT12's own template) is fine — this is the exact same atlas prep
bagpipe already validated in production for one atlas
(Schaefer2018N400n7Tian2020S2, docs/cat12_container_spec.md §2/§4 in that
project).

CAVEAT: CAT12's `ownatlas` field has only ever been verified with a single
atlas at a time (bagpipe's production use). Feeding it all 45 at once is
untested — verify a real single-session snbb_run_cat.sh run produces the
expected per-atlas label/catROI_<name>_<stem>.xml files before relying on
this for a full cohort run.
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

import nibabel as nib

ATLASES = [
    "Schaefer2018N100n7Tian2020S1", "Schaefer2018N100n7Tian2020S2",
    "Schaefer2018N100n7Tian2020S3", "Schaefer2018N100n7Tian2020S4",
    "Schaefer2018N200n7Tian2020S1", "Schaefer2018N200n7Tian2020S2",
    "Schaefer2018N200n7Tian2020S3", "Schaefer2018N200n7Tian2020S4",
    "Schaefer2018N300n7Tian2020S1", "Schaefer2018N300n7Tian2020S2",
    "Schaefer2018N300n7Tian2020S3", "Schaefer2018N300n7Tian2020S4",
    "Schaefer2018N400n7Tian2020S1", "Schaefer2018N400n7Tian2020S2",
    "Schaefer2018N400n7Tian2020S3", "Schaefer2018N400n7Tian2020S4",
    "Schaefer2018N500n7Tian2020S1", "Schaefer2018N500n7Tian2020S2",
    "Schaefer2018N500n7Tian2020S3", "Schaefer2018N500n7Tian2020S4",
    "Schaefer2018N600n7Tian2020S1", "Schaefer2018N600n7Tian2020S2",
    "Schaefer2018N600n7Tian2020S3", "Schaefer2018N600n7Tian2020S4",
    "Schaefer2018N700n7Tian2020S1", "Schaefer2018N700n7Tian2020S2",
    "Schaefer2018N700n7Tian2020S3", "Schaefer2018N700n7Tian2020S4",
    "Schaefer2018N800n7Tian2020S1", "Schaefer2018N800n7Tian2020S2",
    "Schaefer2018N800n7Tian2020S3", "Schaefer2018N800n7Tian2020S4",
    "Schaefer2018N900n7Tian2020S1", "Schaefer2018N900n7Tian2020S2",
    "Schaefer2018N900n7Tian2020S3", "Schaefer2018N900n7Tian2020S4",
    "Schaefer2018N1000n7Tian2020S1", "Schaefer2018N1000n7Tian2020S2",
    "Schaefer2018N1000n7Tian2020S3", "Schaefer2018N1000n7Tian2020S4",
    "HCPex", "TianS1", "TianS2", "TianS3", "TianS4",
]

DEFAULT_ATLAS_PACK_DIR = Path("/media/storage/Projects/snbb-atlas-pack/qsirecon_ext")
DEFAULT_OUT_DIR = Path("/media/storage/yalab-dev/snbb_scheduler/derivatives/cat12_atlases")


def convert_one(name: str, pack_dir: Path, out_dir: Path, force: bool = False) -> None:
    """Gunzip <name>'s volumetric NIfTI and convert its label TSV to CAT12's
    `ROIid;ROIabbr;ROIname` CSV convention, both written as <name>.{nii,csv}
    in out_dir."""
    src_dir = pack_dir / f"atlas-{name}"
    src_nii = src_dir / f"atlas-{name}_space-MNI152NLin2009cAsym_res-01_dseg.nii.gz"
    src_tsv = src_dir / f"atlas-{name}_dseg.tsv"
    if not src_nii.exists():
        raise FileNotFoundError(f"{name}: missing {src_nii}")
    if not src_tsv.exists():
        raise FileNotFoundError(f"{name}: missing {src_tsv}")

    out_dir.mkdir(parents=True, exist_ok=True)
    out_nii = out_dir / f"{name}.nii"
    out_csv = out_dir / f"{name}.csv"

    if force or not out_nii.exists():
        # ponytail: the pack ships RAS (x affine +1); CAT12's ownatlas needs
        # LAS (x affine -1, as in bagpipe's validated copy) or it silently
        # emits an all-empty remapped atlas (cat_main_roi:emptyMappedAtlas).
        img = nib.load(src_nii)
        las_img = img.as_reoriented(
            nib.orientations.ornt_transform(
                nib.orientations.io_orientation(img.affine),
                nib.orientations.axcodes2ornt("LAS"),
            )
        )
        nib.save(las_img, out_nii)

    if force or not out_csv.exists():
        with open(src_tsv, newline="") as f_in, open(out_csv, "w", newline="") as f_out:
            reader = csv.DictReader(f_in, delimiter="\t")
            writer = csv.writer(f_out, delimiter=";")
            writer.writerow(["ROIid", "ROIabbr", "ROIname"])
            for row in reader:
                writer.writerow([row["index"], row["label"], row["name"]])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--atlas-pack-dir", type=Path, default=DEFAULT_ATLAS_PACK_DIR)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--force", action="store_true", help="re-convert even if output exists")
    parser.add_argument("--self-test", action="store_true", help="run the built-in self-check and exit")
    args = parser.parse_args(argv)

    if args.self_test:
        _self_test()
        print("self-test OK")
        return 0

    args.out_dir.mkdir(parents=True, exist_ok=True)
    failed = []
    for name in ATLASES:
        try:
            convert_one(name, args.atlas_pack_dir, args.out_dir, force=args.force)
            print(f"ok:     {name}")
        except FileNotFoundError as e:
            failed.append(name)
            print(f"MISSING: {e}", file=sys.stderr)

    print(f"\n{len(ATLASES) - len(failed)}/{len(ATLASES)} atlases prepared in {args.out_dir}")
    if failed:
        print(f"failed: {failed}", file=sys.stderr)
        return 1
    return 0


def _self_test() -> None:
    """Assert-based smoke test against a synthetic atlas pack — no real data needed."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        pack_dir = tmp / "pack"
        out_dir = tmp / "out"
        name = "TestAtlas1"
        src_dir = pack_dir / f"atlas-{name}"
        src_dir.mkdir(parents=True)

        import numpy as np

        # RAS affine (x +1), matching the real qsirecon pack's orientation.
        ras_affine = np.array(
            [[1, 0, 0, -96], [0, 1, 0, -132], [0, 0, 1, -78], [0, 0, 0, 1]], dtype=float
        )
        src_img = nib.Nifti1Image(np.zeros((4, 4, 4), dtype="int16"), ras_affine)
        nib.save(src_img, src_dir / f"atlas-{name}_space-MNI152NLin2009cAsym_res-01_dseg.nii.gz")

        with open(src_dir / f"atlas-{name}_dseg.tsv", "w", newline="") as f:
            f.write("index\tlabel\tname\themisphere\n")
            f.write("1\tLH_Vis_1\t7Networks_LH_Vis_1\tL\n")
            f.write("2\tRH_Vis_1\t7Networks_RH_Vis_1\tR\n")

        convert_one(name, pack_dir, out_dir)

        out_nii = out_dir / f"{name}.nii"
        out_csv = out_dir / f"{name}.csv"
        assert nib.aff2axcodes(nib.load(out_nii).affine) == ("L", "A", "S"), (
            "output atlas must be reoriented to LAS for CAT12's ownatlas"
        )
        rows = out_csv.read_text().splitlines()
        assert rows[0] == "ROIid;ROIabbr;ROIname", f"unexpected csv header: {rows[0]}"
        assert rows[1] == "1;LH_Vis_1;7Networks_LH_Vis_1", f"unexpected row: {rows[1]}"
        assert rows[2] == "2;RH_Vis_1;7Networks_RH_Vis_1", f"unexpected row: {rows[2]}"

        # force=False must skip re-writing an existing output
        out_nii.write_bytes(b"untouched")
        convert_one(name, pack_dir, out_dir, force=False)
        assert out_nii.read_bytes() == b"untouched", "force=False should not overwrite"

        # missing source raises FileNotFoundError
        try:
            convert_one("DoesNotExist", pack_dir, out_dir)
        except FileNotFoundError:
            pass
        else:
            raise AssertionError("expected FileNotFoundError for a missing atlas")


if __name__ == "__main__":
    raise SystemExit(main())

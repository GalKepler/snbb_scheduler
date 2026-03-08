# Apptainer Images

The `snbb_scheduler` pipeline relies on four Apptainer (formerly Singularity) container images to run the core neuroimaging tools. This page documents the exact versions in use and how to build the `.sif` files on your cluster.

---

## Tool versions

| Procedure | Tool | Version | `.sif` filename |
|---|---|---|---|
| `bids` | [heudiconv](https://heudiconv.readthedocs.io) | **1.3.4** | `heudiconv-1.3.4.sif` |
| `qsiprep` | [QSIPrep](https://qsiprep.readthedocs.io) | **1.1.1** | `qsiprep-1.1.1.sif` |
| `freesurfer` | [FreeSurfer](https://surfer.nmr.mgh.harvard.edu) | **8.1.0** | `freesurfer-8.1.0.sif` |
| `qsirecon` | [QSIRecon](https://qsirecon.readthedocs.io) | **1.2.0** | `qsirecon-1.2.0.sif` |

---

## Building the images

Each image is built from its official Docker Hub source using `apptainer build`. Run these commands on a machine with internet access (or use the cluster's login node if outbound Docker pulls are allowed).

### heudiconv 1.3.4

```bash
apptainer build heudiconv-1.3.4.sif docker://nipy/heudiconv:1.3.4
```

### QSIPrep 1.1.1

```bash
apptainer build qsiprep-1.1.1.sif docker://pennbbl/qsiprep:1.1.1
```

### FreeSurfer 8.1.0

```bash
apptainer build freesurfer-8.1.0.sif docker://freesurfer/freesurfer:8.1.0
```

!!! note "FreeSurfer license"
    A valid FreeSurfer license file is required at runtime. Register for free at
    [https://surfer.nmr.mgh.harvard.edu/registration.html](https://surfer.nmr.mgh.harvard.edu/registration.html)
    and point `SNBB_FS_LICENSE` to the downloaded `license.txt`.

### QSIRecon 1.2.0

```bash
apptainer build qsirecon-1.2.0.sif docker://pennbbl/qsirecon:1.2.0
```

---

## Recommended storage layout

Store all `.sif` files in a shared, read-only directory accessible from compute nodes, for example:

```
/media/storage/apptainer/images/
├── heudiconv-1.3.4.sif
├── qsiprep-1.1.1.sif
├── freesurfer-8.1.0.sif
└── qsirecon-1.2.0.sif
```

Then set the corresponding environment variables in each shell script (or export them from your environment):

```bash
export SNBB_HEUDICONV_SIF=/media/storage/apptainer/images/heudiconv-1.3.4.sif
export SNBB_QSIPREP_SIF=/media/storage/apptainer/images/qsiprep-1.1.1.sif
export SNBB_FREESURFER_SIF=/media/storage/apptainer/images/freesurfer-8.1.0.sif
export SNBB_QSIRECON_SIF=/media/storage/apptainer/images/qsirecon-1.2.0.sif
```

---

## Notes

- **Build time:** Each `apptainer build` pull can take 10–30 minutes depending on network speed and image size (QSIPrep and FreeSurfer are several GB each).
- **Caching:** Apptainer caches Docker layers under `$APPTAINER_CACHEDIR` (default `~/.apptainer/cache`). Set this to a path with sufficient space before building.
- **Offline clusters:** If your cluster nodes have no internet access, build on the login node (or a gateway machine) and copy the `.sif` files to the shared storage path.
- **Upgrading:** When upgrading to a new version, update both the `.sif` file and the corresponding `SNBB_*_SIF` variable. The filename convention `<tool>-<version>.sif` makes it easy to keep multiple versions side-by-side during testing.
- **`bids_post` and `defacing`:** These procedures do not use Apptainer. `bids_post` runs the bundled Python script directly; `defacing` requires `fsl_deface` from FSL to be on `PATH`.

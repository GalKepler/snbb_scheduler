# Deploying the `cat` procedure to the production server

You're already running `bids`/`qsiprep`/`freesurfer`/`qsirecon` on the
production server. This is the checklist to bring the new `cat` procedure
(CAT12 standalone, no MATLAB) over there too. Do these steps **on the
production server**, over SSH — nothing here needs this workstation once
the container image has been transferred.

## 0. Check whether you even need to copy anything

Every existing procedure's `.sif` lives under `/media/storage/apptainer/images/`
on this workstation. If `/media/storage` on the production server is the
*same* network mount (NFS/SMB shared between both machines), you may already
have everything.

```bash
# on the production server
ls -la /media/storage/apptainer/images/          # do the other 6 .sif files already show up here?
ls -la /media/storage/bagpipe/outputs/containers/cat12.sif
```

- If `cat12.sif` is already there and its size matches the one on this
  workstation (`ls -la /media/storage/bagpipe/outputs/containers/cat12.sif`
  here — should be ~10 GB) → **skip step 2**, go straight to step 3.
- If `/media/storage` is a *separate* local disk on each machine (the other
  `.sif` files got there by manual copy, not a shared mount) → do step 2.

## 1. Pull the code

```bash
cd /path/to/snbb_scheduler     # wherever this repo is checked out on the prod server
git fetch origin
git merge origin/main          # once the add-cat12-procedure PR is merged
# or, to test before merging:
git fetch origin add-cat12-procedure
git checkout add-cat12-procedure
```

Confirm the new script arrived and is executable:

```bash
ls -la scripts/snbb_run_cat.sh   # should be present, mode 755
```

## 2. Transfer the CAT12 container image (skip if step 0 said it's already there)

You do **not** need the bagpipe project on the production server — just the
built `.sif` file. From this workstation:

```bash
rsync -avP --progress \
  /media/storage/bagpipe/outputs/containers/cat12.sif \
  <prod-user>@<prod-host>:/media/storage/apptainer/images/cat12-26.0.rc3.sif
```

(`rsync -P` resumes if the ~10 GB transfer drops partway — safer than `scp`
for a file this size.) Note the destination filename: put it in the **same
directory as every other `.sif`** (`/media/storage/apptainer/images/`) and
name it after the CAT version, matching the existing
`qsiprep-1.1.1.sif` / `freesurfer-8.1.0.sif` convention — don't nest it under
a `bagpipe/` path that doesn't exist on the production server.

Verify the transfer:

```bash
# compare sizes/checksums on both ends
sha256sum /media/storage/bagpipe/outputs/containers/cat12.sif                       # here
sha256sum /media/storage/apptainer/images/cat12-26.0.rc3.sif                          # on prod
```

## 2.5. Prepare the custom volumetric atlases

CAT12 can compute its ROI stats against the same atlases qsirecon uses
(`SNBB_ATLASES` in `snbb_run_qsirecon.sh`) via its own `ownatlas` field —
but it needs an uncompressed `.nii` + a `ROIid;ROIabbr;ROIname` `.csv` per
atlas, not the `.nii.gz` + BIDS-Atlas `.tsv` qsirecon reads directly. This is
a one-time conversion, not a per-job step:

```bash
python3 scripts/prepare_cat_atlases.py
# or, if the atlas pack lives somewhere else on this server:
python3 scripts/prepare_cat_atlases.py --atlas-pack-dir /path/to/snbb-atlas-pack/qsirecon_ext \
                                        --out-dir /path/to/derivatives/cat12_atlases
```

This is stdlib-only Python (`gzip`/`csv`), no dependencies — run it directly
wherever the qsirecon atlas pack (`SNBB_ATLASES_DIR`) is reachable on this
server; it's already working for qsirecon there, so it's already reachable.
It converts 45 atlases (~tens of MB total, cheap); the other 14 names in
`SNBB_ATLASES` are QSIRecon's own built-in atlases, not local files, and
aren't included.

Point `snbb_run_cat.sh`'s `SNBB_CAT_ATLAS_DIR` at wherever `--out-dir`
landed (matches the script's own default if you used defaults on both ends).
If you'd rather skip custom atlases for now, just leave `SNBB_CAT_ATLAS_DIR`
unset/nonexistent — the script silently skips `ownatlas` and only runs
CAT12's built-in atlases (neuromorphometrics, lpba40, cobra, thalamus,
thalamic_nuclei, suit).

**This is genuinely untested at this scale** — bagpipe has only ever run
CAT12's `ownatlas` with a single atlas. Verify a real single-session run
(step 6 below) produces the expected `label/catROI_<AtlasName>_<stem>.xml`
per atlas before trusting it for a full cohort — if CAT12 chokes on 45
atlases at once, the fallback is to trim `ATLASES` in
`prepare_cat_atlases.py` down to a smaller set.

## 3. Confirm Apptainer works on the production server

```bash
apptainer --version
apptainer exec /media/storage/apptainer/images/cat12-26.0.rc3.sif echo ok
```

If `apptainer` isn't installed there, this is the one genuinely new
dependency `cat` introduces (the other procedures already required it) —
install it the same way it was installed for the other `.sif` images on that
machine.

## 4. Point the script at the production server's real paths

`snbb_run_cat.sh` follows the same override convention as every other script
in `scripts/` — either edit the defaults in the file directly, or export the
env vars before the scheduler submits (e.g. in the shell/cron/systemd
environment that runs `snbb-scheduler run`):

```bash
SNBB_BIDS_ROOT="${SNBB_BIDS_ROOT:-/media/storage/yalab-dev/snbb_scheduler/bids}"
SNBB_CAT_DERIVATIVES="${SNBB_CAT_DERIVATIVES:-/media/storage/yalab-dev/snbb_scheduler/derivatives/cat12}"
SNBB_CAT_SIF="${SNBB_CAT_SIF:-/media/storage/bagpipe/outputs/containers/cat12.sif}"
SNBB_DEBUG_LOG="${SNBB_DEBUG_LOG:-/media/storage/yalab-dev/snbb_scheduler/logs/cat/debug_submit.log}"
SNBB_CAT_ATLAS_DIR="${SNBB_CAT_ATLAS_DIR:-/media/storage/yalab-dev/snbb_scheduler/derivatives/cat12_atlases}"
```

On the production server:

- `SNBB_BIDS_ROOT` should already match whatever `SNBB_BIDS_ROOT` is set to
  in the other scripts there (`snbb_run_qsiprep.sh` etc.) — don't invent a new
  value, copy theirs.
- `SNBB_CAT_DERIVATIVES` should follow the same `derivatives/<procedure>`
  pattern the other procedures use there (e.g. if their qsiprep output root
  is `<X>/derivatives/qsiprep`, use `<X>/derivatives/cat12`).
- `SNBB_CAT_SIF` → wherever you put the file in step 2/0
  (`/media/storage/apptainer/images/cat12-26.0.rc3.sif` in the example above).
- `SNBB_DEBUG_LOG` → same `logs/<procedure>/debug_submit.log` pattern as the
  others.
- `SNBB_CAT_ATLAS_DIR` → wherever `prepare_cat_atlases.py --out-dir` wrote to
  in step 2.5.

Either edit these five lines directly in `scripts/snbb_run_cat.sh` on the
production checkout, or set the env vars in whatever wraps the scheduler
invocation there — check how the *other* five scripts were overridden on
that server and do the same thing for consistency.

## 5. Add `cat` to the production `config.yaml`

**This is the step most likely to be missed.** If the production server uses
a custom `procedures:` list in its config (commonly deployed to
`/etc/snbb/config.yaml` — check `examples/snbb_config.yaml`'s header comment),
that list **replaces** `DEFAULT_PROCEDURES` wholesale rather than merging
with it. `cat` will silently never run until you add it there too, even
though it's in `config.py`'s built-in defaults.

Find the production config:

```bash
cat /etc/snbb/config.yaml   # or wherever --config points on that server
grep -n "^procedures:" -A2 /etc/snbb/config.yaml
```

If it has its own `procedures:` list, add this block to it (adjust the
`script:` path to match where the repo is actually checked out on that
server — it must be an absolute path, same as the other entries):

```yaml
  - name: cat
    output_dir: cat12
    script: /path/to/snbb_scheduler/scripts/snbb_run_cat.sh
    scope: session
    depends_on: [bids]
    completion_marker:
      - "anat/report/cat_*.xml"
      - "anat/label/catROI_*.xml"
      - "anat/surf/lh.gyrification.*"
      - "anat/surf/lh.depth.*"
      - "anat/surf/lh.fractaldimension.*"
      - "anat/surf/lh.area.*"
```

Insertion position in the list doesn't affect scheduling (only `depends_on`
does), so appending it anywhere is fine.

If the production config has **no** `procedures:` key at all, it's already
using `DEFAULT_PROCEDURES` from `config.py` and picks up `cat` automatically
once you've pulled the code — nothing to edit here.

## 6. Verify before letting it loose on the full cohort

```bash
# 1. does the manifest pick it up?
snbb-scheduler manifest --config /etc/snbb/config.yaml | grep cat

# 2. what would actually be submitted?
snbb-scheduler run --config /etc/snbb/config.yaml --dry-run | grep cat

# 3. run ONE session by hand, outside Slurm, and watch it end-to-end (~1h)
SNBB_BIDS_ROOT=... SNBB_CAT_DERIVATIVES=... SNBB_CAT_SIF=... SNBB_CAT_ATLAS_DIR=... \
  bash scripts/snbb_run_cat.sh sub-XXXX ses-YY
echo $?   # must be 0 — the script checks its own output, since the
          # container always exits 0 regardless of internal success

# 4. once that's clean, submit one real job through the scheduler
snbb-scheduler retry --config /etc/snbb/config.yaml --procedure cat --subject sub-XXXX
snbb-scheduler monitor --config /etc/snbb/config.yaml
```

Only after a real single-session run has produced
`derivatives/cat12/sub-XXXX/ses-YY/anat/{report,label,surf}/...` should you
let the normal scheduled sweep pick up the rest of the cohort. If
`SNBB_CAT_ATLAS_DIR` is set, also check `anat/label/` for one
`catROI_<AtlasName>_<stem>.xml` per atlas in that directory — this is the
part with no prior real-world test at 45-atlases scale (see step 2.5).

## Notes / things that are *not* blockers

- `scripts/snbb_run_cat.sh` has no hardcoded absolute paths outside its five
  `SNBB_*` overrides (unlike `snbb_run_freesurfer.sh`, which calls
  `/home/galkepler/Projects/snbb_scheduler/scripts/snbb_recon_all_helper.py`
  as a literal string with no env-var override at all — a pre-existing,
  unrelated issue if your production checkout lives at a different path).
- The header comments in `snbb_run_cat.sh` reference
  `/media/storage/bagpipe/...` paths for documentation only (tracing back to
  where the invocation contract was ported from) — they're not executed and
  don't need editing.
- `config.py`'s no-config-file fallback defaults (`/data/snbb/...`) don't
  match any real deployment path on either machine — this only matters if
  the production server runs `snbb-scheduler` with no `--config` at all,
  which it presumably doesn't if it's already working today.

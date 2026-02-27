# Slurm Settings

## Config file settings

```yaml
slurm_partition: debug       # omit --partition if empty or omitted
slurm_account:   snbb        # --account=snbb
slurm_mem:       32G         # --mem=32G  (omitted from sbatch if null)
slurm_cpus_per_task: 8       # --cpus-per-task=8  (omitted if null)
slurm_log_dir:   /data/snbb/logs/slurm
```

## Generated sbatch command

For a `qsiprep` job with the above config:

```bash
sbatch \
  --partition=debug \
  --account=snbb \
  --job-name=qsiprep_sub-0001 \
  --mem=32G \
  --cpus-per-task=8 \
  --output=/data/snbb/logs/slurm/qsiprep/qsiprep_sub-0001_%j.out \
  --error=/data/snbb/logs/slurm/qsiprep/qsiprep_sub-0001_%j.err \
  snbb_run_qsiprep.sh sub-0001
```

For a session-scoped procedure like `bids`:

```bash
sbatch \
  --partition=debug \
  --account=snbb \
  --job-name=bids_sub-0001_ses-202411010600 \
  --output=/data/snbb/logs/slurm/bids/bids_sub-0001_ses-202411010600_%j.out \
  --error=/data/snbb/logs/slurm/bids/bids_sub-0001_ses-202411010600_%j.err \
  snbb_run_bids.sh sub-0001 ses-202411010600 /data/snbb/dicom/sub-0001/ses-202411010600
```

Note that session-scoped scripts also receive the `dicom_path` as a third argument.

## Job naming

Job names follow the pattern:

| Scope | Job name |
|---|---|
| `subject` | `<procedure>_<subject>` |
| `session` | `<procedure>_<subject>_<session>` |

## Log file naming

When `slurm_log_dir` is set, logs are placed in per-procedure subdirectories:

```
slurm_log_dir/
├── bids/
│   ├── bids_sub-0001_ses-202411010600_12345.out
│   └── bids_sub-0001_ses-202411010600_12345.err
├── qsiprep/
│   ├── qsiprep_sub-0001_12346.out
│   └── qsiprep_sub-0001_12346.err
└── freesurfer/
    ├── freesurfer_sub-0001_12347.out
    └── freesurfer_sub-0001_12347.err
```

The `%j` placeholder in `--output`/`--error` is replaced by Slurm with the actual job ID.

## Clusters without partitions

Some clusters do not use Slurm partitions. Leave `slurm_partition` empty or omit it:

```yaml
slurm_partition: ""    # --partition flag is NOT added to sbatch
slurm_account:   snbb
```

## Per-invocation overrides

You can override Slurm settings without editing `config.yaml`:

```bash
snbb-scheduler --config config.yaml --slurm-mem 64G run
snbb-scheduler --config config.yaml --slurm-cpus 16 run
snbb-scheduler --config config.yaml --slurm-log-dir /tmp/logs run
```

These take precedence over the config file values.

## Environment variables in scripts

The shell scripts accept their Slurm resource requests from the environment or from embedded `#SBATCH` directives in the script itself. If you need per-procedure resource customization (e.g., freesurfer gets more CPUs than bids), set `#SBATCH` lines inside each script rather than using the global `slurm_mem` / `slurm_cpus_per_task` config. See [Shell Scripts reference](../reference/scripts.md) for details.

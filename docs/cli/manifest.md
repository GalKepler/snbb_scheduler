# `manifest`

Show the current task manifest without submitting anything. Useful for inspecting what `run` would submit.

```bash
snbb-scheduler --config CONFIG manifest
```

## What it shows

The manifest is the list of tasks that need processing: procedures whose dependencies are met but whose own output is not yet complete. It does **not** apply the in-flight filter — it shows everything that would be submitted if you ran with `--force`.

## Example output

```
    subject                session   procedure  priority
   sub-0001  ses-202407110849       bids              0
   sub-0002  ses-202407110849       bids              0
   sub-0003  ses-202407110849       bids_post         1
   sub-0003  ses-202410100845       bids_post         1
   sub-0003                         qsiprep           3
   sub-0003                         freesurfer        4
```

## Columns

| Column | Description |
|---|---|
| `subject` | BIDS subject label |
| `session` | BIDS session label (empty for subject-scoped procedures) |
| `procedure` | Procedure name |
| `priority` | Submission order — lower = submitted first |

## Notes

- `priority` reflects the position of the procedure in `config.procedures` (lower index = lower priority value = submitted first)
- The manifest shows tasks that are **not** complete on the filesystem, regardless of the state file
- To see what `run` would actually submit (after filtering in-flight), use `run --dry-run`

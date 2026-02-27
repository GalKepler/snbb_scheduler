# `retry`

Remove failed state entries so they are re-submitted on the next `run`.

```bash
snbb-scheduler --config CONFIG retry [OPTIONS]
```

## Options

| Option | Description |
|---|---|
| `--procedure NAME` | Limit to failed entries for one procedure (e.g. `bids`) |
| `--subject LABEL` | Limit to failed entries for one subject (e.g. `sub-0002`) |

Both filters can be combined. Without either filter, all failed entries are cleared.

## What it does

1. Loads the state file
2. Finds rows where `status=failed`, optionally filtered by procedure and/or subject
3. Removes those rows and saves the updated state file
4. Logs a `retry_cleared` event to the audit log for each cleared entry

On the next `snbb-scheduler run`, those sessions will be evaluated fresh and re-submitted if their dependencies are met.

## Examples

```bash
# Retry all failed jobs
snbb-scheduler --config config.yaml retry

# Retry only failed bids_post jobs
snbb-scheduler --config config.yaml retry --procedure bids_post

# Retry only sub-0003
snbb-scheduler --config config.yaml retry --subject sub-0003

# Retry bids for sub-0003 only
snbb-scheduler --config config.yaml retry --procedure bids --subject sub-0003
```

## Example output

```
Cleared 3 failed entry/entries. They will be retried on the next run.
```

## Notes

- `retry` only removes `failed` entries; `pending` and `running` entries are not affected
- After clearing, run `status` to confirm the entries are gone
- To also clear `pending` or `running` entries (e.g. after a cluster failure), edit the state file directly with pandas — see [Forcing a Rerun](../guides/forcing-rerun.md)

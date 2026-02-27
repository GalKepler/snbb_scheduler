# `snbb_scheduler.rules`

Rule evaluation logic — decides which procedures need to run for each session.

```python
from snbb_scheduler.rules import build_rules, Rule
```

---

## `Rule` type

```python
Rule = Callable[[pd.Series], bool]
```

A rule is a callable that accepts a session row (`pd.Series`) and returns `True` if the corresponding procedure needs to run for that session.

---

## `build_rules(config, force=False, force_procedures=None)`

Generate a rule function for every procedure in `config`.

```python
rules = build_rules(cfg)
# rules == {"bids": <fn>, "bids_post": <fn>, "qsiprep": <fn>, ...}
```

**Parameters:**
- `config` — `SchedulerConfig` instance
- `force` — if `True`, skip the self-completion check (re-queue already-complete procedures)
- `force_procedures` — if provided (list of names), only skip completion check for those procedures

**Returns:** `dict[str, Rule]` mapping procedure name → rule callable

### Each rule returns `True` when all hold:

1. `row["dicom_exists"]` is `True`
2. All procedures in `proc.depends_on` are complete on disk
3. This procedure's output is **not** yet complete (unless `--force` applies)

### Example

```python
from snbb_scheduler.config import SchedulerConfig
from snbb_scheduler.sessions import discover_sessions
from snbb_scheduler.rules import build_rules

cfg = SchedulerConfig.from_yaml("/etc/snbb/config.yaml")
sessions = discover_sessions(cfg)
rules = build_rules(cfg)

# Check which procedures need to run for the first session
session_row = sessions.iloc[0]
for name, rule in rules.items():
    print(f"{name}: {rule(session_row)}")
```

### Force mode

```python
# Force all procedures
rules = build_rules(cfg, force=True)

# Force only qsiprep
rules = build_rules(cfg, force=True, force_procedures=["qsiprep"])
```

---

## Notes

- Rules are generated fresh on each `build_rules` call — they are lightweight closures
- Rules do not interact with the state file; they only check the filesystem
- The state file filtering (in-flight deduplication) happens separately in `manifest.filter_in_flight`
- `build_manifest` calls `build_rules` internally — you usually don't need to call it directly

# `snbb_scheduler.sessions`

Session discovery and sanitization.

```python
from snbb_scheduler.sessions import discover_sessions, load_sessions
```

---

## `discover_sessions(config)`

Return a DataFrame of all sessions with path information.

```python
sessions = discover_sessions(cfg)
```

**Returns:** `pd.DataFrame` with columns:
- `subject` — BIDS subject label (e.g. `sub-0001`)
- `session` — BIDS session label (e.g. `ses-202407110849`)
- `dicom_path` — path to the DICOM directory (or `None`)
- `dicom_exists` — `True` if the DICOM directory exists
- `<proc>_path` — output path for each configured procedure
- `<proc>_exists` — `True` if that procedure's output path exists

**Discovery modes:**
- **Filesystem walk** (default): scans `config.dicom_root` for `sub-*/ses-*/` directories
- **CSV mode**: when `config.sessions_file` is set, reads the pre-built session list from that CSV

### Example

```python
from snbb_scheduler.config import SchedulerConfig
from snbb_scheduler.sessions import discover_sessions

cfg = SchedulerConfig.from_yaml("/etc/snbb/config.yaml")
sessions = discover_sessions(cfg)

print(sessions[["subject", "session", "bids_exists", "qsiprep_exists"]])
#      subject           session  bids_exists  qsiprep_exists
#   sub-0001  ses-202407110849         True           False
#   sub-0001  ses-202410100845         True            True
#   sub-0002  ses-202407110849        False           False
```

---

## `load_sessions(csv_path)`

Load and sanitize a raw linked_sessions CSV file.

```python
from snbb_scheduler.sessions import load_sessions

df = load_sessions("/data/snbb/linked_sessions.csv")
```

**Expected CSV columns:** `SubjectCode`, `ScanID`, `dicom_path`

**Returns:** Deduplicated DataFrame with sanitized `subject_code`, `session_id`, and `dicom_path` columns.

**Raises:** `ValueError` if required columns are missing.

---

## `sanitize_subject_code(subject_code)`

Strip special characters and zero-pad to 4 digits.

```python
sanitize_subject_code("CLMC-1")   # → "1"  (strips -, then zfill(4) → "0001")
sanitize_subject_code(42)         # → "0042"
sanitize_subject_code("0001")     # → "0001"
```

---

## `sanitize_session_id(session_id)`

Convert to string, strip special characters, and zero-pad to 12 digits.

```python
sanitize_session_id("202407110849")  # → "202407110849"
sanitize_session_id(202407110849)    # → "202407110849"
sanitize_session_id("2024-07-11")    # → "000020240711"
```

---

## Sessions CSV format

When `config.sessions_file` is set, the CSV is read directly without a filesystem walk. The CSV must have been produced by `load_sessions` (which sanitizes `SubjectCode` → `subject_code`, `ScanID` → `session_id`) or match this format:

| Column | Description |
|---|---|
| `SubjectCode` | Raw subject code from the source database |
| `ScanID` | Raw scan/session ID |
| `dicom_path` | Absolute path to the DICOM directory, or blank/NaN if absent |

The scheduler converts these to BIDS labels: `subject = f"sub-{subject_code}"`, `session = f"ses-{session_id}"`.

# ADR-0060: Imported-file archive + per-file source identity

> **Active follow-ups + test plan:**
> [`docs/proposals/TODO-2026-04-29-multi-source-dedup-followups.md`](../proposals/TODO-2026-04-29-multi-source-dedup-followups.md)

**Status:** Accepted

**Date:** 2026-04-29

## Context

Tabular intake (CSV / OFX / QIF / IIF / XLSX / ODS / paste) writes
rows to `staged_transactions` with a coarse source tag — `csv`,
`xlsx`, etc. — and a thin `source_ref` like
`{"session_id": <id>, "row_index": <int>}`. Two failure modes:

1. **Re-import collisions.** Same statement uploaded twice (or
   re-uploaded with minor edits — the user re-saves the spreadsheet
   after fixing a typo) creates two distinct ingest sessions. The
   `session_id` differs, so `source_ref_hash` differs, so the
   staging upsert can't recognize them as the same logical input.
   Every row appears to land twice. The dedup oracle (ADR-0058) can
   catch most of those at intake — but only on content match, not
   provenance match. Two near-identical CSVs with different
   formatting can produce row content that fingerprints differently
   even when the underlying transactions are identical.
2. **No reference back to the actual file.** Once staged, there is
   no way to answer "which CSV did this row come from?" The
   original file isn't archived. Re-opening it to verify a column
   mapping or trace a discrepancy means asking the user to find
   the file again. For multi-sheet inputs (XLSX / ODS), there is
   no record of which sheet or which row position the staging row
   represents.
3. **No round-trip.** ADR-0001 says the ledger directory is
   authoritative. Today, the staged rows don't persist their
   source artifacts there. A DB wipe loses every connection
   between staged content and the file it came from.

User-stated requirement (2026-04-29):

> *"ANY CSV that we import must be retained. We need to have in the
> ledger folder a location where the CSV goes. We should be able to
> pull this CSV back open at any point. We should rename the CSV so
> that we have an ID that can be tracked like
> 00001-original-filename.csv then we can point back to the ACTUAL
> csv. Also what about xlsx and other ods formats that might have
> more than one page? reference the file, the sheet number and the
> row?"*
>
> *"And of course the presence of the csvs in the ledger folder in
> 'imports' or something means they can be reconstructed at any
> time if they need to be."*

## Decision

### 1. Imported file archive

Every imported tabular file is copied verbatim into
`<ledger_dir>/imports/` with a stable monotonic prefix:

```
<ledger_dir>/imports/
  00001-bankone-statement-2026-04.csv
  00002-paypal-2026-04.csv
  00003-bankone-statement-2026-04-corrected.csv
  00004-amex-2026-q1.xlsx
  00005-paste-2026-04-29-091523.csv
```

Filename pattern: `<5-digit-zero-padded-id>-<sanitized-original-name>.<ext>`.
The numeric prefix is the file's stable identity — call it
`file_id`. Sanitization preserves alphanumerics, dashes,
underscores, and dots; everything else collapses to `-`.

The archive directory is created lazily at first import and kept
under version control alongside the ledger (whatever the user
uses — git, plain backups, Dropbox). Users can browse the directory
directly with their OS tools.

### 2. SQLite manifest (cache only)

Migration 063 adds:

```sql
CREATE TABLE imported_files (
  id                 INTEGER PRIMARY KEY,        -- the 00001 prefix
  original_filename  TEXT NOT NULL,              -- "bankone-statement-2026-04.csv"
  archived_path      TEXT NOT NULL UNIQUE,       -- relative to ledger_dir
  content_sha256     TEXT NOT NULL,              -- hex digest, full-file dedup key
  source_format      TEXT NOT NULL,              -- 'csv' | 'ofx' | 'qif' | 'iif' | 'xlsx' | 'ods' | 'paste'
  byte_size          INTEGER NOT NULL,
  imported_at        TEXT NOT NULL,              -- ISO 8601 UTC
  imported_by        TEXT,                       -- auth user id when ADR-0050 is on, else NULL
  notes              TEXT                        -- free-form user note (optional)
);

CREATE UNIQUE INDEX imported_files_sha256_idx ON imported_files(content_sha256);
```

The table is a **cache** in the ADR-0001 / ADR-0015 sense: a wipe
+ rebuild is reconstructible by walking
`<ledger_dir>/imports/` and re-hashing each file. The directory is
the source of truth.

### 3. Refined `source_ref` for tabular intakes

Instead of session-scoped row indices, tabular `source_ref` is
file-scoped:

| Format | source_ref shape |
|---|---|
| CSV / paste / OFX / QIF / IIF | `{"file_id": 1, "row": 42}` |
| XLSX / ODS (multi-sheet) | `{"file_id": 4, "sheet": "Q1", "row": 17}` |

`source_ref_hash` derives from this canonicalized JSON, so
re-importing the same file produces the same hash → the staging
upsert recognizes "same row from same file" and updates in place
rather than duplicating.

Re-importing a *modified* version of the file produces a NEW
`file_id` (different content_sha256), and the rows from that new
file_id stage as new rows. The dedup oracle (ADR-0058) then catches
content collisions across file_ids — so two CSVs with overlapping
rows surface as multi-source observations of the same event, not
as silent dupes.

### 4. Full-file dedup

Before archiving, hash the incoming bytes. If
`imported_files.content_sha256` already has a row, skip the copy
and reuse the existing `file_id` for `source_ref`. The staging
upsert handles row-level idempotency from there.

The user gets a "this file matches a previous import (00001 from
2026-03-15) — proceeding will re-stage from the existing archive"
notification on re-upload. No silent silent-dup; no double-archive.

### 5. Reconstruct invariant

A new reconstruct pass `step25:imported_files`:

1. Lists every regular file under `<ledger_dir>/imports/`.
2. Parses the filename: `<id>-<original>.<ext>` → `file_id`,
   `original_filename`, inferred `source_format` from the
   extension.
3. Computes `content_sha256` and `byte_size`.
4. Upserts the manifest row.

Live → rebuilt drift is acceptable on `imported_at` (timestamp
isn't stored in the filename) and `imported_by` (drops to NULL
on rebuild) and `notes` (drops to NULL). Everything else must
match.

### 6. UI surface

* **Imports page** (`/imports`) — paginated list of every archived
  file with: id, original filename, format, size, hash prefix,
  imported_at, count of staged rows referencing it.
* **Per-file detail** (`/imports/{file_id}`) — file header info
  plus a preview (CSV table view, XLSX sheet picker, etc.) with
  every row annotated by its current staging status (staged / new
  / promoted / dismissed / likely_duplicate).
* **Backreference from any staged row** — `/txn/{token}` and the
  staged-row partial show "from import #00003: bankone-statement-…"
  as a link.

## Consequences

* The ledger folder gains a single source of truth for every input
  artifact. Re-opening, re-running, and offline review are all
  possible without re-uploading.
* Re-imports are idempotent at the file level (same bytes → same
  `file_id`) and at the row level (same `(file_id, sheet, row)` →
  same `source_ref_hash`). The "same CSV with one column changed"
  edge case lands as a NEW file_id with the dedup oracle catching
  per-row overlap.
* Multi-sheet XLSX / ODS inputs become coherent: each sheet is a
  separate row-namespace under the same `file_id`.
* DB wipe → reconstruct rebuilds the manifest from the archive.
  ADR-0001 + ADR-0015 invariants hold.
* Disk usage grows linearly with imports. For statement-class
  files (KB to MB) this is negligible; for unusual cases (huge
  XLSX) the user can prune via a future "archive cleanup" tool —
  but only after confirming the staged rows pointing at the file
  have all reached `promoted` or `dismissed`.

## Compliance

Required follow-ups (each updates this ADR with a checkbox):

* [x] Migration 063 adds `imported_files`.
* [x] `archive.py` module — `archive_file()`, `get_archived_path()`,
  `list_archived()`, `compute_sha256()`, sanitize filename,
  monotonic id mint via SQLite sequence.
* [x] Reconstruct step `step25:imported_files` walks
  `<ledger_dir>/imports/` and rebuilds the manifest.
* [x] Unit tests covering archive + reconstruct + filename
  collision + content dedup paths.
* [x] Wire CSV intake (`_db.import_into_staging`) through the
  archive helper; replace `source_ref` shape with
  `{"file_id": <int>, "row": <int>}`. Landed 2026-04-29 —
  `ImportService.register_upload` archives upload bytes;
  `_mirror_to_staging` joins `sources → imports → imported_files`
  on `content_sha256` to resolve the file_id.
* [x] Wire OFX / QIF / IIF intake similarly. (Same code path —
  every importer source goes through `insert_raw_row →
  _mirror_to_staging`.)
* [x] Wire XLSX / ODS intake including the `sheet` field
  (`source_ref.sheet` populated when `sources.sheet_name` is set).
* [x] Wire paste intake — incoming text is archived as
  `<id>-paste-<timestamp>.csv` so even ad-hoc clipboard imports
  are reconstructible. Landed 2026-04-29 —
  `IntakeService.stage_paste` with `archived_file_id` kwarg.
* [x] `/imports` listing route + template.
* [x] `/imports/{file_id}` detail / preview route.
* [x] `/imports/{file_id}/download` serves archived bytes.
* [x] Staged-row partial backref link to /imports/{file_id}.
* [ ] Archive prune CLI (only after confirming all referencing
  staged rows are terminal).

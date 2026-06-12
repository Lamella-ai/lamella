---
audience: agents
read-cost-target: 100 lines
authority: informative
status: Active Development
cross-refs: docs/adr/0001-ledger-as-source-of-truth.md, docs/adr/0009-card-binding-as-hypothesis.md, docs/adr/0011-autocomplete-everywhere.md, docs/adr/0015-reconstruct-capability-invariant.md
last-derived-from-code: 2026-04-26
---
# Notes

## Summary

User-authored notes with active windows, entity / card / account scope, and overrides surfaced into classify context.

## Owned routes

| Method | Path | Handler | File:line |
|---|---|---|---|
| GET | `/note` | `note_page` | `src/lamella/web/routes/note.py:27` |
| POST | `/note` | `create_note` | `src/lamella/web/routes/note.py:33` |

## Owned templates

- `src/lamella/web/templates/note.html`
- `src/lamella/web/templates/partials/note_list.html`

## Owned source files

- `src/lamella/features/notes/service.py`
- `src/lamella/features/notes/writer.py`

## Owned tests

- `tests/test_note_capture.py`
- `tests/test_notes_active_window.py`
- `tests/test_step7_note_coverage.py`

## ADR compliance

- **ADR-0001**: Notes write `custom "note"` directives to `connector_config.bean`;
  reconstruct step 16 (`step16_notes.py`) rebuilds the `notes` table from them.
  The `notes` table is now state, not ephemeral cache.
- **ADR-0004**: `append_note` delegates to `append_custom_directive` which
  runs bean-check and rolls back on new errors.
- **ADR-0003**: All metadata keys use `lamella-note-*` prefix.
- **ADR-0009**: `card_override=True` on a note is one of the four listed
  override signals that can displace the card-entity hypothesis.
- **ADR-0011**: Entity and merchant hint inputs on `/note` are text inputs
  (not selects); entity picker backed by known_entities list from settings.
- **ADR-0015**: step16 reconstruct is wired. Notes survive a DB wipe.

## Current state


### Compliant ADRs
- **ADR-0001**: Notes write `custom "note"` directives to `connector_config.bean`;
  reconstruct step 16 (`step16_notes.py`) rebuilds the `notes` table from them.
  The `notes` table is now state, not ephemeral cache.
- **ADR-0004**: `append_note` delegates to `append_custom_directive` which
  runs bean-check and rolls back on new errors.
- **ADR-0003**: All metadata keys use `lamella-note-*` prefix.
- **ADR-0009**: `card_override=True` on a note is one of the four listed
  override signals that can displace the card-entity hypothesis.
- **ADR-0011**: Entity and merchant hint inputs on `/note` are text inputs
  (not selects); entity picker backed by known_entities list from settings.
- **ADR-0015**: step16 reconstruct is wired. Notes survive a DB wipe.

### Known violations
- `resolved_txn` and `resolved_receipt` columns exist in the schema and
  are written to the ledger, but the three-way (note + txn + receipt) link
  is not wired end-to-end. Those fields are reserved plumbing.
- No edit route for note body. Once captured, body is immutable; only
  AI-derived hints can be updated via `update_hints`.
- Reconciliation loop: no scheduled scan matches open notes to newly
  arrived transactions automatically.

## Known gaps

- `resolved_txn` and `resolved_receipt` columns exist in the schema and
  are written to the ledger, but the three-way (note + txn + receipt) link
  is not wired end-to-end. Those fields are reserved plumbing.
- No edit route for note body. Once captured, body is immutable; only
  AI-derived hints can be updated via `update_hints`.
- Reconciliation loop: no scheduled scan matches open notes to newly
  arrived transactions automatically.

## Remaining tasks

- Wire the three-way reconciliation loop: scan open notes against newly
  classified transactions and auto-link where `txn_hash` is resolvable.
- Add a note body edit route (requires a new ledger directive write to
  supersede the original, not an in-place rewrite, notes are append-only
  in the ledger).
- Expose entity picker backed by the `entities` table rather than the
  comma-separated `known_entities` settings string.
- Pagination on the recent list (currently hardcoded at 50 in `list()`).

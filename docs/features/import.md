---
audience: agents
read-cost-target: 110 lines
authority: informative
status: Active Development
cross-refs: docs/adr/0008-unconditional-dedup.md, docs/adr/0019-transaction-identity-use-helpers.md, docs/adr/0001-ledger-as-source-of-truth.md, docs/adr/0004-bean-check-after-every-write.md, docs/specs/LEDGER_LAYOUT.md, docs/specs/NORMALIZE_TXN_IDENTITY.md
last-derived-from-code: 2026-04-26
---
# Import

## Summary

Spreadsheet (CSV/XLSX/ODS) and pasted-text import pipeline; lands in connector_imports/<year>.bean via the staging surface.

## Owned routes

| Method | Path | Handler | File:line |
|---|---|---|---|
| GET | `/import` | `import_index` | `src/lamella/web/routes/import_.py:97` |
| POST | `/import` | `upload_file` | `src/lamella/web/routes/import_.py:110` |
| DELETE | `/import/{import_id}` | `hard_delete` | `src/lamella/web/routes/import_.py:510` |
| GET | `/import/{import_id}` | `import_detail` | `src/lamella/web/routes/import_.py:146` |
| GET | `/import/{import_id}.json` | `import_detail_json` | `src/lamella/web/routes/import_.py:165` |
| POST | `/import/{import_id}/cancel` | `cancel` | `src/lamella/web/routes/import_.py:501` |
| GET | `/import/{import_id}/classify` | `classify_page` | `src/lamella/web/routes/import_.py:191` |
| POST | `/import/{import_id}/classify` | `classify_apply` | `src/lamella/web/routes/import_.py:212` |
| POST | `/import/{import_id}/commit` | `commit` | `src/lamella/web/routes/import_.py:487` |
| GET | `/import/{import_id}/ingest` | `ingest_page` | `src/lamella/web/routes/import_.py:349` |
| POST | `/import/{import_id}/ingest` | `ingest_apply` | `src/lamella/web/routes/import_.py:367` |
| GET | `/import/{import_id}/map` | `map_page` | `src/lamella/web/routes/import_.py:240` |
| POST | `/import/{import_id}/map` | `map_apply` | `src/lamella/web/routes/import_.py:307` |
| GET | `/import/{import_id}/preview` | `preview_page` | `src/lamella/web/routes/import_.py:422` |
| POST | `/import/{import_id}/preview/recategorize` | `preview_recategorize` | `src/lamella/web/routes/import_.py:464` |
| GET | `/intake` | `intake_page` | `src/lamella/web/routes/intake.py:61` |
| POST | `/intake/preview` | `intake_preview` | `src/lamella/web/routes/intake.py:79` |
| POST | `/intake/stage` | `intake_stage` | `src/lamella/web/routes/intake.py:122` |
| GET | `/review/ignored` | `staged_review_ignored` | `src/lamella/web/routes/staging_review.py:429` |
| GET | `/review/staged` | `staged_review_page` | `src/lamella/web/routes/staging_review.py:372` |
| POST | `/review/staged/ask-ai-modal` | `staged_review_ask_ai_modal` | `src/lamella/web/routes/staging_review.py:1079` |
| POST | `/review/staged/classify` | `staged_review_classify` | `src/lamella/web/routes/staging_review.py:490` |
| POST | `/review/staged/classify-group` | `staged_review_classify_group` | `src/lamella/web/routes/staging_review.py:837` |
| POST | `/review/staged/dismiss` | `staged_review_dismiss` | `src/lamella/web/routes/staging_review.py:382` |
| POST | `/review/staged/restore` | `staged_review_restore` | `src/lamella/web/routes/staging_review.py:402` |

## Owned templates

- `src/lamella/web/templates/import.html`
- `src/lamella/web/templates/import_classify.html`
- `src/lamella/web/templates/import_detail.html`
- `src/lamella/web/templates/import_ingest.html`
- `src/lamella/web/templates/import_map.html`
- `src/lamella/web/templates/import_preview.html`
- `src/lamella/web/templates/intake.html`
- `src/lamella/web/templates/partials/import_row.html`
- `src/lamella/web/templates/partials/import_source_card.html`
- `src/lamella/web/templates/staging_ignored.html`
- `src/lamella/web/templates/staging_review.html`

## Owned source files

- `src/lamella/features/import_/_db.py`
- `src/lamella/features/import_/_pandas_helpers.py`
- `src/lamella/features/import_/_structured.py`
- `src/lamella/features/import_/categorize.py`
- `src/lamella/features/import_/classify.py`
- `src/lamella/features/import_/emit.py`
- `src/lamella/features/import_/ledger_dedup.py`
- `src/lamella/features/import_/mapping.py`
- `src/lamella/features/import_/preview.py`
- `src/lamella/features/import_/service.py`
- `src/lamella/features/import_/sources/amazon_merch.py`
- `src/lamella/features/import_/sources/amazon_purchases.py`
- `src/lamella/features/import_/sources/amazon_seller.py`
- `src/lamella/features/import_/sources/cards.py`
- `src/lamella/features/import_/sources/chase.py`
- `src/lamella/features/import_/sources/ebay.py`
- `src/lamella/features/import_/sources/eidl.py`
- `src/lamella/features/import_/sources/generic.py`
- `src/lamella/features/import_/sources/iif.py`
- `src/lamella/features/import_/sources/ofx.py`
- `src/lamella/features/import_/sources/paypal.py`
- `src/lamella/features/import_/sources/qif.py`
- `src/lamella/features/import_/sources/wf.py`
- `src/lamella/features/import_/staging/intake.py`
- `src/lamella/features/import_/staging/integrity_check.py`
- `src/lamella/features/import_/staging/matcher.py`
- `src/lamella/features/import_/staging/preflight.py`
- `src/lamella/features/import_/staging/reboot.py`
- `src/lamella/features/import_/staging/reboot_writer.py`
- `src/lamella/features/import_/staging/retrofit.py`
- `src/lamella/features/import_/staging/review.py`
- `src/lamella/features/import_/staging/rule_mining.py`
- `src/lamella/features/import_/staging/service.py`
- `src/lamella/features/import_/staging/transfer_writer.py`
- `src/lamella/features/import_/transfers.py`

## Owned tests

- `tests/test_field_map_setup_panel.py`
- `tests/test_import_apply.py`
- `tests/test_importer_categorize.py`
- `tests/test_importer_classify.py`
- `tests/test_importer_emit.py`
- `tests/test_importer_emit_identity.py`
- `tests/test_importer_end_to_end.py`
- `tests/test_importer_ledger_dedup.py`
- `tests/test_importer_mapping.py`
- `tests/test_importer_preview.py`
- `tests/test_importer_sources_generic.py`
- `tests/test_importer_sources_iif.py`
- `tests/test_importer_sources_ofx.py`
- `tests/test_importer_sources_qif.py`
- `tests/test_importer_sources_wf.py`
- `tests/test_importer_structured_pipeline.py`
- `tests/test_importer_transfers.py`
- `tests/test_staging_cross_source_integration.py`
- `tests/test_staging_intake.py`
- `tests/test_staging_integrity_check.py`
- `tests/test_staging_matcher.py`
- `tests/test_staging_reboot.py`
- `tests/test_staging_reboot_writer.py`
- `tests/test_staging_retrofit.py`
- `tests/test_staging_review.py`
- `tests/test_staging_review_route.py`
- `tests/test_staging_rule_mining.py`
- `tests/test_staging_service.py`

## ADR compliance


- **ADR-0008**: dedup by `(source, source_ref_hash)` on ingest; ledger dedup
  by natural-key hash on commit.
- **ADR-0019**: `render_transaction` uses `mint_txn_id()` and paired source
  keys; never reads raw `meta.get("lamella-import-id")`.
- **ADR-0001**: `connector_imports/<year>.bean` is the durable record;
  SQLite staging rows are cache; `cleanup_terminal` prunes them safely.
- **ADR-0004**: every emit wraps in per-file backup + bean-check + restore.

## Current state


### Upload pipeline state machine

`ImportService` (`importer/service.py`) owns the state machine:
`uploaded → classified → mapped → ingested → categorized → previewed → committed`
(plus `cancelled` and `error`). Each transition corresponds to one route POST.

- **classify**: `importer/classify.py` heuristically matches filename + column
  signatures to a `source_class` string (e.g. `wf_annotated`, `paypal`,
  `amazon_seller`, `generic_csv`). Unknown shapes land as `generic_csv`/`generic_xlsx`
  and route to AI column mapping.
- **map**: `importer/mapping.py` resolves column positions. AI assist available
  for `generic_*` classes.
- **ingest**: per-`source_class` ingesters populate `raw_rows` + `sources` in SQLite.
  `importer/ledger_dedup.py` cross-references existing ledger postings by
  natural-key hash to mark rows as `deduped`.
- **categorize**: `importer/categorize.py` runs the AI cascade (Haiku primary,
  Opus fallback) against each ingested row, writes decisions to `categorizations`.
  Gated on classification philosophy (ADR-0018 equivalent for imports).
- **commit** (`ingest` route): calls `emit.emit_to_ledger()`.

### Emit discipline (`importer/emit.py`)

`emit_to_ledger` renders chunks per calendar year, appends to
`connector_imports/<year>.bean`, updates `_all.bean`, adds the `main.bean`
include if absent, then runs `run_bean_check`. On any failure it restores
every file from per-file pre-write backups byte-identically (ADR-0004).

**Identity stamping (Phase 7c, ADR-0019):** `render_transaction` emits:
- `lamella-txn-id` at transaction meta: reuses `cat_lamella_txn_id` from
  `categorizations` when present (keeps AI `input_ref` and on-disk id identical);
  mints fresh via `mint_txn_id()` for paths that bypass categorize.
- `lamella-source-0: "csv"` + `lamella-source-reference-id-0: "<ref>"` on
  the source-side posting, uses source-provided `transaction_id` when present;
  falls back to `nk-<sha256[:32]>` natural-key hash of (date, amount, payee,
  description) so reconstruct from a wiped DB can re-derive stable identity.

Retired identifiers no longer emitted: `lamella-import-id` (was a SQLite PK,
reconstruct violation), `lamella-import-source`, `lamella-import-txn-id`.
Legacy on-disk content remains readable via `_legacy_meta.normalize_entries`.

### Staging surface (`staging/service.py`)

`StagingService` provides source-agnostic CRUD for `staged_transactions`:
- `stage(source, source_ref, ...)`: insert-or-update on
  `(source, source_ref_hash)`; conflict updates mutable fields, preserves status.
- `record_decision(staged_id, account, confidence, decided_by, ...)`: upsert
  on `staged_decisions`; advances `new → classified`.
- `record_pair(kind, confidence, a_staged_id, ...)`: records transfer/duplicate
  pairs; advances both sides to `matched`.
- `mark_promoted(staged_id, promoted_to_file)`: terminal state; emit calls
  this after writing to `.bean` so the staging table reflects what landed.
- `dismiss` / `restore`: soft ignore with reversal. Dismissed rows are NEVER
  auto-deleted (they are reconciliation evidence).
- `cleanup_terminal(older_than_days=30)`: deletes `promoted` rows older than
  the threshold; `dismissed` rows are excluded.

Dedup key: `(source, source_ref_hash)` where `source_ref_hash` is SHA1 of
`json.dumps({"source": source, "ref": ref}, sort_keys=True)`. Stable across
fetches. (ADR-0008)

### Three-bucket rule (per `docs/specs/LEDGER_LAYOUT.md`)

Imported rows land in exactly one of three buckets:
1. **deduped**, matching ledger posting found; row is skipped at emit.
2. **transfer**, detected as one leg of an internal transfer; paired via
   `staged_pairs(kind='transfer')` and emitted via `Assets:Clearing:Transfers`.
3. **expense/income**, normal posting; emitted to counterparty account
   (AI-classified or user-chosen).

### Compliant ADRs

- **ADR-0008**: dedup by `(source, source_ref_hash)` on ingest; ledger dedup
  by natural-key hash on commit.
- **ADR-0019**: `render_transaction` uses `mint_txn_id()` and paired source
  keys; never reads raw `meta.get("lamella-import-id")`.
- **ADR-0001**: `connector_imports/<year>.bean` is the durable record;
  SQLite staging rows are cache; `cleanup_terminal` prunes them safely.
- **ADR-0004**: every emit wraps in per-file backup + bean-check + restore.

### Known violations

- `importer/emit.py` `PAYMENT_METHOD_PATTERNS` and `_entity_from_path` contain
  placeholder entity names (`Acme`, `WidgetCo`, `Rentals`) that should be driven
  from ledger `entities` table at runtime rather than hard-coded. Current code
  is user-specific scaffolding, not a general-purpose implementation.

## Known gaps


- `importer/emit.py` `PAYMENT_METHOD_PATTERNS` and `_entity_from_path` contain
  placeholder entity names (`Acme`, `WidgetCo`, `Rentals`) that should be driven
  from ledger `entities` table at runtime rather than hard-coded. Current code
  is user-specific scaffolding, not a general-purpose implementation.

## Remaining tasks


- Replace hard-coded `PAYMENT_METHOD_PATTERNS` / `_entity_from_path` with
  runtime lookups from the `entities` + `accounts_meta` tables.
- Surface per-row staging decisions in the review queue so paste-intake rows
  reach the same human-confirmation step as SimpleFIN rows.
- AI surfacing for staged CSV rows (Phase 7c bridge referenced in
  `emit.py` "NEXTGEN Phase C2b" comments), transfer-writer integration for
  multi-leg cross-source transactions.

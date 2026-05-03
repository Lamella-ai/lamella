---
audience: agents
read-cost-target: 120 lines
authority: informative
status: Active Development
cross-refs: docs/adr/0001-ledger-as-source-of-truth.md, docs/adr/0004-bean-check-after-every-write.md, docs/adr/0008-unconditional-dedup.md, docs/adr/0015-reconstruct-capability-invariant.md, docs/adr/0020-adapter-pattern-for-external-data-sources.md, docs/adr/0044-paperless-lamella-custom-fields.md, docs/adr/0061-documents-abstraction-and-ledger-v4.md, docs/adr/0062-tag-driven-workflow-engine.md, docs/adr/0063-bidirectional-document-transaction-matching.md
last-derived-from-code: 2026-05-02
---
# Documents

> Renamed from "Receipts" per ADR-0061. The feature handles **all** Paperless-ngx
> document kinds — receipts, invoices, statements, contracts, warranties,
> notices — not just point-of-sale receipts. URLs, source files, and class
> names still use the legacy `receipt`/`receipts` spelling pending the Phase 4
> mechanical rename; the abstraction is now document-oriented in the ledger
> directive vocabulary (`document-link` / `document-dismissed`) and in the
> user-facing language.

## Summary

Document fetch + match: link Paperless docs to ledger transactions; surface
needed-document queue. Source of truth for the document itself is Paperless;
the link record and dismissal state live in the Beancount ledger as
`document-*` custom directives (with backward-compatible reads of legacy
`receipt-*` directives).

## Phase 1 status (ADR-0061)

Landed at f73a2d01 (2026-05-02):

- Ledger v4 introduces the `document-*` directive vocabulary: writers emit
  `document-link` and `document-dismissed`; readers accept both the new
  forms **and** the legacy `receipt-link` / `receipt-dismissed` forms so
  no migration of existing ledger files is required.
- Custom-field metadata follows the `Lamella_*` PascalCase tag namespace
  per ADR-0044 + ADR-0061 — e.g. `Lamella_DocumentKind`, `Lamella_DocumentDate`,
  `Lamella_DocumentVendor`. Old kebab-case `lamella-*` keys remain the
  on-ledger metadata convention; the PascalCase form is for Paperless
  custom fields specifically.
- The `src/lamella/features/receipts/` directory has not been renamed in
  Phase 1; `ReceiptLinker` etc. retain their legacy class names. Phase 4
  performs the mechanical class/function/file rename inside the existing
  directory, and the directory itself may be renamed in a later cleanup
  phase.
- **Pending:**
  - ADR-0062 (tag-driven workflows): document tags drive automated
    workflows (e.g. "warranty expiring" review, "tax-deductible" routing).
    Not yet implemented.
  - ADR-0063 (bidirectional matching): match documents → transactions
    AND transactions → documents in a unified candidate pipeline. Today
    we only match transactions → documents.

## Owned routes

| Method | Path | Handler | File:line |
|---|---|---|---|
| GET | `/receipts` | `receipts_page` | `src/lamella/web/routes/receipts.py:89` |
| GET | `/receipts/needed` | `needed_page` | `src/lamella/web/routes/receipts_needed.py:193` |
| GET | `/receipts/needed/partial` | `needed_partial` | `src/lamella/web/routes/receipts_needed.py:234` |
| POST | `/receipts/needed/{txn_hash}/dismiss` | `dismiss_txn` | `src/lamella/web/routes/receipts_needed.py:480` |
| POST | `/receipts/needed/{txn_hash}/link` | `link_txn_to_doc` | `src/lamella/web/routes/receipts_needed.py:282` |
| POST | `/receipts/needed/{txn_hash}/undismiss` | `undismiss_txn` | `src/lamella/web/routes/receipts_needed.py:527` |
| POST | `/receipts/{doc_id}/link` | `manual_link` | `src/lamella/web/routes/receipts.py:196` |
| GET | `/txn/{token}/receipt-section` | `receipt_section` | `src/lamella/web/routes/txn_receipt.py:222` |
| GET | `/txn/{token}/receipt-search` | `receipt_search` | `src/lamella/web/routes/txn_receipt.py:255` |
| POST | `/txn/{token}/receipt-link` | `receipt_link` | `src/lamella/web/routes/txn_receipt.py:344` |
| POST | `/txn/{token}/receipt-unlink` | `receipt_unlink` | `src/lamella/web/routes/txn_receipt.py:401` |

URL paths still use `/receipts/` and `receipt-*` segments; these are stable
URLs and will not change in Phase 4 (we add `/documents/` aliases later, if
at all — to be decided when ADR-0062 lands).

## Owned templates

- `src/lamella/web/templates/partials/_txn_receipt_search_results.html`
- `src/lamella/web/templates/partials/_txn_receipt_section.html`
- `src/lamella/web/templates/partials/receipts_needed_body.html`
- `src/lamella/web/templates/receipts.html`
- `src/lamella/web/templates/receipts_needed.html`

## Owned source files

- `src/lamella/features/receipts/auto_match.py`
- `src/lamella/features/receipts/dismissals_writer.py`
- `src/lamella/features/receipts/hunt.py`
- `src/lamella/features/receipts/linker.py`
- `src/lamella/features/receipts/matcher.py`
- `src/lamella/features/receipts/needs_queue.py`
- `src/lamella/features/receipts/txn_matcher.py`

## Owned tests

- `tests/test_ai_receipt_match.py`
- `tests/test_phase_i_receipt_context.py`
- `tests/test_receipt_fetcher.py`
- `tests/test_receipt_matcher.py`
- `tests/test_search_receipt_hunt.py`
- `tests/test_staged_receipt_link.py`
- `tests/test_step1_receipt_dismissals.py`

## ADR compliance

- ADR-0001: link and dismissal state written to ledger as `custom "..."` directives; SQLite rows are cache
- ADR-0004: `ReceiptLinker.link()` baseline-tolerant bean-check with byte-exact rollback
- ADR-0008: `receipt_links` dedup key is `(paperless_id, txn_hash)`; `ON CONFLICT DO UPDATE` prevents double-links
- ADR-0015: dismissals written as `custom "document-dismissed"` directives (legacy `custom "receipt-dismissed"` still read); reconstruct reads them back
- ADR-0044: Paperless custom fields use the `Lamella_*` PascalCase namespace
- ADR-0061: ledger v4 `document-*` directive vocabulary on writes; legacy `receipt-*` accepted on reads

## Current state


`ReceiptLinker` (`linker.py`) writes document links atomically:
1. Snapshot `connector_links.bean` and `main.bean` bytes before mutation.
2. Capture baseline bean-check output (`run_bean_check_vs_baseline`: only new errors vs. baseline fail).
3. Append a `custom "document-link"` block to `connector_links.bean` (legacy `custom "receipt-link"` directives are still read on the load path).
4. Upsert `receipt_links` DB row: `ON CONFLICT (paperless_id, txn_hash) DO UPDATE`.
5. Run baseline-tolerant bean-check; on failure, restore both files from snapshots and delete the DB row (ADR-0004).

`dismissals_writer.py` (`append_dismissal`) stamps `custom "document-dismissed"` directives via `append_custom_directive(run_check=True)`. Dismissal state is reconstruct-capable: `read_custom_directives` in the reconstruct path rebuilds `receipt_dismissals` from ledger (ADR-0015), reading both the new `document-dismissed` form and the legacy `receipt-dismissed` form.

`matcher.py` (`MatchCandidate`, `_txn_signed_totals`) provides the amount-based candidate filter. Matches all `_RECEIPT_TARGET_ROOTS` (`Expenses`, `Income`, `Liabilities`, `Equity`, `Assets`), widened from Expenses-only to handle ATM deposit slips and owner-reimbursement receipts.

`txn_matcher.py` (`find_paperless_candidates`) queries `paperless_doc_index` by amount ± tolerance and date ± window, returns scored candidates.

`needs_queue.py` builds the "expenses needing finalization" queue: walks ledger `Transaction` entries that have at least one `Expenses:*` posting, have no `receipt_links` row, and are not dismissed. Excludes `NON_RECEIPT_PATTERNS` (transfers, card payments, ATM) by narration/payee substring match.

`auto_match.py` (`run_auto_match`) is the post-ingest sweep: walks unlinked transactions in a 60-day window, calls `find_paperless_candidates`, auto-links when top score ≥ 0.90. Lower-scored candidates surface in `/search/receipt-hunt` for user pick.

`hunt.py` (`run_hunt`) is the interactive batch hunt: submitted as a `Job` via `JobRunner`; yields per-txn progress events; calls `ReceiptLinker.link()` for each confirmed match (ADR-0006).

`txn_receipt.py` (commit 9ebcf6d) adds a per-txn document-attach surface under `/txn/{token}/receipt-*` that works for both staged rows and ledger txns. Both forms key off the immutable `lamella-txn-id` (UUIDv7), so `ReceiptLinker`, which keys its `custom "document-link"` directives by that same id, needs no changes; the staged row's UUIDv7 is passed verbatim as `txn_hash`. Unlink goes through `remove_receipt_link` in `linker.py:56`, which snapshots both ledger files, strips a single `custom "document-link"` (or legacy `custom "receipt-link"`) block, runs baseline-tolerant bean-check, and restores on new errors (mirrors `ReceiptLinker.link()`'s ADR-0004 contract). The staged-row template lazy-loads a "Receipt" card via `hx-get` against `receipt-section`; the partial lists linked docs with Unlink buttons and offers a search box that returns candidates with Link buttons. The user-facing card label will be generalized to "Document" in the Phase 4 template pass.

Hash format: `lamella-paperless-hash` written as `md5:<hex>` or `sha256:<hex>`, algorithm-prefixed so both generations coexist. Current source is `original_checksum` from Paperless metadata endpoint (MD5). `cached_paperless_hash()` in `paperless/lookups.py` reads `paperless_doc_index.original_checksum`.

### Compliant ADRs
- ADR-0001: link and dismissal state written to ledger as `custom "..."` directives; SQLite rows are cache
- ADR-0004: `ReceiptLinker.link()` baseline-tolerant bean-check with byte-exact rollback
- ADR-0008: `receipt_links` dedup key is `(paperless_id, txn_hash)`; `ON CONFLICT DO UPDATE` prevents double-links
- ADR-0015: dismissals written as `custom "document-dismissed"` directives; reconstruct reads them back (legacy `receipt-dismissed` also accepted)
- ADR-0044: Paperless custom fields use `Lamella_*` PascalCase namespace
- ADR-0061: writers emit `document-*` directives; readers accept both new and legacy `receipt-*` directives (Phase 1 landed at f73a2d01)

### Known violations
- ADR-0020: `find_paperless_candidates` queries `paperless_doc_index` directly; no `DocumentStorePort` abstracts the Paperless dependency from matching logic (medium-high)
- ADR-0062: tag-driven workflows not yet implemented (whole-ADR pending)
- ADR-0063: matching is one-directional (txn → document); document → txn matching not yet implemented (whole-ADR pending)

## Known gaps

- ADR-0020: `find_paperless_candidates` queries `paperless_doc_index` directly; no `DocumentStorePort` abstracts the Paperless dependency from matching logic (medium-high)
- ADR-0061 Phase 4: mechanical rename of `ReceiptLinker → DocumentLinker`, `receipt_links` → `document_links` etc. inside the existing `features/receipts/` directory still pending; new code added here should prefer document-oriented identifiers where it doesn't fight the surrounding code
- ADR-0062: tag-driven workflows not yet implemented
- ADR-0063: bidirectional matching not yet implemented

## Remaining tasks


1. [ ] Move document candidate search behind a `DocumentStorePort`: decouple `txn_matcher.find_paperless_candidates` from `paperless_doc_index` schema; enables testing without a seeded DB (ADR-0020, medium-high).
2. [ ] `NON_RECEIPT_PATTERNS` is a hard-coded tuple: move to a `Settings` knob or a user-editable table so patterns can be extended without a deploy (medium). Rename to `NON_DOCUMENT_PATTERNS` as part of Phase 4.
3. [ ] Auto-match `DEFAULT_WINDOW_DAYS = 60` is not configurable: expose as a `Settings` field (low).
4. [ ] `receipt_dismissals` DB rows are not pruned when the underlying transaction is edited and its hash changes, the stale row persists (harmless but noisy); add a staleness-cleanup pass on the reconstruct verify path (low).
5. [ ] ADR-0061 Phase 4: mechanical rename of `ReceiptLinker → DocumentLinker`, `receipt_links` → `document_links`, template label "Receipt" → "Document", etc. (medium, owned by Phase 4 worker).
6. [ ] ADR-0062: design + land the tag-driven workflow engine (large, separate workstream).
7. [ ] ADR-0063: design + land bidirectional document↔transaction matching (large, separate workstream).

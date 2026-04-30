# ADR-0008: Dedup Unconditionally

- **Status:** Accepted
- **Date:** 2026-04-26
- **Author:** AJ Quick
- **Related:** [ADR-0019](0019-transaction-identity-use-helpers.md), `CLAUDE.md` ("Dedup unconditionally"), `src/lamella/identity.py`, `src/lamella/duplicates/cleaner.py`

## Context

Three ingest paths can produce the same underlying event:
SimpleFIN bridge, CSV import, and paste/manual entry. A SimpleFIN
fetch that overlaps a prior fetch date window, a CSV re-import, or
a manual entry for a transaction that later clears the bank feed
can all write duplicate transactions to the ledger.

Duplicates produce doubled balances, doubled expense totals, and
misclassified review queue items. They are not detected by
`bean-check`. The user cannot reliably spot them in a large ledger.

Dedup requires stable identifiers that survive ledger edits. The
SimpleFIN bridge id is stable (the external system owns it). For
CSV and paste, the natural-key hash over `(date, amount, payee,
description)` is reconstruct-stable. SQLite PKs are NOT acceptable, because they
are lost on reconstruct.

## Decision

Every ingest path MUST dedup before writing. No ingest function
MAY write a transaction whose source reference already exists in
the ledger.

Specific obligations:

1. **SimpleFIN:** dedup by SimpleFIN bridge id, read via
   `identity.find_source_reference(entry, "simplefin")`. MUST NOT
   read `txn.meta.get("lamella-simplefin-id")` directly (legacy
   shape; see [ADR-0019](0019-transaction-identity-use-helpers.md) for the identity helper contract).
2. **CSV/paste:** dedup by `(paperless_id, txn_hash)` for
   receipt-linked imports; by natural-key hash for others.
3. **Receipt links:** dedup by `(paperless_id, txn_hash)` in
   `connector_links.bean`.
4. **Paperless writebacks:** dedup by `(paperless_id, kind,
   dedup_key)` in `paperless_writeback_log`.
5. The cleaner (`duplicates/cleaner.py`) handles retroactive dedup
   (block removal). It MUST detect both legacy
   `lamella-simplefin-id` lines AND post-Phase-7 paired source meta
   lines via `_extract_sfid_from_block`. MUST NOT add a third
   raw-text scanner. Use `find_source_reference` instead.

## Consequences

### Positive
- Doubled balances are structurally prevented at ingest time.
- Reconstruct produces the same dedup result as the original ingest
  because dedup keys are reconstruct-stable.
- The cleaner can retroactively remove duplicates already on disk
  without breaking the ledger (baseline bean-check gate).

### Negative / Costs
- Every ingest must load existing entries or query the DB for known
  reference ids before writing. For large ledgers this adds latency.
- The identity helper layer (ADR-0019) is a prerequisite; callers
  that bypass it and read raw meta keys will miss post-Phase-7
  entries.

### Mitigations
- `find_source_reference` is a single import; the migration path
  from legacy reads is a one-line change.
- The legacy read-compat layer in `_legacy_meta.normalize_entries`
  ensures that legacy on-disk entries are transparently seen by
  `find_source_reference` without on-disk migration.

## Compliance

How `/adr-check` detects violations:

- **Direct meta reads for SimpleFIN id:** grep
  `src/lamella/` for `meta.get("lamella-simplefin-id")` or
  `meta.get("simplefin-id")`. Every hit must be replaced with
  `find_source_reference(entry, "simplefin")`.
- **Missing dedup in ingest functions:** each ingest entry point
  (SimpleFIN writer, CSV importer, paste handler) must contain a
  call to `find_source_reference` or a dedup-set membership check
  before writing. AST-flag entry-point functions that write to
  `.bean` files without that call.
- **Third raw-text scanner:** grep `src/lamella/` for regex
  patterns matching `simplefin-id|lamella-source` outside of
  `duplicates/cleaner.py` and `simplefin/ingest.py`.

## References

- CLAUDE.md §"Dedup unconditionally"
- CLAUDE.md §"Transaction identity & source provenance"
- `src/lamella/identity.py`: `find_source_reference`, `iter_sources`, `stamp_source`
- `src/lamella/duplicates/cleaner.py`: `_extract_sfid_from_block`, raw-text scanner
- [ADR-0019](0019-transaction-identity-use-helpers.md): identity helpers contract

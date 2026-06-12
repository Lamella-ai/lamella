# ADR-0019: Transaction Identity Reads Must Use `identity.py` Helpers

- **Status:** Accepted
- **Date:** 2026-04-26
- **Author:** AJ Quick
- **Related:** [ADR-0003](0003-lamella-metadata-namespace.md), [ADR-0008](0008-unconditional-dedup.md), `CLAUDE.md` ("Transaction identity & source provenance"), `src/lamella/identity.py`, `docs/specs/NORMALIZE_TXN_IDENTITY.md`

## Context

Transaction identity has two concerns on two different scopes:

1. **Lineage**: `lamella-txn-id` (UUIDv7) at transaction-meta level.
   Minted on first sight, never regenerated, survives ledger edits.
2. **Provenance**: `(source, reference-id)` pairs at posting-meta
   level, encoded as paired indexed keys `lamella-source-N` and
   `lamella-source-reference-id-N`, starting at 0 and dense.

The on-disk shape changed across migrations. Legacy entries still
carry `lamella-simplefin-id` at transaction level, or the bare key
`simplefin-id`, or `lamella-import-txn-id`. After migration these
keys live at posting-meta level as indexed source pairs. A caller
that reads identity directly via `txn.meta.get("lamella-simplefin-id")`
works only on legacy entries and silently returns `None` on all
post-migration content. The bug is invisible until dedup fails.

`_legacy_meta.normalize_entries` mirrors legacy transaction-level
source keys down to the source-side posting at parse time. This
means callers that use the identity helpers in `lamella/identity.py`
see a consistent shape regardless of what is on disk.

## Decision

Every read of transaction identity MUST use helpers from
`src/lamella/identity.py`:

| Need | Helper |
|---|---|
| Lineage UUID | `get_txn_id(entry_or_meta)` |
| Walk source pairs on a posting | `iter_sources(posting_meta)` |
| First reference under a named source | `find_source_reference(entry, source_name)` |
| All references under a named source | `find_all_source_references(entry, source_name)` |

MUST NOT read identity keys via raw `meta.get(...)` with any of
the following key arguments outside `_legacy_meta.py` and
`identity.py` themselves:

- `"lamella-txn-id"`
- `"lamella-source"`, `"lamella-source-N"`, and index variants
- `"lamella-source-reference-id"`, `"lamella-source-reference-id-N"`
- `"lamella-simplefin-id"` (legacy)
- `"simplefin-id"` (legacy bare)
- `"lamella-import-txn-id"` (legacy)

The only code allowed to read these keys directly is
`_legacy_meta.normalize_entries` (the normalizer itself) and
`identity.py` (the helpers' own implementation). All other modules
MUST call the helpers.

Writing identity follows a separate contract: writers stamp
`lamella-txn-id` at txn-meta level and `lamella-source-N` +
`lamella-source-reference-id-N` on the source-side posting via
`identity.stamp_source(posting_meta, source, reference_id)`.

## Consequences

### Positive
- Dedup via `find_source_reference(entry, "simplefin")` works on
  both legacy and post-migration entries. The same call, no
  branching in the caller.
- On-touch normalization in `rewrite/txn_inplace.py` converges
  legacy entries to the new shape during ordinary classification
  work.
- New source types are added by extending `SOURCE_NAMES` in
  `identity.py`; callers need no changes.

### Negative / Costs
- Existing violations of this rule (direct `meta.get` calls on
  identity keys outside the allowed modules) must be found and
  fixed. An AST scan is required to locate them.
- The helpers add one indirection compared to direct dict access.
  The cost is negligible but the pattern requires discipline.

### Mitigations
- AST scan in `/adr-check` targets `meta.get` calls with
  identity-key string literals outside the two allowed modules.
- Any new integration that writes provenance MUST use
  `identity.stamp_source(...)`. The writer contract enforces the
  same discipline on the write side.

## Compliance

AST scan for `\.meta\.get\(["']lamella-(txn-id|source|simplefin-id)` and
`\.meta\.get\(["']simplefin-id` outside `src/lamella/_legacy_meta.py`
and `src/lamella/identity.py`. Any match is a violation. New
tests for identity reads MUST assert via the helpers, not via
raw meta dict access.

## References

- CLAUDE.md § "Metadata schema, Transaction identity & source provenance"
- `docs/specs/NORMALIZE_TXN_IDENTITY.md` (full schema specification)
- `src/lamella/identity.py` (helper implementations)
- `src/lamella/_legacy_meta.py` (normalizer, the only other module allowed to read raw identity keys)
- [ADR-0003](0003-lamella-metadata-namespace.md): `lamella-*` namespace rule
- [ADR-0008](0008-unconditional-dedup.md): dedup uses `find_source_reference(entry, "simplefin")`

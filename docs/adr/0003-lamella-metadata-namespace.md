# ADR-0003: `lamella-*` is the metadata namespace; legacy prefixes are read-compat only

- **Status:** Accepted
- **Date:** 2026-04-26
- **Author:** AJ Quick
- **Related:** ADR-0001, `CLAUDE.md` ("Metadata schema -- the reconstruction contract", "Legacy-key compatibility"), `docs/NORMALIZE_TXN_IDENTITY.md`, `src/lamella/_legacy_meta.py`, `src/lamella/transform/bcg_to_lamella.py`

## Context

Lamella writes to ledger files the user also edits by hand and that
historically were touched by other tooling (lazy-beancount plugins,
ad-hoc scripts). Two requirements fall out of that:

1. **Reconstruct must be able to identify rows we own.** Given a
   ledger of mixed-origin transactions, the reconstruct pipeline
   has to find every receipt-link, classification-rule, budget,
   override, and connector-written transaction without ambiguity.
   That requires a stable, unique prefix on every key, tag, and
   custom directive type we emit.
2. **We must not stomp on the user's keys.** Anything that doesn't
   carry our prefix is user-authored or from another tool, and
   must round-trip unchanged through any of our writers.

The codebase has been through two prior names (`beancounter-glue`
and intermediate variants) and shipped on-disk content under the
`bcg-*` prefix. Switching prefixes is itself an architectural
decision because it crosses every write site and every parse site
in the codebase.

## Decision

`lamella-*` (metadata keys) and `#lamella-*` (transaction tags) are
the canonical namespace. One rule, no exceptions for new code:
every metadata key, tag, and custom directive type we emit is
prefixed with `lamella-` (or `#lamella-` for tags).

Anything not prefixed `lamella-*` is treated as user-authored or
foreign-tool content and is never rewritten by any Lamella writer.

For migration, the legacy `bcg-*` prefix and a small set of
pre-prefix bare keys (`override-of`, raw `simplefin-id`) are
**read** transparently:

- `_legacy_meta.normalize_entries`, wired into `LedgerReader` and
  the few direct `loader.load_file` callers, rewrites every
  `bcg-*` metadata key, transaction tag, and custom directive type
  to its `lamella-*` equivalent before any downstream code sees it.
  In-process readers can therefore rely on the new prefix
  exclusively while on-disk content stays in the old shape.
- `_legacy_meta` also mirrors retired txn-level identifier keys
  (`lamella-simplefin-id`, bare `simplefin-id`,
  `lamella-import-txn-id`) down to the source-side posting as
  paired indexed source meta, so identity helpers
  (`lamella.identity.find_source_reference`) work on legacy and
  current shapes without callers branching.
- A one-shot transform (`python -m lamella.transform.bcg_to_lamella
  --apply`) cleans up the on-disk files when convenient.
- Every in-place rewrite (`rewrite/txn_inplace`) opportunistically
  normalizes the touched transaction's identity meta. Legacy
  entries converge to the new schema as the user categorizes them.
  No "run the migration" step is ever required for correctness.

## Alternatives considered

- **No prefix; identify our content by file location only.**
  Rejected. The user can move entries between files; the override
  writer needs to find rows it owns regardless of which file they
  ended up in; reconstruct needs to scan everything.
- **Keep `bcg-*` permanently and skip the rebrand.** Rejected. The
  product is named Lamella; the on-disk namespace being a relic of
  a prior name guarantees confusion every time a new contributor
  reads a `.bean` file. The product surface and the persistence
  layer should agree.
- **Hard-cutover rebrand requiring the user to run a migration.**
  Rejected. The same self-healing pattern that handles SimpleFIN
  reference-id migration applies: read-compat is permanent, write
  is new-only, on-touch normalization makes the on-disk state
  converge during ordinary use. The user never has to run anything
  for the system to be correct.
- **A cross-tool standard prefix (e.g. just `app-*`).** Rejected.
  The whole point is uniqueness; a shared prefix invites collision
  with future tools or with the user's own keys.

## Consequences

### Positive
- Reconstruct has a single test: does the key/tag/directive-type
  start with `lamella-` (or `bcg-` via legacy compat)? If yes, it
  is ours.
- The user's metadata is provably never rewritten, because writers
  only touch keys they emitted in the first place.
- Migrations follow the same self-healing pattern as the SimpleFIN
  identity work: a permanent read-compat layer, opportunistic
  normalization on touch, and an optional bulk transform, so
  future renames (should we ever need one) have a worked-example
  template.

### Negative
- Two read paths exist (current `lamella-*` and legacy `bcg-*`).
  This is paid every parse, but the cost is one dictionary
  rewrite per entry and is dominated by Beancount's own parse
  cost.
- Writers must consistently emit the new prefix; a regression
  here doesn't fail loudly because the read-compat layer hides it.
  Tests for new writers MUST assert the literal `lamella-` prefix
  on emitted keys.

### Future implications
- New external integrations get prefixed keys (`lamella-<source>-*`)
  by convention. Reusing an existing prefix root (e.g. emitting
  `lamella-paperless-*` for a non-Paperless integration) is a
  bug.
- The Beancount-tag namespace (`#lamella-override`,
  `#lamella-loan-funding`, etc.) is governed by the same rule.
- If the product is ever renamed again, this ADR is the template:
  add a new `_legacy_meta` rule, leave the old prefix readable
  forever, and let on-touch normalization do the rest.

## Implementation notes

- Read-side normalizer:
  `src/lamella/_legacy_meta.py::normalize_entries`.
- One-shot on-disk migration:
  `python -m lamella.transform.bcg_to_lamella --apply`.
- Identity-key migration (paired source meta on postings) is
  documented in `docs/NORMALIZE_TXN_IDENTITY.md` and shares the
  same self-healing model.
- Live key inventory and which-file-emits-what table:
  `CLAUDE.md` "Metadata schema -- the reconstruction contract".
- Identity reads MUST go through `lamella.identity` helpers, not
  raw `meta.get`, because the helpers handle the legacy-shape
  mirror.

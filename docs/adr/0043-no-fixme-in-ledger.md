# ADR-0043: Unclassified Bank Data Is Staged via `custom` Directives, Not FIXME Postings

- **Status:** Accepted (directive shape frozen in ADR-0043b 2026-04-29; migration landed in v0.3.1, flag-gated default-off)
- **Date:** 2026-04-27
- **Author:** AJ Quick
- **Related:** [ADR-0043b](0043b-staged-txn-directive-shape.md), [ADR-0001](0001-ledger-as-source-of-truth.md), [ADR-0015](0015-reconstruct-capability-invariant.md), [ADR-0041](0041-display-names-everywhere.md), [ADR-0042](0042-chart-of-accounts-standard.md)

## Context

The convention inherited from the original SimpleFIN bridge writes
unclassified bank data to the ledger as balanced FIXME postings:
`Expenses:FIXME` (no entity) or `Expenses:<Entity>:FIXME`. Balanced
postings mean account balances include unclassified amounts, making
reports misleading. "FIXME" appears in PDF exports, dashboards, and
AI proposals, a term the user should never see.

Per ADR-0001, every piece of user state must be reconstructable from
the ledger alone. Unclassified data is state: the system must know
which transactions still need classification. This rules out the
alternative of keeping unclassified data only in SQLite.

Per ADR-0041, "FIXME" must never appear in user-facing surfaces. A
FIXME posting in an account name violates ADR-0041 whenever a
transaction list renders. Renaming the posting account doesn't fully
solve it. Any placeholder name in a spending category produces the
same problem.

The solution must: (1) store unclassified data in the ledger for
reconstruct, (2) not distort account balances, and (3) never surface
"FIXME" to the user.

The Phase 7 violation scan found ZERO new FIXME writes from current
production code paths. `simplefin/writer.py` references FIXME only in
a docstring. This ADR locks the discipline going forward and addresses
legacy migration.

## Decision

Unclassified bank data MUST be staged in the `staged_transactions`
SQLite table. New ingest paths, SimpleFIN, importer, paste, MUST
NOT write FIXME postings to any connector-owned `.bean` file.

For ledger reconstructability (ADR-0001, ADR-0015), unclassified
entries MUST also be persisted as `custom "staged-txn"` directives
in the appropriate connector-owned `.bean` file. The directive is
NOT a balanced posting. It carries:

```
YYYY-MM-DD custom "staged-txn"
  lamella-source: "simplefin"
  lamella-source-reference-id: "<id>"
  lamella-txn-date: YYYY-MM-DD
  lamella-txn-amount: <decimal> <currency>
  lamella-source-account: "<account-path>"
  lamella-txn-narration: "<payee / description>"
```

`bean-query` does not aggregate custom directives into account
balances. Bank account balances therefore reflect only classified
transactions. This is the defining property: the ledger balance shows
only real, finalized postings.

### Balance anchoring

The arithmetic difference between a bank account's true balance and
its ledger balance equals total unclassified work for that account at
that date. This difference MUST be anchored by `balance` directives.
The balance assertion fires when Beancount processes the ledger and
catches data loss (classified transactions without a corresponding
staged entry). The difference is surfaced in the account detail view
as "Pending classification: $X.XX."

### Classification flow

When the user classifies a staged entry:
1. The `custom "staged-txn"` directive is replaced with
   `custom "staged-txn-promoted"` (preserving audit trail) in the
   same write.
2. A real balanced transaction is appended in the same `.bean` file
   write. Both changes are in a single atomic file edit with
   bean-check validation before commit.
3. The `staged_transactions` row is marked promoted.

### Reconstruct path

A `@register_migration` step in `transform/reconstruct.py` reads
all `custom "staged-txn"` directives and repopulates
`staged_transactions` rows. `custom "staged-txn-promoted"` directives
are skipped (already classified). This satisfies ADR-0015.

### Legacy compatibility

No new FIXME postings are written. Period.

Existing `Expenses:FIXME` postings (no entity) MUST be migrated to
entity-scoped via the recovery system. A finding category in
`bootstrap/recovery/findings/` detects them and proposes entity
binding. These are treated as the highest-priority finding class.
They distort every expense report.

Existing `Expenses:<Entity>:FIXME` postings remain readable during
the transition window. They appear in the review queue with the
natural-language label "Pending classification", never as "FIXME"
in any user-facing copy (per ADR-0041). Long-term: these migrate to
the `custom "staged-txn"` pattern via bulk-rewrite, on a schedule
the user controls through the recovery UI.

### Forbidden patterns (hard stops)

The following account patterns MUST NOT appear in any newly-written
connector-owned file, under any circumstance:

- `Expenses:FIXME`: no entity, never write, even temporarily
- `Expenses:<Entity>:FIXME`: no new writes; legacy only during migration
- `Income:FIXME`, `Liabilities:FIXME`, `Assets:FIXME`: never
- Any account path containing "FIXME" in any root

## Consequences

### Positive
- Account balances in `Expenses:*`, `Assets:*`, and `Liabilities:*`
  reflect only classified transactions. Reports, graphs, and budget
  comparisons are accurate without filtering out placeholder rows.
- "FIXME" never appears in user-facing surfaces. PDFs, dashboards,
  and AI proposals are clean.
- The balance-anchor arithmetic gives the user a precise measure of
  pending classification work per account.
- Reconstruct works: `custom "staged-txn"` directives round-trip
  back to `staged_transactions` rows.

### Negative / Costs
- Existing ingest code (SimpleFIN writer, importer emit) must be
  rewritten to produce `custom "staged-txn"` directives instead of
  FIXME postings. This is a non-trivial change to two proven code
  paths.
- The `staged_transactions` table and the `custom "staged-txn"` count
  must stay in sync. A bug that writes one without the other
  produces silent data drift until the next reconstruct run.
- `balance` directives must be maintained correctly. A missing or
  stale balance assertion means the unclassified-work figure is wrong
  without any error surfacing.

### Mitigations
- Connector writers include a post-write assertion: count of
  `staged_transactions` rows for the account matches count of
  `custom "staged-txn"` directives. Mismatch raises immediately.
- Reconstruct tests assert that row count matches directive count
  after a clean reconstruct run. This is a CI-enforced invariant.
- The recovery finding for bare `Expenses:FIXME` is highest-priority
  and blocks the health check from showing green until resolved.

## Compliance

- Grep connector-owned `.bean` files for any `FIXME` substring in
  account paths. Any match in a newly-committed file is a violation.
- Pre-write hook in connector writers validates that the account path
  does not contain "FIXME". Raises `InvalidAccountError` before
  any file write.
- Reconstruct test: after `reconstruct --force`, assert
  `staged_transactions` row count equals `custom "staged-txn"`
  directive count across all connector-owned files.

## References

- [ADR-0001](0001-ledger-as-source-of-truth.md): ledger as source of truth (motivates the directive approach for reconstructability)
- [ADR-0015](0015-reconstruct-capability-invariant.md): reconstruct capability (the staged-txn reconstruct step satisfies this ADR)
- [ADR-0041](0041-display-names-everywhere.md): display names; "FIXME" must never appear in user-facing copy (this ADR eliminates the account-level source of violations)
- [ADR-0042](0042-chart-of-accounts-standard.md): chart of accounts standard (cross-references this ADR's prohibition on FIXME account paths)

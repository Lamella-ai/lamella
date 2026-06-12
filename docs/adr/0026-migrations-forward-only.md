# ADR-0026: Migrations Are Forward-Only, Append-Only

- **Status:** Accepted
- **Date:** 2026-04-27
- **Author:** AJ Quick
- **Related:** `src/lamella/transform/`, `docs/specs/RECOVERY_SYSTEM.md`, [ADR-0001](0001-ledger-as-source-of-truth.md), [ADR-0015](0015-reconstruct-capability-invariant.md)

## Context

Lamella carries a numbered migration history. Each migration encodes a
schema transition that runs once against a live database. Editing a
landed migration is silent: the migration table records the file as
already applied, so the edit never runs. The database state diverges
from what the code expects, and the divergence is undetectable until
something breaks at runtime.

Renumbering a migration is equally dangerous. Any deployment that ran
the old number treats the new number as a fresh migration and re-runs
the logic on an already-migrated database.

The reconstruct pipeline (`lamella.transform.reconstruct`) replays
user-configured state from ledger directives back into SQLite. That
pipeline must work against any database that is at any migration level,
not just the latest. Forward-only migrations make that contract
provable across history.

The Phase 7 violation scan found zero substantive violations. Past
modifications to landed migrations were SPDX header additions and
example-data sanitization (PII cleanup). Both are non-substantive.

## Decision

A migration file is immutable once it has been deployed. "Deployed"
means: any commit older than one week, or any file present in the
tip of `main`. New migrations are appended at the next available
number. A bug in a landed migration is corrected by a new migration
that brings the state to the correct shape, never by editing the
original.

Specific obligations:

- Migration filenames follow `NNN_<slug>.py` with a monotonically
  increasing `NNN`. Gaps are not allowed; renumbering is forbidden.
- Destructive `ALTER` or `DROP` operations SHOULD be avoided when an
  additive change works. Prefer additive schema evolution.
- A bump to `LATEST_LEDGER_VERSION` (the ledger-axis version in
  `docs/specs/RECOVERY_SYSTEM.md`) requires a corresponding migration
  that records the new version expectation. Version bumps without a
  migration are a violation.
- The reconstruct test suite (`lamella.transform.verify`) runs on
  every CI pass and must pass against the full migration history,
  not just the latest schema.

### Non-substantive edit exceptions

The following edits to landed migration files do NOT count as
violations:
- SPDX license header additions
- Comment-only changes (no logic change)
- Example data sanitization (replacing real values with canonical
  placeholders per ADR-0017)

Substantive logic changes require a new migration.

## Consequences

### Positive
- Every deployment state is reproducible: given the migration number
  recorded in the migrations table, the exact schema is deterministic.
- Reconstruct pipelines can assert that applying migrations 001 to N
  against a fresh database yields the same tables as the live
  database. That assertion is machine-checkable.
- Git blame on a migration is always meaningful. No silent in-place
  edits.

### Negative / Costs
- A trivial bug (wrong column default, wrong index name) requires
  writing a second migration instead of a one-line fix. The ratio of
  migration files to substantive changes grows over time.
- Additive-only pressure can produce "dead column" accumulation when a
  column was added in migration N and deprecated in N+3 but never
  removed.

### Mitigations
- Periodic "schema consolidation" migrations (rebuilding tables into
  canonical form via `CREATE TABLE ... AS SELECT` + rename) are
  acceptable. They are new migrations, not retroactive edits.
- The migration count is a feature, not a burden. It is a record of
  every schema decision. Keep it.

## Compliance

How `/adr-check` detects violations:

- **Edit of a landed migration:** `git log --diff-filter=M -- src/lamella/`
  for migration files older than one week. Substantive diffs (more than
  comments + SPDX header) are violations.
- **Renumbering:** migration filenames sorted numerically must be a
  gapless sequence starting at 1. Any gap or duplicate prefix is flagged.
- **Version bump without migration:** grep `LATEST_LEDGER_VERSION`
  assignment diff. If the value changes in a commit that contains no
  new migration file, flag it.

## References

- `src/lamella/transform/`: migration step inventory
- `src/lamella/transform/reconstruct.py`: reconstruct pipeline
- `src/lamella/transform/verify.py`: drift verification
- `docs/specs/RECOVERY_SYSTEM.md`: ledger-axis versioning and recovery
- [ADR-0001](0001-ledger-as-source-of-truth.md): ledger as source of truth
- [ADR-0015](0015-reconstruct-capability-invariant.md): reconstruct contract

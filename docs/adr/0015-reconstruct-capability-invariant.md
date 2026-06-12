# ADR-0015: Reconstruct Capability Is a Shipping Gate

- **Status:** Accepted
- **Date:** 2026-04-26
- **Author:** AJ Quick
- **Related:** [ADR-0001](0001-ledger-as-source-of-truth.md), [ADR-0003](0003-lamella-metadata-namespace.md), `CLAUDE.md` ("The Beancount ledger is the single source of truth", "Reconstruct capability"), `src/lamella/transform/reconstruct.py`, `src/lamella/transform/verify.py`

## Context

[ADR-0001](0001-ledger-as-source-of-truth.md) establishes the
principle: the ledger is the single source of truth; SQLite is a
disposable cache. ADR-0015 is the enforcement rule: any new
user-configured state type that ships without a reconstruct path
violates ADR-0001, regardless of intent.

The reconstruct pipeline (`python -m lamella.transform.reconstruct`)
reads every `custom "..."` directive in the ledger's connector-owned
files and rebuilds the corresponding SQLite rows. It is the proof
that ADR-0001 holds. Without it, "ledger is truth" is an aspiration,
not an invariant.

Several state types added post-reconstruct currently live in SQLite
only: entity `classify_context`, `account_classify_context`, project
`closeout_json`, project_txns `decided_by`/`decided_at`, mileage
`mileage_entries` (CSV-backed only), loans credit-limit history.
These are tracked violations. They have no permanent exception;
they must migrate.

## Decision

Every new user-configured state type MUST ship with a reconstruct
path before the feature is considered complete.

A "reconstruct path" requires all three components:

| Component | Requirement |
|---|---|
| Custom directive | A `custom "lamella-<type>"` format defined and written to a connector-owned `.bean` file |
| Reconstruct pass | A `@register` pass in `src/lamella/transform/reconstruct.py` that reads the directive and upserts the SQLite row |
| Verify policy | A `TablePolicy` registered in `src/lamella/transform/verify.py` that declares the table as `kind="state"` |

Normative obligations:

- MUST NOT merge a feature that introduces a new state SQLite table
  without all three components above.
- MUST ensure reconstruct is idempotent: running twice on the same
  ledger inputs yields identical DB state.
- MUST ensure the verify diff for the new table type passes after
  a reconstruct from a known-good ledger.
- MAY ship a transitional state (SQLite-only) behind a feature flag
  while the reconstruct path is in development, provided a tracking
  item exists in the relevant feature blueprint and the flag is off
  by default.
- The reconstruct pipeline is a PERMANENT part of the test suite,
  not a one-off script. New passes run on every `uv run pytest`.

The test: *if `lamella.sqlite` is deleted and `python -m lamella.transform.reconstruct --force` is run, does every state row for this feature come back?*
If the answer is "no", the feature is not complete.

## Consequences

### Positive
- ADR-0001's invariant is verifiable, not just stated.
- Disaster recovery is real at all times, not only for features
  that happened to get a reconstruct pass.
- The verify diff (`python -m lamella.transform.verify`) catches
  drift between ledger and SQLite continuously, not only after
  a production incident.

### Negative / Costs
- Every feature that introduces state pays the design cost of
  defining a `custom` directive format. There is no "add a SQLite
  table and ship" path.
- The reconstruct test suite grows with every new pass. A slow
  `reconstruct` run degrades the test cycle.

### Mitigations
- Existing tracked violations (entity context, account descriptions,
  projects closeout, mileage_entries, loans credit-limit history)
  have a migration path; the gate applies to new work, not
  retroactively to already-shipped items (but those items cannot
  be declared complete until the path lands).
- The `@register` decorator in `reconstruct.py` auto-discovers
  passes at import time, so there is no central registry to update.

## Compliance

- **Test:** `uv run pytest tests/test_reconstruct_*.py` MUST pass.
  A new state table without a passing reconstruct test is a
  blocking PR review issue.
- **Grep:** `grep -rn "CREATE TABLE" src/lamella/migrations/` for
  any new table introduced in a migration. Cross-reference against
  `reconstruct.py` registered passes. Missing entry = violation.
- **Verify:** `python -m lamella.transform.verify` on a
  well-populated dev ledger. State-table drift = bug.

## References

- CLAUDE.md §"The Beancount ledger is the single source of truth"
- CLAUDE.md §"Reconstruct capability"
- [ADR-0001](0001-ledger-as-source-of-truth.md)
- [ADR-0003](0003-lamella-metadata-namespace.md)
- `src/lamella/transform/reconstruct.py`
- `src/lamella/transform/verify.py`

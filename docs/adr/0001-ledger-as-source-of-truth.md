# ADR-0001: Beancount ledger is the single source of truth

- **Status:** Accepted
- **Date:** 2026-04-26
- **Author:** AJ Quick
- **Related:** `CLAUDE.md` ("Non-negotiable architectural rules" -> "The Beancount ledger is the single source of truth"), `docs/LEDGER_LAYOUT.md`, `lamella.transform.reconstruct`, `lamella.transform.verify`

## Context

Lamella sits between the user's real-world financial activity and a
plain-text Beancount ledger. The application also maintains a
SQLite database for caches (review queue, vector embeddings,
Paperless doc index, merchant confidence scores) and for
user-configured state (rules, budgets, dismissals, settings,
recurring confirmations).

If both stores are treated as authoritative, the system has two
problems with no clean answer:

1. Conflict resolution. When the SQLite row and the ledger entry
   disagree, which one wins? Without a hard rule, every subsystem
   invents its own answer and the answers contradict each other.
2. Disaster recovery. The SQLite file is the kind of thing that
   gets corrupted, accidentally deleted, or left behind during a
   container migration. The ledger is plain text under version
   control. Treating both as authoritative makes a lost SQLite
   into lost user state.

The whole product is anchored on Beancount being human-readable,
diff-able, and survivable. That property only holds if the ledger
fully describes user intent.

## Decision

The Beancount ledger is the single source of truth. The SQLite
database is a disposable cache. Every design decision must satisfy
the test: *if the SQLite is deleted tomorrow, can it be fully
reconstructed from the Beancount files alone, with no data loss?*
If the answer is "no", the design is wrong.

Concretely:
- User-configured state (rules, budgets, dismissals, Paperless
  field mappings, settings, recurring confirmations) is persisted
  to ledger custom directives in connector-owned files. SQLite
  rows for these are caches that the reconstruct pipeline can
  rebuild.
- Caches (review queue membership, notify cursors, merchant
  confidence, Paperless doc index, vector embeddings, audit items)
  live in SQLite only and repopulate naturally.
- Every connector-written entry carries enough `lamella-*` metadata
  to round-trip. The `lamella-*` namespace is precisely what lets
  reconstruct identify rows we own without ambiguity.

## Alternatives considered

- **SQLite as source of truth, ledger as export.** Rejected. This
  is the conventional shape for a finance app, but it gives up
  Beancount's defining property (the file the user opens in their
  editor *is* the data). Lost SQLite would mean lost user state
  with no path to recovery.
- **Dual-write with conflict markers.** Rejected. The complexity
  of merge semantics across notes, rules, budgets, and projects is
  enormous, and every conflict is a UX failure in a single-user app.
- **Per-feature choice.** Rejected. Letting each feature pick where
  its state lives is what produces silent drift, load-bearing
  state ending up SQLite-only because it was easier at the time.
  The hard rule eliminates the question.

## Consequences

### Positive
- Disaster recovery is real: deleting `lamella.sqlite` and running
  `python -m lamella.transform.reconstruct --force` rebuilds every
  user-configured row.
- The ledger remains human-readable and the diff in `git log`
  reflects every meaningful change to user state.
- Tooling can be naive: `lamella.transform.verify` reconstructs
  into a scratch DB and diffs against the live one to catch drift.

### Negative
- Every new feature that introduces user-configured state pays the
  cost of designing its custom directive format and writing the
  reconstruct path. There is no "just add a SQLite table" option.
- Some currently-SQLite-only state (entity classify context,
  account descriptions, projects) violates this rule and is tracked
  as follow-up reconstruct work in `FUTURE.md`. The decision
  applies to those. They have to migrate; they don't get a permanent
  exception.

### Future implications
- Any future external integration that produces user state must
  define its connector-owned `.bean` file and the `lamella-*` schema
  before it ships, not after.
- The reconstruct pipeline is a permanent part of the test suite,
  not a one-time script. New custom directives without a
  reconstruct path are a regression.

## Implementation notes

- Reconstruct entry point: `src/lamella/core/transform/reconstruct.py`.
- Drift verification: `src/lamella/core/transform/verify.py`.
- Connector-owned files inventory: `docs/specs/LEDGER_LAYOUT.md`.
- Per-directive writers live under `src/lamella/features/rules/`,
  `src/lamella/features/receipts/`, `src/lamella/features/budgets/`,
  and `src/lamella/features/bank_sync/`.
- The legacy on-disk SQLite filename is auto-renamed at startup by
  `_migrate_legacy_sqlite_filename` so this contract holds across
  the rebrand.

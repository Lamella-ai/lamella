# ADR-0002: In-place .bean rewrites are the default; overrides are the fallback

- **Status:** Accepted
- **Date:** 2026-04-26
- **Author:** AJ Quick
- **Related:** ADR-0001, `CLAUDE.md` ("In-place rewrites are the default; overrides are the fallback"), `src/lamella/core/rewrite/txn_inplace.py`, `src/lamella/features/rules/overrides.py`

## Context

When a SimpleFIN-ingested transaction is classified as
`Expenses:FIXME` (no rule matched, no auto-classify) and the user
later corrects it through the review queue, the system has to put
the corrected account *somewhere*. Two options were considered.

Option A, **override block.** Append a new transaction to
`connector_overrides.bean` that adjusts the original entry:
crediting `Expenses:FIXME` and debiting the chosen target
account, with a `#lamella-override` tag and an `lamella-override-of`
pointer to the original `lamella-txn-id`. The original
`Expenses:FIXME` posting in `simplefin_transactions.bean` stays
exactly as written.

Option B, **in-place rewrite.** Open the source `.bean` file,
find the matched posting line, and replace `Expenses:FIXME` with
the chosen account. No override block, no audit transaction. The
source file now reflects the correct classification directly.

Override-as-default produces three problems:

1. **UI complexity.** The review queue, search, and reporting all
   have to reconcile two representations of the same transaction
   (the raw FIXME and its overlaying correction). Every consumer
   of the ledger has to implement that reconciliation.
2. **Visual cruft.** Every routine FIXME -> category becomes a
   second transaction. After a year of usage the override file is
   the largest file in the ledger and the noisiest in `git log`.
3. **The raw FIXME never goes away.** Re-ingesting from scratch,
   a future audit, or a fresh reconstruct still sees
   `Expenses:FIXME` as the recorded answer; the correction is a
   later patch, not the answer.

For routine "the classifier was conservative; here's the right
account" corrections, the right shape is the source file telling
the truth.

## Decision

When the user corrects a FIXME (or otherwise reclassifies a
single-attribution transaction), the canonical action is rewriting
the source `.bean` file in place: locate the `Expenses:FIXME`
posting, replace its account path with the chosen target, save.
The override-block layer (`connector_overrides.bean`) remains for
the genuine-overlay case (loan-funding splits, intercompany
multi-leg blocks, audit corrections that need a paper trail), but
new code defaulting to overrides for routine FIXME -> category
writes is a regression.

In-place rewrite discipline (enforced by
`rewrite/txn_inplace.py`):

1. **Backup before edit.** Snapshot the source file to
   `.pre-inplace-<ISO-timestamp>/` under `ledger_dir` before any
   byte changes.
2. **Line-level edit.** Only the account path on the matched
   posting is touched; whitespace, amount, currency, trailing
   comments, and posting meta are preserved exactly.
3. **Amount sanity-check.** The line we are about to rewrite must
   match the expected account AND amount. Guards against line
   drift between parse and edit.
4. **bean-check vs. baseline.** If the post-write check finds a
   *new* error vs. the pre-edit baseline, restore the file from
   the snapshot byte-identically.
5. **Path safety.** Refuses paths outside `ledger_dir`, under
   archive/reboot/backup directories, or symlinks.

## Alternatives considered

- **Overrides as the default (status quo prior to this decision).**
  Rejected. See Context. UI cost, file growth, and the fact that
  the source file never reflects truth all compound over time.
- **Rewrite without backup.** Rejected. We take ownership of the
  files we ingest; we do not get to also remove the user's ability
  to undo a session. Every backup directory is a rollback path.
- **Rewrite without bean-check vs. baseline.** Rejected. A naive
  bean-check after write fails on pre-existing ledger errors that
  have nothing to do with the rewrite. Baseline-subtraction makes
  the check actionable: a new error means *we* introduced it.
- **A `--no-check` escape hatch in the writer.** Rejected by
  default; only allowed if the user explicitly asks for it and
  understands the consequences.

## Consequences

### Positive
- The source file always reflects the current classification.
  Search, reporting, and reconstruct see one answer per transaction.
- Override file shrinks to the cases that actually need an audit
  trail (loans, intercompany, audit reclassifications).
- Reconstruct semantics are simpler: the ledger says what the
  ledger means, no overlay layer to apply.

### Negative
- Each rewrite creates a backup directory. Over time these
  accumulate; sessions need eventual cleanup.
- The diff history of a single transaction is now spread across
  the source file (current state) and the backup directory
  (intermediate states), rather than visible inline as overrides.
  Acceptable trade. `git log` on the source file still shows the
  edit chain.

### Future implications
- Bulk-rewrite tooling (e.g. moving an entire merchant from one
  account to another in the source file) can build on the same
  primitive. The discipline points (backup, sanity-check,
  baseline-bean-check, path safety) generalize.
- The override path remains; this ADR does not deprecate it. ADRs
  introducing new bulk reclassification UX should explicitly
  address which path they target.

## Implementation notes

- Primary writer: `src/lamella/core/rewrite/txn_inplace.py`.
- Override writer (still active for overlay cases):
  `src/lamella/features/rules/overrides.py`.
- Baseline bean-check helper:
  `receipts.linker.run_bean_check_vs_baseline`.
- Snapshots land under `<ledger_dir>/.pre-inplace-<ISO-timestamp>/`
  and are not garbage-collected automatically.
- In-place rewrites also opportunistically normalize touched
  transactions' identity meta (see ADR-0003 / `_legacy_meta`).

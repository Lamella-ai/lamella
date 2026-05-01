# ADR-0004: Bean-Check After Every Write

- **Status:** Accepted
- **Date:** 2026-04-26
- **Author:** AJ Quick
- **Related:** [ADR-0002](0002-in-place-rewrites-default.md), `CLAUDE.md` ("Non-negotiable architectural rules" → "`bean-check` runs after every write"), `src/lamella/receipts/linker.py`

## Context

Every write to a connector-owned `.bean` file changes a file that
`beancount.loader.load_file` must parse cleanly. A write that
introduces a syntax error, an unknown account reference, or a
balance violation leaves the ledger in a state Beancount rejects.
The user cannot classify, query, or reconstruct until the error is
manually removed, and the error may not be noticed until the next
time they open Fava or run a query.

A naive post-write `bean-check` call fails on pre-existing errors
the user's ledger already had. Treating pre-existing errors as new
failures blocks every write until the user fixes their entire
ledger, which is unacceptable in production.

The solution is baseline-subtraction: capture the error set before
the write, run `bean-check` again after, and fail only on errors
that are *new relative to the baseline*.

## Decision

Every write to a connector-owned `.bean` file MUST be followed by
`run_bean_check_vs_baseline`. On failure, the write MUST be
reverted byte-identically (from snapshot or in-memory copy) and
a `BeanCheckError` MUST be raised, never silently ignored.

Specific obligations:

1. Capture the baseline via `capture_bean_check(main_bean)` before
   any write, not after.
2. Call `run_bean_check_vs_baseline(main_bean, baseline_output)`
   after the write completes.
3. On `BeanCheckError`, restore the file and re-raise. The caller
   surfaces the error to the user.
4. A `--no-check` escape hatch MUST NOT be added to any writer
   unless the user explicitly requests it and the consequences are
   documented.
5. Writers MUST NOT swallow `BeanCheckError` in a bare `except`
   clause.

## Consequences

### Positive
- The ledger is guaranteed parseable after every write.
- Reconstruct, query, and classify paths cannot see a corrupt ledger.
- Pre-existing errors do not block legitimate writes.

### Negative / Costs
- Every write incurs one extra `bean-check` subprocess call
  (typically 1 to 3 s on a real ledger).
- High-frequency batch writers (bulk rewrite, normalize-txn-identity)
  pay this cost per file, not per transaction.

### Mitigations
- `capture_bean_check` uses a 60 s timeout and logs a warning when
  `bean-check` is not on `PATH`, so there is no silent pass.
- Batch writers call `run_bean_check_vs_baseline` once per file, not
  once per transaction, so the cost scales with file count.

## Compliance

How `/adr-check` detects violations:

- **Missing baseline capture before write:** grep for `.write_text(`
  or `.write_bytes(` in files under `src/lamella/` where
  `capture_bean_check` is NOT called earlier in the same function.
- **Missing post-write check:** grep for `capture_bean_check` calls
  not paired with a `run_bean_check_vs_baseline` call in the same
  function scope.
- **Swallowed errors:** AST-flag bare `except` blocks in writer
  modules that catch `BeanCheckError` without re-raising.
- **Reference implementation:** `src/lamella/receipts/linker.py`
  `run_bean_check_vs_baseline`; in-place rewrite at
  `src/lamella/rewrite/txn_inplace.py`.

## References

- CLAUDE.md §"Non-negotiable architectural rules" → "`bean-check` runs after every write"
- `src/lamella/receipts/linker.py`: `run_bean_check`, `capture_bean_check`, `run_bean_check_vs_baseline`
- [ADR-0002](0002-in-place-rewrites-default.md): in-place rewrite also uses baseline bean-check

# ADR-0029: SQL Is Parameterized; Subprocess Args Are List-Form

- **Status:** Accepted
- **Date:** 2026-04-27
- **Author:** AJ Quick
- **Related:** `src/lamella/db/`, `src/lamella/receipts/linker.py` (bean-check subprocess), CLAUDE.md §"Non-negotiable architectural rules"

## Context

SQL injection and shell injection are not hypothetical threats in a
self-hosted single-user app. They are bugs that arrive silently
through ledger data, Paperless document content, or SimpleFIN payee
strings that contain characters the developer did not anticipate. A
payee name containing a single quote crashes an f-string query. A
filename containing a space passed to `shell=True` executes an
unintended command.

Neither class of bug produces an obvious error in tests. They manifest
at runtime on real data that the test suite never covers.

The Phase 7 violation scan found zero current violations. The codebase
already follows the rule. This ADR locks it in going forward.

The fixes are mechanical: parameterized queries and list-form
subprocess calls eliminate both classes unconditionally. There is no
legitimate use case for f-string SQL or `shell=True` with dynamic input
in this codebase.

## Decision

SQL queries MUST use parameterized form. Subprocess invocations MUST
use list-form arguments. There are no exceptions for "trusted" or
"constant" inputs. The rule is structural, not case-by-case.

Specific obligations:

**SQL:**
- `cursor.execute(query, params)` where `query` is a string literal
  and `params` is a tuple or dict.
- f-string SQL is forbidden: `cursor.execute(f"SELECT * FROM {table}")`
  is a violation even when `table` is a module-level constant. Use
  `cursor.execute("SELECT * FROM accounts")`. Hardcode the table name
  as a literal, not an interpolated value.
- `%` formatting in query strings is equally forbidden:
  `cursor.execute("SELECT * FROM %s" % table)` is a violation.
- ORM query builders that produce parameterized SQL internally (e.g.,
  SQLAlchemy core expressions) are compliant. Raw `.text()` with
  f-strings is not.

**Subprocess:**
- `subprocess.run([cmd, arg1, arg2], ...)` is canonical. The
  bean-check wrapper in `src/lamella/receipts/linker.py` is the
  reference.
- `subprocess.run("cmd arg1 arg2", shell=True, ...)` is forbidden when
  any component of the string is not a literal.
- `shell=True` with a fully-literal string (e.g.,
  `subprocess.run("bean-version", shell=True)`) is tolerated but
  discouraged. Prefer list-form in all cases.
- `subprocess.Popen` follows the same rule.

## Consequences

### Positive
- SQL injection is structurally prevented. A payee string from
  SimpleFIN that contains `'; DROP TABLE accounts; --` is passed as a
  parameter value, not interpolated into the query string.
- Shell injection is structurally prevented. A ledger path containing
  spaces or shell metacharacters is passed as a list element, not a
  shell-expanded string.
- The AST compliance check can flag violations mechanically without
  human review of every query.

### Negative / Costs
- Dynamic query construction (e.g., building a `WHERE` clause with a
  variable number of filters) requires explicit patterns
  (`WHERE x = ? AND y = ?` with a matching params tuple) instead of
  string concatenation. This is slightly more verbose.
- Table names and column names cannot be parameterized in standard
  SQL; they must be hardcoded as literals. This means schema-generic
  helpers that operate on arbitrary tables must hardcode every table
  name they touch.

### Mitigations
- For variable-length `WHERE` clauses, the pattern
  `" AND ".join(["col = ?"] * len(vals))` with a matching tuple is
  safe and idiomatic. Document this pattern in `src/lamella/db/__init__.py`.
- For schema-generic operations (e.g., the reconstruct pipeline
  iterating all state tables), hardcode the table list as a constant
  rather than reading it from `sqlite_master`.

## Compliance

How `/adr-check` detects violations:

- **F-string SQL:** AST scan `src/lamella/` for `cursor.execute(`
  calls where the first argument is a `JoinedStr` (f-string) AST node.
  Flag every hit with file and line number.
- **% SQL:** same scan for `cursor.execute(` where the first argument
  is a `BinOp` with `Mod` operator (the `%` string format operator).
- **shell=True with dynamic input:** AST scan for `subprocess.run(`
  and `subprocess.Popen(` where `shell=True` is a keyword argument
  AND the first argument is not a string literal (`Constant` AST
  node). Flag every hit.

## References

- `src/lamella/receipts/linker.py`: `run_bean_check_vs_baseline`
  (reference subprocess implementation)
- `src/lamella/db/`: database access layer
- CLAUDE.md §"bean-check runs after every write"

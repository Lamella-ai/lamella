# ADR-0024: Tests Hit Real SQLite And Real Beancount Fixtures

- **Status:** Accepted
- **Date:** 2026-04-27
- **Author:** AJ Quick
- **Related:** `CLAUDE.md`, `tests/conftest.py`, `tests/fixtures/ledger/`, [ADR-0017](0017-example-data-policy.md)

## Context

Past incidents in this project: a mocked DB test suite passed
while a schema migration broke production on deploy. The mock
returned hard-coded rows; the real migration had a column rename.
The test gave false confidence.

Beancount has its own parser with validation semantics. Hand-built
`Transaction(...)` objects in tests bypass the parser and can produce
entries that the real loader rejects, or entries with missing metadata
that writers assume will always be present on parsed entries.

The conftest already uses `lamella.db.connect` + `migrate` for DB setup
and `beancount.loader.load_file` / `load_string` for ledger fixtures.
This ADR codifies that pattern as a rule and forecloses mocking.

The Phase 7 violation scan found ZERO existing violations of this rule.
The codebase already follows the pattern; this ADR locks it in going forward.

## Decision

Integration tests MUST use real SQLite migrations and real Beancount
fixtures. DB-level and Beancount-parser-level mocking is forbidden in
integration tests.

Specific obligations:

- Integration tests MUST construct an SQLite DB via
  `lamella.db.connect(path)` + `lamella.db.migrate(conn)`.
  Direct `CREATE TABLE` statements in tests that duplicate migration
  logic are violations.
- Beancount entries in integration tests MUST come from
  `beancount.loader.load_file(fixture_path)` or
  `beancount.loader.load_string(source_text)`. Hand-built
  `Transaction(...)` / `TxnPosting(...)` objects are forbidden in
  integration tests (allowed only in `tests/unit/` pure-function tests).
- `unittest.mock.patch` targeting `sqlite3`, `sqlalchemy`, or any
  symbol in `lamella.db` is forbidden in files under `tests/test_*.py`.
  Mock targets in `tests/unit/` are exempt.
- Test data MUST use canonical placeholders per [ADR-0017](0017-example-data-policy.md).
  No real ledger rows copied from the dev database.
- The `conftest.py` autouse fixture that blocks real HTTP is NOT
  exempt. Tests that need external data stubs use `respx` route mocks,
  not patching the HTTP client itself.

## Consequences

### Positive
- Migrations are tested on every run. A column rename or
  constraint addition that breaks existing code fails immediately.
- Parser-validated Beancount entries surface metadata shape issues
  that hand-built entries hide.
- False-green test runs caused by mocks masking migration drift
  are eliminated.

### Negative / Costs
- Tests that manipulate DB state are slower than in-memory mocks.
  Ephemeral `tmp_path` SQLite files keep this bounded.
- Writing Beancount fixture `.bean` files is more verbose than
  constructing Python objects inline.

### Mitigations
- `tests/fixtures/ledger/` provides reusable fixture ledgers.
  New fixtures go there, not inline strings in test functions,
  so they are reviewed as ledger content, not as Python.
- `tmp_path` (pytest built-in) gives each test a fresh, isolated
  DB path with no teardown overhead.

## Compliance

Manual review primarily. Code review rejects PRs that add
`mock.patch` calls targeting `sqlite3`, `sqlalchemy`, or
`lamella.db` in non-unit test files.

AST scan for `mock\.patch\(["']sqlite3` or
`mock\.patch\(["']lamella\.db` outside `tests/unit/`. Each match
flags for mandatory review.

## References

- CLAUDE.md § tests
- `tests/conftest.py`: `connect` + `migrate` pattern, `load_file` use
- `tests/fixtures/ledger/`: canonical ledger fixtures
- `src/lamella/db.py`: `connect`, `migrate`
- [ADR-0017](0017-example-data-policy.md): canonical placeholder policy for test data

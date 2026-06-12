# ADR-0022: Money Is `Decimal`, Never `float`

- **Status:** Accepted
- **Date:** 2026-04-27
- **Author:** AJ Quick
- **Related:** `CLAUDE.md` ("Non-negotiable architectural rules"), `src/lamella/budgets/`, `src/lamella/loans/`, `src/lamella/recurring/`

## Context

Beancount's `Amount` type stores monetary values as `decimal.Decimal`.
Python `float` uses IEEE 754 double precision: binary floating-point
cannot represent most decimal fractions exactly. `0.1 + 0.2 == 0.30000000000000004`.
Across thousands of transactions, rounding errors compound. Tax
figures derived from floats can be off by cents in either direction,
producing incorrect Schedule C deductions or mismatched reconciliation totals.

Several existing modules (`budgets/service.py`, `budgets/progress.py`,
`recurring/detector.py`, `loans/reader.py`) call `float(...)` on
Beancount amounts. The violation scanner found 45 instances at Phase 7
audit. This ADR closes that path and makes `Decimal` the only permitted
type for money values.

## Decision

Every monetary amount MUST use `decimal.Decimal`. Float arithmetic on
money is forbidden.

Specific obligations:

- All function signatures that accept or return a money value MUST
  use `Decimal`, not `float` or `int`.
- Pydantic model fields for amounts MUST declare type `Decimal`.
- `float(amount)` where `amount` is a Beancount `Amount.number` or any
  money-carrying variable MUST NOT appear in production code.
- SQLite columns storing money MUST use `TEXT` (storing the Decimal
  string representation) or `INTEGER` (storing integer cents). The
  `REAL` affinity MUST NOT be used for money columns.
- Rounding MUST use `decimal.ROUND_HALF_EVEN` (banker's rounding).
  This is the IRS standard for tax computations.
- Display formatting MUST use explicit format strings (`f"{amount:.2f}"`).
  Passing a `Decimal` through `str(float(...))` is forbidden.

### Non-money exception

Threshold values that are inherently approximate (AI confidence scores
`0.0 to 1.0`, AI monthly spend caps in dollars-as-budget-not-as-money, similarity
ratios, percentages used for ranking) MAY use `float`. The distinguishing
question: would a one-cent rounding error in this value affect a user's books?
If no, `float` is acceptable.

### Ratio exception

A ratio computed as `Decimal(spent) / Decimal(budget)` is dimensionless,
the currency units cancel, leaving a pure fraction. The result MAY be coerced
to `float` for display purposes (typically `width: N%` on a progress bar)
provided it **never feeds back into a money calculation**. The round-trip is:
`Decimal` ÷ `Decimal` → dimensionless `Decimal` → `float` for CSS only.

The flagged `budgets/progress.py:81` site
(`ratio = float(spent / budget.amount)`) is compliant under this exception.

## Consequences

### Positive
- No IEEE 754 accumulation errors across transaction histories.
- Beancount `Amount.number` values flow through without conversion loss.
- SQLite TEXT storage round-trips without precision change.

### Negative / Costs
- Existing `float(...)` calls in `budgets/`, `loans/`, and
  `recurring/` modules require remediation (45 sites per Phase 7 scan).
- `REAL` SQLite columns in existing migrations cannot be altered
  in-place without a data migration.

### Mitigations
- New migrations define money columns as `TEXT`. Existing `REAL`
  money columns are flagged in the compliance scan and migrated
  forward one at a time.
- `decimal.Decimal(str(beancount_amount.number))` is the safe
  extraction pattern. Beancount stores `Decimal` natively, so no
  loss occurs.

## Compliance

AST scan for `float(` calls where the enclosing scope contains a
variable name matching `amount|balance|price|total|cost|fee|tax`.
Each match is a violation.

Grep for SQLite migration files defining `REAL` columns where the
column name contains `amount|balance|price|total|cost|fee|tax`.
Each match is a violation.

## References

- CLAUDE.md § "Non-negotiable architectural rules"
- `src/lamella/budgets/service.py`, `budgets/progress.py`,
  `budgets/alerts.py`, `budgets/writer.py`: known `float()` call sites
- `src/lamella/loans/reader.py`, `recurring/detector.py`: known `float()` call sites
- Beancount source: `beancount.core.amount.Amount`. `number` field is `Decimal`
- Python docs: `decimal.ROUND_HALF_EVEN`

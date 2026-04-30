# ADR-0045: Beancount Account Segments MUST Start With `[A-Z]`

- **Status:** Accepted
- **Date:** 2026-04-27
- **Author:** AJ Quick
- **Related:** [ADR-0001](0001-ledger-as-source-of-truth.md), [ADR-0007](0007-entity-first-account-hierarchy.md), [ADR-0031](0031-slugs-immutable-per-parent.md), [ADR-0042](0042-chart-of-accounts-standard.md)

## Context

Beancount's account-name grammar (per the parser) requires every
colon-separated segment to match `[A-Z][A-Za-z0-9-]*`: uppercase
letter first, then alphanumerics or hyphens. Anything else produces:

```
/ledger/connector_accounts.bean:NNN: Invalid token: '<segment>'
/ledger/connector_accounts.bean:NNN: syntax error, unexpected COLON, expecting end of file or EOL
```

Lamella has historically had this rule encoded in one place
(`registry/service.py::_SLUG_RE`) and used it for entity slug
validation, but the rule was NOT enforced at every boundary that
constructs account paths. Concrete failure: the payout-source
catalog had `leaf="eBay"` for the eBay payout pattern, which the
scaffold flow tried to write as `Assets:<Entity>:eBay`. Bean-check
rejected it; the user saw the scaffold appear to do nothing
(diagnosed in commit `4ffa406`).

The bug class repeats wherever a hand-edited string becomes a
segment: payout patterns, vehicle slugs, property slugs, loan
slugs, project slugs, custom merchant aliases, or anywhere the
user types a free-form name that gets stuffed into an account
path without going through `normalize_slug`.

## Decision

Every Beancount account segment MUST validate against the regex
`^[A-Z][A-Za-z0-9-]*$`. This rule applies to:

- Entity slugs (per ADR-0031)
- Asset / liability / income / expense leaf segments
- Vehicle, property, loan, project sub-segments
- All hand-coded constants in the codebase that become segments
  (e.g. `PayoutPattern.leaf`)
- All user input that flows into segment construction

A single canonical helper, `validate_beancount_account(path)`,
lives in `src/lamella/core/identity.py` (or `core/registry/service.py`
alongside the existing `is_valid_slug`). It splits on `:`, asserts
every segment matches the regex, and raises
`InvalidAccountSegmentError` listing the offending segment(s) so
the error surface is precise. Callers MUST use it before any
write that introduces a new account path.

The first segment is also constrained to one of the five
canonical Beancount roots: `Assets`, `Liabilities`, `Equity`,
`Income`, `Expenses` (per Beancount's own schema). Any other root
is rejected.

### Display name vs segment

The display name (per ADR-0041) is unrelated and unconstrained.
"eBay" is fine as a display name; the on-ledger segment is `Ebay`.
The display layer renders the display name; only the structural
identifier hits the ledger.

### Auto-normalization on user input

Where the user types a free-form name (entity creation, vehicle
add, property add, payout-source override), the input flows
through `suggest_slug` → `is_valid_slug` → `normalize_slug` per
ADR-0031. The end of that chain MUST be a slug that satisfies
the same regex. If normalization fails, the form rejects with a
clear error before any write attempt.

### Hand-coded constants

The payout-source catalog
(`src/lamella/features/bank_sync/payout_sources.py`) and any
similar catalog in the codebase MUST have its `leaf` (or
equivalent segment-producing) field validated at module-import
time. A simple module-level assertion that runs through
`validate_beancount_account` for every constant catches drift
before a user-triggered write fails.

### Existing data

Pre-existing account paths that violate this rule MUST be
migrated before this ADR is enforced as a hard gate. The
recovery system (`bootstrap/recovery/`) already has migration
infrastructure (per ADR-0026); a new finding category surfaces
violators and proposes the canonical replacement
(e.g. `Assets:Acme:eBay` → `Assets:Acme:Ebay`).

## Consequences

### Positive

- Users no longer see "Scaffold failed, Invalid token: 'eBay'"
  errors after clicking a Yes button. The boundary catches the
  problem before bean-check.
- Clear, single-source rule. Every code path that constructs an
  account path validates against the same regex.
- Cross-tool compatibility. Beancount itself, fava, beancount-magic,
  and any other tool reading the ledger all use the same grammar.
  Lamella's writes are guaranteed to be readable by any of them.

### Negative / Costs

- One-time migration of any historical paths violating the rule.
  The `eBay` leaf is the only known offender at time of writing;
  the recovery finding is cheap to add.
- Some users may have hand-edited `.bean` files containing custom
  segments that violate the rule. Those will trip the validator
  on the next write that touches the same account; the recovery
  flow surfaces them for review.

### Mitigations

- The validator helper raises with the offending segment in the
  error message so the user can fix the typo without consulting
  the parser docs.
- The display-name layer (ADR-0041) means the user never has to
  type an account path directly. They pick from autocomplete
  populated with display names. The system handles the
  display-name-to-segment translation.

## Compliance

- Pre-write hook in every connector-owned-file writer
  (`AccountsWriter`, `simplefin/writer.py`, `core/ledger_writer.py`)
  validates the account path before the file is touched.
  Validation failure raises `InvalidAccountSegmentError` and
  the writer does not modify the file.
- Module-level assertion in `payout_sources.py` and any similar
  static-segment catalog: every `leaf` value validates at import
  time. Caught at unit-test time, never reaches production.
- `is_valid_slug` and `validate_beancount_account` use the same
  regex. They are kept in lockstep via a shared module-level
  constant.

## References

- [ADR-0001](0001-ledger-as-source-of-truth.md): ledger is the source of truth (this ADR keeps the source of truth syntactically valid).
- [ADR-0007](0007-entity-first-account-hierarchy.md): entity-first hierarchy; entity slugs are the second segment of every path.
- [ADR-0031](0031-slugs-immutable-per-parent.md): slugs are immutable per parent. This ADR adds the syntactic validity check to the slug rules.
- [ADR-0041](0041-display-names-everywhere.md): display names everywhere; account paths are implementation detail. The display layer renders the user-friendly name; only the structural identifier hits the ledger.
- [ADR-0042](0042-chart-of-accounts-standard.md): chart of accounts standard. This ADR adds the syntactic gate to the standard's vocabulary.
- Beancount account-name grammar: <https://beancount.github.io/docs/beancount_language_syntax.html#account-names>

# ADR-0041: Display Names Everywhere; Account Paths Are Implementation Detail

- **Status:** Accepted
- **Date:** 2026-04-27
- **Author:** AJ Quick
- **Related:** [ADR-0007](0007-entity-first-account-hierarchy.md), [ADR-0011](0011-autocomplete-everywhere.md), [ADR-0031](0031-slugs-immutable-per-parent.md), [ADR-0035](0035-dense-data-readability.md), `docs/core/UI_LANGUAGE.md`

## Context

Every user-facing surface in Lamella today renders account paths
(`Liabilities:Acme:BankOne:Card`), entity slugs (`acme`), and
vehicle slugs (`mid-size-suv`) directly from data without a display
name lookup. These identifiers are structural. They carry information
that the financial system needs but that a person navigating financial
data does not.

A user reviewing transactions should see "Acme Credit Card" and
"Spent $45.99 at a home improvement store." They should not have to
parse `Liabilities:Acme:BankOne:Card` to understand what account
is involved. The gap between what the system stores and what it shows
is a usability failure and an onboarding barrier.

ADR-0031 established that slugs are immutable. They appear in URLs,
ledger paths, and DB foreign keys. Slugs cannot be humanized without
ambiguity. Display names are a separate mutable attribute, the correct
surface for user-facing copy.

The Phase 7 violation scan found 81 templates rendering raw account /
entity_slug / vehicle_slug / property_slug variables. Some use partial
filters; many do not. Significant migration debt.

## Decision

Every user-facing surface, HTML pages, notifications, job event log
messages, AI proposals shown to the user, error messages, MUST
render the display name of an account, entity, vehicle, property,
loan, or project. Slugs, account paths, and other structural
identifiers MUST NOT appear in these surfaces.

Raw Beancount account paths MAY appear only in:
- Settings / configuration screens where the user is intentionally
  viewing or configuring the structural identifier
- Power-user / debug views gated on an explicit user toggle
- Audit / diff views where structural identity is the point of the surface
- Account autocomplete pickers (per ADR-0011), the slug is the value
  being submitted; the display name renders as the visible hint label

### Account-type vocabulary

Beancount root segments MUST be translated in narrative copy:

| Beancount root | User-facing translation |
|---|---|
| `Assets:` | "money you have" / context-specific (Checking, Bank, etc.) |
| `Liabilities:` | "money you owe" / context-specific (Credit Card, Loan, etc.) |
| `Equity:` | "owner contributions" / context-specific |
| `Income:` | "earnings" / "money in" |
| `Expenses:` | "spending" / "money out" / "{category}" |

The translation MUST be context-specific where context is known.
"Spent $45.99" is preferred over "Expense of $45.99." "Charged to
Chase Visa" is preferred over "Liabilities:Acme:Chase:Visa."

### Multi-leg transaction rendering

Multi-leg transactions MUST render as natural-language sentences in
the default view. Examples:

- Transfer: "Transferred $1,000 from Bank One Checking to Wells
  Fargo Credit Card"
- Expense: "Spent $45.99 at a home improvement store for Home
  Maintenance, charged to Chase Visa"
- Income: "Received $5,000 from a consulting engagement, deposited
  to Bank Checking"

The raw posting table is available behind a "Show details" / "Raw"
toggle on the same surface, not removed entirely.

### Entity context suppression

When the user is operating within a single entity's view, the entity
prefix in account display names MUST be omitted. Show "Bank One
Checking" not "Acme: Bank One Checking" when already inside the
Acme entity view.

### Fallback and missing display names

When a display name is absent, fall back to a humanized version of
the slug, never the raw slug or path. "wells-fargo-checking" becomes
"Bank One Checking." Raw identifiers are never an acceptable
fallback.

Setup wizard MUST prompt for display names at registration time.
Any account, entity, vehicle, property, loan, or project missing a
display name surfaces a warning in the settings health check.

### Renderer helpers

All rendering goes through `display_account()`, `display_entity()`,
`display_vehicle()`, and equivalent helpers (specified in
`docs/core/UI_LANGUAGE.md`). Direct template access to raw slug or
path variables without going through a helper is a violation.

## Consequences

### Positive
- Users understand what they are seeing without learning Beancount
  syntax. The learning curve drops for new operators.
- Notifications, emails, and job logs become readable by anyone the
  operator shares them with (accountant, business partner).
- AI proposals rendered in the review queue are explainable without
  a Beancount primer.

### Negative / Costs
- Every template that currently renders a slug or path directly needs
  a helper call added. This is a non-trivial template audit (81 sites
  per Phase 7 scan).
- Display names must be populated for all existing entities, accounts,
  vehicles, properties, loans, and projects before this is fully
  effective. A bulk-import path from slug → humanized name is needed
  as a one-time migration.
- The helpers add a database or in-memory lookup per render. These
  must be cached at the request level to avoid N+1 lookups.

### Mitigations
- `/adr-check` grep detects raw slug/path rendering in templates
  (see Compliance below). Violations are caught before merge.
- The humanized-slug fallback means the migration is non-breaking:
  templates with missing display names degrade to humanized slugs,
  not raw identifiers.
- Request-level caching for display name lookups is specified in
  `docs/core/UI_LANGUAGE.md`.

## Compliance

`/adr-check` grep over `src/lamella/templates/` for:
- Direct rendering of `{{ account }}`, `{{ entity_slug }}`,
  `{{ vehicle_slug }}`, `{{ account_path }}` without a filter or
  helper wrapper.
- Any template variable whose name contains `_slug` or `_path`
  rendered without `display_*()` wrapper.

Each match is a violation. Tests: every template that renders
entity or account data MUST pass through `display_account()` or
`display_entity()`. Integration test renders a fixture transaction
and asserts no raw account path or slug appears in the output.

## References

- [ADR-0011](0011-autocomplete-everywhere.md): autocomplete pickers (slug as value, display name as label)
- [ADR-0031](0031-slugs-immutable-per-parent.md): slugs are immutable; display names are mutable
- [ADR-0035](0035-dense-data-readability.md): dense data display; display names are part of readability
- `docs/core/UI_LANGUAGE.md`: renderer function specs + translation table

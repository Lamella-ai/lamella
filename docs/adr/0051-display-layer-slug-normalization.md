# ADR-0051: Display-layer slug normalization

- **Status:** Accepted
- **Date:** 2026-04-29
- **Related:** [ADR-0011](0011-autocomplete-everywhere.md), [ADR-0035](0035-dense-data-readability.md)

## Context

Lamella's data model uses snake_case slug values for low-cardinality
enumerations:

- Account kinds: `checking`, `savings`, `credit_card`, `line_of_credit`,
  `tax_liability`, `money_market`, `hsa`, …
- Sources: `simplefin`, `paperless`, `openrouter`
- Status / state: `new`, `classified`, `matched`, `promoted`, `dismissed`

These slugs are convenient as primary keys, URL segments, JSON wire
values, and database-stored constants. They are not, however, what a
human reading a UI expects to see. Without a normalization rule,
`<select>` dropdowns rendered options like:

```html
<option value="credit_card">credit_card</option>
<option value="line_of_credit">line_of_credit</option>
```

and table cells displayed `credit_card` / `line_of_credit` /
`tax_liability` directly. The user reported these as broken-feeling
internals leaking into the UI.

## Decision

**Internal slug values must never appear in user-facing copy.** Every
template surface that renders a slug runs it through the `humanize`
Jinja filter (registered in `main.py`), which:

1. Looks up the value in an explicit display-label map for known
   slugs. The map handles:
   - **Common-word lowercasing**: `line_of_credit` → `Line of Credit`,
     never `Line Of Credit`.
   - **Acronyms**: `hsa` → `HSA`, `simplefin` → `SimpleFIN`.
   - **Brand spelling**: `paperless` → `Paperless`,
     `openrouter` → `OpenRouter`.
2. Falls back to a generic transform for anything not in the map:
   replace `_` with space, capitalize each word except a stoplist
   of joining words (`of`, `and`, `or`, `the`, `a`, `an`, `in`, `on`,
   `to`).

### Coverage

The filter applies to:

- Every `<datalist>` and `<select>` populated from an enumeration
  variable (`account_kinds`, source list, status list, etc.).
  Datalist `<option>` carries `value="{{ slug }}" label="{{ slug |
  humanize }}"`; selects render `{{ slug | humanize }}` as the
  visible text while keeping `value` as the slug.
- Badges and table cells displaying status / kind / source values.
- Filter chips, segmented controls, and tabs whose labels derive from
  enum values.

### What stays a slug

- HTTP query params (`?show=bank`, `?status=classified`).
- JSON API responses; server emits the slug, clients pretty-print
  if needed.
- Form `value=` attributes (the wire format the server expects).
- Database columns.
- Beancount metadata keys.

The slug is the source of truth; humanization is a presentation-layer
transform applied at render time.

## Consequences

- **No more `credit_card` in dropdowns.** Every existing `<select>`
  /`<datalist>` over `account_kinds` got the filter applied in the
  ADR-0051 commit.
- **One place to add display labels.** Adding a new enum value: add
  the slug to its tuple/frozenset, optionally add a one-line entry
  in the display-label map for special-case capitalization. The
  generic fallback handles ordinary cases correctly.
- **Templates may not render raw slugs in user-facing copy.** A
  `{{ row.kind }}` without `| humanize` is a bug. Code review
  should catch it. Internal display (logs, debug dumps) is fine.
- **Acronyms and brand names live in code, not in templates.** A
  template author writing about HSA or SimpleFIN doesn't have to
  remember the casing. The filter does.

## Alternatives considered

1. **Store pretty labels in the database.** Rejected; couples
   presentation to schema, and the slug is what the rest of the
   system keys off. Migration churn whenever marketing wants
   "Credit Card" → "Credit Account."
2. **Per-template inline `replace()` calls.** Rejected; drifts
   immediately, no central place to fix capitalization.
3. **Use Python's `inflection.titleize()` everywhere.** Closer to
   right, but it doesn't lowercase joining words ("Line Of Credit"
   vs "Line of Credit") and over-capitalizes acronyms ("Hsa"). The
   explicit map handles both.

## Rollout

Filter registered in `main.py`. Datalists / selects updated in the
ADR-0051 commit:
- `account_edit.html`, `business_accounts_edit.html`,
  `settings_accounts.html`, `partials/_account_modal_edit.html`,
  `partials/_setup_account_row.html`, `setup_accounts.html`: every
  `account_kinds` rendering.
- `accounts_index.html`: Kind column badge.

Future audits as new enum surfaces land. Internal CLAUDE.md notes
should reference this ADR when introducing new slug-typed display
fields.

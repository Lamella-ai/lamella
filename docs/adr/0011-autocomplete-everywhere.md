# ADR-0011: Autocomplete Everywhere for Ledger-Derived Lists

- **Status:** Accepted
- **Date:** 2026-04-26
- **Author:** AJ Quick
- **Related:** [ADR-0005](0005-htmx-endpoints-return-partials.md), `CLAUDE.md` ("Account / entity / vehicle / project pickers are autocomplete"), `src/lamella/web/templates/_components/account_picker.html`, `src/lamella/web/static/account_picker.js`

## Context

A Beancount ledger at production scale has hundreds of open
accounts. A `<select>` element with 400+ options is not usable:
the list cannot be searched by keyboard, the browser renders it
as an unscrollable dropdown on mobile, and the cognitive load of
scanning undivided account names is high.

Several early UI surfaces in Lamella used `<select>` for account
picks. This produced quiet friction: users would accept whatever
was highlighted rather than scroll to the correct account. The
result was misclassification that looked like user confirmation.

The account hierarchy is also entity-first (`Expenses:Acme:Supplies`),
which means alphabetical ordering mixes entities. Scanning for
an Acme expense requires skipping every non-Acme account in the
`E` block. A searchable typeahead that accepts a substring match
or entity prefix collapses the problem.

The same issue applies to entity pickers, vehicle pickers, property
pickers, and project pickers, any list derived from the ledger
that can grow to more than ~20 entries.

## Decision

Every UI surface that asks the user to choose a Beancount account,
entity, vehicle, property, or project MUST render a text input
backed by autocomplete, not a native `<select>`.

Rules:

- MUST use `<input>` + `<datalist>` or the `account_picker` macro
  (`src/lamella/web/templates/_components/account_picker.html`) backed
  by `src/lamella/web/static/account_picker.js`.
- MUST list every opened account (or entity / vehicle / etc.) in
  the completion candidates.
- MAY prefill the input with a suggested value (AI classification,
  rule match, last-used). When prefilling a heuristic guess, MUST
  show the `prefill_reason` badge per B5 discipline rules so the
  user knows the value is a suggestion.
- MUST NOT use native `<select>` for any ledger-derived list. A
  `<select>` on any account/entity/vehicle/project field is a bug
  on sight.
- MAY use `<select>` only for fixed, code-defined enumerations
  (e.g. pattern type: `merchant_exact | merchant_contains | regex`).

Implementation notes:
- The macro lives at `src/lamella/web/templates/_components/account_picker.html`
  (B6 Step 0 foundation).
- The vanilla JS controller at `src/lamella/web/static/account_picker.js`
  mounts via event delegation on `document.body` so it survives
  HTMX swaps without re-init.
- Suggestions are served from `/api/accounts/suggest` with a
  150 ms debounce.

## Consequences

### Positive
- Every account pick is a search, not a scroll. Ledger scale does
  not degrade the picker UX.
- Prefill + badge makes AI suggestions visible, not silently
  applied. Users can correct heuristic guesses without hunting
  through a list.
- Event-delegation mount survives HTMX partial swaps, so no per-page
  re-init code is needed.

### Negative / Costs
- Each picker surface requires migration from any existing `<select>`
  to the macro or `<datalist>` pattern. B6 Step 4 is tracking the
  per-surface migration; until it completes, some legacy `<select>`
  elements remain.
- The `/api/accounts/suggest` endpoint must stay fast (<50 ms p95)
  as the ledger grows. Slow completions degrade to a worse UX than
  a static `<select>`.

### Mitigations
- B6 migration is in progress (as of 2026-04-26). The legacy
  `T.account_picker` macro at `_components/_txn.html` coexists
  with the new macro during transition.
- Suggest endpoint uses an in-memory account list built at startup;
  no per-request ledger parse.

## Compliance

- **Grep:** `grep -rn "<select" src/lamella/web/templates/`. Every
  hit that maps to an account/entity/vehicle/project field is a
  violation. Review each hit; document deliberate exceptions.
- **Review:** new templates MUST NOT introduce `<select>` for
  ledger-derived lists; PR review is the enforcement gate.

## References

- CLAUDE.md §"Account / entity / vehicle / project pickers are autocomplete"
- `src/lamella/web/templates/_components/account_picker.html` (B6 Step 0)
- `src/lamella/web/static/account_picker.js`
- [ADR-0005](0005-htmx-endpoints-return-partials.md)

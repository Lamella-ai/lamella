# ADR-0066: Form Validation Errors Must Preserve User Input

- **Status:** Accepted (2026-06-11)
- **Date:** 2026-06-11
- **Author:** AJ Quick
- **Related:** [ADR-0005](0005-htmx-endpoints-return-partials.md), [ADR-0011](0011-autocomplete-everywhere.md)

## Context

Upstream issue Lamella-ai/lamella#8: the mileage quick-log form
validated input server-side and, on failure, responded with
`RedirectResponse("/mileage/quick?error=...")`. The browser followed
the redirect to a fresh GET, which re-rendered an **empty** form —
destroying everything the user typed, including multi-sentence trip
notes. The user's fix was often a one-character odometer correction;
the cost was retyping the whole entry.

The redirect-on-error pattern (PRG applied to the *failure* path) is
scattered across the codebase. PRG exists to prevent double-submits
of **successful** writes; applying it to validation failures throws
away client state for no benefit — nothing was written, so there is
nothing to double-submit.

## Decision

**A form submission that fails validation must never clear the
user's input.** Applied to every new form and to existing forms as
they are touched:

1. **Vanilla (non-HTMX) form POSTs:** on validation failure, do NOT
   redirect. Re-render the page template directly (status 422 for new
   code; 200 acceptable where callers depend on it) with:
   - `error` — human-readable message in the page context, and
   - `form_values` — the raw submitted fields, echoed into every
     input/textarea/select/radio so the form renders filled-in.
   Redirect (303, PRG) **only on success**.
2. **HTMX form POSTs:** return a non-2xx error fragment (toast or
   inline `_htmx.error_fragment`) **without swapping the form** — the
   DOM keeps the user's input. Never respond to a failed validation
   with `HX-Refresh` or a redirect, both of which reload and clear.
3. **Client-side pre-validation** (`required`, `min`, `pattern`,
   datalist constraints) is encouraged as a first line, but is never
   a substitute for 1–2: server-side checks (cross-field rules,
   registry lookups) will always exist, and their failure path must
   preserve input.
4. **Error messages name the field and the fix** ("Unknown vehicle
   'Truck2' — pick one from the list"), not internal codes
   (`?error=bad_odometer`).

Reference implementation: `/mileage/quick` and `/mileage` (POST) in
`src/lamella/web/routes/mileage.py` + `mileage_quick.html` /
`mileage.html` — `_render_quick_page(..., error=, form_values=)` and
`_error_response(..., form_values=)`.

## Consequences

- Query-param error codes (`?error=bad_date`) are deprecated for
  form validation. They remain valid for errors on *link-shaped*
  actions (deletes, toggles) where there is no typed input to lose.
- Templates gain a `form_values` context entry (default `{}`); each
  field reads `{{ form_values.x or <existing default> }}`. Radios and
  selects compare against `form_values` for their checked/selected
  state.
- Existing forms that still redirect-on-error are nonconforming;
  bring them over when touched. The three known offenders at
  acceptance time (`/mileage/{id}` edit submit,
  `/settings/mileage-rates`, `/accounts/{path}/opening-balance`)
  were converted same-day — notably, the latter two never even
  *rendered* their `?error=` codes, so their failures were fully
  silent.
- Tests for a form's validation path should assert both the error
  message AND that the submitted values appear in the response body.

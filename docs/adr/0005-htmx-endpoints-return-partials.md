# ADR-0005: HTMX Endpoints Return Partials, Not Full Pages

- **Status:** Accepted
- **Date:** 2026-04-26
- **Author:** AJ Quick
- **Related:** [ADR-0006](0006-long-running-ops-as-jobs.md), `CLAUDE.md` ("HTMX endpoints return partials, not full pages"), `src/lamella/routes/_htmx.py`, `src/lamella/routes/staging_review.py`

## Context

The app ships a custom HTMX-compatible shim (`static/htmx.min.js`).
When an HTMX swap-driving request hits an endpoint that returns a
full page (a Jinja template extending `base.html`), the entire
sidebar, topbar, and footer are inserted into the `hx-target`
element. The layout nests inside itself. The shim does not support
`hx-select`, so there is no client-side workaround.

The same failure fires on redirect chains: a `30x` from an action
endpoint causes the shim to follow the redirect, fetch the list
page, and swap its full HTML into the target row. The row becomes
the entire list page nested inside the original page.

Both failure modes are silent: no JS error, no visible breakage
until the user scrolls and sees duplicated chrome.

## Decision

Every endpoint that is a possible HTMX swap target MUST return a
partial template (a fragment that does NOT extend `base.html`)
when `_htmx.is_htmx(request)` returns `True`. The full-page
template MUST be returned otherwise.

Specific obligations:

1. Use `_htmx.render(request, full=..., partial=..., context=...)`
   for GET endpoints that drive swaps. Never branch on the header
   manually.
2. Use `_htmx.redirect(request, url)` in place of every
   `RedirectResponse(...)` from an HTMX-targeted action endpoint.
   For HTMX, this returns `204` + `HX-Redirect`; for vanilla
   forms it returns `303`. Never return a bare `RedirectResponse`
   from a POST endpoint that's reachable via HTMX.
3. Use `_htmx.empty()` when a row was deleted and the swap should
   leave nothing.
4. Use `_htmx.error_fragment(html)` when an action failed and an
   inline error must render inside the row slot.
5. MUST NOT add `hx-select` to templates. Fix the route instead.
6. MUST NOT add modifier tokens to `hx-swap` (e.g. `show:`,
   `scroll:`). The shim's mode check is exact-string match and
   silently falls back to `innerHTML`.

## Consequences

### Positive
- Layout nesting is structurally prevented. A partial cannot nest
  `base.html` because it does not extend it.
- Server-side partials are faster than full-page renders filtered
  by `hx-select`.
- The `_htmx` module is a single enforcement point; reviewers check
  one import, not scattered `request.headers.get("hx-request")` calls.

### Negative / Costs
- Every new endpoint requires two templates (full + partial) or a
  single partial that is `{% include %}`d by the full template.
- Legacy endpoints that predate this rule must be audited and
  migrated.

### Mitigations
- Reference implementations: `routes/staging_review.py` +
  `partials/_staged_list.html`, `routes/card.py` +
  `partials/_card_pane.html`.
- `_htmx.render` encapsulates the branch; callers cannot forget it
  by importing the helper.

## Compliance

How `/adr-check` detects violations:

- **Bare RedirectResponse in action endpoints:** grep for
  `RedirectResponse(` in files under `src/lamella/routes/`; flag
  any occurrence not inside a function that also calls
  `_htmx.redirect`.
- **Full-page return in HTMX-targeted GET:** look for
  `TemplateResponse` calls using templates that contain
  `{% extends "base.html" %}` from routes that also have an
  `hx-get` caller in any template.
- **hx-select in templates:** grep `templates/` for `hx-select`.
- **hx-swap modifiers:** grep `templates/` for `hx-swap=` values
  containing `:` (e.g. `hx-swap="outerHTML show:`).

## References

- CLAUDE.md §"HTMX endpoints return partials, not full pages"
- `src/lamella/routes/_htmx.py`: `is_htmx`, `render`, `redirect`, `empty`, `error_fragment`
- `src/lamella/routes/staging_review.py`: reference GET implementation
- [ADR-0006](0006-long-running-ops-as-jobs.md): job modal is itself an HTMX partial

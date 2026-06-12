# ADR-0037: In-Page Actions Do Not Reload the Page or Disturb Scroll Position

- **Status:** Accepted
- **Date:** 2026-04-27
- **Author:** AJ Quick
- **Related:** [ADR-0005](0005-htmx-endpoints-return-partials.md), [ADR-0039](0039-htmx-swap-failure-modes-first-class.md)

## Context

The review queue and card classify view can hold dozens of rows. A user
scrolls to row 40, classifies it, and the page reloads. They are back at
the top. This pattern, page reload as the side effect of a row action,
destroys flow for any task that involves multiple items in sequence.

The same failure occurs in subtler forms: a POST that redirects to a
full page, a swap that targets `body` and replaces everything, or a 5xx
that sends the browser to a generic error page. The user loses their
place and often loses context about what they had selected.

Cross-cutting elements compound the problem. When a transaction is
classified, the review-queue count in the topbar must decrement. With a
full page reload, that's free. Without one, it must be handled via
out-of-band swaps or server-sent events, something that must be
designed in rather than discovered later.

The Phase 7 violation scan found 384 `RedirectResponse` calls outside
`_htmx.py`. Many are in non-HTMX flows (setup wizard, setup_schema)
where they are legitimate. The remediation work is to identify which
of the 384 are in HTMX-targeted handlers and migrate them to
`_htmx.redirect()`.

## Decision

Clicking a row action (classify, accept, dismiss, link receipt, flag,
mileage, etc.) MUST NOT reload the page or change the user's scroll
position.

Specific obligations:

1. Action endpoints MUST target the originating element or its nearest
   containing row. Typical patterns: `hx-target="closest tr"`,
   `hx-target="this"`, `hx-target="#row-{id}"`. Never `hx-target="body"`
   for a row action.
2. Cross-cutting UI elements (queue counters, dashboard totals,
   topbar badges) that change as a result of an action MUST update via
   `hx-swap-oob` fragments returned in the same response, or via a
   server-sent `HX-Trigger` header that causes a targeted refresh.
   They MUST NOT require a full page reload.
3. Navigating to a different page intentionally IS a full-page
   transition and is out of scope for this rule. That transition must
   look like a navigation: an `<a href>` or `_htmx.redirect(request,
   url)`. It must not happen as a silent side effect of a row action.
4. Form errors from a row action MUST render inline using
   `_htmx.error_fragment(html)` in the same slot. They MUST NOT
   redirect to a validation-error page or replace the page with an
   error template.
5. If a row is deleted and the swap should remove the element,
   `_htmx.empty()` is the correct return, not a redirect that
   reloads the surrounding list.

## Consequences

### Positive
- Users in a classify session can work through a queue without losing
  scroll position or focus between actions.
- Out-of-band counter updates keep the topbar and dashboard accurate
  without requiring a reload.
- Error feedback stays inline. The user sees what went wrong without
  losing the form state.

### Negative / Costs
- Every action endpoint must explicitly enumerate which cross-cutting
  UI elements it affects and either return OOB fragments or fire
  `HX-Trigger` events. This requires upfront design of the element
  dependency graph.
- Inline error fragments must replicate the slot's DOM shape so they
  swap cleanly. Template discipline is higher than with redirect-on-error.

### Mitigations
- Reference implementations in `routes/staging_review.py` and
  `routes/card.py` show the OOB counter pattern. New routes follow
  those examples.
- `_htmx.error_fragment` is the single call for inline errors.

## Compliance

Detect violations:

- **Bare RedirectResponse from action handlers:** grep
  `src/lamella/routes/` for `RedirectResponse(` in POST/DELETE
  handler bodies. Any match not inside a function that also calls
  `_htmx.redirect` is a violation of both this ADR and ADR-0005.
- **hx-target="body" on row actions:** grep templates for
  `hx-target="body"` on elements inside `<tr` or `.row` containers.
  `hx-target="body"` is valid only for job-modal-overlay actions
  (per ADR-0006).
- **Cross-cutting counters updated only by reload:** grep for topbar
  badge elements whose count is not wrapped in an `id` that appears
  in any OOB response template.

## References

- CLAUDE.md §"HTMX endpoints return partials, not full pages"
- [ADR-0005](0005-htmx-endpoints-return-partials.md): HTMX swap contract and _htmx helpers
- [ADR-0039](0039-htmx-swap-failure-modes-first-class.md): failure modes for HTMX swaps
- `src/lamella/routes/_htmx.py`: `empty`, `error_fragment`, `redirect`

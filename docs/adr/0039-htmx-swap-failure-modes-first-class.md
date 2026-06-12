# ADR-0039: HTMX Swap Failure Modes Are First-Class

- **Status:** Accepted
- **Date:** 2026-04-27
- **Author:** AJ Quick
- **Related:** [ADR-0005](0005-htmx-endpoints-return-partials.md), [ADR-0006](0006-long-running-ops-as-jobs.md), [ADR-0036](0036-instant-feedback-100ms.md), [ADR-0038](0038-toast-vs-modal-usage-rules.md)

## Context

An HTMX swap can fail in four distinct ways: the server returns a 4xx
validation error, the server returns a 5xx unexpected error, the
network request never completes, or the DOM target element is missing
by the time the response arrives.

Without explicit handling, each failure leaves the UI in a different
degraded state. A 4xx with a JSON body renders JSON text into a table
row. A 5xx with an HTML stack trace expands the row to fill the
viewport. A network timeout leaves the spinner running indefinitely.
A missing target silently drops the response.

The user's mental model is: "I clicked; something should happen." If
nothing happens and no error appears, they assume the click did not
register and click again. If the row fills with a Python traceback,
they lose trust in the system. Either outcome is avoidable.

The Phase 7 violation scan found ~398 `HTTPException` raises across
routes, but only 4 uses of `_htmx.error_fragment()`. The remediation
target is HTMX-targeted POST/PUT/DELETE handlers, those that should
return inline error fragments rather than raise to a generic error page.

## Decision

Every HTMX swap target has a documented failure render. Each failure
class has a defined recovery path.

Specific obligations:

1. **4xx validation errors:** The server MUST return an HTML fragment
   (via `_htmx.error_fragment`) shaped to fit the swap target.
   It MUST NOT return JSON or a bare string. The fragment preserves
   the row's `id` attribute so future swaps from sibling actions can
   still locate it.
2. **5xx unexpected errors:** The server MUST return a generic error
   fragment, never a full-page error template and never a raw
   traceback. The fragment MUST include a "retry" affordance (a button
   that re-fires the original request).
3. **Network failure (htmx:sendError, htmx:responseError):**
   Client-side event handlers MUST catch these events globally and
   show a toast (per ADR-0038) with a "retry" link. The slot's prior
   content MUST be left intact. Do not clear it on network failure.
4. **Target element missing (htmx:targetError):** The global error
   handler MUST log the error and show a toast. It MUST NOT throw an
   uncaught exception.
5. **Long swap without job runner (> 3 s, rare per ADR-0006):** The
   spinner shown per ADR-0036 MUST remain. No additional action is
   needed; the job runner handles all > 5 s cases. This tier covers
   the 3 to 5 s window only.
6. Error fragments and success fragments MUST use the same component
   library (per ADR-0032). A row that shows an error must not break
   adjacent rows' layout.

Global HTMX event listeners live in a single module under
`src/lamella/static/`, not scattered across page-specific scripts.

## Consequences

### Positive
- Every failure class has a prescribed output. Authors do not invent
  fallbacks.
- Network failures show a toast and leave the page intact. Users see
  a clear signal and have a retry path.
- Stack traces never reach the DOM in production.

### Negative / Costs
- Every new endpoint needs a test that asserts 4xx returns an HTML
  fragment, not JSON and not a full-page template.
- The global HTMX error handler in `static/` must be kept in sync with
  the shim version. If the shim changes event names, the handler breaks
  silently.

### Mitigations
- The test fixture is a parametrize pattern: one test per endpoint
  verifying that `POST /endpoint` with bad input returns
  `Content-Type: text/html` at 4xx.
- The shim's event names are documented in `static/htmx.min.js`.
  Changes to event names require updating both files in the same commit.

## Compliance

Detect violations:

- **4xx returning JSON:** grep route handlers for `JSONResponse` calls
  in error branches inside endpoints that are HTMX swap targets.
- **5xx returning full-page template:** grep for `TemplateResponse`
  calls inside `except` blocks in route handlers; check whether
  the template path extends `base.html`.
- **Missing global error handler:** grep `static/` for
  `htmx:sendError\|htmx:responseError\|htmx:targetError` event listeners.
  Absence is a violation.
- **Test coverage:** pytest fixture checks that every route module
  has at least one test asserting `Content-Type: text/html` on a
  4xx response.

## References

- CLAUDE.md §"HTMX endpoints return partials, not full pages"
- [ADR-0005](0005-htmx-endpoints-return-partials.md): error_fragment helper
- [ADR-0006](0006-long-running-ops-as-jobs.md): job runner handles the > 5 s case
- [ADR-0036](0036-instant-feedback-100ms.md): spinner must not spin forever on network failure
- [ADR-0038](0038-toast-vs-modal-usage-rules.md): toasts are the prescribed channel for transient errors
- `src/lamella/routes/_htmx.py`: `error_fragment`

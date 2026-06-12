# ADR-0033: Per-Concern API Endpoints. One URL per Action Across All Surfaces

- **Status:** Accepted
- **Date:** 2026-04-27
- **Author:** AJ Quick
- **Related:** [ADR-0032](0032-component-library-per-action.md), [ADR-0005](0005-htmx-endpoints-return-partials.md), `src/lamella/routes/`, `src/lamella/templates/_components/_txn_actions.html`

## Context

The same operation, classify a transaction, is handled by at least three
separate route handlers today: `POST /review/staged/classify` in
`routes/staging_review.py`, `POST /search/bulk-apply` in `routes/search.py`,
and `POST /txn/<hash>/apply` also in `routes/search.py`. Each handler has its
own parameter names, its own partial response shape, and its own bean-check
invocation. A bug in the classify path has to be found and fixed in all three
independently.

The Phase 7 violation scan confirms 6 distinct classify POST routes across 5
files.

The receipt link, accept-suggestion, and dismiss actions have a similar
proliferation. The root cause is that routes grew surface-first: each page
added its own action handler rather than delegating to a shared one.

`_txn_actions.html` broke this pattern for the classify/ask-AI/ignore cluster:
it posts to `/api/txn/{ref}/classify` and `/api/txn/{ref}/ask-ai` regardless
of which surface the row appears on. That endpoint is thin: it identifies
whether the ref is staged or ledger-backed and calls the appropriate service
method. All surfaces get the same behavior for free.

## Decision

A concern (classify a transaction, ask AI to classify, dismiss/ignore, link a
receipt, apply a suggestion, undo an override) gets ONE URL. Any surface that
triggers the operation posts to that URL. The URL is under `/api/<concern>/`
or `/api/txn/{ref}/<verb>` for per-item actions.

Specific obligations:

1. New action endpoints MUST live under `/api/`:
   - Per-item txn actions: `/api/txn/{ref}/<verb>`. Ref is `staged:<id>`
     or `ledger:<hash>`, verb is `classify`, `ask-ai`, `dismiss`, `restore`,
     `link-receipt`.
   - Bulk/concern actions: `/api/classify`, `/api/receipt/link`,
     `/api/note/apply` for multi-target calls.
2. The endpoint accepts the same payload regardless of which surface calls
   it. No surface-specific query params that alter the core behavior.
3. The endpoint returns the same partial fragment regardless of caller.
   Callers that need a different visual after the action handle that via HTMX
   `hx-on::after-request` (e.g., remove the row, reload the list).
4. The implementation is built around the SERVICE, not the route. The route
   handler is a thin HTMX adapter: validate input, call service, return
   partial. Business logic lives in `src/lamella/` service modules.
5. Existing per-page action routes (`/review/staged/classify`,
   `/txn/<hash>/apply`, etc.) are migration debt. They are tracked in the
   Remaining Tasks of their respective feature docs. They MUST NOT be
   extended; new callers MUST use the canonical URL.

## Consequences

### Positive
- A bug fix in the classify service propagates to every surface without a
  multi-file hunt. The search surface, review surface, and card surface all
  call the same endpoint.
- Integration tests target one endpoint per concern. Coverage is not
  fragmented across surface-specific handlers.
- The component macro (ADR-0032) and the canonical URL are a matched pair.
  `_txn_actions.html` already demonstrates this: macro calls `/api/txn/{ref}/classify`,
  and that endpoint exists.

### Negative / Costs
- The `ref` encoding (`staged:<id>` vs `ledger:<hash>`) adds an
  indirection layer that the endpoint must parse and route. A malformed ref
  returns a 400 rather than a routing error; the endpoint must validate it.
- Migrating existing per-page handlers requires verifying that parameter names
  and response shapes are compatible. The migration window is a period where
  two handlers serve the same concern simultaneously.

### Mitigations
- `_txn_actions.html` is the working reference: its `/api/txn/{ref}/classify`
  and `/api/txn/{ref}/ask-ai` endpoints already follow this ADR.
- The feature doc Remaining Tasks sections track which per-page handlers are
  migration debt. New work picks up the nearest unfinished migration rather
  than adding a new per-page handler.

## Compliance

- Grep `src/lamella/routes/` for handler functions whose path segment contains
  `classify`, `accept`, `dismiss`, `apply`, `link`, `undo`. Count occurrences
  per concern. More than one handler per concern is a tracked violation.
- Check that `/api/` handlers call a service method, not inline business logic.
  Handlers exceeding ~30 lines of non-boilerplate are a candidate for service
  extraction review.
- PR review gate: new action handlers outside `/api/` for concerns already
  served by a canonical endpoint are a violation.

## References

- `src/lamella/templates/_components/_txn_actions.html`: reference macro
  posting to `/api/txn/{ref}/classify` and `/api/txn/{ref}/ask-ai`
- `docs/features/search.md` §Known violations: `POST /txn/<hash>/apply` as tracked debt
- `docs/features/review-queue.md` §Known violations: `_redirect_to_list` as tracked debt
- [ADR-0032](0032-component-library-per-action.md): component macros call canonical URLs
- [ADR-0005](0005-htmx-endpoints-return-partials.md): HTMX partial contract for responses

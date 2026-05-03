# ADR-0036: Every User Action Acknowledges Within 100 ms

- **Status:** Accepted
- **Date:** 2026-04-27
- **Author:** AJ Quick
- **Related:** [ADR-0005](0005-htmx-endpoints-return-partials.md), [ADR-0006](0006-long-running-ops-as-jobs.md), `docs/core/UI_PATTERNS.md`

## Context

A user clicks a button. The browser sends an HTMX request. The server
may respond in 50 ms or 4 s. The user cannot tell which until
something moves on screen. If nothing moves within 100 to 200 ms, the user
clicks again. Double-submissions, lost confirmation clicks, and repeated
fetches follow.

The gap between click and first visual feedback is where trust breaks
down. A spinner that appears after the server responds is useless. By
then the user has already re-clicked. Feedback must be pre-emptive:
the client acknowledges immediately, the server catches up later.

Three duration bands need different treatment:

- **≤ 200 ms**: Response arrives before the user notices a delay.
  A button disable is enough. No spinner needed, but one is acceptable.
- **200 ms to 5 s**: The user is waiting. An inline progress indicator
  signals that work is happening and the system has not hung.
- **> 5 s**: The operation is long-running. Per ADR-0006, it must be
  submitted to the job runner. The job modal IS the feedback.

The Phase 7 violation scan found only 1 HTMX button missing feedback
(`/status/vector-index/rebuild`). Compliance is already strong; this
ADR locks it in.

## Decision

Every user click that triggers async work MUST show visible feedback
within 100 ms of the click event, before the server responds.

Specific obligations:

1. Every `<button>` or `<a>` that triggers an `hx-post`, `hx-get`,
   `hx-delete`, or `hx-put` MUST carry `hx-disabled-elt="this"` to
   prevent double-submission. The disabled state renders within 100 ms.
2. Buttons whose corresponding server operation takes > 200 ms MUST
   also carry `hx-indicator` pointing to a sibling or ancestor spinner
   element. The spinner CSS class `htmx-request` activates on click,
   not on response.
3. Multi-step flows (wizard forms, staged classification) MUST
   acknowledge each step transition within 100 ms, typically by
   showing the next-step skeleton before the HTMX response fills it.
4. Operations that exceed 5 s MUST go through the job runner per
   ADR-0006. The job modal returned immediately by the route IS the
   100 ms feedback for that tier.
5. "Pending" state (submitted, awaiting response) MUST be visually
   distinct from "loading" state (response received, rendering) and
   from "complete" (swap done).

The 100 ms threshold is not an aspirational target. It is a hard
ceiling on time-to-first-visual-change after a click event.

## Consequences

### Positive
- Double-submissions from impatient re-clicks disappear once
  `hx-disabled-elt` is universal.
- The user always knows their click registered. No ambiguous stall.
- The three-tier model (disable / inline spinner / job modal) gives
  developers a clear decision tree for every new action.

### Negative / Costs
- Every existing button must be audited for `hx-disabled-elt`.
  Buttons inside table rows or dynamic partials are easy to miss.
- Spinner elements must be co-located with the button in the template
  so `hx-indicator` can address them. Template structure may need
  adjustment.

### Mitigations
- The UI cookbook (`docs/core/UI_PATTERNS.md`) ships a Jinja macro
  that renders a button + spinner + disabled-elt together. One call
  site, no forgetting.
- Compliance grep is mechanical and can run in CI.

## Compliance

Detect violations:

- **Missing hx-disabled-elt:** grep `src/lamella/templates/` for
  `hx-post=\|hx-delete=\|hx-put=` inside `<button` elements not
  also containing `hx-disabled-elt`.
- **Missing hx-indicator:** grep for `<button` with `hx-post=` where
  no sibling `<span class="htmx-indicator"` exists in the same
  template block.
- **Long ops not using job runner:** covered by ADR-0006 compliance.

## References

- CLAUDE.md §"Long-running operations run as jobs with a progress modal"
- [ADR-0005](0005-htmx-endpoints-return-partials.md): HTMX swap contract
- [ADR-0006](0006-long-running-ops-as-jobs.md): job modal for > 5 s ops
- `src/lamella/templates/partials/_job_modal.html`: modal partial
- `docs/core/UI_PATTERNS.md`: spinner + button macro

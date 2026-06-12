# ADR-0038: Toasts for Transient Feedback; Modals Only for Confirmation or Long-Form Input

- **Status:** Accepted
- **Date:** 2026-04-27
- **Author:** AJ Quick
- **Related:** [ADR-0006](0006-long-running-ops-as-jobs.md), [ADR-0036](0036-instant-feedback-100ms.md), [ADR-0041](0041-display-names-everywhere.md)

## Context

Without a rule, individual features invent their own feedback patterns.
Some show an alert div that persists until navigated away. Some reload
the page to flash a query-string message. Some use the browser's
`window.alert()`. Some do nothing and leave the user uncertain whether
the action worked.

Modals also proliferate. A "saved!" confirmation modal is a worse
user experience than a two-second toast. Conversely, a toast that says
"Are you sure you want to delete this rule?" is too easy to miss. The
user dismisses it reflexively and loses data they did not intend to lose.

The gap between these two concerns, transient success/error versus
irrevocable decisions, needs an explicit line.

The Phase 7 violation scan found modal usage dominant (119 modal vs 4
toast occurrences). Toast pattern is under-used for cases where it
would fit better.

## Decision

Two patterns cover all transient user communication:

| Pattern | Use for | Do not use for |
|---------|---------|----------------|
| Toast | Brief success, error, or info where the outcome is visible in the row or page state | Confirmation; long-form input; multi-step decisions |
| Modal | Irrevocable action confirmation; long-running job progress (per ADR-0006); multi-field forms requiring focused attention | Quick "saved" feedback; success on reversible actions |

Specific obligations:

1. **Toast macro:** rendered via `{{ toast(message, kind, duration) }}`
   from `_components/`. Kind is one of `success`, `error`, `info`.
   Default duration is 3000 ms. Toasts stack vertically and dismiss
   independently. They do not require user interaction.
2. **Toast placement:** toasts are injected via `hx-swap-oob` into a
   fixed toast container anchored to the viewport. They do not occupy
   document flow.
3. **Modals:** modal body uses the page-partial pattern. It does NOT
   extend `base.html`. Modals dismiss on Escape, on backdrop click,
   and on an explicit close button. The close button MUST be present
   even if Escape and backdrop click are also wired.
4. **Confirmation modals:** MUST show the name or identifier of the
   thing being deleted or changed (per ADR-0041, display name, not
   internal path or UUID). The cancel button is the default focused
   element when the modal opens.
5. **Job progress modals** follow ADR-0006. They are a distinct modal
   pattern implemented in `partials/_job_modal.html` and are not
   subject to the open/close rules above (they dismiss on job terminal
   status, not on user input).
6. MUST NOT combine toast and modal for the same action. Pick one.

## Consequences

### Positive
- Confirmation modals with cancel-default reduce accidental deletes.
- Toasts self-dismiss and do not force the user to acknowledge
  non-critical information.
- The job modal (ADR-0006) is a coherent member of the modal family.
  Same overlay, same dismiss behavior for the non-running case.

### Negative / Costs
- A toast container element must exist in `base.html` for OOB swaps
  to target. If a swap fires before `base.html` is loaded, the OOB
  target is missing and the toast is silently dropped.
- Authors must decide upfront whether an action is "reversible" or
  not to pick toast vs. modal. Some edge cases (e.g., bulk-dismiss
  with undo) require judgment.

### Mitigations
- The `_components/` macro library eliminates the decision about toast
  markup; authors call the macro or they call nothing.
- The ADR-0006 job modal is the canonical modal reference. Authors
  compare new modals against it.

## Compliance

Detect violations:

- **window.alert / window.confirm in templates or JS:** grep
  `src/lamella/templates/` and `src/lamella/static/` for
  `window.alert\|window.confirm\|window.prompt`.
- **Toast used for delete confirmation:** grep templates for
  `kind="error"` toasts adjacent to delete `<form` elements.
- **Modal lacking cancel button:** grep modal templates for absence
  of `type="button"` cancel element within `.modal` containers.
- **Dual toast + modal on same action:** code review gate; not
  automatically detectable.

## References

- CLAUDE.md §"Long-running operations run as jobs with a progress modal"
- [ADR-0006](0006-long-running-ops-as-jobs.md): job modal is a distinct modal pattern
- [ADR-0036](0036-instant-feedback-100ms.md): toasts satisfy the 100 ms feedback contract for fast successful ops
- [ADR-0041](0041-display-names-everywhere.md): confirmation modals show display names
- `src/lamella/templates/partials/_job_modal.html`: reference modal implementation

# ADR-0009: Card Binding Is a Starting Hypothesis, Not Ground Truth

- **Status:** Accepted
- **Date:** 2026-04-26
- **Author:** AJ Quick
- **Related:** [ADR-0010](0010-rules-are-signals-not-commands.md), `CLAUDE.md` ("Card binding is the strongest default signal, not infallible truth"), `src/lamella/ai/classify.py`

## Context

Every SimpleFIN transaction arrives with a card identifier. The
obvious inference is "card X was charged → expense belongs to the
entity that holds card X." That inference is wrong often enough to
produce silent accounting errors.

Cases that break it: an employee uses a personal card for a
business purchase; a business card is used for a personal expense;
the card is shared between entities during a transition period; a
recurring charge was set up on the wrong card and never corrected.
None of these produce a `bean-check` error. The ledger accepts
whatever account the classifier writes.

The classification pipeline in `ai/classify.py::build_classify_context`
already assembles the override signals that correct the card
hypothesis: active notes with `card_override`, merchant frequency
histograms (`suspicious_card_binding`), active project entity
bindings, and the AI's own `intercompany_flag`. A path that routes
directly from card to entity, skipping those signals, produces
silent errors.

## Decision

The card identifier MUST be used as the starting hypothesis for
entity attribution, not as the authoritative answer. The classifier
MUST consult all four override signals before settling on an entity.

Specific obligations:

1. `build_classify_context` MUST call `suspicious_card_binding` when
   a merchant string is available, and pass the result as
   `card_suspicion` to `propose_account`.
2. When an active note carries `card_override=True` and a non-empty
   `entity_hint`, the working entity MUST be replaced with
   `note.entity_hint` and the account whitelist MUST be widened to
   `all_expense_accounts_by_entity(entries)`.
3. When `ClassifyResponse.intercompany_flag` is `True`, the
   confidence gate MUST NOT auto-apply. The outcome MUST go to
   human review regardless of confidence score.
4. New classification paths (batch, rule-based, import) MUST call
   `build_classify_context` or replicate all four signal checks.
   MUST NOT implement a card→entity shortcut.
5. `resolve_entity_for_account` MUST be called when `conn` is
   available (registry binding); `entity_from_card` (string-split
   heuristic) is the fallback only when `conn` is `None`.

## Consequences

### Positive
- Intercompany errors are surfaced for human review rather than
  written silently to the ledger.
- Card-override notes let the user describe week-long or project-
  scoped card substitutions once; classify picks them up
  automatically.
- Merchant frequency histogram catches cards that are "wrong"
  by historical pattern even without an explicit note.

### Negative / Costs
- `build_classify_context` is more expensive than a direct
  card→entity lookup: it queries notes, computes a histogram
  over ledger entries, and checks Paperless.
- Every new ingest path must call the full context builder or
  explicitly document which signals it skips and why.

### Mitigations
- The histogram (`suspicious_card_binding`) short-circuits when
  the merchant has no history, so it is a no-op on fresh ledgers.
- Notes query uses a date-window index and is O(active notes),
  typically single-digit rows.
- `build_classify_context` is already the reference path for
  SimpleFIN classification; new paths have a worked example.

## Compliance

How `/adr-check` detects violations:

- **Card→entity shortcut:** grep `src/lamella/` for
  `entity_from_card(` calls outside of `build_classify_context`
  and `ai/context.py`. Each hit is a candidate shortcut.
- **Missing intercompany gate:** grep route handlers and batch
  classification loops for `intercompany_flag` consumption; any
  path that reads `ClassifyResponse` without checking
  `intercompany_flag` before auto-applying is a violation.
- **Missing card_suspicion parameter:** grep calls to
  `propose_account(` for the absence of `card_suspicion=` keyword
  argument.

## References

- CLAUDE.md §"Card binding is the strongest default signal, not infallible truth"
- `src/lamella/ai/classify.py`: `build_classify_context`, `propose_account`
- `src/lamella/ai/context.py`: `suspicious_card_binding`, `resolve_entity_for_account`, `entity_from_card`
- [ADR-0010](0010-rules-are-signals-not-commands.md): both ADRs govern the classification signal chain

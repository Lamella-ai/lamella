# ADR-0010: Rules Are Signals, Not Commands

- **Status:** Accepted
- **Date:** 2026-04-26
- **Author:** AJ Quick
- **Related:** [ADR-0009](0009-card-binding-as-hypothesis.md), `CLAUDE.md` ("Rules are signals, not commands"), `src/lamella/ai/gating.py`, `src/lamella/rules/engine.py`

## Context

Lamella learns from user decisions by promoting accepted
classifications into `classification-rule` directives. Early in
the project, every matching rule that hit a confidence threshold
was applied automatically. That model breaks down in two ways.

First, AI-generated rules encode a statistical pattern at a point
in time. The data the rule was derived from may not represent
the current context. A merchant that previously mapped to
`Expenses:EntityA:Office` may now bill a different entity. Auto-
applying a stale AI-learned rule produces silent errors in the books.

Second, the confidence number is a model estimate, not a guarantee.
An AI proposal at confidence 0.87 is directional input, not a
transaction certified correct. Routing it through human confirmation
produces a user rule at confidence 1.0, and that user rule is the
event that earns auto-apply rights.

See also the card-binding caution in [ADR-0009](0009-card-binding-as-hypothesis.md):
the card tells you who paid; it does not tell you if the expense
is attributed correctly. Rules carry the same caveat.

## Decision

A classification rule is directional input to the classifier, not
a lookup table that bypasses it, with one explicit exception.

| Rule type | Condition | Gate action |
|---|---|---|
| User-created, `confidence >= 0.95`, card-scoped | `created_by == "user"` | `AUTO_APPLY_RULE` |
| User-created, `confidence >= 0.70`, any card | `created_by == "user"` | `REVIEW_WITH_SUGGESTION` |
| AI-learned, any confidence | `created_by != "user"` | `REVIEW_WITH_SUGGESTION` only |
| Any rule, `intercompany_flag=True` | Phase G4 hard gate | `REVIEW_WITH_SUGGESTION` only |
| Any rule, Income target | Phase G4 hard gate | `REVIEW_WITH_SUGGESTION` only |

Normative obligations:

- MUST NOT auto-apply a rule whose `created_by` is not `"user"`,
  regardless of confidence value.
- MUST NOT auto-apply when `ai.intercompany_flag` is `True`.
- MUST NOT auto-apply when the target account root is `Income:`.
- MAY auto-apply a user rule when `confidence >= DEFAULT_AUTO_APPLY_THRESHOLD`
  (0.95) and none of the hard-review gates are triggered.
- A high-confidence AI proposal MUST land in `REVIEW_WITH_SUGGESTION`,
  never `AUTO_APPLY_AI`. User acceptance of that suggestion then
  triggers `learn_from_decision`, which creates a user rule.
  That rule earns auto-apply rights on subsequent identical transactions.

## Consequences

### Positive
- Silent misclassification from stale AI-learned rules is eliminated.
- Income and intercompany transactions always reach human review.
- The auto-apply path has a clear, testable gate: `created_by == "user"`.

### Negative / Costs
- AI proposals at high confidence still require one human click to
  become a rule. Users who prefer full automation will notice the
  extra confirmation step.
- The `created_by` field must be correctly set at rule-creation time.
  A bug that writes `"ai"` for a user-created rule silently downgrades
  its rights.

### Mitigations
- `ConfidenceGate.decide()` in `src/lamella/ai/gating.py` is the
  single routing point. All tests assert `GateAction` outcomes; no
  call site implements its own gate logic.
- Tests for `learn_from_decision` assert `created_by == "user"` on
  the emitted rule row.

## Compliance

- **Grep:** `grep -rn "AUTO_APPLY_AI\|auto_apply_ai" src/` should
  return no call sites that set this action outside of `gating.py`
  tests.
- **Grep:** `grep -n "created_by" src/lamella/ai/gating.py`. The
  auto-apply branch MUST check `rule.created_by == "user"`.
- **Review:** any new rule-creation path (rule writer, bulk-apply,
  import) MUST set `created_by` explicitly, not rely on a default.

## References

- CLAUDE.md §"Rules are signals, not commands"
- `src/lamella/ai/gating.py` (auto-apply branch)
- `src/lamella/rules/engine.py` (rule matching tiers)
- [ADR-0009](0009-card-binding-as-hypothesis.md)

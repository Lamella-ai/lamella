# ADR-0018: Classification Is Intentionally Slow

- **Status:** Accepted
- **Date:** 2026-04-26
- **Author:** AJ Quick
- **Related:** `CLAUDE.md` ("Classification philosophy"), `docs/specs/AI-CLASSIFICATION.md`, `src/lamella/ai/gating.py`, `src/lamella/ai/trickle_classify.py`, [ADR-0006](0006-long-running-ops-as-jobs.md)

## Context

Lamella's classifier sits at the intersection of two scarce
resources: accumulated context (receipts, notes, project tags,
mileage, neighbor confirmations) and paid LLM calls. A transaction
in the review queue without a linked receipt, active project, or
strong vector-neighbor consensus carries insufficient evidence for
a high-confidence classification. Classifying it speculatively
produces a FIXME re-classification or a wrong account, and neither
outcome is better than leaving it pending.

The queue depth is frequently mistaken for a backlog to clear.
It is not. It is a working set waiting for context to arrive.
Strategies that treat queue depth as a metric to minimize (adding
always-on schedules, shortening intervals, bulk-classifying on
every ingest) increase LLM spend without improving accuracy.

The existing implementation in `trickle_classify.py` codifies this:
it runs twice per day, applies a direct-evidence gate before any
LLM call, caps AI calls per run, and processes a pattern-match
sub-tier first (vector neighbors, no LLM cost) before invoking
the model at all.

## Decision

Classification MUST follow this priority order:

1. **Deterministic path.** User-created, confidence-1.0, card-scoped
   rules auto-apply via `ConfidenceGate.decide()`. No LLM call.
2. **Pattern-match path.** Vector-neighbor consensus (≥3 neighbors,
   similarity ≥ 0.85, same target). In-place rewrite, no LLM call.
   Implemented as sub-tier 1 in `trickle_classify.py`.
3. **Context-gated LLM path.** Only fires when the transaction has
   direct evidence: a linked receipt, OR active project + narration,
   OR narration + ≥2 vector neighbors classified to same target.
   Off-gate transactions are left pending.
4. **User-initiated bulk classify.** The user explicitly triggers
   a broader LLM sweep. Runs as a background job per [ADR-0006](0006-long-running-ops-as-jobs.md).

MUST prefer deterministic and pattern-match paths over LLM calls.
MUST prefer one-per-group with user confirmation over parallel
classification sweeps.
MUST NOT add always-on classifier schedules or shorten existing
intervals without a justification tied to an information-complete
transaction class.
MUST NOT treat queue depth as a performance metric to optimize.
MAY add new context signals to the direct-evidence gate when those
signals carry reliable evidence (e.g. a new document type with
predictable field semantics).

AI-created rules NEVER auto-apply; they only suggest. Only
user-created rules at confidence ≥ `DEFAULT_AUTO_APPLY_THRESHOLD`
(0.95) trigger auto-apply. Income targets NEVER auto-apply
regardless of source or confidence. Income attribution is a tax
decision requiring human confirmation every time.

## Consequences

### Positive
- LLM spend is bounded and predictable: `TRICKLE_LIMIT_PER_RUN`
  caps daily AI calls at ~50 in the worst case (two runs/day).
- High-evidence transactions get classified quickly; low-evidence
  transactions are held until context arrives, keeping accuracy high.
- The review queue reflects genuine ambiguity, not classifier
  backpressure.

### Negative / Costs
- Transactions without receipts or project context can sit in the
  queue for extended periods. Users who expect instant
  classification will find this surprising.
- Determining what constitutes "information-complete" for a new
  transaction class requires deliberate design work before adding
  gate criteria.

### Mitigations
- The trickle scheduler runs twice daily, so most context-ripe
  transactions are processed within 12 hours of context arriving.
- The user can always trigger bulk classify manually for urgent
  cases. The UI surfaces queue items waiting for context with
  explicit labels.
- `docs/specs/AI-CLASSIFICATION.md` documents the full gate
  criteria and scheduling rules as the authoritative reference.

## Compliance

`src/lamella/ai/gating.py::ConfidenceGate` is the single decision
point for auto-apply vs. suggest vs. review. New classification
paths MUST route through `ConfidenceGate.decide()`. Any code path
that bypasses the gate and directly applies a classification
without checking `created_by`, confidence, and the income/intercompany
hard gates is a violation. See `docs/specs/AI-CLASSIFICATION.md`
for the full scheduling and prioritization rules. Read it before
touching classifier, ingest, review-queue, or scheduler code.

## References

- CLAUDE.md § "Classification philosophy"
- `docs/specs/AI-CLASSIFICATION.md` (authoritative scheduling rules)
- `src/lamella/ai/gating.py` (`ConfidenceGate`, threshold constants)
- `src/lamella/ai/trickle_classify.py` (gate implementation, sub-tiers)
- [ADR-0006](0006-long-running-ops-as-jobs.md): classification runs as a background job

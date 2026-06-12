# ADR-0063: Bidirectional Document ↔ Transaction Matching

- **Status:** Accepted (2026-05-02)
- **Date:** 2026-05-02
- **Author:** AJ Quick
- **Related:** [ADR-0009](0009-card-binding-as-hypothesis.md), [ADR-0016](0016-paperless-writeback-policy.md), [ADR-0018](0018-classification-intentionally-slow.md), [ADR-0019](0019-transaction-identity-use-helpers.md), [ADR-0044](0044-paperless-lamella-custom-fields.md), [ADR-0053](0053-paperless-read-precedence-and-self-test.md), [ADR-0054](0054-hypothesis-is-ground-truth.md), [ADR-0056](0056-receipts-attach-pre-classification.md), [ADR-0061](0061-documents-abstraction-and-ledger-v4.md), [ADR-0062](0062-tag-driven-workflow-engine.md)
- **Depends on:** ADR-0061, ADR-0062 (composes)

## Context

Lamella's Paperless integration matches in one direction:
**transaction → document**. When a transaction lands in staging or
the ledger, the user (or the auto-sweep) hunts Paperless for a
matching document. The reverse direction is missing.

The current entry point is
`receipts/auto_match.py::sweep_recent` (`auto_match.py:112`),
which iterates unlinked transactions in a 60-day window and calls
`txn_matcher.find_paperless_candidates`
(`txn_matcher.py:177-471`) to score Paperless documents against
each transaction. The scoring is a staged cascade:

| Stage | Predicate | Base score |
|---|---|---|
| 1 | exact total + tight date window (3d) | 0.90 |
| 2 | exact total + wide date (30d) | 0.70 |
| 3 | exact subtotal + tight date | 0.55 |
| 4 | exact amount, any date | 0.45 |
| 5 | amount within ±50¢, tight date | 0.40 |
| 6 | merchant token match, wide window | 0.55 |
| 7 | last-four card-hint match | +0.10 boost |

Final score = base ± merchant/date/amount-in-content adjustments
(`txn_matcher.py:416-441`). The auto-link gate is
`AUTO_LINK_THRESHOLD = 0.90` (`auto_match.py:50`); below that
threshold, candidates surface in the receipt-hunt UI for the user
to confirm.

This logic is **direction-agnostic at its core**. Comparing a
transaction's amount against a document's total is the same
operation as comparing a document's total against a transaction's
amount. Same for date proximity and merchant token match. The only
direction-specific parts are:

- The query side (which table is iterated, which is searched)
- The exclusion filter (`RECEIPT_EXCLUDED_DOCTYPE_PATTERNS` for
  txn→doc; needs an analogue for doc→txn)
- The post-match side-effects (Paperless writeback for the
  forward direction; ledger directive write for both — already
  unified in `linker.py`)

The user's complaint is concrete: a receipt arrives in Paperless
**after** the transaction has already been classified and posted
to the ledger. There is no automatic mechanism to find that
transaction and link the document. The user has to manually open
the document, search for the transaction in Lamella, and click
link. With ADR-0062's polling, Lamella now sees new documents
arriving — but it still has no logic to match them backward.

The user further specified: high-confidence reverse matches should
auto-link, mirroring the existing `AUTO_LINK_THRESHOLD = 0.90`
pattern. Sub-threshold matches surface for review the same way the
classifier surfaces sub-threshold suggestions.

## Decision

Lamella adds a reverse direction matcher that, when a new document
lands via the Paperless poller, searches the ledger for a matching
transaction and auto-links if confidence ≥ 0.90, otherwise
surfaces the candidate set in the existing review UI. The scoring
core is refactored into a shared `Scorer` class used by both
directions; the auto-link threshold is the same; the side-effects
flow through the same `DocumentLinker` (renamed in ADR-0061).

### 1. Scorer abstraction

The current scoring logic is extracted from `txn_matcher.py` into
a new `receipts/scorer.py` (renamed to `documents/scorer.py` as
part of ADR-0061's directory rename). The class is direction-free:

```python
class Scorer:
    def __init__(self, settings: ScoringSettings): ...

    def score(
        self,
        candidate_amount: Decimal,
        candidate_date: date,
        candidate_text: str,           # narration | document content
        candidate_last_four: str | None,
        target_amount: Decimal,
        target_date: date,
        target_text: str,              # vendor | payee
        target_last_four: str | None,
    ) -> ScoredCandidate: ...
```

`ScoringSettings` carries the cascade thresholds (tight window
days, wide window days, fuzzy cents, merchant token rules) so all
of them are tunable from one place. The default values match the
current production tuning so the refactor produces no scoring
drift.

### 2. Two entry points, one scorer

The existing `find_document_candidates` (renamed from
`find_paperless_candidates` per ADR-0061) keeps its txn→doc
signature. A new function `find_ledger_candidates` is added with
the inverse signature:

```python
def find_ledger_candidates(
    conn: Connection,
    doc_date: date,
    doc_total: Decimal,
    doc_vendor: str,
    *,
    min_score: float = 0.60,
    max_candidates: int = 20,
) -> list[ScoredLedgerCandidate]: ...
```

Both call into the same `Scorer`. The query side differs (one
queries `paperless_doc_index`, the other queries staged
transactions and ledger entries), but the scoring is uniform.

### 3. Auto-link entry point

A new function `auto_link_unlinked_documents(conn, settings,
scorer)` is added in `documents/auto_match.py`. It iterates
documents that have `Lamella_Extracted` but lack `Lamella_Linked`,
calls `find_ledger_candidates` for each, and:

- If the top candidate's score ≥ 0.90 → calls
  `DocumentLinker.link()` to write the link directive, applies
  `Lamella_Linked` tag, writes a `workflow_action` audit row.
- If the top candidate's score is between 0.60 and 0.90 → leaves
  the document unlinked, but writes a `workflow_anomaly` row
  surfacing the candidate(s) in the existing review UI (see §5).
- If no candidate scores ≥ 0.60 → no audit row, no action.
  Document waits for the next poll or for a future txn to land
  that matches.

The `0.90` constant is shared with `auto_match.py::AUTO_LINK_THRESHOLD`
so a future tuning change updates both directions atomically.

### 4. Hookup to the workflow engine

ADR-0062 defines a `LinkToLedger` action. This ADR provides the
implementation: `LinkToLedger.run(doc, conn, scorer)` calls
`auto_link_unlinked_documents` for the single document and
returns the result. The default rule set in ADR-0062 §3 includes
the `link_to_ledger` rule that runs on each Paperless sync tick.

The composition order matters: extraction must run before linking,
because linking needs the extracted date and total. The default
rules enforce this via tag predicates: `link_to_ledger` selects
`HasTag("Lamella_Extracted") AND NOT HasTag("Lamella_Linked")`,
so it cannot fire on a document that has not been extracted yet.

### 5. Sub-threshold review UI

Sub-threshold candidates (0.60 ≤ score < 0.90) surface in the
existing `/staging-review` page (the AI classifier's review UI).
A new section "Documents pending link" renders unmatched documents
with their top candidates. Each candidate row offers:

- **Confirm** — POSTs to `/txn/{token}/document-link` (the
  existing manual-link route, renamed per ADR-0061)
- **Reject** — writes a `document_link_blocks` row so the pair is
  not re-scored on the next sweep

This mirrors the AI classifier's sub-threshold pattern: high
confidence auto-applies, sub-threshold surfaces for human
adjudication, rejected pairs are remembered.

### 6. Document-side exclusion filter

The current `RECEIPT_EXCLUDED_DOCTYPE_PATTERNS` regex is removed
in ADR-0061 in favor of a `document_type` discriminator. This ADR
defines the exclusion policy for the reverse direction:

- Documents with `document_type IN ('statement', 'tax')` are not
  candidates for auto-link in either direction. They may carry
  meaningful amounts but their semantics are not a single
  transaction.
- Documents with `document_type = 'other'` are auto-linked with a
  10% score penalty (because the type signal is weak).

The exclusion is applied at the SQL query level in
`find_ledger_candidates`, not as a post-filter, so excluded
documents do not consume scoring cycles.

### 7. Side-effect parity

The forward direction writes Paperless `Lamella_*` custom fields
via `paperless_bridge/writeback.py`. The reverse direction
**reuses the same writeback path**: after `DocumentLinker.link()`
succeeds, `writeback_after_link` runs (it already does, see
`writeback.py:301-418`). The fields written are identical:
`Lamella_Entity`, `Lamella_Category`, `Lamella_TXN`,
`Lamella_Account`. The user sees the same enriched document in
Paperless regardless of which direction discovered the link.

### 8. Confidence model documentation

The auto-link confidence model is exposed in
`docs/specs/MATCHING_CONFIDENCE.md` (new file). It documents:

- The cascade stages and base scores
- The per-adjustment contributions (merchant tokens, date
  proximity, amount-in-content)
- The two thresholds (0.60 review floor, 0.90 auto-link gate)
- How to tune `ScoringSettings` for a tighter or looser policy

The doc references this ADR and ADR-0056 for the linking
philosophy.

## Why this works

- **Inverting direction is the cheap part.** ~60% of
  `txn_matcher.py` is direction-free scoring. Extracting it into
  `Scorer` and adding a second query function is hundreds of
  lines, not thousands. The reverse-direction problem was
  open-ended only because the question framed receipts as the
  noun. Once we generalize to documents (ADR-0061), the inversion
  is structural, not algorithmic.

- **The same threshold applies in both directions.** The user
  asked for auto-link with high confidence in both directions.
  Sharing `AUTO_LINK_THRESHOLD = 0.90` means a tuning change
  affects both directions atomically. There is no asymmetry
  between "confident enough to auto-link forward" and "confident
  enough to auto-link backward."

- **The tag-workflow engine (ADR-0062) is the orchestrator, not
  this ADR.** This ADR provides the matching logic; ADR-0062
  decides when to run it. Separation lets each evolve
  independently — a future improvement to scoring tuning lands
  here; a future improvement to schedule cadence lands there.

- **Side-effect parity makes direction invisible to the user.**
  Whichever direction discovered the link, the document in
  Paperless ends up with the same `Lamella_*` fields populated
  and the same `Lamella_Linked` tag. Grep, search, and reports
  treat both populations identically.

- **Sub-threshold candidates reuse the existing review surface.**
  The `/staging-review` page already shows sub-threshold AI
  classifier suggestions for human adjudication. Adding a
  "Documents pending link" section reuses the user's mental
  model. There is no new review queue for them to learn.

- **The exclusion policy is type-driven, not regex-driven.**
  ADR-0061 replaces the regex-on-document-type-name with a typed
  discriminator. This ADR uses the discriminator. A user-defined
  Paperless document type is correctly classified at sync time
  (via settings), and the exclusion logic stays consistent
  whether the user names their statement type "Statement" or
  "Bank statement" or "Mthly stmt."

## Compliance checks

This ADR is satisfied iff:

1. `documents/scorer.py` exists, `Scorer` is the only place the
   cascade thresholds and adjustment rules are defined. (Test:
   `tests/test_scorer_direction_invariance.py` — same input
   pair scores identically forward and backward.)
2. `find_document_candidates` and `find_ledger_candidates` both
   route through `Scorer`. No copy-pasted scoring code.
3. `auto_link_unlinked_documents` is invoked by the
   `LinkToLedger` action from ADR-0062 and applies the
   `Lamella_Linked` tag on success.
4. The auto-link threshold is `0.90` and is the same constant
   used by both directions. (Test:
   `tests/test_auto_link_threshold_shared.py`.)
5. Sub-threshold candidates appear in `/staging-review` under a
   "Documents pending link" section.
6. Documents with `document_type IN ('statement', 'tax')` are
   excluded from candidate sets at the SQL level. (Test:
   `tests/test_reverse_match_excludes_statements.py`.)
7. After a reverse-direction auto-link, the linked document
   carries the same four `Lamella_*` custom fields a
   forward-direction link would have written (ADR-0044).
8. `docs/specs/MATCHING_CONFIDENCE.md` exists and is referenced
   from the project index.

## What this ADR does not decide

- The polling cadence — that is ADR-0062.
- The tag namespace — that is ADR-0061 and ADR-0062.
- A user-tunable confidence-threshold UI. The threshold is a
  shared constant for now; if the user later wants a slider, it
  becomes a setting.
- ML-based scoring (vendor-name embeddings, learned weights). The
  cascade is rules-based and deterministic per ADR-0018; that is
  the intentional design point. ML is a future ADR if it ever
  becomes the right answer.

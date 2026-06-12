# Document ↔ Transaction Matching Confidence

> Status: living spec for [ADR-0063](../adr/0063-bidirectional-document-transaction-matching.md). Updates land here when the scorer's tuning changes.

This document describes how Lamella decides whether a document
(receipt, invoice, order confirmation, etc.) and a ledger
transaction are the same financial event, and what the scoring
output is used for.

The scoring core is [`receipts/scorer.py`](../../src/lamella/features/receipts/scorer.py).
Both the forward direction (`txn → doc`, used by the receipt-hunt
sweep at `/inbox` and by `auto_match.sweep_recent`) and the reverse
direction (`doc → txn`, used by `auto_link_unlinked_documents`)
route through the same `Scorer.score()` method. Direction
invariance — scoring `(doc, txn)` yields the same total as scoring
`(txn, doc)` with the inputs swapped — is a tested invariant.

---

## Scoring model

The cascade picks the **strongest applicable stage** as the base
score, then layers per-adjustment contributions on top, capped at
1.0.

### Cascade stages

| Stage | Predicate | Base |
|---|---|---|
| 1 | total exact + tight date (≤3 days) | 0.90 |
| 2 | total exact + wide date (≤30 days) | 0.70 |
| 3 | subtotal exact + tight date | 0.55 |
| 4 | total exact, any date | 0.45 |
| 5 | total within ±$0.50 + tight date | 0.40 |
| 6 | correspondent/vendor token match + wide date | 0.55 |

A pair that satisfies multiple stages takes the highest base; the
others contribute as `reasons` in the result.

### Per-adjustment contributions

| Signal | Δ |
|---|---|
| last-four card hint match | +0.10 |
| amount appears literally in document content | +0.08 |
| ≥2 merchant tokens shared | +0.10 |
| 1 merchant token shared | +0.05 |
| date 4–30d off (penalty) | −0.03 |
| date >30d off (penalty) | −0.10 |
| `document_type = 'other'` (soft penalty per ADR-0063 §6) | −0.10 |

### Hard rejects

| Condition | Result |
|---|---|
| `document_type ∈ {statement, tax}` | total = 0.00, verdict = reject |
| currency mismatch (USD vs EUR, etc.) | total = 0.00, verdict = reject |

These are enforced **at the SQL query level** in
`find_document_candidates` and `find_ledger_candidates` so excluded
documents do not consume scoring cycles. The `Scorer.score()`
method also short-circuits on them as a defense in depth.

---

## Thresholds

Two constants live in `scorer.py` and are imported by every caller
that cares about the verdict. **Both directions share the same
constants** so a tuning change updates the system atomically.

| Constant | Value | Verdict | What it triggers |
|---|---|---|---|
| `REVIEW_THRESHOLD` | 0.60 | review | Surfaces in /inbox under "Documents pending link" with Confirm/Reject row actions. |
| `AUTO_LINK_THRESHOLD` | 0.90 | auto_link | Auto-linked unattended (subject to the confidence-gap rule below). |

A score below `REVIEW_THRESHOLD` is a `reject` verdict; the pair is
not surfaced and not blocked — the document waits for the next
sweep or for a future txn that scores higher.

### Confidence gap (auto-link only)

In `auto_link_unlinked_documents`, even a score ≥ 0.90 is **not**
auto-linked when the second-place candidate trails by less than
`paperless_auto_link_min_confidence_gap` (default 0.10). Two near-
equal candidates are ambiguous; the doc is queued for review
instead. This guards against the failure mode where a vendor
submits two same-amount transactions in the same window (a tip vs
no tip, two same-amount purchases) and the scorer picks one
arbitrarily.

---

## Tuning guide

Knobs live on `ScoringSettings` in `scorer.py`. Today they are not
exposed as a UI; tuning is a code change + restart.

| Symptom | Knob to turn |
|---|---|
| Too many false-positive auto-links on noisy ledgers | Raise `AUTO_LINK_THRESHOLD` (e.g. 0.92). |
| Too many real matches getting queued for review | Lower `AUTO_LINK_THRESHOLD` (e.g. 0.88) or `paperless_auto_link_min_confidence_gap` (e.g. 0.05). |
| Same-amount different-day pairs incorrectly auto-link | Tighten `tight_window_days` (e.g. 2) or raise `base_amount_wide_date` so a 3-day match scores higher than a 25-day match. |
| Tip-bearing receipts (amount = subtotal + tip) miss | Raise `fuzzy_cents` (default 50 = $0.50). |
| Bank statements still leaking through as candidates | Verify `document_type` is being populated by the sync; the regex fallback only catches names matching `RECEIPT_EXCLUDED_DOCTYPE_PATTERNS`. |
| Reverse direction misses obvious matches | Widen `window_days` in the `find_ledger_candidates` call (currently 30). |

---

## Direction invariance

The scorer is direction-free by design — every input field is paired
with its counterpart on the other side, and the formula uses no
predicate that would yield a different value for `(doc, txn)` vs
`(txn, doc)` with the same data. Tested in
`tests/test_scorer_direction_invariance.py`.

If you change the scoring formula, **keep this invariant**. Any
asymmetry between forward and reverse scoring is a bug.

---

## Side-effect parity

When the reverse direction (doc → txn) auto-links a pair, the
written ledger directive and Paperless metadata are **identical**
to a forward-direction (txn → doc) link. The `DocumentLinker.link()`
method is the single write path; it doesn't know which direction
discovered the match. This means the user sees the same enriched
document in Paperless and the same `document-link` directive in
the ledger regardless of which direction caught the pair.

References:
- [ADR-0063](../adr/0063-bidirectional-document-transaction-matching.md) — the spec this implements
- [ADR-0061](../adr/0061-documents-abstraction-and-ledger-v4.md) — `document-*` directive vocabulary
- [ADR-0044](../adr/0044-paperless-lamella-custom-fields.md) — Paperless `Lamella_*` custom fields
- [ADR-0056](../adr/0056-receipts-attach-pre-classification.md) — pre-classification linking philosophy
- [ADR-0018](../adr/0018-classification-intentionally-slow.md) — rules-based, deterministic scoring (no ML in scope today)

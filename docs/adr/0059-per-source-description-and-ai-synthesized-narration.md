# ADR-0059: Per-source description preservation + AI-synthesized canonical narration

> **Active follow-ups + test plan:**
> [`docs/proposals/TODO-2026-04-29-multi-source-dedup-followups.md`](../proposals/TODO-2026-04-29-multi-source-dedup-followups.md)

**Status:** Accepted

**Date:** 2026-04-29

## Context

ADR-0019 established paired source meta on the bank-side posting:

```
  Liabilities:Acme:BankOne:Card  -42.17 USD
    lamella-source-0: "simplefin"
    lamella-source-reference-id-0: "TRN-..."
```

This shape supports up to 9 source observations of one event (per
the index suffix). ADR-0058 (the cross-source dedup oracle) made
intake-time multi-source observation feasible — when a new source
sees a leg an existing source already saw, the row inherits the
matched record's `lamella-txn-id`. The next question (raised by AJ
2026-04-29) is what the *content* of those observations looks like
when sources disagree.

> Concrete scenario: the same $50 transfer event is observed by:
> * the original ledger entry (narration written by hand, e.g.
>   `"Checking → PayPal"`),
> * a bank-feed source's row (description as the bank phrased it,
>   e.g. `"Transfer from PayPal"`),
> * a payment-processor CSV import (description as the processor
>   phrased it, e.g. `"Transfer to Bank"`).

Three different texts describing the same event. Today the bank
ledger entry has ONE narration field, which means whichever source
landed first wins and the other phrasings are silently dropped on
re-import or never visible to the user.

The implications:

* Provenance is incomplete. "What did SimpleFIN actually call this
  on date X?" is unanswerable from the ledger alone.
* The dedup oracle's strict description match misses the case
  above (different sources phrase the same event differently —
  and the oracle requires normalized equality today).
* The user has no way to construct a description that synthesizes
  what each source said. The system either picks one source's
  phrasing or the user re-edits manually.

## Decision

Per-source descriptions are first-class on-disk content. Each
source observation that carries a description text writes it
verbatim alongside the existing paired source / source-reference-id
keys, at the same index:

```
  Liabilities:Acme:BankOne:Card  -42.17 USD
    lamella-source-0: "simplefin"
    lamella-source-reference-id-0: "TRN-AAA"
    lamella-source-description-0: "POS DEBIT — COFFEE SHOP #1234"
    lamella-source-1: "csv"
    lamella-source-reference-id-1: "ROW-42"
    lamella-source-description-1: "Coffee Shop — Decaf and a scone"
```

The transaction's top-level `narration` is the **canonical**
description — not "the description from any one source," but a
synthesized text we own, ideally produced by an inexpensive LLM
(Haiku) reading every source's phrasing for the same event and
emitting one coherent line. The user can also set it manually; the
synthesizer never overwrites a user-set narration.

### Key shape

* `lamella-source-description-N` (string) — the source's verbatim
  description text for this leg, at the same index as
  `lamella-source-N` and `lamella-source-reference-id-N`.
* Optional. Some sources don't carry useful description text
  (e.g. plain wire references); when absent, the index just
  doesn't include a `-description-N` line. The presence of
  `lamella-source-N` is what defines an observation; the
  description is a richer payload on top of it.

### Canonical narration

The transaction's top-level narration field is generated, not
copied. Sources of truth for the synthesizer:

1. Every `lamella-source-description-N` on the bank-side posting.
2. The transaction's `payee` (often the cleanest single-merchant
   token).
3. The signed amount + the account names on each posting (so the
   synthesizer knows direction — "from PayPal" vs "to PayPal").

The synthesizer is called:

* **On promotion** — when a staged row is collapsed into a ledger
  entry, run the synthesizer over whatever source descriptions are
  on the bank-side posting (typically just one at first creation).
* **On multi-source confirm** — when the user clicks "Confirm —
  same event" on `/review/duplicates`, the new source's
  description is appended as `lamella-source-description-N` AND
  the synthesizer re-runs against the now-richer set, refreshing
  the canonical narration if it isn't user-pinned.

User-pinned narrations are left alone. We mark synthesizer-owned
narrations with `lamella-narration-synthesized: TRUE` so the
re-run pass can distinguish them from a manually-edited line. A
user editing the narration drops the marker; from then on the
synthesizer doesn't touch it.

### Why Haiku

* Cheap and fast — multi-source synthesis is a per-event call.
  Haiku is the right cost band for "summarize ≤ 5 short strings
  into one short line."
* Latency-tolerant — synthesis happens at promote time / confirm
  time, not on a hot read path.
* No reasoning required — the input is already structured (amount,
  direction, source descriptions); the model just composes.

The synthesizer is implemented as a port (`NarrationSynthesizer`)
behind the same adapter pattern as other AI calls (ADR-0020). The
default implementation calls Haiku via the `ai_cascade` service;
tests use a deterministic in-memory adapter that returns "synth: "
+ joined source texts.

## Consequences

* Every ledger entry carries the full multi-source provenance on
  disk. "What did each source actually say?" is answerable from
  the .bean file alone — no DB required (ADR-0001).
* The dedup oracle's exact-description match remains correct as
  the high-confidence tier; the medium tier (different
  descriptions, same date + signed amount + payee or
  account-direction signal) becomes feasible because each source's
  raw description survives instead of being dropped.
* The user reads ONE coherent narration on review surfaces while
  retaining access to per-source detail when debugging.
* Re-emit (ADR-0057) preserves every `lamella-source-description-N`
  through the round trip via the typed envelope.
* User edits to narration are sticky — the synthesizer never
  overwrites manual edits.

## Compliance

* [x] ADR drafted documenting the contract.
* [x] Writer emits `lamella-source-description-0` next to
  `lamella-source-0` / `lamella-source-reference-id-0` on the
  bank-side posting when a source description is present.
* [x] `PendingEntry` gains an optional `source_description` field
  threaded from the staged row.
* [x] `bank_sync.ingest` populates `source_description` from the
  staged row's `description` (or `payee` fallback).
* [x] Tests for writer rendering (description present / absent
  branches) + tests for `_q` escaping of awkward source text
  (quotes, backslashes).
* [x] `NarrationSynthesizer` port +
  `DeterministicNarrationSynthesizer` (no-AI default) +
  `HaikuNarrationSynthesizer` (production adapter with
  graceful fallback to deterministic on any error). Landed
  2026-04-29 — `features/ai_cascade/narration_synthesizer.py`.
* [x] Confirm-as-dup writer (ADR-0058 follow-up): append
  `lamella-source-description-N` alongside source / source-ref
  paired meta on the matched ledger entry's bank-side posting,
  and re-run the deterministic synthesizer to refresh the
  canonical narration. Landed 2026-04-29 in
  `/review/duplicates/{id}/confirm`.
* [x] `lamella-narration-synthesized: TRUE` marker writeback so
  future synthesis passes detect user edits. Landed 2026-04-29 —
  `bank_sync.synthetic_replace.rewrite_narration_in_place`.
* [ ] Promotion path calls the synthesizer when no manual
  narration is present (single-source first-emit invocation).
* [ ] `_legacy_meta.normalize_entries` reads the new key shape
  transparently (forward-compat across the ADR-0019 paired-meta
  cleanup path).

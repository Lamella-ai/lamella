# ADR-0056: Receipt-Linking Is a Pre-Classification Affordance, Not a Post-Promotion One

- **Status:** Accepted
- **Date:** 2026-04-28
- **Related:** [ADR-0009](0009-card-binding-as-hypothesis.md), [ADR-0018](0018-classification-intentionally-slow.md), [ADR-0044](0044-paperless-lamella-custom-fields.md), [ADR-0053](0053-paperless-read-precedence-and-self-test.md), [ADR-0054](0054-hypothesis-is-ground-truth.md)

## Context

A receipt is a context signal. It carries the line-items, vendor
name, total, and (often) the payment account that the bank-feed
narration alone does not. The classification cascade — rules → AI
tier 1 → tier 2 → user — uses receipts as an input the moment one
is linked to a transaction. The link is the dependency edge.

Until this ADR, the UI implicitly assumed the dependency edge ran
the wrong way: a transaction had to land in the ledger before it
became eligible for receipt-linking. The /search page exposed a
"Find receipts" bulk affordance on the Ledger results section but
not on the Pending in staging section. /transactions, /card, and
the per-txn detail page each route through the immutable
``/txn/{token}`` URL, which already supports staged-side linking
via ``/txn/{token}/receipt-link`` — but on /search the staged
section dropped the affordance entirely. A user looking for a
receipt across both staged and posted views had to find it on the
ledger side, OR promote first and then hunt — both reverse the
intended flow.

The reversed flow has a real cost. Pre-classification is exactly
when the receipt's signal is most valuable: the AI is choosing the
target account, and the receipt's vendor + line-items + total
narrow the choice. Withholding the link until after promotion
classifies the transaction without the very signal the design
calls "context-first" (CLAUDE.md, ADR-0018). The
post-promotion link still works — it just no longer steers
classification, which was the point.

The backend already accepts staged-side links. ``receipt_links.txn_hash``
is overloaded to store either a Beancount content hash (ledger
row) or a UUIDv7 ``lamella-txn-id`` (staged row). The
``ReceiptLinker.link()`` call signature treats ``txn_hash`` as
opaque-string. The only thing missing was the UI surface and the
``run_hunt`` resolver step that maps tokens back to a row in
``staged_transactions`` so the hunt's amount/date/vendor logic has
something to score against.

## Decision

Every transaction-list surface in the UI must offer the same
receipt-link affordance, regardless of whether the row is staged
or posted to the ledger.

Concretely:

1. Any page that lists transactions and shows a "Find receipts"
   action MUST show that action on staged rows too, not only
   ledger rows. The action's bulk-bar checkbox column, hunt
   trigger, and tolerance-days field are all required on the
   staged section.

2. The staged checkbox value is the row's UUIDv7
   ``lamella_txn_id``. The form field name is the same
   (``txn_hash``) the ledger section uses. The same
   ``/search/receipt-hunt`` endpoint accepts both keyings.

3. ``run_hunt`` resolves selected values against ``by_hash`` first
   (ledger), and any unresolved value against
   ``staged_transactions`` second. A staged row produces a synth
   txn-shape carrying the four pieces the candidate-find logic
   needs: target_amount, date, payee/narration label, currency.
   ``ReceiptLinker.link(txn_hash=<token>, ...)`` writes the link
   directive to ``connector_links.bean`` keyed on the token, just
   like ledger-side links are keyed on the content hash.

4. Auxiliary side-effects that genuinely require a full Beancount
   ``Transaction`` (Lamella_* writeback per ADR-0044, post-link
   verify-and-correct per ADR-0054) are gated on "is real
   Transaction" and skipped on staged-side links. Those
   side-effects re-run naturally when the staged row is later
   promoted, because the promote path already wires the same
   linker hooks. The link itself — the must-have — succeeds in
   both cases.

5. Per-txn detail pages (``/txn/{token}``) and per-txn drawers
   already conform: the receipt-link panel renders against the
   token regardless of staged vs ledger. No new rule for those.
   The rule above is concretely about list surfaces (search,
   transactions list, card pane, future bulk views).

## Why this works

- **Receipts are read into the AI prompt.** The design point
  (CLAUDE.md, ADR-0018) sets classification as context-determined;
  the receipt is a first-class context input. Feeding it in
  pre-classification is
  the design point. Feeding it post-promotion is correction, not
  classification.

- **The token is durable.** ADR-0046 + the immutable
  ``/txn/{token}`` URL guarantee the lamella-txn-id survives the
  staged → promoted transition. A receipt linked on the staged
  row is still linked on the promoted row — the directive in
  ``connector_links.bean`` keys on the token, and the promoted
  row's directive carries the same token via the
  ``lamella-txn-id`` meta. No relink, no orphan.

- **Backend already supports it.** No schema change. The
  affordance gap was UI-only plus a 30-line resolver step in
  ``run_hunt``. The cost of the gap was the entire
  context-determines-classification design point being silently
  bypassed on the largest list surface a user uses.

- **The auxiliary writeback gate is a feature, not a workaround.**
  Lamella_* fields encode entity / category / payment-account
  derived from postings the staged row doesn't have yet. Skipping
  writeback on staged-side is correct; running writeback once
  postings exist is correct. The two cases are the same gate
  expressed in opposite directions.

## Consequences

- **No UI may show a transaction without offering the link path.**
  Any new list view (existing or future) must include the
  receipt-find affordance on staged rows. Reviewer rule of thumb
  on PR: if a template iterates over staged rows and renders a
  Ledger-section bulk-bar in the same file, the staged section
  needs a parallel bulk-bar.

- **``/search/receipt-hunt`` carries mixed keyings.** A single
  POST may contain ledger hashes AND staged tokens in the same
  ``txn_hash`` multi-value list. The handler is unaware; the
  resolver in ``run_hunt`` partitions them. This is the durable
  shape — don't add a separate ``staged_token`` form field.

- **Classification gets receipt context earlier.** Once a hunt
  links a receipt to a staged row, the AI cascade reads it on
  the next classification pass. This narrows ambiguous
  classifications from "merchant + amount" to "merchant + amount
  + receipt total + receipt vendor + receipt line-items," which
  is the intended steady state.

- **No new ADR is needed when adding the affordance to a new
  surface** — this ADR governs all of them. The acceptance
  criterion is uniform: every list of transactions offers the
  same receipt-link path, full stop.

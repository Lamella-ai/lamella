# ADR-0054: Linked-Txn Hypothesis Is Ground Truth, Not a Suggestion

- **Status:** Accepted
- **Date:** 2026-04-29
- **Related:** [ADR-0009](0009-card-binding-as-hypothesis.md), [ADR-0010](0010-rules-are-signals-not-commands.md), [ADR-0016](0016-paperless-writeback-policy.md), [ADR-0018](0018-classification-intentionally-slow.md), [ADR-0044](0044-paperless-lamella-custom-fields.md), [ADR-0053](0053-paperless-read-precedence-and-self-test.md)

## Context

When a user links a Paperless receipt to a Lamella transaction and
then runs receipt verify, the service builds a "hypothesis" object
from the linked transaction's known fields (date, amount, payee /
narration) and passes it to the AI alongside the OCR'd receipt text.
The AI extracts structured fields (vendor / total / date / line
items) and the service compares them against current Paperless
state to decide what to PATCH back.

The verify cascade has, in production, treated the AI's extraction
as authoritative even when the AI's vendor disagreed with the
hypothesis. The general failure shape: a receipt with stylized
header branding (logo image, brand glyph) where Paperless's OCR
captured surrounding text but not the brand mark. The AI returned
a confident vendor reading from whatever header line OCR did
capture, which contradicted the hypothesis. The verify service
accepted the AI's result on average-confidence alone and stamped
fields that disagreed with the linked transaction.

This is the wrong default. The hypothesis is not a suggestion. It
is the bank statement, the SimpleFIN row, the user's own typing.
It IS ground truth for "which transaction this receipt belongs to."
The AI is corroborating; the AI is not the source of truth about
the merchant.

The existing ADR-0009 rule for classification ("card binding is a
starting hypothesis, not ground truth") is the OPPOSITE of what's
needed here. ADR-0009 deliberately weakened a single signal
(card-charge identity) so AI could override it with note / project
/ history evidence. ADR-0054 codifies the inverse rule for
verify: the LINK between a Paperless document and a ledger
transaction is established BEFORE verify runs, by user action or
high-confidence matcher action; verify's job is not to second-guess
that link, it is to extract structured facts from the receipt that
match the link the user already trusts.

## Decision

When receipt verify runs against a Paperless document linked to a
ledger transaction, the linked transaction's fields (date, amount,
payee) are GROUND TRUTH. The AI's role is corroboration only. Any
disagreement between the AI's extraction and the hypothesis MUST
trigger one of two outcomes, never silent acceptance:

1. **Vision escalation** when the disagreement is plausibly an OCR
   limitation (most often vendor mismatch — stylized brand logos do
   not survive Paperless's OCR pipeline). The service forces a Tier
   2 vision pass against the receipt image.
2. **User-visible flag** when vision also disagrees with the
   hypothesis. The receipt is flagged as POSSIBLY MISATTACHED on
   the /receipts row and the verify outcome carries a
   ``link_suspect=True`` field for the UI to surface. No PATCHes
   land in this state — the system asks the user to confirm or
   unlink.

### Implementation rules

The detection must NOT depend on the AI self-flagging. Past failure:
the model invented an ad-hoc tag string
(``VENDOR_MISMATCH_WITH_HYPOTHESIS``) instead of the canonical
``NEEDS_VISION`` and the escalation logic missed it. The escalation
gate is a code-level comparison of the extracted vendor against the
hypothesized vendor, performed AFTER the AI replies and BEFORE the
"accept Tier 1" decision. The AI's self-flag remains a signal but
is no longer the only one.

The vendor-match function uses substring + normalize semantics so
"The {brand}" matches "{brand}" and casing / punctuation don't
trip it. Empty hypothesis vendor returns "match" (no claim to
disagree with).

The same rule applies to:

- **Date mismatch beyond 7 days**: receipt date that disagrees with
  the linked transaction's date by more than a week is a probable
  link-error signal, not a receipt-OCR-error signal. Escalate to
  vision; if vision agrees the dates are different, flag link as
  suspect.
- **Total mismatch beyond 5%**: same shape. The transaction's amount
  is the bank's record. A receipt total that disagrees by a few
  cents is rounding / tip / sales-tax-locality drift; a receipt
  total that disagrees by 50% is the wrong receipt.

Threshold tuning is a follow-up; the principle is the same in all
three: the linked txn's value is ground truth, the AI's value is
the suspect.

### Prompt-side reinforcement

The Tier 1 + Tier 2 system prompts state the rule explicitly:

> The caller's hypothesis is GROUND TRUTH from the linked transaction.
> Your job is to corroborate, not to overrule. Stylized logos and
> branding don't always come through OCR; if your extracted vendor
> disagrees with the hypothesized vendor, the OCR is more likely
> wrong than the hypothesis.

The prompt also pins the canonical tag: ``NEEDS_VISION`` is the
ONLY string the caller treats as an escalation request. Synonyms
are ignored. The model is told this so it doesn't waste tokens
inventing alternatives.

### Counterexample: when this rule does NOT apply

This ADR is scoped to RECEIPT VERIFY, where the (txn, doc) link is
already established. It does NOT apply to:

- **The matcher** (``features/receipts/hunt.py``,
  ``features/receipts/auto_match.py``). The matcher's job is
  precisely to PROPOSE a link from receipt fields to txn fields;
  AI extraction there has full authority. The hypothesis-as-ground-
  truth rule depends on the link existing already.
- **AI classify** (``features/ai_cascade/classify.py``). The
  classifier proposes the txn's CATEGORY; there's no "hypothesis"
  about the category from the bank's side. ADR-0009/0010 govern
  there.

## Consequences

- **Misattached receipts surface instead of stamping wrong data.**
  Any verify run where the AI's extracted vendor disagrees with the
  hypothesis goes through vision automatically. If vision also
  disagrees with the hypothesis, the receipt gets flagged as
  suspect rather than stamped with custom-field values that
  contradict the linked transaction.

- **Vision is invoked more often than before.** Cost increase is
  bounded: only verifies that disagree on vendor / date / total
  trigger Tier 2. The user-perceived latency for the legitimate
  case (receipt and txn agree, OCR is fine) is unchanged: Tier 1
  finishes and the avg-confidence gate accepts.

- **AI's self-flag is no longer load-bearing.** The model can drift
  on tag wording (recent run invented one) and the escalation still
  fires correctly. The flag remains a hint but is no longer the
  only path.

- **A test must lock in the rule.** New test:
  ``test_verify_escalates_on_hypothesis_vendor_mismatch`` asserts
  that a Tier 1 extraction with confident-but-disagreeing vendor
  triggers vision regardless of confidence average. Without this
  the rule rots silently.

- **The /receipts UI eventually surfaces "link suspect" rows.** Out
  of scope for this ADR; tracked as a follow-up. Today the suspect
  signal lives only in logs and the verify outcome dataclass.

## Notes

The bug that prompted this ADR was caught during user testing on
2026-04-29 with a Pro Xtra-membered hardware-store receipt
combined into a single PDF with an unrelated retailer's header.
The user's exact framing: "OUR hypothetical data is what needs to
govern the connection. We are saying to the AI: this is the
receipt that we THINK matches this transaction. We are asking it
to confirm our suspicion. We are NOT asking it to overrule us."

Implementation landed in commit d830f1f (this same date).

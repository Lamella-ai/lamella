# ADR-0055: AI Prompts Must Be Generalized, Never Overfitted to a Specific Failure

- **Status:** Accepted
- **Date:** 2026-04-29
- **Related:** [ADR-0010](0010-rules-are-signals-not-commands.md), [ADR-0017](0017-example-data-policy.md), [ADR-0018](0018-classification-intentionally-slow.md), [ADR-0054](0054-hypothesis-is-ground-truth.md)

## Context

When a user reports that the AI gave the wrong answer on a specific
input, the temptation is to patch the prompt with a counter-example
of that exact case. "When you see X, don't do Y." This pattern is
appealing because it's concrete, testable on the failing input, and
ships a visible fix. It is also corrosive at scale.

The failure mode: each specific patch makes the prompt longer and
narrower. A prompt that grows by one example per bug becomes a
prompt that distracts the model with corner cases on every call.
The model starts pattern-matching the counter-examples instead of
reasoning from the general rule. New failures appear in the gaps
between the listed examples, each one inviting another concrete
patch, and the prompt becomes a list of historical embarrassments
rather than a description of what the model should do.

A concrete instance prompted this ADR. The receipt-verify Tier 1
prompt asked the model to return ``NEEDS_VISION`` in
``ocr_errors_noted`` when escalation was warranted. A model run
returned a synonym tag (different verbatim string) and the
caller's literal-match check missed the escalation. The patch
that nearly went out: append "do not invent synonyms like
'<the specific synonym the model just used>' — the caller only
honors the literal token 'NEEDS_VISION'." The user caught it:
"You're breaking a rule. The prompts must be so generalized that
it won't introduce specifics that cloud all other uses against
the AI agent."

That patch would fix the next run that emitted that exact synonym
and do nothing for the next run that emits a different one. The
right fix is to state the contract once, generally: "use the
literal token NEEDS_VISION; any other string is ignored." No
counter-examples, no enumerated wrong tags. The model reasons
from the rule.

## Decision

Every AI prompt in this codebase follows three rules:

1. **State the contract, not the failure.** When the model is
   expected to do or not do something, the prompt says what's
   expected, not what it must avoid that one of yesterday's runs
   happened to produce. Concrete bad: "do not invent synonyms like
   X / Y / Z." Concrete good: "use the literal token T; any other
   string is ignored."

2. **No data from the user's actual world inside the prompt.** Per
   ADR-0017 the canonical-placeholder rule already governs example
   data; this ADR extends it to prompts. If a prompt needs an
   illustrative example, the example uses canonical placeholders
   (Acme, Example LLC, Jane Doe, 123 Main St). Real merchants /
   account names / regions never appear, even as "what not to do."

3. **No reactive patches.** When a user reports a specific failed
   input, the response is to find the GENERAL rule the model
   missed and reinforce it, OR to fix the code-side gate the
   AI's output flows through, OR to escalate to a richer model
   tier. The response is NOT to add the specific failed input to
   the prompt as a counter-example. If a single failure can't be
   addressed without naming it in the prompt, the right remediation
   is to raise the issue with the model provider or to switch
   providers, not to bake the specific into the system contract.

## Why this works

- **The model is a reasoner, not a lookup table.** Counter-examples
  shift it toward pattern matching on the listed cases and away
  from the rule those cases were illustrating. A clear rule
  performs better than a rule + a list of "and especially not
  these specific things."

- **Prompts are read on every call.** Every concrete counter-example
  is paid for in tokens and attention budget on every successful
  call. Most calls don't need the counter-example — the call that
  produced the original failure is rare. Bloating the prompt for
  the rare case taxes the common one.

- **Code-side gates are stronger than prompt-side gates.** When the
  contract really matters (escalation tags, output schema,
  monetary parsing), enforce it in code AFTER the AI replies, not
  in the prompt BEFORE. The AI's reply is just an input to the
  service; the service is what makes the decision. Prompts coax;
  code enforces.

- **Generalization survives drift.** Models change. Prompt patches
  written against a 2026-04 model behavior may become anti-patches
  against a 2026-08 model behavior. The general rule survives;
  the specific counter-example becomes lore that no one
  remembers why we wrote.

## Application to existing prompts

A sweep of every prompt in the codebase against this ADR is a
follow-up. The acceptance criteria: each prompt describes the
contract abstractly, lists no merchant / vendor / customer / region
specifics from the user's real data, and contains no "and don't do
X like Y" patterns where Y is a verbatim string from a past failure.

The receipt-verify Tier 1 + Tier 2 prompts (commit d830f1f) were
the first prompts updated under this rule.

## Consequences

- **Bug-fix workflow changes.** Reports of "the AI did X on
  receipt Y" do not produce prompt edits citing receipt Y. They
  produce one of: a code-side gate, an escalation tier change, a
  general rule clarification, or a prompt rewrite at the contract
  level. The verbatim Y stays out of the prompt.

- **Prompts shrink over time.** As the codebase reviews existing
  prompts under this rule, accumulated counter-examples get
  removed. This is a desired outcome.

- **Code-side gates carry more weight.** ADR-0054's rule
  ("hypothesis-vendor mismatch forces vision escalation") is
  enforced in ``_ocr_extraction_quality`` regardless of what tag
  string the AI produces. The prompt asks the AI to use
  ``NEEDS_VISION``, but the gate is the comparison, not the tag.
  This is the right shape: prompt says what's polite, code says
  what's required.

- **One ADR sometimes births two.** ADR-0054 captured the
  hypothesis-as-ground-truth rule. The first draft of ADR-0054
  contained a prompt patch citing the specific synonym the model
  had emitted; that patch violated this ADR before it existed.
  Splitting the prompt-hygiene rule out as ADR-0055 is the right
  separation: ADR-0054 is about WHAT the verify cascade believes;
  ADR-0055 is about HOW the cascade tells the AI what to believe.

## Notes

This ADR was written reactively; the first commit that should have
followed it (d830f1f) shipped a violating prompt that was
immediately rolled back. Future violations should be caught at PR
review by referencing this ADR.

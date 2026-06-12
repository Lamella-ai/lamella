# Classification Philosophy

This document describes a deliberate design choice that governs how
transaction classification works in this codebase. Any agent
touching classifier code, ingest flows, scheduler configuration, or
review-queue logic should read this first to understand *why* the
system is intentionally slow and *what that implies* for their
work.

---

## The core principle

Classification is a continuing process, intended to take time.
Running the classifier early and often is the wrong default in this
system. Running it selectively, when it has the context to do a
good job, is the right default.

There are two reasons for this, and they reinforce each other.

**Reason 1: token cost.** The classifier uses LLM calls. Each call
is paid. Running 800 classifications in one sweep costs 800 calls.
If half of those could have been answered correctly with more
context acquired later, those tokens were wasted.

**Reason 2: accuracy through accumulated context.** A single
classification is not independent. Users add receipts, notes,
project tags, vehicle mileage logs, and memos over time. A receipt
can land months after the transaction it belongs to. A note might
be added when a user reviews their finances quarterly. Every piece
of context that arrives between a transaction's creation and its
classification makes the classification better. Classifying
immediately forfeits all that future context.

Worse: classifications compound. Transaction B's correct category
often depends on transaction A having already been correctly
categorized. If the system tries to classify 100 similar
transactions in parallel, each makes its decision without the
benefit of the other 99. One at a time, with user confirmation in
between. Each subsequent classification gets smarter. The first
gets resolved; the user confirms or corrects it; that confirmation
becomes context (vector-DB neighbor, pattern template, explicit
rule) for the next 99.

The operational consequence: **800 unclassified FIXMEs sitting in
the review queue is not a backlog to clear. It is a working set
waiting for context to accumulate.** Clearing it aggressively is
both expensive and wrong.

### Caveat: context only helps when it's correctly attributed

"More context = better classification" is true *when the model
correctly attributes the context to this transaction*. It is
false when the model bridges gaps with plausible stories: a
day-note about merchant X applied to a same-day txn at
unrelated merchant Y, a mileage entry for a UPS trip used to
"explain" a same-day grocery charge as fuel. Bad weighting on
rich context is **worse** than no context, because confabulation
has more raw material to work with.

The operational fix lives in the prompt template; see
`src/lamella/ai/prompts/classify_txn.j2`. The structural
rules:

1. **Two evidence tiers, separated physically in the prompt.**
   *Direct evidence* = memo on this txn, receipt linked to this
   txn, the txn's own merchant + narration. *Circumstantial
   context* = day-notes, mileage logs, active projects, similar-
   history. They have different powers.
2. **Direct evidence can establish a classification on its own.**
   Circumstantial context can only *corroborate* a hypothesis
   that direct evidence already supports. It cannot create one.
3. **Circumstantial-only confidence is capped.** Without a memo,
   receipt, or decisive merchant signal, the AI must report
   confidence ≤ 0.5, which the gate routes to review. The
   construction quality of the inference is irrelevant; what
   matters is whether the foundation exists.
4. **Absence of expected evidence counts AGAINST a hypothesis,
   not neutral.** A densely-logged vehicle missing from the txn
   date weakens the vehicle-attribution case. A project active
   on the date with no description-level match weakens the
   project case.
5. **Provenance labels at retrieval time.** Each context item is
   tagged DIRECT or CIRCUMSTANTIAL inline so the model is reading
   the rule alongside the data, not relying on memory.
6. **Zero-mile mileage entries default to RULE OUT, not opt-in.**
   A vehicle that did not move cannot have driven to a merchant.
   The prompt section gated on `mileage_entries`
   (`src/lamella/features/ai_cascade/prompts/classify_txn.j2`,
   §"Zero-mile rows are meaningful signal", lines 253 to 286) now
   instructs the model to RULE OUT the vehicle for any drive-to
   merchant (carwash, fuel pumped at a station, drive-through,
   parking, restaurants) when the closest mileage entry is 0 mi
   without explicit "service came to the vehicle" notes. This is
   the same shape as rule 4 (absence of expected evidence counts
   AGAINST), specialized for the high-confabulation case where
   "Vehicle X had a mileage log entry on date D" was previously
   read as corroboration even when the entry recorded zero miles.

The philosophy ("context accumulates, classification gets better
over time") still holds at the system level; this is about
preventing the AI from confidently misusing context the user
just provided.

---

## Operational implications

### Scheduling

The classifier infrastructure is deliberately restrained, but
"restrained" is not "off." Two distinct shapes are legitimate:

1. **Unconditional sweeps** (running the classifier across every
   pending FIXME on a fixed cadence) are NOT legitimate. That's
   the shape workstream D retired. "The queue is backing up" is
   not a justification; the queue is *supposed* to have depth
   while context accumulates.
2. **Context-gated trickle** (running the classifier on rows
   that already meet a direct-evidence bar, with a small per-run
   cap) IS legitimate. The gate is the justification: a row
   that has a linked receipt, or memo + active project, or ≥2
   classified vector neighbors providing direct anchors, is in
   the same shape as the "information-complete classes" exception
   below. Waiting longer doesn't sharpen the answer once the
   foundation exists; it just delays a classification the user
   will accept.

Concrete rules today:

- SimpleFIN ingest does NOT classify at fetch time. Rows that
  don't match a user-rule or loan claim land in staging
  (`/review/staged`) for user touch. Workstream C1 retired the
  per-fetch AI call.
- **User-triggered bulk classification** is the heavy hammer; the user decides when the ledger is in a good state to classify
  against. When it does run, it sorts FIXMEs by accumulated
  context (workstream B) so the LLM budget lands on the rows
  where classification has the inputs to answer well.
- **Scheduled context-gated trickle** runs twice a day (04:00 +
  16:00). It walks pending FIXMEs, applies the direct-evidence
  gate, and processes at most ~25 rows per run. Two sub-tiers:
  pattern-match-from-neighbors first (free, no LLM call) when ≥3 vector neighbors agree on a target with
  similarity ≥ 0.85), then AI classify only when the gate is
  met AND no pattern-match fired. Off-gate rows are left for
  the user-triggered path or for context to accumulate further.
- The per-review-item enricher has **no schedule**. Workstream D
  retired the 15-minute interval. The enricher class still exists
  but is callable only on demand. The legitimate user-facing
  trigger is the "Ask AI" button on `/review/staged` and
  `/txn/<hash>`: tier-3, one transaction at a time, when the
  user explicitly asks.
- AI audit passes are user-triggered. The calendar per-day audit
  and the cross-ledger audit are buttons, not schedules.
- FixmeScanner runs at boot (one cold-start prime so
  `/review/staged` is populated on first load) and is event-
  driven elsewhere (after `/review/staged/classify-group` writes,
  after SimpleFIN ingest commits). No interval.
- **Receipt auto-match** (`receipts/auto_match.sweep_recent`) is
  deterministic (pure scoring on amount/date/merchant against
  the Paperless index) and runs on an interval. No AI call. It
  is a different shape from classification entirely; the
  restraint above does not apply.

Do not add new always-on classifier schedules without a specific
justification tied to the *class of transaction being classified*
(loan payments, exception below) OR to a *gate that selects only
context-ripe rows* (the trickle pattern above). "The queue is
backing up" remains not a justification.

### Prioritization within the queue

Not all FIXMEs in the queue are equally ready to classify. When the
classifier does run, it should favor transactions with the most
surrounding context. Rough priority order (higher = more
context-rich = better classification outcome):

1. Has a receipt located and connected to the transaction.
2. Has a memo, note, or description directly attached.
3. Has a project note or journal entry dated the same day.
4. Has a vehicle mileage log from a vehicle used that day.
5. Has been explicitly flagged by the user for classification.

Lower-context items can wait. They're not lost; they're accumulating
the context that will make their eventual classification correct on
the first try.

### One-per-group, not all-at-once

Within a group of similar-looking transactions (same merchant, same
amount pattern, same source account), prefer classifying one, letting
the user confirm, and using that confirmation as context for the
rest. Classifying 100 similar FIXMEs in parallel wastes tokens *and*
risks 100 independent wrong answers. Classifying one, confirming,
and then pattern-matching the other 99 against that confirmation
uses 1 LLM call instead of 100 and produces better results.

### Whitelist construction: sign-aware roots and cross-entity widening

When the AI does run, the account whitelist offered to it is
*not* a flat list of every account in the ledger. It's narrowed
on two axes (txn root and entity), and the narrowing is
load-bearing for accuracy. Two recent corrections to the
narrowing:

1. **Sign-aware FIXME root override.** The pre-sign-aware ingest
   path always wrote `Expenses:{entity}:FIXME` as the placeholder,
   even for deposits. Running the AI with that placeholder against
   a deposit produced guaranteed-wrong proposals (Expenses
   subcategories for an Income-shaped row). The fix in
   `src/lamella/features/ai_cascade/classify.py`
   (`build_classify_context`, lines 298 to 317) re-derives the root
   from the FIXME-leg sign at context-build time: a NEGATIVE FIXME
   amount (= positive on the bank side = deposit / credit /
   refund) flips the root from `Expenses` to `Income`, so the
   whitelist is filtered to `Income:*` candidates (line 390,
   `valid_accounts_by_root(entries, root=fixme_root, entity=entity)`)
   and the prompt header switches to the Income branch
   (`prompts/classify_txn.j2` lines 68 to 76). The deposit-bypass
   above is the primary defense; this is the secondary defense
   for any row that slips through (e.g., a user-triggered "Ask AI"
   on a row whose deposit-shape was misread upstream).

2. **Cross-entity whitelist widening on retry.** A user who
   rejects a proposal and types an account in a *different*
   entity (card is `Personal` but user typed
   `Expenses:Acme:OfficeExpense`) is signalling that the card
   binding is wrong for this txn. The next retry pass widens the
   whitelist cross-entity so the AI can actually pick from the
   user-named entity (classify.py lines 423 to 452, gated on
   `prior_attempts_for_txn` returning at least one prior decision
   that mentions an entity ≠ the card-binding entity). Without
   this, the AI is stuck in the card's entity on retry and
   silently maps the user's hint to the closest same-entity
   account, a subtle confabulation failure mode.

The shared principle: an AI proposal that lies outside the
whitelist is suppressed by `propose_account`'s allowed-set guard
(classify.py line 229, "off-whitelist {target}, suppressing"),
so getting the whitelist wrong doesn't produce a wrong
classification; it produces no classification, which routes the
row back to the user. The cost of a too-narrow whitelist is
thrown-away tokens; the cost of a too-wrong whitelist
(`Expenses:*` shown for a deposit) is a guaranteed-wrong proposal.
Sign-aware narrowing tightens the wrong-shape case;
cross-entity widening relaxes the user-already-told-us case.

### One-click Accept is gated on confidence band

The "Ask AI" terminal panel in
`src/lamella/web/templates/partials/_ask_ai_result.html` (lines
160 to 188) only renders the Accept button when
`proposal.confidence != 'low'`. Low-confidence proposals render
a banner instead: *"the classifier recommends reviewing this
manually. Either reject & retry with more context, or pick an
account yourself below."* This is a UI consequence of the
confidence-gating rule in `prompts/classify_txn.j2` §50 to 66
(circumstantial-only confidence is capped at ≤ 0.50 = "send to
review"): if the model itself reported it can't get above the
review band, the UI must not offer a one-click write that
bypasses review. The user can still reach the same target via
"Pick the account myself" + the account-picker datalist; the
proposal's target is visible above for reference. The friction is
deliberate: Accept is a *confirmation-of-AI-judgment* affordance,
not a *fast-path-to-write* affordance.

### Non-AI paths are preferred where they apply

The classifier doesn't need to be LLM-powered every time. Three
tiers, in decreasing order of preference when they apply:

1. **Deterministic.** Some transaction classes are
   information-complete at ingest time; the system already has all
   the data it needs to split them correctly, and no amount of
   waiting will improve the answer. Loan payments are the primary
   example: the configured amortization schedule + escrow amounts
   fully determine the split of any payment that matches the
   expected amount. These can and should be classified immediately,
   without AI involvement, using domain logic.
2. **Pattern-match from prior confirmations.** After the user has
   confirmed several classifications of similar transactions, the
   system can auto-apply the established pattern to new matches,
   still without an LLM call. This is aspirational in parts of the
   codebase; where it exists, prefer it over AI.
3. **AI with accumulated context.** When no deterministic or
   pattern-match path applies, fall back to the LLM classifier,
   feeding it the context that has accumulated (receipts, notes,
   nearby vector-DB neighbors from similar past transactions).

The design intent is that AI handles only the genuinely ambiguous
cases. Everything that can be handled deterministically or by
pattern should be, because those paths are free and accurate.

### Exceptions: information-complete classes

Some transaction classes do *not* benefit from waiting. For these,
immediate classification is correct because no future context will
improve the answer. The canonical example is loan payments: a
mortgage payment is the same amount, same date, same source account
every month, and the loan's configured amortization already
contains all the context needed to split it. There is nothing to
wait for.

When you are confident a class of transaction is information-
complete at ingest time, an immediate deterministic classifier is
appropriate; it is *the exception*, not a template for relaxing
the broader restraint. Justify the exception in terms of "this
class's correct classification does not improve with additional
context," not in terms of "classifying faster is better."

### Deposits bypass AI entirely: manual-only Income classify

Deposits (money IN to the bank or credit-card account) are a class
the AI is *never* allowed to classify. The "Ask AI" worker in
`src/lamella/web/routes/api_txn.py` short-circuits the cascade
before any context-build runs (lines 754 to 913, `_is_deposit` branch
and `_render_terminal(..., ai_skip_reason="deposit")`):

- For staged rows, the deposit signal is `amount > 0` per the
  SimpleFIN sign convention (positive = money IN).
- For ledger rows, the signal is a NEGATIVE FIXME-leg amount
  (FIXME is the offset of the bank-side leg, so a negative FIXME
  means the bank side was positive (a deposit / credit / refund)).

The justification is two-sided:

1. **Income subcategory is a tax decision, not a categorisation
   decision.** The AI cannot guess whether a deposit belongs in
   `Income:{Entity}:Sales`, `:Consulting`, `:Reimbursement`, or
   `:Interest` better than the user can; the underlying ground
   truth lives in the user's bookkeeping intent, not in any
   ledger-derived signal.
2. **Bias risk is real.** Training-set patterns like "user
   usually deposits to account A" leak into other-account
   decisions in ways that are hard to detect after the fact. The
   bypass eliminates the failure mode entirely rather than trying
   to gate around it.

The terminal panel renders deposit-specific UI: the user picks an
`Income:{Entity}:*` subcategory manually, recognises the row as
half of a transfer pair, or one-click-routes against a refund
candidate (see below). This shape is parallel to the loan-payment
exception above: both are classes where "no future context will
improve the answer," but the rationale is the inverse: loan
payments are information-complete, deposits are
intent-determined-by-the-user.

#### Refund-of-expense detection runs alongside deposit-skip

When the deposit-skip terminal renders, the worker also calls
`find_refund_candidates(...)` from
`src/lamella/features/bank_sync/refund_detect.py` (api_txn.py
lines 798 to 906, refund_detect.py lines 201 to 311). The helper scores
recent expenses against the deposit by merchant similarity (+0.40),
amount proximity (within 5%: +0.30; within 20%: +0.10), date
window before the refund (+0.20), and same source account
(+0.10); threshold ≥ 0.50, top 5 by score. Each candidate is
re-routed against the *original expense's category*. Picking a
candidate stamps `lamella-refund-of: "<original-lamella-txn-id>"`
on the override block (api_txn.py lines 268 to 286, commit 836860e)
so bidirectional /txn-page lookup can walk the link in either
direction.

Refund detection is deterministic scoring, not AI; it is
shape-compatible with the receipt-auto-match exception in the
Scheduling section (a different shape from classification
entirely). The restraint above does not apply.

---

## What to do with this

When you are working on classifier, ingest, review-queue, or
scheduler code:

- **Don't speed things up by default.** If you find yourself
  reducing an interval, moving a job from user-triggered to
  scheduled, or adding a new classification trigger, stop and
  check whether the change conflicts with this philosophy.
- **Don't clear the queue as a performance goal.** Queue depth is
  not a bug. If a user asks for the queue to be shorter, the
  right answer is usually better prioritization (more context-rich
  items surfaced first) or better non-AI paths (deterministic +
  pattern-match handling more of the easy cases), not "run the AI
  more often."
- **Do add deterministic and pattern-match paths wherever they
  genuinely apply.** Every transaction handled deterministically
  or by pattern is a transaction that never needs an LLM call.
  That's the cheapest and most accurate form of classification.
- **Do preserve classifier preemption for domain-owned
  transactions.** When a domain module (loans is the current
  example) has better structured information about a transaction
  than the general classifier could derive, the domain module
  claims the transaction and the general classifier must not act
  on it. Weaker domain knowledge than the AI would be a bug worth
  fixing; stronger domain knowledge being overridden by an AI
  guess is a regression.
- **Do justify exceptions.** If you genuinely need a transaction
  class classified immediately, justify it in terms of "this class
  is information-complete at ingest and no waiting improves
  accuracy." If you can't make that case, the transaction class
  should wait.

The summary, in one sentence: classification is not a race to
empty the queue; it is a process of waiting until the context is
good enough to classify well, with deterministic and pattern-match
shortcuts for the cases where waiting buys nothing.

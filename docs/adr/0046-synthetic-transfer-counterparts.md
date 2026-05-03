# ADR-0046: Synthetic Transfer Counterparts, Replaceable Placeholders for Single-Leg Transfers

- **Status:** Accepted
- **Date:** 2026-04-27
- **Author:** AJ Quick
- **Related:** [ADR-0001](0001-ledger-as-source-of-truth.md), [ADR-0003](0003-lamella-metadata-namespace.md), [ADR-0008](0008-unconditional-dedup.md), [ADR-0019](0019-transaction-identity-use-helpers.md), [ADR-0042](0042-chart-of-accounts-standard.md)

## Context

Bank-account transfers (PayPal → Bank One Checking, Card payment
from Savings, etc.) appear as TWO separate transactions in their
respective bank feeds: one debit on the source account, one credit
on the destination. Lamella's matcher pairs these into a single
balanced Beancount transaction when both halves arrive within the
matching window:

```
2026-04-22 * "PAYPAL TRANSFER 1049791515428"
  Assets:Personal:BankOne:Checking   -840.82 USD
  Assets:Personal:PayPal                  840.82 USD
```

When ONLY ONE half is currently in staging, typical when the user
imports one bank but not the other, or when the timing window
hasn't elapsed, the row sits in /review with the "Looks like a
transfer" hint, no automatic pair, and no ability to act without
manually classifying it to a destination account.

The user can already pick `Assets:Personal:PayPal` as the target,
which produces the balanced Beancount transaction shown above,
but BOTH legs of that transaction are now "Lamella-authored." If
the user later imports the PayPal feed (or pastes a CSV), the
genuine PayPal-side row arrives in staging and the matcher has a
problem:

1. Naive dedup compares `(date, amount, account)`. The synthetic
   leg matches; the importer would skip the real one as a
   duplicate, losing the bank's authoritative source-id.
2. A more careful matcher would recognize the new row IS the same
   real-world event but can't tell whether to keep the synthetic
   leg, replace it, or merge metadata from both.
3. If the user typed the wrong destination account ("PayPal" but
   the bank deposit actually hit "Venmo"), the synthetic leg is
   silently incorrect; the real leg arriving in the wrong account
   does nothing to surface the error.

## Decision

Manually-created counterpart legs MUST be marked synthetic in the
Beancount metadata so the matcher can recognize them, prefer real
data when it arrives, and surface conflicts when the destination
account differs.

### On-disk shape

When the user picks an `Assets:<…>` or `Liabilities:<…>` target for
a transfer-suspect single-leg row, the writer emits a balanced
transaction with the counterpart-leg metadata flagging the synthetic
provenance:

```
2026-04-22 * "PAYPAL TRANSFER 1049791515428"
  lamella-txn-id: "01JN3F7Z9X..."
  lamella-source: "simplefin"
  Assets:Personal:BankOne:Checking  -840.82 USD
    lamella-source-reference-id-1: "TRN-e25e28f2-..."
  Assets:Personal:PayPal                840.82 USD
    lamella-synthetic: "user-classified-counterpart"
    lamella-synthetic-confidence: "guessed"
    lamella-synthetic-replaceable: TRUE
    lamella-synthetic-decided-at: "2026-04-27T18:32:14+00:00"
```

The `lamella-synthetic` posting-level meta is the signal. Three
companion keys carry the disposition:

- **`lamella-synthetic`**: provenance tag. Always present on a
  synthetic leg. Values: `"user-classified-counterpart"` (this ADR's
  case), `"matcher-inferred"` (future: the matcher creates a
  synthetic when it has high enough signal), `"reconstruct-derived"`
  (recovery-system rebuild that couldn't find the real other side).
- **`lamella-synthetic-confidence`**: `"guessed"`, `"likely"`,
  `"confirmed"`. The user-classified case starts as `"guessed"` and
  upgrades to `"confirmed"` when the real other side arrives and
  matches the destination account.
- **`lamella-synthetic-replaceable`**: boolean. `TRUE` means a
  matching real import SHOULD replace this leg. `FALSE` means the
  user has manually confirmed this is the correct counterpart (e.g.
  cash withdrawal from an ATM has no second-bank import to look for).

### Replacement protocol

When a new staged row arrives whose `(date, amount, account)`
matches an existing synthetic leg with `lamella-synthetic-replaceable:
TRUE`, the matcher routes the row through the **synthetic-replacement
path**, not the dedup-skip path:

1. **Same destination account, opposite sign, within 5-day window**:
   the real row is the genuine other half. Replace the synthetic
   leg in place. The meta keys flip from synthetic to real
   (`lamella-source: <new-source>`,
   `lamella-source-reference-id: <new-id>`), the synthetic provenance
   keys are stripped. The original transaction's `lamella-txn-id`
   stays stable. Audit log records the replacement.

2. **Different destination account, opposite sign, within 5-day
   window**: the user picked the wrong destination. The real row
   surfaces in /review with a special "We thought this was a
   transfer to `<wrongAccount>`, it's actually `<correctAccount>`"
   prompt. The user confirms; on confirmation, the synthetic leg
   is rewritten to the correct account.

3. **Same/similar amount + date but different sign or far outside
   window**: the matcher does NOT auto-replace. The new row goes
   through normal dedup. The synthetic leg stays. The user is
   surfaced an "AI suggests these may be related, confirm or
   dismiss" hint.

The replacement uses an in-place rewrite (per ADR-0002, in-place
.bean rewrites are the default) with bean-check + revert-on-fail
discipline.

### UI surface: single-leg transfer assignment

When the user is on /review and a row (or group) is flagged
transfer-suspect, the classify form for that row gets a
**"Create transfer to this account"** affordance instead of (or
alongside) the standard expense-classify input:

- The picker pre-fills with `Assets:<entity>:` / `Liabilities:<entity>:`
- A clear caption: "This will write a balanced transfer to that
  account, marked synthetic so a future import can replace it"
- A confirm prompt before submit: "We're about to create a
  Lamella-authored transfer leg in `<account>`. If you later import
  that account, Lamella will detect the real other half and
  replace this. Continue?"
- Per the ADR-0045 path-segment rule, the chosen account validates
  before the write fires.

The escape hatch, "No, classify as an expense instead", flips
the picker back to the standard `Expenses:<entity>:` shape and
removes the synthetic-meta-emit logic from the writer's call chain.

### Reconstruct + audit

Per ADR-0015, the reconstruct capability rebuilds SQLite from the
ledger alone. The synthetic meta keys are first-class structural
data, not derived state. They survive reconstruct. The audit log
gets one entry per synthetic-create and per synthetic-replace so
the user can see "which Lamella-authored counterparts are still
synthetic, which got confirmed by real imports."

A reporting view in /audit (or /reports) surfaces the count of
**synthetic-still-replaceable** legs per account so the user knows
how much of their ledger is "Lamella's best guess" vs. confirmed
bank data.

### Idempotency + ADR-0008 dedup

The standard SimpleFIN-id dedup (ADR-0008) operates on the
bank-source-side leg's `lamella-source-reference-id`. The synthetic
leg has NO `lamella-source-reference-id` (it has no bank source by
definition), so it never collides with an incoming real row through
the dedup path. The replacement protocol above is the ONLY path
that reasons about synthetic legs.

## Consequences

### Positive

- Single-leg transfers become actionable on /review without losing
  the ability to incorporate real bank data later. The user
  isn't choosing between "leave it sitting forever" and "commit
  to a guess that may be wrong."
- Audit trail is honest. The `lamella-synthetic-confidence: guessed`
  signal makes "Lamella made this up" explicit in the ledger,
  surface-able on reports, and reversible.
- The dedup protocol stays simple. Synthetic legs are out-of-band
  for the standard `(source, source-reference-id)` check.
- Reconstruct (ADR-0015) doesn't need a special case; the synthetic
  meta keys are part of the on-disk truth.

### Negative / Costs

- The matcher gains a third path (replace) on top of dedup-skip
  and pair-into-existing. More logic to maintain, more test
  coverage required.
- The user has a new mental model: "this leg is a guess until real
  data arrives." Most users won't think about it; the UI must
  carry that meaning without requiring conscious tracking.
- A user who creates a synthetic leg, manually confirms it, then
  imports the real other half from a different account is in a
  conflict-resolution flow we have to handle gracefully (UI
  prompt, see protocol step 2 above).

### Mitigations

- The `lamella-synthetic-confidence: confirmed` upgrade path means
  a user who manually verifies a synthetic leg (e.g., they
  reconcile their PayPal statement and confirm the leg is correct)
  flips it out of replaceable status. Future imports that match
  it then go through normal dedup, not replacement.
- The /review and /audit surfaces show "N synthetic legs still
  replaceable" so the user can see what they've authored.
- The replacement audit log is reversible. If a replacement
  turns out to be wrong (e.g., the matcher matched the wrong real
  row), the user can revert through the same flow that created
  the original synthetic.

## Compliance

- The writer that creates a synthetic counterpart MUST emit all
  three meta keys (`lamella-synthetic`,
  `lamella-synthetic-confidence`, `lamella-synthetic-replaceable`).
  Missing any of them is a writer bug; CI test asserts the shape.
- The matcher's dedup path MUST skip synthetic legs (no
  source-reference-id on them; the standard dedup query naturally
  excludes them; assert this with a test fixture).
- The replacement protocol's three branches (same-account,
  different-account, no-match) are each covered by a test fixture.
- Reconstruct correctly rebuilds the synthetic flags from the on-disk
  meta. CI test reconstructs from a fixture ledger that contains
  synthetic legs and confirms the SQLite state matches.

## Implementation phases

This ADR is the architectural decision; the implementation lands
incrementally:

1. **Phase 1**: Writer emits the three meta keys when the user
   picks an `Assets:`/`Liabilities:` target on a transfer-suspect
   row. No matcher changes; the leg is just marked. (Gives us
   the audit trail and surfaces the count in /audit.)
2. **Phase 2**: Matcher's incoming-row path checks for a matching
   synthetic leg before dedup. Same-account match → replace in
   place. (Closes the duplicate-import bug.)
3. **Phase 3**: Different-account match → /review prompts the user.
   Manual confirmation → rewrite. (Catches the wrong-account
   guess.)
4. **Phase 4**: /audit shows the synthetic-leg count by account
   with a "promote to confirmed" button per leg for the user
   who wants to lock something in manually.

Each phase is a single feature blueprint update + corresponding
implementation commit. Phase 1 is the unblocking deliverable.

## References

- [ADR-0001](0001-ledger-as-source-of-truth.md): ledger is the source of truth. The synthetic flags live in the ledger; reconstruct rebuilds SQLite from them.
- [ADR-0003](0003-lamella-metadata-namespace.md): `lamella-*` metadata namespace. The synthetic keys follow the same convention.
- [ADR-0008](0008-unconditional-dedup.md): unconditional dedup by `(source, source-reference-id)`. Synthetic legs have no source-reference-id, so they don't trip the standard dedup; the replacement protocol is the only path that reasons about them.
- [ADR-0019](0019-transaction-identity-use-helpers.md): txn identity uses helpers. The replacement protocol uses `lamella-txn-id` to anchor the original transaction's identity across the synthetic-to-real swap.
- [ADR-0042](0042-chart-of-accounts-standard.md): chart of accounts standard. The Assets/Liabilities target accounts validated against the same rules as a normal classify.
- `docs/features/bank-sync.md`: feature blueprint; will be updated to reflect this ADR's matcher changes once Phase 2 lands.

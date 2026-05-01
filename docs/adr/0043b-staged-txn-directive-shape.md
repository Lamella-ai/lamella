# ADR-0043b: Staged-Txn Directive Shape, Frozen Spec for v0.3.1 Migration

- **Status:** Accepted
- **Date:** 2026-04-29
- **Related:** [ADR-0043](0043-no-fixme-in-ledger.md), [ADR-0001](0001-ledger-as-source-of-truth.md), [ADR-0003](0003-lamella-metadata-namespace.md), [ADR-0015](0015-reconstruct-capability-invariant.md), [ADR-0019](0019-transaction-identity-use-helpers.md), [ADR-0022](0022-money-is-decimal.md), [ADR-0023](0023-datetimes-tz-aware-utc.md)

## Context

ADR-0043 ("Unclassified bank data is staged via `custom` directives, not
FIXME postings") froze the high-level shape but left five concrete
gaps: (1) where `lamella-txn-id` lives in the directive, (2) the
exhaustive set of `lamella-source` values, (3) the sign convention for
`lamella-txn-amount`, (4) which supplemental fields land on
`staged-txn-promoted`, and (5) the format and timing of the
balance-anchor directive. The migration plan at
`docs/internal/plans/0043-staged-txn-migration.md` Section 2 enumerates
each gap. The migration itself (24 to 35h, see plan §8) is deferred to
v0.3.1, but the v0.3.1 implementation team needs an unambiguous spec
*before* the public v0.3.0 release goes live so the directive shape is
stable on day one. This ADR resolves the five gaps by explicit decision
and freezes the directive grammar. Code changes that emit or read
`custom "staged-txn"` directives in v0.3.1 must conform to this spec.

## Decision 1: `lamella-txn-id` placement

**Decision:** `lamella-txn-id` is **REQUIRED** on every
`custom "staged-txn"` directive at directive-level meta. It is minted
once at staging time (UUIDv7 per ADR-0019) and carried verbatim onto
the promoted balanced transaction at promotion time, never
regenerated.

```
YYYY-MM-DD custom "staged-txn" "<source>"
  lamella-txn-id: "<uuidv7>"
  lamella-source: "<source>"
  lamella-source-reference-id: "<id>"
  ...
```

The `"<source>"` positional argument duplicates the `lamella-source`
meta value verbatim. This is intentional. Beancount's stdlib `pad`
plugin (auto-loaded by `loader.load_file`) walks every Custom
directive's `values` field and crashes with `TypeError: 'NoneType'
object is not iterable` when a custom directive has no positional
args. Existing lamella custom directives (`loan-pause "<slug>"`,
`lamella-ledger-version "2"`) already follow this pattern. The
positional arg makes the source value immediately visible in
`grep 'custom "staged-txn"'` searches without parsing meta blocks.

**Rationale.** ADR-0019 mandates that every internal subsystem keys off
`lamella-txn-id` (lineage), not `txn_hash` or per-source ids.
`/txn/{token}` URL stability (see `docs/specs/NORMALIZE_TXN_IDENTITY.md`
"Transaction-level: lamella-txn-id") requires the lineage id to exist
before the user ever sees the row, i.e. while it is still a
`staged-txn` directive, so the bookmarkable URL doesn't change at
promotion. AI decisions logged at staging time use the same lineage as
their `input_ref`, so the staging-time → promotion bridge is a single
column lookup. Making the field optional would re-introduce the
SimpleFIN-PK-as-bridge anti-pattern that NORMALIZE_TXN_IDENTITY.md
Phase 7c eliminated for the importer.

## Decision 2: Multi-source directive values

**Decision:** `lamella-source` is **REQUIRED** and is a **closed enum**
governed by `lamella.core.identity.SourceName`. Valid values for v0.3.1
staging:

- `"simplefin"`: SimpleFIN bridge ingest
- `"csv"`: spreadsheet importer (subsumes the migration plan's `"import"`)
- `"paste"`: pasted tabular data
- `"reboot"`: entries surfaced by the reboot scan path

**Implementation note (2026-04-29 amendment):** the original draft used
the value `"reboot-scan"`; aligned to `"reboot"` to match the existing
`staged_transactions.source` column convention. The DB column predates
this ADR; introducing a new value would force a migration and break
P4 reconstruction round-trip. The four source values above are the
exact byte-for-byte values the staging table already writes.

`"manual"` from `SourceName` is **NOT** a valid staging source. Manual
entries skip the staging table and go straight to a balanced txn.

**Unknown values:** **REJECT AT WRITE TIME.** The writer raises
`InvalidSourceError` before any file write, in the same hook that
already blocks FIXME account paths per ADR-0043 Compliance. Read-side
behaviour is permissive: an unknown `lamella-source` value on disk
(hand-edited, future-version content) is logged as a warning and the
directive is surfaced to the user as "Pending classification (unknown
source)", never silently dropped. This matches the ADR-0003 stance:
preserve user-authored content wholesale, but writers emit only what
the spec allows.

**Rationale.** The closed-enum pattern is already in use for posting
provenance (see NORMALIZE_TXN_IDENTITY.md "Allowed source names"); the
staging directive uses the same vocabulary so the post-promotion
posting-level `lamella-source-N` value matches the directive's
`lamella-source` value byte-for-byte. The plan's `"import"` was a
naming inconsistency. `"csv"` is the canonical name.

## Decision 3: Sign convention for `lamella-txn-amount`

**Decision:** **Match `PendingEntry.amount` (signed from
source-account POV; negative = money leaving the source account).**
The directive carries a single signed amount; no separate `direction`
field.

```
  lamella-txn-amount: -42.17 USD
```

`Decimal` per ADR-0022; bare beancount amount syntax (number +
commodity, unquoted) per LEDGER_LAYOUT.md §6.3.

**Rationale.** `PendingEntry.amount` (`writer.py:50`) is signed from
the source-account's POV per the SimpleFIN bridge contract, `negative
= debit`. Every existing reader, every test fixture, and every dedup
path in `bank_sync/` already treats this convention as ground truth.
Introducing a separate `lamella-txn-direction` field would force every
ingest call site to translate (signed → magnitude+direction at write,
magnitude+direction → signed at read), doubling the surface area for
sign-flip bugs. The single-amount approach also keeps the directive's
amount line drop-in compatible with the post-promotion balanced txn's
source-leg amount (Decision 4 promotes the directive amount onto the
source posting verbatim).

The migration plan recommends option (a); this ADR adopts it.

## Decision 4: `staged-txn-promoted` supplemental fields

**Decision:** When a `custom "staged-txn"` directive is replaced with
`custom "staged-txn-promoted"` (in the same atomic write that appends
the balanced txn), the promoted directive carries every original field
PLUS the following supplemental meta:

| Key | Required | Type | Notes |
|---|---|---|---|
| `lamella-promoted-at` | **REQUIRED** | string (ISO-8601 TZ-aware UTC, seconds precision) | Per ADR-0023. Format: `"2026-04-29T14:23:07+00:00"` |
| `lamella-promoted-by` | **REQUIRED** | string enum: `"rule"`, `"ai"`, `"manual"` | Single value; closed enum |
| `lamella-promoted-rule-id` | **OPTIONAL** | string (rule id) | Present iff `lamella-promoted-by == "rule"`; absent otherwise |
| `lamella-promoted-ai-model` | **OPTIONAL** | string (model identifier, e.g. `"claude-opus-4-7"`) | Present iff `lamella-promoted-by == "ai"`; absent otherwise |

The original directive fields (`lamella-txn-id`, `lamella-source`,
`lamella-source-reference-id`, `lamella-txn-date`,
`lamella-txn-amount`, `lamella-source-account`,
`lamella-txn-narration`) are preserved verbatim on the promoted
directive. The audit trail must let a future reader reconstruct
"what the staged row looked like" without consulting the balanced txn.

**Rationale.** The promoted directive is an audit anchor: it tells a
future reader "row X was promoted at time T by mechanism M". The
required pair (`-at`, `-by`) is the minimum non-redundant audit
information; the conditional fields (`-rule-id`, `-ai-model`) carry
the *which* rule or model only when applicable. Making `-rule-id` and
`-ai-model` conditional rather than always-present matches ADR-0019's
stance: optional metadata reflects optional state, not mandatory
nullable fields. `lamella-promoted-by` deliberately omits a `"recovery"`
or `"reconstruct"` value. Those code paths must use `"manual"` or
`"rule"` whichever applies, since reconstruct never *promotes* on its
own (it only reads).

## Decision 5: Balance-anchor directive format and timing

**Decision:**

- **Format:** vanilla beancount `balance` directive:
  `YYYY-MM-DD balance <account> <amount> <currency>`. **No `lamella-*`
  meta on the directive.** It is a real beancount assertion that
  `bean-check` enforces.

- **Timing:** **one per `(source_account, ingest_run)`**, emitted at
  the END of an ingest run after all `staged-txn` directives for that
  run have been written, NOT per-directive. Per-directive balance
  assertions would force `bean-check` to evaluate N assertions on a
  ledger that already requires the N-1 directives to be in
  arithmetic-correct order, which is not a property the writer can
  guarantee for re-delivered or reordered SimpleFIN events.

- **Failure mode:** when a balance assertion fails at promotion time,
  the **directive is treated as corrupt → log a structured warning
  + skip + flag for user review**, NOT throw. The user sees the
  affected row in the recovery UI's "balance-assertion-failed" finding
  category; the rest of the promotion proceeds. Throwing on a balance
  assertion mismatch would block all subsequent promotions on a single
  bad row, which violates ADR-0036 (every action acknowledges within
  100ms; promotion of unrelated rows must not be held hostage by one
  arithmetic gap).

**Rationale.** Per ADR-0043 "Balance anchoring", the arithmetic
difference between bank-true balance and ledger balance equals the
total unclassified work for that account at that date. A vanilla
`balance` directive is the only way to make `bean-check` actually
catch data loss. `custom` directives are invisible to bean-check.
Per-account-per-ingest-run timing matches the natural transaction
boundary of the ingest cycle: each SimpleFIN poll for a single account
is one atomic batch with one anchoring assertion at the tail.
LEDGER_LAYOUT.md §2.1 already restricts writes to connector-owned
files; the balance directive lands in
`simplefin_transactions.bean` (or the appropriate connector-owned
file per source) at the same lock acquisition that wrote the staging
directives.

## Consequences

### Positive
- v0.3.1 implementation team has a single source of truth for directive
  grammar; no schema-change-after-real-data risk.
- Closed enum on `lamella-source` keeps post-promotion posting-level
  source meta byte-identical to the staging-time directive value.
- `lamella-txn-id` lineage survives every transition (staging →
  promoted directive → balanced txn). `/txn/{token}` URLs are stable
  before, during, and after promotion.
- Audit trail is rich enough to answer "who promoted this and when"
  without crossing module boundaries.

### Negative / Costs
- Balance-anchor failure-mode-as-skip means a corrupt anchoring
  directive can silently leave the user's "Pending classification"
  total slightly wrong until they review the recovery finding. The
  finding category is the mitigation. The failure is surfaced, just
  not fatal.
- Closed `lamella-source` enum means adding a new ingest path requires
  an `identity.py::SourceName` enum bump. By design (see
  NORMALIZE_TXN_IDENTITY.md "Source-name enum extensibility").

### Mitigations
- Pre-write hook validates `lamella-source` against `SourceName` enum;
  raises `InvalidSourceError` on mismatch.
- Reconstruct test (ADR-0015) asserts every promoted directive has the
  required `-at` + `-by` pair and that conditional fields match their
  triggering enum value.
- Recovery finding category for balance-assertion failure is
  highest-priority within its class; surfaces in the
  `Pending classification` total breakdown.

## Reference

- Migration plan: [docs/internal/plans/0043-staged-txn-migration.md](../internal/plans/0043-staged-txn-migration.md) §2
- Parent ADR: [ADR-0043](0043-no-fixme-in-ledger.md)

## Required reading before changes

- [ADR-0043](0043-no-fixme-in-ledger.md): parent ADR; this spec freezes the directive shape it left open
- [ADR-0001](0001-ledger-as-source-of-truth.md): all state must reconstruct from the ledger alone
- [ADR-0003](0003-lamella-metadata-namespace.md): `lamella-*` namespace rules; closed enum for source values
- [ADR-0015](0015-reconstruct-capability-invariant.md): staged-txn directives must round-trip to `staged_transactions` rows
- [ADR-0019](0019-transaction-identity-use-helpers.md): `lamella-txn-id` lineage discipline; never regenerate, never bypass `identity.py`
- [ADR-0022](0022-money-is-decimal.md): amount is `Decimal`, never `float`
- [ADR-0023](0023-datetimes-tz-aware-utc.md): `lamella-promoted-at` is TZ-aware UTC ISO-8601
- `docs/specs/LEDGER_LAYOUT.md` §6: metadata namespace contract
- `docs/specs/NORMALIZE_TXN_IDENTITY.md`: `SourceName` enum and lineage rules

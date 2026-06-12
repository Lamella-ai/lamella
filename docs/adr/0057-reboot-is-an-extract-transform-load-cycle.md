# ADR-0057: Reboot is a round-trip ETL — extract, transform, re-emit, validate

> **Active follow-ups + test plan:**
> [`docs/proposals/TODO-2026-04-29-multi-source-dedup-followups.md`](../proposals/TODO-2026-04-29-multi-source-dedup-followups.md)

**Status:** Accepted

**Date:** 2026-04-29

## Context

Lamella's intended audience includes users who arrive with
hand-written or foreign-tool Beancount ledgers. Those files are
authoritative as input — they carry the user's history — but they
were not written against Lamella's chart-of-accounts standard
(ADR-0007), metadata namespace (ADR-0003), or transaction-identity
contract (ADR-0019). Before Lamella can operate on them
(classification, matching, reports, dashboards), the ledger has to
be normalized: account paths in the canonical entity-first shape,
every transaction stamped with `lamella-txn-id`, source-side
postings carrying paired `lamella-source-N` /
`lamella-source-reference-id-N` keys, etc.

The reboot flow is the user-facing onboarding path for that case.
Its purpose is **a round-trip ETL that doubles as validation**:

1. **Extract:** parse every transaction in the user's ledger into
   the database in fully-typed form (txn-level meta, every posting
   with account / signed amount / currency / cost / price /
   posting-level meta, tags, links, flag).
2. **Transform:** apply normalizations the staging layer is
   responsible for — account-path migration to the canonical
   shape, mint a `lamella-txn-id` for any entry missing one,
   migrate retired identifier keys to paired source meta, dedup
   collisions across intake paths, etc. Pure SQLite work.
3. **Re-emit:** serialize each staged transaction back to
   Beancount syntax, preserving every captured key and value in
   its canonical type (LEDGER_LAYOUT.md §6.3).
4. **Validate:** bean-check the re-emitted ledger against the
   pre-extract baseline. If it parses clean and the only diffs are
   normalization-driven, the user's data is structurally
   compatible with Lamella; the new ledger replaces the original
   (with `.pre-reboot-<timestamp>/` snapshot for rollback). If
   bean-check rejects the output OR a transaction couldn't
   round-trip (metadata loss, malformed posting that the parser
   accepted but the serializer can't reproduce), that's the
   signal the user's original entry has a structural problem
   they need to address — surface it with the file + line + the
   offending entry, leave the original ledger untouched.

This is the convergence test from NEXTGEN.md Phase E: a clean
ledger round-trips byte-identical (modulo whitespace
normalization); a messy one produces a diff the user reviews and
applies, OR a list of failures pointing at exactly which entries
need manual fixes.

## Why this works in spite of `.bean` being authoritative

ADR-0001 makes Beancount files the source of truth. The reboot
round trip doesn't violate that — it strengthens it:

* The original ledger is snapshotted to
  `.pre-reboot-<timestamp>/` before any write. The pre-extract
  authoritative state is recoverable byte-for-byte.
* `bean-check` runs against the re-emitted output before commit.
  Any new error vs. the baseline triggers a full rollback. If
  the new ledger doesn't parse, the original stays as-is.
* `txn_hash` (Beancount content hash over date / narration /
  postings) is preserved across the round trip when the
  transformations are non-content (metadata-only, account-path
  normalization on user-authored unparseable shapes).
  Receipt links keyed by `txn_hash` continue to resolve. When a
  transformation does change content (e.g., a fresh
  `lamella-txn-id` mint), the link writeback compares old/new
  hashes during apply and rewrites `receipt_links.txn_hash`
  rows so the join survives.
* Per-transaction round-trip is provable: parse a Beancount
  transaction, serialize, parse again — the second parse must
  produce a bytewise-equivalent in-memory entry. Failures here
  are surfaced inline as "this entry can't round-trip cleanly,"
  pointing at the exact file:line and the unsupported feature.

The user-visible end state matches the intent the user described:
"capture the patterns, try to validate, write back. If it can't,
those original entries had issues."

## What was wrong before

The implementation that shipped under "Phase E" diverges from this:

1. **Phase E1 ingest is non-destructive.** It stages every txn
   into `staged_transactions` but never re-emits. The original
   ledger entries stay in place; the DB rows are a parallel
   copy, not a working set. Users see "every transaction got
   duplicated" because the review queue surfaces every
   `status='new'` row and the live ledger entry simultaneously.

2. **Phase E2b file-side has no real cleaner.** `RebootWriter`
   uses `noop_cleaner`, so prepare/apply produces output ≡ input.
   There is no validation-by-round-trip happening; the file-side
   pipeline is plumbing waiting for a transformer.

3. **Metadata isn't captured in a round-trip-safe form.** The
   staging row's `raw_json` carries `entry.meta` as
   string-coerced values (`{k: str(v) for k, v in raw_meta}`),
   which loses type information (boolean vs. string-`"TRUE"`,
   date vs. string, decimal vs. string). A re-emit from this
   storage shape can't honor LEDGER_LAYOUT.md §6.3 type rules.

4. **Posting-level metadata (`lamella-source-N` and friends)
   isn't captured at all.** The `raw["postings"]` array stores
   account / amount / currency only. ADR-0019's paired source
   meta on the bank-side posting would be silently dropped on
   any future re-emit.

5. **No round-trip property test.** Without
   `extract(parse(serialize(extract(x)))) == extract(x)` as a
   gate, a regression in the serializer can ship a metadata-loss
   bug that surfaces only when a real user runs reboot.

## Decision

The reboot flow is a round-trip ETL with validation as its
primary user value. The implementation has to honor this with
five guarantees:

### 1. Extract captures everything in a round-trip-safe form

`staged_transactions.raw_json` carries a typed envelope. Each
metadata value records its Beancount type so the serializer can
emit it correctly:

```json
{
  "flag": "*",
  "tags": ["lamella-override", "tax"],
  "links": ["..."],
  "txn_meta": [
    {"key": "lamella-txn-id", "type": "string",
     "value": "0190f000-0000-7000-8000-000000000001"},
    {"key": "tax-year", "type": "integer", "value": 2025},
    {"key": "reimbursable", "type": "boolean", "value": true},
    {"key": "purchase-date", "type": "date",
     "value": "2025-06-15"}
  ],
  "postings": [
    {
      "account": "Liabilities:Acme:BankOne:Card",
      "amount": "-42.17", "currency": "USD",
      "cost": null, "price": null,
      "meta": [
        {"key": "lamella-source-0", "type": "string",
         "value": "simplefin"},
        {"key": "lamella-source-reference-id-0",
         "type": "string", "value": "TRN-..."}
      ]
    },
    ...
  ]
}
```

`filename` and `lineno` go into `source_ref` (already do).

### 2. Transform layer is composable, pluggable, and pure

A `RebootCleaner` is a `(staged_row) -> staged_row | DropDecision`
function. The pipeline composes them:

```
account_path_normalize
  -> lamella_txn_id_mint
  -> retired_meta_key_migrate
  -> dedup_collide_groups
  -> ...
```

Each cleaner's pre/post values are recorded so the per-file diff
the UI shows the user is faithful. A cleaner that wants to drop
a row (e.g., dedup picks a winner) returns `DropDecision` with a
rationale; the row's status flips to `'dismissed'` with that
rationale logged.

### 3. Re-emit preserves every captured value in canonical form

The serializer takes a typed envelope and produces Beancount
syntax following LEDGER_LAYOUT.md §6.3:

* booleans → bare `TRUE` / `FALSE`
* dates → bare `YYYY-MM-DD`
* amounts → bare `<n> <ccy>`
* strings → double-quoted
* numbers → bare

Posting-level meta is indented one level deeper than the
posting itself. Tags / links are appended to the header line.

### 4. Validation is the apply-gate

Per-transaction round-trip property:
`serialize(parse(serialize(parse(text)))) == serialize(parse(text))`.
The apply pipeline runs this property against every captured
entry; failures are reported with file:line and a labeled cause
("posting `cost` is a complex Beancount feature the
serializer doesn't yet handle"). The user sees a "these entries
couldn't be re-emitted cleanly" list and can either: (a) edit
those entries in their original files, (b) skip them (kept as-is
in the new ledger), or (c) abort the reboot apply.

After per-entry validation, the file-level bean-check runs
against the re-emitted ledger as a whole; new errors vs.
baseline trigger a full rollback to the
`.pre-reboot-<timestamp>/` snapshot.

### 5. Receipt-link / override / source-ref preservation

Receipt links and override blocks are keyed by `txn_hash`. When a
cleaner changes content in a way that mutates `txn_hash` (rare —
account-path normalization in non-FIXME postings doesn't touch
hash inputs; metadata changes never touch hash inputs), the apply
phase computes both old and new hashes per entry and rewrites
`receipt_links.txn_hash`, override `lamella-override-of`, and
`connector_links.bean` references atomically. A change that would
orphan a link is flagged and held for user review before commit.

## Disabled by default until guarantees are proven

`Settings.reboot_ingest_enabled = False` until:

* The typed-envelope extract is implemented and round-trip tested
  against a corpus of real-world Beancount files (hand-written +
  generated).
* At least one real `RebootCleaner` (account-path normalization)
  is wired up and produces a non-trivial cleaned output for a
  messy fixture.
* The per-entry round-trip property test is in CI.
* `connector_links.bean` rewriting on a hash-changing transform
  is exercised by a test.

While disabled, the routes return 503 with a clear explanation;
the data-integrity page surfaces a destructive-warning banner;
the recovery action lets users undo the duplicated-classified-rows
symptom from earlier broken versions.

## Consequences

* The user's mental model is the design: "import the data,
  validate by re-emitting, surface what didn't round-trip." The
  flow is fundamentally a validator that produces a normalized
  canonical ledger.
* `.bean` files stay authoritative — the snapshot guarantees the
  pre-extract state is byte-recoverable, and a failed apply
  rolls back automatically.
* Round-trip property is a gate, not a hope. Regressions in the
  serializer can't ship without breaking CI.
* Failures are productized: the user sees exactly which entries
  in their original ledger don't fit the structure, with
  file:line and an actionable cause label, instead of a silent
  data-loss bug.
* Receipt links, override blocks, and source-ref metadata
  survive the round trip even when content changes mutate
  `txn_hash`, because the apply phase rewrites the join columns
  atomically.

## Compliance

Already landed:

* `Settings.reboot_ingest_enabled` (default False) — kill switch
  enforced on every destructive route.
* Recovery: `/settings/data-integrity/purge-reboot-orphans`.
* Cross-source dedup-group filter (c1e6d61).
* Web-facing scan skips already-classified entries
  (`include_classified=False` from the route; programmatic callers
  default `True`).

Required follow-ups before re-enabling:

* [x] Replace the string-coerced metadata capture in
  `RebootService.scan_ledger` with the typed envelope above.
  *Landed 2026-04-29 — `_typed_meta_value` / `_typed_meta_list` /
  `_capture_amount` / `_capture_cost` in `reboot.py`.*
* [x] Add `posting.meta` capture to `raw_json["postings"]`.
  *Landed 2026-04-29 in the same pass — every posting now records
  `cost`, `price`, `flag`, and a typed `meta` list. Paired
  `lamella-source-N` / `lamella-source-reference-id-N` keys
  (ADR-0019) survive the round trip.*
* [x] Implement a real `account_path_normalize` cleaner against
  `CHART_OF_ACCOUNTS.md` / ADR-0007.
  *Landed 2026-04-29 — `staging/cleaners.py` provides the
  `RebootCleaner` protocol (CleanedEnvelope / DropDecision /
  compose) and the first concrete cleaner. Rewrites legacy
  category-first paths like ``Expenses:Vehicles:Acme:Fuel`` →
  ``Expenses:Acme:Vehicles:Fuel`` when a known entity slug is at
  position 3. Composable; DropDecision short-circuits.*
* [x] Implement the typed-envelope serializer with a property test
  (`serialize(parse(serialize(parse(text)))) == serialize(parse(text))`).
  *Landed 2026-04-29 — `staging/envelope_serializer.py`. Honors
  LEDGER_LAYOUT.md §6.3 type rules. `tests/
  test_envelope_serializer_round_trip.py` verifies the property
  on clean expense entries + 2-leg transfers with paired source
  meta on each leg.*
* [ ] Implement the hash-rewrite atomicity for receipt-link /
  override preservation when content changes.
* [ ] Write the round-trip CI test against a fixture corpus that
  includes both clean and intentionally-broken Beancount entries.

Tests landed against the typed-envelope contract — `tests/
test_staging_reboot_typed_envelope.py` covers boolean/decimal/date/
string capture + tags/links/flag + posting.cost + paired-source-meta
preservation. Internal parser keys (`filename`/`lineno`/dunder) are
filtered out of the envelope (they're carried as `source_ref`).

Each of those follow-ups updates this ADR with a checkbox.

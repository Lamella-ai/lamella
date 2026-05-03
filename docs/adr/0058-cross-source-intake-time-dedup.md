# ADR-0058: Cross-source intake-time deduplication

> **Active follow-ups + test plan:**
> [`docs/proposals/TODO-2026-04-29-multi-source-dedup-followups.md`](../proposals/TODO-2026-04-29-multi-source-dedup-followups.md)

**Status:** Accepted

**Date:** 2026-04-29

## Context

Lamella supports multiple intake paths to the unified staging
surface, and the architecture treats them as peers — none privileged:

* Bank-feed sources (e.g. the SimpleFIN bridge today; Plaid or any
  other live-feed adapter tomorrow).
* Reboot scan (existing Beancount entries → staged_transactions).
* Tabular imports — CSV / OFX / QIF / IIF / ODS / XLSX.
* Paste (CSV-like clipboard text).
* Any new source registered through `service.SOURCES`.

Each one writes rows to `staged_transactions`. None of them, before
this ADR, queried each other or the live ledger before deciding "is
this a brand new event or a duplicate of something already known."
The result is the scenario the user surfaced (2026-04-29):

> *"You have a perfect hand-written ledger. You bring it over and
> connect a bank-feed source. How does it know those rows are brand
> new? How does it identify duplicates? What if I import via CSV
> first, then a bank feed? Or bank feed first, then CSV?"*

The primitives for cross-source dedup already exist:

* `content_fingerprint(date, abs(amount), normalized_description)`
  in `import_/staging/intake.py` — source-agnostic by design.
* `_find_duplicate_groups` in `RebootService` — runs the fingerprint
  collision check across the entire staging surface.
* `intake.detect_paste_duplicates` — runs it for incoming pasted
  rows against staged history.
* `bank_sync.duplicates` raw-text scanner — catches re-delivered
  SimpleFIN events with new `txn_id`s by date+amount+merchant.

What was missing was a single oracle that **every** staging path
calls **at the moment a row is about to be inserted**, and a status
the row lands in when it looks like a known event.

## What was wrong before

* The bank-feed source's ingest checked for in-source-id collisions
  and content-matched against its own owned `.bean` file, but did
  NOT check against `staged_transactions` from CSV / paste, and did
  NOT check against ledger entries written by foreign tools or
  hand authoring. Same shape applies to every other source — each
  one was siloed against its own history.
* Reboot can produce cross-source false-positive duplicate groups
  (`_find_duplicate_groups` filters them), but only after the row
  is staged. The user has to run a scan and review results.
* CSV / OFX / QIF / paste paths had no built-in cross-source
  collision check at intake time.
* The "staged-then-detected" model means duplicates show up on the
  review queue alongside legitimate work, requiring active sweeping.
  Users with imperfect import sequencing paid the cost of mental
  triage forever.

## What "duplicate" actually means in a multi-source world

A single real-world transaction can be observed by multiple source
records. Concretely:

> Hand-written ledger has a transfer from `Assets:Checking` to
> `Assets:PayPal` for $50. Both legs in one entry. Then the user
> connects a bank-feed source — its ingest pulls the Checking-side
> debit. Then the user imports a payment-processor CSV — it carries
> the PayPal-side credit. Four source records (ledger entry's two
> postings, bank feed's debit row, CSV's credit row); two
> real-world legs; one transaction.

The framing "this row is a duplicate" is too coarse for that world.
The accurate framing is: **this row is one of N source observations
of the same event-leg.** ADR-0019's paired
`lamella-source-N` / `lamella-source-reference-id-N` keys exist
exactly to record those observations on the bank-side posting; this
oracle is the intake-time half of that contract.

Implications the oracle MUST honor:

1. **Sign-aware matching.** A `+50` import row matching an existing
   `+50` record is a same-leg re-observation (duplicate). Matching
   it against an existing `-50` record is a transfer-counterpart
   relationship — handled by the existing matcher sweep, NOT by
   this oracle. Conflating the two via `abs(amount)` would mark
   transfer counterparts as duplicates and silently lose data.
2. **Multi-leg ledger walk.** A 2-leg ledger entry has two postings
   the oracle needs to consider against an incoming row. Walking
   only the first concrete posting blinds the oracle to credit-side
   imports of debit-first entries.
3. **Lamella-txn-id inheritance.** When the oracle hits, the new
   staged row must adopt the matched record's `lamella-txn-id` so
   every observation of one event shares one identity. Minting a
   fresh id for each duplicate severs the lineage and makes the
   "N sources of the same event" view impossible.
4. **No "N duplicates" framing in the UI.** The user must see
   "N sources observing this event," with the option to confirm
   (the new source is the same event — append it to the existing
   entry's paired source meta, ADR-0019) or release (the oracle
   was wrong, treat as a separate event).

## Decision

Every staging path consults a unified **dedup oracle** before
inserting a new row. The oracle takes `(posting_date, amount,
description)` and returns either `None` or a `DedupHit` describing
what the new row collides with.

### Lifecycle: a new status, `likely_duplicate`

When the oracle returns a hit, the row is staged with
`status='likely_duplicate'`. This is distinct from `'new'` (work to
classify), `'matched'` (paired with a transfer counterpart), and
`'dismissed'` (user said no). Likely-duplicate rows are NOT shown on
the main review queue; they live on a dedicated "Review duplicates"
surface with two actions:

* **Confirm — same event.** Row goes to `'dismissed'` with
  `dismissal_reason='dedup'`. If the match was against the ledger,
  the ledger entry's source meta is stamped with this row's source
  ref so future fetches dedup on id (idempotent).
* **Release — different.** Row's status flips to `'new'` and goes
  through normal review. The dedup pointer is preserved so an audit
  trail exists ("the user said this wasn't a duplicate of X").

If the oracle returns no hit, the row is staged as `'new'` like before.

### The oracle: `staging.dedup_oracle.find_match`

```python
def find_match(
    conn,
    *,
    posting_date: str,
    amount: Decimal,
    description: str | None,
    reader: LedgerReader | None = None,
    window_days: int = 3,
    exclude_id: int | None = None,
) -> DedupHit | None: ...
```

Algorithm:

1. Compute `content_fingerprint` of the incoming `(date, |amount|,
   description)`.
2. Query `staged_transactions` within `±window_days` of the date.
   Fingerprint each candidate; first match wins. Skip rows already
   `'dismissed'` (the user already said "yes, dup, leave it") and
   `'failed'` (terminal, no signal).
3. If no staged match, compute the same fingerprint over every
   `Transaction` returned by `LedgerReader.load()` whose date is
   within the window. First match wins.
4. Return `DedupHit(kind='staged'|'ledger', staged_id|txn_hash,
   fingerprint, matched_date, matched_description)` or `None`.

`window_days` default of 3 absorbs date-of-record drift (a
bank-feed source's posted_date that lags the original ledger entry
by a day or two is the same event). Currency match is implicit in
the fingerprint (currencies don't normalize;
same-amount-different-ccy is a different fingerprint by
construction).

### Wiring: opt-in by source

`StagingService.stage()` gains an optional keyword:

```python
def stage(self, ..., dedup_check: bool = False, reader=None) -> StagedRow: ...
```

When `dedup_check=True` the service runs `find_match` before the
upsert and lands the row in `'likely_duplicate'` if a match is
found. Default `False` keeps every existing call site behaving the
same; sources that want intake-time dedup opt in.

Wired in this ADR's first cut:

* `bank_sync.ingest` — the live bank-feed source (currently the
  SimpleFIN adapter; future bank-feed adapters land via the same
  `BankDataPort` interface and inherit this opt-in).

Deferred to follow-ups (each adds a checkbox to this ADR):

* `_db.import_into_staging` — CSV / OFX / QIF / IIF / ODS / XLSX
  intake.
* `IntakeService.stage_paste` — already has its own
  `detect_paste_duplicates`; re-route that through the oracle so
  there's a single algorithm.
* `RebootService.scan_ledger` — does NOT opt in. Reboot rows ARE
  the ledger; checking each one against the ledger would mark
  every row as a duplicate of itself. The cross-source dup
  detection that reboot needs is the existing
  `_find_duplicate_groups` (post-stage scan).

### Status filtering

`StagingService.list_by_status` already supports arbitrary status
filters; the new `'likely_duplicate'` status flows through unchanged.
The default tuple stays `('new', 'classified', 'matched')` so the
existing review queue does not surface these rows. A dedicated
`/review/duplicates` route lists them with the matching reference
inline.

## Consequences

* **"Existing ledger then any new source" works.** Rows that match
  an existing ledger entry never make it onto the review queue as
  work-to-classify. They land on the duplicates surface for the
  user to confirm or release. Same applies whether the new source
  is a bank feed, CSV, paste, or anything else.
* **"Source A then source B" works for any pair.** Whichever source
  arrived first becomes the canonical staged record; later sources
  attach to its lineage as additional observations of the same
  event-leg.
* **Multi-leg, multi-source transfers stay coherent.** A transfer
  with N source records per leg renders as one event with N
  observations on each side, not as 2N "duplicates" the user has
  to manually reconcile.
* **One algorithm, not three.** Every source uses the same
  `content_fingerprint` via the same oracle. Adding a new source
  is a one-line `dedup_check=True` opt-in.
* **The user retains override.** Every "likely duplicate" decision
  is reviewable; the user can release a row that the oracle got
  wrong. False positives are visible and undoable, never silent.
* **Idempotent.** The oracle uses content fingerprint (not source
  id) so re-running an import does not re-create likely-duplicate
  rows for the same payload — the upsert path keeps the existing
  row's status.

## Compliance

Already landed:

* `content_fingerprint` (Phase D1.1) is the universal key.
* `_find_duplicate_groups` (reboot) does post-stage detection.
* `detect_paste_duplicates` does paste-time detection.
* `'likely_duplicate'` is a free new status (no migration —
  `staged_transactions.status` is open TEXT).

Required follow-ups before this ADR is "fully wired":

* [x] `staging.dedup_oracle` module with `find_match` querying
  staged + ledger.
* [x] `'likely_duplicate'` added to `STATES`.
* [x] Optional `dedup_check` parameter on `StagingService.stage`.
* [x] Bank-feed source opts in (currently the SimpleFIN adapter at
  `bank_sync.ingest`; new bank-feed adapters inherit through the
  shared `BankDataPort`).
* [x] Sign-aware matching — duplicates require equal signed
  amounts; opposite-sign pairs flow to the transfer matcher, not
  this oracle.
* [x] Multi-leg ledger walk — every posting on each candidate
  Transaction is a separate match candidate.
* [x] Lamella-txn-id inheritance — staged rows the oracle hits
  adopt the matched record's `lamella-txn-id`, so multi-source
  observations of one event share one event identity.
* [x] `/review/duplicates` lists likely-duplicate rows with
  confirm / release actions.
* [x] Unit tests for the oracle (staged-vs-staged,
  staged-vs-ledger, no-match).
* [x] Integration test: bank-feed ingest against a ledger that
  already has the same content lands the row as `likely_duplicate`.
* [x] Test for sign-aware matching — `+50` MUST NOT match `-50`
  (transfer counterpart, not duplicate).
* [x] Test for multi-leg ledger walk — credit-side import matches
  the credit leg of an existing 2-leg transfer entry.
* [x] Test for the multi-source transfer scenario the user
  surfaced (ledger → bank feed → payment-processor CSV; verifies
  observations land on correct legs and share lamella-txn-ids
  per leg).
* [x] On confirm, append `lamella-source-N` /
  `lamella-source-reference-id-N` /
  `lamella-source-description-N` paired meta to the matched
  ledger entry's bank-side posting (ADR-0019 + ADR-0059
  writeback). Landed 2026-04-29 — `bank_sync.synthetic_replace.
  append_source_paired_meta_in_place`.
* [x] Medium-tier dedup for description-divergent observations
  (signed amount + payee equality / description-token-overlap
  Jaccard ≥ 0.5). Landed 2026-04-29 with confidence labelling on
  the duplicates page.
* [x] CSV / OFX / QIF / IIF / ODS / XLSX intake opts in (via
  the ADR-0060 archive registration; `_mirror_to_staging` resolves
  file_id from content_sha256 join). Landed 2026-04-29.
* [x] Paste path archives content + uses `{file_id, row}`
  source_ref. Landed 2026-04-29 — `IntakeService.stage_paste`
  with `archived_file_id` kwarg.
* [ ] Paste path migrates from `detect_paste_duplicates` to the
  oracle (one algorithm, not two).
* [ ] First-run "ledger bootstrap": when Lamella starts on a
  non-empty ledger and `staged_transactions` is empty, build a
  ledger fingerprint cache so the oracle doesn't re-walk the
  entries on every intake call.
* [ ] `/review/duplicates` UX upgrade: group rows by
  `lamella-txn-id` so the user sees "this event has N source
  observations" instead of N independent rows. Confirm-all and
  release-all per event.

Each of those follow-ups updates this ADR with a checkbox.

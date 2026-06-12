# Normalize transaction identity metadata

**Status**: shipped, self-healing. Phases 1 to 4 landed. Phase 5 was reverted
(see "Self-healing model" below); Phase 6 cancelled.
**Audience**: an agent or developer picking this up cold.
**Triggers**: AI history bug on `/txn/{hash}` (tests/test_txn_detail_ai_history.py)
exposed that `ai_decisions.input_ref` has been overloaded with whatever
shape of id the AI happened to see at call time. Fix is broader than
that one route. It's a metadata-schema normalization spanning
transaction identity (lineage) and posting-level provenance.

## Self-healing model: the user never has to "run the migration"

Same shape as the `bcg-*` → `lamella-*` rebrand:

  * **Read-side compat is permanent.** `_legacy_meta.normalize_entries`
    transparently mirrors legacy txn-level source keys
    (`lamella-simplefin-id` / bare `simplefin-id` /
    `lamella-import-txn-id`) down to the source-side posting as paired
    indexed source meta at parse time. Every reader sees the new
    shape regardless of what's on disk.
  * **`/txn` AI history accepts every input_ref shape forever.** The
    `candidate_refs` expansion in `routes/search.txn_detail` matches
    decisions keyed on lineage UUID, Beancount `txn_hash`, OR the
    SimpleFIN bridge id. This is permanent compat, not migration
    scaffolding.
  * **Writers emit the new schema.** Every new SimpleFIN ingest /
    importer emit / staging promote stamps `lamella-txn-id` plus
    posting-level paired source meta. The legacy txn-level keys are
    still emitted today (dual-emit) for the benefit of direct readers
    that haven't been migrated to `iter_sources` yet; that's a
    follow-up cleanup, not a correctness issue.
  * **On-touch re-write.** Whenever the in-place rewriter
    (`rewrite/txn_inplace`) edits a transaction for any other reason
    (FIXME → category, M→N posting rewrite), it also normalizes that
    transaction's identity meta on disk: mints lineage if missing,
    migrates legacy txn-level source keys to posting-level paired
    source meta, drops retired keys. Legacy entries converge to the
    new schema as the user works.
  * **Bulk action lives in `/setup/recovery`.** For users who want
    clean disk content all at once, `POST /setup/normalize-txn-identity`
    runs the same normalization across every `.bean` file under
    `ledger_dir`, snapshots before write, bean-checks against
    baseline, restores on any new error, and backfills
    `ai_decisions.input_ref` to lineage. Never required;
    everything works without it.

The user is **not** expected to run the bulk transform from a CLI.
The implementation is in `transform/normalize_txn_identity.py`; the
module-level `__main__` exists for tests + dev only.

---

## Why this matters

Today, "what transaction is this" and "where did each leg come from"
are encoded across multiple ad-hoc transaction-level keys, depending
on which ingest path produced the entry:

| Ingest path        | Metadata stamped on the **transaction**             |
|--------------------|-----------------------------------------------------|
| SimpleFIN          | `lamella-simplefin-id: "TRN-…"`                     |
| Spreadsheet import | `lamella-import-id: "42"` + `lamella-import-txn-id: "<csv-id>"` |
| Manual / paste     | nothing                                             |

And `ai_decisions.input_ref` mirrors that mess: it stores the
Beancount `txn_hash` for post-promotion calls, the SimpleFIN `txn.id`
for ingest-time calls, the composite `import:<imports.id>:row:<raw_rows.id>`
for spreadsheet-AI calls (both halves are SQLite PKs!), and nothing
useful for manually entered transactions.

Three separate problems compound:

1. **Identity-shape coupling**: every reader has to know every shape
   the txn id might take. New ingest path = touch every reader.
2. **Posting-vs-transaction confusion**: provenance is a property of
   each leg (each posting came from one source), not the whole
   transaction. Today's transaction-level `lamella-simplefin-id`
   silently fails for two-leg transfers and cross-source merges.
3. **Reconstruct violations**: SQLite PKs have leaked into ledger
   metadata (`lamella-import-id: "42"` is an `imports.id`). Wipe
   the cache, the 42 means nothing. Per CLAUDE.md, every metadata
   value must be reconstructable from the ledger alone.

Concrete production bugs already on file:

- `/txn/{hash}` AI history was silently empty for every SimpleFIN-ingested
  transaction (patched in `routes/search.py:562`; patch knows about
  SimpleFIN only; importer-AI rows still invisible).
- `/ai/suggestions` supersession dedup at `routes/ai.py:148` keys off
  `input_ref`, so the same logical txn classified once at ingest and
  once post-promotion shows up as two pending rows.
- Three compat reads exist for SimpleFIN alone (`lamella-simplefin-id`,
  legacy `simplefin-id`, plus the `bcg-*` → `lamella-*` transition).
  Same compat zoo will reappear for the importer when it's renamed.
- Reconstruct cannot link an AI decision back to its txn for any
  decision logged before promotion, because no stable bridge exists
  between the staging id and the eventual ledger entry.

---

## Final schema

Two **orthogonal** concepts on **two different scopes**.

### Transaction-level: `lamella-txn-id` (lineage)

A UUIDv7 we mint and stamp on the entry **the first time we see it**,
regardless of ingest path. Stable across edits, reformats, account
re-targeting; anything that would change `txn_hash`. UUIDv7 chosen
for natural time-ordering when sorted lexicographically (cheap audit
log scans).

This is what every internal subsystem keys off:

- `ai_decisions.input_ref` for `classify_txn` and `match_receipt`
  decisions.
- Override resolution (replaces `lamella-override-of: <txn_hash>`).
- Future receipt-link bridge, recurring-confirmation bridge, etc.

### Posting-level: paired indexed source keys (provenance)

Each posting carries 0 or more `(source, reference-id)` pairs as
**indexed paired metadata keys, dense from 0**:

```
2026-04-10 * "Transfer A → Amazon"
  lamella-txn-id: "0190fe22-7c10-..."
  Assets:Acme:Checking            -500.00 USD
    lamella-source-0: "simplefin"
    lamella-source-reference-id-0: "TRN-A1"
  Assets:Personal:Amazon:Seller    500.00 USD
    lamella-source-0: "simplefin"
    lamella-source-reference-id-0: "TRN-B2"
```

After cross-source dedup (CSV row matched into existing SimpleFIN
posting):

```
2026-04-10 * "Hardware Store" "Supplies"
  lamella-txn-id: "0190fe22-7c10-..."
  Liabilities:Acme:Card:CardA1234  -42.17 USD
    lamella-source-0: "simplefin"
    lamella-source-reference-id-0: "TRN-X"
    lamella-source-1: "csv"
    lamella-source-reference-id-1: "ROW-99"
  Expenses:Acme:Supplies            42.17 USD
```

Single-source charge (only the bank-side posting carries provenance;
the expense leg was synthesized by us):

```
2026-04-10 * "Hardware Store" "Supplies"
  lamella-txn-id: "0190fe22-7c10-..."
  Liabilities:Acme:Card:CardA1234  -42.17 USD
    lamella-source-0: "simplefin"
    lamella-source-reference-id-0: "TRN-X"
  Expenses:Acme:Supplies            42.17 USD
```

#### Schema rules

- **Indexes start at 0** and are dense (no gaps). When source N is
  removed from a posting, indexes ≥ N+1 renumber down. The writer
  enforces this; readers tolerate sparse indexes during a hand-edit
  window but log a warning.
- **Pairs are required**: `lamella-source-N` MUST coexist with
  `lamella-source-reference-id-N`. The load-time validator (in
  `_legacy_meta.normalize_entries`) warns on orphans and drops them.
- **Un-indexed form is accepted on read**: a hand-edited posting may
  carry bare `lamella-source` + `lamella-source-reference-id` (no
  `-N`). The load-time normalizer treats the bare pair as equivalent
  to index 0 if 0 isn't already present; otherwise it's folded into
  the next free index. If both bare and `-0` are present (a writer
  bug or a hand-edit collision), the indexed form wins and a warning
  is logged. The transform's `--apply` mode rewrites bare keys to the
  indexed canonical form on disk so the ambiguity is one-shot.
- **Allowed source names** (governed by an enum in
  `lamella/identity.py::SourceName`):
  - `simplefin`: SimpleFIN bridge (external system owns the id)
  - `csv`: generic CSV import (one of: source-provided id, or
    natural-key hash of `(date, amount, payee, description)`)
  - `paste`: pasted tabular data
  - `manual`: entered by hand in the editor; recorded for
    completeness but `reference-id` is the natural-key hash
  - Future ingest paths add to the enum.
- **Reference ids must be reconstruct-stable**: derivable from the
  source data, not from a SQLite PK. SimpleFIN id, CSV native id,
  or natural-key hash; never an `imports.id` / `raw_rows.id`.

#### Why posting-level

A two-leg transfer is one transaction with two halves; each half
came from at most one ingest event. SimpleFIN-fed Account A
transferring to SimpleFIN-fed Account B produces two SimpleFIN ids,
one per side. Cross-source merges (CSV import detecting overlap
with existing SimpleFIN entry) attach a second source to the
matched leg, not to the transaction.

#### Reader contract

Readers should never read raw meta keys directly. Use the helper:

```python
def iter_sources(posting_meta: dict) -> Iterator[tuple[str, str]]:
    """Yield (source, reference_id) tuples in index order, then any
    bare un-indexed pair as a trailing entry.

    Orphaned keys (one half of a pair missing) are warned and skipped.
    Bare un-indexed keys (lamella-source / lamella-source-reference-id
    with no -N suffix) are tolerated for hand-edits and yielded last
    so they don't shift indexed entries' positions during reads.
    """
```

Implementation lives in `lamella/identity.py` so every reader uses
one path. Callers that want a list use `list(iter_sources(...))`;
callers that want a specific source check `(source_name, ref_id) in
list(iter_sources(...))`.

The Phase 1 normalizer rewrites bare un-indexed pairs to their
indexed equivalent at parse time so downstream code only ever sees
the canonical indexed form. The bare-key tolerance in the helper is
defense-in-depth for code paths that bypass the normalizer.

### Keys retired by this change

| Old key                   | Replacement                                                |
|---------------------------|------------------------------------------------------------|
| `lamella-simplefin-id`    | `lamella-source-N: "simplefin"` + `lamella-source-reference-id-N: <id>` on the bank-side posting |
| `simplefin-id` (bare)     | same                                                       |
| `lamella-import-id`       | retired (was `imports.id`, a SQLite PK; reconstruct violation) |
| `lamella-import-txn-id`   | `lamella-source-N: "csv"` + `lamella-source-reference-id-N: <csv-id-or-natural-key-hash>` on the bank-side posting |
| `lamella-import-source`   | retire (was a free-form `source=… row=…` string for debug) |

### Keys NOT changed

- All `lamella-paperless-*` keys (different concern: doc index, not ingest)
- `lamella-ai-classified`, `lamella-ai-decision-id`, `lamella-rule-id`
  (annotate the AI outcome, not txn identity)
- `lamella-mileage-*`, `lamella-loan-slug`, `#lamella-*` tags
- `lamella-import-memo` (preserves user-visible memo separately from id)
- `lamella-override-of`: **deferred decision**. Re-keying to
  `lamella-txn-id` is the right answer but adds scope. Held for
  follow-up; today's `txn_hash`-keyed overrides keep working because
  the override file is regenerated on every override write.

---

## Reconstruct guarantees

After this change, every value in posting meta is recomputable from
inputs that survive a cache wipe:

| Source     | Reference id origin                                         | Reconstruct path |
|------------|-------------------------------------------------------------|------------------|
| simplefin  | SimpleFIN bridge (external)                                 | Re-fetch from SimpleFIN; ids stable. |
| csv        | CSV's own column if present; else SHA256 of `(date, amount, payee, description)` | Re-import the same CSV; same content → same hash. |
| paste      | SHA256 natural-key hash                                     | Re-paste the same content → same hash. |
| manual     | SHA256 natural-key hash                                     | Reconstruct from the .bean file fields directly. |

`lamella-txn-id` is minted at first sight. Once on disk, it's
authoritative and never regenerated. The transform stamps it on
every existing entry (mass-mint pass) so all entries (historical,
SimpleFIN, importer, manual) share the same identity scheme from
day one.

### Cross-source dedup: collision risk and mitigation

Natural-key hash collisions at the SHA256 level are astronomically
unlikely for 4-field tuples, but the surface that produces them is:

- **CSV row dedup against existing SimpleFIN entry**: when the
  importer detects a high-confidence match, it appends a new
  `(csv, ref-id)` pair to the existing posting via in-place
  rewrite; does NOT create a new transaction. Match must clear
  the same threshold the receipt linker uses.
- **False-positive auto-merge mitigation**: cross-source dedup runs
  as a job with a review modal, not silently. The user reviews
  each candidate match before the source link is appended. Same
  pattern as receipt linking. Wrong auto-merge would silently
  fabricate provenance, a serious data-integrity failure mode.

---

## Backwards compatibility

### At-load normalization (read side)

`lamella/_legacy_meta.py::normalize_entries` learns to:

1. **Move transaction-level legacy keys down to the source-side
   posting**:
   - `lamella-simplefin-id: X` (or bare `simplefin-id: X`) on a txn
     → find the posting whose account matches the SimpleFIN-mapped
     account; stamp `lamella-source-0: "simplefin"` +
     `lamella-source-reference-id-0: X` on that posting; drop the
     transaction-level key.
   - `lamella-import-id: I` + `lamella-import-txn-id: T` on a txn
     → find the source-side posting (importer convention: first
     non-clearing posting); stamp `lamella-source-0: "csv"` +
     `lamella-source-reference-id-0: T` on that posting; drop both
     transaction-level keys. The `imports.id` value `I` is dropped
     entirely (SQLite-only artifact).
2. **Mint `lamella-txn-id` if missing** at parse time. Cache-only:
   the value lives in the in-memory entry but is NOT written
   back to disk. (Disk writes happen via the transform's
   `--apply` mode, which the user runs explicitly.)
3. **Validate paired source keys**: orphans warned and dropped.

This means: every reader downstream sees the new schema, even
on a ledger that hasn't been transformed yet. New code can rely
on `lamella-txn-id` and `iter_sources()` unconditionally.

### `ai_decisions` lineage backfill (one-shot)

Run as part of the Phase 4 transform:

- `input_ref` matches a `txn_hash` (40/64-char hex) → look up the
  entry via `LedgerReader`, read its `lamella-txn-id` (mint if
  needed during the transform's mass-mint pass), rewrite
  `input_ref` to the lineage id.
- `input_ref` matches a SimpleFIN id (`TRN-…` or starts with
  `sf-`) → resolve via the simplefin-id-to-entry map (already
  built today by `routes/ai.py::_build_simplefin_id_to_hash_map`),
  read the entry's lineage id, rewrite.
- `input_ref` matches `import:<id>:row:<raw_row_id>` → resolve
  via the staging table if it still exists; if not, leave as-is
  (legacy unreachable row). Most importer-AI history is recoverable
  if the staging tables haven't been wiped.
- Decisions whose underlying entry has been deleted: leave alone.

After the backfill, per-txn AI history is a single column lookup
again: `SELECT … FROM ai_decisions WHERE input_ref = ?` with
`?` = `lamella-txn-id`.

---

## Rollout phases

Each phase is a separate commit, individually revert-able. None push
without the user's explicit instruction.

### Phase 1: read-side compat (zero-risk)

`_legacy_meta.normalize_entries` learns:

- **Mirror** legacy txn-level source keys (`lamella-simplefin-id`,
  bare `simplefin-id`, `lamella-import-txn-id`) down to the
  source-side (first) posting as paired indexed source meta. The
  legacy txn-level key STAYS in place so existing readers that
  do `meta.get("lamella-simplefin-id")` keep working untouched.
  Phase 1 is additive. The on-disk drop happens in Phase 4.
- **Fold** bare un-indexed posting source pairs to the indexed
  canonical form.

**No auto-mint of `lamella-txn-id` at parse time.** Several places
in the codebase (`bootstrap/classifier.py:374`,
`main.py:416`) use "any `lamella-*` key" as a "this entry is
Lamella-managed" heuristic. Silently minting a lineage id on
every entry breaks that heuristic for every entry. Lineage is
stamped only by:
- Writers (Phase 2: every emit gets a fresh UUIDv7).
- The on-disk transform (Phase 4 `--apply`: mass-mint pass).
- Lazy-mint helpers invoked explicitly when a subsystem needs
  one (e.g., the AI history backfill; writes back to disk via
  the in-place rewriter).

Downstream code that wants the lineage id reads `meta.get(TXN_ID_KEY)`
and handles `None`; the absence indicates a pre-transform legacy
entry.

New module: `lamella/identity.py` exposing `mint_txn_id()`,
`iter_sources()`, `stamp_source()`, `normalize_bare_to_indexed()`,
plus the `SOURCE_NAMES` enum constant. Pure helpers; no
dependencies beyond stdlib.

Tests: extend `tests/test_legacy_meta_normalization.py` for each
rewrite case + idempotency + the explicit "no auto-mint" assertion.
Add `tests/test_identity_helpers.py`.

### Phase 2: writers emit the new schema

- `simplefin/writer.py::render_entry` and `render_entry`-style
  callers emit `lamella-txn-id` + posting-level paired source keys.
  Stop emitting `lamella-simplefin-id`.
- `importer/emit.py::render_transaction` emits the same. Stop
  emitting `lamella-import-id` / `lamella-import-txn-id` /
  `lamella-import-source`.
- The unified staging promoter stamps `lamella-txn-id` on every
  promoted entry, even with no source.
- Pre-existing tests using fixture ledgers continue to pass via the
  Phase 1 normalizer; new tests assert the new shape on freshly
  written entries.

Fixtures: update `tests/fixtures/ledger/simplefin_transactions.bean`
to the new schema so newly-added tests demonstrate the target state.
Older fixtures keep their legacy keys to exercise the compat layer.

### Phase 3: AI decisions key off `lamella-txn-id`

Update every `classify_txn` call site:
- `ai/classify.py:160`, `ai/classify.py:190`
- `ai/bulk_classify.py:424`, `ai/bulk_classify.py:444`
- `simplefin/ingest.py:1318`, `simplefin/ingest.py:1343`
- `importer/categorize.py:451`

Each passes the entry's `lamella-txn-id` as `input_ref`. For
ingest-time calls (the entry doesn't yet exist), the staging row
carries the future `lamella-txn-id` minted at staging time.

Revert the SimpleFIN-aware expansion in `routes/search.py:562`;
the AI history query collapses to `WHERE input_ref = ?` again.

Tests: keep `tests/test_txn_detail_ai_history.py`, re-key it to
`lamella-txn-id`. Add a test asserting an ingest-time decision
survives staging→promotion via the lineage id.

### Phase 4: one-shot transform (DRY-RUN by default)

`python -m lamella.transform.normalize_txn_identity`:

- Walks every `.bean` file under `ledger_dir` (skips `_archive*/`,
  `.pre-inplace-*/`, `.pre-normalize-*/`).
- For each transaction directive:
  - If has `lamella-simplefin-id` (or bare `simplefin-id`), move
    to source-side posting as paired source key.
  - If has `lamella-import-id` + `lamella-import-txn-id`, move to
    source-side posting as paired source key (`csv`).
  - If lacks `lamella-txn-id`, mint a UUIDv7 and stamp at txn level.
- Edits are line-based (same discipline as `rewrite/txn_inplace.py`):
  preserve whitespace, comments, posting meta order.
- Writes a `.pre-normalize-<ISO-timestamp>/` snapshot before any byte
  changes (mirrors the in-place rewriter's `.pre-inplace-*/`).
- Runs `bean-check` against baseline; on new errors, restores from
  snapshot.
- Backfills `ai_decisions.input_ref` per the strategy above.
- Default mode is dry-run with a per-file diff summary. `--apply`
  performs the writes.

Tests: `tests/test_transform_normalize_txn_identity.py` end-to-end.

### Phase 5: REVERTED

Originally: drop the SimpleFIN-aware expansion in `routes/search.py`
once Phase 4's transform stamps lineage everywhere. Reverted in
favour of the self-healing model; the candidate-refs expansion is
**permanent compat** (matches the `bcg-*` pattern). Users never have
to run the transform for `/txn` AI history to surface every decision.

### Phase 6: CANCELLED

Originally: drop the legacy-key compat in
`_legacy_meta.normalize_entries`. Cancelled. Read-side compat stays
forever, like `bcg-*`.

### Phase 7a: drop importer dual-emit + migrate parsed-entry readers (DONE)

  * Importer dual-emit dropped from `importer/emit.py`. New writes
    no longer carry `lamella-import-id` (a SQLite PK reconstruct
    violation), `lamella-import-source` (free-form debug), or
    `lamella-import-txn-id` (replaced by paired source meta on
    source-side posting). `lamella-import-memo` stays (user content,
    not an identifier).
  * `identity.py::find_source_reference(entry, source_name)` and
    `find_all_source_references(entry, source_name)` are the new
    canonical helpers. Walk every posting's paired source meta;
    legacy txn-level keys mirror down to first posting via
    `_legacy_meta.normalize_entries` so they're seen transparently.
  * Migrated readers (all SimpleFIN id lookups now go through
    `find_source_reference(entry, "simplefin")`):
      - `simplefin/dedup.py::_meta_simplefin_id`
      - `duplicates/scanner.py::_sfid`
      - `routes/ai.py` (4 sites: by_hash builder, suggestion href
        resolver, audit-page hash resolver, `_build_simplefin_id_to_hash_map`)
      - `routes/api_txn.py` (ai_decisions correction stamp)
      - `routes/search.py` (per-txn AI history candidate_refs)

### Phase 7c: bridge importer categorize → emit lineage (DONE)

The importer's AI calls historically logged
`ai_decisions.input_ref = "import:<imports.id>:row:<raw_rows.id>"`,
both sides SQLite PKs with no reconstruct-stable bridge to the
eventual ledger entry. Once Phase 7 dropped `lamella-import-source`
from the writer, even that shaky composite was no longer
back-resolvable, leaving every importer-AI decision orphaned from
`/txn` AI history.

Fix is structural: mint the entry's `lamella-txn-id` at categorize
time, persist it on the categorizations row, use it as the AI
`input_ref`, and have `emit.render_transaction` read it back so
the on-disk `lamella-txn-id` matches what `ai_decisions` already
claims. Single shared identifier, end to end. Same shape as the
SimpleFIN flow now.

  * Migration `055_categorizations_lamella_txn_id.sql` adds a
    nullable `lamella_txn_id` column to `categorizations`.
  * `categorize._resolve_or_mint_lineage(conn, raw_row_id)` returns
    the existing lineage on re-categorize (preserves the binding to
    pre-existing AI decisions) or mints fresh.
  * `categorize_import` mints lineage at the top of the per-row
    loop, threads it through every `_upsert_categorization` call
    (annotated, payee_rule, classification_rule, AI, fall-through),
    AND uses it as the AI `input_ref` instead of the SQLite-PK
    composite.
  * `emit.render_transaction` reads `row["cat_lamella_txn_id"]`
    when present and uses it as the entry's `lamella-txn-id`;
    falls back to a fresh mint only for callers that bypass
    categorize (legacy paths, tests).

### Phase 7b: drop SimpleFIN dual-emit (DONE)

  * `duplicates/cleaner.py` learned the new format. New helper
    `_extract_sfid_from_block(block_lines)` matches the SimpleFIN
    id from either the legacy txn-level `lamella-simplefin-id`
    line OR the post-Phase-7 paired source meta on a posting
    (any index N where `lamella-source-N: "simplefin"` is paired
    with `lamella-source-reference-id-N: "<id>"`).
  * `_inject_aliases_into_block` picks an insertion anchor:
    legacy `lamella-simplefin-id` if present, else the
    `lamella-txn-id` lineage line, else the date header. Aliases
    continue to live at txn-meta level; readers in
    `simplefin/dedup` look there.
  * `simplefin/ingest.py::_stamp_alias_on_ledger` now uses
    `_extract_sfid_from_block` so the alias-stamp flow finds
    primary SimpleFIN ids in either format.
  * `simplefin/writer.py` stops emitting `lamella-simplefin-id`
    at txn level (both `render_entry` and `append_split_entry`).
    New entries carry only `lamella-txn-id` at txn meta + paired
    indexed source on the source-side posting.

Legacy on-disk content carrying `lamella-simplefin-id` at txn
level still parses transparently via
`_legacy_meta.normalize_entries` (mirrors down to first posting),
so the migration is fully self-healing.

`registry/account_meta_writer.py` is **not** in scope: it stamps
`lamella-simplefin-id` on Open directives to record an account's
SimpleFIN account id (different concept than transaction id).
Same key name, different scope, different concern.

---

## Bulk normalization (for the user)

Lives at `POST /setup/normalize-txn-identity`. Triggered from
`/setup/recovery`. Runs the transform across every `.bean` file
under `ledger_dir`, snapshots to `.pre-normalize-<ISO>/`, bean-
checks against baseline, restores on any new error, and backfills
`ai_decisions.input_ref` to lineage where resolvable.

**The user is not expected to run anything from a CLI.** The
read-side compat in `_legacy_meta` plus on-touch normalization in
`rewrite/txn_inplace` mean the system functions correctly forever
without the bulk action ever firing; the recovery action exists
only as an opt-in cleanup for users who want their on-disk
content tidied up all at once.

The CLI entry-point at
`python -m lamella.transform.normalize_txn_identity` is retained
for tests + dev use.

---

## Open questions deferred to a follow-up

1. **`lamella-override-of` re-keying**: should override pointers
   migrate from `txn_hash` to `lamella-txn-id`? Probably yes for
   long-term coherence; held for follow-up to keep this scope
   bounded.
2. **Receipt links** (`connector_links.bean` `custom "receipt-link"`):
   reference `txn_hash` today. Same migration question. Defer.
3. **`match_receipt` AI decisions**: log `input_ref = txn_hash`
   today. For consistency they should key off `lamella-txn-id`.
   Confirm before Phase 3 ships; small extra scope.
4. **Cross-source dedup matcher**: the schema enables this; the
   matcher itself is separate work (importer-side, run as a job
   with review modal). Tracked in FUTURE.md after Phase 5 lands.
5. **Source-name enum extensibility**: `SourceName` is a closed
   enum today. If we ever add `plaid`, `csv-bofa`, etc., the enum
   gains members. Deliberately not extensible by the user; keeps
   the dedup index well-defined.

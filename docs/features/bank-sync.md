---
audience: agents
read-cost-target: 100 lines
authority: informative
status: Active Development
cross-refs: docs/adr/0001-ledger-as-source-of-truth.md, docs/adr/0004-bean-check-after-every-write.md, docs/adr/0008-unconditional-dedup.md, docs/adr/0019-transaction-identity-use-helpers.md, docs/adr/0020-adapter-pattern-for-external-data-sources.md, docs/specs/AI-CLASSIFICATION.md, docs/specs/NORMALIZE_TXN_IDENTITY.md
last-derived-from-code: 2026-04-29
---
# Bank Sync

## Summary

SimpleFIN Bridge ingest: fetch → dedup → classify → write to simplefin_transactions.bean with bean-check rollback.

## Owned routes

| Method | Path | Handler | File:line |
|---|---|---|---|
| GET | `/simplefin` | `simplefin_page` | `src/lamella/web/routes/simplefin.py:373` |
| POST | `/simplefin/account-map` | `save_account_map` | `src/lamella/web/routes/simplefin.py:450` |
| POST | `/simplefin/discover` | `discover_accounts` | `src/lamella/web/routes/simplefin.py:717` |
| POST | `/simplefin/fetch` | `fetch_now` | `src/lamella/web/routes/simplefin.py:538` |
| POST | `/simplefin/map` | `save_account_mapping` | `src/lamella/web/routes/simplefin.py:754` |
| POST | `/simplefin/mode` | `set_mode` | `src/lamella/web/routes/simplefin.py:382` |
| POST | `/simplefin/settings` | `update_simplefin_settings` | `src/lamella/web/routes/simplefin.py:407` |

## Owned templates

- `src/lamella/web/templates/simplefin.html`

## Owned source files

- `src/lamella/adapters/simplefin/client.py`
- `src/lamella/adapters/simplefin/schemas.py`
- `src/lamella/features/bank_sync/dedup.py`
- `src/lamella/features/bank_sync/ingest.py`
- `src/lamella/features/bank_sync/notify_hook.py`
- `src/lamella/features/bank_sync/payout_sources.py`
- `src/lamella/features/bank_sync/refund_detect.py`
- `src/lamella/features/bank_sync/schedule.py`
- `src/lamella/features/bank_sync/writer.py`

## Owned tests

- `tests/test_simplefin_client.py`
- `tests/test_simplefin_dedup.py`
- `tests/test_simplefin_ingest_ai.py`
- `tests/test_simplefin_ingest_rules.py`
- `tests/test_simplefin_notify_hook.py`
- `tests/test_simplefin_revert_on_beancheck_fail.py`
- `tests/test_simplefin_route_emit_contract.py`
- `tests/test_simplefin_staging_mirror.py`
- `tests/test_simplefin_writer.py`

## ADR compliance

- ADR-0001: ledger is dedup authority; `build_index` never consults SQLite as primary
- ADR-0004: `SimpleFINWriter` truncates + restores on any `BeanCheckError`
- ADR-0008: unconditional dedup by simplefin id AND content fingerprint before write
- ADR-0019: `_meta_simplefin_id` calls `find_source_reference(txn, "simplefin")`; legacy txn-level keys normalized at parse time

## Current state


`SimpleFINClient` (`client.py`) wraps `httpx.AsyncClient` with tenacity 3-attempt retry, token claim, and credential splitting. `fetch_accounts()` returns a validated `SimpleFINBridgeResponse` pydantic model.

`SimpleFINIngest` (`ingest.py`) orchestrates fetch → dedup → classify → write:
1. `dedup.build_index()` walks the ledger and builds a `set[str]` of known SimpleFIN ids. Uses `find_source_reference(entry, "simplefin")` (ADR-0019) plus `lamella-simplefin-aliases` for content-matched alternate ids.
2. Content-fingerprint dedup: `_build_content_index()` catches re-delivered events with fresh IDs; `_stamp_alias_on_ledger()` stamps the new id as an alias instead of writing a duplicate.
3. Per-transaction: `staging.stage()` upserts a `staged_transactions` row, then `_classify()` calls `rules.engine.evaluate()`. A user-created rule at ≥ 0.95 confidence fires `GateAction.AUTO_APPLY_RULE`; everything else defers to staging for user action.
4. Loan preemption (`claim_from_simplefin_facts`) bypasses the AI path for mortgage-class transactions; `_auto_classify_claimed_ingest_entries()` runs post-write.
5. Matcher sweep (`staging.sweep`) and `TransferWriter` run before the final write; paired transfers route to `connector_transfers.bean`.
6. `SimpleFINWriter.append_entries()` acquires a `.lamella.lock` file lock, appends, fsyncs, runs `run_bean_check(main_bean)`, and truncates-to-pre-size on failure (ADR-0004).
7. `render_entry()` emits `lamella-txn-id` (UUIDv7 at txn level) and `lamella-source-0: "simplefin"` + `lamella-source-reference-id-0: "<id>"` on the source-side posting (ADR-0003, ADR-0019).

FIXME placeholder routing is sign-aware (e70d3c6): `_fixme_for_entity(entity, amount)` routes positive amounts (deposits, credit-to-source) to `Income:{entity}:FIXME` so the AI's per-root whitelist can propose `Income:*` accounts; non-positive amounts stay at `Expenses:{entity}:FIXME`. `amount=None` callers fall back to legacy Expenses-only routing.

ADR-0043 staged-txn directives are wired in as an opt-in path behind `Settings.enable_staged_txn_directives` (default `False`). When enabled, deferred (un-classified) rows accumulate as `PendingEntry` on `_pending_staged_directives` and are drained at end-of-run via `writer.append_staged_txn_directives()`, each staged row gets a `custom "staged-txn"` directive in `simplefin_transactions.bean` carrying the same `lamella-txn-id` as its `staged_transactions` row, so `/txn/{token}` is stable across the staging → promotion bridge. Both paths coexist; the legacy FIXME-posting path remains the default. Promotion is atomic: `SimpleFINWriter.promote_staged_txn()` replaces the directive with `custom "staged-txn-promoted"` and appends the real txn under one lock + one bean-check, rolling both files back byte-for-byte on failure (ADR-0004).

Account-open guard (`account_open_guard.ensure_target_account_open`, 7a8cc94) auto-scaffolds a new leaf into `connector_accounts.bean` when its entity (segment 1) is already attested by any opened account under any of the five top-level roots, not just when the parent prefix is itself open. Brand-new entities still get rejected; deepening an established hierarchy (e.g. first `Expenses:Acme:COGS:Materials` under an existing Acme entity) no longer requires a `/settings/accounts` round-trip.

Refund detection (`refund_detect.py`, dea9c81 + 836860e) is a read-only ledger walk. `find_refund_candidates(conn, reader, refund_amount, refund_date, merchant, narration, source_account, window_days=60)` scores recently-classified expense txns against an incoming positive-amount row (merchant 0.40 + amount-tolerance 0.30/0.10 + date-window 0.20 + same-account 0.10, threshold 0.50, top 5). It is invoked from the `/api/txn/{ref}/classify` endpoint, not from ingest itself; on classify, the chosen candidate's `lamella-txn-id` is stamped as `lamella-refund-of` at the txn-meta level via the writer (`PendingEntry.refund_of_txn_id`) or `OverrideWriter.append(extra_meta=...)`. The link is the source of truth, the SQLite cache is disposable (ADR-0001).

Post-workstream-C1: no ingest-time AI classify. The `_maybe_ai_classify` method exists but is never called during normal ingest. Users trigger AI via `/review/staged/ask-ai`.

Scheduling: `schedule.register()` configures APScheduler with `IntervalTrigger(hours=N, jitter=300)`; modes `shadow` and `active` share the same trigger.

### Compliant ADRs
- ADR-0001: ledger is dedup authority; `build_index` never consults SQLite as primary
- ADR-0004: `SimpleFINWriter` truncates + restores on any `BeanCheckError`
- ADR-0008: unconditional dedup by simplefin id AND content fingerprint before write
- ADR-0019: `_meta_simplefin_id` calls `find_source_reference(txn, "simplefin")`; legacy txn-level keys normalized at parse time

### Known violations
- ADR-0020: `SimpleFINClient` is consumed directly by `SimpleFINIngest.__init__`; no port/adapter interface isolates the HTTP dependency (medium-high)

## Known gaps

- ADR-0020: `SimpleFINClient` is consumed directly by `SimpleFINIngest.__init__`; no port/adapter interface isolates the HTTP dependency (medium-high)

## Remaining tasks


1. [ ] Move SimpleFIN behind an adapter port: define a `BankFeedPort` protocol in `lamella/ports/`; `SimpleFINClient` becomes one implementation. Enables offline/test stubs without monkey-patching (ADR-0020, medium-high).
2. [ ] Flip `enable_staged_txn_directives` default to `True` after the soak window and retire the legacy FIXME-posting branch in `_classify()` once the bulk-migration script (`migrate_fixme_to_staged_txn.py`) has been run on representative ledgers (ADR-0043, medium-high).
3. [ ] Surface `_maybe_ai_classify` path gating in settings UI: the method exists but is dead code post-C1; either wire it to an explicit setting or delete it to reduce confusion (medium).
4. [ ] Balance anchor directive writes (`append_balance_anchor`) skip bean-check: low-risk but inconsistent with ADR-0004 discipline (low).
5. [ ] Shadow-mode preview bean path is not included in `main.bean` by design, but there is no negative test confirming it never leaks through (low).

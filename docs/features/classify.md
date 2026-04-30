---
audience: agents
read-cost-target: 100 lines
authority: informative
status: Active Development
cross-refs: docs/adr/0009-card-binding-as-hypothesis.md, docs/adr/0010-rules-are-signals-not-commands.md, docs/adr/0018-classification-intentionally-slow.md, docs/adr/0006-long-running-ops-as-jobs.md
last-derived-from-code: 2026-04-29
---
# Classify

## Summary

Resolve Expenses:FIXME / Income:FIXME / Liabilities:FIXME postings via context-rich AI classification (trickle + bulk paths).

## Owned routes

_No routes own this feature._

## Owned templates

_No templates own this feature._

## Owned source files

- `src/lamella/features/ai_cascade/audit.py`
- `src/lamella/features/ai_cascade/bulk_classify.py`
- `src/lamella/features/ai_cascade/classify.py`
- `src/lamella/features/ai_cascade/context.py`
- `src/lamella/features/ai_cascade/decisions.py`
- `src/lamella/features/ai_cascade/draft_description.py`
- `src/lamella/features/ai_cascade/enricher.py`
- `src/lamella/features/ai_cascade/gating.py`
- `src/lamella/features/ai_cascade/match.py`
- `src/lamella/features/ai_cascade/mileage_context.py`
- `src/lamella/features/ai_cascade/notes.py`
- `src/lamella/features/ai_cascade/receipt_context.py`
- `src/lamella/features/ai_cascade/service.py`
- `src/lamella/features/ai_cascade/subcategory_miner.py`
- `src/lamella/features/ai_cascade/trickle_classify.py`
- `src/lamella/features/ai_cascade/vector_index.py`

## Owned tests

- `tests/test_bulk_classify_priority.py`
- `tests/test_classifier.py`
- `tests/test_classify_account_extension.py`
- `tests/test_classify_group.py`
- `tests/test_classify_open_date_validation.py`
- `tests/test_classify_prompt_guards.py`
- `tests/test_loans_auto_classify.py`
- `tests/test_loans_auto_classify_inplace.py`
- `tests/test_phase2_widen_classify.py`
- `tests/test_simplefin_ingest_ai.py`
- `tests/test_trickle_classify.py`

## ADR compliance

- ADR-0009: card binding is the starting hypothesis; `entity_from_card` / `resolve_entity_for_account` sets it, override signals (notes, prior rejections, suspicion) can widen it
- ADR-0010: AI proposals never self-promote; `AUTO_APPLY_RULE` fires only for `created_by='user'` rules at ≥0.95
- ADR-0018: gate criteria enforce context-ripe checks in the trickle; bulk is user-triggered only; no always-on aggressive schedule
- ADR-0006: bulk classify runs as a background job via `app.state.job_runner.submit`

## Current state


`build_classify_context` in `src/lamella/ai/classify.py` is the orchestrator. It assembles a 10-tuple:

1. `TxnForClassify`: date, amount, payee, narration, card_account, txn_hash, lamella_txn_id
2. `similar: list[SimilarTxn]`, vector search (Phase H, opt-in) or 180-day substring fallback
3. `valid_accounts: list[str]`, whitelist by FIXME root and entity
4. `entity: str | None`, resolved via `resolve_entity_for_account` (DB registry) or string-split heuristic
5. `active_notes: list`, `NoteService.notes_active_on` filtered by date, entity, card
6. `card_suspicion: CardBindingSuspicion | None`, merchant histogram via `suspicious_card_binding`
7. `accounts_by_entity: dict | None`, cross-entity whitelist, populated when a card-override note or prior user rejection points at a different entity
8. `receipt: ReceiptContext | None`, `fetch_receipt_context` skipped during Paperless sync
9. `mileage_entries: list[MileageContextEntry]`, proximity ±3 days via `mileage_context_for_txn`
10. `log_density: list[VehicleLogDensity]`, per-vehicle log completeness via `vehicle_log_density`

Missing from the 10-tuple: account descriptions (`account_classify_context` table), entity free-form context (`entities.classify_context`), and active projects (`projects` + `project_txns`). These are documented in CLAUDE.md's signal table but not yet pulled by `build_classify_context`.

`propose_account` in `classify.py` calls OpenRouter with a Jinja2 prompt (`prompts/classify_txn.j2`). Two-agent cascade: primary model (Haiku) runs first; if confidence is below `fallback_threshold` AND a `fallback_model` (Opus) is configured, a second call runs. `escalated_from` is set on the returned `AIProposal` for audit.

`ConfidenceGate.decide` in `src/lamella/ai/gating.py` routes the proposal: auto-apply (user rule, ≥0.95 confidence, not intercompany, not Income target) → `AUTO_APPLY_RULE`; ≥0.70 confidence suggestion → `REVIEW_WITH_SUGGESTION`; below threshold → `REVIEW_FIXME`.

**Trickle path** (`src/lamella/ai/trickle_classify.py`): runs twice/day, capped at 25 AI calls per run. Sub-tier 1 (pattern-from-neighbors): ≥3 vector neighbors at ≥0.85 similarity agreeing on one target → no AI call, direct rewrite. Sub-tier 2 (AI gate): fires only for rows with a linked receipt, OR an active project on the txn date, OR ≥2 neighbors at the agreement similarity.

**Bulk path** (`src/lamella/ai/bulk_classify.py`): user-triggered background job via `/search/bulk-apply`. Iterates all FIXME txns, calls `build_classify_context` + `_classify_one` per txn, emits Success/Failure/Error events through `JobContext`. Cancellable.

Income targets (`Income:*`) are hard-blocked from auto-apply in `_targets_income`. Intercompany flag forces `REVIEW_WITH_SUGGESTION` regardless of confidence.

### Post-v0.3.1 sign-aware behavior

Deposits / money-in rows now bypass AI inference entirely. The `/api/txn/{ref}/ask-ai` worker (`src/lamella/web/routes/api_txn.py`, commit 7e79922) detects deposit-shaped rows before calling the AI service, staged: `row.amount > 0`; ledger: FIXME-leg amount `< 0` (positive bank-side leg). Detected rows return `ai_skip_reason="deposit"` and the modal renders a "money in, pick `Income:{Entity}:*` or recognize as a transfer" message; Reject-and-retry is hidden because retry hits the same skip. Token usage on deposits drops to zero.

For non-deposit cases that still ingest, sign-aware routing now extends through the whole pipeline:

- `_fixme_for_entity(entity, amount)` in bank-sync ingest routes positive amounts to `Income:{Entity}:FIXME` instead of `Expenses:{Entity}:FIXME` (commit e70d3c6). `amount=None` callers keep legacy Expenses routing for back-compat.
- `_maybe_ai_classify` picks the whitelist root from the signed amount and calls `valid_accounts_by_root` instead of the Expenses-only `valid_expense_accounts` (commit 29caa3e), so the prompt's `fixme_account`, the whitelist, and the per-root prompt branch agree.
- `classify.py` overrides `fixme_root` to `Income` when the FIXME-leg amount is negative, catching pre-fix rows that were staged with `Expenses:{Entity}:FIXME` placeholders (commit 992faf9).
- Cross-entity widening: when memo / rejection_reason mentions another entity (account-path regex or word-boundary slug match against the entity registry), `_maybe_ai_classify` calls `valid_accounts_by_root(entity=None)` so the AI can legitimately propose another entity's accounts and the gate routes the result to intercompany review per ADR-0009/G3 (commit 704f9dd).

Refund detection (`src/lamella/features/bank_sync/refund_detect.py`, commits dea9c81 / 836860e / c625f93 / 4ad3f26) integrates into the deposit-skip modal: `find_refund_candidates` scores recently-classified expense txns (merchant 0.40 + amount-5% 0.30 / amount-20% 0.10 + date-window 0.20 + same-account 0.10, threshold 0.50, top 5) and renders one-click buttons that post `target_account + refund_of_txn_id` to `/api/txn/{ref}/classify`. The classify writers stamp `lamella-refund-of: "<original-lamella-txn-id>"` on the resulting txn (staged via `PendingEntry.refund_of_txn_id`; ledger via `OverrideWriter.append(extra_meta=...)`). `txn_detail` walks ledger entries once per render to resolve both directions ("Refund of" / "Refunded by"), meta on disk is the source of truth, no SQLite cache.

The Accept button is suppressed in `_ask_ai_result.html` when `proposal.confidence == 'low'`; the modal shows a banner pointing the user at Reject-retry or Pick-myself instead (commit 992faf9).

The classify prompt's mileage section flips its default posture for 0-mile log entries from positive attribution to RULE OUT (commit fa71cde). Case 1 ("0 mi → fuel/maintenance/parts") now requires explicit "service came to the vehicle" notes (mobile mechanic, gas can, in-driveway, garage); without those notes the prompt rules out drive-to merchants, carwash, drive-through, gas pumping, parking, drive-up tolls, restaurants, errand-shaped purchases.

The classify modal pipeline now renders an in-place "Classified" tile via OOB swap instead of refreshing the page (commits 949ea3e / 89fc48d / e4ba4c7). Each `rsg-group` carries `data-rsg-staged-ids="<sp-separated id list>"` for OOB targeting; the response is `_classify_group_done.html` with `hx-swap-oob="outerHTML:[data-rsg-staged-ids~='<id>']"` plus a `#toast-area` confirmation. The custom `htmx.min.js` shim gained an `htmx.ajax(method, url, opts)` implementation and extended `processOob` to support upstream-htmx `<mode>:<selector>` forms.

### Compliant ADRs
- ADR-0009: card binding is the starting hypothesis; `entity_from_card` / `resolve_entity_for_account` sets it, override signals (notes, prior rejections, suspicion) can widen it
- ADR-0010: AI proposals never self-promote; `AUTO_APPLY_RULE` fires only for `created_by='user'` rules at ≥0.95
- ADR-0018: gate criteria enforce context-ripe checks in the trickle; bulk is user-triggered only; no always-on aggressive schedule
- ADR-0006: bulk classify runs as a background job via `app.state.job_runner.submit`

### Known violations
- ADR-0018: `account_descriptions`, `entity_context`, and `active_projects` signals documented in CLAUDE.md are absent from `build_classify_context` (medium, the prompt has slots for them via `propose_account` kwargs but the orchestrator does not populate them)
- ADR-0019: `bulk_classify._classify_one` falls back to `txn_hash` when `lamella_txn_id` is absent, correct, but several callers still pass raw `txn.meta.get("lamella-simplefin-id")` directly rather than using `identity.find_source_reference` (low, confined to the bulk path's logging)

## Known gaps

- ADR-0018: `account_descriptions`, `entity_context`, and `active_projects` signals documented in CLAUDE.md are absent from `build_classify_context` (medium, the prompt has slots for them via `propose_account` kwargs but the orchestrator does not populate them)
- ADR-0019: `bulk_classify._classify_one` falls back to `txn_hash` when `lamella_txn_id` is absent, correct, but several callers still pass raw `txn.meta.get("lamella-simplefin-id")` directly rather than using `identity.find_source_reference` (low, confined to the bulk path's logging)

## Remaining tasks

1. Add `account_descriptions` pull from `account_classify_context` table into `build_classify_context` (CLAUDE.md signal gap)
2. Add `entity_context` pull from `entities` table into `build_classify_context`
3. Add `active_projects_for_txn` query into `build_classify_context`
4. Audit callers that bypass `identity.find_source_reference` in bulk_classify logging paths
5. Add integration test covering the two-agent cascade path (escalated_from is set, fallback answer wins)

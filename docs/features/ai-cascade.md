---
audience: agents
read-cost-target: 100 lines
authority: informative
status: Active Development
cross-refs: docs/adr/0009-card-binding-as-hypothesis.md, docs/adr/0010-rules-are-signals-not-commands.md, docs/adr/0018-classification-intentionally-slow.md, docs/adr/0020-adapter-pattern-for-external-data-sources.md, docs/specs/AI-CLASSIFICATION.md
last-derived-from-code: 2026-04-26
---
# Ai Cascade

## Summary

Two-agent OpenRouter cascade (Haiku primary, Opus fallback) plus shared client / cost telemetry / prompt registry.

## Owned routes

| Method | Path | Handler | File:line |
|---|---|---|---|
| GET | `/ai/audit` | `audit_page` | `src/lamella/web/routes/ai.py:29` |
| GET | `/ai/cost` | `cost_page` | `src/lamella/web/routes/ai.py:73` |
| GET | `/ai/decisions/{decision_id}` | `decision_detail` | `src/lamella/web/routes/ai.py:484` |
| POST | `/ai/retry/{decision_id}` | `retry_decision` | `src/lamella/web/routes/ai.py:586` |
| GET | `/ai/suggestions` | `suggestions_page` | `src/lamella/web/routes/ai.py:89` |
| POST | `/ai/suggestions/{decision_id}/reject` | `suggestion_reject` | `src/lamella/web/routes/ai.py:341` |

## Owned templates

- `src/lamella/web/templates/ai_audit.html`
- `src/lamella/web/templates/ai_cost.html`
- `src/lamella/web/templates/ai_decision_detail.html`
- `src/lamella/web/templates/ai_suggestions.html`
- `src/lamella/web/templates/partials/ai_cost_card.html`
- `src/lamella/web/templates/partials/ai_suggestion.html`

## Owned source files

- `src/lamella/adapters/openrouter/client.py`
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

- `tests/test_ai_audit_link_resolution.py`
- `tests/test_ai_classify.py`
- `tests/test_ai_classify_lineage.py`
- `tests/test_ai_decisions_log.py`
- `tests/test_ai_enricher.py`
- `tests/test_ai_gating.py`
- `tests/test_ai_note_parse.py`
- `tests/test_ai_receipt_match.py`
- `tests/test_ask_ai_route.py`
- `tests/test_openrouter_client.py`
- `tests/test_txn_ask_ai_route.py`

## ADR compliance

- ADR-0009: `ConfidenceGate.decide()` hard-gates intercompany flag and income targets; card suspicion sets flag via `suspicious_card_binding()`
- ADR-0010: AI proposals never auto-apply; `AUTO_APPLY_RULE` requires `created_by == "user"` at ≥ 0.95; AI-created rules only suggest
- ADR-0018: no ingest-time AI classify post-C1; classification runs on-demand or via trickle scheduler with context-completeness checks

## Current state


`OpenRouterClient` (`client.py`) wraps OpenRouter's OpenAI-compatible chat endpoint. It calls `POST /chat/completions` with `response_format.type = "json_schema"`. Retry logic via tenacity (3 attempts, exponential backoff) handles 5xx and upstream errors embedded in 200 responses. A second-chance repair pass sends the original body plus a correction system message when schema validation fails.

Each `chat()` call computes a SHA-256 prompt hash; `DecisionsLog.find_cache_hit()` returns a cached result within `cache_ttl_hours` (default 24h) to avoid duplicate paid calls on re-ingest.

`ConfidenceGate` (`gating.py`) is pure routing logic:
- `decide(rule, ai)`: user-created rule at ≥ 0.95 → `AUTO_APPLY_RULE`. Income targets always force `REVIEW_WITH_SUGGESTION` regardless of confidence (tax-decision hard gate). Intercompany flag (`ai.intercompany_flag=True`) forces review regardless of confidence (ADR-0009).
- AI proposals NEVER produce `AUTO_APPLY_AI` post-workstream-A. They land as `REVIEW_WITH_SUGGESTION` at ≥ 0.70, or `REVIEW_FIXME` below that.
- `decide_match(ranking, candidates_present)`: receipt matching gate; `AUTO_LINK` requires ≥ 0.90 primary and < 0.60 runner-up.

`AIService` (`service.py`) is the facade for routes and background jobs: `new_client()` returns None when disabled or monthly spend cap is reached; `model_for(decision_type)` resolves per-decision-type overrides from `app_settings`; `fallback_model_for()` resolves the cascade escalation model.

Two-model cascade lives in `ingest._maybe_ai_classify()` and `classify.propose_account()`: primary call → if `primary.confidence < fallback_threshold` (default configurable) → second call with fallback model; `escalated_from` field on `AIProposal` carries the primary model id for audit.

`classify.py` assembles `TxnForClassify` and calls `build_classify_context()` → gathers all context signals → renders `classify_txn.j2` → calls `propose_account()`. Card-suspicion check (`suspicious_card_binding()`) sets `intercompany_flag=True` on the proposal when the merchant's entity history diverges from the card entity (ADR-0009).

Decision types supported: `classify_txn`, `match_receipt`, `parse_note`, `rule_promotion`, `column_map`, `receipt_verify`, `receipt_enrich`, `draft_description`, `summarize_day`, `audit_day`.

### Compliant ADRs
- ADR-0009: `ConfidenceGate.decide()` hard-gates intercompany flag and income targets; card suspicion sets flag via `suspicious_card_binding()`
- ADR-0010: AI proposals never auto-apply; `AUTO_APPLY_RULE` requires `created_by == "user"` at ≥ 0.95; AI-created rules only suggest
- ADR-0018: no ingest-time AI classify post-C1; classification runs on-demand or via trickle scheduler with context-completeness checks

### Known violations
- ADR-0020: `OpenRouterClient` is the only implementation; no `LLMPort` protocol separates the OpenRouter HTTP dependency from callers (medium-high)

## Known gaps

- ADR-0020: `OpenRouterClient` is the only implementation; no `LLMPort` protocol separates the OpenRouter HTTP dependency from callers (medium-high)

## Remaining tasks


1. [ ] Move OpenRouter behind an adapter port: define `LLMPort` protocol; `OpenRouterClient` becomes one implementation; test doubles no longer need `httpx` mocking (ADR-0020, medium-high).
2. [ ] `_maybe_ai_classify` in `ingest.py` is dead code post-workstream-C1: either wire to an explicit `ingest_time_ai_enabled` setting or delete; dead code misleads future readers (medium).
3. [ ] `trickle_classify.py` scheduling and context-completeness gate need a doc reference to `docs/specs/AI-CLASSIFICATION.md` criteria, currently the gate logic is not tested against the spec's four exception criteria (medium).
4. [ ] `CACHED_MODEL_SENTINEL` sentinel in logged decisions makes cost summaries double-count when summarizing by model, add a filter in `cost_summary()` (low).

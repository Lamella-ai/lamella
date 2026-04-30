---
audience: agents
read-cost-target: 100 lines
authority: informative
status: Active Development
cross-refs: docs/adr/0010-rules-are-signals-not-commands.md, docs/adr/0001-ledger-as-source-of-truth.md, docs/adr/0004-bean-check-after-every-write.md
last-derived-from-code: 2026-04-26
---
# Rules

## Summary

User-authored classification rules; engine evaluates with tiebreak ordering; teach UI for rule capture.

## Owned routes

| Method | Path | Handler | File:line |
|---|---|---|---|
| GET | `/rules` | `list_rules` | `src/lamella/web/routes/rules.py:34` |
| POST | `/rules` | `create_rule` | `src/lamella/web/routes/rules.py:119` |
| POST | `/rules/promote-mined` | `promote_mined` | `src/lamella/web/routes/rules.py:70` |
| DELETE | `/rules/{rule_id}` | `delete_rule` | `src/lamella/web/routes/rules.py:225` |
| POST | `/rules/{rule_id}/delete` | `delete_rule_post` | `src/lamella/web/routes/rules.py:241` |
| GET | `/teach` | `teach_page` | `src/lamella/web/routes/teach.py:27` |
| POST | `/teach` | `teach_rule` | `src/lamella/web/routes/teach.py:38` |

## Owned templates

- `src/lamella/web/templates/partials/rule_row.html`
- `src/lamella/web/templates/rules.html`
- `src/lamella/web/templates/teach.html`

## Owned source files

- `src/lamella/features/rules/engine.py`
- `src/lamella/features/rules/models.py`
- `src/lamella/features/rules/overrides.py`
- `src/lamella/features/rules/rule_writer.py`
- `src/lamella/features/rules/scanner.py`
- `src/lamella/features/rules/service.py`

## Owned tests

- `tests/test_rule_engine_tiebreak_snapshot.py`
- `tests/test_rules_ai_learn.py`
- `tests/test_rules_engine.py`
- `tests/test_rules_learn.py`
- `tests/test_simplefin_ingest_rules.py`
- `tests/test_step2_classification_rules.py`

## ADR compliance

- ADR-0010: `created_by='user'` + confidence ≥ 0.95 is the only auto-apply path; AI rules enter at 0.85 and require 3 acceptances before promotion
- ADR-0001: `connector_rules.bean` carries the canonical rule state; `hit_count` / `confidence` mutations in SQLite are explicitly documented as cache in `rule_writer.py` header
- ADR-0004: `append_rule` calls `append_custom_directive` which invokes `run_bean_check`; reverts on new errors

## Current state


Rules live in `classification_rules` (SQLite), written to `connector_rules.bean` for ledger persistence. `RuleRow` (`src/lamella/rules/models.py`) carries: `pattern_type`, `pattern_value`, `card_account`, `target_account`, `confidence`, `hit_count`, `last_used`, `created_by`.

Four pattern types (`src/lamella/rules/models.py::PatternType`): `merchant_exact`, `merchant_contains`, `regex`, `amount_range`. The engine (`src/lamella/rules/engine.py`) ranks matches in six tiers: card-scoped exact (1) > global exact (2) > card-scoped contains (3) > global contains (4) > regex (5) > amount_range (6). A card-scoped rule that does not match the txn's card is disqualified entirely.

`RuleService.learn_from_decision` (`src/lamella/rules/service.py`) is the write path from review decisions. When the user target matches an existing rule → `bump` + possible promotion. When it contradicts an AI rule → `demote_on_contradiction` (step 0.10, floor 0.30) + new user rule at confidence=1.0. AI-accepted proposals insert at `AI_INITIAL_CONFIDENCE` (0.85) with `created_by='ai'`; after `AI_PROMOTION_THRESHOLD` (3) user acceptances, `promote_ai_if_eligible` flips the rule to `created_by='user'` at confidence=1.0.

`hit_count`, `last_used`, `confidence` mutations are NOT written back to `connector_rules.bean`, they are cache. The rule's identity (pattern + target + created_by + added_at) is what goes to the ledger; `rule_writer.append_rule` and `append_rule_revoke` are the only ledger write functions. Deletes append a revoke directive rather than removing the existing one (append-only).

`OverrideWriter` (`src/lamella/rules/overrides.py`) handles the override-block layer (`connector_overrides.bean`). It is not the default path for FIXME corrections, in-place rewrite is. Override writer is retained for multi-leg splits, loan funding, and audit-driven reclassifications.

`FixmeScanner` (`src/lamella/rules/scanner.py`) walks ledger entries and finds FIXME postings for the trickle/bulk classify inputs. It is the supply side of the classify pipeline, not the rule-matching side.

### Compliant ADRs
- ADR-0010: `created_by='user'` + confidence ≥ 0.95 is the only auto-apply path; AI rules enter at 0.85 and require 3 acceptances before promotion
- ADR-0001: `connector_rules.bean` carries the canonical rule state; `hit_count` / `confidence` mutations in SQLite are explicitly documented as cache in `rule_writer.py` header
- ADR-0004: `append_rule` calls `append_custom_directive` which invokes `run_bean_check`; reverts on new errors

### Known violations
- ADR-0001: `confidence` after AI promotion is not written back to `connector_rules.bean` (medium, the ledger carries the original AI confidence, not the promoted 1.0; reconstruct would rebuild the rule at AI confidence, not user tier). The `promote_ai_if_eligible` path only updates SQLite.
- ADR-0010: `scanner.py` is named "scanner" but is load-bearing for classify supply, not rules; naming creates confusion for new contributors (low, documentation gap only)

## Known gaps

- ADR-0001: `confidence` after AI promotion is not written back to `connector_rules.bean` (medium, the ledger carries the original AI confidence, not the promoted 1.0; reconstruct would rebuild the rule at AI confidence, not user tier). The `promote_ai_if_eligible` path only updates SQLite.
- ADR-0010: `scanner.py` is named "scanner" but is load-bearing for classify supply, not rules; naming creates confusion for new contributors (low, documentation gap only)

## Remaining tasks

1. Write promoted confidence (1.0) and `created_by='user'` back to `connector_rules.bean` on AI promotion so reconstruct sees the correct tier
2. Add a revoke-and-reappend path to `rule_writer.py` that can update an existing rule directive (needed for promotion writeback above)
3. Rename or add a module-level docstring to `scanner.py` clarifying it is the FIXME supply scanner, not a rule scanner
4. Add `/rules` UI datalist for `card_account` field backed by opened accounts (ADR-0011 gap)
5. Expose `demote_on_contradiction` events in the `/rules` list so the user can see which AI rules have been penalized

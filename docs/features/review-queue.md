---
audience: agents
read-cost-target: 100 lines
authority: informative
status: Active Development
cross-refs: docs/adr/0005-htmx-endpoints-return-partials.md, docs/adr/0011-autocomplete-everywhere.md, docs/adr/0001-ledger-as-source-of-truth.md
last-derived-from-code: 2026-04-29
---
# Review Queue

## Summary

Human review surface for AI suggestions, FIXME items, paired transfers, dismissal management.

## Owned routes

| Method | Path | Handler | File:line |
|---|---|---|---|
| GET | `/inbox` | `inbox_page` | `src/lamella/web/routes/inbox.py:43` |
| GET | `/review` | `review_page` | `src/lamella/web/routes/review.py:253` |
| GET | `/review/ignored` | `staged_review_ignored` | `src/lamella/web/routes/staging_review.py:429` |
| POST | `/review/rescan` | `rescan` | `src/lamella/web/routes/review.py:385` |
| GET | `/review/staged` | `staged_review_page` | `src/lamella/web/routes/staging_review.py:372` |
| POST | `/review/staged/ask-ai-modal` | `staged_review_ask_ai_modal` | `src/lamella/web/routes/staging_review.py:1079` |
| POST | `/review/staged/classify` | `staged_review_classify` | `src/lamella/web/routes/staging_review.py:490` |
| POST | `/review/staged/classify-group` | `staged_review_classify_group` | `src/lamella/web/routes/staging_review.py:837` |
| POST | `/review/staged/dismiss` | `staged_review_dismiss` | `src/lamella/web/routes/staging_review.py:382` |
| POST | `/review/staged/restore` | `staged_review_restore` | `src/lamella/web/routes/staging_review.py:402` |
| POST | `/review/{item_id}/mark_transfer` | `mark_as_transfer` | `src/lamella/web/routes/review.py:532` |
| POST | `/review/{item_id}/mark_transfer_to` | `mark_transfer_to_account` | `src/lamella/web/routes/review.py:650` |
| POST | `/review/{item_id}/resolve` | `resolve_item` | `src/lamella/web/routes/review.py:396` |

## Owned templates

- `src/lamella/web/templates/_components/cards.html`
- `src/lamella/web/templates/ai_suggestions.html`
- `src/lamella/web/templates/card.html`
- `src/lamella/web/templates/card_empty.html`
- `src/lamella/web/templates/card_staged.html`
- `src/lamella/web/templates/inbox.html`
- `src/lamella/web/templates/partials/review_item.html`
- `src/lamella/web/templates/review.html`

## Owned source files

- `src/lamella/features/review_queue/grouping.py`
- `src/lamella/features/review_queue/pair_detector.py`
- `src/lamella/features/review_queue/service.py`
- `src/lamella/features/review_queue/suggestions.py`

## Owned tests

- `tests/test_bulk_apply.py`
- `tests/test_card_swap_smoke.py`
- `tests/test_inbox.py`
- `tests/test_review_grouping.py`
- `tests/test_review_partial_swap.py`
- `tests/test_review_queue.py`
- `tests/test_suggestion_cards.py`

## ADR compliance

- ADR-0005: HTMX: `/review/staged` returns partials when `HX-Request` is set; classify/dismiss actions use `_htmx.redirect`
- ADR-0011: Target-account input on the classify form uses `<datalist>` backed by opened accounts
- ADR-0001: `review_queue` is cache; no user-configured state lives only here; `source_ref` traces back to a ledger entry

## Current state


Queue rows live in `review_queue` (SQLite). Six kinds are valid: `fixme`, `receipt_unmatched`, `ambiguous_match`, `note_orphan`, `simplefin_unmapped_account`, `import_categorization`. `ReviewService` (`src/lamella/review/service.py`) is the sole write path; it enforces the kind allowlist and handles the `_post_resolve` side-effect for `import_categorization` rows (clears `categorizations.needs_review`).

Grouping (`src/lamella/review/grouping.py`) clusters staged rows by `(normalized_payee_stem, source_account_key)`. The stem is lowercased, punctuation-stripped, truncated to 40 chars. Groups of size 1 are valid, they render identically, just without a group action. Prototype is the most-recent row (upstream sorts `posting_date DESC`).

The main render path is `GET /review/staged` in `src/lamella/routes/staging_review.py`. It calls `list_pending_items` from `lamella.staging`, groups results via `group_staged_rows`, and builds a context dict. `POST /review/staged/classify` accepts either an explicit `target_account` or `accept_proposed=1` (uses the row's `staged_decision`); it writes a clean CLASSIFIED entry and flips the staged row to `promoted`. `POST /review/staged/dismiss` marks a row dismissed (terminal).

Transfer detection (`src/lamella/review/pair_detector.py`) is a legacy path for pre-B2 FIXME entries still in the ledger. New data goes through `staging/matcher.py`. `pair_detector.py` is explicitly flagged for retirement after a full reboot pass.

Auto-apply resolved rows still get a `ReviewService.enqueue_resolved` row so every ledger change has a paper trail.

### Post-v0.3.1 changes (2026-04-29, 6cced2a..8575dab)

- **Sign-aware /inbox amount display** (be6514e), group rows now render the prototype amount with `txns-amount--{in,out,flat}` flow classes derived from sign rather than treating every staged row as an outflow. Deposit-shaped groups (positive amount) additionally suppress stale "AI Expenses" proposals that were stamped pre-sign-aware.
- **Deposit-skip modal** (7e79922), `/review/staged/ask-ai-modal` short-circuits AI classification when the prototype amount is positive and renders a deposit-specific manual Income picker instead of an AI suggestion.
- **Refund candidate buttons in deposit modal** (c625f93, 836860e), the deposit-skip modal surfaces refund candidates wired through `_ask_ai_result.html`; clicking promotes the staged row and stamps `lamella-refund-of` linking the deposit to the original outflow.
- **Modal classify chain on /inbox** (949ea3e, 89fc48d, e4ba4c7), Accept and "Pick myself" inside the AI modal previously failed silently on `/inbox`. Fix landed in three steps: htmx.ajax shim so the modal's form submit reaches the server, OOB swap targeting `data-rsg-staged-ids` to replace the staged tile in-place with a "Classified" confirmation, and a toast confirmation post-classify.
- **AI confidence + sign-aware FIXME root** (992faf9), Accept button is now hidden when AI confidence is low; FIXME root account is chosen sign-aware (Income:* for positive, Expenses:* for negative) instead of always defaulting to Expenses.

### Compliant ADRs
- ADR-0005: HTMX: `/review/staged` returns partials when `HX-Request` is set; classify/dismiss actions use `_htmx.redirect`
- ADR-0011: Target-account input on the classify form uses `<datalist>` backed by opened accounts
- ADR-0001: `review_queue` is cache; no user-configured state lives only here; `source_ref` traces back to a ledger entry

### Known violations
- ADR-0005: `_redirect_to_list` returns a vanilla `RedirectResponse` without the `_htmx.redirect` helper; HTMX swap consumers get a 303 that follows to the full page (moderate, causes layout nesting when the form's `hx-target` is inside the page body)
- ADR-0011: The group-action "apply to all" form does not render a `<datalist>` for target selection in all code paths (minor)

## Known gaps

- ADR-0005: `_redirect_to_list` returns a vanilla `RedirectResponse` without the `_htmx.redirect` helper; HTMX swap consumers get a 303 that follows to the full page (moderate, causes layout nesting when the form's `hx-target` is inside the page body)
- ADR-0011: The group-action "apply to all" form does not render a `<datalist>` for target selection in all code paths (minor)

## Remaining tasks

1. Replace all `RedirectResponse` calls in `staging_review.py` with `_htmx.redirect` from `routes/_htmx.py`
2. Retire `src/lamella/review/pair_detector.py` after confirming no pre-B2 FIXMEs remain in the live ledger
3. Add `<datalist>` to the group-action form path
4. Add reconstruct coverage for `review_queue` kinds that aren't yet ledger-backed (currently `fixme` rows are fully cache; confirm no state loss on DB wipe)
5. Wire `note_orphan` kind to a visible UI band on `/review/staged`

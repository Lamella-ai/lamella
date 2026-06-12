---
audience: agents
read-cost-target: 100 lines
authority: informative
status: Active Development
cross-refs: docs/adr/0001-ledger-as-source-of-truth.md, docs/adr/0005-htmx-endpoints-return-partials.md, docs/adr/0011-autocomplete-everywhere.md
last-derived-from-code: 2026-04-29
---
# Dashboard

## Summary

Landing page after setup: per-account balances, recent activity, FIXME backlog, AI/job status tiles.

## Owned routes

| Method | Path | Handler | File:line |
|---|---|---|---|
| GET | `/` | `dashboard` | `src/lamella/web/routes/dashboard.py:142` |
| POST | `/dashboard/welcome/dismiss` | `dismiss_welcome` | `src/lamella/web/routes/dashboard.py:257` |

## Owned templates

- `src/lamella/web/templates/dashboard.html`

## Owned source files

- `src/lamella/features/dashboard/balances/reader.py`
- `src/lamella/features/dashboard/balances/service.py`
- `src/lamella/features/dashboard/balances/writer.py`
- `src/lamella/features/dashboard/service.py`

## Owned tests

_No tests own this feature._

## ADR compliance

- ADR-0001: Balance figures come from ledger entries, not a separate balance
  cache. `entity_balances()` walks entries on each request.
- ADR-0005 (partial): `POST /dashboard/welcome/dismiss` checks `hx-request`
  and returns `HTMLResponse("")` for HTMX vs. `RedirectResponse` for vanilla.
  `GET /` has no HTMX variant, it always returns the full page, which is
  correct because it is a navigation target, not a swap target.
- ADR-0006 (not applicable here): No long-running operations on the dashboard
  page itself. SimpleFIN fetch and other heavy ops are submitted from other
  routes.

## Recent changes (post-v0.3.1)

- `59a386b` (`fix(dashboard): drop duplicate KPI tiles + use standard accounting
  sign placement`): the period KPI grid no longer duplicates net worth / net
  income tiles between sections. Negative amounts render as `-$X` (standard
  accounting convention) instead of `$-X`.
- `a463c1d` (`fix(ui): sign-aware |money filter + audit/_card_pane drift fixes`):
  the global Jinja `|money` filter now wraps every value in
  `<span class="money money--{pos|neg|zero} num">`, so dashboard KPI tiles,
  monthly P&L chart axes, and budget rows all gain refund-vs-charge color
  distinction without per-template churn. Same commit flipped Net worth + Net
  income tiles from `signed=false` to `signed=true` so positive nets show `+`
  and negative nets show `−` (the existing `tone='ok'/'err'` already drove the
  green/red color; now the prefix matches).

## Current state


### Compliant ADRs
- ADR-0001: Balance figures come from ledger entries, not a separate balance
  cache. `entity_balances()` walks entries on each request.
- ADR-0005 (partial): `POST /dashboard/welcome/dismiss` checks `hx-request`
  and returns `HTMLResponse("")` for HTMX vs. `RedirectResponse` for vanilla.
  `GET /` has no HTMX variant, it always returns the full page, which is
  correct because it is a navigation target, not a swap target.
- ADR-0006 (not applicable here): No long-running operations on the dashboard
  page itself. SimpleFIN fetch and other heavy ops are submitted from other
  routes.

### Known violations
- ADR-0005: `GET /` does not check `HX-Request`. If navigated to via an HTMX
  push, it returns the full shell. In practice the dashboard is always a full
  navigation, but the pattern diverges from the HTMX partial rule.
- Real-time vs. cached: `money_groups` has a `business_cache` layer for
  per-entity KPI widgets warmed by an APScheduler job every 10 minutes.
  The main dashboard balance cards (`entity_balances`) are NOT cached, they
  re-walk the ledger on every request. For large ledgers this adds latency.
- `suggestion_cards` silently swallows exceptions (`except Exception: pass`)
  to prevent bad data from killing the dashboard. The failure path has no
  telemetry, a broken suggestions module becomes invisible.

## Known gaps

- ADR-0005: `GET /` does not check `HX-Request`. If navigated to via an HTMX
  push, it returns the full shell. In practice the dashboard is always a full
  navigation, but the pattern diverges from the HTMX partial rule.
- Real-time vs. cached: `money_groups` has a `business_cache` layer for
  per-entity KPI widgets warmed by an APScheduler job every 10 minutes.
  The main dashboard balance cards (`entity_balances`) are NOT cached, they
  re-walk the ledger on every request. For large ledgers this adds latency.
- `suggestion_cards` silently swallows exceptions (`except Exception: pass`)
  to prevent bad data from killing the dashboard. The failure path has no
  telemetry, a broken suggestions module becomes invisible.

## Remaining tasks

- Add a partial for the "next up" card region so HTMX can refresh just the
  categorize-count tile without a full reload after the user acts on a card.
- Cache `entity_balances` in the `business_cache` layer alongside the
  per-entity KPI widgets (currently only `money_groups` is cached).
- Add structured logging on `suggestion_cards` exception path so failures
  are visible in logs.
- First-run welcome panel auto-dismiss threshold (5 txns or 7 days) is
  hardcoded; move to a setting or named constant.

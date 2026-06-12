---
audience: agents
read-cost-target: 60 lines
authority: normative
cross-refs: docs/core/PROJECT_CHARTER.md
---

# SUCCESS_CRITERIA

v1 finish line. When all of these are true, v1 is done.

## User-visible outcomes (must all be true)

| Outcome | Measurable signal | Status |
|---|---|---|
| End-of-year books are tax-ready without manual cleanup | Zero `Expenses:FIXME` postings; zero unmatched receipts above `receipt_required_threshold_usd`; zero unclassified SimpleFIN entries in `simplefin_transactions.bean` | partial |
| One-click receipt lookup from any transaction | Every transaction detail page shows either a linked Paperless document or an explicit "no receipt" state; neither state is ambiguous | done |
| AI explains every classification decision | Each AI-classified transaction links to an `ai_decisions` row accessible from the transaction detail page | done |
| Classification improves from user corrections | User-corrected transactions carry `user_corrected=True` in `ai_decisions`; vector index signature increments on each correction | done |
| Books survive a database wipe without data loss | `python -m lamella.transform.reconstruct --force` followed by `python -m lamella.transform.verify` exits zero; all state tables match | partial |
| Wrong-card charges produce correct four-leg entries | Intercompany review queue surfaces cross-entity card charges; user can resolve to a balanced receivable/payable pair | done |
| Multi-entity P&L is readable at a glance | `/` dashboard shows per-entity balance cards and open review count with no manual ledger query required | done |
| Tax report output is auditable | Schedule C and Schedule F PDFs link to supporting transactions; each transaction links to its receipt or dismissal | done |
| Fava is not required for any user workflow | Zero routes check for Fava availability; zero user-facing pages link to port 5003 | done |
| Long-running ops never produce a hung browser tab | Every operation touching AI, SimpleFIN, or Paperless runs as a background job with a progress modal | done |

## System-level invariants (always true, never regressed)

- `bean-check` passes after every connector write; any new error triggers a byte-identical file restore from the pre-write snapshot.
- SQLite delete + `reconstruct --force` reproduces all state rows; `verify` exits zero on state tables.
- All HTMX swap-target endpoints return partials (no `base.html` extension) when `HX-Request` header is set.
- All long-running operations (AI calls, external API calls, N>=50 item loops) submit to `app.state.job_runner` and return a job modal partial.
- Every connector-written metadata key is prefixed `lamella-`; every connector-written tag is prefixed `#lamella-`.
- No connector-written entry posts to an account not opened in the ledger or scaffolded in `connector_accounts.bean`.
- In-place rewrites snapshot before edit, sanity-check amount + account, and restore on any new `bean-check` error.
- `lamella-txn-id` (UUIDv7) is stamped on every connector-written transaction; provenance uses paired indexed source keys on the bank-side posting.

## Quality bars (numeric where possible)

- Test count: >=711 passing (current baseline); count must not decline on main.
- New routes: each new HTMX-targeted endpoint has at least one test covering the partial/full response split.
- `/adr-check`: zero violations on main.
- `/doc-drift`: zero stale cross-references on main.
- `python -m lamella.transform.verify`: exits zero on a live deploy before any v1 release tag.
- No open `Expenses:FIXME` entries remain in `simplefin_transactions.bean` at year-end cutoff.

## Out of scope for v1 success

- Multi-user or role-based access.
- Mobile native app (PWA capture is sufficient).
- Public REST API for third-party integrations.
- Real-time bank push (scheduled poll via SimpleFIN is sufficient).
- Actual-expense mileage method (standard mileage only).
- Multi-page PDF vision verify (page 1 only is acceptable).
- Per-project signal-strength scoring (AI prose triangulation is sufficient).
- Fava feature parity for bean-query (Fava remains a fallback developer tool until explicitly replaced).

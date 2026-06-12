---
audience: agents
read-cost-target: 80 lines
authority: normative
cross-refs: docs/core/SUCCESS_CRITERIA.md, docs/adr/index.md
---

# PROJECT_CHARTER

## What Lamella IS

Lamella is a self-hosted, single-user, single-container system that sits between
real-world financial activity and a Beancount plain-text ledger. It ingests bank
transactions (SimpleFIN), document images (Paperless-ngx), and user-written context
(notes, projects, mileage), assembles every available signal per transaction, and
produces correct double-entry Beancount output. The Beancount ledger is the single
source of truth; SQLite is a disposable cache rebuilt on demand.

## What Lamella IS NOT

- Not a SaaS. Lamella is single-tenant, self-hosted only.
- Not a full accounting suite; it produces `.bean` files, the user runs `beancount` tooling.
- Not a bookkeeping replacement; it classifies and corrects, a human reviews and approves.
- Not a tax-preparation service; it produces accurate books, it does not file returns.
- Not a bank; it reads from SimpleFIN Bridge, it never moves money.
- Not a Fava wrapper; Fava is a non-load-bearing dev fallback, see `CLAUDE.md §Relationship to Fava`.

## Target user

A single freelancer or side-hustler running one to four entities with mixed
personal and business transactions across shared cards. Specific traits:

- Maintains a Beancount ledger (or is willing to start one) at adoption time.
- Has 500 to 50,000 transactions per year across 2 to 6 cards.
- Runs services that produce documents: a receipt scanner (Paperless-ngx) and a bank
  data bridge (SimpleFIN).
- Has ambiguous spend (card-binding errors, multi-entity charges, project-scoped
  purchases) that pure rules cannot resolve.
- Self-hosts on a NAS or home server; comfortable with Docker and environment variables.
- Does not have a dedicated bookkeeper; reviews and corrects AI output personally.

## Non-target users

- Enterprises or teams with payroll, HR, or multi-approval workflows.
- Multi-employee operations where individual card holders vary.
- Users who want a hosted SaaS with no server administration.
- Users with no Beancount familiarity and no willingness to learn the file format.
- Users who need real-time push from their bank (SimpleFIN pulls on a schedule).

## Core integrations (today)

| Integration      | Direction      | Purpose                                              | Adapter ADRs    |
|------------------|----------------|------------------------------------------------------|-----------------|
| Beancount        | Read + Write   | Ledger parse, bean-check validation, in-place rewrite | ADR-0001, 0004 |
| Paperless-ngx    | Read + Write   | Receipt context at classify time; field corrections written back (gated on `paperless_writeback_enabled`) | ADR-0016, 0020 |
| SimpleFIN Bridge | Read           | Scheduled bank transaction pull every 6 hours; dedup + classify + write to `simplefin_transactions.bean` | ADR-0008, 0019, 0020 |
| OpenRouter       | Write (call)   | Two-model cascade (Haiku primary, Opus fallback at <0.60 confidence) for classify, rule promotion, receipt verify, draft description | ADR-0006, 0010, 0020 |

## Architectural anchors

- **Ledger as single source of truth.** SQLite is a cache; deleting it loses nothing.
  Every user-configured state row has a corresponding `custom "…"` directive in the
  ledger. See `CLAUDE.md §Non-negotiable rules` and ADR-0001.
- **In-place rewrites by default.** FIXME corrections rewrite the source `.bean` file;
  the override block is the exception, not the default. See `CLAUDE.md §In-place rewrites`
  and `src/lamella/rewrite/txn_inplace.py`.
- **`lamella-*` metadata namespace.** Every key and tag we write is prefixed `lamella-`.
  Legacy `bcg-*` keys are read transparently but never written. See `CLAUDE.md §Metadata schema`.
- **`bean-check` on every write.** Post-write validation runs against a baseline; any new
  error triggers a file restore. No exceptions. See ADR-0004.
- **Context determines classification.** Rules are signals, not commands. The AI cascade
  draws on card binding, entity descriptions, account descriptions, vector-similar history,
  active notes, projects, mileage, and Paperless receipt content. See ADR-0009, ADR-0010.

## Out of scope for v1

- Multi-tenant or multi-user access; single-operator deployment only.
- OAuth or external identity provider integration.
- Mobile application or native client.
- Real-time bank push (webhooks from financial institutions).
- Payroll, invoicing, or accounts-receivable workflows.
- Automated tax filing or IRS form generation.
- Fava feature parity as a goal; Lamella replaces Fava's useful surface,
  remaining Fava features are tracked in `docs/archive/FUTURE.md`.

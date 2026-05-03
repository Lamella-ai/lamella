# ADR-0007: Entity-First Account Hierarchy

- **Status:** Accepted
- **Date:** 2026-04-26
- **Author:** AJ Quick
- **Related:** `CLAUDE.md` ("Non-negotiable architectural rules" → "Entity-first account hierarchy"), `src/lamella/registry/discovery.py`

## Context

Lamella is a multi-entity bookkeeping tool. The same expense
category (e.g. office supplies) occurs across multiple business
entities and across personal finances. The chart of accounts must
be navigable by entity first so the user can reason about one
entity's spending in isolation.

The alternative, category-first (`Expenses:Supplies:Acme`),
produces an accounts list where every entity's accounts are
scattered across category buckets. A query for "what did Acme
spend?" requires filtering on a fragment of the account name
rather than on the top-level segment. Report subtotals at any
category node cross-contaminate entities.

A startup guard refuses to run if fewer than 20% of the ledger's
expense accounts parse as entity-first. This gate catches
misconfigured ledgers early and prevents silent incorrect
attribution during classification.

## Decision

Expense accounts MUST use the entity-first shape:
`Expenses:<Entity>:<Category>[:<Subcategory>...]`.
Category-first paths (`Expenses:<Category>:<Entity>`) MUST NOT be
written by any Lamella writer and MUST NOT be scaffolded by
`connector_accounts.bean`.

Specific obligations:

1. The AI classifier MUST only propose accounts from the ledger's
   opened account list. Never invent accounts. Entity-first is
   enforced by the list itself.
2. New connector-scaffolded accounts written to
   `connector_accounts.bean` MUST follow the entity-first pattern.
3. The startup discovery guard MUST remain enabled in production.
   `LAMELLA_SKIP_DISCOVERY_GUARD=1` is for test fixtures only.
4. Accounts outside the `Expenses:` root (Assets, Liabilities,
   Income, Equity) are not governed by this rule.

## Consequences

### Positive
- Queries for a single entity's expenses use `account ~ "^Expenses:Acme:"`,
  a simple prefix filter.
- The autocomplete picker groups accounts by entity naturally
  (alphabetic sort = entity-grouped at this prefix depth).
- Reconstruct can scaffold correct accounts without ambiguity.

### Negative / Costs
- Ledgers migrated from category-first tools require a one-time
  account rename pass before the startup guard passes.
- The 20% threshold is permissive; a ledger with a mix of old and
  new shapes passes until the old-shape accounts are removed.

### Mitigations
- The discovery guard fires at startup with a clear error message
  listing the offending accounts.
- The AI classifier never writes accounts. It only proposes from
  the opened list, so it cannot introduce a category-first account
  unless the user opened one.

## Compliance

How `/adr-check` detects violations:

- **Category-first account in connector files:** grep
  `connector_accounts.bean` and `connector_overrides.bean` for
  `Expenses:[A-Z][a-z].*:[A-Z]` where the second segment is a
  known category word (Supplies, Meals, Travel, etc.) rather than
  an entity slug.
- **Discovery guard disabled in non-test code:** grep
  `src/lamella/` (excluding `tests/`) for
  `LAMELLA_SKIP_DISCOVERY_GUARD` assignments or env reads that
  force-set it.
- **Entity-first check rate:** the discovery guard computes the
  ratio at startup; assert in integration tests that the ratio
  is ≥0.8 against the fixture ledger.

## References

- CLAUDE.md §"Entity-first account hierarchy"
- CLAUDE.md §"Non-negotiable architectural rules"
- `src/lamella/registry/discovery.py`: startup guard

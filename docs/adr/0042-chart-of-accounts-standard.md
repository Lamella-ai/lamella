# ADR-0042: Chart of Accounts Standard

- **Status:** Accepted
- **Date:** 2026-04-27
- **Author:** AJ Quick
- **Related:** [ADR-0007](0007-entity-first-account-hierarchy.md), [ADR-0031](0031-slugs-immutable-per-parent.md), [ADR-0043](0043-no-fixme-in-ledger.md), `docs/specs/CHART_OF_ACCOUNTS.md`

## Context

ADR-0007 established entity-first hierarchy for `Expenses:` accounts.
It did not govern `Assets:`, `Liabilities:`, `Income:`, or `Equity:`.
Writers for these roots have accumulated inconsistent patterns:
`Assets:Checking` (no entity), `Assets:Acme:Checking` (entity but no
institution), `Assets:Acme:BankOne:Checking` (full form). These
patterns produce different query structures for what is conceptually
the same rule: entity first, then institution, then account type.

Classification, display name lookup, cross-entity dedup, and the
startup discovery guard all need predictable account structure to
work correctly. When the structure is inconsistent, each subsystem
must handle multiple shapes, compounding maintenance cost.

Lamella is also a product. A published standard chart of accounts is
part of the value proposition for a self-employed operator setting up
their books from scratch. The standard removes the "what should I
name this account?" decision from their workflow.

The Phase 7 violation scan found 3 specific non-conformances (test
fixtures + one route scaffolding call missing the institution segment).
Production writers are largely conformant; the spec locks behavior in.

## Decision

Lamella publishes a standard chart of accounts. The authoritative
specification is `docs/specs/CHART_OF_ACCOUNTS.md`. Connector-owned
`.bean` files MUST follow this standard. Imported ledgers MAY deviate,
subject to the conformance check below.

### Structural rules

All five Beancount roots are governed. Entity-first hierarchy extends
to every root:

```
Assets:<Entity>:<Institution>:<AccountType>
Liabilities:<Entity>:<Institution>:<AccountType>
Equity:<Entity>:<SubCategory>
Income:<Entity>:<Category>[:<Subcategory>]
Expenses:<Entity>:<Category>[:<Subcategory>]
```

Institution segment is required for bank and card accounts in
`Assets:` and `Liabilities:`. Omitting the institution segment
(e.g. `Assets:Acme:Checking`) is a violation for new writes.

### Per-feature subhierarchies

Vehicles:
```
Assets:<Entity>:Vehicles:<VehicleSlug>
Expenses:<Entity>:Vehicles:<VehicleSlug>:<Category>
Equity:<Entity>:Vehicles:<VehicleSlug>:MileageDeductions
```

Properties:
```
Assets:<Entity>:Properties:<PropertySlug>
Expenses:<Entity>:Properties:<PropertySlug>:<Category>
```

Loans:
```
Liabilities:<Entity>:<Institution>:Loans:<LoanSlug>
Expenses:<Entity>:<Institution>:Loans:<LoanSlug>:Interest
```

Accounts Receivable and Payable:
```
Assets:<Entity>:AccountsReceivable:<Counterparty>
Liabilities:<Entity>:AccountsPayable:<Counterparty>
```

Cross-entity transfer clearing (entity-scoped, not global):
```
Assets:<Entity>:Transfers:InFlight
```

### Deferred

Investment accounts are deferred to a future ADR. The current
standard does not govern `Assets:<Entity>:Investments:*` or
`Income:<Entity>:Investments:*`. Beancount has well-developed
conventions for commodity lots, dividends, and gains; Lamella will
adopt those when the investment-tracking feature lands.

### Writer obligations

Connector-scaffolded accounts (via `connector_accounts.bean`) MUST
conform. Connector writers, review queue, SimpleFIN, importer,
recovery, MUST validate the account path against the standard's
structural rules before writing. A writer that would produce a
non-conforming path MUST raise rather than write.

### Conformance for imported ledgers

Imported ledgers are tolerated up to the conformance threshold
enforced by the startup discovery guard (≥20% of expense accounts
parse as entity-first, per ADR-0007). Below the threshold the app
refuses to start until the user runs an account-rename migration or
sets `LAMELLA_SKIP_DISCOVERY_GUARD=1` (test fixtures only).

The published standard is part of Lamella's value proposition.
Departures from it are user choice; the system is not required to
support every possible Beancount account shape.

## Consequences

### Positive
- Every subsystem that reads accounts can assume a known structure.
  Entity extraction from an account path is a fixed-depth split,
  not a heuristic.
- A new operator can use the published chart as their starting
  configuration. Setup wizard scaffolds accounts from it.
- Cross-entity dedup, the discovery guard, and display-name lookup
  all simplify when they can rely on the standard shape.

### Negative / Costs
- Existing ledgers with non-standard shapes require a migration
  before connector writers can touch those accounts. This is a
  one-time cost but it is real.
- The institution segment requirement is new for many accounts. A
  ledger using `Assets:Acme:Checking` must rename to
  `Assets:Acme:BankName:Checking` before Lamella writers will
  scaffold child accounts under it.
- Investment accounts are unresolved. Users with investment holdings
  get no scaffold guidance until the follow-up ADR lands.

### Mitigations
- The recovery system's account-rename finding detects non-standard
  paths and proposes corrections with a one-click rename.
- The institution-segment requirement is enforced on writes, not
  retroactively on reads. Existing non-standard accounts remain
  readable; the guard only blocks new writes under them.
- Investment deferral is explicit. The spec flags the gap so users
  know it is planned, not forgotten.

## Compliance

`/adr-check` pattern scan on connector-owned `.bean` files:
- `Assets:[A-Z][a-zA-Z]+:Checking` (missing institution segment):
  violation for newly-committed writes.
- `Expenses:[A-Z][a-zA-Z]+:[A-Z][a-zA-Z]+:[A-Z][a-zA-Z]+:.*` where
  the second segment is a known category word rather than an entity
  slug: violation.
- Any account path containing "FIXME": violation (per ADR-0043).

Connector writer unit tests MUST include a case where a
non-conforming account path triggers a raised error, not a write.

## References

- [ADR-0007](0007-entity-first-account-hierarchy.md): entity-first hierarchy (this ADR extends it to all roots)
- [ADR-0031](0031-slugs-immutable-per-parent.md): slugs in account paths are immutable
- [ADR-0043](0043-no-fixme-in-ledger.md): no FIXME postings (cross-referenced from this ADR's compliance section)
- `docs/specs/CHART_OF_ACCOUNTS.md`: full publishable specification

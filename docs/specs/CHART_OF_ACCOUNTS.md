---
audience: agents + CPAs reading the published spec
read-cost-target: 200 lines
authority: normative
cross-refs: docs/adr/0042-chart-of-accounts-standard.md, docs/adr/0007-entity-first-account-hierarchy.md, docs/adr/0031-slugs-immutable-per-parent.md, docs/adr/0043-no-fixme-in-ledger.md
---

# Chart of Accounts Standard

Lamella generates and expects a specific Beancount chart of accounts. This
document is the canonical reference. Connector-owned `.bean` files (everything
Lamella writes) follow it strictly. Imported user ledgers may deviate up to
the conformance threshold (see "Import tolerance" below).

## Why this matters

A predictable account structure is what makes automated classification, per-entity
reporting, and tax-prep export work reliably. When every expense account follows
`Expenses:<Entity>:<Category>`, a query for one entity's spending is a single
prefix filter. When accounts are named ad-hoc, the classifier guesses, reports
cross-contaminate, and tax-time exports require manual cleanup.

The standard also enforces auditability. A CPA opening the ledger for the first
time can identify which entity incurred which expense without reading transaction
narratives. Subcategory names map directly to IRS Schedule lines wherever
applicable so drag-and-drop tax preparation requires no account-to-line
translation.

## The 5 roots (Beancount-fixed)

| Root | Lamella's use |
|---|---|
| Assets | What you have: bank accounts, property basis, vehicle basis, A/R, transfers in-flight |
| Liabilities | What you owe: credit cards, loans, mortgages, A/P |
| Equity | Owner stake: capital, draws, retained earnings, mileage deduction offsets |
| Income | Money in: earnings, dividends, rental income, interest received |
| Expenses | Money out: categorized spending by entity and category |

## Hierarchy convention: entity-first throughout

Every account path follows `<Root>:<Entity>:<...>`. The entity segment is always
the second position. This makes per-entity reports a simple prefix filter and
groups accounts naturally in any alphabetic listing.

Example: `Expenses:Acme:Supplies`, `Assets:Personal:BankOne:Checking`.

The rule applies to all five roots. An account without an entity segment in
position two is non-conformant and will not be written by any Lamella connector
file. (See ADR-0007 for full reasoning and the startup discovery guard.)

## Account patterns by root

### Assets

| Pattern | Use case | Example |
|---|---|---|
| `Assets:<Entity>:<Institution>:<AccountType>` | Bank and cash accounts | `Assets:Acme:BankOne:Checking` |
| `Assets:<Entity>:Properties:<PropertySlug>` | Real-property cost basis | `Assets:Personal:Properties:NorthHouse` |
| `Assets:<Entity>:Vehicles:<VehicleSlug>` | Vehicle cost basis | `Assets:Acme:Vehicles:V2008WorkSUV` |
| `Assets:<Entity>:AccountsReceivable:<Counterparty>` | Intercompany receivables | `Assets:Acme:AccountsReceivable:Personal` |
| `Assets:<Entity>:DueFrom:<OwingEntity>` | Wrong-card intercompany receivable | `Assets:Acme:DueFrom:Personal` |
| `Assets:<Entity>:Transfers:InFlight` | Cross-entity or cross-date clearing | `Assets:Personal:Transfers:InFlight` |

### Liabilities

| Pattern | Use case | Example |
|---|---|---|
| `Liabilities:<Entity>:<Institution>:<CardSlug>` | Credit cards | `Liabilities:Acme:BankOne:Card` |
| `Liabilities:<Entity>:<Institution>:<LoanSlug>` | Loans and mortgages | `Liabilities:Personal:Wells:Mortgage2024` |
| `Liabilities:<Entity>:AccountsPayable:<Counterparty>` | Intercompany payables | `Liabilities:Personal:AccountsPayable:Acme` |
| `Liabilities:<Entity>:DueTo:<PayingEntity>` | Wrong-card intercompany payable | `Liabilities:Personal:DueTo:Acme` |

### Equity

| Pattern | Use case | Example |
|---|---|---|
| `Equity:<Entity>:OpeningBalances` | Initial balance bootstrap | `Equity:Acme:OpeningBalances` |
| `Equity:<Entity>:OwnerCapital` | Owner contributions in | `Equity:Acme:OwnerCapital` |
| `Equity:<Entity>:OwnerDraws` | Owner distributions out | `Equity:Acme:OwnerDraws` |
| `Equity:<Entity>:RetainedEarnings` | Year-end roll-up | `Equity:Acme:RetainedEarnings` |
| `Equity:<Entity>:Vehicles:<VehicleSlug>:MileageDeductions` | Standard-mileage deduction offset (per-vehicle) | `Equity:Acme:Vehicles:V2008WorkSUV:MileageDeductions` |

### Income

| Pattern | Use case | Example |
|---|---|---|
| `Income:<Entity>:<Type>` | General income | `Income:Acme:Consulting` |
| `Income:<Entity>:<Type>:<Source>` | Income with source qualifier | `Income:Personal:Salary:CompanyX` |
| `Income:<Entity>:Interest:<Institution>` | Interest received | `Income:Personal:Interest:BankOne` |
| `Income:<Entity>:Properties:<PropertySlug>:Rental` | Rental income | `Income:Personal:Properties:NorthHouse:Rental` |

### Expenses

| Pattern | Use case | Example |
|---|---|---|
| `Expenses:<Entity>:<Category>` | General spending | `Expenses:Acme:Supplies` |
| `Expenses:<Entity>:<Category>:<Subcategory>` | Detailed spending | `Expenses:Personal:Home:Maintenance` |
| `Expenses:<Entity>:Vehicles:<VehicleSlug>:<Subcategory>` | Per-vehicle expenses | `Expenses:Acme:Vehicles:V2008WorkSUV:Fuel` |
| `Expenses:<Entity>:Properties:<PropertySlug>:<Subcategory>` | Per-property expenses | `Expenses:Personal:Properties:NorthHouse:PropertyTax` |
| `Expenses:<Entity>:<Institution>:Loans:<LoanSlug>:Interest` | Loan interest | `Expenses:Personal:Wells:Loans:Mortgage2024:Interest` |
| `Expenses:<Entity>:Bank:<Institution>:Fees` | Bank service fees | `Expenses:Personal:Bank:BankOne:Fees` |

## Per-feature subtrees

### Vehicles (`Expenses:<Entity>:Vehicles:<VehicleSlug>:<Category>`)

Scaffolded on vehicle registration. Slug convention: `V<year><Make><Model>` with
all non-alphanumerics stripped (e.g. `V2009FabrikamSuv`).

Every vehicle gets all categories below. These mirror IRS Pub 463 "Actual Car
Expenses" so per-vehicle totals map directly to Schedule C Part IV.

| Subcategory | IRS / purpose |
|---|---|
| `Fuel` | Gas / fuel (Pub 463, Sched C Part IV line 29) |
| `Oil` | Oil changes and lubricants (Pub 463) |
| `Tires` | Tire purchases and installation (Pub 463) |
| `Maintenance` | Routine care: wiper blades, fluids, batteries, alignments |
| `Repairs` | Major fixes: transmission, engine, brakes, body work (Pub 463) |
| `Insurance` | Auto insurance premiums (Pub 463) |
| `Registration` | DMV registration and renewal fees (Pub 463) |
| `Licenses` | Commercial driver / trade / operator licenses (Pub 463) |
| `OwnershipTax` | Personal-property tax on vehicle (deductible on Schedule A) |
| `Tolls` | Toll road and bridge fees (Pub 463) |
| `Parking` | Parking fees (Pub 463) |
| `GarageRent` | Off-site vehicle storage / garage rent (Pub 463) |
| `CarWash` | Car washes and detailing |
| `Lease` | Lease payments (when leasing, not depreciating; Pub 463) |
| `Accessories` | Cargo racks, floor mats, hitch, etc. |
| `Depreciation` | Annual depreciation expense (Pub 463) |

Asset account: `Assets:<Entity>:Vehicles:<VehicleSlug>` (cost basis).
Mileage offset: `Equity:<Entity>:Vehicles:<VehicleSlug>:MileageDeductions`.

### Properties (`Expenses:<Entity>:Properties:<PropertySlug>:<Category>`)

Scaffolded on property registration. Per-property accounts are required because
Schedule E rows are per-property and a shared `Expenses:Personal:HomeInsurance`
account cannot distinguish which property incurred the charge.

**Base categories** (all property types):

| Subcategory | IRS / purpose |
|---|---|
| `PropertyTax` | Local real-estate tax (Sched A line 5b / Sched E line 16) |
| `HOA` | Homeowners-association dues / condo fees |
| `Insurance` | Homeowners / landlord insurance premium |
| `Maintenance` | Routine upkeep: yard, HVAC service, pest control |
| `Repairs` | Non-capital repairs (Schedule E line 14) |
| `Utilities` | Electric / gas / water / sewer / trash / internet |
| `MortgageInterest` | Mortgage interest paid (Sched A line 8a / Sched E line 12) |

**Rental extras** (rental properties only):

| Subcategory | Purpose |
|---|---|
| `Depreciation` | Annual depreciation on the rental property |
| `Advertising` | Listing fees, photography, marketing to renters |
| `Cleaning` | Turnover cleaning and management-company services |
| `Management` | Property-management fees |
| `Supplies` | Consumables specific to the rental |

Asset account: `Assets:<Entity>:Properties:<PropertySlug>` (cost basis).
Rental income: `Income:<Entity>:Properties:<PropertySlug>:Rental`.

### Loans

Loan accounts are user-supplied at wizard time, not auto-generated from a slug.
The wizard requires three account paths:

| Account path | Purpose |
|---|---|
| `Liabilities:<Entity>:<Institution>:Loans:<LoanSlug>` | Liability balance (principal remaining) |
| `Expenses:<Entity>:<Institution>:Loans:<LoanSlug>:Interest` | Interest portion of each payment |
| `Expenses:<Entity>:<Institution>:Loans:<LoanSlug>:Escrow` (optional) | Escrow holdback (taxes + insurance collected) |

Late fees land at `Expenses:<Entity>:<Institution>:Loans:<LoanSlug>:LateFees`,
written by the loan payment writer when a late-fee amount is provided.

Mortgage interest must use the property's `MortgageInterest` subcategory
(`Expenses:<Entity>:Properties:<PropertySlug>:MortgageInterest`) rather than
the generic loan-interest path when the loan is linked to a registered
property. This keeps Schedule E line 12 unambiguous.

## Slug rules

Slugs are the stable, immutable identifier for vehicles, properties, and loan
instances. Key rules (per ADR-0031):

- Slugs are **immutable** once written. Rename the display name (per ADR-0041),
  not the slug.
- Slug uniqueness is **per parent entity**. `(Personal, MainHouse)` and
  `(Acme, MainHouse)` may coexist; `(Personal, MainHouse)` cannot have two
  live registrations.
- Tombstoned slugs are **not reusable** in their `(entity, slug)` namespace.
  A disposed vehicle's slug stays retired even after the vehicle leaves the
  books.
- Slug format: alphanumeric only, no spaces, no hyphens. Vehicles prepend `V`
  before the year.

User-facing displays use display names (mutable, per ADR-0041). The slug is
what appears in account paths, metadata, and ledger directives.

## No FIXME postings (per ADR-0043)

`Expenses:FIXME` and `Expenses:<Entity>:FIXME` are forbidden in newly-written
connector-owned files. Unclassified bank data is staged in
`staged_transactions` (SQLite) AND persisted as `custom "staged-txn"`
directives in connector-owned `.bean` files (out-of-band data; does not
affect account balances). When the user classifies, the directive is
replaced with a real balanced transaction in the same write.

A `bean-query` over a clean ledger should return zero rows matching
`account ~ "FIXME"`.

## Special accounts

| Account | Purpose | Notes |
|---|---|---|
| `Assets:<Entity>:Transfers:InFlight` | Cross-date or cross-entity transfer clearing | Should net to zero when all transfer pairs are matched |
| `Assets:<Entity>:DueFrom:<OwingEntity>` | Intercompany receivable (wrong-card) | Paired with `Liabilities:<OwingEntity>:DueTo:<PayingEntity>` |
| `Liabilities:<Entity>:DueTo:<PayingEntity>` | Intercompany payable (wrong-card) | Cleared by settlement transaction |
| `Equity:<Entity>:Vehicles:<VehicleSlug>:MileageDeductions` | Standard-mileage deduction offset | Written by `mileage_summary.bean` |

## Import tolerance

Lamella imports existing user ledgers that may not follow this standard. The
startup discovery guard (`registry/discovery.py`) checks that at least 20% of
expense accounts parse as entity-first. Below that threshold:

- Lamella refuses to start and lists the non-conformant accounts.
- The user runs an account-rename migration to bring the ledger to standard.
- OR sets `LAMELLA_SKIP_DISCOVERY_GUARD=1`, intended for test fixtures only,
  not production ledgers.

Accounts in the user-authored files (`accounts.bean`, `manual_transactions.bean`)
may use non-standard paths as long as the 20% floor is met. Connector-owned
files written by Lamella always emit conformant paths, no exceptions.

## Future categories (deferred)

**Investments / brokerage**: Beancount has well-developed conventions for
commodity lots, cost basis, dividends, and realized gains. Lamella will adopt
those conventions and add entity-prefixed namespacing
(`Assets:<Entity>:<Brokerage>:<AccountType>`,
`Income:<Entity>:Dividends:<Fund>`, etc.). Specified in a future ADR when the
investment-tracking feature lands.

## Connector files that follow this standard

Every connector-owned file writes accounts that conform to this spec. For the
full file-ownership table see `docs/specs/LEDGER_LAYOUT.md`.

| File | What it writes |
|---|---|
| `simplefin_transactions.bean` | Expense and liability postings for classified bank transactions |
| `connector_accounts.bean` | `Open` directives for all scaffolded accounts |
| `connector_overrides.bean` | Corrected FIXME accounts (legacy), intercompany four-leg blocks, loan funding |
| `connector_links.bean` | Receipt-link directives (no new accounts) |
| `connector_rules.bean` | Classification-rule directives (no new accounts) |
| `connector_budgets.bean` | Budget directives referencing existing accounts |
| `connector_config.bean` | Setting and feature-state directives (no new accounts) |
| `connector_imports/*.bean` | Spreadsheet-imported transactions |
| `mileage_summary.bean` | Year-end deduction blocks against vehicle expense + mileage Equity |

## What this standard does NOT cover

- **Display names**: mutable labels shown in the UI; governed by ADR-0041.
- **Account-type translations**: narrative copy mapping account paths to
  plain-English descriptions; governed by ADR-0041.
- **Per-feature scaffolding logic**: when accounts are opened, in which order,
  and how missing accounts are detected; see the relevant feature blueprint.
- **Plugin behavior**: how `beancount_lazy_plugins.auto_accounts` synthesizes
  `Open` directives for accounts that appear in transactions but have no
  explicit `Open`; governed by `docs/specs/LEDGER_LAYOUT.md`.

# ADR-0031: Slugs Are Immutable Per-Parent Identifiers

- **Status:** Accepted
- **Date:** 2026-04-27
- **Author:** AJ Quick
- **Related:** [ADR-0001](0001-ledger-as-source-of-truth.md), [ADR-0007](0007-entity-first-account-hierarchy.md), `src/lamella/identity.py`, `CLAUDE.md` §"Entity-first account hierarchy"

## Context

Slugs appear in three positions in the system: as entity identifiers
(e.g., `personal`), as scoped resource identifiers under an entity
(e.g., vehicle slug `suv` under entity `personal`), and embedded in
Beancount account paths (e.g., `Expenses:Personal:Suv:Fuel`). Once an
account path is written to the ledger and transactions are classified
against it, the slug is load-bearing.

Renaming a slug after references exist produces two failure modes.
First, the old account path remains in historical transactions; the new
path appears in new transactions. Queries spanning both periods produce
split results. Second, metadata keys that carry the old slug
(e.g., `lamella-mileage-vehicle: suv`) no longer match the resource the
slug names. The mismatch is silent.

The question of *scope* matters too. If vehicle slug uniqueness is
global, two entities cannot both have a `car` vehicle. That constraint
is arbitrary and breaks the natural organization of the entity-first
hierarchy, where each entity owns its own namespace.

## Decision

This ADR encodes three sub-decisions that together define slug identity.

### Sub-decision 1: Slugs are immutable post-reference

A slug written into a ledger directive or a transaction posting is
immutable from that point forward. "Written into the ledger" means: any
`custom` directive whose type or metadata carries the slug, or any
transaction whose account path embeds the slug.

Pre-reference renames, during initial setup, before any transaction
or directive references the slug, are allowed. The setup wizard MAY
offer a rename step up to the point of first use.

Post-reference renames are implemented as a three-step operation:

1. **Tombstone** the old slug: write a `custom "<resource>-disposed"`
   tombstone directive with `lamella-tombstone: true` and the old slug.
   Mark the resource as inactive in SQLite.
2. **Create** a new slug: write a fresh resource with the new slug.
3. **Bulk-rewrite job**: run the in-place rewrite system across all
   connector-owned `.bean` files under `ledger_dir`, replacing the old
   slug's account paths and metadata values with the new slug's
   equivalents. The job runs under the standard snapshot + bean-check
   guard ([ADR-0004](0004-bean-check-after-every-write.md)).

Display names (`name` field on the resource record) are always mutable
with no constraints. The slug is not the display name.

### Sub-decision 2: Slug uniqueness is scoped to the parent entity

`(entity_slug, slug)` is the identity tuple for vehicles, properties,
loans, and projects. The uniqueness constraint is per-parent, not
global:

- `personal / car` and `business / car` can coexist.
- Within `personal`, two resources of the same type cannot share the
  slug `car`.
- Account paths embed the entity prefix and disambiguate naturally:
  `Expenses:Personal:Car:...` vs. `Expenses:Business:Car:...`.

Entity slugs themselves are globally unique. An entity has no parent,
so its namespace is the global one.

### Sub-decision 3: Tombstoned slugs are not reusable within their namespace

A disposed `(personal, car)` cannot be reassigned to a future vehicle
under the `personal` entity. The account path
`Expenses:Personal:Car:...` refers to one physical thing for all time.
Reusing the slug would cause historical transactions classified under
the old vehicle to appear as if they belong to the new one.

If the user acquires a replacement vehicle, they MUST use a different
slug (e.g., `car2`, `suv`, or a descriptive string). The UI SHOULD
suggest a non-conflicting slug and explain why the old one is
unavailable.

### Cross-reference schema

Metadata keys that reference a scoped slug MUST carry both the parent
entity key and the slug key as separate metadata entries. Using the
slug alone is forbidden because slug uniqueness is not global.

Current examples:
- `lamella-mileage-entity: personal` + `lamella-mileage-vehicle: car`
- Same pattern for properties, loans, projects

A future metadata key that carries only a vehicle slug with no entity
context is a schema violation.

### Documented escape hatch

`src/lamella/registry/slug_rename.py` is the user-initiated rename
tool (typo fix / merge flow) with bean-check + snapshot safety. It
performs the tombstone + bulk-rewrite operation atomically. This is
the SOLE permitted code path that updates a slug column in SQLite.
Any other `UPDATE ... SET slug = ?` is a violation.

## Consequences

### Positive
- Any given account path uniquely identifies one physical real-world
  resource for the entire ledger history. Cross-period queries are
  correct.
- Metadata keys carrying `(entity, slug)` pairs are unambiguous even
  after display names change.
- The uniqueness constraint on `(entity_slug, slug)` is a SQL UNIQUE
  constraint, enforced by the database, not application logic.

### Negative / Costs
- Users who mis-name a slug during setup face a tombstone-and-rename
  flow rather than a simple edit. The flow is correct but adds friction.
- Tombstone slugs accumulate in the database and ledger. Over time, the
  list of "unavailable" slugs for a given entity grows.

### Mitigations
- The setup wizard rename window (pre-reference) gives users a natural
  correction opportunity before the slug is committed.
- The tombstone list is small in practice. Users rarely dispose of and
  re-acquire the same category of resource frequently enough for
  accumulation to matter.

## Compliance

How `/adr-check` detects violations:

- **Mutable slug update outside slug_rename.py:** AST scan for
  `UPDATE vehicles SET slug =`, `UPDATE properties SET slug =`,
  `UPDATE loans SET slug =`, `UPDATE projects SET slug =`,
  `UPDATE entities SET slug =`. Any hit outside
  `src/lamella/registry/slug_rename.py` is a violation.
- **Missing UNIQUE constraint:** schema check that each scoped table
  (`vehicles`, `properties`, `loans`, `projects`) has a
  `UNIQUE (entity_slug, slug)` constraint. `entities` table has
  `UNIQUE (slug)`.
- **Slug-only metadata:** grep `src/lamella/` for metadata key writes
  that emit a vehicle/property/loan/project slug key without a paired
  entity key in the same metadata dict.
- **Reader taking slug alone:** AST scan for functions named
  `get_vehicle`, `get_property`, `get_loan`, `get_project` (or
  equivalents) that accept a single `slug` positional argument with no
  `entity_slug` parameter. Flag for manual review.

## References

- [ADR-0001](0001-ledger-as-source-of-truth.md): history preservation
- [ADR-0002](0002-in-place-rewrites-default.md): bulk-rewrite job depends
  on this machinery
- [ADR-0004](0004-bean-check-after-every-write.md): post-reference
  rename job uses this
- [ADR-0007](0007-entity-first-account-hierarchy.md): per-parent
  uniqueness is natural under this scheme
- `src/lamella/identity.py`: identity helpers
- `src/lamella/registry/slug_rename.py`: sole permitted rename path
- CLAUDE.md §"Entity-first account hierarchy"

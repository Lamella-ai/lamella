# ADR-0017: Example and Placeholder Data Policy

- **Status:** Accepted
- **Date:** 2026-04-26
- **Author:** AJ Quick
- **Related:** `CLAUDE.md` ("Example & Placeholder Data Policy"), no superseding ADR

## Context

The development database contains real financial records from the
maintainer's live ledger. Tests, fixtures, comments, and docs are
authored against that context. Without an explicit policy, real
names, businesses, and locations leak into committed code,
identifying the maintainer and their real counterparties to anyone
who reads the repo.

The repo is currently private but is planned to move to a public
namespace (`lamella-ai/lamella`). Any identifying data committed
before the move becomes permanently public via git history.

A secondary risk: ad-hoc placeholder invention creates
inconsistency. One developer uses a real retailer as an "example";
another uses a different real brand; a third uses a generic
category. The result is a codebase where the placeholder policy
has to be reverse-engineered from scattered examples.

## Decision

No personal or business-identifying data from the maintainer's
real context MAY appear in any of the following locations:

- Test fixtures, seed data, factories, snapshots
- Form placeholders, default values, `<input value="...">`
- Code comments and docstring examples
- README / docs / ADR examples
- Variable names, file names, route names, commit messages
- Demo pages and scratch routes

### Forbidden categories

- Real personal names, business names, or brands owned by the maintainer
- Real addresses, ZIP codes, neighborhoods, or city/state combinations
  near the maintainer
- Real phone numbers, emails, or domains belonging to the maintainer
- Real vehicles, retailers, suppliers, or vendors the maintainer uses
- Any row copied from the dev database mapping to a real person or entity

### Canonical placeholder table

| Concept | Use |
|---|---|
| Person | Jane Doe, John Smith |
| Business / entity | Acme Co., Example LLC, EntityA, EntityB |
| Email | jane@example.com |
| Phone | 555-0100 through 555-0199 (reserved range) |
| Address | 123 Main St, Anytown, ST 00000 |
| Retailer | "a home improvement store" |
| Vehicle | "a mid-size SUV" |
| Industry term | generic ("a service business", "a retailer") |
| Filler text | standard lorem ipsum |

MUST use only the canonical placeholders above. MUST NOT invent
substitutes (naming a specific real ride-share company instead of
the generic "a ride-share service" placeholder).

### Rule of thumb

When generalizing, generalize the **category**, not the specific
brand. Write "a ride-share service" rather than naming a specific
ride-share company. Write "a mid-size SUV" rather than naming a
specific SUV model. If an example would let a stranger reading
the repo identify the project owner, their location, or their real
customers, it is forbidden.

## Consequences

### Positive
- The repo can become public without a sanitization pass.
- Placeholders are consistent and machine-checkable: a regex over
  the canonical allowlist can flag violations in CI.
- Contributors outside the project have no path to identifying the
  maintainer's real counterparties.

### Negative / Costs
- Developers who know the real data must consciously translate when
  writing examples. The cognitive overhead is real but bounded.
- Existing commits may contain violations; a history rewrite to
  remove them is out of scope (and would break collaborator forks).
  Only forward-committed content is governed.

### Mitigations
- The canonical placeholder table is authoritative and short.
  Memorizing it requires one reading.
- `/adr-check` regex patterns can scan for the forbidden-category
  signals (known brand names, real area codes, etc.) as a pre-commit
  hook or CI gate.

## Compliance

Enforcement has two layers:

1. **Pattern scan.** `/adr-check` runs regex patterns for the
   forbidden categories (known brand patterns, real phone area
   codes, real ZIP formats near the maintainer) against all
   non-binary committed files.
2. **Canonical-allowlist diff.** Any placeholder string not in the
   canonical table triggers a warning for human review.

Code review is the final gate. Reviewers MUST reject any example
that maps to a real-world entity associated with the maintainer.

## References

- CLAUDE.md § "Example & Placeholder Data Policy" (full section)
- CLAUDE.md § "When using the dev database"

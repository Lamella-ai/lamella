# ADR-0040: Source Code Is Organized by Concern Type, Not Flat Under `src/lamella/`

- **Status:** Accepted
- **Date:** 2026-04-27
- **Author:** AJ Quick
- **Related:** [ADR-0020](0020-adapter-pattern-for-external-data-sources.md), [ADR-0032](0032-component-library-per-action.md)

## Context

`src/lamella/` currently holds 47 items at the top level. A flat mix
of domain features, external integration clients, cross-cutting
infrastructure, web routes, templates, static files, and utility
modules. Every new feature adds to this flat list. Finding where
something lives requires reading code, not inferring from structure.

ADR-0020 introduced `adapters/` and `ports/` as target directories
but did not define the full target layout. Without a complete layout
contract, new code lands wherever the author chooses. The layout
diverges further with each PR.

The flat structure also obscures the adapter/business-logic boundary.
Code that must be replaceable (adapters) lives next to code that must
not be touched during substitution (business logic). Separating them
reduces the surface area that adapter substitution touches.

## Decision

Top-level under `src/lamella/` is exactly:

```
src/lamella/
├── main.py
├── __init__.py
├── CLAUDE.md
├── features/        — one directory per feature blueprint
├── adapters/        — implementations of ports (per ADR-0020)
├── ports/           — interfaces the adapters satisfy (per ADR-0020)
├── core/            — cross-cutting infrastructure
├── web/             — FastAPI surface: routes, templates, static, components
└── utils/           — pure utility, no domain knowledge, no I/O
```

No other top-level directories are permitted. New code MUST land in
one of these six buckets. New top-level directories under
`src/lamella/` are forbidden without a superseding ADR.

### Bucket rules

**`features/<slug>/`**: one directory per feature blueprint in
`docs/features/`. Standard internal shape: `service.py`, `reader.py`,
`writer.py`, `models.py`, `__init__.py`. A feature may omit files it
does not need; it may not add non-standard files without documenting
the deviation in its blueprint.

**`adapters/<provider>/`**: implementations of a port from
`ports/`. Covers: `simplefin/`, `paperless/`, `openrouter/`,
`notify/`. Each adapter imports only from its port interface and
`core/`. Never imports from `features/` or `web/`.

**`ports/`**: abstract base classes or `Protocol` definitions that
adapters satisfy. One file per port (e.g. `bank_data.py`,
`ai_provider.py`, `document_store.py`). No implementation code.

**`core/`**: infrastructure used by multiple features with no
feature-specific knowledge. Includes: `identity.py`, `jobs/`,
`rewrite/`, `bootstrap/`, `transform/`, `registry/`, `beancount_io/`,
`settings/`, `db.py`. A module belongs in `core/` if removing it
would break two or more features.

**`web/`**: FastAPI surface only. Subdirectories: `routes/`,
`templates/`, `static/`, `components/` (Jinja macros per ADR-0032),
`deps.py`. No business logic. Routes call feature services; they do
not contain logic beyond request parsing and response formatting.

**`utils/`**: pure utility modules: no domain knowledge, no I/O, no
imports from other `lamella` subpackages. Today: `_legacy_env.py`,
`_legacy_meta.py`, `_uid_compat.py`.

### Migration

The actual code move is a separate workstream (Phase 8): approximately
25 to 30 commits, 2 to 3 hours of swarm time. This ADR is the contract; the
move is its implementation. Until Phase 8 lands, the legacy flat
layout coexists with new code that follows the target structure.

Every PR adding a new directory directly under `src/lamella/` is
reviewed for placement against this ADR before merge.

## Consequences

### Positive
- Finding any module requires only knowing its type (feature, adapter,
  infrastructure, web surface, utility). No code reading needed.
- The adapter/business-logic boundary is physically visible: adapters
  and ports are separated from feature logic. Adapter substitution
  touches `adapters/` and `ports/` only.
- ADR-0020's port/adapter obligation now has a mandatory directory
  location, removing the ambiguity of "where does the adapter live?"
- New contributors can infer placement rules from the layout itself.

### Negative / Costs
- Phase 8 is a large mechanical refactor with import-path changes
  throughout. Test suite must pass before and after each batch of
  moves.
- Python import paths change (`lamella.simplefin.client` →
  `lamella.adapters.simplefin.client`). External scripts or docs
  referencing old paths need updates.
- The move creates a window where the layout is partially migrated.
  Pre-Phase-8 code and post-Phase-8 code coexist.

### Mitigations
- Phase 8 moves are purely mechanical, no logic changes. CI catches
  import errors immediately.
- Old import paths can be re-exported from `__init__.py` files during
  the transition to avoid breaking changes to any external tooling.
- `/adr-check` enforces the new-code rule going forward, preventing
  further accumulation in the flat layout.

## Compliance

`/adr-check` checks:
- **New top-level directory:** any PR adding a directory directly under
  `src/lamella/` that is not one of the six permitted buckets is a
  violation.
- **New file in root:** any PR adding a `.py` file directly under
  `src/lamella/` (other than `main.py` or `__init__.py`) is a
  violation.
- **Post-Phase-8:** `ls src/lamella/` MUST show only the six
  directories, `main.py`, `__init__.py`, and `CLAUDE.md`. CI asserts
  this after Phase 8 lands.

## References

- [ADR-0020](0020-adapter-pattern-for-external-data-sources.md): adapter/port pattern (this ADR defines where those directories live)
- [ADR-0032](0032-component-library-per-action.md): Jinja macros live under `web/components/`
- `docs/features/`: one blueprint per `features/<slug>/` directory

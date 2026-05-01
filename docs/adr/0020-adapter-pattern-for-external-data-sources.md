# ADR-0020: Adapter Pattern for External Data Sources

- **Status:** Accepted
- **Date:** 2026-04-26
- **Author:** AJ Quick
- **Related:** `CLAUDE.md` ("Non-negotiable architectural rules"), [ADR-0006](0006-long-running-ops-as-jobs.md), [ADR-0008](0008-unconditional-dedup.md), [ADR-0016](0016-paperless-writeback-policy.md)

## Context

Lamella is a single-tenant, self-hosted system. Four external
integrations are wired directly into routes and business logic:

| Integration | Module | Role |
|---|---|---|
| SimpleFIN Bridge | `src/lamella/simplefin/` | Bank data ingestion |
| Paperless-ngx | `src/lamella/paperless/` | Document store, receipt context |
| OpenRouter | `src/lamella/ai/client.py` | AI provider (classify, verify, match) |
| SQLite / filesystem | `src/lamella/` (various) | Storage backend |

Code in `routes/simplefin.py`, `routes/webhooks.py`,
`routes/paperless_fields.py`, `routes/staging_review.py`, and
several others imports directly from `lamella.simplefin.client`,
`lamella.paperless.client`, and `lamella.ai.client`. Business
logic and route handlers call provider-specific APIs directly.

Each integration should be substitutable: a different bank data
source, a locally-hosted AI model, an alternative document store,
or a different storage backend. Code that hardcodes these choices
today forecloses substitution without rewriting business logic.

No `src/lamella/ports/` or `src/lamella/adapters/` directories
currently exist. This ADR codifies the target architecture and
establishes the rule for all new integration code.

## Decision

Every external integration MUST sit behind a port (interface) with
an adapter implementation. Four ports are required:

| Port | Adapter today | Possible alternatives |
|---|---|---|
| `BankDataPort` | `adapters/simplefin/` (SimpleFIN Bridge) | CSV import adapter, OFX adapter, other bank APIs |
| `AIProviderPort` | `adapters/openrouter/` (OpenRouter cascade) | Local llama.cpp adapter, Anthropic direct adapter |
| `DocumentStorePort` | `adapters/paperless/` (Paperless-ngx) | Local filesystem adapter, S3-backed adapter |
| `StoragePort` | `adapters/sqlite/` (SQLite + local FS) | Postgres adapter, alternative storage backends |

### Specific obligations: new integration code

- MUST write the port interface in `src/lamella/ports/<name>.py`
  (e.g. `BankDataPort` as an abstract base class or Protocol).
- MUST write the adapter in `src/lamella/adapters/<provider>/`
  (e.g. `adapters/simplefin/`).
- MUST NOT call external client classes directly from routes,
  classify logic, or business logic. Call the port; inject the
  adapter.
- MUST route all configuration through the settings service.
  No direct `os.environ` reads in business code or routes.
- MUST route all "current user" lookups through `current_user_id()`
  and `current_user_ledger_path()`. Never hardcode paths or
  user identifiers in integration code.

### Existing code

Existing violations (direct imports of `lamella.simplefin.client`,
`lamella.paperless.client`, `lamella.ai.client` from routes and
business logic) are tracked as remediation backlog in per-feature
blueprint files under `docs/features/`. This ADR applies going
forward; retrofitting is tracked separately and prioritized with
each feature's next major revision.

MAY call adapter constructors in the application factory
(`app.py` or lifespan startup). Adapters are injected from there;
routes and business logic receive them via `app.state` or
dependency injection.

## Consequences

### Positive
- Adapter substitution requires only a new adapter implementation;
  no business logic changes.
- Integration tests can inject a fake adapter without patching
  `httpx` or SQLite internals.
- The port interface documents the contract each integration must
  satisfy, making new adapter development tractable for contributors
  who don't know the provider API.

### Negative / Costs
- Existing code does NOT follow this. `lamella.simplefin.client`,
  `lamella.paperless.client`, and `lamella.ai.client` have direct
  call sites in routes and business logic throughout the codebase.
  This is a significant remediation backlog.
- Adding a port abstraction to existing tightly-coupled code
  carries refactoring risk. Each migration must be validated
  against the live ledger.
- Port interfaces designed prematurely may not fit real future
  requirements. Interfaces will need revision when new providers
  are defined.

### Mitigations
- Remediation is incremental: new features adopt the pattern; old
  features migrate during major revision cycles. No flag day.
- Port interfaces are kept minimal, only the operations the
  current adapters use. They grow when new providers add requirements.
- `/adr-check` AST scans catch new violations before they merge,
  preventing the backlog from growing while existing debt is paid.

## Compliance

`/adr-check` AST scan for direct imports of:
- `lamella.simplefin.client`
- `lamella.paperless.client`
- `lamella.ai.client`

outside `src/lamella/adapters/`. Any match on a newly-committed
file is a violation and blocks merge. Files with pre-existing
violations are allowlisted until each feature's remediation PR
lands. New external integrations (any `httpx.AsyncClient`,
`sqlite3`, or third-party SDK instantiation) outside
`src/lamella/adapters/` trigger a mandatory design review.

## References

- CLAUDE.md § "Non-negotiable architectural rules"
- `src/lamella/simplefin/client.py`: current bank adapter (not yet behind a port)
- `src/lamella/paperless/client.py`: current document adapter (not yet behind a port)
- `src/lamella/ai/client.py`: current AI adapter (not yet behind a port)
- `src/lamella/routes/simplefin.py`, `routes/webhooks.py`, `routes/paperless_fields.py`: current direct-import violation sites
- [ADR-0006](0006-long-running-ops-as-jobs.md): adapters for long-running ops submit jobs
- [ADR-0008](0008-unconditional-dedup.md): bank data dedup contract the `BankDataPort` must satisfy
- [ADR-0016](0016-paperless-writeback-policy.md): writeback policy the `DocumentStorePort` must enforce

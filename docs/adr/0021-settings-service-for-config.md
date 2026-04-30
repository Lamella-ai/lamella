# ADR-0021: Configuration Reads Go Through The Settings Service

- **Status:** Accepted
- **Date:** 2026-04-27
- **Author:** AJ Quick
- **Related:** `CLAUDE.md` ("Non-negotiable architectural rules"), `src/lamella/config.py`, `src/lamella/settings_store.py`, `src/lamella/settings_writer.py`, `src/lamella/_legacy_env.py`, [ADR-0020](0020-adapter-pattern-for-external-data-sources.md)

## Context

`config.py` defines a `Settings` class (via `pydantic_settings.BaseSettings`)
that reads all env vars at process startup. `_legacy_env.apply_env_aliases()`
runs first to normalize `LAMELLA_*` and pre-rebrand `CONNECTOR_*` names into
the bare names pydantic reads. The `Settings` instance is the authoritative
config surface.

Scattered `os.environ.get(...)` calls in business logic bypass this path.
They skip alias normalization, skip type coercion, skip `SecretStr` masking,
and produce a second read-path that diverges from `Settings` in subtle ways
that are hard to test. Any future per-user config injection requires the
config to flow through `Settings` instances rather than env vars.

ADR-0020 already forbids `os.environ` reads in adapters and routes. This ADR
generalizes that rule to all production code.

## Decision

All configuration MUST flow through one settings module. No `os.environ.get`
or `os.environ[...]` call MAY appear in production code outside
`config.py`, `settings_store.py`, `settings_writer.py`, and `_legacy_env.py`.

Specific obligations:

- All env-var reads MUST live in `config.py` (`Settings` class fields) or in
  `_legacy_env.py` (alias normalization only).
- Business code and route handlers MUST receive a `Settings` instance, via
  `app.state.settings`, a FastAPI dependency, or a function argument.
  They MUST NOT call `os.environ` directly.
- Per-feature config MUST be a typed attribute on `Settings`, never a
  free-form `dict` or a module-level `os.environ.get(...)` fallback.
- Test fixtures MUST construct `Settings(...)` directly with keyword
  arguments. They MUST NOT patch `os.environ` to change config values.
- `_legacy_env.apply_env_aliases()` MUST be called before any `Settings()`
  construction. It already runs in `main.py` startup and in `conftest.py`.
  No new call sites needed.

### Bootstrap exception

`_uid_compat.py` (UID/HOME shim that runs before pydantic settings is
importable) and the HuggingFace cache path setup at the top of `main.py` are
documented exceptions. These run at process-startup before the settings
service is available. They are allowlisted in `/adr-check`.

## Consequences

### Positive
- One read path. Alias normalization, type coercion, and `SecretStr`
  masking apply consistently everywhere.
- Tests construct a `Settings` instance with explicit values. No
  env-var patching, no ordering hazards between fixtures.
- Per-user config injection is possible: substitute a different
  `Settings` instance without touching env vars.

### Negative / Costs
- Existing `os.environ.get` calls in `main.py` and `_uid_compat.py`
  are violations that must be remediated (tracked in feature blueprints).
- Passing a `Settings` instance through call stacks that don't today
  accept one requires function-signature changes.

### Mitigations
- FastAPI dependency injection (`Depends(get_settings)`) avoids
  threading `Settings` through deep call stacks. The DI container
  resolves it at the handler boundary.
- Remediation of existing violations is incremental: new code
  adopts the rule; existing violations are fixed during each
  feature's next revision.

## Compliance

AST scan: grep `src/lamella/` for `os\.environ` outside
`config.py`, `settings_store.py`, `settings_writer.py`,
`_legacy_env.py`, `_uid_compat.py`, and the documented `main.py`
HuggingFace cache bootstrap. Each match outside the allowlist is a violation.

`/adr-check` enforces this scan on new files. Pre-existing violations
are tracked per-feature in the relevant blueprint's Remaining Tasks.

## References

- CLAUDE.md § "Non-negotiable architectural rules"
- `src/lamella/config.py`: `Settings` class (pydantic_settings)
- `src/lamella/_legacy_env.py`: alias normalization (the only approved `os.environ` reader)
- [ADR-0020](0020-adapter-pattern-for-external-data-sources.md): "MUST route all configuration through the settings service"

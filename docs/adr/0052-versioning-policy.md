# ADR-0052: Versioning policy, when the app version bumps

- **Status:** Accepted
- **Date:** 2026-04-29
- **Related:** [ADR-0001](0001-ledger-as-source-of-truth.md),
  [ADR-0015](0015-reconstruct-capability-invariant.md),
  [ADR-0026](0026-migrations-forward-only-append-only.md)

## Context

Lamella has at least three independently-evolving version axes:

1. **App SemVer**: the `version` field in `pyproject.toml`, surfaced
   through `lamella.__version__` (read at import time from
   `importlib.metadata`) and rendered into templates as `app_version`.
   Currently `0.2.0`.
2. **Ledger schema version**: the on-disk contract for what the
   `.bean` files look like. The canonical constant is
   `LATEST_LEDGER_VERSION` in `src/lamella/core/bootstrap/detection.py`
   (currently `3`). The matching on-disk stamp is the
   `2026-01-01 custom "lamella-ledger-version" "N"` directive emitted
   into `main.bean` by bootstrap. Forward migrations live in
   `src/lamella/features/recovery/migrations/migrate_ledger_v{N}_to_v{N+1}.py`.
3. **SQLite migration number**: the auto-incrementing files under
   `/migrations/NNN_*.sql` (currently at `061`). The migrations are
   forward-only and append-only per ADR-0026.

These axes have drifted historically. The ledger schema bumped from V2
to V3 (commit `migrate_ledger_v2_to_v3.py` landed) without a
corresponding bump in `pyproject.toml::version`. Operators reading the
app version cannot tell whether a deploy contains a breaking ledger
change or a docs typo. There is no written rule about which axis bump
forces which other axis bump, so future migrations will drift the
same way.

## Decision

### App SemVer is the externally visible version

`pyproject.toml::version` and `src/lamella/__init__.py::__version__`
(via `importlib.metadata.version("lamella")`) are the same value by
construction. `__version__` reads the installed distribution's
metadata. They MUST stay in sync at release time; if a release ever
hand-edits `__version__` instead of the package metadata, the policy
is violated.

The bump tier for any release is decided by walking these rules in
order; the first match wins:

#### MAJOR (X.0.0), required on any of:

- A bump to `LATEST_LEDGER_VERSION`. The ledger schema is the public
  data contract per ADR-0001; a new ledger version is by definition a
  breaking change for anyone with an existing ledger.
- A breaking change to a load-bearing ADR (e.g. ADR-0001's
  ledger-as-source-of-truth contract, ADR-0003's metadata namespace,
  ADR-0015's reconstruct invariant).
- A removed or renamed public route (someone's bookmark or external
  script breaks).
- A removed env var or a change in the meaning of an existing one.
- Dropped Python version support (e.g. `requires-python` raised).
- A SQLite migration that destroys data, renames columns referenced by
  external tooling, or otherwise cannot run silently.

#### MINOR (x.Y.0), required on any of:

- A new user-facing feature surface that is **on by default** (a new
  route, page, CLI command, or job that an operator will encounter
  without setting any flag).
- A new env var **whose default behavior changes what the app does**
  (e.g. a tunable threshold that takes effect at every install).
- A new SQLite migration under `/migrations/` that runs automatically
  and is schema-additive (new tables, new nullable columns, new
  indices).
- A documented behavior change that operators will observe even
  without changing config.

A new ADR alone is NOT enough. ADRs document decisions; they only
trigger a MINOR bump when they coincide with a user-facing change
that meets the criteria above.

#### PATCH (x.y.Z), everything else, including:

- Bug fix.
- Documentation change.
- Performance improvement with no observable behavior change.
- Refactor with no observable behavior change.
- Test-only change.
- **Flag-gated features that default OFF.** A new feature surface
  (route, env var, CLI command) that operators only encounter after
  explicitly opting in is PATCH-tier; installs that don't flip the
  flag are byte-compatible with the prior PATCH. Once the flag's
  default flips to ON in a later release, that flip is the MINOR
  bump (see "default-flip" rule below).
- A new ADR that documents an existing decision, freezes a spec for
  future work, or is otherwise inert at the call sites (no code
  paths change at install time).
- A new internal-only API surface (e.g. a new module function used
  only from existing code paths).
- Recovery / migration tooling that operators run explicitly (CLI
  one-shots, recovery findings the user has to click); they don't
  affect installs that don't run them.

#### Default-flip rule

When a previously flag-gated feature flips its default from OFF to
ON, that release is MINOR. Every install starts seeing the new
behavior at upgrade time. The same flag's introduction (default OFF)
was PATCH; the flip is what costs the MINOR.

#### Why this discipline matters

Adding tunables and additive ADRs is routine maintenance work in a
project of this shape. If every additive change forced MINOR, the
version would race ahead of any meaningful contract change. `0.3 ->
0.4 -> 0.5` becomes weekly noise rather than a signal that the
operator-visible surface shifted. PATCH-tier additive work + MINOR
on the default-flip means: when an operator sees `0.x.y -> 0.x.(y+1)`
they know "no action required, upgrade is safe"; `0.x.y -> 0.(x+1).0`
means "read the changelog before upgrading."

#### Worked example: ADR-0043 staged-txn migration (v0.3.1)

Landed substantial new code: a directive renderer, a reconstruct
step, an atomic promotion writer, an ingest wire-up, two new
classify-endpoint code paths, a CLI migration tool, an ADR (0043b),
and ~80 new tests. *None of this affects installs that don't flip
the `enable_staged_txn_directives` flag.* Default-off means the
upgrade is invisible to existing operators; therefore PATCH (0.3.0
-> 0.3.1) is correct. The default-flip release will be MINOR
(0.3.x -> 0.4.0).

#### Pre-1.0, rules apply, with one carve-out

The rules above apply during `0.x`. The carve-out: a MAJOR-tier
breaking change does NOT force a `1.0.0` bump while in `0.x`.
Breaking changes during `0.x` increment the MINOR digit (e.g.
`0.3.5 -> 0.4.0`) instead of moving to `1.0.0`. Everything else
follows the standard rules.

Don't treat `0.x` as "anything goes." Operators reading the version
delta still need a reliable signal: PATCH-tier upgrades should be
safe to take blindly, MINOR-tier upgrades should warrant a glance
at the changelog. If every release rolls a MINOR digit because of
a generous interpretation of "feature surface," that signal goes
to zero.

### Ledger schema version

The ledger schema version is defined by the constant
`LATEST_LEDGER_VERSION` in
`src/lamella/core/bootstrap/detection.py`, surfaced on disk as the
`lamella-ledger-version` Beancount custom directive in `main.bean`,
and walked at startup to detect drift (see
`features/recovery/findings/schema_drift.py`).

A change to `LATEST_LEDGER_VERSION` requires all of:

1. A forward migration script at
   `src/lamella/features/recovery/migrations/migrate_ledger_v{N}_to_v{N+1}.py`
   following the existing module shape (idempotent, runs against an
   on-disk ledger, leaves the version stamp updated).
2. A documented rollback story. Forward migrations in this codebase
   are not automatically reversible; the rollback is "restore from
   the pre-migration ledger backup that bootstrap took." The ADR
   documenting the bump must say so explicitly.
3. An ADR if the change is contract-level (new metadata key, account
   path shape change, new directive type). Tweaks to data already in
   the ledger may not need a new ADR but still need the migration
   script.
4. A MAJOR app version bump (per the rules above).
5. A `CHANGELOG.md` entry that names the new ledger version
   explicitly (e.g. "Ledger schema bumped to v4, see ADR-NNNN").

### SQLite migration number

Auto-incrementing under `/migrations/`. The bump rule is "use the
next integer". There is no policy decision to make. A new SQLite
migration implies at least a MINOR app bump; if the migration is
destructive or changes the meaning of an existing column, it implies
MAJOR per the rules above.

### Synchronization invariant

`pyproject.toml::version` and the value of `lamella.__version__` at
runtime must agree. They agree by construction today
(`__version__ = importlib.metadata.version("lamella")`). If a future
change introduces a hand-maintained constant for `__version__`, that
change MUST also introduce a CI guard that asserts the two are equal.

## How to apply: release checklist

For every release:

1. Walk the rules above to decide PATCH / MINOR / MAJOR.
2. Update `pyproject.toml::version`. Verify
   `python -c "import lamella; print(lamella.__version__)"` reports
   the new value after a reinstall (`pip install -e .`).
3. Add a `CHANGELOG.md` entry: new version header, date, what
   changed. Group entries by bump tier so the reader can scan
   "what's a breaking change" at a glance.
4. **If MAJOR:** the changelog entry must call out the trigger (which
   ADR / which ledger version / which removed route). If the trigger
   is a ledger-version bump, document the upgrade note in the release
   so operators know to take a backup before pulling the new image.
5. Tag the commit `v<version>` (the docker build pipeline keys on the
   tag for image labeling).

## Consequences

- **Operators get a meaningful upgrade signal.** A MAJOR bump means
  "back up before pulling." A MINOR bump means "new feature, safe to
  pull." A PATCH bump means "bug fix, safe to pull." Today the
  version is silent about all three.
- **One canonical source for the ledger version.** The ADR formalises
  `LATEST_LEDGER_VERSION` in `core/bootstrap/detection.py` as the
  single source of truth. Future code that needs to gate on the
  ledger version reads that constant; it does not invent a parallel
  one.
- **The CHANGELOG becomes load-bearing.** The release-checklist
  requirement makes the changelog the place where bumps are
  justified. The existing `/changelog-check` skill can lint that
  routes / migrations / env vars are accompanied by changelog
  entries.
- **More frequent MAJOR bumps than typical web-app projects.** Lamella's
  data contract is the ledger; that contract evolves more often than
  a HTTP API contract. Operators see a `2.0.0`, `3.0.0`, `4.0.0`
  cadence rather than a long-tailed `1.x` line. This is correct: the
  SemVer signal exists to communicate breaking changes, and ledger
  schema bumps are breaking changes.
- **0.x lifecycle remains permissive.** Today's `0.2.0` codebase can
  iterate without ceremony until `1.0.0` ships. The rules ratchet on
  at `1.0.0`.

## Alternatives considered

1. **Calendar versioning (e.g. `2026.04.29`).** Rejected. CalVer is
   right when the product is delivered as a service and operators
   treat new versions as "always upgrade." Lamella is self-hosted;
   operators decide when to upgrade and need to know whether the
   upgrade is risky. SemVer's MAJOR/MINOR/PATCH signal is exactly
   the upgrade-risk classifier the operator needs.
2. **Decouple ledger version from app version.** Rejected. The
   ledger contract IS a public API surface for this app; `.bean`
   files are user data, the schema defines what the app can read
   and write, and a ledger-version mismatch means the operator
   needs to act. Decoupling means an operator on app `2.5.0` cannot
   tell from the version whether their ledger needs a migration.
   Coupling them via "ledger bump → MAJOR app bump" makes the
   relationship single-axis from the operator's perspective.
3. **Add a fourth axis (a separate `data_version`) and only bump
   MAJOR when both move.** Rejected. More axes is the opposite of
   the simplification we want. The whole point is "operator reads
   one number and knows the upgrade-risk class."
4. **Skip the policy, just be careful.** Rejected; that's what
   produced the V2→V3 drift in the first place.

## Migration plan

- The current baseline is `0.2.0` with `LATEST_LEDGER_VERSION = 3`.
  This ADR does not retroactively rewrite version history.
- The CHANGELOG MAY note retroactively that V2→V3 was a schema bump
  that pre-dated this policy, so the historical record is not
  silent. No version is rewritten.
- From this ADR forward, any change to `LATEST_LEDGER_VERSION`
  triggers a MAJOR app bump per the rules above. The next ledger
  bump (V3→V4) will therefore land alongside `1.0.0` or higher.
- Recommended follow-up commit (out of scope for this ADR): a small
  CI / startup-banner check that surfaces `LATEST_LEDGER_VERSION`
  alongside `app_version` in the startup log and in `/healthz`, so
  operators can verify both axes match expectations without grepping
  the source.

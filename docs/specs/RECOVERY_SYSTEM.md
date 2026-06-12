# Recovery System

Canonical reference for `/setup/recovery` and the broader recovery
infrastructure (detection → gating → findings → heals → migrations).
Read before changing anything in `src/lamella/bootstrap/`,
`src/lamella/bootstrap/recovery/`, or any code that emits new
`lamella-*` directives or metadata.

Companion docs: `LEDGER_LAYOUT.md` (the on-disk file/directive
contract; recovery enforces this), `NORMALIZE_TXN_IDENTITY.md` (the
canonical worked example of a non-versioned schema migration).

---

## 1. Purpose

The recovery system answers one question on every boot: **does the
ledger on disk match what the running software expects?** If yes,
serve the dashboard. If no, route the user through `/setup` (first
run) or `/setup/recovery` (drift / migration / repairs) and refuse
to serve normal UI until the gate clears.

The system is built around a strict separation:

- **Detection** is pure. It reads the ledger + SQLite and produces
  a state classification or a tuple of `Finding` objects. No writes.
- **Heals** consume `Finding` objects and write. Every heal goes
  through `with_bean_snapshot` so a failed write rolls back
  byte-identically.
- **Migrations** are heals for one specific category
  (`schema_drift`) that is versioned and runs in sequential atomic
  units instead of grouped batches.

This separation is load-bearing. Detectors can be re-run cheaply on
every page load; heals are slow and side-effecting and only run when
the user explicitly confirms.

---

## 2. Glossary

- **`LedgerState`**: enum at `bootstrap/detection.py:40` classifying
  the ledger as MISSING / UNPARSEABLE / STRUCTURALLY_EMPTY /
  NEEDS_VERSION_STAMP / NEEDS_MIGRATION / NEEDS_NEWER_SOFTWARE / READY.
  `NEEDS_NEWER_SOFTWARE` is the forward-compat refusal state resolved
  on 2026-04-26 (decisions §2.1); the other six are pre-existing.
- **`SetupProgress`**: output of
  `bootstrap/setup_progress.py::compute_setup_progress`. ~12
  `SetupStep` instances, each `is_complete` + `required`. Drives
  the gate at `routes/dashboard.py`.
- **`Finding`**: frozen dataclass at
  `bootstrap/recovery/models.py:68`. One drift signal raised by one
  detector. Hashable, persistent across reboots via `make_finding_id`.
- **`HealResult`**: frozen dataclass at
  `bootstrap/recovery/models.py:157`. What a heal returns.
- **`Migration`**: abstract base at
  `bootstrap/recovery/migrations/base.py:120`. One unit of versioned
  schema migration.
- **`BulkContext`**: Phase 8.3 envelope kwarg passed from the
  orchestrator to a heal so the heal skips its own snapshot wrap and
  participates in the orchestrator's outer envelope (atomic groups).
- **Connector-owned file**: one of the eleven `.bean` files Lamella
  writes to (see `CLAUDE.md` "Connector-owned files"). Everything
  else is user-authored and never rewritten.

---

## 3. The State Machine

### 3.1 The six states

`bootstrap/detection.py:40-46` defines `LedgerState`. The classifier
at `detect_ledger_state(main_bean_path)` runs in this order:

| Order | State | Trigger | User experience |
|-------|-------|---------|-----------------|
| 1 | `MISSING` | `main.bean` doesn't exist | First-run wizard |
| 2 | `UNPARSEABLE` | `loader.load_file` raised or returned a fatal error | Error page; manual fix required |
| 3 | `NEEDS_NEWER_SOFTWARE` | Has version stamp, but `version > LATEST_LEDGER_VERSION` | Refusal page; app refuses to boot |
| 3 | `READY` | Has `custom "lamella-ledger-version" "1"` directive **and** version == LATEST | Dashboard |
| 3 | `NEEDS_MIGRATION` | Has version stamp, but `version < LATEST_LEDGER_VERSION` | `/setup/recovery` migration prompt |
| 4 | `NEEDS_VERSION_STAMP` | Parses, has Transaction/Balance/Pad content, no stamp | "Confirm + stamp" page |
| 5 | `STRUCTURALLY_EMPTY` | Parses cleanly, no content, no stamp | First-run wizard |

The `NEEDS_NEWER_SOFTWARE` branch checks `version > LATEST_LEDGER_VERSION`
**before** the equality / `<` branches, so a v(N+1) ledger never falls
through to `READY` on a v(N) install. See §3.2 + §6.3 for the resolved
forward-compat behavior.

Two convenience properties at `detection.py:57-81`:

- `needs_setup`: True for MISSING / UNPARSEABLE / STRUCTURALLY_EMPTY
  / NEEDS_VERSION_STAMP. App refuses to serve dashboard.
- `can_serve_dashboard`: True for READY / NEEDS_VERSION_STAMP /
  NEEDS_MIGRATION. Gentler; a stamp-less but populated ledger can
  still render reports while we nag about the stamp.

### 3.2 Forward-compat refusal: `NEEDS_NEWER_SOFTWARE`

Resolved 2026-04-26 (`decisions-pending.md` §2.1).

When older Lamella detects a ledger written by newer Lamella
(`version > LATEST_LEDGER_VERSION`), it **refuses to boot** and
renders a clear error page: *"your ledger is from a newer version
of Lamella; upgrade or restore from backup."* No silent boot. No
warning-and-continue. No subsequent write that could corrupt
v(N+1) data through a v(N) writer.

The implementation is not just the refusal check at
`detection.py`. It's a real version-tracking system, with four
load-bearing pieces that move together:

1. **`LATEST_LEDGER_VERSION` constant in code** (`detection.py:37`):
   the highest schema version this build understands without
   translation.
2. **Layout markers in the ledger**:
   `custom "lamella-ledger-version" "N"` written by the three
   stamp sites listed in §3.3.
3. **Version-bump checklist** (§6.2 + §11 gap #10): six things
   that MUST land in the same commit when the constant goes from
   N to N+1.
4. **Introspection test** (§11 gap #2): every `lamella-*` key a
   writer stamps must be registered in `OWNED_CUSTOM_TARGETS` or
   in a `RETIRED_KEYS` allowlist, so a new key landing without a
   registration entry fails CI.

The refusal page is the most-visible piece, but the
infrastructure underneath is what makes the refusal meaningful.
Without (1) to (4), the page is a façade; there's no reliable way
to know whether v(N+1) data has actually drifted from v(N).

### 3.3 The version stamp itself

The directive on disk is:

```beancount
2026-01-01 custom "lamella-ledger-version" "1"
```

Three places write it (idempotently; all check for an existing
stamp and skip):

1. `bootstrap/import_apply.py::_inject_version_stamp`: fires when
   the user finishes `/setup/import`.
2. `bootstrap/templates.py:104,121,130`: included in the scaffold
   templates emitted by the first-run wizard.
3. `bootstrap/recovery/migrations/migrate_ledger_v0_to_v1.py:46-49,
   162-177`: the v0→v1 migration writes it after backfilling
   directives from SQLite.

`_extract_ledger_version` at `detection.py:202-219` reads it: walks
entries, finds the `Custom` of type `lamella-ledger-version`, parses
the first value as int. Beancount sometimes wraps custom values in
an Amount-like object; line 213-214 unwraps via `.value` if needed.

The at-load compat in `_legacy_meta.normalize_entries` runs
**before** the version check (`detection.py:119-120`), so a
pre-rebrand ledger with a `bcg-ledger-version` directive is read
correctly without on-disk changes.

---

## 4. Gating: When the User Sees Recovery

### 4.1 The decision

Setup-completeness gating lives in
`bootstrap/setup_progress.py::compute_setup_progress` (lines
698-726). It returns a `SetupProgress` carrying ~12 `SetupStep`
objects:

- entities, accounts, charts, companions, vehicles, properties, loans
- schema drift (`_check_schema_drift`, lines 729-791)
- legacy paths (`_check_legacy_paths`, lines 794-858)
- import applied
- SimpleFIN

Any required step that's incomplete forces the gate. The dashboard
checks `progress.is_complete` and redirects to `/setup` (first-run
flavor) or `/setup/recovery` (drift flavor) accordingly.

### 4.2 Cadence

`compute_setup_progress` is **computed per request, not cached.**
Every dashboard render parses the ledger fresh, queries SQLite, and
runs each detector. There is no startup cache, no mtime invalidation,
no SSE invalidation hook. The cost is bounded by the per-detector
cost; today it's fast enough that nobody's measured it.

If a future detector becomes expensive (e.g., walks every posting),
the right fix is per-detector caching keyed on
`(main_bean_path.stat().st_mtime, sqlite_user_version)`, not a
global cache. The orchestrator already re-detects between groups
(`bulk_apply.py:544-549`) so it would need to opt out of the cache
during apply.

### 4.3 Wizard vs recovery: same gate, two surfaces

`/setup` (the wizard) and `/setup/recovery` share `setup_progress`.
The difference is presentation: the wizard is a linear walk for
first-run; recovery is a list of findings with batch controls for
ongoing drift. A user can move between them freely; the underlying
state is the same.

---

## 5. Schema-Evolution Patterns

This is the most important section for day-to-day development. Four
patterns coexist in the codebase. When you add a new metadata key,
rename a directive, or change how data is shaped on disk, **pick one
of these patterns and document the pick.** Don't invent a fifth.

### Pattern A. At-Load Compat: `_legacy_meta.normalize_entries`

**What it is.** A pure read-side rewrite that runs at parse time.
On every `LedgerReader.load()`, every `bcg-*` key/tag/custom-type is
mirrored to `lamella-*` in-memory; legacy txn-level source keys are
mirrored down to first-posting paired source meta. **Disk content is
never modified.**

**When to use it.** A read-shape change that has a clean new shape
and an old shape readable by translation. Examples in the codebase:

- The `bcg-*` → `lamella-*` rebrand
- The txn-identity migration (txn-level `lamella-simplefin-id` →
  posting-level paired source meta)

**Cost.** Permanent. Pattern A code lives forever; the moment you
add a compat rule, the codebase carries it until every ledger in the
wild has been physically rewritten. Run by `LedgerReader` and a small
number of direct `loader.load_file` callers.

**When NOT to use it.** When the old shape is genuinely ambiguous
(can't be mechanically translated). When the rewrite would silently
change downstream computation. When the old shape carries data that
no longer maps to anything.

### Pattern B. On-Touch Normalize: `rewrite/txn_inplace.py`

**What it is.** Opportunistic on-disk normalization. Every time a
transaction is edited (e.g., `rewrite_fixme_to_account`), the
post-edit bytes opportunistically migrate to the new shape:
`_opportunistic_normalize` (lines 60-85) mints a missing
`lamella-txn-id`, migrates legacy txn-level source keys, drops
retired keys. Failures log but don't block the main rewrite; the
post-write `bean-check` is the guard.

**When to use it.** As a *companion* to Pattern A, when you want
old ledgers to converge to the new shape over time without forcing
a one-shot migration. The user clears legacy meta as they
categorize. No user action required.

**Cost.** Cheap. The normalizer runs on every rewrite anyway; adding
a new rule is a few lines.

**When NOT to use it alone.** It only converges if the user touches
the row. Cold rows in a long-tail ledger may carry the legacy shape
forever. Pair with D for the cold case.

### Pattern C. Explicit Versioned Migration: `MigrateLedgerV0ToV1`

**What it is.** Bump `LATEST_LEDGER_VERSION`, write a `Migration`
subclass, register in `MIGRATION_REGISTRY`, and a versioned heal
fires from `/setup/recovery`. Wraps in a snapshot envelope, runs
bean-check, rolls back atomically on failure.

**When to use it.** When the change can't be expressed as
read-side translation:

- Backfilling SQLite state into ledger directives (the v0→v1 case;
  pre-Phase-5 ledgers had no `dismissals.bean` rows; the migration
  read SQLite, wrote directives, then stamped v1).
- Restructuring a directive in a way that breaks readers (rare;
  prefer A+B+D when possible).
- Anything where the ledger needs to be atomically transformed
  before any new feature can read from it correctly.

**Cost.** Heavy. Requires:
- A new `Migration` subclass with `dry_run` + `apply` + `declared_paths`
- Registration in `MIGRATION_REGISTRY` (currently keyed by axis +
  source-version range)
- A confirm screen if `SUPPORTS_DRY_RUN = False`
- A `LATEST_LEDGER_VERSION` bump and a coordinated software release
- Updates to all code that branches on `ledger_version`

**When NOT to use it.** When the change can be done without a
version bump via A+B+D. The v0→v1 migration was used because
SQLite-state-to-ledger-state is genuinely a recompute. The
txn-identity migration deliberately used A+B+D *instead* of bumping
v2, because the new shape is mechanically derivable from the old
shape and nothing breaks if a rewrite happens lazily.

### Pattern D. Optional Bulk Transform: `transform/*.py`

**What it is.** A standalone CLI tool (or a one-shot route) that
walks every `.bean` file under `ledger_dir` and rewrites legacy
shapes to canonical ones in-place, with snapshot-and-restore on
each file. Examples: `transform/bcg_to_lamella.py`,
`transform/normalize_txn_identity.py`.

**When to use it.** As the final companion to A+B. After A makes the
new shape readable transparently and B converges touched rows, D
gives the user a "convert everything now" button when they want a
clean slate.

**Cost.** Medium. One module + one CLI entrypoint + one route handler
+ a `/setup/recovery` button. No version bump, no migration registry
touch. Bean-check vs baseline; restore on new errors.

**When NOT to use it.** When A+B alone are sufficient and convergence
is expected within reasonable time. Don't write D for a transient
compat rule that will be removed in two releases.

### Decision matrix

| Change shape | Pattern |
|--------------|---------|
| Renamed metadata key, mechanically translatable | A + B + D (no version bump) |
| Renamed metadata key, lossy or ambiguous | C (version bump) |
| Moved a value from txn-meta to posting-meta | A + B + D |
| Reshape that requires reading SQLite to backfill | C |
| Added a new directive type readers may emit | A only (read both names) plus a deprecation period |
| Changed semantics of an existing directive value | C, almost always |
| New optional metadata key (additive, no compat needed) | None, just emit |

### The version-bump rule

Resolved 2026-04-26 (`decisions-pending.md` §2.2).

`LATEST_LEDGER_VERSION` bumps **only when an older version of
Lamella would genuinely misread or corrupt new-format data.** Two
explicit non-triggers:

- **Don't bump for additive changes that older versions can ignore.**
  A new optional metadata key that older readers simply skip is not
  a corruption risk; it's an A-only change.
- **Don't bump for silent on-touch normalizations** (Pattern B). The
  whole point of B is that the new shape is mechanically derivable
  from the old shape, so a v(N) reader on v(N+B-rule) data still
  reads correctly via Pattern A.

Worked example: the SimpleFIN-id → `lamella-txn-id` migration
**would** have qualified as a breaking-change bump under this rule
(the new shape lives at posting-meta level, not txn-meta level, and
older code reading raw `meta.get("lamella-simplefin-id")` would see
nothing). It did **not** bump because there are no other Lamella
installs yet (pre-launch freebie). Once the project goes public
(`lamella-ai/lamella` per `LAMELLA.md`), this rule applies for real
and the next breaking change does bump v2.

---

## 6. The Versioning Contract

### 6.1 What `LATEST_LEDGER_VERSION` means

`LATEST_LEDGER_VERSION = 1` at `detection.py:37`. The constant is
the highest ledger schema version this build of the software knows
how to read **without translation**. It is bumped when:

- A breaking change is made that A+B+D cannot express (per §5).
- `LEDGER_LAYOUT.md` receives a breaking change (per §12.3 of that
  doc; currently the canonical reason cited in the constant's
  docstring).
- The software's own semver major version bumps.

The version stamp is **not tax-relevant**, **not user-meaningful**,
and **not part of the ledger's accounting content.** It exists
entirely as a schema marker so future migrations can run safely.

### 6.2 What a bump implies

If you bump `LATEST_LEDGER_VERSION` from N to N+1, you must:

1. Add a `Migrate*VNToVN1` subclass under
   `bootstrap/recovery/migrations/`.
2. Register it in `MIGRATION_REGISTRY` keyed by axis +
   from-version=N + to-version=N+1.
3. Update the heal-dispatch in
   `bulk_apply._heal_one()` if your migration needs special routing.
4. Add a confirm screen at `/setup/recovery/schema/confirm` if
   `SUPPORTS_DRY_RUN = False`.
5. Update `LEDGER_LAYOUT.md` §12.3.
6. Write a regression test that loads an N-stamped ledger, runs the
   migration, and asserts the post-stamp is N+1 + bean-check passes.

### 6.3 Forward compatibility (refusal, resolved 2026-04-26)

When v(N+1) ships, v(N) software refuses to boot against a v(N+1)
ledger and renders a clear message ("your ledger is from a newer
version of Lamella; upgrade or restore from backup"). See §3.2
for the full state-machine entry and §2.1 of `decisions-pending.md`
for the resolution log.

Implementation pieces (track these together; none of them is
useful alone):

- `LedgerState.NEEDS_NEWER_SOFTWARE` enum value.
- Detection branch at `detection.py`: `version > LATEST` check
  ordered **before** the equality / `<` checks.
- Setup-progress step that surfaces the refusal as a hard gate.
- Refusal page template under `templates/setup_recovery/`.
- Whatever guard prevents `compute_setup_progress` from running
  expensive detectors against a refused ledger (no point walking
  postings we can't safely write to).

---

## 7. Findings and Heals

### 7.1 The detect/heal split

Detectors are pure functions `(conn, entries) -> tuple[Finding, ...]`.
They live under `bootstrap/recovery/findings/`. Today there are two:

- `findings/legacy_paths.py::detect_legacy_paths`: non-canonical
  account paths (hand-entered, old-importer outputs)
- `findings/schema_drift.py::detect_schema_drift`: version
  mismatches (ledger stamp < v1, missing SQLite columns)

The aggregator at `findings/__init__.py::detect_all` materializes
entries to a list once, calls each detector in registration order,
catches per-detector exceptions (logged, that detector's output
becomes empty), and concatenates results.

Heals live under `bootstrap/recovery/heal/`. Today there are two,
one per category. The dispatch at `bulk_apply._heal_one()` (lines
1006-1055) is a hand-coded `if category == ...` chain. There is no
auto-discovery; new categories must be wired manually.

### 7.2 The Finding shape

`models.py:68-118`. Frozen, hashable, immutable. Every field is
load-bearing:

- `id`: `make_finding_id(category, target)` (lines 40-65). Stable
  across reboots so the user's draft (`apply` / `dismiss` / `edit`)
  in `setup_repair_state` survives a restart.
- `category`: one of seven (`legacy_path`, `schema_drift`,
  `orphan_ref`, `missing_scaffold`, `missing_data_file`,
  `unset_config`, `unlabeled_account`).
- `target_kind` + `target`: the subject (account path, entity slug,
  file path, etc.).
- `proposed_fix`: tuple of `(key, value)` pairs (hashable; dict
  accessor at `proposed_fix_dict`). The heal-action payload.
- `alternatives`: tuple of alt fix-tuples. Dismiss is implicit.
- `confidence`: drives default-checked vs unchecked in batch UI.
- `requires_individual_apply`: Phase 6.1.4d per-finding override.
  When True, batch controls are hidden for this row; user must use
  the individual confirm flow. The HTMX writers refuse 400 if a
  curl-POST tries to compose drafts against a True-flagged finding.

### 7.3 The HealResult shape

`models.py:157-176`. `success: bool`, `message: str`, `files_touched:
tuple[Path, ...]`, `finding_id: str`. `files_touched` is for logging,
not for restore; restore is the snapshot envelope's job.

---

## 8. The Bulk-Apply Orchestrator

`bulk_apply.py` runs as a job-runner worker (the route POSTs to
`JobRunner.submit` and returns a progress modal pointed at the job
id). It emits `BatchEvent` instances over SSE.

### 8.1 Group definitions

`CATEGORY_GROUP` at `bulk_apply.py:180-198`:

- **Group 1: schema.** `schema_drift`. Best-effort. Partial success
  is OK; failures don't block subsequent groups.
- **Group 2: labels.** `unlabeled_account`, `unset_config`. Atomic.
- **Group 3: cleanup.** `legacy_path`, `orphan_ref`,
  `missing_scaffold`, `missing_data_file`. Atomic.

Groups always run in this order so a schema migration can land before
labels reference newly-created columns, and labels can land before
cleanup that depends on them.

### 8.2 Atomic vs best-effort

**Best-effort (Group 1)** at `_run_group_best_effort` (lines
643-723). Each finding's heal is wrapped in its own snapshot envelope.
Per-finding failures are reported as `FindingFailed` events and the
group continues. Used for schema migrations because SQLite DDL
commits are non-rollback-able by an outer transaction; pretending
to be atomic would lie.

**Atomic (Groups 2+3)** at `_run_group_atomic` (lines 776-908):

1. Collect declared paths (worst-case superset including every
   connector-owned file, main.bean, every loaded source).
2. Open ONE outer `with_bean_snapshot` envelope around the entire
   group.
3. For each finding: heal with `bulk_context` set so the heal skips
   its own snapshot wrap and writes directly.
4. Buffer `FindingApplied` events locally.
5. On a clean group exit: flush buffered events; emit
   `GroupCommitted`.
6. On any per-finding failure: `_GroupRollbackTrigger` fires; the
   outer envelope catches it; every declared file restores
   byte-identically; buffered events are dropped; emit
   `GroupRolledBack` with the failure list.

This preserves the SSE invariant: **every visible `FindingApplied`
corresponds to a write that committed.**

### 8.3 Pre-flight edit validation

Phase 8.8. `_preflight_edit_payloads()` (lines 914-978) walks every
selected finding before any group runs. For `legacy_path` with
`action=edit`, it validates the user's edited canonical destination
via `_passes_destination_guards()`. If any pre-flight check fails,
the orchestrator emits `FindingFailed × N` + `BatchDone(failed)`
with `summary["preflight_failures"]` carrying the list, without
starting any group. The user sees every stale edit in one pass and
can fix them all before retrying.

### 8.4 Re-detection between groups

`bulk_apply.py:544-549` re-runs detectors between groups. The
orchestrator silently filters to findings that still exist; new
findings are ignored for this run. There is no concurrency guard;
if the user runs another `/setup/...` action in another tab
mid-recovery, the resulting drift is silently filtered, not
flagged.

---

## 9. Migrations: The Versioned Path

### 9.1 The Migration interface

`bootstrap/recovery/migrations/base.py:120-238`. Subclasses set:

- `AXIS: str`: `"sqlite"` or `"ledger"`. Dispatches to the right
  detector → heal path.
- `SUPPORTS_DRY_RUN: bool = True`: if False, the route forces a
  confirm step before apply.

And implement:

- `declared_paths(settings) -> tuple[Path, ...]`: the snapshot
  envelope's protection set. SQLite-only migrations return `()`.
- `dry_run(conn, settings) -> DryRunResult`: pure preview.
- `apply(conn, settings) -> None`: the actual write. Caller wraps
  in SQLite BEGIN + bean-snapshot + bean-check + COMMIT/ROLLBACK.

`failure_message_for(exc)` is a class method with a default that
classifies `BeanSnapshotCheckError`, `sqlite3.IntegrityError`,
`sqlite3.OperationalError`, `PermissionError`, `OSError`, and
`MigrationError`. Subclass override only when migration-specific
context sharpens the message.

### 9.2 DryRunKind

`base.py:73-75` defines four kinds:

- `rename`: pure rename (column / file). Diff renders inline.
- `additive`: schema additions (new column / table / directive
  type). Renders "this adds N items; existing data is not modified."
- `recompute`: derived-state recompute. Honest preview is
  expensive; renders "cannot be previewed cleanly; confirm to
  apply."
- `unsupported`: migration declared no dry-run. Renders the same
  confirm step as `recompute`.

### 9.3 The current registry

Today `MIGRATION_REGISTRY` has one entry: `MigrateLedgerV0ToV1`.
Adding a v1→v2 migration means adding the subclass, registering it
keyed by axis + version range, and ensuring the heal-dispatch path
in `bulk_apply._heal_one()` knows how to find it for a `schema_drift`
finding with the right detail payload.

The `@register_migration` decorator (§11 gap #5) is in flight as of
2026-04-26; until it lands, a subclass added without a manual
registry entry is dead code with no warning. After it lands,
subclassing + decorator is enough; the import side-effect populates
the registry.

---

## 10. The Classifier: File Ownership

`bootstrap/classifier.py::OWNED_CUSTOM_TARGETS` (lines 63-131) is
the canonical map of which connector-owned `.bean` file owns each
custom directive type Lamella writes. Excerpt:

```python
"lamella-ledger-version": "main.bean",
"receipt-link": "connector_links.bean",
"classification-rule": "connector_rules.bean",
"entity": "connector_config.bean",
"account-meta": "connector_config.bean",
# ... 40+ entries
```

Used during Import analysis to route directives to the canonical
layout and during recovery heals to know where a given directive
should land. **The introspection test
`tests/test_lamella_keys_registered.py` (resolved §11 gap #2,
2026-04-26) walks every writer module's AST, collects every
`lamella-*` key it stamps, and asserts each one is registered here
or in the explicit `RETIRED_KEYS` allowlist.** Adding a writer
without updating the classifier fails CI on the next test run.
Pre-test: the new directive would have landed in whichever file
the writer hard-coded, which may not have matched the intended
ownership. See §11 gap #2 for the full status entry.

---

## 11. Known Gaps

The system has shipped through Phase 8 and is in production use.
The 2026-04-26 walk-through (`decisions-pending.md` §3) scheduled
all ten gaps for action; none deferred indefinitely. Status column
reflects that schedule; "later" gaps are sequenced, not abandoned.

References below to "decisions §X.Y" point at sections of
`decisions-pending.md` (the 2026-04-26 walk-through answer-of-record).

| # | Gap | Status | Resolution |
|---|---|---|---|
| 1 | No `NEEDS_NEWER_SOFTWARE` state; v(N+1) ledger boots v(N) app as `READY` | **Resolved** by decisions §2.1; see §3.2 here | Implementer wires the state + refusal page |
| 2 | No automated registry enforcement for new `lamella-*` keys | **Resolved** by decisions §2.3 (introspection test) | `tests/test_lamella_keys_registered.py` |
| 3 | No honest dry-run for recompute migrations | **Action later** | Needs scratch-ledger infra; lands when that infra exists |
| 4 | No mid-session drift re-detection | **Resolved** by decisions §2.4 (narrow on-write triggers) | See §12.6 trigger set here |
| 5 | No migration auto-discovery | **Action (in flight)** | `@register_migration` decorator |
| 6 | Repair-state staleness across restart | **Action** | Extend pre-flight validation across all categories (not just `legacy_path` edit) |
| 7 | No concurrency guard on `/setup/recovery` | **Action (in flight)** | Single in-flight flag in SQLite (~30 min) |
| 8 | Bulk-applicable defaults to True | **Action (in flight)** | Flip default; audit existing categories (<15 min) |
| 9 | `SUPPORTS_DRY_RUN=False` migrations need a confirm route | **Action later** | Auto-generate confirm screen when the second case appears |
| 10 | No version-bump checklist enforcement | **Action later** | Lands alongside the next actual version bump (which decisions §2.1's infrastructure makes meaningful) |

### Per-gap detail

1. **`NEEDS_NEWER_SOFTWARE` state: RESOLVED.** See §3.2 + §6.3.
   The state, detection branch, setup-progress step, and refusal
   page are the implementation contract.

2. **Registry enforcement: RESOLVED via introspection test.** A
   `tests/test_lamella_keys_registered.py` walks every writer
   module's AST, collects every `lamella-*` key it stamps, and
   asserts each one is registered in the classifier's
   `OWNED_CUSTOM_TARGETS` / `KEY_REGISTRY` or explicitly listed
   in a `RETIRED_KEYS` allowlist. Runs on `pytest`. New writer
   adds a new key → test fails until the key is registered. No
   pre-commit hook needed; the test catches the regression on PR
   re-run uniformly across machines. Non-test responsibilities
   that the developer still owns (compat rule in
   `_legacy_meta.normalize_entries` if replacing an older shape;
   `LEDGER_LAYOUT.md` "Metadata schema" table update; detector if
   absence of the directive is a drift signal) are listed in
   §12.1 and not enforced by the test; the test guarantees the
   write side, not the documentation side.

3. **Honest dry-run for recompute migrations: DEFERRED.** True
   preview needs a scratch ledger copy. The `recompute` and
   `unsupported` `DryRunKind` values exist as an admission of this
   limitation. Lands when the scratch-ledger infrastructure exists
   (currently no scheduled work for that infra).

4. **Mid-session drift re-detection: RESOLVED via narrow
   triggers.** Detection re-runs at the END of specific writer
   paths that mutate ledger format. NOT polling. NOT every
   request. See §12 for the trigger set.

5. **Migration auto-discovery: IN FLIGHT.** Worker 3 of the
   2026-04-26 swarm is implementing the `@register_migration`
   decorator. Migration subclasses no longer require manual
   registry wiring; subclassing + decorator is enough.

6. **Repair-state staleness: SCHEDULED.** Pre-flight validation
   currently catches `legacy_path` edit case (Phase 8.8). Action
   is to extend the same pre-flight discipline across all finding
   categories so a stale draft fails before a snapshot opens, not
   inside a group.

7. **Concurrency guard: IN FLIGHT.** Worker 3 is implementing a
   single in-flight flag in SQLite. Recovery surface holds the
   flag for the duration of a bulk run; the second tab gets a
   "another recovery is in progress" page instead of an
   interleaved snapshot envelope.

8. **Bulk-applicable defaults to True: IN FLIGHT.** Worker 3 is
   flipping `_BULK_APPLICABLE`'s default to False (opt-in per
   category) and auditing every existing category to add an
   explicit True entry where batching is correct. Trivial change;
   the audit is the work.

9. **Confirm route for `SUPPORTS_DRY_RUN=False` migrations:
   DEFERRED.** Auto-generate the confirm screen when the second
   `SUPPORTS_DRY_RUN=False` migration appears (currently zero;
   only `MigrateLedgerV0ToV1`, which supports dry-run). Don't
   build the abstraction for a single case.

10. **Version-bump checklist enforcement: DEFERRED.** Lands
    alongside the next actual version bump, which §2.1's
    infrastructure (constant + markers + checklist + introspection
    test) makes meaningful for the first time. The check itself is
    a small script that asserts the six §6.2 items co-occur in the
    same commit; cheap to write, hard to test until there's a real
    bump to gate.

---

## 12. Developer Playbook

### 12.1 "I'm adding a new `lamella-*` metadata key"

1. Decide: txn-meta or posting-meta? Use `identity.py` helpers as
   the model for paired posting keys.
2. Add the writer (in `simplefin/writer.py`, `importer/emit.py`,
   or wherever).
3. Add a reader helper to the canonical reader module if downstream
   code will consult it from many places.
4. Update `LEDGER_LAYOUT.md` "Metadata schema" table.
5. Update `CLAUDE.md` "Live keys today" table if user-facing.
6. **Do not** add to `_legacy_meta.normalize_entries` unless this is
   replacing an older shape.

### 12.2 "I'm renaming an existing `lamella-*` key"

1. Pick Pattern A+B+D from §5.
2. Add the rename to `_legacy_meta.normalize_entries`.
3. Update the writer to emit the new name.
4. Add the rename to `rewrite/txn_inplace._opportunistic_normalize`.
5. Add a one-shot transform under `transform/` for the bulk case.
6. Add a `/setup/recovery` button surfacing the transform if you
   want it user-visible.
7. **Do not** bump `LATEST_LEDGER_VERSION`.

### 12.3 "I'm restructuring data in a way that requires backfill"

1. Pick Pattern C from §5.
2. Bump `LATEST_LEDGER_VERSION` (this requires a coordinated release).
3. Write the `Migration` subclass.
4. Register in `MIGRATION_REGISTRY`.
5. Update heal-dispatch in `bulk_apply._heal_one()` if needed.
6. Write a confirm route if `SUPPORTS_DRY_RUN = False`.
7. Update `LEDGER_LAYOUT.md` §12.3.
8. Add a regression test loading an N-stamped ledger, running the
   migration, asserting v(N+1) + bean-check passes.
9. Verify v(N+1) software gracefully handles a v(N) ledger via the
   existing `NEEDS_MIGRATION` path.
10. Verify v(N) software refuses a v(N+1) ledger with the
    `NEEDS_NEWER_SOFTWARE` refusal page (§3.2 + §6.3).
11. Apply the version-bump rule from §5: this bump is justified
    only because v(N) would genuinely misread or corrupt the v(N+1)
    shape. Additive changes and silent on-touch normalizations don't
    qualify.

### 12.4 "I'm adding a new connector-owned `.bean` file"

1. Update `LEDGER_LAYOUT.md` file list.
2. Update `CLAUDE.md` "Connector-owned files" list.
3. Update `OWNED_CUSTOM_TARGETS` in `classifier.py` for any directive
   types that should land in the new file.
4. Add an `Open` directive emission if the file is fresh-on-first-use.
5. Wire bean-check coverage if the file isn't included in main.bean.

### 12.5 "I'm adding a new detector"

1. Write the detector under `findings/`. Pure function `(conn,
   entries) -> tuple[Finding, ...]`.
2. Register in `findings/__init__.py::DETECTORS`.
3. Pick a `category` (extend `CATEGORIES` in `models.py` if new).
4. Write the matching heal under `heal/`.
5. Wire dispatch in `bulk_apply._heal_one()`.
6. Decide bulk-applicable: add to `_BULK_APPLICABLE` map.
7. Add to the appropriate group in `CATEGORY_GROUP`.
8. Decide if any finding should set `requires_individual_apply=True`.
9. Add a `_check_*` to `setup_progress.py` if the detector should
   gate the dashboard.
10. Test the detect/heal round-trip end-to-end.

### 12.6 "I'm adding a writer that mutates ledger layout"

Re-run drift detection at the **end** of any code path that could
change ledger format mid-session. This is the §11 gap #4 contract:
narrow on-write triggers, NOT polling, NOT every request.

The trigger set is the small list of paths that *can* mutate
layout; everything else is a pure read or a non-layout write
(transaction-level edits, posting amount changes) and doesn't need
re-detection.

Trigger set today:

- **Transform-module CLI runs.** Every
  `python -m lamella.transform.*` entrypoint (e.g., `bcg_to_lamella`,
  `normalize_txn_identity`, `key_rename`, `migrate_to_ledger`,
  `reconstruct`, `verify`). These walk every `.bean` file and can
  reshape directives at scale.
- **Recovery actions.** Every `/setup/recovery` writer that lands
  a heal or migration. After the bulk-apply orchestrator emits its
  final `BatchDone`, drift re-detection runs once.
- **Any other writer that mutates layout (TBD by implementer).** For example, a future bulk re-categorize or a one-shot config
  rewrite. If the writer touches connector-owned `.bean` file shape
  (not just transaction content), add it to the trigger set.

Out of scope for re-detection (intentionally; these are layout-stable
or already covered by per-write bean-check + snapshot rollback):

- Single-transaction in-place rewrites (`rewrite/txn_inplace`):
  edits one posting line, snapshot+restore on failure.
- SimpleFIN ingest: appends new transactions in canonical shape;
  doesn't reshape existing directives.
- Receipt-link writes: append-only.
- Override writes: append-only.

If you're unsure: does this writer change the *shape* of the
ledger (move a field's location, rename a directive type, restructure
a custom directive) in a way that a fresh `_legacy_meta.normalize_entries`
read would expose new compat work? If yes, add to the trigger set.
If it just adds a transaction or edits an existing field in place,
it doesn't belong here.

---

## 13. References

- Code: `src/lamella/bootstrap/`, `src/lamella/bootstrap/recovery/`
- Routes: `src/lamella/routes/setup_recovery.py`,
  `src/lamella/routes/setup.py`
- Templates: `src/lamella/templates/setup_recovery/`
- Companion docs: `docs/LEDGER_LAYOUT.md`,
  `docs/NORMALIZE_TXN_IDENTITY.md`,
  `docs/MIGRATE_SIMPLEFIN_MAP_TO_LEDGER.md`
- Design context: `CLAUDE.md` "Non-negotiable architectural rules",
  "Canonical ledger layout", "Connector-owned files"

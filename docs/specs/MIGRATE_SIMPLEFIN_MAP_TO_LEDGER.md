# Migrate SimpleFIN account map from YAML to ledger directives

**Status**: planned, not started.
**Audience**: an agent or developer picking this up cold.

---

## Why this matters

`simplefin_account_map.yml` is the only piece of bcg-managed user
state that lives outside the ledger. Per the "ledger is the single
source of truth" rule in `CLAUDE.md`, that's a violation: if the
SQLite cache is wiped, the YAML *does* get re-read so the registry
recovers; but if the YAML is deleted or corrupted, the ACT-* →
account bindings vanish with no ledger-side audit trail and no way
for `transform/reconstruct.py` to recover them.

Goal: move the mappings into `connector_config.bean` (or a new
`connector_simplefin.bean`) as `custom "simplefin-account-mapping"`
directives, so `reconstruct` can rebuild the registry from the
ledger alone, every change is bean-checked, and the YAML becomes
optional/legacy.

---

## Current state

**File**: `<ledger_dir>/simplefin_account_map.yml`. Path resolved
in `src/lamella/config.py::Settings.simplefin_account_map_resolved`
(defaults to `ledger_dir / "simplefin_account_map.yml"`).

**Format** (flat dict; loader also tolerates a wrapped
`{"accounts": {...}}` shape):

```yaml
ACT-d3437d85-c9a6-4b1f-b5b9-53fb718502e0: Assets:Personal:BankOne:Checking
ACT-cf787654-e3d8-48cc-bf9b-2cbe34099062: Liabilities:Personal:BankOne:VisaSignature
```

**Readers** (all in `src/lamella/`):
- `simplefin/ingest.py::load_account_map(path)`: YAML loader.
- `registry/discovery.py::sync_simplefin_account_map(conn, map_path)`: copies mappings onto `accounts_meta.simplefin_account_id` at
  startup. Called from `sync_from_ledger`.
- `simplefin/ingest.py::Ingester.account_map` (property): reads
  on every fetch.
- `routes/simplefin.py`: settings page reads/writes the YAML
  for the SimpleFIN account-mapping UI.

There is no current writer module other than `routes/simplefin.py`
mutating the YAML directly.

---

## Target state

### Schema

New custom directive in `connector_config.bean` (or a dedicated
`connector_simplefin.bean`; see Decision 1 below):

```beancount
2026-04-25 custom "simplefin-account-mapping" "ACT-d3437d85-c9a6-4b1f-b5b9-53fb718502e0"
  bcg-account: Assets:Personal:BankOne:Checking
  bcg-set-at: "2026-04-25T10:00:00"
  bcg-description: "EVERYDAY CHECKING ...4085"
```

Plus a revoke variant for unmappings (mirrors
`recurring-revoked` / `setting-unset` patterns):

```beancount
2026-05-01 custom "simplefin-account-mapping-revoked" "ACT-d3437d85-..."
  bcg-revoked-at: "2026-05-01T12:00:00"
```

**Key fields**:
- The directive's string argument is the SimpleFIN ACT-* id
  (matches the `setting` pattern where the arg is the key).
- `lamella-account` carries the Beancount account path. **Note**:
  `custom` directive metadata cannot be a `Account` typed value
  in beancount 3.x; it's a string. Validation against opened
  accounts must happen in our reader, not via bean-check.
- `lamella-set-at` is the ISO timestamp of the write.
- `lamella-description` is optional; lets us preserve the trailing
  comments that today live in the YAML (`# EVERYDAY CHECKING ...4085`).

**Latest-wins semantics**: like `setting`, multiple directives
for the same ACT-* id are valid; the reader takes the latest by
date+timestamp, with revokes overriding.

### Writer

New module `src/lamella/connectors/simplefin_map_writer.py`
that exposes:

- `set_mapping(simplefin_id, account_path, description=None) -> None`
- `unset_mapping(simplefin_id) -> None`
- `read_all() -> dict[str, MappingEntry]` (latest-wins, revokes applied)

Discipline (mirror `rewrite/txn_inplace.py`):
1. Snapshot file to `.pre-simplefin-map-<ISO>/` under `ledger_dir`
   before any byte change.
2. Append the new directive (don't try to in-place edit existing
   ones; latest-wins lets us append safely).
3. Run `run_bean_check_vs_baseline`; on new errors, restore from
   snapshot and raise.
4. Refuse paths outside `ledger_dir`, under archive/reboot/backup
   dirs, or symlinks.

### Reader changes

Update `simplefin/ingest.py::load_account_map(path)` to:
1. First try ledger directives via `simplefin_map_writer.read_all()`.
2. Fall back to YAML if no directives found (legacy compat).
3. If both exist, ledger wins; emit a once-per-process warning
   asking the user to retire the YAML.

Update `registry/discovery.py::sync_simplefin_account_map`:
no signature change; it already calls through `load_account_map`.

Update `routes/simplefin.py`: every YAML-mutating code path
(set, unset, bulk-edit) must call the new writer instead of
rewriting YAML. UI behavior unchanged.

### Reconstruct

Add to `src/lamella/transform/reconstruct.py`:
- A new pass that reads `simplefin-account-mapping` (and
  `-revoked`) directives from the ledger and rebuilds
  `accounts_meta.simplefin_account_id` from scratch.
- Wire it into the existing reconstruct registration list so
  `--force` reset includes it.

### One-shot YAML → ledger transform

New module `src/lamella/transform/migrate_simplefin_map.py`:
- `--dry-run` (default): reads YAML, lists what would be written,
  detects edge-case violations (see below), exits.
- `--apply`: writes the directives via the new writer module
  (so each one bean-checks individually). Does **not** delete
  the YAML.
- After successful `--apply`, prints next steps (verify with
  reconstruct, then optionally rename YAML to `.legacy`).

### Tests

- Unit tests for the writer (set, unset, snapshot/restore, path
  safety).
- Reader test: ledger directives win over YAML; legacy YAML
  fallback works when no directives present.
- Reconstruct test: wipe `accounts_meta.simplefin_account_id`,
  run reconstruct, verify mappings rebuilt from ledger alone.
- Migration test: feed a YAML, run `--apply`, verify connector_config
  is bean-check-clean and `read_all()` round-trips.

---

## Edge cases

### Case 1: mapping references a non-existent account

The YAML may bind an ACT-* id to an account path with no `Open`
directive in the ledger (e.g., the account got renamed or never
opened). bean-check won't catch this because account names in
custom-directive metadata are strings, not Account typed values.

**Required behavior**:
- The migration transform's `--dry-run` mode emits a warning
  per missing-account mapping and lists each one with the
  ACT-* id, the proposed account, and the closest matches in the
  current chart of accounts (case-insensitive substring + Levenshtein).
- `--apply` does **not** auto-correct. It refuses to migrate any
  mapping with a missing target unless the user passes
  `--allow-missing` (which writes the directive anyway and lets
  setup-check surface it later).
- The system-side validator (new function in
  `setup_check.py` or wherever the data-integrity scan lives)
  finds these post-migration and surfaces them as a finding with
  the same closest-matches list and a "remap" / "delete mapping" /
  "open account" action.

### Case 2: mapping references the wrong account type

A mapping pointing at `Equity:OpeningBalances:...` or
`Expenses:...` is structurally wrong: SimpleFIN data flows
through bank/credit/loan accounts only, which live under
`Assets:` or `Liabilities:`.

**Required behavior**:
- Writer rejects any `set_mapping` call whose `account_path`
  does not start with `Assets:` or `Liabilities:`. Raises
  `InvalidAccountTypeForSimplefin` with the offending path.
- Migration transform's `--dry-run` flags wrong-type mappings
  and proposes the same path with `Assets:` / `Liabilities:`
  swapped (e.g., `Equity:OpeningBalances:Personal:BankOne:Visa`
  → suggest `Liabilities:Personal:BankOne:Visa`); only proposes
  if the swapped-prefix path actually exists as an opened account.
- `--apply` refuses wrong-type mappings unless the user passes
  `--accept-suggestions`, in which case it uses the suggestion
  and writes that.
- Setup-check surfaces wrong-type mappings post-migration with
  the same proposal flow.

### Case 3: YAML mapping uses a stale account path that has a clear successor

Specific to this codebase right now: the live YAML has
`Liabilities:Personal:BankOne:Mortgage` but the active mortgage
account is `Liabilities:Personal:BankTwoBank:BankTwoMortgage`.
This is a Case 1 instance, but worth calling out: the migration
transform should detect and surface, never auto-correct. Renames
require human judgment.

---

## Decisions for the agent to make

**Decision 1**: New file or extend `connector_config.bean`? Two
reasonable options:
- **(a) Extend `connector_config.bean`**: already includes
  `setting`, `paperless-field`. One file, fewer includes.
  Downside: file is already 56K and growing; mixing concerns.
- **(b) New `connector_simplefin.bean`**: clean separation;
  mirrors `connector_overrides.bean` / `connector_links.bean`
  per-concern split. Add include to `main.bean`.

Pick (b) unless there's a reason not to. Document the choice
at the top of whichever file you write to.

**Decision 2**: Migration window length. Per CLAUDE.md, "Don't
remove compat reads without a coordinated two-deploy window."
The plan above keeps YAML fallback indefinitely. Add a tracking
issue / FUTURE.md note for "remove YAML fallback after one
release of zero-fallback hits in production logs."

---

## Acceptance criteria

1. `connector_simplefin.bean` (or `connector_config.bean`) holds
   one `custom "simplefin-account-mapping"` per current YAML
   row, with `lamella-account`, `lamella-set-at`, and `lamella-description`
   metadata.
2. `bean-check main.bean` clean.
3. `simplefin_map_writer.read_all()` returns the same dict as
   `load_account_map(yaml_path)` did before migration.
4. Wiping `accounts_meta.simplefin_account_id` and running
   `reconstruct` rebuilds the SimpleFIN mappings from the ledger
   with no reference to the YAML.
5. The settings UI for SimpleFIN account mapping (in
   `routes/simplefin.py`) writes to ledger, not YAML, on every
   user action.
6. Unit + integration tests pass.
7. The migration transform documents Cases 1, 2, 3 in its
   `--dry-run` output for the live YAML and refuses `--apply`
   with violations unless the operator passes the relevant
   `--allow-*` / `--accept-*` flag.
8. The legacy YAML is **not** deleted by the migration; the
   operator decides when to remove it.

---

## Out of scope

- Renaming `simplefin_account_map.yml` or moving its location.
- Adding new SimpleFIN-related state types (e.g., last-fetched
  cursor, token bindings); those are separate features.
- Replacing the YAML loader at `simplefin/ingest.py::load_account_map`
  with a stub that errors out; legacy fallback stays for the
  migration window.
- Touching anything in `connector_links.bean`, `connector_overrides.bean`,
  or other connector files; this work is scoped to SimpleFIN
  account mappings only.

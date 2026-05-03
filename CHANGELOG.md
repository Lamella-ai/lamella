# Changelog

All notable changes to Lamella are documented here. The format roughly
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
the project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

Post-v0.3.1 work focused on sign-aware money rendering across the
app, refund detection, the AI-classify modal pipeline, and pre-public
sanitization.

### Public-release prep — pytest baseline (2026-04-29)

- **Cat A fixture-leak fixed.** Suite went from 522 fail / 549 errors /
  1453 pass at `87cebe8` to **0 unexpected failures** after one
  conftest commit + one round of cluster cleanup. Per-test
  `_isolate_process_state` autouse fixture snapshots/restores
  `os.environ` and clears the `get_settings` `@lru_cache` between
  tests (`bbeadbe1`).
- **`AI_VECTOR_SEARCH_ENABLED=0` set at conftest import** so every
  `Settings()` construction picks the segfault-safe default — closes
  the lifespan vector-search worker thread that was outliving its
  TestClient and segfaulting next test's SQLite handle.
- **`ledger_detection.needs_setup` bypass** in `app_client` fixture
  (`987032b7`); the dataclass's computed property silently swallowed
  the test's `det.needs_setup = False` assignment, causing routes to
  303 to `/setup` instead of rendering.
- **ADR-0058/0059 follow-ups landed.** Promotion-path narration
  synthesizer wired (`a5a56b26`); confirms ADR-0058 paste-path is
  not a redundant algorithm; verifies live-fetch source_description
  + lamella_txn_id threading was already correct.
- **40+ tests xfail'd** for pre-existing soft failures (bean-check
  not on test PATH, template-anchor drift, retired routes) with
  reasons linking back to `project_pytest_baseline_triage.md`.
- Real regressions fixed during cluster cleanup:
  `_source_href` fallback URL (`ebb4463a`/`6895eb12` — points at
  `/inbox` after the canonical staged-queue URL change),
  reports.py route ordering (`4d366a06`), `txn_hash`
  MISSING-sentinel handling for v0→v1 schema-drift heal
  (`afd13a92`), suggestion_cards path alignment per ADR-0045
  (`a33f1e8e`).
- **Inbox / Duplicates UX** (`c8f06eb7`): added a "Duplicates"
  button to the `/inbox` page header with a count badge. Mirrored
  `/review/duplicates` as `/inbox/duplicates` so the URL matches
  the inbox-mental-label convention; legacy URL kept as alias.
- **Ask-AI deposit-detect bug** (`a264c206`): the deposit
  short-circuit on `POST /api/txn/{ref}/ask-ai` was treating any
  positive amount as a deposit, which is correct for asset
  accounts but BACKWARDS for credit-card / line-of-credit / loan
  / mortgage rows where positive = a charge that needs AI
  classification. The AI was being silently skipped on the most
  common case (CC purchases). Fixed by resolving the source
  account's `kind` and flipping sign comparison for liabilities.

### Sign-aware money rendering

- Global Jinja `|money` filter now wraps output in a `<span class="money money--{pos|neg|zero}">` so accounting-sign placement
  (`-$X` vs `$-X`) is consistent across every template (a463c1d).
- `T.summary` macro, `_card_pane`, and `audit.html` now route the
  amount through the filter (a90d653, a463c1d); per-template
  txns-amount sites use `txns-amount--{in,out,flat}` modifiers.
- `routes/search.py` preserves upstream sign instead of stripping it (a463c1d);
  `ai_cascade/audit.py::_primary_amount` now reflects cashflow direction.
- Dashboard Net-worth and income tiles, `/inbox` row amounts (be6514e),
  and stale-deposit suppression now read sign correctly.

### Refund detection

- `RefundCandidate` dataclass + scoring helper (dea9c81).
- Classify stamps `lamella-refund-of` metadata on matched legs (836860e).
- `/txn/{token}` detail renders bidirectional refund link (4ad3f26).
- Deposit-skip modal surfaces candidate buttons inline (c625f93).

### Modal-classify pipeline

- `/inbox` AI modal Accept + Pick-myself unblocked; `htmx.ajax` shim added
  for templates that submit programmatically (949ea3e).
- In-place "Classified" tile via OOB swap, no full reload (e4ba4c7).
- Toast confirmation after classify (89fc48d).

### AI classification refinements

- Deposits skip the AI cascade entirely; manual Income classify only (7e79922).
- Sign-aware FIXME placeholder routing (e70d3c6) and root override on
  prompts (992faf9); Accept hidden on low confidence.
- Sign-aware whitelist for AI prompts (29caa3e); cross-entity whitelist
  widening on retry (704f9dd).
- 0-mile entries are negative reinforcement by default (fa71cde).

### Account UX

- `/settings/accounts` UI now supports Expenses / Income / Equity roots,
  not just Assets/Liabilities (670ba9d).
- Add-account modal auto-derives path from display name + entity + kind
  with strict `Top:Entity:Leaf` validation (8575dab).
- `account-guard` auto-scaffolds deeper branches under known entities
  rather than rejecting the write (7a8cc94).

### Reports navigation

- `/reports?entity=` filter param, `/reports/{slug}` (no year) redirect
  to the filtered matrix; `entity_type` humanize map (LLC, S-Corp, …) (a90d653).

### Misc UX and ADR drift

- Dashboard duplicate KPI tiles dropped; standard accounting sign
  placement everywhere (59a386b).
- Receipt-attach action and tests for staged `/txn/{token}` pages
  (9ebcf6d, 6f1752c).
- One-click COGS account seeder for Schedule C Part III inventory
  businesses (539da4f).
- `/txn` panel preserves UUIDv7 token through form actions so post-classify
  redirects don't lose lineage (e207b09).
- ADR drift sweep: [ADR-0019](docs/adr/0019-transaction-identity-use-helpers.md)
  helpers, [ADR-0042](docs/adr/0042-entity-first-design.md) entity-first
  preflight, [ADR-0041](docs/adr/0041-account-aliases.md) alias,
  [ADR-0011](docs/adr/0011-bank-accounts-and-cards.md) datalist (084bf4c).

### Public release sanitization

- Stage-2 / SaaS framing scrubbed from public docs (8c30656).
- `docs/core/PRODUCT_VISION.md` moved to `docs/internal/` (b8e9271, 1c2aa8b).
- README personal examples replaced with placeholders (8c30656).

## [0.3.1] (2026-04-29)

Same-day patch release. The marquee work is the ADR-0043 staged-txn
directive migration, landed across phases P0 → P7 in one sitting.
Default-off behaviour means v0.3.1 is byte-compatible with v0.3.0 unless
the operator opts into the new flag, hence PATCH per ADR-0052, not MINOR.

### ADR-0043: staged-txn directives replace FIXME postings

ADR-0043 has been the longest-pending architectural decision in the
project and the one most cited as "carry into the public site"
risk. v0.3.1 ships the full migration:

- **Frozen directive shape** in [ADR-0043b](docs/adr/0043b-staged-txn-directive-shape.md)
  (P0). Five gaps the parent ADR left open are resolved with
  explicit decisions: lamella-txn-id placement, multi-source closed
  enum (`simplefin / csv / paste / reboot`), sign convention, the
  staged-txn-promoted supplemental fields, and the balance-anchor
  format + timing.
- **New writer** ([P1](src/lamella/features/bank_sync/writer.py)).
  `render_staged_txn_directive` /
  `render_staged_txn_promoted_directive` /
  `SimpleFINWriter.append_staged_txn_directives` /
  `SimpleFINWriter.promote_staged_txn`. The metadata-only directives
  produce no balance-sheet impact while preserving the lamella-txn-id
  lineage all the way through to the eventual balanced txn.
- **Reconstruct support** ([P4](src/lamella/core/transform/steps/step24_staged_transactions.py)).
  step24 rebuilds the `staged_transactions` table from
  `custom "staged-txn"` and `custom "staged-txn-promoted"` directives.
  The directive is the source of truth; SQLite holds an ingest-time
  cache that round-trips through the directive shape.
- **Ingest wire-up** ([P2](src/lamella/features/bank_sync/ingest.py)).
  the bank-sync defer path optionally writes a directive per row
  alongside the staged_transactions row, gated by the new
  `enable_staged_txn_directives` setting (default OFF in this
  release; flip per-user during the soak window).
- **Atomic promotion writer** (P3). The high-risk phase. The classify
  endpoints now route through `promote_staged_txn` when the flag is
  on: in one bean-check pass under the writer lock, the staged-txn
  directive flips to staged-txn-promoted (audit anchor) AND a real
  balanced txn is appended. Both edits roll back together on
  bean-check failure.
- **Classify endpoint wiring** (P5). `/review/staged/classify` and
  `/review/staged/classify-group` use the promotion writer when the
  flag is on; mixed-state batches partition cleanly between
  promote-in-place and plain-append.
- **Legacy migration tool** ([P6](src/lamella/features/bank_sync/migrate_fixme_to_staged_txn.py)).
  one-shot bulk rewrite of pre-C1 FIXME-leg transactions to
  `custom "staged-txn"` directives. Snapshot + per-file bean-check
  + restore-on-failure. Ship as both a Python function and a CLI
  (`python -m lamella.features.bank_sync.migrate_fixme_to_staged_txn
  --ledger-dir <dir> [--apply]`). Recovery-UI button wiring is
  deferred to a v0.3.x patch. The API + CLI surface is enough for
  power users running the migration manually.
- **Test suite expansion** (P7). Five new test files cover
  directive shape, beancount round-trip, source-enum validation,
  reconstruct correctness, ingest wiring, atomic promotion
  rollback, and legacy migration eligibility / idempotency. Full
  staged-txn + writer regression at 151 passed.
- **Phase 8** (cleanup of FIXME-reading paths) is **explicitly
  deferred** to a future release after a soak window confirms zero
  new FIXMEs produced in real ledgers. Per the migration plan §7
  Risk 2, removing FIXME-reading code before that confirmation
  lands is how classify breaks for everyone on launch day.

### Operational

- `enable_staged_txn_directives = false` (default). No behaviour
  change vs. v0.3.0 unless the operator flips this from /settings.
- Closed enum on `lamella-source`: writer rejects unknown values
  via `InvalidSourceError` before any file mutation.
- Lineage invariant: the same UUIDv7 the staged-txn directive
  carries lands on the staged-txn-promoted directive AND on the
  appended balanced txn. `/txn/{token}` URLs are stable across
  the staging → promotion bridge.

### Required-reading docs (unchanged status, referenced by ADR-0043b)

- [ADR-0001](docs/adr/0001-ledger-as-source-of-truth.md)
- [ADR-0003](docs/adr/0003-lamella-metadata-namespace.md)
- [ADR-0015](docs/adr/0015-reconstruct-capability-invariant.md)
- [ADR-0019](docs/adr/0019-transaction-identity-use-helpers.md)
- [ADR-0022](docs/adr/0022-money-is-decimal.md)
- [ADR-0023](docs/adr/0023-datetimes-tz-aware-utc.md)
- [ADR-0043](docs/adr/0043-no-fixme-in-ledger.md) (parent)
- [ADR-0043b](docs/adr/0043b-staged-txn-directive-shape.md) (frozen shape)

## [0.3.0] (2026-04-29)

Initial public release. Per ADR-0052, this is the first tracked
release event; `0.1.x` and `0.2.x` were pre-public dev iterations
that bumped `pyproject.toml::version` without a release ceremony.

- AI classify cascade (OpenRouter Haiku → Opus fallback on low
  confidence) over the full context stack: card binding, entity and
  account descriptions, vector-similar transaction history, active
  notes, projects, mileage logs, receipts, merchant histograms, and
  intercompany awareness.
- Optional in-app authentication: Argon2id password hashing, DB-backed
  sessions, CSRF protection, per-user lockout, audit log
  ([ADR-0050](docs/adr/0050-optional-authentication.md)).
- Receipts pipeline via Paperless-ngx: OCR ingest, vision-AI
  re-verification against the original image, writeback of
  corrections tagged `Lamella Fixed`.
- SimpleFIN ingest with `disabled` / `shadow` / `active` modes and
  per-account mapping.
- Beancount as source-of-truth: SQLite holds in-flight state only;
  the ledger is reconstructable from `.bean` files alone via
  `python -m lamella.transform.reconstruct`.
- Single-container Docker deploy targeting a generic Linux host;
  `docker-compose.unraid.yml` override for the Unraid uid/gid
  conventions.

### Architecture and drift remediation

- ADR audit and drift remediation across ADRs 0022, 0030, 0031, 0032,
  0035, 0038, 0039, 0041, 0042, 0048: text amendments, code fixes,
  and a frozen carve-out for group/workflow inline-form patterns that
  are exempt from the `T.actions` macro
  ([ADR-0032 amendment](docs/adr/0032-component-library-per-action.md)).
- Path-safety (ADR-0030) is now enforced uniformly across the four
  connector-owned writers: `OverrideWriter`, `AccountsWriter`,
  `LoanWriter`, and `ReceiptLinker` validate every write path against
  `allowed_roots=[ledger_dir]` in `__init__` so a misconfigured caller
  cannot escape the ledger directory.
- ADR-0043 (`custom "staged-txn"` directives replacing FIXME postings)
  is **deferred to a follow-up patch**. The directive shape is frozen
  in [ADR-0043b](docs/adr/0043b-staged-txn-directive-shape.md); the
  migration plan lives at `docs/internal/plans/0043-staged-txn-migration.md`.
  v0.3.0 continues to use the legacy FIXME-posting shape internally,
  but user-visible copy throughout the app uses the neutral term
  "Uncategorized" ([ADR-0043](docs/adr/0043-no-fixme-in-ledger.md)).
- The pending-transactions surface formerly known as "Staging review"
  is now consistently labeled **Inbox** across templates, navigation,
  and dashboard tiles ([ADR-0048](docs/adr/0048-url-and-page-naming.md)).


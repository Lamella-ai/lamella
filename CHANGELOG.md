# Changelog

All notable changes to Lamella are documented here. The format roughly
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
the project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Delete confirmations actually fire, everywhere (2026-06-12)

Upstream issue Lamella-ai/lamella#4 (follow-through on its third
finding): `B.btn(confirm=…)` renders `data-confirm` on the button,
but the site-wide submit guard only read the attribute off the form —
so every button-level confirmation (including the import page's
Hard delete and Cancel) submitted with no modal at all. The guard now
also reads confirm attributes from the submitting button
(`ev.submitter`) and re-submits via `requestSubmit()` so HTMX
behavior and the button's name/value survive the round-trip. Two
more holes in the same machinery: the guard didn't stop propagation,
so an `hx-post` form with `data-confirm` fired its request while the
modal was still open; and the bundled htmx shim handled `hx-confirm`
with a native `window.confirm` (the `htmx:confirm` hook it was
assumed to emit never fires) — the shim now routes `hx-confirm`
through the site modal.

Audit sweep per the same issue: destructive forms that had no
confirmation at all now warn before acting — data-integrity purge
(both), loan balance-anchor and payment-pause deletes, and the five
setup-wizard draft Remove buttons; the two `onsubmit="return
confirm(…)"` natives (nuclear reboot-row purge, vehicle disposal
revoke) moved to the modal. A new template-audit test fails CI if a
delete/remove/purge/revoke/clear form ships without a confirm.

### Mileage delete actually deletes (2026-06-12)

Upstream issue Lamella-ai/lamella#9: on the trip edit page the
Delete button behaved like a second Save — the delete `<form>` was
nested inside the edit `<form>`, which HTML forbids, so the browser
dropped the inner form tag and the button submitted the edit
endpoint (where validation then rejected the unchanged mileage).
The delete form is now a sibling of the edit form with the button
associated via the `form=` attribute, which also restores the
site-wide delete-confirmation modal that was being discarded along
with the dropped tag. Related fixes: the confirm message named a
nonexistent `driven_on` field on three surfaces and rendered
"Delete the  trip" — it now shows the trip date; and the vehicle
page's Recent-trips table gained per-trip edit links so the trip
editor is reachable from the vehicle itself.

### Classify popover suggestion list contained (2026-06-12)

Upstream issue Lamella-ai/lamella#10: the account-picker suggestion
dropdown had no styling at all — it rendered as an in-flow unstyled
list that grew its flex parent, pushing long account paths through
the right edge of the classify popover. The popup is now a real
dropdown: absolutely positioned under the input, panel-styled,
scrollable past ~20rem, deep paths wrap, keyboard highlight is
visible, and the flip-above-keyboard mode the JS already set
(`data-flip="up"`) finally has an effect. The classify popover also
widened 22rem → 26rem so typical
`Expenses:Entity:Category:Sub` paths fit on one line.

### Form errors preserve input — ADR-0066 (2026-06-11)

Upstream issue Lamella-ai/lamella#8: a validation error on
the mileage quick-log form redirected to a fresh GET and wiped every
field, including long trip notes. New ADR-0066 makes the rule
explicit — **a failed form submission never clears the user's
input** — and the mileage quick form + /mileage New-trip form are the
reference implementations: errors re-render with `error` +
`form_values` (HTTP 422 on the quick form), success still redirects
(PRG). Error messages now name the field and the fix instead of
`?error=bad_odometer` codes.

Same-day follow-up: the three remaining redirect-on-error forms are
converted too — `/mileage/{id}` edit, `/settings/mileage-rates`, and
`/accounts/{path}/opening-balance`. The latter two never even
rendered their `?error=` codes, so their failures were completely
invisible; both now show a banner and keep the submission.

### Backdated mileage entries (2026-06-11)

The quick-log derive-start path anchored to the vehicle's
globally-latest entry, so logging a forgotten earlier trip always
conflicted with whatever was logged since. The odometer chain is now
date-aware: with 1000 logged Jan 1 and 1200 Jan 3, a Jan 2 reading
of 1100 slots between them and the Jan 3 trip re-anchors to
1100→1200 (no double-counted miles). Within a day the odometer is
the timeline, so a same-day backfill (1000, 1050 logged; 1025 added
later) splices in by value. Impossible readings fail with the
conflicting entry named: below the previous reading, above the next
one, or inside a trip whose start the user typed explicitly.

### Cleanup sweep (2026-06-11)

- **Test-suite triage:** fixed the 10 pre-existing failures —
  migration-registry pins updated for the v3→v4 ledger bump, shared
  fixture ledger re-stamped v4, simplefin timezone tests pointed at
  the app DB, receipt-unlink tests updated for the intentional
  link-blocked tombstone.
- **Ledger-version bump gate:** new `tests/test_ledger_version_gate.py`
  pins `LATEST_LEDGER_VERSION`, ties the fixture stamp to it, and
  requires a registered migration from the previous version — a bump
  can no longer merge without updating the test surface.
- **`GET /import/{id}.json` unshadowed** — it registered after
  `/{import_id}` and always 422'd; now reachable.
- **Picker migration complete (B6 Step 4):** the legacy datalist
  `T.account_picker` macro is retired; staged-review bulk/single
  classify, the card pane, and the Ask-AI manual form all use the
  modern component with `allow_create` + ranked search. Import
  classify's entity `<select>` became a datalist input (ADR-0011).
- **Ledger classify parity:** `/api/txn/ledger:*/classify` now also
  runs ADR-0042 entity-first validation, matching staged classify.
- **Dev env:** the test toolchain (pytest, respx, ruff, …) moved into
  `[dependency-groups].dev` so plain `uv sync` installs it; `uv run
  pytest` no longer falls back to a global binary.
- Ruff clean-up in routes/import service modules (unused imports,
  unused locals, import placement).

### Upstream issue fixes (2026-06-11)

Fixes for the five issues reported against the public repo
(Lamella-ai/lamella #3–#7):

- **Import classify/preview entity pickers read the registry** (#3).
  The hardcoded demo tuple (`Acme`, `WidgetCo`, …) is gone; classify
  and preview pages list active entities from `entities` with slug
  values and display-name labels.
- **Import hard delete is a standard form POST** (#4). New
  `POST /import/{id}/delete` route; the detail-page button submits a
  plain form so CSRF injection and the confirm modal work, and
  failures surface as HTTP errors instead of a silent redirect. The
  `DELETE /import/{id}` API endpoint remains.
- **Ingest progress no longer crashes on `duplicate_rows`** (#5).
  `IngestSummary` gained a `duplicate_rows` property backed by
  `transfers["duplicates"]`.
- **Entity scaffold page** (#6): "manage →" links only target
  `/accounts/{path}` for accounts_meta-registered paths (Expense/
  Income paths route to `/settings/account-descriptions` instead of
  404ing); an "All set" banner renders when every category already
  exists; the extras list ("Not on this chart") now renders; an empty
  submission bounces back with a "Nothing to create" notice instead
  of silently redirecting.
- **AI Suggestions → Classify can create new target accounts** (#7).
  The classify popover uses the modern account picker with
  `allow_create` (prefilled with the AI's proposal when present), and
  the ledger classify path runs the same
  `ensure_target_account_open` / `check_account_open_on` guards as
  staged classify — new paths that deepen an attested branch are
  auto-scaffolded; illegitimate ones get a clean 400.

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


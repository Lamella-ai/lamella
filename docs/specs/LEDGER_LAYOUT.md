# Canonical Ledger Layout

**Status:** Draft, WS2 output. Reviewed before WS3 (first-run scaffolder)
and WS4 (in-app editor) build against it.

This document defines what a Lamella ledger *is*: which files
exist, which we own, which the user owns, what `main.bean` looks like,
what plugin set the parser loads, what metadata contracts the ledger
honors, and what we refuse to accept.

Everything in WS3 (scaffold + import) and WS4 (editor) is built to
produce or preserve the layout specified here. When a future version
changes the layout, it bumps `lamella-ledger-version` and adds a migration
pass; it does not silently diverge.

---

## 1. The governing principle

> The ledger is the product. Our app is a UI + automation layer over
> plain-text files the user could open in any editor, grep, move to
> another tool, or inspect in twenty years.

Consequences enforced by every section below:

- Every file in the ledger directory is either ours, the user's, or
  deliberately archived. There is no ambiguous category.
- Everything we write is identified by the `lamella-*` metadata prefix
  (keys) or `#lamella-` tag prefix. Everything unprefixed is either
  user-authored or from a pre-glue tool and must not be rewritten.
- We never delete user-authored content silently. Stripping on
  import, auto-fix in the editor, and cleanup in the scaffolder all
  comment-out (`; ` prefix with a dated marker) rather than delete.
  "Lamella never makes your ledger smaller without leaving a
  trail."
- Every write runs `bean-check` against a baseline. On new errors,
  the write rolls back. No exceptions.
- **All writes to `/ledger/` go through a single file lock**
  (`/ledger/.lamella-lock`). Single-user product, single-directory,
  multiple writers: the SimpleFIN cron, the editor save path, the
  import flow, the scaffolder. Writers acquire the lock, snapshot,
  write, run `bean-check`, release. Concurrent attempts block with
  a 30-second timeout, then fail with a clear user-facing error
  ("another write is in progress; try again in a moment"). The
  lock file is never committed to git or read by beancount (it's
  `.lamella-lock`, not `.lamella-lock.bean`).

### 1.1 What Lamella is *not* responsible for

Naming non-goals in the layout doc prevents scope creep in every
future feature discussion.

- **Lamella does not maintain your chart of accounts.** You own
  `accounts.bean`. The app reads it; the app adds opened accounts to
  `connector_accounts.bean` when the admin UI scaffolds them; the app
  never rewrites `accounts.bean`.
- **Lamella does not reconcile investment lots.** Use
  `beangrow` or similar. We leave `prices.bean` for you to maintain;
  we don't compute returns.
- **Lamella does not produce tax returns.** It produces
  tax-ready exports (Schedule C / F portfolios, per-entity P&L,
  receipt bundles). A human or an accountant files them.
- **Lamella does not own foreign metadata.** See §6: metadata keys outside the `lamella-*` namespace are yours. We
  preserve them wholesale on import, on edit, on every write path.
- **Lamella does not validate your accounting.** `bean-check`
  does that. We run it, surface its output, refuse to write on new
  errors; but the semantics of double-entry are beancount's
  contract, not ours.

---

## 2. Ownership model

Every `.bean` file in the ledger directory is exactly one of:

| Class | Who writes | Who reads | Editor behavior |
|---|---|---|---|
| **Connector-owned** | Lamella only | Everyone | Read-only by default; "force-edit" toggle with warning |
| **User-authored** | User only | Everyone | Fully editable |
| **Archived** | Neither (sits in `_archive*/`) | Neither | Hidden from editor sidebar by default |

### 2.1 Connector-owned files (exactly seven, all in `/ledger/` root)

| File | Purpose | Key directive shapes |
|---|---|---|
| `connector_links.bean` | Receipt ↔ txn links + receipt dismissals | `custom "receipt-link"`, `custom "receipt-dismissed"` (+ revokes) |
| `connector_overrides.bean` | FIXME → target-account override txns + loan-funding blocks | Normal txn directives with `#lamella-override` tag + `lamella-override-of` metadata; `#lamella-loan-funding` for loan-initial blocks |
| `connector_accounts.bean` | `Open`/`Close` directives scaffolded by the admin UI | `Open`, `Close` with `lamella-*` metadata |
| `connector_rules.bean` | Classification rules (user-taught) + recurring-expense confirmations | `custom "classification-rule"`, `custom "recurring-confirmed"`, `custom "recurring-ignored"` (+ revokes) |
| `connector_budgets.bean` | Budget targets + alert thresholds | `custom "budget"` (+ revokes) |
| `connector_config.bean` | Non-secret UI settings + Paperless field role mapping | `custom "setting"`, `custom "paperless-field"` (+ unsets) |
| `simplefin_transactions.bean` | Classified SimpleFIN transactions (live, `active` mode) | Normal txn directives with `lamella-txn-id` at txn meta + paired `lamella-source-0: "simplefin"` / `lamella-source-reference-id-0: "<TRN-…>"` on the bank-side posting (see §6.2.1). `lamella-ai-*` / `lamella-rule-id` per writer. |

Plus one **optional** preview variant used only in `shadow` mode:
- `simplefin_transactions.connector_preview.bean`: exists only when
  SimpleFIN is in preview mode; not included from `main.bean`; ignored
  on reconstruct.

Plus two **optional** output trees:
- `connector_imports/<year>.bean` + `connector_imports/_all.bean`: spreadsheet importer output; one file per tax year plus an aggregate
  include. Created on first import.
- `mileage_summary.bean`: year-end mileage deduction summaries,
  created when the mileage feature first writes.

Every Connector-owned file begins with this header (written by the
scaffolder and preserved by every writer):

```beancount
;; ---------------------------------------------------------------
;; Managed by Lamella. Do not edit by hand.
;;
;; This file is regenerated from user actions in the web UI. Manual
;; edits may be reverted silently on the next write. To modify
;; behavior, use the app. To inspect, read freely.
;;
;; File:      connector_<name>.bean
;; Owner:     lamella
;; Schema:    bcg-ledger-version=1
;; Generated: YYYY-MM-DD by Lamella vX.Y.Z
;; ---------------------------------------------------------------
```

The `Generated:` line is rewritten on every write that touches the
file. It's a debugging aid; when a user reports a bug and opens a
Connector-owned file, the app version that last wrote it is
explicit.

The header is prose; it is not a directive. Removing it breaks no
parser behavior; the scaffolder re-inserts on next write.

### 2.2 User-authored files

These are created by the scaffolder on first run (empty, with a
header) and the user owns them going forward. We read them every
parse; we never rewrite them.

| File | Purpose | Required? |
|---|---|---|
| `main.bean` | Root ledger file; see §3 | Yes |
| `accounts.bean` | `Open`/`Close` directives for user-owned accounts | Yes (scaffolder creates empty-with-header) |
| `commodities.bean` | `commodity` directives | Yes (scaffolder creates empty-with-header) |
| `prices.bean` | `price` directives for commodities | Yes (scaffolder creates empty-with-header) |
| `events.bean` | `event` directives (user life events, not accounting) | Optional |
| `manual_transactions.bean` | Hand-entered transactions not from SimpleFIN / imports | Optional |
| `historical_<year>.bean` | Archived historical transactions, one per year | Optional; no naming enforcement beyond `historical_*.bean` |

`prices.bean` is always scaffolded even on a USD-only ledger. Cost
is one empty file; benefit is the user never wonders "why isn't
there a prices file?" the first time they track a share of stock
or a crypto holding. Same reasoning applies to `commodities.bean`.

A user-authored file's header, written by the scaffolder:

```beancount
;; ---------------------------------------------------------------
;; User-authored file. Edit freely.
;;
;; Lamella reads this file every parse but never rewrites it.
;; Changes made in the app's editor land here only if you explicitly
;; save them from the editor. Manual edits via SSH / nano / etc.
;; are always fine.
;;
;; File:      <name>.bean
;; Owner:     user
;; Generated: YYYY-MM-DD by Lamella vX.Y.Z (scaffolder only)
;; ---------------------------------------------------------------
```

For user-authored files the `Generated:` line is stamped once by
the scaffolder and never updated thereafter (we don't rewrite these
files, so we can't refresh the stamp without violating the
ownership rule). It marks the initial scaffold only.

### 2.3 Archived files

`_archive-*/` subdirectories contain files that were in the ledger at
some point (usually inherited from an earlier tool) but are not part
of the current layout. The parser does not read `_archive*/` (nothing
includes it). The editor hides it from the sidebar by default, with
a "show archived" toggle.

Created by (a) the import flow's "Foreign" bucket when the user opts
to archive rather than comment-in-place, and (b) manual cleanup
(tonight's WS1 orphans go in `_archive-lazybeancount/`).

Archived files are never silently deleted by the app. User may delete
directly if they want.

---

## 3. `main.bean`: canonical shape

`main.bean` is the entry point passed to `beancount.loader.load_file`.
Every fresh-scaffolded ledger starts with exactly this, and the
import flow normalizes existing ledgers to match.

### 3.1 Section ordering contract

`main.bean` has exactly five sections, in this order, separated by
blank lines and comment banners. The order is load-bearing, not
cosmetic; beancount processes directives in parse order, and
several of the behaviors we rely on depend on that.

```
1. Header comment block         (prose; identifies the file as ours)
2. option directives            (title, operating_currency)
3. bcg-ledger-version marker    (custom directive)
4. plugin directives            (in allowlist order; see §5)
5. include directives           (user-authored first, then Connector-owned, then optional)
```

**Why this order:**

- **Options before plugins.** Plugins can read `option` values at
  load time (e.g. `operating_currency`). Plugins declared before
  options get no value and fail silently.
- **Version marker immediately after options.** A future migration
  pass wants the version to be the first `custom` directive it
  sees. Stamping it at the top keeps detection O(1) regardless of
  ledger size.
- **Plugins before includes.** Plugins declared *after* an
  `include` only see directives from includes that come *after*
  the plugin. `auto_accounts` in particular must be the last
  plugin, so it sees every transaction in every included file
  before it decides which `Open` directives to synthesize.
- **User-authored includes before Connector-owned includes.** This is a convention for file ordering, not a conflict resolution
  mechanism. Beancount actually *errors* on duplicate `Open`
  directives for the same account; parse-order does not "win."
  The implication for the import flow: Open directives must be
  deduplicated **before** write, not relied on parse order to
  sort out. The dedup rule: for any account with multiple `Open`
  directives across source files, keep the earliest-dated one;
  on date ties, prefer user-authored source over Connector-owned
  source over import source. The loser `Open` directives are
  dropped (with a marker comment in the import preview if the
  user wants to see what was deduped).
- **Optional includes last.** `connector_imports/_all.bean`,
  `mileage_summary.bean`, `historical_*.bean` are commented-in by
  scaffolder as templates. User uncomments the ones that apply.

A contributor reordering sections for aesthetics breaks this
contract and must update the `lamella-ledger-version` (§11) along
with whatever logic is changing. The scaffolder and import flow
both enforce this shape; the editor warns when a user-edited
`main.bean` violates it.

### 3.2 Canonical template

```beancount
;; ---------------------------------------------------------------
;; Lamella — ledger root
;;
;; This file is user-authored. The scaffolder creates it on first
;; run; the import flow normalizes an existing main.bean to this
;; shape. You can edit freely — Lamella reads this file every
;; parse but never rewrites it.
;;
;; Owner: user
;; Schema: bcg-ledger-version=1
;; ---------------------------------------------------------------

option "title"              "Lamella Ledger"
option "operating_currency" "USD"

;; Schema version marker — updated only by Lamella migration
;; passes. Do not edit.
2026-01-01 custom "lamella-ledger-version" "1"

;; Plugin set — fixed by the Lamella install. Adding plugins
;; here that are not installed in the image will cause the app to
;; refuse to start. See docs/LEDGER_LAYOUT.md §5.
plugin "beancount_lazy_plugins.auto_accounts"

;; User-authored includes (load before Connector-owned so user
;; precedence wins on directive collisions — see §3.1).
include "accounts.bean"
include "commodities.bean"
include "prices.bean"
include "events.bean"
include "manual_transactions.bean"

;; Connector-owned includes. These files are managed by the app.
include "connector_accounts.bean"
include "connector_links.bean"
include "connector_overrides.bean"
include "connector_rules.bean"
include "connector_budgets.bean"
include "connector_config.bean"
include "simplefin_transactions.bean"

;; Optional — uncomment when applicable.
;; include "mileage_summary.bean"
;; include "connector_imports/_all.bean"
;; include "historical_2024.bean"
```

Notes:

- **Plugin directives are an allowlist, not a suggestion.** §5 defines
  what's installed. The import flow refuses any ledger that declares a
  plugin not in the allowlist, with instructions for the user.
- **No `fava-extension` / `fava-option` / `fava-sidebar-link` lines.**
  Fava reads the same ledger in our sidecar, but it reads only; no
  Fava-specific configuration lives in the ledger. If users want Fava
  options, they go in a sidecar-only `.fava.conf` outside the canonical
  layout.
- **No multi-currency infrastructure.** v1 of the layout is
  USD-only. The scaffolder has no multi-currency flag; users who
  need multi-currency will get a v2 layout when it ships. The
  reason: multi-currency requires not just more `option` lines but
  also price-history maintenance, conversion UI, and report
  semantics, none of which exist in v1. A flag that creates the
  skeleton without the behavior is worse than no flag. Deferred
  until a real multi-currency user shows up or until WS-MC is
  scoped.
- **The `lamella-ledger-version` directive is a `custom` directive, not
  metadata on `option`.** This lets a future migration pass detect the
  version via a single `custom` scan without parsing options.
  Currently always `"1"`.

---

## 4. Directory layout

```
/ledger/
├── main.bean
├── accounts.bean
├── commodities.bean            (optional; empty+header if present)
├── events.bean                 (optional)
├── manual_transactions.bean    (optional)
├── historical_2024.bean        (optional; one per year)
├── historical_2025.bean
│
├── connector_accounts.bean
├── connector_links.bean
├── connector_overrides.bean
├── connector_rules.bean
├── connector_budgets.bean
├── connector_config.bean
├── simplefin_transactions.bean
│
├── connector_imports/          (created on first spreadsheet import)
│   ├── _all.bean
│   ├── 2024.bean
│   └── 2025.bean
│
├── mileage/                    (created on first mileage entry)
│   └── vehicles.csv
├── mileage_summary.bean        (created on first year-end rollup)
│
└── _archive-*/                 (inherited-from-other-tool files; ignored)
    └── ...
```

- **Flat at the root.** No nesting of `.bean` files except under
  `connector_imports/` (which already has year-based sharding that
  justifies its own directory) and `_archive-*/`.
- **`mileage/` is a non-bean data dir.** The CSV inside is
  user-editable. `mileage_summary.bean` is the Connector-owned
  aggregate.
- **No `logs/`, `scripts/`, `importers/`, or `prices/` dirs.** These
  came from lazy-beancount and are not part of our layout. On import,
  they go to `_archive-*/`. The app never creates them.

---

## 5. Plugin allowlist

The Lamella container installs exactly two packages relevant to
the beancount parser:

1. **`beancount>=3.2`**: the core library.
2. **`beancount-lazy-plugins`**: only the `auto_accounts` plugin is
   loaded by any canonical `main.bean`. The rest of the package's
   plugins (`valuation`, `filter_map`, `group_pad_transactions`,
   `generate_inverse_prices`, `generate_base_ccy_prices`) are
   installed-but-unused and will be removed from the image when the
   package is repackaged.

And as a fallback UI only, never imported by our code:

3. **`fava>=1.27`**: runs as a backgrounded sidecar process in the
   container. No Fava extensions.

**Any `plugin "..."` directive in an imported ledger that is not in
the allowlist causes the import to refuse**, with an error message
that names the offending plugin, links to the docs section explaining
our stance, and points to a flatten-script (where we have one; see
§7.3).

Plugins explicitly refused today (appear in lazy-beancount ledgers):

- `beancount_share.share`
- `beancount_reds_plugins.effective_date.effective_date`
- `beancount_reds_plugins.rename_accounts`
- `beancount_interpolate.recur`
- `beancount_interpolate.split`
- `beancount_lazy_plugins.valuation`
- `beancount_lazy_plugins.filter_map`
- `beancount_lazy_plugins.group_pad_transactions`
- `beancount_lazy_plugins.generate_inverse_prices`
- `beancount_lazy_plugins.generate_base_ccy_prices`

---

## 6. Metadata contract (`lamella-*` namespace)

### 6.1 Bidirectional namespace contract

The `lamella-*` prefix is the ownership marker, but it's bidirectional,
not one-way. **Three rules** that together define the contract:

1. **Metadata keys and tags Lamella writes are prefixed `lamella-*`
   (keys) or `#lamella-` (tags).** No exceptions. A key or tag without
   the prefix was not written by us.

2. **`custom` directive type names Lamella writes are
   enumerated in §6.2.** These names (`"receipt-link"`,
   `"classification-rule"`, `"budget"`, `"setting"`, etc.)
   pre-date the `lamella-*` prefix convention and are grandfathered as
   ours by explicit enumeration rather than by prefix. The schema
   anchor `custom "lamella-ledger-version"` is the sole exception
   that follows the prefix convention directly, because it *is*
   the namespace marker. Adding a new `custom` directive type name
   outside the enumerated list without updating §6.2 in the same
   commit is a violation of this spec.

3. **Everything else is user-owned.** Metadata keys and tags without
   `lamella-*`, custom directive type names not in §6.2's enumeration,
   arbitrary metadata values on any directive: Lamella reads
   them, displays them, and preserves them wholesale on every write
   path. We never rewrite, normalize, strip, or rename them.

Practical consequences:

- Users can add their own metadata freely: `tax-year: 2025`,
  `project: "kitchen-remodel"`, `reimbursable: TRUE`, anything.
  We touch nothing of theirs.
- Users can add their own `custom` directives too (e.g.
  `custom "my-tooling-marker" ...`) and Lamella preserves
  them wholesale. §6.2's enumeration is a list of *ours*, not a
  whitelist of *allowed*.
- The import flow's "Foreign" bucket (§7.3) for metadata is trivial:
  if the key doesn't start with `lamella-`, it's Keep, full stop.
- A future plugin or third-party tool choosing a `lamella-*` key would
  be in violation of this contract. External integrations should
  use their own prefix.

Tags follow the same rule: `#lamella-override`, `#lamella-loan-funding` are
ours; `#tax`, `#travel`, `#reconciled` are the user's.

### 6.2 Schema table: keys and tags we own

Authoritative list. Every `lamella-*` key Lamella reads or writes
appears here with its type, whether it's required, which file(s) it
appears on, and which module owns its creation. When a new feature
needs a new key, this table is updated in the same commit that
introduces the key.

| Key | Scope | Type | Required | Appears on | Owning module |
|---|---|---|---|---|---|
| `lamella-ledger-version` | custom-arg | integer (as custom string arg) | yes | `main.bean` (as `custom` directive) | `bootstrap/scaffold` |
| `lamella-txn-id` | txn meta | string (UUIDv7) | yes | every connector-written txn | `identity` (via every writer) |
| `lamella-source-N` | posting meta | string enum (`simplefin`/`csv`/`paste`/`manual`) | paired with `-reference-id-N` | source-side posting on every txn we own | every writer (paired indexed; N starts at 0, dense) |
| `lamella-source-reference-id-N` | posting meta | string | paired with `-N` | source-side posting on every txn we own | same |
| `lamella-paperless-id` | txn meta | string | yes | `connector_links.bean` receipt-link txns | `receipts/linker` |
| `lamella-paperless-hash` | txn meta | string (`md5:<hex>` or `sha256:<hex>`) | yes | `connector_links.bean` receipt-link txns | `receipts/linker` |
| `lamella-paperless-url` | txn meta | string (URL) | yes | `connector_links.bean` receipt-link txns | `receipts/linker` |
| `lamella-match-method` | txn meta | string enum (`exact`, `amount-date`, `manual`, ...) | yes | `connector_links.bean` receipt-link txns | `receipts/linker` |
| `lamella-match-confidence` | txn meta | float 0 to 1 | yes | `connector_links.bean` receipt-link txns | `receipts/linker` |
| `lamella-txn-date` | txn meta | date | yes | `connector_links.bean` receipt-link txns | `receipts/linker` |
| `lamella-txn-amount` | txn meta | amount | yes | `connector_links.bean` receipt-link txns | `receipts/linker` |
| `lamella-simplefin-aliases` | txn meta | string (whitespace-separated ids) | optional | `simplefin_transactions.bean` (set when duplicates merged) | `duplicates/cleaner` |
| `lamella-ai-classified` | txn meta | boolean | optional | `simplefin_transactions.bean` txns | `simplefin/classifier` |
| `lamella-ai-decision-id` | txn meta | string (UUID) | optional | `simplefin_transactions.bean` txns | `ai/audit` |
| `lamella-rule-id` | txn meta | string | optional | `simplefin_transactions.bean` txns | `classifier/rules` |
| `lamella-override-of` | txn meta | string (txn hash) | yes | `connector_overrides.bean` override txns | `overrides/writer` |
| `lamella-loan-slug` | txn meta | string | yes | `connector_overrides.bean` loan-funding blocks | `registry/loans` |
| `lamella-import-memo` | txn meta | string | optional | `connector_imports/*.bean` txns (user content, not an identifier) | spreadsheet importer |
| `lamella-mileage-vehicle` | txn meta | string | yes | `mileage_summary.bean` summary entries | `mileage/writer` |
| `lamella-mileage-entity` | txn meta | string | yes | `mileage_summary.bean` summary entries | `mileage/writer` |
| `lamella-mileage-miles` | txn meta | decimal | yes | `mileage_summary.bean` summary entries | `mileage/writer` |
| `lamella-mileage-rate` | txn meta | decimal | yes | `mileage_summary.bean` summary entries | `mileage/writer` |
| `lamella-intercompany` | txn meta | boolean | yes | `connector_overrides.bean` wrong-card txns | `overrides/writer` (Phase G) |
| `lamella-paying-entity` | txn meta | string (entity slug) | yes | `connector_overrides.bean` wrong-card txns | `overrides/writer` (Phase G) |
| `lamella-owning-entity` | txn meta | string (entity slug) | yes | `connector_overrides.bean` wrong-card txns | `overrides/writer` (Phase G) |

#### 6.2.1 Transaction identity & source provenance: special schema rules

Two orthogonal concerns on two different scopes; full spec at
`docs/NORMALIZE_TXN_IDENTITY.md`.

- **`lamella-txn-id`** (txn meta, UUIDv7) is the *lineage*: minted
  on first sight, never regenerated, stable across ledger edits.
  Every internal subsystem keys off it (AI decisions, override
  pointers, future receipt-link bridge). Writers stamp it on emit;
  the on-touch normalizer in `rewrite/txn_inplace` mints it for
  any legacy entry the user touches; the
  `POST /setup/normalize-txn-identity` recovery action mints it
  in bulk.
- **`lamella-source-N` + `lamella-source-reference-id-N`** (posting
  meta, paired) is the *provenance*: each posting carries 0+
  `(source, reference-id)` pairs as paired indexed keys starting
  at 0, dense. Source-side posting carries the pair; synthesized
  legs (e.g. the expense leg of a card charge) have no provenance.
  A single posting can carry multiple `(source, ref)` pairs when
  cross-source dedup matches a CSV row into an existing SimpleFIN
  entry.

Allowed source names: `simplefin`, `csv`, `paste`, `manual`.
Reference ids must be reconstruct-stable: SimpleFIN bridge id,
CSV's own column, or SHA256 natural-key hash of `(date, amount,
payee, description)`. Never a SQLite PK.

**Reading**: use `lamella.identity.find_source_reference(entry, name)`
and `iter_sources(posting_meta)`, never `meta.get("lamella-...")`
directly. Helpers see legacy on-disk content transparently because
`_legacy_meta.normalize_entries` mirrors retired txn-level keys
down to the source-side posting at parse time.

**Retired identifier keys** (still read on legacy on-disk content,
never written by new code):

| Retired key | Replacement |
|---|---|
| `lamella-simplefin-id` (txn meta) | `lamella-source-N: "simplefin"` + `lamella-source-reference-id-N` on bank-side posting |
| `simplefin-id` (bare, pre-prefix era) | same |
| `lamella-import-id` | retired entirely (was a SQLite PK; reconstruct violation) |
| `lamella-import-txn-id` | `lamella-source-N: "csv"` + `lamella-source-reference-id-N: <id-or-natural-key-hash>` on bank-side posting |
| `lamella-import-source` | retired entirely (was free-form `source=X row=Y` debug) |

Custom-directive metadata (keys that appear on `custom "..."` directives
in the reconstruct-layer state, including receipt dismissals, classification
rules, budgets, etc.) follows the same schema discipline. Per-directive
keys are listed in each writer module's docstring; each writer is
expected to keep its listing in sync with this table.

Tags we own:

| Tag | Appears on | Owning module |
|---|---|---|
| `#lamella-override` | `connector_overrides.bean` override txns | `overrides/writer` |
| `#lamella-loan-funding` | `connector_overrides.bean` loan-funding blocks | `registry/loans` |
| `#lamella-intercompany` | `connector_overrides.bean` wrong-card txns | `overrides/writer` (Phase G) |

`custom` directive type names we own (the grandfathered enumeration
referenced by §6.1 rule 2):

| Custom directive type | Appears in | Owning module |
|---|---|---|
| `"lamella-ledger-version"` | `main.bean` | `bootstrap/scaffold` |
| `"receipt-link"` | `connector_links.bean` | `receipts/linker` |
| `"receipt-dismissed"` + `"receipt-dismissal-revoked"` | `connector_links.bean` | `receipts/dismissals` |
| `"classification-rule"` + `"classification-rule-revoked"` | `connector_rules.bean` | `classifier/rules` |
| `"recurring-confirmed"` + `"recurring-ignored"` + `"recurring-revoked"` | `connector_rules.bean` | `recurring/detector` |
| `"budget"` + `"budget-revoked"` | `connector_budgets.bean` | `budgets/writer` |
| `"paperless-field"` | `connector_config.bean` | `paperless/fields` |
| `"setting"` + `"setting-unset"` | `connector_config.bean` | `settings_store` |

Any new `custom` directive type name Lamella introduces ships
with a row added to this table in the same commit. Directive type
names outside this list are user-owned per §6.1 rule 3.

### 6.3 Type conventions

- **boolean.** Beancount's bare `TRUE` / `FALSE` tokens, never
  quoted. Writers must emit `lamella-auto-assigned: FALSE`, not
  `lamella-auto-assigned: "FALSE"`. Each new boolean key gets a dedicated
  test for this.
- **string.** Always double-quoted. Empty-string-allowed unless
  the column says otherwise.
- **date.** Beancount's bare `YYYY-MM-DD` token; never quoted.
- **amount.** Beancount's bare `<number> <commodity>` pair; never
  quoted.
- **integer / decimal / float.** Bare numbers; never quoted. For
  the `lamella-ledger-version` exception (it's a `custom` directive's
  string argument, not metadata), the integer is wrapped in quotes
  per beancount's `custom` syntax.

### 6.4 Version marker

The `lamella-ledger-version` `custom` directive is the anchor for schema
migrations:

```beancount
2026-01-01 custom "lamella-ledger-version" "1"
```

Currently always `"1"`.

**Date semantics.** The `2026-01-01` date is a fixed schema-epoch
date, not a per-install scaffold date. Two reasons:
- Fixtures are deterministic across test runs.
- Dates on `custom "lamella-*"` directives are metadata-only; they do
  not participate in `bean-check` balance assertions or transaction
  ordering semantics. The ledger-version directive is never
  load-bearing on a date.

The same "dates on custom `lamella-*` directives are metadata-only"
rule applies to every other `custom` directive we own (receipt
dismissals, classification rules, budgets, etc.). Their dates are
there because beancount's `custom` directive syntax requires one;
they carry audit-trail information only and do not affect what
`bean-check` allows or rejects.

**Layout version ↔ software major version coupling.** A change to
the layout spec that bumps `lamella-ledger-version` from v`N` to v`N+1`
also bumps the software's semver major version from v`M.y.z` to
v`(M+1).0.0`. The rule is:

- Layout v1 = software v1.x.x (or v0.x.x during initial development)
- Layout v2 = software v2.x.x
- Layout v3 = software v3.x.x

A breaking change to the canonical files is by definition a major
software change; coupling them makes the relationship explicit and
lets users reason about compatibility without cross-referencing
two version numbers.

**Rule for future versions.** Each layout change increments the
integer, bumps the software major version, ships with a migration
pass that reads v`N` and writes v`N+1`, and preserves a test
fixture for v`N` that proves the migration round-trips (v`N` in →
v`N+1` out → v`N` in a reverse-migrator, same bytes at both ends
modulo the version stamp).

**Missing-marker handling.** When the app boots against a ledger
that has no `lamella-ledger-version` directive but otherwise looks
canonical, we treat it as v0 and run a no-op migration that stamps
`"1"`. The log line `bootstrap: stamped bcg-ledger-version=1 on
existing ledger at /ledger/main.bean` is emitted prominently; if
anything goes weird later, that breadcrumb is there. The
alternative (refusing to boot and forcing Import) would punish
every existing Lamella install for no semantic reason.

---

## 6.5 Intercompany convention (Phase G)

When a card belonging to one entity physically pays an expense
that belongs to another entity (a "wrong card" situation
common in multi-entity operations), the corrected transaction
is a **zero-sum four-leg** entry that lands in
`connector_overrides.bean` with the `#lamella-intercompany` tag.

**Account naming (load-bearing):**

- `Assets:<PayingEntity>:DueFrom:<OwingEntity>`: the receivable
  on the paying entity's side. The paying entity is owed the
  amount back by the owing entity.
- `Liabilities:<OwingEntity>:DueTo:<PayingEntity>`: the payable
  on the owing entity's side. The owing entity owes the amount
  to the paying entity.
- These account pairs must always exist together. `open`
  directives for both sides are scaffolded into
  `connector_accounts.bean` on first use; the Phase G review
  action that emits an intercompany override is responsible for
  ensuring both accounts are open.

**Required metadata:**

Every intercompany override carries:

- `#lamella-intercompany` tag (for reports + reconciliation
  queries).
- `lamella-intercompany: TRUE` (boolean, per §6.3; matches the
  tag for queries that operate on metadata instead of tags).
- `lamella-paying-entity: "<entity_slug>"`: the entity whose card
  actually paid. Should match the entity prefix of the
  `DueFrom` account.
- `lamella-owning-entity: "<entity_slug>"`: the entity that
  incurred the real expense. Should match the entity prefix of
  the `DueTo` and `Expenses:` accounts.
- `lamella-override-of: "<original_txn_hash>"`: the original
  wrong-card txn this override corrects.

**Canonical shape** (wrong-card scenario: Acme card pays a
WidgetCo supplies expense):

```beancount
2026-04-20 * "Target" "wrong-card: Acme card paid WidgetCo supplies" #lamella-override #lamella-intercompany
  bcg-override-of:       "<orig-hash>"
  bcg-intercompany:      TRUE
  bcg-paying-entity:     "Acme"
  bcg-owning-entity:     "WidgetCo"
  Liabilities:Acme:Card:0123        -100.00 USD   ; card that was actually charged
  Assets:Acme:DueFrom:WidgetCo         100.00 USD   ; Acme is owed 100 back
  Expenses:WidgetCo:Supplies             100.00 USD   ; the real expense
  Liabilities:WidgetCo:DueTo:Acme     -100.00 USD   ; WidgetCo owes 100 to Acme
```

**Balance properties this preserves:**

- Global: zero-sum (legitimate Beancount transaction).
- Per-entity: each entity internally balances (Acme:
  card −100 offset by receivable +100; WidgetCo: expense +100
  offset by payable −100).
- Intercompany balance sheet: reports against
  `Assets:*:DueFrom:*` and `Liabilities:*:DueTo:*` tell you
  who owes whom as of any date.

**Settlement:** When the owing entity actually pays the paying
entity (via a cash transfer), a separate two-leg transaction
clears both balance-sheet accounts:

```beancount
2026-05-01 * "Intercompany settlement: WidgetCo → Acme" #lamella-intercompany-settlement
  bcg-intercompany-settlement-of: "<first-override-hash>"
  Liabilities:WidgetCo:DueTo:Acme       100.00 USD   ; clears WidgetCo's payable
  Assets:Acme:DueFrom:WidgetCo         -100.00 USD   ; clears Acme's receivable
```

(Plus the actual money movement: `Assets:WidgetCo:Checking
-100` / `Assets:Acme:Checking +100`, which is a normal
transfer the matcher or the user will record.)

**Why four legs, not two:**

A two-leg override that moves the expense from
`Expenses:Acme:FIXME` to `Expenses:WidgetCo:Supplies` leaves
the liability on the wrong card untouched and creates no
receivable/payable record. The ledger balances in dollar terms
but each entity's books are individually wrong: Acme paid
but shows no expense; WidgetCo has an expense with nothing paying
for it; Schedule C filed on either entity comes out wrong and
the two errors hit different tax returns rather than
cancelling. The four-leg form is the only correct shape.

---

## 7. Import flow: three-bucket model

When the user points the import flow at an existing `.bean` file tree
(their own pre-existing ledger, or a lazy-beancount export, or whatever),
we classify every top-level directive into exactly one of three buckets
and present the user with a line-by-line review before any write.

Every directive shown in the preview carries a **reversibility** tag
so the user can see at a glance which items can be undone later and
which cannot:

| Reversibility | Meaning |
|---|---|
| **Reversible** | The action leaves the original content intact and recoverable. A Keep is always reversible (no-op). A comment-out Transform is reversible by stripping the `; ` prefix. An archive-to-`_archive-*/` is reversible by moving the file back. |
| **Lossy** | The action destroys information that cannot be recovered by local edits. An expand-recurring-transactions Transform replaces a single rule with N concrete transactions, and the rule is gone. A true strip (vs. comment-out) drops content entirely. |

The import preview **flags every Lossy item** with a warning badge
and requires a separate opt-in checkbox before the Apply button
becomes active. We never run a Lossy transform silently; the user
must see what they're giving up.

### 7.1 Bucket 1: **Keep**

Directives that match this spec's canonical shape and need no
transformation. Pass through unchanged.

Examples:
- `Open`, `Close` directives for user accounts → go into `accounts.bean`.
- Normal transaction postings with no plugin-specific metadata → go into
  `manual_transactions.bean` (or `historical_<year>.bean` if older than
  the current tax year).
- `option "title"`, `option "operating_currency"` → go into `main.bean`.
- `custom "budget"`, `custom "classification-rule"` (already bcg-native) →
  go into the matching Connector-owned file.

### 7.2 Bucket 2: **Transform**

Directives that we recognize and can rewrite to our canonical shape.
Every transform is documented here; no transform is applied silently.

Examples (with reversibility tags):

- **[Lossy]** `plugin "beancount_reds_plugins.effective_date"` blocks
  with `effective_date:` metadata on postings → flatten the posting
  to its effective date, drop the metadata, drop the plugin directive.
  Lossy because the original posting-date / effective-date distinction
  is gone after the transform.
- **[Lossy]** `plugin "beancount_interpolate.recur"` with recurring
  postings → expand the recurrence into its concrete set of dated
  transactions (snapshot at import time), drop the plugin directive.
  Lossy because the recurrence rule is replaced by materialized
  transactions; editing the rule no longer updates them.
- **[Reversible]** `custom "fava-option" ...`,
  `custom "fava-sidebar-link" ...`, `custom "fava-extension" ...`
  → comment out in place with a §7.4 marker (not deleted). Fully
  reversible with a single `sed` un-comment.
- **[Reversible]** Loose `Open` directives scattered across multiple
  files → consolidate into `accounts.bean` (for user-authored) or
  `connector_accounts.bean` (for anything with a `lamella-*` metadata
  stamp). Reversible because the original `Open` content survives
  intact, only the file location changed.

Every Transform shows the user the before / after diff before we
apply. Lossy transforms additionally require the opt-in checkbox
described at the top of §7.

### 7.3 Bucket 3: **Foreign**

Directives that we do not recognize. Default action is **keep with
warning, not strip**. The user chooses per-item or in bulk.

Examples (from lazy-beancount ledgers we've seen in the wild):
- `plugin "some.package.you.never.heard.of"` → blocks the import
  entirely until the user decides. Options: (a) remove the plugin
  directive, accept that directives depending on it may fail parse,
  (b) cancel the import and flatten the dependency manually.
- Arbitrary `custom "random-thing" ...` directives we don't recognize.
  Default: keep as-is. UI offers "comment out" and "delete" as
  opt-in actions per item.
- Unknown metadata keys on postings (e.g. `effective_date:` without
  the plugin to interpret it). Default: keep as-is. UI warns that
  these will be parsed as opaque strings without the plugin.

The ruling principle: **Lamella never deletes a user-authored
directive silently.** Every removal produces either (a) a comment
with a dated marker, or (b) a move into `_archive-*/`.

### 7.4 Marker comment format

When a directive is commented out by import / editor / cleanup
tools, the inserted comment follows one format so future tooling
can find and potentially un-comment them:

```beancount
; [bcg-removed YYYY-MM-DD reason=<short-reason> tool=<tool-name>]
; <original line, verbatim>
```

Example after tonight's WS1 cleanup, applied retroactively in the
editor when we ship WS4:

```beancount
; [bcg-removed 2026-04-21 reason=foreign-fava-extension tool=ws1-manual]
; 2010-01-01 custom "fava-extension" "fava_dashboards"
```

Un-comment is a single-line sed: strip the leading `; ` on the data
line; the marker line can stay as audit trail or also get stripped.

The format is owned by a single module: `src/lamella/
bootstrap/markers.py`. That module exposes both sides of the contract:

- `format_removal_marker(original_line, reason, tool) -> str`: writer, used by every tool that comments out a directive (import
  flow, editor auto-fix, cleanup scripts).
- `parse_removal_marker(text) -> MarkerInfo | None`: parser, used
  by the editor's "un-comment this removal" one-click action and by
  any future tool that wants to introspect what a previous pass did.

Every tool that emits a marker uses the writer. Every tool that
reads one uses the parser. No other module hand-assembles or
regex-parses the marker text.

---

## 8. Scaffolder behavior (WS3 implementation contract)

### 8.1 First-run detection

On every app boot, before the dashboard routes are reachable, a
detection pass runs. **Order is load-bearing.** The version marker
is checked *before* content counting because a just-scaffolded
ledger has the marker and zero transactions; treating that as
structurally empty would redirect the user back to setup in a loop.

1. Is `/ledger/main.bean` present?
   - **No** → ``MISSING``; serve `/setup`.
2. Does `main.bean` parse with our plugin set?
   - **No** → ``UNPARSEABLE``; serve `/setup` with Import
     pre-selected and the parse error shown.
3. Does the parsed ledger carry a `lamella-ledger-version` marker?
   - **Yes, current version** → ``READY``; serve dashboard.
     (Regardless of whether any transactions have been added yet;
     the stamp is the "user has been through setup" signal.)
   - **Yes, outdated version** → ``NEEDS_MIGRATION``; trigger
     migration pass.
4. No version marker. Count `Transaction` / `Balance` / `Pad`
   entries (per §8.2's "structurally empty" definition).
   - **Content present** → ``NEEDS_VERSION_STAMP``; serve
     dashboard. The gentle path (§6.4) stamps the marker on the
     next write.
   - **No content** → ``STRUCTURALLY_EMPTY``; serve `/setup`. A
     ledger that's never been initialized by any version of
     Lamella and has no transactions.

The implementation in `bootstrap/detection.py` exposes
`needs_setup` / `can_serve_dashboard` convenience properties so
the routing layer doesn't have to pattern-match every state.

### 8.2 "Structurally empty": precise definition

A ledger is structurally empty if, after all `include` directives
resolve, the collected entry set contains **zero** of:

- `Transaction` entries (flagged `*`, `!`, or `txn`)
- `Balance` entries
- `Pad` entries

The following do **not** count as content:

- Comments (`;` lines)
- Blank lines
- `option` directives
- `plugin` directives
- `include` directives
- `Open` and `Close` directives (these are structure, not content)
- `commodity` directives
- `price` directives
- `event` directives
- `document` directives
- `note` directives
- `custom` directives (including our own `lamella-*` state stamps;
  a ledger with only config and no money is still empty)

Rationale: the test is "does this ledger have any actual money
movement recorded?" If no, the user has either just scaffolded or
imported a shell and the product experience should help them put
real transactions in; silently dropping them on the dashboard with
empty charts is a bad first experience.

### 8.3 "Start fresh" flow

When the user clicks **"Start fresh"** on `/setup`:

1. Creates `main.bean` per §3 (USD-only by default; multi-currency
   flag later).
2. Creates empty-with-header versions of: `accounts.bean`,
   `commodities.bean`, `prices.bean`, `events.bean`,
   `manual_transactions.bean`, and all seven Connector-owned files.
3. Stamps `custom "lamella-ledger-version" "1"` into `main.bean` per §6.4.
4. Runs `bean-check /ledger/main.bean`; refuses to return success
   if `bean-check` reports any errors (baseline: a fresh scaffold
   has zero errors, full stop).
5. Writes a bootstrap record into SQLite so the next boot skips
   first-run detection.
6. Redirects to dashboard.

Idempotency: if any of the listed files already exists, the scaffolder
refuses to run and directs the user to the Import flow.

---

## 9. Import flow behavior (WS3 implementation contract)

When the user clicks **"Import existing"**:

1. **Input:** absolute path to a directory (the user bind-mounts
   their legacy ledger into the container, or points at `/ledger`
   itself for in-place re-canonicalization).
2. **Discover:** walks the directory, identifies every `.bean` file,
   parses each with our installed plugin set, collects every
   directive.
3. **Classify:** every directive goes into Keep / Transform / Foreign
   per §7.
4. **Plugin allowlist check:** if any declared plugin is outside §5's
   list, refuses before presenting the preview.
5. **Present preview:** a per-directive table with columns (File,
   Line, Bucket, Current, Proposed, User Action). User reviews,
   adjusts individual items (e.g. "this Foreign → mark as Keep"),
   saves preferences.
6. **Apply:** writes the new canonical ledger, either in a temp dir
   (safer default) or in place with `.pre-import-<timestamp>` backups
   of every file touched.
7. **Verify:** `bean-check` against the result. Failure reverts
   from the `.pre-import-<timestamp>` backups.
8. **Seed:** reconstruct SQLite state from the new ledger (this is
   the same pipeline as `transform/reconstruct.py`).
8.5. **Reconstruct-failure rollback.** If the `reconstruct` pass in
   step 8 throws (e.g. because some directive combination produces
   state reconstruct doesn't currently handle), the import is
   reverted from the `.pre-import-<timestamp>` backups in the same
   way step 7 reverts on bean-check failure. The user sees:
   > "This ledger is valid beancount (bean-check passed) but
   > Lamella cannot interpret some of it. The import was not
   > applied. Flagged directives: <list>. Options: (a) edit these
   > directives manually and re-run import, (b) file a bug with
   > the directive snippets attached."
   Without this step, a user could end up with a ledger that
   parses but the app can't boot against, the worst possible
   failure mode for a first-run experience.

The dry-run mode for this same flow (WS3 integration test against
AJ's live ledger) short-circuits step 6 and emits the planned file
writes as a diff.

---

## 10. Editor behavior (WS4 implementation contract)

Short version; WS4's own design doc will expand.

- Sidebar lists `.bean` files in `/ledger/`. Files matching
  `connector_*.bean` are flagged read-only; a "force edit" toggle
  with warning opens them. `_archive*/` files hidden by default.
- Save goes through the writer discipline: snapshot → write →
  `bean-check` → rollback on new errors.
- **Save-as-draft mode.** Writes to `<name>.draft.bean`. A synthetic
  `main.bean` with the draft included replaces the live `main.bean`
  for the bean-check run. Promote-on-confirm renames the draft to
  replace the live file atomically.
- Account-name autocomplete sourced from parsed `Open` directives.
- Error surface renders `bean-check` output inline, anchored to the
  offending line.
- Auto-fix suggestions (syntax errors, balance asserts, etc.) follow
  §7.4 marker comment format on the original line.

---

## 11. Import decision matrix: quick reference

Condensed lookup for the full §7 + §9 rules. When the import sees a
ledger built by another tool and has to decide how to represent it
in our layout:

| If directive | Goes to |
|---|---|
| already matches our shape | Keep bucket, no-op |
| is recognized but foreign-shaped | Transform bucket, shown before/after diff |
| is unrecognized | Foreign bucket, kept with warning unless user opts to strip |
| declares a plugin outside §5 allowlist | Blocks the import until resolved |
| has `lamella-*` metadata already | Dedup by that metadata; never re-stamp |

The output of a successful import is a ledger that would pass a
second import run through the Keep bucket 100%, with zero items in
Transform or Foreign. That's the convergence test.

---

## 12. Decisions log + remaining open items

### 12.1 Closed decisions (locked in this draft)

1. **Header prose.** Approved as written in §2.1 / §2.2 with the
   addition of a `Generated: YYYY-MM-DD by Lamella vX.Y.Z` line
   rewritten on every Connector-owned write, stamped once by the
   scaffolder for user-authored files.
2. **`lamella-ledger-version` date.** Fixed schema-epoch date
   `2026-01-01`, not per-install. See §6.4 date-semantics paragraph.
3. **Plugin allowlist.** Static, compile-time, enforced at the
   image level. No user-editable config file. Re-introducing a
   dynamic list would re-introduce the drift vector the
   consolidation was designed to eliminate.
4. **Marker format location.** `src/lamella/bootstrap/
   markers.py`, exposing both `format_removal_marker` (writer) and
   `parse_removal_marker` (parser). See §7.4.
5. **Missing-version-marker handling.** Gentle path: treat as v0,
   stamp v1 on next write, log the stamping event prominently. See
   §6.4 missing-marker paragraph.

### 12.2 Remaining open items

None blocking WS3. The seven gaps raised during review are all
addressed in the spec above:

| Gap | Address |
|---|---|
| Reconstruct-failure rollback | §9 step 8.5 |
| Concurrent-access lock | §1 invariants (`.lamella-lock`) |
| Open directive collision (beancount errors, dedup before write) | §3.1 ordering contract |
| `custom "lamella-*"` date semantics | §6.4 date-semantics paragraph |
| Transform reversibility | §7 reversibility column + §7.2 tags |
| Multi-currency flag specced vs. deferred | §3.2 "no multi-currency in v1" |
| `custom` directive type name inconsistency with `lamella-*` rule | §6.1 rule 2 + §6.2 enumeration |

### 12.3 Meta-rule (from user, locked in)

Any future change to the canonical layout files defined in this
document is a major-version-bump event. `lamella-ledger-version`
bumps, the software's semver major bumps, a migration pass ships,
and a v`N` → v`N+1` fixture is preserved for the test suite. This
rule applies to:

- Any change to the `main.bean` canonical shape (§3).
- Any change to the Connector-owned file list (§2.1).
- Any change to the header templates (§2.1 / §2.2).
- Any change to the plugin allowlist (§5).
- Any change to the `lamella-*` schema table (§6.2) that renames,
  removes, or changes the type of an existing key.
- Any change to the marker comment format (§7.4).

Adding *new* Connector-owned files, *new* `lamella-*` keys, *new*
custom directive type names, or *new* optional includes is a minor
version bump: existing ledgers remain valid because old data still
parses and the scaffolder populates the new file on next write.

**The rule in one sentence:** additions are backward-compatible and
bump minor; changes and removals are not backward-compatible and
bump major.

### 12.4 Spec status

WS3 is unblocked. Implementation may proceed once this draft is
committed to git.

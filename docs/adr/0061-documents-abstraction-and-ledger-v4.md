# ADR-0061: Documents Abstraction and Ledger v4 Cutover

- **Status:** Accepted (2026-05-02)
- **Date:** 2026-05-02
- **Author:** AJ Quick
- **Related:** [ADR-0001](0001-ledger-as-source-of-truth.md), [ADR-0003](0003-lamella-metadata-namespace.md), [ADR-0015](0015-reconstruct-capability-invariant.md), [ADR-0016](0016-paperless-writeback-policy.md), [ADR-0020](0020-adapter-pattern-for-external-data-sources.md), [ADR-0026](0026-migrations-forward-only.md), [ADR-0044](0044-paperless-lamella-custom-fields.md), [ADR-0053](0053-paperless-read-precedence-and-self-test.md), [ADR-0055](0055-prompts-must-be-generalized.md), [ADR-0056](0056-receipts-attach-pre-classification.md)
- **Supersedes:** None (extends ADR-0044)

## Context

The Paperless integration was designed under the assumption that
linked documents are receipts. That assumption is baked into seven
load-bearing surfaces:

1. **Ledger directives.** Links are written as
   `custom "receipt-link" "<paperless_id>" "<txn_hash>"` blocks
   (`linker.py:254`). Dismissals are `custom "receipt-dismiss"`. The
   directive type is part of the ledger contract that the
   reconstruct invariant (ADR-0015) preserves.

2. **DB tables.** `receipt_links`, `receipt_dismissals`,
   `receipt_link_blocks` all carry the noun in the schema. Joins,
   queries, and reboot-time backfill steps name the tables
   directly.

3. **Class and function names.** `ReceiptLinker`, `ReceiptContext`,
   `ReceiptVerification`, `NeedsReceiptItem`,
   `find_paperless_candidates`, `fetch_receipt_context`,
   `_doctype_excluded` (which excludes statements, 1099s, tax docs
   from receipt matching).

4. **AI prompt strings.** The two cascade prompts in
   `paperless_bridge/verify.py` (`SYSTEM` line 417, `SYSTEM_OCR_ONLY`
   line 427, plus the user-prompt builders) name the document a
   "receipt" 20+ times. The extracted schema
   (`ReceiptVerification`, line 293) carries `receipt_date`,
   `vendor`, `total`, `subtotal`, `tax`, `tip`, `line_items`. There
   is no per-document-type branching.

5. **Routes and templates.** `/receipts`, `/receipts/needed`,
   `/receipts/dangling`, `/txn/{token}/receipt-link`,
   `/txn/{token}/receipt-unlink`, `/txn/{token}/receipt-section`,
   `/txn/{token}/receipt-search`, plus 14 templates under
   `web/templates/` whose filenames carry "receipt".

6. **Settings keys.** `receipt_required_threshold_usd`,
   `paperless_doc_type_roles` (which maps Paperless document-type
   IDs to the strings `receipt | invoice | ignore`).

7. **Excluded-document filter.** `RECEIPT_EXCLUDED_DOCTYPE_PATTERNS`
   (`txn_matcher.py:124`) hard-codes that statements, 1099s, and
   tax docs are NOT receipts and therefore not match candidates.
   This is correct for the txn→doc match path but wrong for a
   future system that wants to match invoices and orders.

The system needs to handle invoices, orders, and other document
types where the matching, extraction, and linking logic is mostly
the same — date, amount, vendor are common to all — but where some
fields (PO number for invoices, line items for orders) require
type-conditional extraction. Without a generic abstraction, every
new document type forces a parallel code path with its own table,
prompt, and matcher.

This ADR is foundational: ADR-0062 (tag-driven workflow engine) and
ADR-0063 (reverse doc→txn auto-link) both depend on a
document-type-aware extraction prompt and a generic document
abstraction.

## Decision

Lamella generalizes "receipt" to "document" across the entire
Paperless integration. The cutover is a **forward-only ledger
version bump from v3 to v4** with **opportunistic rewrite** of
legacy `receipt-link` and `receipt-dismiss` directives, and a
**document-type discriminator** on the new abstraction.

The `Lamella_*` writeback namespace established by ADR-0044 is
extended to cover Paperless tags (the same `Lamella_` prefix
applies, PascalCase, no second separator).

### 1. Ledger version bump v3 → v4

`LATEST_LEDGER_VERSION` (`core/bootstrap/detection.py:51`) goes
from `3` to `4`. Bootstrapped ledgers stamp
`custom "lamella-ledger-version" "4"`. The migration runs once on
first read of a v3 ledger:

- A new transform step `step26_receipt_link_to_document_link.py`
  rewrites every `custom "receipt-link"` and
  `custom "receipt-dismiss"` directive in-place to `document-link`
  and `document-dismiss`, preserving all positional arguments,
  metadata, and surrounding context.
- The step is **opportunistic at write time, not eager at read
  time**: the v4 reader continues to parse `receipt-link` and
  `receipt-dismiss` directives (backwards-compatible read), and
  only rewrites them when the surrounding file is touched for any
  other reason. This keeps the v3→v4 transition diff-bounded
  rather than producing a giant one-shot rewrite of every ledger
  file the user has ever touched.
- The migration is forward-only per ADR-0026. v4 ledgers have the
  `lamella-ledger-version` stamp set to 4; v3 Lamella refuses to
  read them rather than silently truncating the new directive
  vocabulary.

### 2. Directive vocabulary

The legacy and current vocabulary coexist in the v4 reader:

| Legacy (v3) | Current (v4) | Reader | Writer |
|---|---|---|---|
| `custom "receipt-link"` | `custom "document-link"` | reads both | writes only `document-link` |
| `custom "receipt-dismiss"` | `custom "document-dismiss"` | reads both | writes only `document-dismiss` |

The metadata keys carried by these directives (`lamella-paperless-id`,
`lamella-paperless-hash`, `lamella-paperless-url`,
`lamella-match-method`, `lamella-match-confidence`,
`lamella-txn-date`, `lamella-txn-amount`) are unchanged. Only the
directive type is renamed; the payload shape is preserved.

### 3. Database renames

| Old | New |
|---|---|
| `receipt_links` | `document_links` |
| `receipt_dismissals` | `document_dismissals` |
| `receipt_link_blocks` | `document_link_blocks` |

Column renames inside those tables:

| Old | New |
|---|---|
| `receipt_date` (in `paperless_doc_index`) | `document_date` |

The migration is a forward-only `ALTER TABLE ... RENAME` followed
by a backfill of existing rows with `document_type = 'receipt'`
(unless the source's Paperless document-type already says
otherwise). All callers update to read the new names; no shim
layer.

### 4. Document type discriminator

A new column `document_type TEXT` is added to `paperless_doc_index`
(populated from Paperless's `document_type` field at sync time)
with the canonical values:

- `receipt` — point-of-sale receipt
- `invoice` — vendor invoice or bill
- `order` — purchase order or order confirmation
- `statement` — bank/credit-card statement (excluded from auto-match)
- `tax` — tax form (excluded from auto-match)
- `other` — anything else (default)

The mapping from Paperless's user-defined document types to these
canonical values lives in settings (`paperless_doc_type_roles`,
already exists for `receipt | invoice | ignore`; this ADR widens
the value set). Statements and tax forms map to types that are
excluded from auto-link by default (replacing the hard-coded
`RECEIPT_EXCLUDED_DOCTYPE_PATTERNS` regex).

### 5. AI extraction generalization

`ReceiptVerification` (`verify.py:293`) is renamed to
`DocumentVerification`. The prompt builders accept a
`document_type` parameter and select the extraction schema:

- All types extract: `document_date`, `vendor`, `total`, `confidence`
- `receipt` adds: `subtotal`, `tax`, `tip`, `line_items`
- `invoice` adds: `po_number`, `invoice_number`, `due_date`,
  `line_items`
- `order` adds: `order_number`, `line_items`, `ship_date`
- `statement` and `tax` extract minimal fields and are not auto-linked

The cascade structure (Tier 1 OCR-text → Tier 2 vision) is
unchanged. The prompts cite the document type explicitly per
ADR-0055 (no overfit to one failure mode); type-conditional
sections of the prompt are append-only blocks gated on the type.

### 6. Routes and 308 redirects

User-bookmarkable routes are renamed and the legacy paths return
**308 Permanent Redirect** (preserves method, unlike 301):

| Legacy | Current |
|---|---|
| `/receipts` | `/documents` |
| `/receipts/needed` | `/documents/needed` |
| `/receipts/dangling` | `/documents/dangling` |
| `/txn/{token}/receipt-section` | `/txn/{token}/document-section` |
| `/txn/{token}/receipt-search` | `/txn/{token}/document-search` |
| `/txn/{token}/receipt-link` | `/txn/{token}/document-link` |
| `/txn/{token}/receipt-unlink` | `/txn/{token}/document-unlink` |

The 308s are added in `web/routes/_legacy_redirects.py` (new
file). They live indefinitely — there is no removal date. The cost
of keeping them is one route table entry each.

### 7. Class and function renames

| Legacy | Current |
|---|---|
| `ReceiptLinker` | `DocumentLinker` |
| `ReceiptContext` | `DocumentContext` |
| `ReceiptVerification` | `DocumentVerification` |
| `NeedsReceiptItem` | `NeedsDocumentItem` |
| `find_paperless_candidates` | `find_document_candidates` |
| `fetch_receipt_context` | `fetch_document_context` |
| `RECEIPT_EXCLUDED_DOCTYPE_PATTERNS` | removed (replaced by `document_type` discriminator) |

Renames are mechanical. No behavior change except the discriminator
replacing the regex.

### 8. Tag namespace extension to ADR-0044

ADR-0044 reserved `Lamella_*` PascalCase for Paperless custom
fields. This ADR extends that reservation to **Paperless tags** (a
distinct namespace in Paperless, but the prefix convention is
shared for grep-ability and user clarity). Concrete tag names are
defined in ADR-0062, not here. The reservation is what this ADR
codifies.

### 9. Settings keys

| Legacy | Current |
|---|---|
| `receipt_required_threshold_usd` | `document_required_threshold_usd` |
| `paperless_doc_type_roles` (values: `receipt \| invoice \| ignore`) | unchanged key, widened value set per §4 |

The settings rename uses the existing settings-migration helper
(see `core/settings/store.py`). On first read, a v3 settings row
is renamed in-place to its v4 key.

## Why this works

- **The ledger contract (ADR-0001, ADR-0015) is preserved.** The
  reconstruct invariant says any reboot must rebuild DB state from
  the ledger. The v4 reader reads both directive vocabularies, so
  a reconstruct against a partially-migrated ledger (some
  `receipt-link`, some `document-link`) produces the correct DB
  state. The migration step in §1 only adds rewrites; it never
  drops information.

- **Forward-only migration (ADR-0026) is preserved.** The version
  bump prevents downgrade. v3 Lamella refuses v4 ledgers rather
  than silently dropping `document-link` directives it doesn't
  understand. There is no rollback path — if a v4 ledger needs to
  be undone, the user restores from the ledger backup created on
  bootstrap.

- **The opportunistic rewrite policy avoids a giant one-shot
  diff.** A user with three years of receipts in their ledger would
  see a churn-the-world commit if the migration were eager. The
  opportunistic version produces small, targeted diffs each time a
  file is touched — same pattern reconstruct already uses. The
  cost is that legacy directives linger until touched; the
  benefit is that the migration is reviewable.

- **The `Lamella_*` reservation extends naturally to tags
  (ADR-0044).** Users grep `Lamella_` once and find both their
  custom fields and their tags. The PascalCase convention is
  reused without modification.

- **The document-type discriminator replaces a regex hack.** The
  current `RECEIPT_EXCLUDED_DOCTYPE_PATTERNS` regex matches on
  free-text Paperless document-type names ("statement", "1099",
  "tax"), which is fragile against user-defined types. The
  discriminator is a typed enum populated from
  `paperless_doc_type_roles` settings, which the user controls
  explicitly.

- **Prompt generalization respects ADR-0055.** Type-conditional
  prompt sections are append-only and parameterized; they do not
  overfit to one failure mode. A new document type adds a
  conditional block; it does not edit the shared prompt body.

## Compliance checks

This ADR is satisfied iff:

1. `LATEST_LEDGER_VERSION = 4` in `detection.py`.
2. `step26_receipt_link_to_document_link.py` exists and is
   registered in the transform pipeline.
3. The v4 reader parses both `receipt-link`/`document-link` and
   both `receipt-dismiss`/`document-dismiss`. The writer only
   emits `document-link` and `document-dismiss`. (Test:
   `tests/test_step26_directive_rewrite.py`.)
4. `paperless_doc_index.document_type` column exists, populated
   from the Paperless `document_type` field, with the canonical
   value set from §4.
5. All 308 redirects from §6 resolve to the new paths and preserve
   the request method. (Test:
   `tests/test_legacy_receipt_route_redirects.py`.)
6. Class names from §7 are renamed; legacy names are removed (no
   shim re-exports per the project's no-backwards-compat rule for
   internal code).
7. ADR-0044 is updated with a one-paragraph reservation extending
   `Lamella_*` to Paperless tags (this ADR is the supersession
   reference).
8. `docs/features/receipts.md` is renamed to
   `docs/features/documents.md` and rewritten to current state.

## Sequencing

ADR-0061 lands first. It is a precondition for ADR-0062 (which
needs document-type-aware extraction prompts) and ADR-0063 (which
needs the generic `find_document_candidates` matcher to invert
direction).

## What this ADR does NOT decide

- The polling cadence and rule schema for tag-driven automation —
  that is ADR-0062.
- The reverse direction matcher — that is ADR-0063.
- Custom shell-command escape hatches for anomaly resolution —
  rejected; review-queue UI handles it (see ADR-0062).
- Removal of the v4 reader's backwards-compat for `receipt-link`
  directives. There is no removal date. Backwards-compat read is
  permanent.

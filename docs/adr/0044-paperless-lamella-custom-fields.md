# ADR-0044: Paperless Writeback Uses `Lamella_`-Prefixed Custom Fields

- **Status:** Accepted
- **Date:** 2026-04-27
- **Author:** AJ Quick
- **Related:** [ADR-0003](0003-lamella-metadata-namespace.md), [ADR-0016](0016-paperless-writeback-policy.md), [ADR-0019](0019-transaction-identity-use-helpers.md), [ADR-0042](0042-chart-of-accounts-standard.md), [ADR-0064](0064-lamella-paperless-namespace-colon.md)

> **Separator superseded by [ADR-0064](0064-lamella-paperless-namespace-colon.md)
> (2026-05-02).** The reserved `Lamella` namespace is preserved; only
> the separator flips from `_` to `:`. The four canonical writeback
> fields below are now `Lamella:Entity`, `Lamella:Category`,
> `Lamella:TXN`, `Lamella:Account`. Reads accept both forms; writes
> use the colon form. The body of this ADR is otherwise unchanged.

## Context

Lamella links Paperless documents (receipts, invoices) to ledger
transactions. The current Paperless integration uses two distinct
classes of custom fields and has accumulated friction in both:

1. **Pre-match parsing fields**: `Amount`, `Subtotal`, `Total`,
   `Sales tax`. These are populated by the user (or by Paperless's
   own OCR / field extraction) and read by Lamella's matcher to
   compare against staged transactions. Their names vary across
   Paperless deployments because users name them however they want.

2. **Post-match writeback fields**: currently a mixed bag:
   `vendor`, `receipt_date`, `payment_last_four`. The matcher writes
   these back onto a Paperless document after a transaction match
   succeeds. The names were chosen ad-hoc, and the Setup status page
   prompts users to "Create in Paperless" for missing roles even
   though Paperless already has reliable built-in fallbacks for
   `vendor` (the `correspondent` field) and `receipt_date` (the
   `created` date).

The mismatch surfaces three concrete problems:

- **Setup friction**: users see a "missing" badge and a "Create in
  Paperless" CTA for fields that aren't actually required, making the
  setup feel mandatory when it isn't.
- **Namespace collision**: `vendor`, `receipt_date`,
  `payment_last_four` are common-English-word field names. Any user
  who already has a `vendor` field for their own purposes will collide
  with Lamella's writeback semantics.
- **Insufficient writeback richness**: the system only writes back the
  payment card hint (`payment_last_four`) but not the entity, category,
  or transaction id. The richer link would let Paperless searches
  surface "all receipts for `AcmeCoLLC`" or "all receipts for
  `Expenses:AcmeCoLLC:OfficeSupplies`" without consulting Lamella.

ADR-0003 already established that Beancount metadata uses the
`lamella-*` prefix to namespace system-managed keys against
user-authored keys. This ADR mirrors that decision on the Paperless
side, with the casing convention adjusted to Paperless's typical
field-name style (`Title_Case`).

## Decision

Lamella's Paperless integration MUST use `Lamella_`-prefixed custom
fields for all post-match writeback. Four canonical fields are
established:

| Field name | Type | Populated by | Purpose |
|---|---|---|---|
| `Lamella_Entity` | string | matcher | Entity slug for the matched txn (e.g. `AcmeCoLLC`). Lets Paperless searches filter to documents per legal entity. |
| `Lamella_Category` | string | matcher | Full Beancount account path (e.g. `Expenses:AcmeCoLLC:OfficeSupplies`). Lets Paperless searches surface all receipts for a tax category. |
| `Lamella_TXN` | string | matcher | The matched transaction's `lamella-txn-id` UUIDv7. Canonical link back to the ledger; survives ledger renames and reconstructs because the UUID is stable per transaction. |
| `Lamella_Account` | string | matcher | Display name of the payment account (e.g. `Bank One Signature Credit Card`). Replaces the prior `payment_last_four` field; the human-readable form is more useful for search and disambiguates better in multi-card households than four digits. |

The matcher writes these four fields atomically when a transaction
match is finalized (per the writeback policy in ADR-0016). Confidence
gates from ADR-0016 still apply. Low-confidence matches do not
write back.

### Pre-match parsing fields stay as-is

`Amount`, `Subtotal`, `Total`, `Sales tax` (and any other monetary
fields the user maps for parsing) are NOT renamed. They are user-owned
fields the matcher reads, not system-managed fields the matcher
writes. The Setup status table continues to map these to canonical
roles (`total`, `subtotal`, `tax`).

### Built-in fallbacks supersede prior fields

`vendor` and `receipt_date` are removed from the required-roles list
entirely. Paperless's built-in `correspondent` and `created` fields
are the canonical sources, and the matcher reads them without a
custom-field mapping. The Setup status table MUST NOT prompt the user
to create these fields.

### Migration path for `payment_last_four`

`payment_last_four` is superseded by `Lamella_Account`. The matcher
stops writing the four-digit suffix; new writes use the full account
display name. Existing `payment_last_four` values on past documents
are not migrated. They remain as historical data. The Setup status
table no longer surfaces `payment_last_four` as a role.

### Field creation discipline

When the matcher first attempts to write `Lamella_*` fields against
a Paperless instance that doesn't have them yet, the writer creates
the four fields once (idempotent, guarded by a "field exists" check
against the Paperless field listing). No user action required. Field
type is always `string`; no schema versioning needed because field
semantics are stable per this ADR.

### What Lamella never writes

The decision constrains what Lamella writes in addition to what it
writes. Specifically:

- **Tags** are the user's namespace. The matcher MUST NOT inject tags
  for entity / category / payment account. Tags are reserved for
  user-applied semantic labels (`important`, `tax-deductible`,
  `to-review`), not system-managed metadata.
- **Notes** stay user-authored. The matcher MAY append a single
  `Auto-linked by Lamella to txn <uuid>` note when first matching,
  per ADR-0016's writeback policy. It MUST NOT rewrite existing notes.
- **Other custom fields** (anything without the `Lamella_` prefix) are
  user-owned. The matcher MUST NOT modify them.

## Consequences

### Positive

- Namespace clarity. Any field beginning with `Lamella_` is
  unambiguously system-managed; everything else is the user's. The
  same defense ADR-0003 provides for Beancount metadata.
- Setup simplification. The Setup status table drops `vendor`,
  `receipt_date`, and `payment_last_four`. Only `total` (with
  `subtotal` / `tax` as recommended companions) remains as a parsing
  role. Optional `Lamella_*` fields are created automatically by the
  matcher; no user intervention.
- Richer search in Paperless. Filtering documents by entity,
  category, or matched-txn UUID becomes possible without hitting
  Lamella's API.
- Stable cross-system link. `Lamella_TXN` survives ledger reorgs,
  account renames, and reconstructs because UUIDv7 is stable per
  transaction (ADR-0019).

### Negative / Costs

- Existing Paperless deployments with `vendor` / `receipt_date` /
  `payment_last_four` fields lose Lamella's writeback to those names.
  Operators using those fields for their own purposes are unaffected
  (Lamella was never the primary author); operators relying on
  Lamella's writeback there see the data move to the new
  `Lamella_Account` field instead.
- One-time field creation against a fresh Paperless instance. The
  matcher's first writeback creates four fields. Idempotent and
  guarded; no user action required, but the very first match takes
  marginally longer.
- Documentation churn. The Setup status page, the Paperless feature
  blueprint (`docs/features/paperless-bridge.md`), and the matcher
  module docstrings need to be updated.

### Mitigations

- The field-creation step uses the same tenacity retry + 30s timeout
  pattern as every other Paperless HTTP call (ADR-0027). Failure to
  create fields surfaces as a job-event-log error, never blocks the
  match itself.
- The Setup status UI is updated to remove the misleading "missing"
  badges and "Create in Paperless" CTAs for the deprecated roles.
- A one-line note in `docs/features/paperless-bridge.md` explains
  the migration to operators reading the existing `payment_last_four`
  field on historical documents.

## Compliance

- Grep `src/lamella/` for `payment_last_four` and `receipt_required`
  related field names. The only occurrences after this ADR lands
  must be migration helpers, historical-doc readers, or the deprecated
  Setup-status fallback. New writes MUST use `Lamella_*`.
- The Paperless writer asserts that every writeback field name
  begins with `Lamella_`. Any non-conforming field name raises
  `InvalidWritebackFieldError` before any HTTP call.
- The Setup status page MUST NOT list `vendor`, `receipt_date`, or
  `payment_last_four` as required or as candidates for "Create in
  Paperless".

## References

- [ADR-0003](0003-lamella-metadata-namespace.md): `lamella-*` namespace for Beancount metadata. This ADR mirrors the same defense on the Paperless side.
- [ADR-0016](0016-paperless-writeback-policy.md): confidence-gated writeback. Unchanged; this ADR refines what gets written.
- [ADR-0019](0019-transaction-identity-use-helpers.md): `lamella-txn-id` is the canonical txn UUID, stored in `Lamella_TXN` per this ADR.
- [ADR-0027](0027-http-tenacity-30s.md): tenacity retry policy applies to the new field-creation HTTP calls.
- [ADR-0042](0042-chart-of-accounts-standard.md): defines the account paths that populate `Lamella_Category`.
- `docs/features/paperless-bridge.md`: feature blueprint; needs an update to reflect this ADR.

# ADR-0053: Paperless Read Precedence + Integration Self-Test

- **Status:** Accepted
- **Date:** 2026-04-29
- **Related:** [ADR-0016](0016-paperless-writeback-policy.md), [ADR-0020](0020-adapter-pattern-for-external-data-sources.md), [ADR-0044](0044-paperless-lamella-custom-fields.md)

## Context

Lamella's Paperless integration has accumulated bugs that are
architecturally invisible: each one looks like a small implementation
miss, but they share a root cause. Lamella reads metadata FROM Paperless
in many places (verify cascade, matcher, /receipts page, audit PDF) and
writes metadata TO Paperless in fewer places (verify writeback, matcher
post-link writeback). Neither direction has a single canonical helper
that enforces the contract, so each call site re-implements the read or
write logic from memory and drifts.

Recent failures the user caught after the system had been broken for
days:

- The verify cascade rendered "Current Paperless fields: vendor (not
  set), receipt_date (not set)" in the prompt baseline even when the
  user's Paperless document had a populated `correspondent` and a
  populated `created` date. Diff machinery compared the AI extraction
  against null and tried to overwrite null with the extraction every
  time. The fix was 4 lines in `_current_fields_dict`, but the bug
  persisted for the full life of the verify feature because no other
  code path read those columns and noticed the gap.

- The matcher's `apply_writeback` (write the four `Lamella_*` fields
  after a receipt link) was wired into `auto_match.py` and `hunt.py`
  but not into the six manual-attach routes (`receipts.py`,
  `txn_receipt.py`, `card.py`, `review.py`, `webhooks.py`,
  `receipts_needed.py`). Result: when a user manually linked a
  receipt through the UI, Paperless's Lamella_* custom fields stayed
  empty even when the linked txn had everything to populate them.

- The Paperless field-map UI surfaced auto-created `Lamella_*`
  fields as `ignore`, which read as "these don't do anything" rather
  than "the matcher writes to these after a receipt is linked." Took
  user inspection to surface as confusing.

ADR-0044 said built-ins ARE the fallback for vendor / receipt_date.
ADR-0016 said writeback is gated on `paperless_writeback_enabled`.
But neither ADR established a code-level helper or a self-test that
keeps the contract from drifting. The result was that callers
re-implemented the read / write logic ad-hoc and got it wrong.

## Decision

Two interlocking rules: a read-precedence helper that every consumer
must use, and a self-test that exercises the full read+write loop at
setup time so future drift is caught before user testing.

### 1. Read precedence: a single helper, used everywhere

Reading "the current value of field F on Paperless document D" must
go through one helper. The helper's contract:

```
read(field, doc_row):
  1. If a Paperless custom field is mapped to role F AND has a value:
     return that value.
  2. Else, if F has a built-in fallback per the table below AND the
     built-in is populated: return the built-in value.
  3. Else: return None.
```

Built-in fallback table (matches ADR-0044's spec):

| Role           | Custom-field source        | Built-in fallback          |
|----------------|----------------------------|----------------------------|
| `vendor`       | role=`vendor` mapped field | `correspondent_name`       |
| `receipt_date` | role=`receipt_date` field  | `created` date             |
| `total`        | role=`total` mapped field  | (no built-in: returns None)|
| `subtotal`     | role=`subtotal` mapped     | (no built-in)              |
| `tax`          | role=`tax` mapped          | (no built-in)              |
| `payment_last_four` | role=`payment_last_four` | (no built-in)         |

The helper lives at `lamella.features.paperless_bridge.read_field`
(new module). Every existing read site must migrate; new reads MUST
NOT inline the fallback chain. A grep test in CI fails on direct
reads of `row["vendor"]` or `row["receipt_date"]` from
`paperless_doc_index` outside the helper.

**This rule retroactively forbids the bug from
`verify.py::_current_fields_dict`**: it read only custom-field
columns. With the helper, the fallback chain is enforced once,
correctly, and shared by every consumer.

### 2. Setup-time integration self-test

`/setup/check` (or a new `/settings/paperless/diagnose`) MUST run an
end-to-end smoke test against the user's configured Paperless instance
and report PASS / FAIL per check, with the failing reason inline:

| Check                      | What it does                                                                         |
|----------------------------|--------------------------------------------------------------------------------------|
| `paperless.connect`        | GET `/api/`. PASS if 200 + auth accepted; FAIL with the actual HTTP body otherwise.  |
| `paperless.read.builtin`   | Pick a recent doc; read correspondent + created via the helper. PASS if non-null.    |
| `paperless.read.custom`    | If any custom fields are mapped, read them via the helper. PASS unconditionally.     |
| `paperless.write.lamella`  | Create / verify the four `Lamella_*` custom fields exist; write a test value to a    |
|                            | sentinel doc; read it back; clean up. PASS if round-trip succeeds.                   |
| `paperless.tag.fixed`      | Verify `Lamella Fixed` and `Lamella Enriched` tags exist or can be created.          |

The self-test runs:

- On demand from the settings page (button: "Test Paperless integration").
- As part of `/setup/check` — failures here block the green-light banner.
- After every successful `/settings/paperless-fields/refresh` so a
  freshly-resynced field map gets validated.

Failures are surfaced with the actual HTTP response body, not a
generic "Paperless is unhealthy." A `paperless.write.lamella` failure
that returns "field not found" tells the user "click Refresh local
cache or check that you didn't delete the Lamella_* fields in
Paperless."

### 3. Writeback consistency

All callers that write to Paperless after a successful receipt link
go through `writeback_after_link`. Already established in commit
06a7076 / 2fb1d0a but explicitly captured here as part of the
canonical contract: there is exactly ONE function that knows how to
populate the four `Lamella_*` fields, and any new linking code path
MUST call it.

`paperless_writeback_enabled` (default OFF) gates the verify-cascade's
PATCH of `total / subtotal / tax / vendor / receipt_date / title` only.
The matcher's `Lamella_*` writeback is unconditional: those fields are
the canonical Lamella↔Paperless link, populating them is part of what
"linking a receipt to a transaction" means by definition. The verify
cascade's gating is about "does Lamella overwrite the user's
already-set fields when its OCR re-extraction disagrees" — a different
question than "does Lamella populate the link metadata." The Setup
status panel makes this distinction explicit.

## Consequences

- **One read helper, many writes.** Future code that needs the
  current value of a Paperless field uses `read_field`. CI grep test
  prevents the inlined-fallback drift that was the recent bug.

- **Self-test catches drift before users do.** A user upgrading to a
  new Paperless version that changes API shape, or a user who
  accidentally deletes a `Lamella_*` field, sees a red badge on
  `/settings/paperless` instead of silently broken writebacks for
  weeks.

- **Setup green-light has a real meaning.** Today `/setup/check`
  green means "Lamella's local config looks consistent." After this
  ADR, green also means "we've actually round-tripped a write to
  Paperless and read it back." The bar for green moves up; the
  user's confidence in the green-light moves up too.

- **Diagnostic page surfaces the actual error.** When Paperless
  returns 4xx or 5xx, the self-test reports the body. Users debugging
  setup don't have to read container logs to know the API token is
  wrong.

- **Manageable migration cost.** The read sites are countable
  (`grep -rn "row\[.vendor.\]" src/lamella/`). The migration is a
  rename in each. The self-test is one new module + one new route +
  one button on /settings/paperless. Estimate: 4-6 hours of work,
  most of it in the read-site migration. The self-test itself is
  ~150 lines.

## Notes

The recent verify-baseline bug (`_current_fields_dict` ignoring
`correspondent_name`) was patched directly in
`features/paperless_bridge/verify.py:1373` (commit bf7d326). That fix
stays even after this ADR lands; once the read-helper module exists,
the helper subsumes the inline fallback in `_current_fields_dict` and
that file gets simplified by 6 lines.

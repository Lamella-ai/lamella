# ADR-0016: Paperless Writeback Policy

- **Status:** Accepted
- **Date:** 2026-04-26
- **Author:** AJ Quick
- **Related:** [ADR-0001](0001-ledger-as-source-of-truth.md), [ADR-0015](0015-reconstruct-capability-invariant.md), `CLAUDE.md` ("Non-negotiable architectural rules" → "Paperless is the source of truth for documents"), `src/lamella/paperless/verify.py`, `src/lamella/paperless/client.py`

## Context

Lamella reads the Paperless-ngx document index for two purposes:
receipt context at classify time and receipt verification flows.
Paperless is the authoritative store for document images and OCR
text. We do not own those documents; the user manages them.

A narrow set of writeback operations has clear value: when a
vision AI validates a structured field value against the original
image, correcting that field in Paperless keeps the document
system accurate without requiring manual edits. But unrestricted
writeback creates risks: overwriting OCR text on multi-page PDFs
destroys content on pages 2+, AI-invented values produce worse
data than the original OCR, and silent writes are unauditable.

The writeback surface must be narrow enough that every write is
defensible and reversible by a human auditor.

## Decision

Lamella MAY write back to Paperless only under all of the following conditions:

1. **Vision-AI validated.** The field value was verified against
   the original document image by a vision AI call. Untested
   field values MUST NOT be written back.
2. **Specific structured fields only.** Only fields with a known
   semantic mapping (via `paperless_field` custom directives in
   `connector_config.bean`) may be patched. Free-form `content`
   (OCR text) and `content_excerpt` MUST NOT be overwritten.
3. **Multi-page PDF content is off-limits.** The vision call
   sees only page 1. Writing `content` on a multi-page PDF would
   destroy pages 2+. This prohibition has no exception.
4. **Gated on `paperless_writeback_enabled`.** Default is off.
   The user must explicitly enable writebacks in settings.
5. **Tagged on every write.** Every patched document MUST receive
   either the `Lamella Fixed` tag (field correction) or the
   `Lamella Enriched` tag (enrichment from data we already hold).
6. **Logged with before/after diff.** Every writeback MUST be
   recorded in `paperless_writeback_log` with the old value, new
   value, field id, document id, and timestamp. Dedup key is
   `(paperless_id, kind, dedup_key)`.
7. **Field confidence threshold.** Fields below
   `DEFAULT_FIELD_CONFIDENCE_THRESHOLD` (0.80 as defined in
   `verify.py`) are flagged in the audit log and NOT patched.

MUST NOT: invent content not present in the source document.
MUST NOT: run writebacks on any document not linked to a
Lamella-classified transaction.

## Consequences

### Positive
- Paperless field corrections flow automatically after verification
  without manual user edits to each document.
- The `Lamella Fixed` / `Lamella Enriched` tags let a human audit
  every write by filtering Paperless to those tags.
- The `paperless_writeback_log` table provides a before/after diff
  for every write, making reversals straightforward.

### Negative / Costs
- The gate (vision AI + field confidence threshold) means most
  documents are never corrected automatically, even when the OCR
  was wrong. Low-confidence corrections require manual action.
- `paperless_writeback_log` is a SQLite cache: if deleted, the
  dedup guard is reset and old documents could be re-patched.
  This is acceptable because the tags in Paperless itself form
  a secondary audit trail.

### Mitigations
- Default-off setting ensures no writes happen until the user
  consciously enables them and understands the behavior.
- The `Lamella Fixed` / `Lamella Enriched` tags are visible in
  Paperless without any Lamella tooling, so the audit trail
  survives even if this system is retired.

## Compliance

`src/lamella/paperless/verify.py` is the canonical implementation.
Key constants: `TAG_FIXED`, `TAG_ENRICHED`, `DEFAULT_FIELD_CONFIDENCE_THRESHOLD`.
The `_writeback_enabled()` guard in `VerifyService` enforces the
settings gate. New code that writes to Paperless outside
`paperless/verify.py` or `paperless/client.py` write endpoints
is a violation and requires a design review.

## References

- CLAUDE.md § "Non-negotiable architectural rules, Paperless is the source of truth for documents"
- `src/lamella/paperless/verify.py` (canonical writeback service)
- `src/lamella/paperless/client.py` (write endpoints: `patch_document`, `ensure_tag_exists`)
- [ADR-0001](0001-ledger-as-source-of-truth.md): `paperless_writeback_log` is a SQLite cache, not state
- [ADR-0015](0015-reconstruct-capability-invariant.md): writeback log repopulates from Paperless tags

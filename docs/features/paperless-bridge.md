---
audience: agents
read-cost-target: 100 lines
authority: informative
status: Active Development
cross-refs: docs/adr/0001-ledger-as-source-of-truth.md, docs/adr/0004-bean-check-after-every-write.md, docs/adr/0016-paperless-writeback-policy.md, docs/adr/0020-adapter-pattern-for-external-data-sources.md, docs/adr/0044-paperless-lamella-custom-fields.md
last-derived-from-code: 2026-04-27
---
# Paperless Bridge

## Summary

Paperless-ngx integration: receipt index, document fetch, fields writeback, verify pass.

## Owned routes

| Method | Path | Handler | File:line |
|---|---|---|---|
| GET | `/paperless/preview/{doc_id}` | `preview` | `src/lamella/web/routes/paperless_proxy.py:101` |
| GET | `/paperless/thumb/{doc_id}` | `thumbnail` | `src/lamella/web/routes/paperless_proxy.py:75` |
| GET | `/paperless/writebacks` | `writebacks_page` | `src/lamella/web/routes/paperless_writebacks.py:100` |
| POST | `/paperless/{doc_id}/enrich` | `enrich_document` | `src/lamella/web/routes/paperless_verify.py:286` |
| POST | `/paperless/{doc_id}/verify` | `verify_document` | `src/lamella/web/routes/paperless_verify.py:101` |
| POST | `/paperless/{doc_id}/verify/sync` | `verify_document_sync` | `src/lamella/web/routes/paperless_verify.py:251` |
| GET | `/settings/paperless-fields` | `page` | `src/lamella/web/routes/paperless_fields.py:97` |
| POST | `/settings/paperless-fields` | `save_roles` | `src/lamella/web/routes/paperless_fields.py:415` |
| POST | `/settings/paperless-fields/classify` | `classify_field` | `src/lamella/web/routes/paperless_fields.py:371` |
| POST | `/settings/paperless-fields/create` | `create_field` | `src/lamella/web/routes/paperless_fields.py:185` |
| POST | `/settings/paperless-fields/refresh` | `refresh` | `src/lamella/web/routes/paperless_fields.py:123` |

## Owned templates

- `src/lamella/web/templates/paperless_writebacks.html`
- `src/lamella/web/templates/partials/paperless_enrich_result.html`
- `src/lamella/web/templates/partials/paperless_verify_result.html`
- `src/lamella/web/templates/settings_paperless_fields.html`

## Owned source files

- `src/lamella/adapters/paperless/client.py`
- `src/lamella/adapters/paperless/schemas.py`
- `src/lamella/features/paperless_bridge/field_map.py`
- `src/lamella/features/paperless_bridge/field_map_writer.py`
- `src/lamella/features/paperless_bridge/lookups.py`
- `src/lamella/features/paperless_bridge/sync.py`
- `src/lamella/features/paperless_bridge/verify.py`
- `src/lamella/features/paperless_bridge/writeback.py`

## Owned tests

- `tests/test_enricher_paperless_writeback.py`
- `tests/test_paperless_client.py`
- `tests/test_paperless_verify.py`
- `tests/test_paperless_verify_routes.py`
- `tests/test_paperless_writebacks_route.py`
- `tests/test_webhook_paperless.py`

## ADR compliance

- ADR-0001: field-role mappings survive DB wipe via `custom "paperless-field"` in `connector_config.bean`
- ADR-0004: `field_map_writer.append_field_mapping()` runs bean-check via `append_custom_directive(run_check=True)`
- ADR-0016: writeback gated on `paperless_writeback_enabled`; multi-page PDF content never overwritten; every write tagged and logged with before/after diff
- ADR-0044: post-match writeback uses `Lamella_`-prefixed custom fields (`Lamella_Entity`, `Lamella_Category`, `Lamella_TXN`, `Lamella_Account`); the writer creates them idempotently the first time it needs them; `vendor` / `receipt_date` / `payment_last_four` are no longer required Setup-status roles

## Current state


`PaperlessClient` (`client.py`) wraps `httpx.AsyncClient` with a one-retry pattern for 5xx. Provides: `get_document()`, `get_document_metadata()` (for `original_checksum` / MD5), `iter_documents()` (paginated), `iter_recent_documents()`, `download_original()`, `download_thumbnail()`, `download_preview()`, `get_custom_fields()`.

`sync.py` runs two modes: full (all docs within `lookback_days`, ordered by `created asc`) and incremental (`modified__gt=<cursor>`). Upserts into `paperless_doc_index`; stores `content_excerpt` up to 4000 chars. Also syncs correspondents and document_types so denormalized name columns stay fresh.

`verify.py` handles writeback:
- `receipt_source_type()` / `classify_pdf_bytes()` (via PyMuPDF) determine whether to run vision. Multi-page PDFs: only page 1 is rendered to the AI; `content` / `content_excerpt` are NEVER overwritten (ADR-0016).
- Vision call: `OpenRouterClient.chat(decision_type="receipt_verify", images=[(bytes, mime)])`. Per-field confidence threshold: 0.80.
- Tags `Lamella Fixed` when corrections are applied; tags `Lamella Enriched` on the enrich path.
- Every writeback logged to `paperless_writeback_log` with before/after diff.
- `enrich_with_context()` is the non-vision path: context we already know (mileage → vehicle attribution) is pushed back as a note + custom field.

`field_map.py` / `field_map_writer.py`: user maps Paperless custom field IDs to canonical roles (`amount`, `date`, `merchant`, etc.) via `/settings/paperless`; `append_field_mapping()` stamps a `custom "paperless-field"` directive to `connector_config.bean` (ADR-0001 reconstruct path).

`writeback.py`: ADR-0044's matcher → Paperless writeback path. After `ReceiptLinker.link()` succeeds, the receipt-hunt and auto-match flows call `write_match_fields()`, which uses `PaperlessClient.ensure_lamella_writeback_fields()` (idempotent, guarded by a field-existence check) to create `Lamella_Entity`, `Lamella_Category`, `Lamella_TXN`, `Lamella_Account` if missing, then PATCHes their values onto the linked document. Field-creation and PATCH failures are logged via `log.warning` and do NOT block the match, the writeback is best-effort and gets retried next round. The `Setup status` panel on `/settings/paperless-fields` no longer surfaces `vendor` / `receipt_date` / `payment_last_four` (Paperless's built-in `correspondent` / `created` cover the first two; `Lamella_Account` carries the readable payment name and supersedes `payment_last_four`). Historical `payment_last_four` values are NOT migrated, old documents keep their existing data; new matches use the four `Lamella_*` fields. `InvalidWritebackFieldError` is raised by `PaperlessClient.writeback_lamella_fields()` before any HTTP call when a field name doesn't begin with `Lamella_`, enforcing the namespace defense at the client edge.

`lookups.py`: `cached_paperless_hash()` reads `paperless_doc_index.original_checksum`, the `md5:` prefixed hash used in `connector_links.bean`.

Writeback gate: all PATCH calls are skipped when `settings.paperless_writeback_enabled` is False (default). The `PaperlessError` exception propagates to the calling job; the job logs and continues.

### Compliant ADRs
- ADR-0001: field-role mappings survive DB wipe via `custom "paperless-field"` in `connector_config.bean`
- ADR-0004: `field_map_writer.append_field_mapping()` runs bean-check via `append_custom_directive(run_check=True)`
- ADR-0016: writeback gated on `paperless_writeback_enabled`; multi-page PDF content never overwritten; every write tagged and logged with before/after diff

### Known violations
- ADR-0020: `PaperlessClient` is instantiated directly in sync jobs and routes via a `paperless_client_factory` callable, but no `DocumentStorePort` protocol formalizes the contract (medium-high)

## Known gaps

- ADR-0020: `PaperlessClient` is instantiated directly in sync jobs and routes via a `paperless_client_factory` callable, but no `DocumentStorePort` protocol formalizes the contract (medium-high)

## Remaining tasks


1. [ ] Move Paperless behind a `DocumentStorePort` protocol: lets test fixtures inject a fake without an HTTP server; matches ADR-0020 adapter intent (medium-high).
2. [ ] Deletion events from Paperless are unhandled: stale `paperless_doc_index` rows persist until manual cleanup; add a reconciliation pass or at least a staleness-age column (medium).
3. [ ] `original_checksum` from `/api/documents/{id}/metadata/` is available but not always fetched during sync (sync path uses main doc endpoint which omits it in some Paperless versions), add a targeted metadata fetch for docs whose `original_checksum` is NULL (low).
4. [ ] `content_excerpt` truncation at 4000 chars is arbitrary: make it a `Settings` knob so a user with large receipts can increase it without a code change (low).

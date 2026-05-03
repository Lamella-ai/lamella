# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import logging
import time
from datetime import date, timedelta
from typing import Any, AsyncIterator

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from lamella.adapters.paperless.schemas import CustomField, Document

log = logging.getLogger(__name__)


# ADR-0064: namespace constants. The canonical names live in
# ``lamella.features.paperless_bridge.lamella_namespace`` and are
# duplicated as plain literals below to avoid a circular import
# (paperless_bridge.__init__ imports PaperlessClient from this
# module). The helpers
# (``test_lamella_namespace_helpers.test_writeback_field_constants_use_colon``)
# enforce that the literals stay in sync.


# ADR-0027: 3 attempts max, exponential backoff (2-10s), retry only on
# transient network/timeout errors (NOT on 4xx — those are caller bugs
# and the existing _get/_patch/_post bodies already raise PaperlessError
# on >=400). reraise=True so the original httpx exception bubbles up
# after exhaustion; the existing except-blocks keep working unchanged.
# These wrap only the bare network call — the surrounding semantics
# (_get's own 5xx-once retry, status-code checks, JSON parsing) are
# preserved verbatim.
_RETRY_KW: dict[str, Any] = dict(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(
        (httpx.TimeoutException, httpx.NetworkError)
    ),
    reraise=True,
)


@retry(**_RETRY_KW)
async def _client_get(
    client: httpx.AsyncClient,
    url: str,
    *,
    params: dict[str, Any] | None = None,
) -> httpx.Response:
    return await client.get(url, params=params)


@retry(**_RETRY_KW)
async def _client_patch(
    client: httpx.AsyncClient, url: str, *, json: dict[str, Any],
) -> httpx.Response:
    return await client.patch(url, json=json)


@retry(**_RETRY_KW)
async def _client_post(
    client: httpx.AsyncClient, url: str, *, json: dict[str, Any],
) -> httpx.Response:
    return await client.post(url, json=json)


class PaperlessError(RuntimeError):
    pass


class InvalidWritebackFieldError(ValueError):
    """Raised when a writeback field name doesn't carry a ``Lamella``
    namespace prefix (``Lamella:`` per ADR-0064; legacy ``Lamella_``
    accepted on the input side for the migration window). The matcher
    MUST only write custom fields it owns; ad-hoc names ('vendor',
    'receipt_date', 'payment_last_four') collide with user-owned
    fields and break the namespace defense ADR-0003 / ADR-0044 /
    ADR-0064 establish.

    Raised BEFORE any HTTP call so a misnamed field never reaches
    Paperless. Callers see a programming error (a clear
    ``InvalidWritebackFieldError``) instead of a silent collision
    with someone else's field on the receiving end.
    """

    pass


# ADR-0044 / ADR-0064: the four canonical writeback custom fields
# the matcher creates and writes after a successful txn match. Order
# is stable so log lines (and tests) can iterate deterministically.
# The names live in ``lamella_namespace`` so the migration module and
# this adapter stay in sync. Materialized as plain literals here to
# avoid a circular-import dance — kept in lock-step by
# ``test_lamella_namespace_helpers.test_writeback_field_constants_use_colon``.
LAMELLA_WRITEBACK_FIELD_NAMES: tuple[str, ...] = (
    "Lamella:Entity",
    "Lamella:Category",
    "Lamella:TXN",
    "Lamella:Account",
)
LAMELLA_FIELD_PREFIX = "Lamella:"
"""Canonical (colon) writeback prefix per ADR-0064. Use
``is_lamella_name(...)`` for namespace-defense checks that should also
accept the legacy underscore form."""

# Legacy underscore equivalents — used by the backwards-compat read
# shim in ``ensure_lamella_writeback_fields`` and
# ``writeback_lamella_fields`` to surface a partially-migrated
# Paperless instance under the canonical key.
_LEGACY_WRITEBACK_FIELD_NAMES: tuple[str, ...] = (
    "Lamella_Entity",
    "Lamella_Category",
    "Lamella_TXN",
    "Lamella_Account",
)
LAMELLA_NAMESPACE_PREFIX_NEW = "Lamella:"
LAMELLA_NAMESPACE_PREFIX_LEGACY = "Lamella_"


class PaperlessClient:
    """Thin async wrapper around the Paperless-ngx REST API.

    Phase 1 uses only the read side: fetch a document by id, iterate recent
    documents, and resolve custom fields to named values.
    """

    def __init__(
        self,
        base_url: str,
        api_token: str,
        *,
        timeout: float = 30.0,  # ADR-0027: 30s hard timeout
        client: httpx.AsyncClient | None = None,
        extra_headers: dict[str, str] | None = None,
    ):
        self._base_url = base_url.rstrip("/")
        self._headers = {"Authorization": f"Token {api_token}"}
        if extra_headers:
            self._headers.update(extra_headers)
        self._timeout = timeout
        self._external_client = client is not None
        self._client = client or httpx.AsyncClient(
            base_url=self._base_url,
            headers=self._headers,
            timeout=timeout,
        )
        self._field_cache: dict[int, CustomField] | None = None
        # ADR-0062: in-memory TTL cache for list_tags(). The workflow
        # engine evaluates several rules per tick and each one wants
        # to resolve a tag name → id; without memoization that's a
        # full /api/tags/ pagination per evaluation. Cache TTL is
        # short (60s) so freshly-created tags appear quickly without
        # a full restart, and ensure_tag invalidates the cache on a
        # create so the new id is visible immediately.
        self._list_tags_cache: tuple[float, dict[str, int]] | None = None
        self._list_tags_cache_ttl_seconds: float = 60.0

    async def aclose(self) -> None:
        if not self._external_client:
            await self._client.aclose()

    async def __aenter__(self) -> "PaperlessClient":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()

    async def _get(self, path: str, *, params: dict[str, Any] | None = None) -> dict:
        url = path if path.startswith("http") else path
        last_exc: Exception | None = None
        for attempt in (0, 1):
            try:
                resp = await _client_get(self._client, url, params=params)
            except httpx.HTTPError as exc:
                last_exc = exc
                if attempt == 1:
                    raise PaperlessError(f"GET {url} failed: {exc}") from exc
                continue
            if resp.status_code >= 500 and attempt == 0:
                last_exc = PaperlessError(f"GET {url} returned {resp.status_code}")
                continue
            if resp.status_code >= 400:
                raise PaperlessError(f"GET {url} returned {resp.status_code}: {resp.text}")
            return resp.json()
        raise PaperlessError(str(last_exc))  # pragma: no cover

    async def _load_field_cache(self) -> dict[int, CustomField]:
        if self._field_cache is not None:
            return self._field_cache
        cache: dict[int, CustomField] = {}
        url: str | None = "/api/custom_fields/"
        while url:
            payload = await self._get(url)
            for row in payload.get("results", []):
                field = CustomField.model_validate(row)
                cache[field.id] = field
            url = payload.get("next")
        self._field_cache = cache
        return cache

    async def get_document(self, doc_id: int) -> Document:
        payload = await self._get(f"/api/documents/{doc_id}/")
        return Document.model_validate(payload)

    async def get_document_metadata(self, doc_id: int) -> dict[str, Any]:
        """File-level metadata (checksums, sizes, mime types). Paperless-ngx
        omits these fields from the main ``/api/documents/{id}/`` response
        in many versions, so we use the dedicated ``/metadata/`` subroute.
        Returns the raw JSON — keys of interest:
          - ``original_checksum``  (MD5 of the uploaded file)
          - ``archive_checksum``   (MD5 of the OCR'd archive, if present)
          - ``original_size``, ``original_mime_type``, ``original_filename``
        Missing keys = the field wasn't populated for this document."""
        return await self._get(f"/api/documents/{doc_id}/metadata/")

    async def iter_recent_documents(self, days: int = 7) -> AsyncIterator[Document]:
        since = (date.today() - timedelta(days=days)).isoformat()
        async for doc in self.iter_documents(
            {"created__date__gte": since, "ordering": "-created"}
        ):
            yield doc

    async def iter_documents(
        self, params: dict[str, Any] | None = None
    ) -> AsyncIterator[Document]:
        """Paginate /api/documents/ with arbitrary query params. The first
        response controls the page size; subsequent pages follow `next`."""
        url: str | None = "/api/documents/"
        current_params: dict[str, Any] | None = dict(params or {})
        while url:
            payload = await self._get(url, params=current_params)
            for row in payload.get("results", []):
                yield Document.model_validate(row)
            url = payload.get("next")
            current_params = None  # `next` already carries params

    async def get_document_types(self) -> dict[int, str]:
        out: dict[int, str] = {}
        url: str | None = "/api/document_types/"
        while url:
            payload = await self._get(url)
            for row in payload.get("results", []):
                rid = row.get("id")
                name = row.get("name")
                if isinstance(rid, int) and isinstance(name, str):
                    out[rid] = name
            url = payload.get("next")
        return out

    async def download_original(self, doc_id: int) -> tuple[bytes, str]:
        """Stream the original file for a document. Returns (raw_bytes,
        content_type). Raises PaperlessError on non-2xx."""
        url = f"/api/documents/{doc_id}/download/"
        try:
            resp = await _client_get(self._client, url)
        except httpx.HTTPError as exc:
            raise PaperlessError(f"GET {url} failed: {exc}") from exc
        if resp.status_code >= 400:
            raise PaperlessError(
                f"GET {url} returned {resp.status_code}: {resp.text[:200]}"
            )
        return resp.content, resp.headers.get("content-type", "")

    async def download_thumbnail(self, doc_id: int) -> tuple[bytes, str]:
        """Fetch the thumbnail (small JPEG preview). Returns (bytes,
        content_type). Used by the Connector's proxy endpoint so the
        browser doesn't need the Paperless API token."""
        url = f"/api/documents/{doc_id}/thumb/"
        try:
            resp = await _client_get(self._client, url)
        except httpx.HTTPError as exc:
            raise PaperlessError(f"GET {url} failed: {exc}") from exc
        if resp.status_code >= 400:
            raise PaperlessError(
                f"GET {url} returned {resp.status_code}: {resp.text[:200]}"
            )
        return resp.content, resp.headers.get("content-type", "image/jpeg")

    async def download_preview(self, doc_id: int) -> tuple[bytes, str]:
        """Fetch the larger preview (typically a PDF or larger image).
        Used for inline display when a user clicks to enlarge."""
        url = f"/api/documents/{doc_id}/preview/"
        try:
            resp = await _client_get(self._client, url)
        except httpx.HTTPError as exc:
            raise PaperlessError(f"GET {url} failed: {exc}") from exc
        if resp.status_code >= 400:
            raise PaperlessError(
                f"GET {url} returned {resp.status_code}: {resp.text[:200]}"
            )
        return resp.content, resp.headers.get("content-type", "application/pdf")

    async def get_custom_fields(self, doc: Document) -> dict[str, Any]:
        if not doc.custom_fields:
            return {}
        cache = await self._load_field_cache()
        out: dict[str, Any] = {}
        for cf in doc.custom_fields:
            field = cache.get(cf.field)
            if field is None:
                continue
            out[field.name] = cf.value
        return out

    async def get_correspondents(self) -> dict[int, str]:
        """Return {correspondent_id: name} for all Paperless correspondents.

        Used as the fallback vendor when a document has no `vendor` custom
        field set."""
        out: dict[int, str] = {}
        url: str | None = "/api/correspondents/"
        while url:
            payload = await self._get(url)
            for row in payload.get("results", []):
                rid = row.get("id")
                name = row.get("name")
                if isinstance(rid, int) and isinstance(name, str):
                    out[rid] = name
            url = payload.get("next")
        return out

    # ------------------------------------------------------------------
    # Write endpoints (Paperless verify-and-writeback).
    # ------------------------------------------------------------------

    async def _patch(self, path: str, *, body: dict[str, Any]) -> dict:
        try:
            resp = await _client_patch(self._client, path, json=body)
        except httpx.HTTPError as exc:
            raise PaperlessError(f"PATCH {path} failed: {exc}") from exc
        if resp.status_code >= 400:
            raise PaperlessError(
                f"PATCH {path} returned {resp.status_code}: {resp.text[:400]}"
            )
        return resp.json() if resp.content else {}

    async def _post(self, path: str, *, body: dict[str, Any]) -> dict:
        try:
            resp = await _client_post(self._client, path, json=body)
        except httpx.HTTPError as exc:
            raise PaperlessError(f"POST {path} failed: {exc}") from exc
        if resp.status_code >= 400:
            raise PaperlessError(
                f"POST {path} returned {resp.status_code}: {resp.text[:400]}"
            )
        return resp.json() if resp.content else {}

    async def patch_document(
        self,
        doc_id: int,
        *,
        title: str | None = None,
        created: str | None = None,          # ISO date
        correspondent: int | None = None,
        custom_fields: list[dict[str, Any]] | None = None,
        tags: list[int] | None = None,
        content: str | None = None,
    ) -> dict:
        """PATCH a subset of fields on a Paperless document.

        Pass only the fields you intend to change. `tags` replaces the
        full tag list, so callers that want to *add* a tag should read
        the current tags first and pass the union.
        `custom_fields` shape is `[{"field": <id>, "value": <v>}]`;
        replacement semantics match Paperless (the whole list is
        overwritten, so callers should merge before calling).

        ``content`` overwrites the stored OCR text. Callers MUST NOT
        pass this for multi-page docs — passing only page 1's content
        would truncate pages 2+. See verify.py's gating.
        """
        body: dict[str, Any] = {}
        if title is not None:
            body["title"] = title
        if created is not None:
            body["created"] = created
        if correspondent is not None:
            body["correspondent"] = correspondent
        if custom_fields is not None:
            body["custom_fields"] = custom_fields
        if tags is not None:
            body["tags"] = tags
        if content is not None:
            body["content"] = content
        if not body:
            return {}
        return await self._patch(f"/api/documents/{doc_id}/", body=body)

    async def list_tags(self, *, force_refresh: bool = False) -> dict[str, int]:
        """Return {tag_name: tag_id} for every tag in Paperless.

        Memoized for ``self._list_tags_cache_ttl_seconds`` (60s by
        default) per ADR-0062 §6 — the workflow engine evaluates
        several rules per tick and each one wants to resolve a tag
        name to an id. Without memoization that's one full /api/tags/
        pagination per evaluation. ``ensure_tag`` invalidates the
        cache when it creates a new tag so the new id is visible
        immediately. Pass ``force_refresh=True`` to bypass the cache
        for one call.
        """
        now = time.monotonic()
        if (
            not force_refresh
            and self._list_tags_cache is not None
            and (now - self._list_tags_cache[0]) < self._list_tags_cache_ttl_seconds
        ):
            # Defensive copy: callers occasionally mutate the dict.
            return dict(self._list_tags_cache[1])
        out: dict[str, int] = {}
        # page_size=1000 collapses what would otherwise be N round
        # trips at the default DRF page size (~25-50) into one call
        # for any reasonable Paperless instance. Pagination loop is
        # kept as a safety net for instances with >1000 tags.
        url: str | None = "/api/tags/?page_size=1000"
        while url:
            payload = await self._get(url)
            for row in payload.get("results", []):
                rid = row.get("id")
                name = row.get("name")
                if isinstance(rid, int) and isinstance(name, str):
                    out[name] = rid
            url = payload.get("next")
        self._list_tags_cache = (now, dict(out))
        return out

    def _invalidate_list_tags_cache(self) -> None:
        """Drop the memoized list_tags() result. Called whenever we
        know the tag set has changed (ensure_tag created a new tag,
        a workflow added/removed a tag on a doc, etc.)."""
        self._list_tags_cache = None

    async def ensure_tag(self, name: str, *, color: str = "#0d6efd") -> int:
        """Return the id of the tag named `name`, creating it if it
        doesn't exist. Idempotent — safe to call on every writeback
        without worrying about duplicates.

        ADR-0064 backwards-compat: when ``name`` is a canonical
        ``Lamella:X`` and the canonical tag does not exist BUT a legacy
        ``Lamella_X`` tag does exist, this method returns the legacy
        tag's id without creating a duplicate. The migration module is
        the only thing that ever renames legacy tags to canonical; this
        helper just makes sure callers that have already moved to the
        canonical form continue to work against a not-yet-migrated
        Paperless instance.
        """
        tags = await self.list_tags()
        if name in tags:
            return tags[name]
        # ADR-0064 backwards-compat: when caller asked for the
        # canonical Lamella: form but only the legacy Lamella_ form
        # exists, return that id. Caller's downstream code is
        # already comfortable with both per the read shim.
        if name.startswith(LAMELLA_NAMESPACE_PREFIX_NEW):
            suffix = name[len(LAMELLA_NAMESPACE_PREFIX_NEW):]
            legacy = LAMELLA_NAMESPACE_PREFIX_LEGACY + suffix
            if legacy in tags:
                log.debug(
                    "ADR-0064: ensure_tag(%r) found legacy tag %r — "
                    "returning legacy id; migration will rename in place",
                    name, legacy,
                )
                return tags[legacy]
        created = await self._post(
            "/api/tags/", body={"name": name, "color": color},
        )
        tag_id = created.get("id")
        if not isinstance(tag_id, int):
            raise PaperlessError(f"POST /api/tags/ returned no id: {created}")
        # ADR-0062: invalidate the cache so subsequent list_tags()
        # callers see the just-created tag without waiting for the TTL.
        self._invalidate_list_tags_cache()
        return tag_id

    async def add_tag(self, doc_id: int, tag_id: int) -> None:
        """Idempotently add ``tag_id`` to a document's tag list.

        Implemented as read-`get_document` → union-merge →
        `patch_document(tags=union)`. Wraps the replacement-semantics
        gotcha on Paperless's PATCH so callers cannot accidentally
        clobber other tags on the doc. Calling twice with the same
        ``tag_id`` produces the same end state — the union is a set,
        not a list with duplicates.
        """
        doc = await self.get_document(doc_id)
        current = list(doc.tags or [])
        if tag_id in current:
            return
        merged = current + [tag_id]
        await self.patch_document(doc_id, tags=merged)

    async def remove_tag(self, doc_id: int, tag_id: int) -> None:
        """Idempotently remove ``tag_id`` from a document's tag list.

        Symmetric counterpart to :meth:`add_tag`. Calling on a doc
        that doesn't have the tag is a no-op (no PATCH issued).
        """
        doc = await self.get_document(doc_id)
        current = list(doc.tags or [])
        if tag_id not in current:
            return
        merged = [t for t in current if t != tag_id]
        await self.patch_document(doc_id, tags=merged)

    # Paperless custom-field data_type values. The matcher pairs each
    # canonical role with one of these; see CANONICAL_ROLE_DEFAULTS in
    # paperless.field_map.
    _VALID_FIELD_DATA_TYPES = frozenset({
        "string", "integer", "float", "monetary", "boolean",
        "date", "url", "documentlink", "select",
    })

    async def create_custom_field(
        self, *, name: str, data_type: str,
    ) -> CustomField:
        """Create a custom field in Paperless and return it. If a field
        with the same name already exists in Paperless, return that
        one instead (idempotent).

        After POST, we RE-FETCH the custom-fields list and verify the
        field is present. Some Paperless setups — permission
        middleware, older API versions, reverse-proxies that silently
        rewrite POST to GET — return 200/201 bodies without actually
        persisting. Without this post-POST verification, the UI would
        tell the user "✓ Created!" while Paperless has nothing.

        On verification failure, raises PaperlessError with the raw
        response body attached so the user can see what Paperless
        actually said.
        """
        if data_type not in self._VALID_FIELD_DATA_TYPES:
            raise PaperlessError(
                f"unknown Paperless data_type {data_type!r}; expected one of "
                f"{sorted(self._VALID_FIELD_DATA_TYPES)}"
            )
        # Force a fresh fetch so an older stale cache doesn't give a
        # false cache-hit on a name that was deleted from Paperless
        # since we last looked.
        self._field_cache = None
        cache = await self._load_field_cache()
        for field in cache.values():
            if field.name == name:
                log.info(
                    "paperless field %r already exists (id=%d); reusing",
                    name, field.id,
                )
                return field

        # POST to create.
        payload = await self._post(
            "/api/custom_fields/", body={"name": name, "data_type": data_type},
        )
        # Log only response shape — never the raw payload, which may
        # echo OCR text or field values from documents (PII).
        log.info(
            "paperless create_custom_field POST response: type=%s id=%s len=%s",
            type(payload).__name__,
            payload.get("id") if isinstance(payload, dict) else None,
            len(payload) if hasattr(payload, "__len__") else None,
        )
        # Attempt to parse the response; if that fails (weird shape,
        # proxy rewrite, etc.), fall through to verification.
        claimed_field: CustomField | None = None
        try:
            claimed_field = CustomField.model_validate(payload)
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "paperless POST /api/custom_fields/ returned unparseable "
                "body (type=%s, body=<scrubbed>) — falling through to "
                "list-verification: %s",
                type(payload).__name__, exc,
            )

        # Verification pass: re-fetch the field list and confirm the
        # field exists. This catches the case where Paperless returned
        # a success-shaped body but didn't actually persist the field
        # (permission middleware silently dropping writes, older
        # Paperless versions with different create semantics, etc.).
        self._field_cache = None
        fresh = await self._load_field_cache()
        # Prefer matching by the id from the POST response if we have
        # one; otherwise match by name. Both branches end up at the
        # same place when things are working.
        if claimed_field is not None and claimed_field.id in fresh:
            return fresh[claimed_field.id]
        for f in fresh.values():
            if f.name == name:
                return f

        # Verification failed. Paperless said "ok" but nothing is
        # there. Report what we saw.
        raise PaperlessError(
            f"Paperless accepted the POST but the field {name!r} is "
            f"NOT in the custom-fields list after a fresh re-fetch. "
            f"This usually means a permission middleware, proxy "
            f"rewrite, or older Paperless version is silently dropping "
            f"create requests. POST response body was: {payload!r}. "
            f"Check Paperless Settings → Custom Fields manually — if "
            f"the field really isn't there, try creating it via the "
            f"Paperless UI directly and use the 'Classify' button on "
            f"this page to map it."
        )

    async def ensure_correspondent(
        self, name: str, *, match_alg: int = 6,
    ) -> int:
        """Return the id of the correspondent named `name`,
        creating it if it doesn't exist. Idempotent.

        Used when vision verify identifies a real merchant for a
        receipt whose Paperless correspondent was mis-extracted
        (classic case: OCR picked 'MORRISON CO' from the store
        location line instead of 'Hardware Store' from the logo).
        The verify flow calls this then PATCHes the document's
        correspondent id."""
        # list_tags pattern — fetch all, find by name, create if
        # missing. Correspondents lists can be large (thousands),
        # so we paginate.
        name_clean = (name or "").strip()
        if not name_clean:
            raise PaperlessError("correspondent name is required")
        existing = await self.get_correspondents()
        # get_correspondents returns {id: name}; invert for lookup.
        by_name_lower = {
            n.lower(): i for i, n in existing.items() if n
        }
        hit = by_name_lower.get(name_clean.lower())
        if hit is not None:
            return int(hit)
        created = await self._post(
            "/api/correspondents/",
            body={"name": name_clean, "matching_algorithm": match_alg},
        )
        new_id = created.get("id")
        if not isinstance(new_id, int):
            raise PaperlessError(
                f"POST /api/correspondents/ returned no id: {created}"
            )
        return new_id

    async def ensure_lamella_writeback_fields(self) -> dict[str, int]:
        """Idempotently ensure the four ADR-0044 ``Lamella_*`` custom
        fields exist in Paperless. Returns ``{name: field_id}`` for
        all four after the call.

        Algorithm:
          1. Load the current Paperless field listing (cached, but we
             force a refresh so a stale cache doesn't cause a duplicate
             POST when Paperless was edited since we last looked).
          2. For any of the four names not present, POST to create.
          3. Re-verify via the freshly-fetched listing (matches the
             same post-POST verification ``create_custom_field`` does).

        Each POST goes through the tenacity-wrapped helpers
        (ADR-0027) for transient-error retry. A POST failure is logged
        and the partial dict is returned — callers MUST tolerate a
        missing field by skipping that piece of writeback, never by
        blocking the match itself.
        """
        # Fresh cache so deletions in Paperless aren't masked by stale state.
        self._field_cache = None
        existing = await self._load_field_cache()
        by_name: dict[str, int] = {}
        # ADR-0064 backwards-compat: a partially-migrated Paperless
        # instance may still have ``Lamella_X`` (legacy underscore)
        # fields. Surface them under the canonical ``Lamella:X`` key
        # so callers see a unified view; the migration module is
        # responsible for the actual rename.
        legacy_to_canonical = dict(zip(
            _LEGACY_WRITEBACK_FIELD_NAMES, LAMELLA_WRITEBACK_FIELD_NAMES,
        ))
        for f in existing.values():
            if f.name in LAMELLA_WRITEBACK_FIELD_NAMES:
                by_name[f.name] = f.id
            elif f.name in legacy_to_canonical:
                canonical_n = legacy_to_canonical[f.name]
                # Only adopt the legacy id if a canonical isn't
                # already present — canonical wins per ADR-0064.
                if canonical_n not in by_name:
                    by_name[canonical_n] = f.id
        missing = [
            n for n in LAMELLA_WRITEBACK_FIELD_NAMES if n not in by_name
        ]
        if not missing:
            return by_name
        for name in missing:
            try:
                field = await self.create_custom_field(
                    name=name, data_type="string",
                )
                by_name[name] = field.id
                log.info(
                    "ADR-0044: created Paperless writeback field %r (id=%d)",
                    name, field.id,
                )
            except PaperlessError as exc:
                # Per the ADR, field-creation failures must NOT block
                # matching. We log and continue; the writeback caller
                # skips fields it doesn't have an id for. The next
                # match attempt re-runs ensure_* and may succeed.
                log.warning(
                    "ADR-0044: failed to create Paperless writeback "
                    "field %r — match will continue, writeback for "
                    "this field will be retried later: %s",
                    name, exc,
                )
        return by_name

    async def writeback_lamella_fields(
        self,
        doc_id: int,
        *,
        values: dict[str, str],
        ensure_fields: bool = True,
    ) -> dict[str, Any]:
        """Write the given ``Lamella_*`` field values onto a Paperless
        document. Replaces (per-field) any existing value; preserves
        all other custom fields on the doc.

        ``values`` is ``{field_name: value}`` where every name MUST
        start with ``Lamella_`` (ADR-0044). A non-conforming name
        raises :class:`InvalidWritebackFieldError` BEFORE any HTTP
        call so a programming error never collides with a user-owned
        field on the receiving end.

        ``ensure_fields=True`` (default) calls
        :meth:`ensure_lamella_writeback_fields` first so the four
        canonical fields are guaranteed to exist. Callers that
        already ensured them (e.g. matcher startup) can pass
        ``ensure_fields=False`` to skip the listing round-trip.

        Returns the Paperless PATCH response body (or ``{}`` when
        no values had a matching field to write to).
        """
        # Fail loud + early on any non-Lamella name. The ADR is
        # explicit: writeback writes ONLY namespaced fields.
        # Both Lamella: (canonical) and Lamella_ (legacy) are accepted
        # on input per the ADR-0064 backwards-compat read shim; the
        # actual write below uses whatever the field is currently
        # named in Paperless.
        for name in values.keys():
            if not (
                name.startswith(LAMELLA_NAMESPACE_PREFIX_NEW)
                or name.startswith(LAMELLA_NAMESPACE_PREFIX_LEGACY)
            ):
                raise InvalidWritebackFieldError(
                    f"writeback field name {name!r} must start with "
                    f"{LAMELLA_NAMESPACE_PREFIX_NEW!r} (or the legacy "
                    f"{LAMELLA_NAMESPACE_PREFIX_LEGACY!r}) per ADR-0044 / "
                    f"ADR-0064; non-Lamella fields are user-owned and "
                    f"the matcher MUST NOT write to them."
                )
        if not values:
            return {}

        if ensure_fields:
            field_ids = await self.ensure_lamella_writeback_fields()
        else:
            # Pull from cache; fields the caller said exist must be there.
            # ADR-0064 backwards-compat: ``ensure_lamella_writeback_fields``
            # surfaces legacy-named fields under their canonical key,
            # so this branch needs the same treatment.
            cache = await self._load_field_cache()
            legacy_to_canonical = dict(zip(
                _LEGACY_WRITEBACK_FIELD_NAMES, LAMELLA_WRITEBACK_FIELD_NAMES,
            ))
            field_ids = {}
            for f in cache.values():
                if f.name in LAMELLA_WRITEBACK_FIELD_NAMES:
                    field_ids[f.name] = f.id
                elif f.name in legacy_to_canonical:
                    canonical_n = legacy_to_canonical[f.name]
                    if canonical_n not in field_ids:
                        field_ids[canonical_n] = f.id

        # Merge new values into the doc's existing custom_fields list
        # so we preserve everything the user (or other Lamella tiers)
        # has already written. Paperless replaces the whole list on
        # PATCH, so we MUST read-merge-write.
        doc = await self.get_document(doc_id)
        existing: dict[int, Any] = {
            cf.field: cf.value for cf in doc.custom_fields
        }
        # Build a lookup that accepts either separator on the input
        # name and resolves to the same field id (per ADR-0064
        # backwards-compat). Callers passing ``Lamella_Vendor`` get
        # the same field id as callers passing ``Lamella:Vendor``.
        def _to_canonical(n: str) -> str:
            if n.startswith(LAMELLA_NAMESPACE_PREFIX_LEGACY):
                return LAMELLA_NAMESPACE_PREFIX_NEW + n[len(LAMELLA_NAMESPACE_PREFIX_LEGACY):]
            return n
        wrote: list[str] = []
        for name, value in values.items():
            canonical_name_for_lookup = _to_canonical(name)
            fid = field_ids.get(canonical_name_for_lookup)
            if fid is None:
                # ensure_lamella_writeback_fields couldn't create it
                # (Paperless rejected the POST). Skip; ADR-0044 says
                # the match still succeeds — the writeback gets
                # retried next round.
                log.warning(
                    "ADR-0044: skipping writeback of %r on doc %d — "
                    "field id unavailable (creation failed earlier)",
                    name, doc_id,
                )
                continue
            existing[fid] = "" if value is None else str(value)
            wrote.append(name)
        if not wrote:
            return {}
        body_fields = [
            {"field": fid, "value": val} for fid, val in existing.items()
        ]
        log.info(
            "ADR-0044: writing %s to Paperless doc %d",
            ", ".join(wrote), doc_id,
        )
        return await self.patch_document(
            doc_id, custom_fields=body_fields,
        )

    async def add_note(self, doc_id: int, body: str) -> dict:
        """Append a note to a Paperless document. Paperless stores
        each note with the authoring user (derived from the API token)
        and a timestamp, which makes these durable audit breadcrumbs."""
        return await self._post(
            f"/api/documents/{doc_id}/notes/", body={"note": body},
        )

    async def list_notes(self, doc_id: int) -> list[dict[str, Any]]:
        """Return the raw notes list for a document. Used for dedup
        before adding a new note."""
        payload = await self._get(f"/api/documents/{doc_id}/notes/")
        if isinstance(payload, list):
            return payload
        return payload.get("results", []) or []

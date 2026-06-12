# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Small SQLite lookups over the paperless_doc_index cache, plus an
async helper that resolves a checksum via live Paperless fetch when
the cache is empty.

Callers that need the `lamella-paperless-hash` value at receipt-link time
but don't already have a Document object in scope (e.g. manual-link
routes) use the sync helpers — those hit SQLite only and return None
if the cache hasn't been primed.

Webhook paths (async, have a PaperlessClient in hand) use
``resolve_and_cache_checksum`` to do the two-step fallback that the
backfill pass does: try the main document endpoint first, then the
dedicated ``/metadata/`` subroute (many Paperless versions omit the
checksum from the main response), and write whatever we find back to
the cache so later manual-link calls are offline."""
from __future__ import annotations

import logging
import sqlite3
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lamella.adapters.paperless.client import PaperlessClient

log = logging.getLogger(__name__)


def cached_original_checksum(
    conn: sqlite3.Connection, paperless_id: int
) -> str | None:
    """Return the MD5 of the original file as Paperless reported it,
    or None if the doc hasn't been sync'd yet (or we haven't re-sync'd
    since migration 017 added the column). Caller formats with algorithm
    prefix for the ledger stamp (``md5:<hex>``)."""
    row = conn.execute(
        "SELECT original_checksum FROM paperless_doc_index WHERE paperless_id = ?",
        (paperless_id,),
    ).fetchone()
    if row is None:
        return None
    value = row["original_checksum"] if isinstance(row, sqlite3.Row) else row[0]
    if not value:
        return None
    return str(value)


def cached_paperless_hash(
    conn: sqlite3.Connection, paperless_id: int
) -> str | None:
    """Return the hash formatted for ledger stamping (``md5:<hex>``)."""
    raw = cached_original_checksum(conn, paperless_id)
    if not raw:
        return None
    return f"md5:{raw}"


async def resolve_and_cache_checksum(
    client: "PaperlessClient",
    conn: sqlite3.Connection,
    paperless_id: int,
    *,
    doc_original_checksum: str | None = None,
) -> str | None:
    """Return the raw MD5 (no prefix) for a Paperless document, trying
    in order: (1) the value the caller already has on hand from
    ``Document.original_checksum``, (2) the cache, (3) the live
    ``/api/documents/{id}/metadata/`` endpoint. On a successful live
    fetch, writes the value back to the cache. Returns None if
    Paperless has no checksum for this document at all."""
    if doc_original_checksum:
        _cache_set(conn, paperless_id, doc_original_checksum)
        return doc_original_checksum
    cached = cached_original_checksum(conn, paperless_id)
    if cached:
        return cached
    try:
        meta = await client.get_document_metadata(paperless_id)
    except Exception as exc:  # defensive — webhook must not 500 on this
        log.warning("paperless metadata fetch failed for doc %s: %s", paperless_id, exc)
        return None
    raw = meta.get("original_checksum") or meta.get("archive_checksum")
    if not (isinstance(raw, str) and raw.strip()):
        return None
    value = raw.strip()
    _cache_set(conn, paperless_id, value)
    return value


async def resolve_and_cache_paperless_hash(
    client: "PaperlessClient",
    conn: sqlite3.Connection,
    paperless_id: int,
    *,
    doc_original_checksum: str | None = None,
) -> str | None:
    """Same as ``resolve_and_cache_checksum`` but formatted with the
    algorithm prefix for ledger stamping (``md5:<hex>``)."""
    raw = await resolve_and_cache_checksum(
        client, conn, paperless_id, doc_original_checksum=doc_original_checksum
    )
    if not raw:
        return None
    return f"md5:{raw}"


def _cache_set(conn: sqlite3.Connection, paperless_id: int, checksum: str) -> None:
    """Upsert the checksum onto the doc_index row. No-op if the row is
    missing (sync hasn't seen the doc yet — the next sync will insert
    it fresh). Never raises."""
    try:
        conn.execute(
            "UPDATE paperless_doc_index SET original_checksum = ? "
            "WHERE paperless_id = ? AND "
            "(original_checksum IS NULL OR original_checksum = '')",
            (checksum, paperless_id),
        )
    except sqlite3.Error as exc:
        log.warning("cache write for doc %s failed: %s", paperless_id, exc)

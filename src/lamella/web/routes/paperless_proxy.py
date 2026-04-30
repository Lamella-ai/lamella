# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Proxy Paperless thumbnails and previews to the browser.

The browser can't fetch directly from Paperless (the API needs a
token). So the Connector proxies: user requests
/paperless/thumb/{id}, we call Paperless with our stored token, stream
the bytes back. Small in-memory cache keyed by doc id so repeated
requests (e.g. an index page rendering 50 thumbnails) don't hammer
Paperless.
"""
from __future__ import annotations

import logging
import time
from collections import OrderedDict

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response

from lamella.core.config import Settings
from lamella.web.deps import get_settings
from lamella.adapters.paperless.client import PaperlessClient, PaperlessError

log = logging.getLogger(__name__)

router = APIRouter()


# Simple TTL cache — bytes keyed by (doc_id, kind). Evicts oldest when
# it grows past MAX_ENTRIES. Not shared across workers; that's fine for
# the single-worker Connector.
_CACHE: OrderedDict[tuple[int, str], tuple[float, bytes, str]] = OrderedDict()
_CACHE_TTL_SECONDS = 600
_MAX_ENTRIES = 200


def _cache_get(doc_id: int, kind: str) -> tuple[bytes, str] | None:
    key = (doc_id, kind)
    entry = _CACHE.get(key)
    if entry is None:
        return None
    expires_at, data, content_type = entry
    if time.time() > expires_at:
        _CACHE.pop(key, None)
        return None
    _CACHE.move_to_end(key)
    return data, content_type


def _cache_put(doc_id: int, kind: str, data: bytes, content_type: str) -> None:
    key = (doc_id, kind)
    _CACHE[key] = (time.time() + _CACHE_TTL_SECONDS, data, content_type)
    while len(_CACHE) > _MAX_ENTRIES:
        _CACHE.popitem(last=False)


def _client(settings: Settings) -> PaperlessClient:
    if not settings.paperless_configured:
        raise HTTPException(
            status_code=503, detail="Paperless is not configured",
        )
    return PaperlessClient(
        base_url=settings.paperless_url,  # type: ignore[arg-type]
        api_token=settings.paperless_api_token.get_secret_value(),  # type: ignore[union-attr]
        extra_headers=settings.paperless_extra_headers(),
    )


@router.get("/paperless/thumb/{doc_id}")
async def thumbnail(
    doc_id: int,
    settings: Settings = Depends(get_settings),
):
    cached = _cache_get(doc_id, "thumb")
    if cached is not None:
        data, content_type = cached
        return Response(
            content=data, media_type=content_type,
            headers={"Cache-Control": "private, max-age=600"},
        )
    client = _client(settings)
    try:
        data, content_type = await client.download_thumbnail(doc_id)
    except PaperlessError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    finally:
        await client.aclose()
    _cache_put(doc_id, "thumb", data, content_type)
    return Response(
        content=data, media_type=content_type or "image/jpeg",
        headers={"Cache-Control": "private, max-age=600"},
    )


@router.get("/paperless/preview/{doc_id}")
async def preview(
    doc_id: int,
    settings: Settings = Depends(get_settings),
):
    cached = _cache_get(doc_id, "preview")
    if cached is not None:
        data, content_type = cached
        return Response(content=data, media_type=content_type)
    client = _client(settings)
    try:
        data, content_type = await client.download_preview(doc_id)
    except PaperlessError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    finally:
        await client.aclose()
    _cache_put(doc_id, "preview", data, content_type)
    return Response(content=data, media_type=content_type)

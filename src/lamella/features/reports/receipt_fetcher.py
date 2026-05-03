# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import base64
import io
import logging
from dataclasses import dataclass

log = logging.getLogger(__name__)


MAX_THUMB_PX = 1200


@dataclass(frozen=True)
class FetchedReceipt:
    """Either a base64 data URL ready for inline embed or a fallback link."""
    data_url: str | None
    fallback_link: str | None
    too_large: bool = False
    error: str | None = None


def thumbnail_and_strip_exif(raw: bytes) -> bytes:
    """Re-encode an image to JPEG with EXIF removed and the long edge
    capped at MAX_THUMB_PX. Returns the original bytes unchanged if Pillow
    can't decode them (e.g., it's a PDF — caller should embed/link as-is)."""
    from importlib import import_module

    try:
        Image = import_module("PIL.Image")
    except ImportError:
        log.warning("Pillow not available; embedding receipt without thumbnail")
        return raw
    try:
        img = Image.open(io.BytesIO(raw))
        img.load()
    except Exception:  # noqa: BLE001
        return raw  # Pillow couldn't open it (probably a PDF) — leave alone

    img = img.convert("RGB")
    img.thumbnail((MAX_THUMB_PX, MAX_THUMB_PX))
    out = io.BytesIO()
    # save without exif
    img.save(out, format="JPEG", quality=85, optimize=True)
    return out.getvalue()


def to_data_url(raw: bytes, *, mime: str = "image/jpeg") -> str:
    encoded = base64.b64encode(raw).decode("ascii")
    return f"data:{mime};base64,{encoded}"


async def fetch_for_audit(
    client,
    *,
    paperless_id: int,
    max_bytes: int,
) -> FetchedReceipt:
    """Pull the original from Paperless. If it's too large, return a
    fallback link instead of embedding. EXIF is stripped on raster images;
    PDFs are passed through unchanged (still embedded as PDF data URL).

    ``client`` is an instance of PaperlessClient (or anything with a
    compatible ``download_original`` async method)."""
    try:
        raw, content_type = await client.download_original(paperless_id)
    except Exception as exc:  # noqa: BLE001
        return FetchedReceipt(
            data_url=None,
            fallback_link=f"paperless:{paperless_id}",
            error=f"{type(exc).__name__}: {exc}",
        )
    if not raw:
        return FetchedReceipt(
            data_url=None,
            fallback_link=f"paperless:{paperless_id}",
            error="empty response",
        )
    if len(raw) > max_bytes:
        return FetchedReceipt(
            data_url=None,
            fallback_link=f"paperless:{paperless_id}",
            too_large=True,
        )
    mime = (content_type or "").split(";")[0].strip() or "application/octet-stream"
    if mime.startswith("image/"):
        thumbed = thumbnail_and_strip_exif(raw)
        return FetchedReceipt(data_url=to_data_url(thumbed, mime="image/jpeg"), fallback_link=None)
    if mime == "application/pdf":
        return FetchedReceipt(data_url=to_data_url(raw, mime="application/pdf"), fallback_link=None)
    # Unknown format — link rather than embed.
    return FetchedReceipt(
        data_url=None,
        fallback_link=f"paperless:{paperless_id}",
    )

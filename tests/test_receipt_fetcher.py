# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import io

import pytest

from lamella.features.reports.receipt_fetcher import (
    fetch_for_audit,
    thumbnail_and_strip_exif,
    to_data_url,
)


class _FakeClient:
    def __init__(self, *, content: bytes, content_type: str = "image/jpeg", raise_: Exception | None = None):
        self.content = content
        self.content_type = content_type
        self.raise_ = raise_
        self.requested_ids: list[int] = []

    async def download_original(self, doc_id: int) -> tuple[bytes, str]:
        self.requested_ids.append(doc_id)
        if self.raise_:
            raise self.raise_
        return self.content, self.content_type


def _make_jpeg_bytes() -> bytes:
    try:
        from PIL import Image
    except ImportError:
        pytest.skip("Pillow not installed")
    img = Image.new("RGB", (2000, 2000), color=(120, 140, 160))
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=85)
    return buf.getvalue()


def test_thumbnail_caps_long_edge():
    raw = _make_jpeg_bytes()
    thumbed = thumbnail_and_strip_exif(raw)
    from PIL import Image

    thumb_img = Image.open(io.BytesIO(thumbed))
    assert max(thumb_img.size) <= 1200
    assert thumb_img.format == "JPEG"


def test_to_data_url_prefix():
    url = to_data_url(b"abc", mime="image/jpeg")
    assert url.startswith("data:image/jpeg;base64,")


async def test_fetch_embeds_image():
    client = _FakeClient(content=_make_jpeg_bytes(), content_type="image/jpeg")
    result = await fetch_for_audit(client, paperless_id=42, max_bytes=10_000_000)
    assert result.data_url is not None
    assert result.data_url.startswith("data:image/jpeg;")
    assert result.fallback_link is None
    assert client.requested_ids == [42]


async def test_fetch_falls_back_for_large_files():
    big = b"x" * (10_000_001)
    client = _FakeClient(content=big, content_type="image/jpeg")
    result = await fetch_for_audit(client, paperless_id=1, max_bytes=10_000_000)
    assert result.too_large is True
    assert result.data_url is None
    assert "paperless:1" in (result.fallback_link or "")


async def test_fetch_pdf_embeds_as_pdf_data_url():
    client = _FakeClient(content=b"%PDF-1.4 fake", content_type="application/pdf")
    result = await fetch_for_audit(client, paperless_id=2, max_bytes=1_000_000)
    assert result.data_url is not None
    assert result.data_url.startswith("data:application/pdf;")


async def test_fetch_handles_error_gracefully():
    client = _FakeClient(content=b"", raise_=RuntimeError("paperless down"))
    result = await fetch_for_audit(client, paperless_id=3, max_bytes=1_000_000)
    assert result.data_url is None
    assert result.fallback_link == "paperless:3"
    assert "paperless down" in (result.error or "")

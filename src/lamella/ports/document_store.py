# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Document store port — abstracts Paperless-ngx-like document fetch + writeback.

The concrete adapter today is :mod:`lamella.adapters.paperless.client`.
"""

from __future__ import annotations

from typing import Any, AsyncIterator, Protocol, runtime_checkable


@runtime_checkable
class DocumentStorePort(Protocol):
    """Read + structured-writeback contract for a documents archive."""

    async def get_document(self, doc_id: int) -> Any: ...

    async def get_document_metadata(self, doc_id: int) -> dict[str, Any]: ...

    async def iter_recent_documents(self, days: int = 7) -> AsyncIterator[Any]: ...

    async def download_original(self, doc_id: int) -> tuple[bytes, str]: ...

    async def download_thumbnail(self, doc_id: int) -> tuple[bytes, str]: ...

    async def download_preview(self, doc_id: int) -> tuple[bytes, str]: ...

    async def patch_document(self, *args: Any, **kwargs: Any) -> Any:
        """Writeback structured fields (correspondent, custom fields, tags)."""
        ...

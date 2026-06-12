# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

# Paperless-ngx exposes custom fields as (field_id, value) pairs on the
# document. We resolve IDs to the names below when reading a doc.
KNOWN_CUSTOM_FIELDS: set[str] = {
    "receipt_total",
    "vendor",
    "payment_last_four",
    "receipt_date",
}


class CustomField(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: int
    name: str
    data_type: str | None = None


class DocumentCustomFieldValue(BaseModel):
    model_config = ConfigDict(extra="ignore")

    field: int
    value: Any


class Document(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: int
    title: str | None = None
    created: datetime | date | None = None
    modified: datetime | None = None
    correspondent: int | None = None
    document_type: int | None = None
    tags: list[int] = Field(default_factory=list)
    content: str | None = None
    custom_fields: list[DocumentCustomFieldValue] = Field(default_factory=list)
    # Paperless exposes checksums on every GET /api/documents/{id}/.
    # `original_checksum` is MD5 of the file the user uploaded; it's
    # stable across reinstalls/migrations whereas `id` is not, so we
    # stamp it on every receipt link as `lamella-paperless-hash: "md5:<hex>"`.
    original_checksum: str | None = None
    archive_checksum: str | None = None
    # Set from the `/metadata/` subroute when we have it. Used to
    # decide whether to bother with vision re-OCR (image, PDF) or
    # to skip verification (native-text formats where the content
    # is already authoritative).
    mime_type: str | None = None


def paperless_hash_from_doc(doc: Document) -> str | None:
    if doc.original_checksum:
        return f"md5:{doc.original_checksum}"
    return None


def paperless_url_for(base_url: str | None, doc_id: int) -> str | None:
    if not base_url:
        return None
    return f"{base_url.rstrip('/')}/documents/{doc_id}/"

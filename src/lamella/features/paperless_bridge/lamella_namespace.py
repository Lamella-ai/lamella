# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Lamella Paperless namespace helpers (ADR-0064).

The reserved namespace for Lamella-managed Paperless tags and custom
fields is ``Lamella:X`` (colon-separated, PascalCase suffix). The
legacy form ``Lamella_X`` (underscore-separated) is read-compatible
during the migration window described in ADR-0064 §3 — never written
by new code, but accepted by every read-side helper so a partially-
migrated Paperless instance keeps working.

This module exposes:
  * Two prefix constants (``LAMELLA_NAMESPACE_PREFIX_NEW`` and
    ``LAMELLA_NAMESPACE_PREFIX_LEGACY``) — every name comparison
    should go through one of these instead of hard-coding ``"Lamella:"``
    or ``"Lamella_"`` literals.
  * Lookup helpers (``canonical_name``, ``legacy_name``,
    ``is_lamella_name``, ``to_canonical``, ``name_variants``) for
    building tag/field names and for routing reads through both forms.
  * Canonical tag-name constants (``TAG_AWAITING_EXTRACTION`` etc.)
    plus their legacy equivalents the migration uses to find
    rename candidates.
"""
from __future__ import annotations

# ── Namespace prefixes ───────────────────────────────────────────────
LAMELLA_NAMESPACE_PREFIX_NEW: str = "Lamella:"
"""Canonical prefix per ADR-0064. All NEW writes use this form."""

LAMELLA_NAMESPACE_PREFIX_LEGACY: str = "Lamella_"
"""Legacy prefix from ADR-0044's original separator choice. Read-
compatible only — ``namespace_migration.py`` rewrites these in place
on first boot after the upgrade."""


# ── Helpers ──────────────────────────────────────────────────────────
def canonical_name(suffix: str) -> str:
    """Return the canonical ``Lamella:<suffix>`` name.

    >>> canonical_name("Vendor")
    'Lamella:Vendor'
    >>> canonical_name("AwaitingExtraction")
    'Lamella:AwaitingExtraction'
    """
    return f"{LAMELLA_NAMESPACE_PREFIX_NEW}{suffix}"


def legacy_name(suffix: str) -> str:
    """Return the legacy ``Lamella_<suffix>`` name.

    Used by the migration to find rename candidates and by the
    backwards-compat read shims to fall back when a canonical lookup
    misses. NEW code MUST NOT call this when writing.

    >>> legacy_name("Vendor")
    'Lamella_Vendor'
    """
    return f"{LAMELLA_NAMESPACE_PREFIX_LEGACY}{suffix}"


def is_lamella_name(name: str) -> bool:
    """Return True when ``name`` carries either Lamella separator.

    Used as the namespace-defense gate for writeback — a non-Lamella
    name is user-owned and the matcher MUST NOT touch it (per ADR-0044
    and ADR-0064).

    >>> is_lamella_name("Lamella:Vendor")
    True
    >>> is_lamella_name("Lamella_Vendor")
    True
    >>> is_lamella_name("vendor")
    False
    >>> is_lamella_name("")
    False
    """
    if not name:
        return False
    return (
        name.startswith(LAMELLA_NAMESPACE_PREFIX_NEW)
        or name.startswith(LAMELLA_NAMESPACE_PREFIX_LEGACY)
    )


def to_canonical(name: str) -> str:
    """Rewrite a ``Lamella_X`` name to ``Lamella:X``. Idempotent on
    already-canonical names; pass-through on non-Lamella names so
    callers can safely invoke this on any string.

    >>> to_canonical("Lamella_Vendor")
    'Lamella:Vendor'
    >>> to_canonical("Lamella:Vendor")
    'Lamella:Vendor'
    >>> to_canonical("vendor")
    'vendor'
    """
    if name.startswith(LAMELLA_NAMESPACE_PREFIX_LEGACY):
        suffix = name[len(LAMELLA_NAMESPACE_PREFIX_LEGACY):]
        return LAMELLA_NAMESPACE_PREFIX_NEW + suffix
    return name


def name_variants(suffix: str) -> tuple[str, str]:
    """Return ``(canonical, legacy)`` name pair for backwards-compat
    lookups.

    First element is what to write; second is what to also accept on
    read. Helpers that need to look up a tag or custom field by name
    iterate this tuple, trying the canonical first then the legacy.

    >>> name_variants("Vendor")
    ('Lamella:Vendor', 'Lamella_Vendor')
    """
    return canonical_name(suffix), legacy_name(suffix)


# ── ADR-0044 canonical writeback custom field names ─────────────────
#
# Order is stable so log lines and tests can iterate deterministically.
FIELD_ENTITY: str = canonical_name("Entity")
FIELD_CATEGORY: str = canonical_name("Category")
FIELD_TXN: str = canonical_name("TXN")
FIELD_ACCOUNT: str = canonical_name("Account")

ALL_WRITEBACK_FIELDS: tuple[str, ...] = (
    FIELD_ENTITY,
    FIELD_CATEGORY,
    FIELD_TXN,
    FIELD_ACCOUNT,
)
"""The four canonical Lamella-managed custom fields per ADR-0044
(separator updated by ADR-0064)."""

_WRITEBACK_FIELD_SUFFIXES: tuple[str, ...] = (
    "Entity", "Category", "TXN", "Account",
)

_LEGACY_WRITEBACK_FIELDS: tuple[str, ...] = tuple(
    legacy_name(s) for s in _WRITEBACK_FIELD_SUFFIXES
)
"""Legacy names for the migration to find and rewrite. NOT for new code."""


# ── ADR-0062 canonical workflow tag names ────────────────────────────
TAG_AWAITING_EXTRACTION: str = canonical_name("AwaitingExtraction")
TAG_EXTRACTED: str = canonical_name("Extracted")
TAG_NEEDS_REVIEW: str = canonical_name("NeedsReview")
TAG_DATE_ANOMALY: str = canonical_name("DateAnomaly")
TAG_LINKED: str = canonical_name("Linked")

ALL_WORKFLOW_TAGS: tuple[str, ...] = (
    TAG_AWAITING_EXTRACTION,
    TAG_EXTRACTED,
    TAG_NEEDS_REVIEW,
    TAG_DATE_ANOMALY,
    TAG_LINKED,
)
"""The five canonical Lamella-managed workflow state tags per
ADR-0062 (separator updated by ADR-0064)."""

_WORKFLOW_TAG_SUFFIXES: tuple[str, ...] = (
    "AwaitingExtraction", "Extracted", "NeedsReview", "DateAnomaly", "Linked",
)

_LEGACY_WORKFLOW_TAGS: tuple[str, ...] = tuple(
    legacy_name(s) for s in _WORKFLOW_TAG_SUFFIXES
)
"""Legacy names for the migration to find and rewrite. NOT for new code."""


__all__ = [
    "ALL_WORKFLOW_TAGS",
    "ALL_WRITEBACK_FIELDS",
    "FIELD_ACCOUNT",
    "FIELD_CATEGORY",
    "FIELD_ENTITY",
    "FIELD_TXN",
    "LAMELLA_NAMESPACE_PREFIX_LEGACY",
    "LAMELLA_NAMESPACE_PREFIX_NEW",
    "TAG_AWAITING_EXTRACTION",
    "TAG_DATE_ANOMALY",
    "TAG_EXTRACTED",
    "TAG_LINKED",
    "TAG_NEEDS_REVIEW",
    "_LEGACY_WORKFLOW_TAGS",
    "_LEGACY_WRITEBACK_FIELDS",
    "canonical_name",
    "is_lamella_name",
    "legacy_name",
    "name_variants",
    "to_canonical",
]

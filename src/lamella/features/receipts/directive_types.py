# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Document/receipt directive vocabulary — read-both, write-new.

Per ADR-0061 the ledger generalizes "receipt" to "document". The
v3 vocabulary (``receipt-link``, ``receipt-dismissed``, etc.) is
read indefinitely for backwards compatibility; the v4 writer emits
only the ``document-*`` form. Existing receipt-* directives in a
ledger remain valid and parseable; they are rewritten to
``document-*`` opportunistically when the surrounding file is
touched for any other reason (a new link, an unlink, a dismissal).

Each ``DIRECTIVE_TYPES_*`` tuple is ``(new, legacy)`` so callers can
pass it directly to ``entry.type in DIRECTIVE_TYPES_X`` checks.
``DIRECTIVE_*_NEW`` is the only constant a writer should reference.
"""
from __future__ import annotations


# --- forward (link / unlink) -----------------------------------------

DIRECTIVE_LINK_NEW: str = "document-link"
DIRECTIVE_LINK_LEGACY: str = "receipt-link"
DIRECTIVE_TYPES_LINK: tuple[str, ...] = (
    DIRECTIVE_LINK_NEW,
    DIRECTIVE_LINK_LEGACY,
)

DIRECTIVE_LINK_HASH_BACKFILL_NEW: str = "document-link-hash-backfill"
DIRECTIVE_LINK_HASH_BACKFILL_LEGACY: str = "receipt-link-hash-backfill"
DIRECTIVE_TYPES_LINK_HASH_BACKFILL: tuple[str, ...] = (
    DIRECTIVE_LINK_HASH_BACKFILL_NEW,
    DIRECTIVE_LINK_HASH_BACKFILL_LEGACY,
)


# --- dismissals ------------------------------------------------------

DIRECTIVE_DISMISSED_NEW: str = "document-dismissed"
DIRECTIVE_DISMISSED_LEGACY: str = "receipt-dismissed"
DIRECTIVE_TYPES_DISMISSED: tuple[str, ...] = (
    DIRECTIVE_DISMISSED_NEW,
    DIRECTIVE_DISMISSED_LEGACY,
)

DIRECTIVE_DISMISSAL_REVOKED_NEW: str = "document-dismissal-revoked"
DIRECTIVE_DISMISSAL_REVOKED_LEGACY: str = "receipt-dismissal-revoked"
DIRECTIVE_TYPES_DISMISSAL_REVOKED: tuple[str, ...] = (
    DIRECTIVE_DISMISSAL_REVOKED_NEW,
    DIRECTIVE_DISMISSAL_REVOKED_LEGACY,
)

# Combined: the dismissal subsystem reads dismissals + revokes in one
# pass and routes by entry.type.
DIRECTIVE_TYPES_ALL_DISMISSAL: tuple[str, ...] = (
    *DIRECTIVE_TYPES_DISMISSED,
    *DIRECTIVE_TYPES_DISMISSAL_REVOKED,
)


# --- link blocks (the "user explicitly unlinked this pair") --------

DIRECTIVE_LINK_BLOCKED_NEW: str = "document-link-blocked"
DIRECTIVE_LINK_BLOCKED_LEGACY: str = "receipt-link-blocked"
DIRECTIVE_TYPES_LINK_BLOCKED: tuple[str, ...] = (
    DIRECTIVE_LINK_BLOCKED_NEW,
    DIRECTIVE_LINK_BLOCKED_LEGACY,
)

DIRECTIVE_LINK_BLOCK_REVOKED_NEW: str = "document-link-block-revoked"
DIRECTIVE_LINK_BLOCK_REVOKED_LEGACY: str = "receipt-link-block-revoked"
DIRECTIVE_TYPES_LINK_BLOCK_REVOKED: tuple[str, ...] = (
    DIRECTIVE_LINK_BLOCK_REVOKED_NEW,
    DIRECTIVE_LINK_BLOCK_REVOKED_LEGACY,
)

DIRECTIVE_TYPES_ALL_LINK_BLOCK: tuple[str, ...] = (
    *DIRECTIVE_TYPES_LINK_BLOCKED,
    *DIRECTIVE_TYPES_LINK_BLOCK_REVOKED,
)


# --- summary tuples for callers that want everything ----------------

#: Every legacy directive type the v4 reader still accepts. Useful for
#: detection/migration code that wants to count or rewrite legacy
#: entries.
LEGACY_DIRECTIVE_TYPES: tuple[str, ...] = (
    DIRECTIVE_LINK_LEGACY,
    DIRECTIVE_LINK_HASH_BACKFILL_LEGACY,
    DIRECTIVE_DISMISSED_LEGACY,
    DIRECTIVE_DISMISSAL_REVOKED_LEGACY,
    DIRECTIVE_LINK_BLOCKED_LEGACY,
    DIRECTIVE_LINK_BLOCK_REVOKED_LEGACY,
)

#: Every directive type Lamella emits as of v4. Pairs 1:1 with
#: LEGACY_DIRECTIVE_TYPES.
NEW_DIRECTIVE_TYPES: tuple[str, ...] = (
    DIRECTIVE_LINK_NEW,
    DIRECTIVE_LINK_HASH_BACKFILL_NEW,
    DIRECTIVE_DISMISSED_NEW,
    DIRECTIVE_DISMISSAL_REVOKED_NEW,
    DIRECTIVE_LINK_BLOCKED_NEW,
    DIRECTIVE_LINK_BLOCK_REVOKED_NEW,
)

#: Mapping legacy → new for the v3→v4 opportunistic rewrite. The
#: regex pass that does the rewrite uses this dict directly.
LEGACY_TO_NEW: dict[str, str] = dict(zip(LEGACY_DIRECTIVE_TYPES, NEW_DIRECTIVE_TYPES))


# --- tag-workflow bindings (ADR-0065) --------------------------------
#
# User-defined tag → action bindings are persisted in connector_config.bean
# (not connector_links.bean — bindings are config, not link state). The
# vocabulary is append-only with a separate revoke form; reconstruct
# step 26 rebuilds the tag_workflow_bindings cache from these directives.
#
# Shape of lamella-tag-binding:
#   2026-05-02 custom "lamella-tag-binding" "Lamella:Process" "extract_fields"
#     lamella-enabled: TRUE
#     lamella-config-json: ""
#     lamella-created-at: "2026-05-02T14:30:00"
#
# Shape of lamella-tag-binding-revoked:
#   2026-05-02 custom "lamella-tag-binding-revoked" "Lamella:Process"
#     lamella-revoked-at: "2026-05-02T14:31:00"

DIRECTIVE_TAG_BINDING_NEW: str = "lamella-tag-binding"
DIRECTIVE_TAG_BINDING_REVOKED_NEW: str = "lamella-tag-binding-revoked"
DIRECTIVE_TYPES_TAG_BINDING: tuple[str, ...] = (DIRECTIVE_TAG_BINDING_NEW,)
DIRECTIVE_TYPES_TAG_BINDING_REVOKED: tuple[str, ...] = (DIRECTIVE_TAG_BINDING_REVOKED_NEW,)
DIRECTIVE_TYPES_ALL_TAG_BINDING: tuple[str, ...] = (
    *DIRECTIVE_TYPES_TAG_BINDING,
    *DIRECTIVE_TYPES_TAG_BINDING_REVOKED,
)

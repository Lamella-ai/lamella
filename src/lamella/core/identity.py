# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Transaction identity helpers.

Two orthogonal concerns live here:

1. **Lineage** — every txn carries a stable ``lamella-txn-id`` (UUIDv7
   string) at the transaction-meta level. Minted on first sight of an
   entry, never regenerated, survives ledger edits that would change
   the Beancount content-hash. This is what AI decisions, override
   pointers, and other internal subsystems key off.

2. **Provenance** — each posting carries 0 or more ``(source,
   reference_id)`` pairs at the posting-meta level, encoded as paired
   indexed keys ``lamella-source-N`` + ``lamella-source-reference-id-N``
   starting at 0 and dense. A bare un-indexed pair
   (``lamella-source`` / ``lamella-source-reference-id``) is also
   tolerated for hand-edits and treated as the next-free-slot.

Spec: ``docs/specs/NORMALIZE_TXN_IDENTITY.md``.
"""
from __future__ import annotations

import logging
import os
import time
import uuid
from typing import Iterator

log = logging.getLogger(__name__)

# Transaction-level lineage key.
TXN_ID_KEY = "lamella-txn-id"

# Transaction-level refund link key (ADR-0019 / refund-detect feature).
# When a txn is the refund of a previously-classified expense, the
# original's lamella-txn-id is stamped here so /txn detail can render
# bidirectional "refund of X" / "refunded by Y" links by walking the
# ledger for matching values. Read via ``get_refund_of``.
REFUND_OF_KEY = "lamella-refund-of"

# Posting-level provenance keys (indexed).
SOURCE_KEY = "lamella-source"
REF_KEY = "lamella-source-reference-id"

# Closed enum of recognized source names. Extend by adding members;
# unknown values are tolerated on read but the writer rejects them.
SOURCE_NAMES: frozenset[str] = frozenset({
    "simplefin",   # SimpleFIN bridge — external system owns the id
    "csv",         # CSV import — source-provided id or natural-key hash
    "paste",       # pasted tabular data — natural-key hash
    "manual",      # entered by hand in the editor — natural-key hash
})

# Closed enum of recognized *staging* source names — the value the
# `lamella-source` field carries on a `custom "staged-txn"` directive
# per ADR-0043b. Distinct from SOURCE_NAMES because:
#   * "reboot" is a Lamella-specific ingest *path* (not a posting
#     provenance system) — reboot-driven SimpleFIN rows promote to a
#     balanced txn with posting-source="simplefin", but their staging
#     directive carries lamella-source="reboot" to preserve the
#     ingest-path provenance.
#   * "manual" is excluded — manual entries skip the staging table
#     and go straight to a balanced txn (ADR-0043b §2).
STAGING_SOURCE_NAMES: frozenset[str] = frozenset({
    "simplefin",   # SimpleFIN poll
    "csv",         # spreadsheet import
    "paste",       # pasted tabular data
    "reboot",      # reboot-scan path (subsumes "reboot-scan" from earlier draft)
})

# Sanity cap when scanning for indexed source slots. Real postings
# never have more than a handful; this only bounds pathological input.
_MAX_SOURCE_INDEX = 100


def mint_txn_id() -> str:
    """Mint a UUIDv7 string. Time-ordered (lexicographic sort ≈
    chronological) so audit-log scans don't need a separate
    ``decided_at`` column to walk in order.

    Python stdlib's ``uuid`` module doesn't ship a v7 generator until
    3.14; this is the canonical 6-byte-timestamp + 10-byte-random
    layout from RFC 9562 §5.7.
    """
    ts_ms = int(time.time() * 1000) & ((1 << 48) - 1)
    ts_bytes = ts_ms.to_bytes(6, "big")
    rand = bytearray(os.urandom(10))
    # Version 7 in the high nibble of byte 6 (= rand[0]).
    rand[0] = (rand[0] & 0x0F) | 0x70
    # Variant '10' in the high two bits of byte 8 (= rand[2]).
    rand[2] = (rand[2] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(ts_bytes) + bytes(rand)))


def _used_source_indexes(posting_meta: dict) -> set[int]:
    used: set[int] = set()
    for k in posting_meta:
        if not isinstance(k, str):
            continue
        # Check the longer prefix first so REF_KEY's keys don't match
        # SOURCE_KEY's prefix and get mis-parsed.
        if k.startswith(f"{REF_KEY}-"):
            suffix = k[len(REF_KEY) + 1:]
        elif k.startswith(f"{SOURCE_KEY}-"):
            suffix = k[len(SOURCE_KEY) + 1:]
        else:
            continue
        if suffix.isdigit():
            used.add(int(suffix))
    return used


def stamp_source(
    posting_meta: dict,
    source: str,
    reference_id: str,
) -> int:
    """Append ``(source, reference_id)`` to ``posting_meta`` as the
    next free indexed pair. Mutates the dict in place. Returns the
    index used.

    If an identical pair already exists at any index, this is a no-op
    and returns the existing index.
    """
    if not source or not reference_id:
        raise ValueError("source and reference_id must both be non-empty")
    if source not in SOURCE_NAMES:
        log.warning("stamp_source: unknown source %r — accepted but check spelling", source)
    # Idempotency — don't double-stamp.
    for existing_src, existing_ref in iter_sources(posting_meta):
        if existing_src == source and existing_ref == reference_id:
            # Find the index it lives at.
            for i in range(_MAX_SOURCE_INDEX):
                if (posting_meta.get(f"{SOURCE_KEY}-{i}") == source
                        and posting_meta.get(f"{REF_KEY}-{i}") == reference_id):
                    return i
            # It came from the bare un-indexed pair; promote it to -0.
            return 0
    used = _used_source_indexes(posting_meta)
    next_idx = 0
    while next_idx in used:
        next_idx += 1
    posting_meta[f"{SOURCE_KEY}-{next_idx}"] = source
    posting_meta[f"{REF_KEY}-{next_idx}"] = reference_id
    return next_idx


def iter_sources(posting_meta: dict | None) -> Iterator[tuple[str, str]]:
    """Yield ``(source, reference_id)`` tuples in index order, then any
    bare un-indexed pair as a trailing entry.

    Tolerates:
      * Sparse indexes (one-slot gaps from in-progress renumbering).
        Stops after two consecutive empty slots.
      * Orphan keys (one half of a pair missing) — warned and skipped.
      * Bare un-indexed pair — yielded last; deduplicated against
        already-yielded pairs so a hand-edit that has both bare and
        ``-0`` of the same content doesn't double-count.

    Callers that want a list use ``list(iter_sources(meta))``.
    Callers that want to test membership use ``(name, ref) in
    list(iter_sources(meta))``.
    """
    if not posting_meta:
        return
    yielded: set[tuple[str, str]] = set()
    consecutive_empty = 0
    for i in range(_MAX_SOURCE_INDEX):
        src = posting_meta.get(f"{SOURCE_KEY}-{i}")
        ref = posting_meta.get(f"{REF_KEY}-{i}")
        if src is None and ref is None:
            consecutive_empty += 1
            if consecutive_empty >= 2:
                break
            continue
        consecutive_empty = 0
        if src is None or ref is None:
            log.warning(
                "orphaned source key at index %s (source=%r ref=%r)",
                i, src, ref,
            )
            continue
        pair = (str(src), str(ref))
        if pair in yielded:
            continue
        yielded.add(pair)
        yield pair
    bare_src = posting_meta.get(SOURCE_KEY)
    bare_ref = posting_meta.get(REF_KEY)
    if bare_src is None and bare_ref is None:
        return
    if bare_src is None or bare_ref is None:
        log.warning(
            "orphaned bare source key (source=%r ref=%r)", bare_src, bare_ref,
        )
        return
    bare_pair = (str(bare_src), str(bare_ref))
    if bare_pair in yielded:
        return
    yield bare_pair


def get_txn_id(entry_or_meta) -> str | None:
    """Return the transaction's ``lamella-txn-id`` if present, else None.

    Accepts either a Beancount entry (Transaction or any namedtuple with
    a ``.meta`` attribute) or a raw meta dict. Does not mint — callers
    that want lazy-mint-with-disk-write call ``get_or_mint_txn_id``
    instead (Phase 4 helper, not in Phase 3 scope).

    During the Phase 3 transition, callers that need an identifier for
    a txn fall back to ``txn_hash`` when this returns None — that's the
    legacy entry case (no lineage stamped on disk yet). After the
    Phase 4 transform stamps every entry, the fallback never fires.
    """
    if entry_or_meta is None:
        return None
    meta = getattr(entry_or_meta, "meta", entry_or_meta)
    if not isinstance(meta, dict):
        return None
    val = meta.get(TXN_ID_KEY)
    if val is None:
        return None
    s = str(val).strip()
    return s or None


def get_refund_of(entry_or_meta) -> str | None:
    """Return the refunded-original's ``lamella-txn-id`` if this entry
    is stamped as a refund, else None.

    Accepts the same shapes as :func:`get_txn_id`. Centralizes refund-
    link reads so callers don't grep for the literal string
    ``lamella-refund-of`` (per ADR-0019 — every internal subsystem keys
    off the lineage helpers, not raw meta keys).
    """
    if entry_or_meta is None:
        return None
    meta = getattr(entry_or_meta, "meta", entry_or_meta)
    if not isinstance(meta, dict):
        return None
    val = meta.get(REFUND_OF_KEY)
    if val is None:
        return None
    s = str(val).strip()
    return s or None


def find_source_reference(entry, source_name: str) -> str | None:
    """Walk every posting on ``entry`` and return the first
    ``reference_id`` stamped under ``source_name``. Returns ``None``
    if no posting carries that source.

    Read-stability across the schema migration: legacy on-disk
    entries that still carry a transaction-level ``lamella-simplefin-id``
    (or bare ``simplefin-id`` / ``lamella-import-txn-id``) get those
    keys mirrored down to the source-side posting at parse time by
    ``_legacy_meta.normalize_entries``, so this helper sees them
    transparently. Code paths that used to read
    ``txn.meta.get("lamella-simplefin-id")`` directly should switch
    to ``find_source_reference(txn, "simplefin")`` — they'll work on
    both legacy AND post-migration content.
    """
    postings = getattr(entry, "postings", None) or []
    for p in postings:
        meta = getattr(p, "meta", None)
        for src, ref in iter_sources(meta):
            if src == source_name:
                return ref
    return None


def find_all_source_references(
    entry, source_name: str,
) -> list[str]:
    """Return every ``reference_id`` stamped under ``source_name``
    across every posting on ``entry``. Used when one transaction
    can carry the same source multiple times via cross-source
    dedup (a posting matched into both a SimpleFIN ingest AND a
    later CSV import gets two ``simplefin`` refs OR two ``csv``
    refs etc.).

    Order: per-posting in posting order, then per-source-index
    within each posting (indexed pairs first, then bare).
    Duplicates across postings are NOT deduplicated — the
    distinct-postings case is real (transfer with both legs from
    SimpleFIN).
    """
    out: list[str] = []
    postings = getattr(entry, "postings", None) or []
    for p in postings:
        meta = getattr(p, "meta", None)
        for src, ref in iter_sources(meta):
            if src == source_name:
                out.append(ref)
    return out


def normalize_bare_to_indexed(posting_meta: dict) -> bool:
    """If the posting carries a bare un-indexed source pair, fold it
    into an indexed slot in place. Returns True if anything changed.

    Resolution rule from the spec:
      * If ``lamella-source-0`` is empty, the bare pair becomes ``-0``.
      * Otherwise the bare pair is appended at the next free index.
      * If both bare and ``-0`` exist with identical values, the bare
        is dropped silently (already represented).
      * If both bare and ``-0`` exist with conflicting values, the
        indexed form wins; the bare is dropped and a warning logged.
    """
    if not posting_meta:
        return False
    bare_src = posting_meta.get(SOURCE_KEY)
    bare_ref = posting_meta.get(REF_KEY)
    if bare_src is None and bare_ref is None:
        return False
    if bare_src is None or bare_ref is None:
        # Orphan — drop both halves so iter_sources doesn't warn again.
        posting_meta.pop(SOURCE_KEY, None)
        posting_meta.pop(REF_KEY, None)
        return True
    indexed_zero_src = posting_meta.get(f"{SOURCE_KEY}-0")
    indexed_zero_ref = posting_meta.get(f"{REF_KEY}-0")
    if indexed_zero_src is not None or indexed_zero_ref is not None:
        if indexed_zero_src == bare_src and indexed_zero_ref == bare_ref:
            # Bare duplicates -0 — drop bare.
            posting_meta.pop(SOURCE_KEY, None)
            posting_meta.pop(REF_KEY, None)
            return True
        # Conflict: bare and -0 disagree. Indexed wins; bare moves
        # to next free slot (so the user's hand-edit isn't lost).
        log.warning(
            "bare source pair conflicts with indexed -0; bare moved to next free index",
        )
        posting_meta.pop(SOURCE_KEY, None)
        posting_meta.pop(REF_KEY, None)
        stamp_source(posting_meta, str(bare_src), str(bare_ref))
        return True
    # -0 is empty — bare becomes -0.
    posting_meta[f"{SOURCE_KEY}-0"] = bare_src
    posting_meta[f"{REF_KEY}-0"] = bare_ref
    posting_meta.pop(SOURCE_KEY, None)
    posting_meta.pop(REF_KEY, None)
    return True

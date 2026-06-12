# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""At-load normalization for legacy metadata.

Two transitions overlap here:

1. **Rebrand (``bcg-*`` → ``lamella-*``)** — old prefix on every owned
   metadata key, tag, and Custom directive type rewrites to the new
   prefix at load time. New writes use the new prefix unconditionally.

2. **Identity normalization (txn→posting source mirror)** — the
   pre-normalization era stamped source identity at the transaction
   level (``lamella-simplefin-id`` etc.) when it actually belongs at
   the posting level (one source per leg). On load we **mirror**
   every legacy transaction-level source key down to the source-side
   posting and re-encode it as the paired indexed
   ``lamella-source-N`` / ``lamella-source-reference-id-N`` schema.
   The legacy txn-level key stays in place so existing readers keep
   working. Bare un-indexed pairs on postings (hand-edits) are
   folded to indexed canonical form.

   **Lineage ids are NOT auto-minted at parse time** — that would
   silently invalidate every "is this txn Lamella-managed?"
   heuristic in the codebase. Lineage is stamped only by writers
   (Phase 2), the on-disk transform (Phase 4 ``--apply``), or
   explicit lazy-mint helpers that write back to disk.

Spec for (2): ``docs/specs/NORMALIZE_TXN_IDENTITY.md``.

Both transitions are read-side compat; on-disk content is rewritten
by the dedicated transforms (``transform/bcg_to_lamella`` for (1)
and ``transform/normalize_txn_identity`` for (2)), which the user
runs explicitly.

Drop this module once no on-disk ledger carries ``bcg-*`` *and* the
identity-normalization transform has been applied everywhere.
"""
from __future__ import annotations

from typing import Any, Iterable

from lamella.core.identity import (
    normalize_bare_to_indexed,
    stamp_source,
)

_LEGACY = "bcg-"
_NEW = "lamella-"
_LEGACY_LEN = len(_LEGACY)

# Transaction-level legacy source keys we move down to the source-side
# posting. Order matters for collision resolution: the first matching
# key wins; later ones for the same posting append at the next free
# index. Both ``lamella-simplefin-id`` (post-rebrand) and the bare
# ``simplefin-id`` (pre-prefix-era) are accepted.
_LEGACY_SIMPLEFIN_KEYS = ("lamella-simplefin-id", "simplefin-id")

# Importer's transaction-level legacy keys. Both must be present to
# convert; ``lamella-import-id`` alone is a SQLite PK and reconstruct-
# unsafe — drop it without converting if its companion is absent.
_LEGACY_IMPORT_TXN_ID_KEY = "lamella-import-txn-id"
_LEGACY_IMPORT_ID_KEY = "lamella-import-id"


def _renamed_key(key: Any) -> Any:
    if isinstance(key, str) and key.startswith(_LEGACY):
        return _NEW + key[_LEGACY_LEN:]
    return key


def _renamed_meta(meta: Any) -> bool:
    """Mutate ``meta`` in place. Returns True if anything changed.

    Beancount entry ``meta`` is a regular dict — safe to mutate.
    On collision (both legacy and new key present) the new key wins.
    """
    if not isinstance(meta, dict):
        return False
    changed = False
    for k in list(meta.keys()):
        if isinstance(k, str) and k.startswith(_LEGACY):
            new_k = _renamed_key(k)
            if new_k in meta:
                # Both present — drop the legacy without overwriting.
                del meta[k]
            else:
                meta[new_k] = meta.pop(k)
            changed = True
    return changed


def _renamed_tags(tags: Any) -> Any | None:
    """Return a new frozenset if any tag was rewritten, else None.

    Tags are an immutable frozenset on Transaction; we replace the
    field via ``_replace`` only if the rename is non-empty.
    """
    if tags is None:
        return None
    out = set()
    changed = False
    for t in tags:
        if isinstance(t, str) and t.startswith(_LEGACY):
            out.add(_NEW + t[_LEGACY_LEN:])
            changed = True
        else:
            out.add(t)
    if not changed:
        return None
    return frozenset(out)


def _mirror_txn_source_keys_to_posting(entry: Any) -> None:
    """Mirror legacy transaction-level source keys down to the
    source-side posting as paired indexed source meta.

    This is the load-bearing piece of the post-Phase-7 read-side
    compat: writers stop emitting legacy txn-level keys, but legacy
    on-disk content (and any pre-Lamella hand-edited entries the
    user keeps in their ledger) still carries them. Mirroring at
    parse time means every reader can use ``iter_sources`` /
    ``find_source_reference`` against posting meta and see the
    same value regardless of which schema the entry was written in.

    Permanent — matches the ``bcg-*`` rebrand pattern. The legacy
    txn-level key is left in place in memory (additive); the
    ``/setup/recovery`` normalize action and the on-touch
    rewriter in ``rewrite/txn_inplace`` are what drop it from disk.

    Convention: the first posting on the entry is the source side
    (matches every writer in the codebase — see
    ``simplefin/writer.py`` and ``importer/emit.py``).
    """
    meta = getattr(entry, "meta", None)
    if not isinstance(meta, dict):
        return
    postings = getattr(entry, "postings", None)
    if not postings:
        return
    target_posting = postings[0]
    if target_posting.meta is None:
        # Beancount sometimes leaves posting.meta as None when no meta
        # was present in the source. Backfill with an empty dict so
        # stamp_source has somewhere to write.
        try:
            postings[0] = target_posting._replace(meta={})
            target_posting = postings[0]
        except (AttributeError, ValueError):
            return

    # SimpleFIN legacy keys (and the bare pre-prefix variant). The
    # post-rebrand ``lamella-simplefin-id`` takes precedence; the bare
    # pre-prefix ``simplefin-id`` is mirrored only if it doesn't
    # collide with the prefixed form.
    sf_ref = meta.get("lamella-simplefin-id") or meta.get("simplefin-id")
    if sf_ref:
        stamp_source(target_posting.meta, "simplefin", str(sf_ref))

    # Importer legacy keys. ``lamella-import-id`` is a SQLite PK on its
    # own (reconstruct-unsafe); the value worth keeping is the source-
    # provided ``lamella-import-txn-id``. Mirror the pair as a ``csv``
    # source when present.
    csv_ref = meta.get(_LEGACY_IMPORT_TXN_ID_KEY)
    if csv_ref:
        stamp_source(target_posting.meta, "csv", str(csv_ref))


def _normalize_posting_sources(postings: Any) -> None:
    """Fold any bare un-indexed source keys on each posting into the
    indexed canonical form."""
    if not postings:
        return
    for p in postings:
        meta = getattr(p, "meta", None)
        if isinstance(meta, dict):
            normalize_bare_to_indexed(meta)


def normalize_entries(entries: Iterable) -> list:
    """Walk ``entries``, applying both legacy transitions:

    * ``bcg-*`` → ``lamella-*`` rewrites for keys, tags, custom types.
    * Legacy transaction-level source keys → posting-level paired
      source meta (``lamella-source-N`` / ``lamella-source-reference-id-N``).
    * Bare un-indexed posting source keys → indexed canonical form.
    * Mint ``lamella-txn-id`` in memory when absent.

    Returns a new list — entries that needed a tag/type rewrite are
    replaced via ``_replace``; everything else passes through by
    reference. Meta dicts are mutated in place.

    Always safe to call. Entries with no legacy refs and a present
    lineage id pass through unchanged (modulo the in-memory mint
    when absent).
    """
    from beancount.core.data import Transaction
    out: list = []
    for e in entries:
        # Postings carry their own meta; do those first so the
        # Transaction we ultimately yield has clean nested state.
        postings = getattr(e, "postings", None)
        if postings:
            for p in postings:
                _renamed_meta(getattr(p, "meta", None))

        # Top-level entry meta.
        _renamed_meta(getattr(e, "meta", None))

        # Custom directive type (Custom is a namedtuple; replace via
        # ``_replace``). Only fires for the rare ``bcg-ledger-version``
        # / future ``bcg-*`` custom types.
        e_type = getattr(e, "type", None)
        if isinstance(e_type, str) and e_type.startswith(_LEGACY):
            e = e._replace(type=_NEW + e_type[_LEGACY_LEN:])

        # Transaction tags.
        new_tags = _renamed_tags(getattr(e, "tags", None))
        if new_tags is not None:
            e = e._replace(tags=new_tags)

        # Identity normalization — only meaningful for transactions.
        if isinstance(e, Transaction):
            _mirror_txn_source_keys_to_posting(e)
            _normalize_posting_sources(getattr(e, "postings", None))

        out.append(e)
    return out

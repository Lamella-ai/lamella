# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from typing import Iterable

from beancount.core.data import Transaction


import re as _re


def _meta_simplefin_id(txn: Transaction) -> str | None:
    """Read the SimpleFIN transaction id off a ledger transaction.

    Reads from posting-level paired source meta
    (``lamella-source-N: "simplefin"`` + ``lamella-source-reference-id-N``).
    Legacy on-disk shapes (``lamella-simplefin-id`` /
    ``simplefin-id`` at txn level) are mirrored down to the source-
    side posting at parse time by ``_legacy_meta.normalize_entries``,
    so this helper sees them transparently. Works on both pre- and
    post-migration content.
    """
    from lamella.core.identity import find_source_reference
    ref = find_source_reference(txn, "simplefin")
    if ref:
        return str(ref).strip() or None
    return None


def _meta_simplefin_aliases(txn: Transaction) -> list[str]:
    """Read any ``lamella-simplefin-aliases`` value as a list of ids.

    Written by the duplicate-cleanup flow when the user merges N
    copies of the same bank event into one: the kept transaction
    records every other id here so future ingests (where SimpleFIN
    may re-issue the same event with yet another fresh id) still
    match against dedup. Accepts comma- or whitespace-separated.
    """
    meta = getattr(txn, "meta", None) or {}
    raw = meta.get("lamella-simplefin-aliases")
    if raw is None:
        return []
    # Split on comma and/or whitespace so we tolerate either style.
    tokens = [t for t in _re.split(r"[,\s]+", str(raw).strip()) if t]
    return tokens


def build_index(entries: Iterable) -> set[str]:
    """Return the set of SimpleFIN transaction ids + aliases present
    on any Transaction in ``entries``. This set is the authority for
    dedup — SQLite is never consulted.

    Aliases (``lamella-simplefin-aliases``) are folded in so SimpleFIN
    re-delivering the same event with a fresh id — which it does
    routinely until the event slides out of the 30/60/90-day
    feed window — still gets caught as a duplicate.
    """
    seen: set[str] = set()
    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        sfid = _meta_simplefin_id(entry)
        if sfid:
            seen.add(sfid)
        for alias in _meta_simplefin_aliases(entry):
            seen.add(alias)
    return seen

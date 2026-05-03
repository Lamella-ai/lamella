# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Workstream C2.1 — group similar staged rows for one-confirmation apply.

The philosophy in docs/specs/AI-CLASSIFICATION.md "one-per-group, not
all-at-once" says: when the user confirms item A in /review/staged,
rows B..Z that look just like A should auto-resolve via the same
user-rule that A's confirmation creates. The grouping here is the
UI-side half of that story — surfacing which rows are siblings so
the classify-group action can act on all of them with a single
decision.

Group key: normalized (payee_or_narration_stem, source_account).
Normalization: lowercase, strip punctuation, trim to first 40 chars.
Singletons stay as groups of size 1; the UI treats them identically,
they just don't offer a group action.
"""
from __future__ import annotations

import re
import string
from dataclasses import dataclass, field

from lamella.features.import_.staging.review import StagingReviewItem


__all__ = [
    "StagingReviewGroup",
    "group_staged_rows",
]


_PUNCTUATION_RE = re.compile(rf"[{re.escape(string.punctuation)}]+")
_WHITESPACE_RE = re.compile(r"\s+")
_STEM_MAX_LEN = 40


@dataclass(frozen=True)
class StagingReviewGroup:
    """A set of staged rows sharing a normalized (payee-stem,
    source-account) key. Size-1 groups are valid — the UI renders
    them identically, just without a group-action checkbox."""

    key: tuple[str, str]
    prototype: StagingReviewItem
    items: tuple[StagingReviewItem, ...]

    @property
    def size(self) -> int:
        return len(self.items)

    @property
    def is_singleton(self) -> bool:
        return self.size == 1


def group_staged_rows(
    rows: list[StagingReviewItem],
) -> list[StagingReviewGroup]:
    """Partition rows into groups by (normalized payee/narration stem,
    source-account). Groups preserve the input order of their first
    member; rows inside a group preserve their input order.

    Upstream callers should treat the `prototype` item as the one the
    user sees most prominently — today it's the first row in the
    group (which, given upstream sorts by posting_date DESC, is the
    most-recent occurrence)."""
    buckets: dict[tuple[str, str], list[StagingReviewItem]] = {}
    key_order: list[tuple[str, str]] = []
    for row in rows:
        key = _group_key(row)
        bucket = buckets.get(key)
        if bucket is None:
            buckets[key] = [row]
            key_order.append(key)
        else:
            bucket.append(row)
    return [
        StagingReviewGroup(
            key=key,
            prototype=buckets[key][0],
            items=tuple(buckets[key]),
        )
        for key in key_order
    ]


def _group_key(row: StagingReviewItem) -> tuple[str, str]:
    stem = _normalize_stem(row.payee or row.description or "")
    account = _source_account_key(row)
    return (stem, account)


def _normalize_stem(raw: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace, trim to 40.

    Empty strings pass through as "" so an unnamed row still lands in
    a deterministic bucket (by source-account). This is intentional —
    two unnamed rows on the same card are still siblings worth
    grouping, even if the AI shouldn't classify them without more
    context."""
    if not raw:
        return ""
    lowered = raw.lower()
    no_punct = _PUNCTUATION_RE.sub(" ", lowered)
    collapsed = _WHITESPACE_RE.sub(" ", no_punct).strip()
    return collapsed[:_STEM_MAX_LEN]


def _source_account_key(row: StagingReviewItem) -> str:
    """Grouping identity for the card / bank account that saw the
    charge. SimpleFIN carries the account id in source_ref; other
    sources may carry an account_path. Fallback to the source name
    so rows without any account hint still group deterministically."""
    ref = row.source_ref or {}
    if isinstance(ref, dict):
        aid = ref.get("account_id")
        if aid:
            return f"simplefin:{aid}"
        path = ref.get("account_path")
        if isinstance(path, str) and path:
            return f"account:{path}"
    return f"source:{row.source}"

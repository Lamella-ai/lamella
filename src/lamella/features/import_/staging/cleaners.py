# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0057 §2 — composable RebootCleaner pipeline.

A ``RebootCleaner`` is a function ``(staged_envelope) ->
CleanedEnvelope | DropDecision``. Cleaners run in order; each gets
the previous cleaner's output as input. The pipeline is the
"transform" half of the reboot ETL — applies normalizations the
staging layer is responsible for (account-path migration, retired
meta-key migration, dedup, etc.) without writing to disk.

This module provides:

* :class:`CleanedEnvelope` — the wrapper around the typed envelope
  carrying ``(envelope, changes, notes)`` so the per-file diff
  surface can render "what changed and why" verbatim.
* :class:`DropDecision` — the cleaner's "drop this row" return; the
  reboot apply path flips the staged row to ``status='dismissed'``
  with the rationale.
* :func:`account_path_normalize` — the first concrete cleaner.
  Rewrites legacy category-first account paths (e.g.
  ``Expenses:Vehicles:VAcmeVan2:Fuel``) into the entity-first
  shape ADR-0007 mandates.

The cleaner is pure: takes an envelope, returns an envelope. No
ledger writes; no AI calls. The reboot apply pipeline runs every
cleaner in sequence and only writes when every cleaner has
returned a CleanedEnvelope (DropDecisions short-circuit).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


__all__ = [
    "CleanedEnvelope",
    "DropDecision",
    "RebootCleaner",
    "account_path_normalize",
    "compose",
]


@dataclass(frozen=True)
class CleanedEnvelope:
    """Output of a successful cleaner pass.

    ``envelope`` is the (possibly-mutated) typed envelope.
    ``changes`` is a list of {field, before, after, reason} dicts
    for the per-file diff UI. ``notes`` is free-text rationale
    surfaced under the diff.
    """
    envelope: dict[str, Any]
    changes: list[dict[str, Any]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class DropDecision:
    """A cleaner that wants to drop a row instead of mutating it.

    ``rationale`` is the free-text reason logged on the staged
    row when its status flips to ``'dismissed'`` during apply.
    """
    rationale: str


RebootCleaner = Callable[
    [CleanedEnvelope], CleanedEnvelope | DropDecision,
]


def compose(*cleaners: RebootCleaner) -> RebootCleaner:
    """Run every cleaner in sequence; short-circuit on
    :class:`DropDecision`. Each cleaner sees the previous one's
    ``CleanedEnvelope`` output and accumulates ``changes`` /
    ``notes``."""
    def _composed(input_: CleanedEnvelope):
        current = input_
        for cleaner in cleaners:
            result = cleaner(current)
            if isinstance(result, DropDecision):
                return result
            # Accumulate changes / notes across passes.
            current = CleanedEnvelope(
                envelope=result.envelope,
                changes=list(current.changes) + list(result.changes),
                notes=list(current.notes) + list(result.notes),
            )
        return current
    return _composed


# --- account_path_normalize ---------------------------------------


# Categories the legacy "category-first" format put in position 2
# but which the entity-first shape (ADR-0007) puts later. When we
# see one of these tokens at position 2 of an Expenses / Income /
# Assets / Liabilities path AND a known entity slug appears at
# position 3, we rewrite to the entity-first shape.
_LEGACY_CATEGORY_TOKENS = frozenset({
    "Vehicles",
    "Vehicle",
    "Property",
    "Properties",
    "Custom",
    "Fees",
    "Bank",
})

_ENTITY_FIRST_ROOTS = frozenset({
    "Assets", "Liabilities", "Income", "Expenses",
})


def _rewrite_account(
    path: str, *, known_entity_slugs: frozenset[str],
) -> str | None:
    """Return the canonical entity-first form of ``path`` when a
    rewrite applies, or None when the path is already entity-first
    (or doesn't match any rewrite rule)."""
    parts = path.split(":")
    if len(parts) < 3:
        return None
    root = parts[0]
    if root not in _ENTITY_FIRST_ROOTS:
        return None
    pos2 = parts[1]
    pos3 = parts[2]
    # If pos2 is already a known entity, the path is canonical.
    if pos2 in known_entity_slugs:
        return None
    # Legacy category at pos 2 + entity at pos 3 → swap.
    if pos2 in _LEGACY_CATEGORY_TOKENS and pos3 in known_entity_slugs:
        rewritten = [root, pos3, pos2] + parts[3:]
        return ":".join(rewritten)
    return None


def account_path_normalize(
    *,
    known_entity_slugs: frozenset[str],
) -> RebootCleaner:
    """Build a cleaner that rewrites legacy category-first account
    paths into the entity-first shape ADR-0007 mandates.

    Operates on every posting's ``account`` field in the typed
    envelope. Records each rewrite as a ``changes`` entry so the
    per-file diff UI can render "before → after" verbatim. Notes
    nothing when no rewrite applies (the cleaner is a no-op on
    already-clean envelopes).

    The set of known entity slugs is supplied at build time —
    typically the registry's current entities. A path whose pos-2
    is one of those is canonical and never rewritten.
    """
    def _cleaner(input_: CleanedEnvelope) -> CleanedEnvelope:
        envelope = dict(input_.envelope)
        postings = envelope.get("postings") or []
        new_postings: list[dict[str, Any]] = []
        changes: list[dict[str, Any]] = []
        for p in postings:
            if not isinstance(p, dict):
                new_postings.append(p)
                continue
            account = p.get("account")
            if not isinstance(account, str):
                new_postings.append(p)
                continue
            rewritten = _rewrite_account(
                account, known_entity_slugs=known_entity_slugs,
            )
            if rewritten is None:
                new_postings.append(p)
                continue
            new_p = dict(p)
            new_p["account"] = rewritten
            new_postings.append(new_p)
            changes.append({
                "field": "posting.account",
                "before": account,
                "after": rewritten,
                "reason": (
                    "ADR-0007 entity-first normalize "
                    f"(legacy category {account.split(':')[1]!r} "
                    "→ entity-first)"
                ),
            })
        if changes:
            envelope["postings"] = new_postings
            return CleanedEnvelope(
                envelope=envelope,
                changes=changes,
                notes=[
                    f"normalized {len(changes)} legacy account "
                    "path(s) to entity-first"
                ],
            )
        return input_

    return _cleaner

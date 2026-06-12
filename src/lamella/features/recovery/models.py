# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Recovery domain types: Finding, HealResult, id helper.

The shapes here are the contract Phases 4–6 inherit. See
SETUP_IMPLEMENTATION.md "Locked specs (Phase 3 onward)" for the
full rationale.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


# --- enums (str literals, deliberately not Enum classes for JSON friendliness) ---

SEVERITIES = ("blocker", "warning", "suggestion")
TARGET_KINDS = (
    "account", "entity", "vehicle", "property",
    "file", "config", "schema",
)
CONFIDENCES = ("high", "medium", "low")
CATEGORIES = (
    "legacy_path",
    "schema_drift",
    "orphan_ref",
    "missing_scaffold",
    "missing_data_file",
    "unset_config",
    "unlabeled_account",
)


def make_finding_id(category: str, target: str) -> str:
    """Stable hash used as the repair_state overlay key.

    Computed from inputs that don't change between detector runs:
    category and target. The same finding raised on two consecutive
    boots has the same id, so a Phase 6 dismissal persists across
    reboots without storing the finding body.

    Format: ``<category>:<sha1(category|target)[:12]>``. The
    leading category prefix makes the id self-describing in logs and
    tracebacks; the truncated sha1 is collision-safe at any
    realistic finding count.
    """
    if not category:
        raise ValueError("make_finding_id: category required")
    if not target:
        raise ValueError("make_finding_id: target required")
    if category not in CATEGORIES:
        # Permissive — allow new categories without a code change here,
        # but log so a typo doesn't silently produce orphan ids.
        import logging
        logging.getLogger(__name__).warning(
            "make_finding_id: unknown category %r", category,
        )
    digest = hashlib.sha1(f"{category}|{target}".encode("utf-8")).hexdigest()
    return f"{category}:{digest[:12]}"


@dataclass(frozen=True)
class Finding:
    """A single drift signal raised by one detector. Pure data —
    detectors don't write; heal actions consume Findings and write.

    Frozen + hashable so a tuple of Findings can be compared across
    runs and a Finding can serve as a dict key during overlay
    construction. ``alternatives`` is a tuple, not a list, for the
    same reason.
    """

    id: str
    category: str
    severity: str
    target_kind: str
    target: str
    summary: str
    detail: str | None
    proposed_fix: tuple[tuple[str, Any], ...]
    """Action-specific payload as a tuple of (key, value) pairs.
    Stored as a tuple instead of a dict so Finding stays hashable.
    Helpers below convert to / from dict for ergonomics."""

    alternatives: tuple[tuple[tuple[str, Any], ...], ...]
    """Other actions the user could pick. Each entry has the same
    shape as proposed_fix. Always implicitly includes "do nothing"
    by ignoring the row — Phase 3 doesn't ship persistent
    dismissal, so we don't model it explicitly."""

    confidence: str
    source: str

    requires_individual_apply: bool = False
    """Per-finding override of the route layer's category-level
    bulk-applicability map (Phase 6.1.4d).

    When True, the bulk-review page renders this row with only the
    "Apply individually →" link — no Dismiss / Edit / batch-apply
    controls — even if the row's category is otherwise bulk-
    applicable. The HTMX draft writers refuse with 400 if the user
    curl-POSTs against a True-flagged finding so a stale browser
    state can't compose drafts that the UI shouldn't have offered.

    Default False so existing detectors don't need to change. A
    detector flips this on when the per-finding heal action has
    side effects the user must confirm interactively (e.g. a
    schema_drift recompute migration where SUPPORTS_DRY_RUN=False
    means the bulk path can't preview the work). The check lives in
    the route layer, not the orchestrator — the orchestrator never
    sees True-flagged findings because the user couldn't compose
    drafts for them in the first place."""

    def __post_init__(self) -> None:
        # Cheap sanity checks — caught at construction so a typo
        # ("blockerr") doesn't propagate into a Finding the UI
        # silently mishandles.
        if self.severity not in SEVERITIES:
            raise ValueError(
                f"Finding.severity must be in {SEVERITIES}, "
                f"got {self.severity!r}"
            )
        if self.target_kind not in TARGET_KINDS:
            raise ValueError(
                f"Finding.target_kind must be in {TARGET_KINDS}, "
                f"got {self.target_kind!r}"
            )
        if self.confidence not in CONFIDENCES:
            raise ValueError(
                f"Finding.confidence must be in {CONFIDENCES}, "
                f"got {self.confidence!r}"
            )

    @property
    def proposed_fix_dict(self) -> dict[str, Any]:
        """Convenience accessor — proposed_fix as a regular dict."""
        return dict(self.proposed_fix)

    @property
    def alternatives_dicts(self) -> tuple[dict[str, Any], ...]:
        return tuple(dict(alt) for alt in self.alternatives)


def fix_payload(**kwargs: Any) -> tuple[tuple[str, Any], ...]:
    """Build a proposed_fix / alternative tuple from kwargs. Sorts
    keys so two equivalent payloads hash identically regardless of
    construction order."""
    return tuple(sorted(kwargs.items()))


@dataclass(frozen=True)
class HealResult:
    """What a heal action returns. Frozen for the same reason
    Finding is — callers may collect a tuple of HealResults from a
    Phase-6 batch and compare/hash them."""

    success: bool
    message: str
    """User-facing text. On failure, this is the rollback reason
    (bean-check error, refused guard, etc.). On success, what was
    written ("Closed Assets:Vehicles:V2008FabrikamSuv")."""

    files_touched: tuple[Path, ...]
    """Files the action modified. Used for logging + debugging,
    not for restore — restore is the snapshot envelope's job."""

    finding_id: str
    """The id of the Finding this result corresponds to. Lets
    Phase 6's batch path attribute partial failures to specific
    findings without re-deriving."""

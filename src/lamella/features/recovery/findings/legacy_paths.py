# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Legacy-path detector: non-canonical account paths.

Phase 3 of /setup/recovery (SETUP_IMPLEMENTATION.md). Catches
non-canonical chart shapes that earlier code paths created and that
the current convention (per CLAUDE.md and docs/specs/LEDGER_LAYOUT.md) no
longer produces:

  - ``Assets:Vehicles:<slug>``                (canonical:
        ``Assets:<Entity>:Vehicle:<slug>``)
  - ``Expenses:Vehicles:<slug>:<cat>``        (canonical:
        ``Expenses:<Entity>:Vehicle:<slug>:<cat>``)
  - ``Assets:Property:<slug>``                (canonical:
        ``Assets:<Entity>:Property:<slug>``)
  - ``Assets:Properties:<slug>``              (canonical:
        ``Assets:<Entity>:Property:<slug>``)
  - ``Expenses:<Entity>:Custom:*``            (catch-all bucket from
        old classifier; canonical is the entity's expense subtree
        without the Custom segment)
  - ``Expenses:Personal``                     (root-only; pre-canonical
        flat bucket)

The canonical equivalent is computed from the registered vehicles /
properties tables when available — those carry ``entity_slug``. When
no canonical equivalent can be computed (e.g. the slug isn't
registered), the heal action collapses to "close" only.

Detector contract is pure:
``(conn, entries) -> tuple[Finding, ...]``. No writes, no DB
mutations.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Iterable

from beancount.core.data import Open, Transaction

from lamella.features.recovery.models import (
    Finding,
    fix_payload,
    make_finding_id,
)


_LEGACY_VEHICLE_ROOTS = ("Assets:Vehicles:", "Expenses:Vehicles:")
_LEGACY_PROPERTY_ROOTS = ("Assets:Property:", "Assets:Properties:")
_LEGACY_FLAT_PERSONAL = ("Expenses:Personal",)
_CUSTOM_SEGMENT = ":Custom:"


def detect_legacy_paths(
    conn: sqlite3.Connection,
    entries: Iterable[Any],
) -> tuple[Finding, ...]:
    """Walk Open directives and raise a Finding for each non-canonical
    path encountered. Detector is idempotent — same input ledger
    yields the same Findings (same ids) on every call, so Phase 6's
    repair-state overlay matches across reboots.
    """
    entries_list = list(entries)
    open_paths: set[str] = set()
    for e in entries_list:
        if isinstance(e, Open):
            open_paths.add(e.account)

    posting_counts = _count_postings(entries_list, open_paths)
    vehicle_entity_by_slug = _vehicles_entity_map(conn)
    property_entity_by_slug = _properties_entity_map(conn)

    findings: list[Finding] = []
    for path in sorted(open_paths):
        finding = _classify_one(
            path,
            opened=open_paths,
            posting_count=posting_counts.get(path, 0),
            vehicle_entity_by_slug=vehicle_entity_by_slug,
            property_entity_by_slug=property_entity_by_slug,
        )
        if finding is not None:
            findings.append(finding)
    return tuple(findings)


# --- helpers --------------------------------------------------------------


def _count_postings(
    entries: list[Any], paths_of_interest: set[str],
) -> dict[str, int]:
    out: dict[str, int] = {}
    for e in entries:
        if not isinstance(e, Transaction):
            continue
        for p in e.postings:
            acct = getattr(p, "account", None)
            if isinstance(acct, str) and acct in paths_of_interest:
                out[acct] = out.get(acct, 0) + 1
    return out


def _vehicles_entity_map(conn: sqlite3.Connection) -> dict[str, str | None]:
    """Return {vehicle_slug: entity_slug_or_None} from the vehicles
    table. Used to compute canonical paths for legacy
    ``Assets:Vehicles:<slug>`` accounts."""
    try:
        rows = conn.execute(
            "SELECT slug, entity_slug FROM vehicles"
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    return {r["slug"]: r["entity_slug"] for r in rows}


def _properties_entity_map(conn: sqlite3.Connection) -> dict[str, str | None]:
    try:
        rows = conn.execute(
            "SELECT slug, entity_slug FROM properties"
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    return {r["slug"]: r["entity_slug"] for r in rows}


def _passes_destination_guards(
    canonical: str, opened: set[str],
) -> bool:
    """Validate a proposed canonical destination. Phase 3's heal
    action reuses these rules so detector and heal agree on what's
    a legitimate move target.

    Why this guard differs from
    ``routes.staging_review._ensure_target_account_open``:

    Staging-review asks "is the user about to write a posting against
    a real branch?" — and answers "the user typed a deepening of
    something that's already opened." That's the immediate-parent
    check: parent itself opened, or parent is a prefix of some
    opened path.

    Legacy-path heal asks a structurally different question: "is the
    canonical destination structurally valid even though its parent
    branch hasn't been scaffolded yet?" A registered-but-unscaffolded
    vehicle has a legitimate ``Root:Entity:Vehicle`` *ancestor*
    (because some OTHER vehicle's subtree is already open under that
    ancestor) but the specific ``Root:Entity:Vehicle:<slug>`` parent
    isn't open yet — the heal action handles scaffolding the missing
    intermediate Open directives. Walking ancestors is therefore the
    right loosening; immediate-parent-only would refuse moves the
    user definitely wants. A future cleanup pass that reunifies the
    two guards would re-introduce that bug — don't.

    Rules:
      1. Path must be syntactically valid Beancount.
      2. Path must have ≥3 segments (Root:Entity:Leaf).
      3. Some ancestor of the canonical, from its immediate parent
         up to Root:Entity (inclusive), must itself be opened OR be
         a prefix of some opened path. Stopping at depth-2 prevents
         a brand-new entity branch from sneaking in: typo'd
         ``Expenses:AcmeCoLLC2:Misc`` finds no legitimate ancestor
         at depth 2 if ``AcmeCoLLC2`` isn't anywhere in opened.
    """
    from beancount.core import account as account_lib

    if not account_lib.is_valid(canonical):
        return False
    parts = canonical.split(":")
    if len(parts) < 3:
        return False
    if canonical in opened:
        return True
    # Walk from the immediate parent up to depth 2 (Root:Entity).
    # range(len(parts) - 1, 1, -1) → parent, grandparent, …, Root:Entity.
    for i in range(len(parts) - 1, 1, -1):
        ancestor = ":".join(parts[:i])
        if ancestor in opened:
            return True
        if any(p.startswith(ancestor + ":") for p in opened):
            return True
    return False


def _vehicle_resolution_hint(
    *, slug: str, entity: str | None, posting_count: int,
) -> str:
    """Build the entity-aware hint sentence appended to vehicle
    legacy-path findings. Resolves three cases:

    - Registered: name the entity so the user knows the canonical.
    - Unregistered + 0 postings: Close is the path forward.
    - Unregistered + postings: Close would refuse (postings would
      orphan), so spell out the register-first workflow rather
      than offering an action that's structurally guaranteed to
      fail.
    """
    if entity:
        return f" Registered entity for `{slug}`: `{entity}`."
    if posting_count == 0:
        return (
            f" No registered vehicle for slug `{slug}` — Close removes the empty"
            " account."
        )
    return (
        f" No registered vehicle for slug `{slug}` and the account has"
        f" {posting_count} posting{'s' if posting_count != 1 else ''}."
        " Close would orphan the postings — refused. Register the vehicle"
        " under /setup/vehicles first (with the correct entity), then"
        " return here for Move &amp; close."
    )


def _property_resolution_hint(
    *, slug: str, entity: str | None, posting_count: int,
) -> str:
    """Property-side analog of :func:`_vehicle_resolution_hint`."""
    if entity:
        return f" Registered entity for `{slug}`: `{entity}`."
    if posting_count == 0:
        return (
            f" No registered property for slug `{slug}` — Close removes the"
            " empty account."
        )
    return (
        f" No registered property for slug `{slug}` and the account has"
        f" {posting_count} posting{'s' if posting_count != 1 else ''}."
        " Close would orphan the postings — refused. Register the property"
        " under /setup/properties first (with the correct entity), then"
        " return here for Move &amp; close."
    )


def _build_finding(
    *,
    legacy: str,
    canonical: str | None,
    posting_count: int,
    summary: str,
    detail: str,
    confidence: str,
) -> Finding:
    """Common assembly. Builds the proposed_fix + alternatives based
    on whether postings exist and whether a canonical destination
    is known."""
    actions: list[tuple[tuple[str, Any], ...]] = []

    if posting_count == 0:
        # Empty account — closing is the obvious default.
        close = fix_payload(action="close")
        if canonical:
            move = fix_payload(action="move", canonical=canonical)
            proposed = close
            alternatives = (move,)
        else:
            proposed = close
            alternatives = ()
    else:
        # Postings exist. Move-and-close beats close-only (would
        # orphan the postings); fall back to close-only if no
        # canonical is known, but flag low confidence.
        if canonical:
            move = fix_payload(action="move", canonical=canonical)
            close = fix_payload(action="close")
            proposed = move
            alternatives = (close,)
        else:
            close = fix_payload(action="close")
            proposed = close
            alternatives = ()

    return Finding(
        id=make_finding_id("legacy_path", legacy),
        category="legacy_path",
        severity="warning",
        target_kind="account",
        target=legacy,
        summary=summary,
        detail=detail,
        proposed_fix=proposed,
        alternatives=alternatives,
        confidence=confidence,
        source="detect_legacy_paths",
    )


def _classify_one(
    path: str,
    *,
    opened: set[str],
    posting_count: int,
    vehicle_entity_by_slug: dict[str, str | None],
    property_entity_by_slug: dict[str, str | None],
) -> Finding | None:
    """Match `path` against each legacy pattern. Returns one Finding
    or None if the path is canonical."""

    # --- Vehicle non-canonical (Assets:Vehicles:<slug>) ---
    if path.startswith("Assets:Vehicles:"):
        parts = path.split(":")
        if len(parts) >= 3:
            slug = parts[2]
            entity = vehicle_entity_by_slug.get(slug)
            canonical = (
                f"Assets:{entity}:Vehicle:{slug}" if entity else None
            )
            if canonical and not _passes_destination_guards(canonical, opened):
                canonical = None  # destination would be drive-by-created
            confidence = (
                "high" if canonical and posting_count == 0 else
                "medium" if canonical else "low"
            )
            return _build_finding(
                legacy=path, canonical=canonical,
                posting_count=posting_count,
                summary=f"Non-canonical vehicle path: {path}",
                detail=(
                    "Older code wrote vehicle accounts under "
                    "`Assets:Vehicles:<slug>` (no entity segment). "
                    "Canonical shape is `Assets:<Entity>:Vehicle:<slug>` "
                    "(per CLAUDE.md entity-first rule)."
                    + _vehicle_resolution_hint(
                        slug=slug, entity=entity,
                        posting_count=posting_count,
                    )
                ),
                confidence=confidence,
            )

    # --- Vehicle expense subtree (Expenses:Vehicles:<slug>:<cat>) ---
    if path.startswith("Expenses:Vehicles:"):
        parts = path.split(":")
        if len(parts) >= 4:
            slug = parts[2]
            entity = vehicle_entity_by_slug.get(slug)
            canonical = (
                f"Expenses:{entity}:Vehicle:" + ":".join(parts[2:])
                if entity else None
            )
            # NB: the canonical is Expenses:{Entity}:Vehicle:{slug}:{cat...}
            # — segment "Vehicle" singular per the CLAUDE.md note.
            if entity:
                # Re-build canonical correctly: Expenses:{entity}:Vehicle:{slug}:{cat...}
                canonical = (
                    f"Expenses:{entity}:Vehicle:" + ":".join(parts[2:])
                )
            if canonical and not _passes_destination_guards(canonical, opened):
                canonical = None
            confidence = (
                "high" if canonical and posting_count == 0 else
                "medium" if canonical else "low"
            )
            return _build_finding(
                legacy=path, canonical=canonical,
                posting_count=posting_count,
                summary=f"Non-canonical vehicle expense path: {path}",
                detail=(
                    "Older code wrote vehicle expense subtrees under "
                    "`Expenses:Vehicles:<slug>:<cat>`. Canonical shape is "
                    "`Expenses:<Entity>:Vehicle:<slug>:<cat>`."
                    + _vehicle_resolution_hint(
                        slug=slug, entity=entity,
                        posting_count=posting_count,
                    )
                ),
                confidence=confidence,
            )

    # --- Property under root (Assets:Property:<slug> / Assets:Properties:<slug>) ---
    for prefix in _LEGACY_PROPERTY_ROOTS:
        if path.startswith(prefix):
            parts = path.split(":")
            if len(parts) >= 3:
                slug = parts[2]
                entity = property_entity_by_slug.get(slug)
                canonical = (
                    f"Assets:{entity}:Property:{slug}"
                    + ("" if len(parts) == 3 else ":" + ":".join(parts[3:]))
                    if entity else None
                )
                if canonical and not _passes_destination_guards(canonical, opened):
                    canonical = None
                confidence = (
                    "high" if canonical and posting_count == 0 else
                    "medium" if canonical else "low"
                )
                return _build_finding(
                    legacy=path, canonical=canonical,
                    posting_count=posting_count,
                    summary=f"Non-canonical property path: {path}",
                    detail=(
                        "Property accounts under `Assets:Property[ies]:<slug>` "
                        "are pre-canonical. Canonical shape is "
                        "`Assets:<Entity>:Property:<slug>`."
                        + _property_resolution_hint(
                            slug=slug, entity=entity,
                            posting_count=posting_count,
                        )
                    ),
                    confidence=confidence,
                )

    # --- Custom catch-all (Expenses:<Entity>:Custom:*) ---
    if path.startswith("Expenses:") and _CUSTOM_SEGMENT in path:
        parts = path.split(":")
        # Keep parts before "Custom" + parts after. The canonical
        # equivalent depends on what the leaf actually represents,
        # so we don't auto-derive — we only flag.
        return _build_finding(
            legacy=path, canonical=None,
            posting_count=posting_count,
            summary=f"Catch-all `Custom` segment: {path}",
            detail=(
                "Old classifier wrote uncategorized expenses under "
                "`Expenses:<Entity>:Custom:<Leaf>`. The canonical shape "
                "drops `Custom` — the actual category should sit "
                "directly under the entity. We don't auto-derive the "
                "destination because it depends on what the leaf "
                "represents; close the account once its postings have "
                "been re-classified."
            ),
            confidence="low",
        )

    # --- Flat root bucket (Expenses:Personal — exactly two segments) ---
    if path in _LEGACY_FLAT_PERSONAL:
        return _build_finding(
            legacy=path, canonical=None,
            posting_count=posting_count,
            summary=f"Flat root expense bucket: {path}",
            detail=(
                "`Expenses:Personal` (no leaf) is a pre-canonical flat "
                "bucket. Canonical shape is "
                "`Expenses:Personal:<Category>`. Re-classify postings "
                "into specific categories, then close this bucket."
            ),
            confidence="low",
        )

    return None

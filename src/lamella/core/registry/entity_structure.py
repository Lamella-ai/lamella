# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Entity structural metadata — what legal/tax shape each entity is,
and the derived rules for when bookkeeping flows commingle vs
require intercompany separation.

User rule (not IRS rule — owner's choice):
  - Personal ↔ Sole-Prop  = commingle (2-leg override).
    Sole proprietorships are disregarded for tax and the owner's
    funds and the business's funds are the same pool.
  - ANY entity ↔ LLC / Partnership / S-corp / C-corp = 4-leg intercompany.
    Even a single-member LLC disregarded for tax gets the full
    DueFrom/DueTo separation — the owner WANTS the legal clarity
    regardless of how the IRS collapses it.

No owner-contribution equity entries — per the user: "the whole
point of a sole prop is you do not do any owner contributions or
withdrawals. The money is the owner's at all times."
"""
from __future__ import annotations

import sqlite3


ENTITY_TYPES: tuple[tuple[str, str], ...] = (
    ("", "(not set)"),
    ("personal", "Personal / household"),
    ("sole_proprietorship", "Sole proprietorship — Schedule C, commingles with personal"),
    ("llc", "LLC (single- or multi-member) — legal separation required"),
    ("partnership", "Partnership"),
    ("s_corp", "S-Corp"),
    ("c_corp", "C-Corp"),
    ("trust", "Trust"),
    ("estate", "Estate"),
    ("nonprofit", "Nonprofit / 501(c)(3)"),
    ("skip", "Skip — vestigial / imported, ignore entirely"),
)


# Entity types that downstream flows should ignore entirely. A `skip`
# entity satisfies the setup gate but is excluded from chart scaffold,
# commingle resolution, classify whitelists, and reports — exists so
# users importing a ledger with stale or junk entity slugs (Clearing,
# RegularTransactionForSummariesFrom, etc.) can move past the setup
# requirement without inventing a fake type.
SKIP_TYPES: frozenset[str] = frozenset({"skip"})


def is_skipped(entity_type: str | None) -> bool:
    """Return True when an entity should be ignored by classification,
    chart scaffolding, and reports."""
    return (entity_type or "").strip().lower() in SKIP_TYPES


# Entity types whose funds commingle with personal cash. When BOTH
# sides of a cross-entity charge are in this set, the owner moved
# money around their own pocket and no intercompany debt exists.
COMMINGLE_TYPES: frozenset[str] = frozenset({"personal", "sole_proprietorship"})


def entity_type_for(
    conn: sqlite3.Connection | None, entity_slug: str | None,
) -> str | None:
    """Look up an entity's entity_type. Returns None when the slug
    is unknown, the column is NULL, or the DB call fails (graceful
    default so callers don't have to try/except around every lookup).
    """
    if conn is None or not entity_slug:
        return None
    try:
        row = conn.execute(
            "SELECT entity_type FROM entities WHERE slug = ?",
            (entity_slug,),
        ).fetchone()
    except Exception:  # noqa: BLE001
        return None
    if row is None:
        return None
    value = (row["entity_type"] or "").strip() if row["entity_type"] is not None else ""
    return value or None


def commingles(card_type: str | None, target_type: str | None) -> bool:
    """True when the two entity types share a taxpayer pool — i.e.,
    Personal and Sole Prop. Unknown types default to False so we err
    on the side of recording proper intercompany separation.
    """
    if not card_type or not target_type:
        return False
    return card_type in COMMINGLE_TYPES and target_type in COMMINGLE_TYPES


def resolved_override_shape(
    conn: sqlite3.Connection | None,
    *,
    card_entity: str | None,
    target_entity: str | None,
) -> str:
    """Decide the override shape the writer should use.

    Returns:
      ``"two_leg"``  — simple FIXME → target override. Used when the
        entities are the same OR both types are in COMMINGLE_TYPES.
      ``"four_leg"`` — DueFrom/DueTo intercompany block. Used when
        the entities differ AND at least one side is NOT in
        COMMINGLE_TYPES.

    Unknown entity metadata (no slug resolvable, entity_type NULL)
    conservatively falls back to ``"two_leg"`` for same-entity and
    ``"four_leg"`` for cross-entity — you'd rather record phantom
    debt that cancels out than silently move money between legally-
    separate entities without tracking it.
    """
    if not card_entity or not target_entity:
        return "two_leg"
    if card_entity == target_entity:
        return "two_leg"
    card_type = entity_type_for(conn, card_entity)
    target_type = entity_type_for(conn, target_entity)
    if commingles(card_type, target_type):
        return "two_leg"
    return "four_leg"

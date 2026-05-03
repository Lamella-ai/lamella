# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Public loader API for tag-workflow bindings (ADR-0065).

Workers K (settings UI) and L (per-doc trigger UI) call these
functions to read current binding state and the action catalog.

The DB cache (``tag_workflow_bindings``) is the fast-read path;
the ledger directives are the source of truth rebuilt by step26.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass


@dataclass(frozen=True)
class BindingRow:
    """One active tag→action binding from the DB cache.

    ``tag_name``    — the Paperless tag that acts as the trigger.
    ``action_name`` — the registered action name (e.g. "extract_fields").
    ``enabled``     — False means the binding exists but is paused.
    ``config_json`` — JSON blob for action-specific config (empty string
                      when the action uses its defaults).
    ``created_at``  — ISO-8601 string of when the binding was created.
    """

    tag_name: str
    action_name: str
    enabled: bool
    config_json: str
    created_at: str


@dataclass(frozen=True)
class ActionMeta:
    """Metadata for an action a binding can target.

    ``name``               — the action's canonical slug (e.g. "extract_fields").
    ``display_label``      — human-readable name for the settings UI dropdown.
    ``description``        — one sentence on what the action does.
    ``default_config_json``— empty string for v1 actions that take no config.
    ``completion_tag``     — an optional Lamella state tag stamped on
                             success IN ADDITION to removing the trigger tag.
                             The scheduler always removes the trigger tag on
                             success (so the doc drops out of the selector and
                             can't re-trigger); if completion_tag is set, that
                             tag is also added. Use ``None`` for actions
                             whose only successful side effect should be
                             "drop out of the queue" — e.g. verify_date_only,
                             which is a cleanup pass that doesn't need to
                             re-stamp Lamella:Extracted on a doc that was
                             already extracted earlier.
    ``suggested_trigger_tag``— the canonical Paperless tag this action is
                             intended to be bound to. Powers two settings-UI
                             affordances: a "Use as binding" button on each
                             action card that pre-fills the binding form, and
                             a "Run now (one-shot)" button that fires the
                             action against docs currently carrying this tag
                             without persisting a binding. ``None`` means the
                             action has no obvious default pairing (e.g.
                             date_sanity_check is a schedule-driven scan).
    """

    name: str
    display_label: str
    description: str
    default_config_json: str
    completion_tag: str | None
    suggested_trigger_tag: str | None = None


# ── Action catalog (v1) ───────────────────────────────────────────────
#
# Three actions ship with v1. The catalog is the single source of
# truth for the settings UI dropdown — Worker K renders this list
# to populate the action <select>/<datalist>.
#
# completion_tag values must match the TAG_* constants in
# lamella_namespace.py / tag_workflow.py. They are the tags the
# scheduler applies on success and uses in the on_success TagOps.

def list_known_actions() -> list[ActionMeta]:
    """Return the catalog of actions a binding can target.

    For v1: extract_fields, date_sanity_check, link_to_ledger. Each
    ActionMeta has name, display_label, description, default_config_json,
    and completion_tag.

    Worker K renders these as the action dropdown in the settings UI.
    """
    from lamella.features.paperless_bridge.lamella_namespace import (
        TAG_AWAITING_EXTRACTION,
        TAG_EXTRACTED,
        TAG_DATE_ANOMALY,
        TAG_LINKED,
    )
    return [
        ActionMeta(
            name="extract_fields",
            display_label="AI Field Extraction",
            description=(
                "Run AI extraction against the document and stamp "
                "Lamella:Extracted on success or Lamella:NeedsReview "
                "when confidence is below threshold."
            ),
            default_config_json="",
            completion_tag=TAG_EXTRACTED,
            suggested_trigger_tag=TAG_AWAITING_EXTRACTION,
        ),
        ActionMeta(
            name="date_sanity_check",
            display_label="Date Sanity Check",
            description=(
                "Flag documents whose extracted date is impossibly old "
                "(before year 2000) or in the future. Stamps "
                "Lamella:DateAnomaly on flagged docs."
            ),
            default_config_json="",
            completion_tag=TAG_DATE_ANOMALY,
            # No suggested trigger — runs as a schedule-driven scan
            # against every recent doc by default; binding it to a
            # specific tag is unusual.
            suggested_trigger_tag=None,
        ),
        ActionMeta(
            name="link_to_ledger",
            display_label="Auto-Link to Ledger",
            description=(
                "Run the ADR-0063 reverse-matcher to find a candidate "
                "transaction for this document and write the link on a "
                "high-confidence match. Stamps Lamella:Linked on success."
            ),
            default_config_json="",
            completion_tag=TAG_LINKED,
            suggested_trigger_tag=TAG_EXTRACTED,
        ),
        ActionMeta(
            name="verify_date_only",
            display_label="Date-only Re-extract (cheap)",
            description=(
                "Re-extract and PATCH only the receipt_date. Tier 1 "
                "OCR-text only unless the date itself is ambiguous "
                "(then escalates to vision). ~50× cheaper than the "
                "full extract_fields cascade because vendor / total / "
                "subtotal / tax don't trigger vision escalation. Bind "
                "to Lamella:DateAnomaly so docs the date-sanity check "
                "flagged get cheap auto-correction."
            ),
            default_config_json="",
            # No completion tag — the on-success op only REMOVES the
            # trigger (Lamella:DateAnomaly) so the doc drops out of
            # the anomaly queue. Stamping Lamella:Extracted here
            # would be misleading: the doc was already extracted
            # before being flagged as a date anomaly, so re-marking
            # it would overwrite real workflow state. Lamella Fixed
            # is still applied separately by _apply_corrections when
            # an actual date diff is patched.
            completion_tag=None,
            suggested_trigger_tag=TAG_DATE_ANOMALY,
        ),
    ]


# ── DB read path ──────────────────────────────────────────────────────

def list_active_bindings(conn: sqlite3.Connection) -> list[BindingRow]:
    """Return all currently-active tag→action bindings from the DB cache.

    Workers K and L call this to render UI and look up which bindings
    exist. Only enabled=1 rows are returned here because the scheduler
    also skips disabled bindings — if the caller needs disabled bindings
    too (e.g. to show a toggle), use ``list_all_bindings``.

    Returns an empty list when no bindings have been created (fresh
    install with no user-defined workflows).
    """
    try:
        rows = conn.execute(
            """
            SELECT tag_name, action_name, enabled, config_json, created_at
            FROM tag_workflow_bindings
            WHERE enabled = 1
            ORDER BY created_at ASC
            """
        ).fetchall()
    except Exception:  # noqa: BLE001
        return []

    return [
        BindingRow(
            tag_name=_col(row, "tag_name", 0),
            action_name=_col(row, "action_name", 1),
            enabled=bool(_col(row, "enabled", 2)),
            config_json=_col(row, "config_json", 3) or "",
            created_at=_col(row, "created_at", 4) or "",
        )
        for row in rows
    ]


def list_all_bindings(conn: sqlite3.Connection) -> list[BindingRow]:
    """Return ALL bindings (enabled and disabled).

    Worker K uses this to show the full binding list including
    paused/disabled entries so the user can toggle them.
    """
    try:
        rows = conn.execute(
            """
            SELECT tag_name, action_name, enabled, config_json, created_at
            FROM tag_workflow_bindings
            ORDER BY created_at ASC
            """
        ).fetchall()
    except Exception:  # noqa: BLE001
        return []

    return [
        BindingRow(
            tag_name=_col(row, "tag_name", 0),
            action_name=_col(row, "action_name", 1),
            enabled=bool(_col(row, "enabled", 2)),
            config_json=_col(row, "config_json", 3) or "",
            created_at=_col(row, "created_at", 4) or "",
        )
        for row in rows
    ]


def _col(row, name: str, idx: int):
    """Read a column from a sqlite3.Row or plain tuple."""
    if isinstance(row, sqlite3.Row):
        return row[name]
    return row[idx]

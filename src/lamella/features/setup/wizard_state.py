# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Persistent state for the first-run onboarding wizard.

Backs the table created in ``migrations/051_setup_wizard.sql``. The
contract is single-row (single-user app), keyed on ``slug='default'``,
JSON blob payload. Closing the tab, browser crash, or clicking back
all preserve everything the user has typed because every step writes
through here before rendering.

This is **not** the state of truth for the user's data — entities,
accounts, properties, and vehicles live in their canonical tables and
ledger directives. This module just remembers (a) the current step,
(b) step-1 answers (name + intent) so step 2 can scaffold from them,
and (c) the wizard's done-ness so we don't show it twice.
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass, field
from typing import Any


WIZARD_SLUG = "default"

STEP_WELCOME = "welcome"
STEP_ENTITIES = "entities"
STEP_BANK = "bank"
STEP_ACCOUNTS = "accounts"
STEP_PROPVEHICLE = "propvehicle"
STEP_DONE = "done"

# Order matters — the progress bar + the "go to current step" redirect
# both use it. Adding a step? Append, don't insert mid-list, or you'll
# strand any user mid-wizard from the previous deploy.
STEP_ORDER = (
    STEP_WELCOME,
    STEP_ENTITIES,
    STEP_BANK,
    STEP_ACCOUNTS,
    STEP_PROPVEHICLE,
    STEP_DONE,
)


VALID_INTENTS = frozenset({
    "personal",
    "business",
    "both",
    "household",
    "everything",
    "manual",
})


@dataclass
class WizardState:
    """Wizard answers persisted across requests.

    Fields:
      step: which step the user is currently on. Steps that haven't
        been started have empty data on later fields.
      name: first-step answer — display name to greet the user. Used
        to title-case the personal entity slug as a sensible default.
      intent: first-step intent radio. Drives entity scaffolding.
      individuals_planned: individual entity slugs the wizard is
        currently configured to render in step 2 (mix of pre-existing
        and wizard-created).
      businesses_planned: same, for businesses.
      wizard_created_indiv: subset of individuals_planned that the
        wizard *created* in this session (rollback may touch these).
        Pre-existing entities are intentionally excluded; the wizard
        never deletes data it didn't create.
      wizard_created_biz: same, for businesses.
      scaffolded_paths_by_slug: per-slug list of category-tree
        account paths the wizard scaffolded. Tracked so rollback
        can issue Close directives for the Opens we wrote, instead
        of orphaning the directives in connector_accounts.bean.
      simplefin_connected: whether step 3 connected SimpleFIN.
      created_account_paths: account paths created during step 4.
      created_loan_paths: paths whose kind=loan, with loan_kind tag.
      loan_kinds: per-loan-account-path → "Mortgage|Auto|Student|...".
      created_properties: property slugs created in step 5.
      created_vehicles: vehicle slugs created in step 5.
      completed_at: ISO timestamp set when wizard finishes; presence
        marks the install as past-onboarding.
    """

    step: str = STEP_WELCOME
    name: str = ""
    intent: str = ""
    # Drafts of entities the user has typed into the wizard. NOTHING
    # in this list has been written to the entities table yet — the
    # wizard runs in DRAFT MODE and only commits at the Done step.
    # Each entry: {slug, display_name, kind, entity_type, tax_schedule}
    # where kind = "individual" or "business".
    draft_entities: list[dict] = field(default_factory=list)
    # Convenience caches over draft_entities — populated alongside it
    # by the entities POST handler so the welcome step's capacity
    # check + the rest of the wizard can read counts without
    # re-walking draft_entities. Always derive these from
    # draft_entities; never set them independently.
    individuals_planned: list[str] = field(default_factory=list)
    businesses_planned: list[str] = field(default_factory=list)
    simplefin_connected: bool = False
    # No longer used as a one-shot gate (the wizard now re-fetches
    # SimpleFIN every time the user passes through the bank step so
    # accounts added in the bridge mid-wizard show up). Kept for
    # backward-compat with older saved state JSON.
    simplefin_seeded: bool = False
    # SimpleFIN account IDs the user has explicitly removed from the
    # drafts list. Re-seeding skips these so a dismissed account
    # doesn't resurrect on every refetch.
    simplefin_dismissed_ids: list[str] = field(default_factory=list)
    # Drafts for the accounts step — same draft-mode contract: each
    # entry is the raw form input the user typed; the Done step
    # composes paths and writes them.
    draft_accounts: list[dict] = field(default_factory=list)
    # Convenience caches populated when drafts commit.
    created_account_paths: list[str] = field(default_factory=list)
    created_loan_paths: list[str] = field(default_factory=list)
    loan_kinds: dict[str, str] = field(default_factory=dict)
    created_properties: list[str] = field(default_factory=list)
    created_vehicles: list[str] = field(default_factory=list)
    # Drafts for the property/vehicle step. Each entry is the raw
    # form input. Final commit happens in the Done step.
    draft_properties: list[dict] = field(default_factory=list)
    draft_vehicles: list[dict] = field(default_factory=list)
    completed_at: str | None = None

    def to_json(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"))

    @classmethod
    def from_json(cls, raw: str | None) -> "WizardState":
        if not raw:
            return cls()
        try:
            data = json.loads(raw)
        except (TypeError, ValueError):
            return cls()
        if not isinstance(data, dict):
            return cls()
        # Field-by-field load with type guards so a partial / older blob
        # doesn't 500 the wizard.
        out = cls()
        out.step = data.get("step") or STEP_WELCOME
        out.name = (data.get("name") or "").strip()
        out.intent = (data.get("intent") or "").strip()
        out.individuals_planned = _safe_str_list(data.get("individuals_planned"))
        out.businesses_planned = _safe_str_list(data.get("businesses_planned"))
        drafts = data.get("draft_entities") or []
        if isinstance(drafts, list):
            for d in drafts:
                if not isinstance(d, dict):
                    continue
                out.draft_entities.append({
                    "slug": str(d.get("slug") or ""),
                    "display_name": str(d.get("display_name") or ""),
                    "kind": "individual" if d.get("kind") == "individual" else "business",
                    "entity_type": str(d.get("entity_type") or ""),
                    "tax_schedule": str(d.get("tax_schedule") or ""),
                })
        accts = data.get("draft_accounts") or []
        if isinstance(accts, list):
            for d in accts:
                if isinstance(d, dict):
                    out.draft_accounts.append({str(k): str(v) for k, v in d.items()})
        for fld in ("draft_properties", "draft_vehicles"):
            arr = data.get(fld) or []
            if isinstance(arr, list):
                lst = getattr(out, fld)
                for d in arr:
                    if isinstance(d, dict):
                        lst.append({str(k): str(v) for k, v in d.items()})
        out.simplefin_connected = bool(data.get("simplefin_connected") or False)
        out.simplefin_seeded = bool(data.get("simplefin_seeded") or False)
        out.simplefin_dismissed_ids = _safe_str_list(
            data.get("simplefin_dismissed_ids"),
        )
        out.created_account_paths = _safe_str_list(data.get("created_account_paths"))
        out.created_loan_paths = _safe_str_list(data.get("created_loan_paths"))
        kinds = data.get("loan_kinds") or {}
        out.loan_kinds = {
            str(k): str(v) for k, v in kinds.items() if isinstance(kinds, dict)
        } if isinstance(kinds, dict) else {}
        out.created_properties = _safe_str_list(data.get("created_properties"))
        out.created_vehicles = _safe_str_list(data.get("created_vehicles"))
        ca = data.get("completed_at")
        out.completed_at = ca if isinstance(ca, str) and ca else None
        return out


def _safe_str_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(v) for v in value if isinstance(v, (str, int, float))]


def load_state(conn: sqlite3.Connection) -> WizardState:
    """Load the wizard state row, returning a fresh default if absent."""
    try:
        row = conn.execute(
            "SELECT payload_json, completed_at FROM setup_wizard_state "
            "WHERE slug = ?",
            (WIZARD_SLUG,),
        ).fetchone()
    except sqlite3.OperationalError:
        # Migration hasn't run yet — treat as no state.
        return WizardState()
    if row is None:
        return WizardState()
    state = WizardState.from_json(row["payload_json"])
    # Trust the column over the JSON blob for completed_at — the column
    # is the source of truth a finalize() write touches first.
    if row["completed_at"] and not state.completed_at:
        state.completed_at = str(row["completed_at"])
    return state


def save_state(conn: sqlite3.Connection, state: WizardState) -> None:
    """Persist (upsert) the wizard state row."""
    payload = state.to_json()
    conn.execute(
        """
        INSERT INTO setup_wizard_state (slug, payload_json, completed_at)
        VALUES (?, ?, ?)
        ON CONFLICT (slug) DO UPDATE SET
            payload_json = excluded.payload_json,
            completed_at = excluded.completed_at,
            updated_at   = CURRENT_TIMESTAMP
        """,
        (WIZARD_SLUG, payload, state.completed_at),
    )


def is_wizard_complete(conn: sqlite3.Connection) -> bool:
    """True when the wizard has been finished. Used to gate redirects."""
    try:
        row = conn.execute(
            "SELECT completed_at FROM setup_wizard_state WHERE slug = ?",
            (WIZARD_SLUG,),
        ).fetchone()
    except sqlite3.OperationalError:
        return False
    return bool(row and row["completed_at"])


def reset_state(conn: sqlite3.Connection) -> None:
    """Clear wizard state. Used when intent changes mid-wizard.

    Only clears the wizard's own row. Entities, accounts, etc. created
    during the wizard live in their canonical tables and require
    explicit removal (the route handler does that before calling this).
    """
    conn.execute(
        "DELETE FROM setup_wizard_state WHERE slug = ?", (WIZARD_SLUG,),
    )

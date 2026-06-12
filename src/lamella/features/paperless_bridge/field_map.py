# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Editable mapping from Paperless custom-field ids/names to canonical roles.

Paperless custom fields are user-defined per instance. This module gives the
connector a stable, user-editable way to tell us which Paperless field
corresponds to "receipt total" vs "subtotal" vs "vendor" vs "last four" vs
noise. Without a mapping the matcher has no way to pick the right number out
of a doc that has five monetary custom fields (Total, Total Amount, Subtotal,
Amount, Sales tax).

Workflow:
  1. `sync_fields(conn, client)` on a schedule or on settings-page refresh:
     pulls /api/custom_fields/, inserts unseen fields with a keyword-guessed
     role, and LEAVES alone any row the user has edited (auto_assigned=0).
  2. `get_map(conn)` returns the mapping in a shape that's easy to apply when
     reading a doc's custom_fields payload.
  3. `FieldAccessor` applies the map to a single doc.
"""
from __future__ import annotations

import logging
import re
import sqlite3
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from lamella.adapters.paperless.client import PaperlessClient
from lamella.adapters.paperless.schemas import CustomField

log = logging.getLogger(__name__)


# Canonical roles a Paperless custom field can map to. 'ignore' is the default
# for fields we have no use for (note fields, OCR confidence, archive serial,
# etc.) so the mapping table is exhaustive by construction.
CANONICAL_ROLES = (
    "total",
    "subtotal",
    "tax",
    "vendor",
    "payment_last_four",
    "receipt_date",
    # Writeback-only role for Paperless fields the matcher creates and
    # writes to (Lamella_Entity / Lamella_Category / Lamella_TXN /
    # Lamella_Account per ADR-0044). Distinct from ``ignore`` so the
    # field-map UI can label them as auto-managed writeback targets
    # instead of confusingly displaying them as "ignored." No read-side
    # consumer of this role exists; the Paperless field is populated by
    # ``writeback.py`` after a receipt is linked to a classified txn.
    "lamella_writeback",
    "ignore",
)


# Role → (default field name, Paperless data_type) when creating the field
# in Paperless from the settings page.
#
# ADR-0044: ``vendor``, ``receipt_date``, and ``payment_last_four`` are no
# longer offered for creation from the Setup status panel. Paperless's
# built-in ``correspondent`` and ``created`` cover vendor / receipt_date,
# and ``payment_last_four`` is superseded by the auto-created
# ``Lamella_Account`` writeback field. Their entries are intentionally
# absent here so the Setup panel can never render a "Create in Paperless"
# button for them.
CANONICAL_ROLE_DEFAULTS: dict[str, tuple[str, str]] = {
    "total": ("Receipt Total", "monetary"),
    "subtotal": ("Receipt Subtotal", "monetary"),
    "tax": ("Sales Tax", "monetary"),
}


# Only `total` is critical. Paperless has no native monetary
# field, so without this mapping the matcher can't filter
# receipts by amount — the whole "find receipts for selected
# txns" flow breaks.
#
# ADR-0044: the Setup status table no longer prompts for
# ``vendor`` / ``receipt_date`` / ``payment_last_four``. Paperless's
# built-in ``correspondent`` and ``created`` cover the first two;
# ``payment_last_four`` is superseded by ``Lamella_Account``, which
# the writer creates automatically the first time it writes back.
# The four ``Lamella_*`` writeback fields are NOT shown in the
# Setup status table — they're auto-created by the matcher writer
# and require no user action.
SETUP_CRITICAL_ROLES: tuple[str, ...] = ("total",)
SETUP_OPTIONAL_ROLES: tuple[str, ...] = (
    "subtotal", "tax",
)


def _guess_role(name: str) -> str:
    """Keyword-based first guess for a field's role. Ordered: more specific
    first so "Sales tax" doesn't collide with "Total Amount"."""
    raw = name.strip()
    n = raw.lower()
    if not n:
        return "ignore"
    # Lamella-namespaced writeback fields take precedence over keyword
    # matches so e.g. ``Lamella:Account`` doesn't get tagged
    # ``payment_last_four`` by the "card" / "account" heuristics
    # below. ADR-0064 canonical (``Lamella:``); ADR-0044 legacy
    # (``Lamella_``); both lowercased forms accepted as a defensive
    # measure for users who rename fields by hand. The matcher writer
    # creates these on first writeback and they have no read-side role.
    from lamella.features.paperless_bridge.lamella_namespace import (
        is_lamella_name,
    )
    if is_lamella_name(raw) or n.startswith("lamella:") or n.startswith("lamella_"):
        return "lamella_writeback"
    # Tax first: "sales tax", "tax" (but not "total" or "subtotal")
    if "subtotal" in n:
        return "subtotal"
    if re.search(r"\btax\b", n) or "sales tax" in n:
        return "tax"
    # Monetary totals
    if "total" in n or re.fullmatch(r"amount", n) or "amount paid" in n or "grand total" in n:
        return "total"
    # Last four
    if "last four" in n or "last 4" in n or n.endswith(" card") or "card number" in n:
        return "payment_last_four"
    # Vendor / merchant
    if "vendor" in n or "merchant" in n or "payee" in n or "supplier" in n:
        return "vendor"
    # Receipt date
    if n in {"date", "receipt date", "txn date", "transaction date", "purchase date"}:
        return "receipt_date"
    return "ignore"


async def sync_fields(conn: sqlite3.Connection, client: PaperlessClient) -> dict[str, int]:
    """Fetch Paperless's custom_fields list and reconcile
    paperless_field_map against it.

    * New Paperless fields get a keyword-guessed role with
      auto_assigned=1.
    * Existing rows are updated to track name changes. Roles are
      preserved when auto_assigned=0 (user picked explicitly).
    * Rows whose paperless_field_id no longer appears in the
      Paperless response are REMOVED — the field was deleted in
      Paperless, so the local mapping is dead weight. This keeps
      the setup-status panel honest: if the user deletes "Total"
      in Paperless, the ``total`` role should report back to
      unmapped so the Create button reappears. Before this fix
      stale rows lingered forever and the UI claimed the role was
      mapped when the underlying field no longer existed.
    """
    cache = await client._load_field_cache()  # uses client's internal cache
    stats = {"added": 0, "updated": 0, "unchanged": 0, "removed": 0}
    for field in cache.values():
        role = _guess_role(field.name)
        row = conn.execute(
            "SELECT canonical_role, auto_assigned, paperless_field_name "
            "FROM paperless_field_map WHERE paperless_field_id = ?",
            (field.id,),
        ).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO paperless_field_map "
                "  (paperless_field_id, paperless_field_name, canonical_role, auto_assigned) "
                "VALUES (?, ?, ?, 1)",
                (field.id, field.name, role),
            )
            stats["added"] += 1
            continue
        # Existing row: update the name if Paperless renamed the field.
        # Respect user-set role (auto_assigned=0); otherwise refresh from keyword.
        if int(row["auto_assigned"]) == 1:
            conn.execute(
                "UPDATE paperless_field_map SET paperless_field_name = ?, canonical_role = ?, "
                "  updated_at = CURRENT_TIMESTAMP "
                "WHERE paperless_field_id = ?",
                (field.name, role, field.id),
            )
            stats["updated"] += 1
        elif row["paperless_field_name"] != field.name:
            conn.execute(
                "UPDATE paperless_field_map SET paperless_field_name = ?, "
                "  updated_at = CURRENT_TIMESTAMP "
                "WHERE paperless_field_id = ?",
                (field.name, field.id),
            )
            stats["updated"] += 1
        else:
            stats["unchanged"] += 1

    # Reconcile deletions: anything in our local map that isn't in
    # the Paperless response has been deleted upstream.
    live_ids = set(cache.keys())
    local_ids = {
        int(r["paperless_field_id"])
        for r in conn.execute(
            "SELECT paperless_field_id FROM paperless_field_map"
        ).fetchall()
    }
    stale_ids = local_ids - live_ids
    for stale_id in stale_ids:
        conn.execute(
            "DELETE FROM paperless_field_map WHERE paperless_field_id = ?",
            (stale_id,),
        )
        stats["removed"] += 1
    if stale_ids:
        log.info(
            "paperless field map: removed %d stale row(s) (ids=%s) — "
            "fields were deleted in Paperless",
            len(stale_ids), sorted(stale_ids),
        )
    return stats


def set_role(conn: sqlite3.Connection, field_id: int, role: str) -> None:
    """User-driven override from the settings page. Flips auto_assigned=0
    so subsequent sync_fields() calls leave this row alone."""
    if role not in CANONICAL_ROLES:
        raise ValueError(f"unknown canonical role: {role!r}")
    conn.execute(
        "UPDATE paperless_field_map SET canonical_role = ?, auto_assigned = 0, "
        "  updated_at = CURRENT_TIMESTAMP "
        "WHERE paperless_field_id = ?",
        (role, field_id),
    )


@dataclass(frozen=True)
class FieldMapping:
    """Runtime-friendly view of paperless_field_map."""

    by_id: dict[int, str]            # field_id -> role
    rows: list[dict[str, Any]]       # full rows for settings UI rendering

    def id_for_role(self, role: str) -> list[int]:
        return [fid for fid, r in self.by_id.items() if r == role]


def get_map(conn: sqlite3.Connection) -> FieldMapping:
    rows = conn.execute(
        "SELECT paperless_field_id, paperless_field_name, canonical_role, "
        "       auto_assigned, updated_at "
        "FROM paperless_field_map "
        "ORDER BY canonical_role, paperless_field_name"
    ).fetchall()
    by_id: dict[int, str] = {}
    out_rows: list[dict[str, Any]] = []
    for row in rows:
        by_id[int(row["paperless_field_id"])] = row["canonical_role"]
        out_rows.append(dict(row))
    return FieldMapping(by_id=by_id, rows=out_rows)


def suggest_for_role(
    conn: sqlite3.Connection, role: str,
) -> list[dict[str, Any]]:
    """Return existing paperless_field_map rows that are currently
    role='ignore' but whose name would keyword-guess into `role`. Used
    by the setup panel to offer "classify this existing field" before
    falling back to "create a new field in Paperless."

    Returned rows are plain dicts with keys: paperless_field_id,
    paperless_field_name, canonical_role.
    """
    if role not in CANONICAL_ROLES or role == "ignore":
        return []
    rows = conn.execute(
        "SELECT paperless_field_id, paperless_field_name, canonical_role "
        "FROM paperless_field_map WHERE canonical_role = 'ignore' "
        "ORDER BY paperless_field_name"
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        if _guess_role(row["paperless_field_name"] or "") == role:
            out.append(dict(row))
    return out


def insert_created_field(
    conn: sqlite3.Connection,
    *,
    field_id: int,
    field_name: str,
    canonical_role: str,
) -> None:
    """Record a just-created Paperless field in paperless_field_map with
    auto_assigned=0 — user explicitly asked for this mapping, so later
    sync_fields() calls must leave the role alone. UPSERT in case a
    prior keyword-guess row already exists for the same id (can happen
    if the user created the field in Paperless manually, then the sync
    picked it up, then they click Create anyway — idempotent recovery)."""
    if canonical_role not in CANONICAL_ROLES:
        raise ValueError(f"unknown canonical role: {canonical_role!r}")
    conn.execute(
        "INSERT INTO paperless_field_map "
        "  (paperless_field_id, paperless_field_name, canonical_role, auto_assigned) "
        "VALUES (?, ?, ?, 0) "
        "ON CONFLICT(paperless_field_id) DO UPDATE SET "
        "  paperless_field_name = excluded.paperless_field_name, "
        "  canonical_role       = excluded.canonical_role, "
        "  auto_assigned        = 0, "
        "  updated_at           = CURRENT_TIMESTAMP",
        (field_id, field_name, canonical_role),
    )


def _as_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


class FieldAccessor:
    """Apply a FieldMapping to a single Paperless doc's custom_fields list.

    Because multiple fields can map to the same role ("Total", "Total Amount",
    "Amount" all mapped to 'total'), the accessor returns a priority-ordered
    list per role and a convenience primary value (first populated).
    """

    def __init__(self, custom_fields: list[dict[str, Any]], mapping: FieldMapping):
        self._by_role: dict[str, list[Any]] = {r: [] for r in CANONICAL_ROLES}
        for cf in custom_fields:
            fid = cf.get("field") if isinstance(cf, dict) else cf.field  # type: ignore[union-attr]
            value = cf.get("value") if isinstance(cf, dict) else cf.value  # type: ignore[union-attr]
            role = mapping.by_id.get(int(fid), "ignore") if fid is not None else "ignore"
            if value in (None, ""):
                continue
            self._by_role[role].append(value)

    @property
    def total(self) -> Decimal | None:
        # Prefer the largest-named total if multiple populate; fall back through
        # subtotal only when no total is present (subtotal excludes tax, so a
        # matcher using it will miss by ~tax amount).
        for value in self._by_role["total"]:
            d = _as_decimal(value)
            if d is not None:
                return d
        return None

    @property
    def subtotal(self) -> Decimal | None:
        for value in self._by_role["subtotal"]:
            d = _as_decimal(value)
            if d is not None:
                return d
        return None

    @property
    def tax(self) -> Decimal | None:
        for value in self._by_role["tax"]:
            d = _as_decimal(value)
            if d is not None:
                return d
        return None

    @property
    def vendor(self) -> str | None:
        for value in self._by_role["vendor"]:
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    @property
    def payment_last_four(self) -> str | None:
        for value in self._by_role["payment_last_four"]:
            if value is None:
                continue
            s = str(value).strip()
            # Keep only digits; Paperless fields may include spaces or hyphens.
            digits = re.sub(r"\D", "", s)
            if 3 <= len(digits) <= 4:
                return digits[-4:]
        return None

    @property
    def receipt_date(self):
        from datetime import date
        for value in self._by_role["receipt_date"]:
            if isinstance(value, str):
                try:
                    return date.fromisoformat(value[:10])
                except ValueError:
                    continue
            if isinstance(value, date):
                return value
        return None

    def amount_candidates(self) -> list[Decimal]:
        """All populated monetary roles (total, subtotal) as Decimals, with
        the most reliable first. The matcher tries each in order."""
        out: list[Decimal] = []
        for role in ("total", "subtotal"):
            for value in self._by_role[role]:
                d = _as_decimal(value)
                if d is not None and d not in out:
                    out.append(d)
        return out

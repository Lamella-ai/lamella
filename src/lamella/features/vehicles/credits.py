# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 6 — vehicle tax credits / incentives.

Free-form capture. Deliberately no knowledge base of eligibility —
the user (or their CPA) decides whether the credit applies; we store
the label, amount, and status they entered so the detail page surfaces
what's tracked.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass


@dataclass(frozen=True)
class Credit:
    id: int
    vehicle_slug: str
    tax_year: int
    credit_label: str
    amount: str | None
    status: str | None
    notes: str | None


def add_credit(
    conn: sqlite3.Connection,
    *,
    vehicle_slug: str,
    tax_year: int,
    credit_label: str,
    amount: str | None = None,
    status: str | None = None,
    notes: str | None = None,
    connector_config_path=None,
    main_bean_path=None,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO vehicle_credits
            (vehicle_slug, tax_year, credit_label, amount, status, notes)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (vehicle_slug, int(tax_year), credit_label, amount, status, notes),
    )
    if connector_config_path is not None and main_bean_path is not None:
        try:
            from lamella.features.vehicles.writer import append_vehicle_credit
            append_vehicle_credit(
                connector_config=connector_config_path,
                main_bean=main_bean_path,
                slug=vehicle_slug, tax_year=int(tax_year),
                credit_label=credit_label,
                amount=amount, status=status, notes=notes,
            )
        except Exception as exc:  # noqa: BLE001
            import logging as _l
            _l.getLogger(__name__).warning(
                "vehicle-credit directive write failed for %s %s: %s",
                vehicle_slug, tax_year, exc,
            )
    return int(cur.lastrowid)


def delete_credit(conn: sqlite3.Connection, credit_id: int) -> bool:
    cur = conn.execute(
        "DELETE FROM vehicle_credits WHERE id = ?", (int(credit_id),),
    )
    return bool(cur.rowcount and cur.rowcount > 0)


def list_credits(
    conn: sqlite3.Connection, vehicle_slug: str,
) -> list[Credit]:
    rows = conn.execute(
        "SELECT id, vehicle_slug, tax_year, credit_label, amount, "
        "       status, notes "
        "FROM vehicle_credits WHERE vehicle_slug = ? "
        "ORDER BY tax_year DESC, id DESC",
        (vehicle_slug,),
    ).fetchall()
    return [
        Credit(
            id=int(r["id"]),
            vehicle_slug=r["vehicle_slug"],
            tax_year=int(r["tax_year"]),
            credit_label=r["credit_label"],
            amount=r["amount"],
            status=r["status"],
            notes=r["notes"],
        )
        for r in rows
    ]

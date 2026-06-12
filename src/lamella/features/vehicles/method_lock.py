# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 6 — method-lock advisory.

Reads the vehicle's `vehicle_elections` history and returns a
fact-statement advisory when the current-year deduction comparison
implies a method change IRS rules typically restrict. Never blocks,
never asserts eligibility — the first sentence is always what the
user told us; the second is a soft pointer.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass


_MACRS_METHODS = {"MACRS-5YR", "MACRS-SL", "bonus", "section-179"}


@dataclass(frozen=True)
class MethodLockAdvisory:
    text: str
    first_macrs_year: int
    first_standard_year: int | None


def advisory_for_vehicle(
    conn: sqlite3.Connection,
    *,
    vehicle_slug: str,
    target_year: int,
) -> MethodLockAdvisory | None:
    """Return an advisory if the election history shows MACRS /
    §179 / bonus in a prior year AND the target year has no
    recorded method (implying the comparison on the detail page
    invites the user to consider switching methods)."""
    rows = conn.execute(
        "SELECT tax_year, depreciation_method FROM vehicle_elections "
        "WHERE vehicle_slug = ? ORDER BY tax_year ASC",
        (vehicle_slug,),
    ).fetchall()
    if not rows:
        return None
    macrs_years = [
        int(r["tax_year"]) for r in rows
        if r["depreciation_method"] in _MACRS_METHODS
           and int(r["tax_year"]) < int(target_year)
    ]
    if not macrs_years:
        return None
    target_row = next(
        (r for r in rows if int(r["tax_year"]) == int(target_year)),
        None,
    )
    if target_row is not None and target_row["depreciation_method"] in _MACRS_METHODS:
        # User is continuing the MACRS track — no method change implied.
        return None
    first_macrs = min(macrs_years)
    method = next(
        r["depreciation_method"] for r in rows
        if int(r["tax_year"]) == first_macrs
    )
    text = (
        f"In {first_macrs} you recorded {method} depreciation for this "
        f"vehicle. Standard-mileage was generally not available in "
        f"subsequent years after MACRS/§179 was claimed — confirm with "
        f"your tax professional before choosing a different method for "
        f"{target_year}."
    )
    return MethodLockAdvisory(
        text=text,
        first_macrs_year=first_macrs,
        first_standard_year=None,
    )

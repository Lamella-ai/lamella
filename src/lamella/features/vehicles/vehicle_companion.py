# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Canonical per-vehicle chart accounts.

When a vehicle is registered, we scaffold a strict set of accounts
under ``Expenses:<Entity>:Vehicle:<Slug>:*`` (and the asset account
``Assets:<Entity>:Vehicle:<Slug>``). The slug convention is
``V<year><Make><Model>`` with no spaces — e.g. ``V2009FabrikamSuv``,
``V2008FabrikamSuv``, ``V2010BravoMinivan``.

Why strict per-vehicle:
  - Two Fabrikam Suvs (2008 + 2009) end up ambiguous under
    ``Expenses:Personal:Custom:FabrikamSuvFuel`` — you can't tell
    which vehicle a fuel charge belongs to.
  - Per-vehicle scope lets the AI pair a fuel charge with a mileage
    log entry on the same vehicle for the same day.
  - IRS Schedule C Part IV needs per-vehicle totals anyway, so
    the canonical split matches the form.

``vehicle_chart_paths_for`` returns the list; ``ensure_vehicle_chart``
opens any missing ones via AccountsWriter and is safe to re-run.
Detectors that scan for orphan paths like
``Expenses:Personal:Custom:VehicleFabrikamFuel`` use this as the
target shape when proposing rewrites.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date


# The categories every vehicle gets. Mirrors the Schedule C Part IV
# required lines + the TurboTax business-vehicle expense breakdown
# so drag-and-drop categorization during tax prep matches the form
# verbatim. Ownership/property tax is a separate line because it's
# deductible even when other vehicle expenses aren't (Colorado +
# most states treat ownership tax as a personal-property tax).
# Mirrors IRS Pub 463 "Actual Car Expenses" — every line item the
# IRS recognizes for vehicle-expense substantiation gets its own
# account so drag-and-drop categorization maps 1:1 onto the form.
# OwnershipTax and CarWash are additions on top of the IRS list:
# OwnershipTax is deductible separately on Schedule A (state
# personal-property tax), CarWash is a common business expense that
# falls under "maintenance" but users categorize it distinctly.
VEHICLE_CHART_CATEGORIES: tuple[tuple[str, str], ...] = (
    ("Fuel",         "Gas / fuel — Schedule C Part IV, line 29 (IRS Pub 463)"),
    ("Oil",          "Oil changes + lubricants (IRS Pub 463)"),
    ("Tires",        "Tire purchases + installation (IRS Pub 463)"),
    ("Maintenance",  "Routine care — wiper blades, fluids, batteries, alignments"),
    ("Repairs",      "Major fixes — transmission, engine, brakes, body work (IRS Pub 463)"),
    ("Insurance",    "Auto insurance premiums (IRS Pub 463)"),
    ("Registration", "DMV registration + renewal fees (IRS Pub 463)"),
    ("Licenses",     "Commercial driver / trade / operator licenses (IRS Pub 463)"),
    ("OwnershipTax", "Ownership / personal property tax — deductible on Schedule A"),
    ("Tolls",        "Toll road + bridge fees (IRS Pub 463)"),
    ("Parking",      "Parking fees (IRS Pub 463)"),
    ("GarageRent",   "Off-site vehicle storage / garage rent (IRS Pub 463)"),
    ("CarWash",      "Car washes, detailing"),
    ("Lease",        "Lease payments, if leasing instead of depreciating (IRS Pub 463)"),
    ("Accessories",  "Cargo racks, floor mats, hitch, etc."),
    ("Depreciation", "Annual depreciation expense (IRS Pub 463)"),
)


@dataclass(frozen=True)
class VehicleChartPath:
    path: str
    purpose: str

    def __str__(self) -> str:
        return self.path


def vehicle_slug_from_year_make_model(
    year: int | str, make: str, model: str,
) -> str:
    """Produce the canonical slug convention: V<year><Make><Model>
    with all non-alphanumerics stripped. Stable across re-saves so
    account paths don't drift."""
    y = str(year).strip()
    m = "".join(c for c in (make or "").strip() if c.isalnum())
    mod = "".join(c for c in (model or "").strip() if c.isalnum())
    return f"V{y}{m}{mod}"


def vehicle_chart_paths_for(
    *,
    vehicle_slug: str,
    entity_slug: str | None,
) -> list[VehicleChartPath]:
    """The canonical account paths for a vehicle owned by an entity.

    Always returns ``Assets:<Entity>:Vehicle:<Slug>`` plus one
    ``Expenses:<Entity>:Vehicle:<Slug>:<Category>`` per
    VEHICLE_CHART_CATEGORIES entry. Returns empty when entity_slug
    is missing — we refuse to scaffold entity-less vehicle paths.
    """
    if not entity_slug or not vehicle_slug:
        return []
    out: list[VehicleChartPath] = [
        VehicleChartPath(
            path=f"Assets:{entity_slug}:Vehicle:{vehicle_slug}",
            purpose="vehicle's asset account (cost basis + disposal)",
        ),
    ]
    for cat, purpose in VEHICLE_CHART_CATEGORIES:
        out.append(VehicleChartPath(
            path=f"Expenses:{entity_slug}:Vehicle:{vehicle_slug}:{cat}",
            purpose=purpose,
        ))
    return out


def ensure_vehicle_chart(
    *,
    conn,
    settings,
    reader,
    vehicle_slug: str,
    entity_slug: str | None,
) -> list[VehicleChartPath]:
    """Open every missing vehicle-chart account via AccountsWriter.
    Returns the list of paths that were actually newly opened (may
    be empty). Idempotent — already-open paths are skipped."""
    from beancount.core.data import Open
    from lamella.core.registry.accounts_writer import AccountsWriter

    desired = vehicle_chart_paths_for(
        vehicle_slug=vehicle_slug, entity_slug=entity_slug,
    )
    if not desired:
        return []
    existing: set[str] = set()
    for entry in reader.load().entries:
        acct = getattr(entry, "account", None)
        if isinstance(acct, str):
            existing.add(acct)
    needed = [p for p in desired if p.path not in existing]
    if not needed:
        return []
    # Backdate Opens when historical postings for this vehicle exist
    # on legacy paths. If a user ran the app on a previous version
    # with `Expenses:<Entity>:Vehicles:<Slug>:*` (plural) or
    # `Expenses:<Entity>:Custom:<Whatever><Slug><Whatever>` shapes,
    # those paths may have postings dating back years. When the user
    # later migrates those postings to the canonical singular shape,
    # the override txns carry the original posting date — which would
    # fail bean-check with "inactive account" if the canonical Open
    # is dated today. Proactively find the earliest posting date that
    # mentions the vehicle slug anywhere in the account path and
    # backdate each new canonical Open to that date.
    earliest_date: date | None = None
    slug_segment = f":{vehicle_slug}:"
    slug_suffix = f":{vehicle_slug}"
    for entry in reader.load().entries:
        from beancount.core.data import Transaction
        if not isinstance(entry, Transaction):
            continue
        for p in entry.postings or ():
            acct = p.account or ""
            if slug_segment in f"{acct}:" or acct.endswith(slug_suffix):
                d = entry.date
                if earliest_date is None or d < earliest_date:
                    earliest_date = d
                break
    earliest_ref = (
        {p.path: earliest_date for p in needed}
        if earliest_date is not None
        else None
    )
    writer = AccountsWriter(
        main_bean=settings.ledger_main,
        connector_accounts=settings.connector_accounts_path,
    )
    writer.write_opens(
        [p.path for p in needed],
        comment=(
            f"Vehicle chart for {entity_slug}:{vehicle_slug}"
            f" — {len(needed)} account(s)"
            + (f" (backdated to {earliest_date})" if earliest_date else "")
        ),
        existing_paths=existing,
        earliest_ref_by_path=earliest_ref,
    )
    reader.invalidate()
    return needed

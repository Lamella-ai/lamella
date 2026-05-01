# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Canonical per-property chart accounts.

Mirrors vehicle_companion for real estate. Each registered property
gets a strict per-property expense tree under
``Expenses:<Entity>:Property:<Slug>:*`` plus the asset account
``Assets:<Entity>:Property:<Slug>``. Rentals also get
``Income:<Entity>:Property:<Slug>:Rental``.

Why strict per-property:
  - A user with two rental properties needs per-property P&L
    at tax time (Schedule E rows are per-property).
  - Ambiguous paths like ``Expenses:Personal:HomeInsurance`` can't
    tell whether the charge is on the primary residence, a second
    home, or a rental.
  - The AI classifier can pair a utility charge with the property
    whose address matches a receipt/address on file.
"""
from __future__ import annotations

from dataclasses import dataclass


# Categories every property gets. Rentals get the extras at the end.
BASE_PROPERTY_CATEGORIES: tuple[tuple[str, str], ...] = (
    ("PropertyTax", "Local real-estate tax — Schedule A line 5b / Schedule E line 16"),
    ("HOA", "Homeowners-association dues / condo fees"),
    ("Insurance", "Homeowners / landlord insurance premium"),
    ("Maintenance", "Routine upkeep: yard, HVAC service, pest control"),
    ("Repairs", "Non-capital repairs (Schedule E line 14)"),
    ("Utilities", "Electric / gas / water / sewer / trash / internet if paid"),
    ("MortgageInterest", "Mortgage interest paid — Schedule A line 8a / Schedule E line 12"),
)

RENTAL_EXTRA_CATEGORIES: tuple[tuple[str, str], ...] = (
    ("Depreciation", "Annual depreciation on the rental property"),
    ("Advertising", "Listing fees, photography, marketing to renters"),
    ("Cleaning", "Turnover cleaning + management-company services"),
    ("Management", "Property-management fees"),
    ("Supplies", "Consumables specific to the rental"),
)


@dataclass(frozen=True)
class PropertyChartPath:
    path: str
    purpose: str

    def __str__(self) -> str:
        return self.path


def property_chart_paths_for(
    *,
    property_slug: str,
    entity_slug: str | None,
    is_rental: bool = False,
) -> list[PropertyChartPath]:
    """The canonical account paths for a property owned by an entity.

    Always returns ``Assets:<Entity>:Property:<Slug>`` + one
    ``Expenses:<Entity>:Property:<Slug>:<Cat>`` per category; rentals
    also get rental-specific expense categories and
    ``Income:<Entity>:Property:<Slug>:Rental``. Returns empty when
    entity_slug is missing — entity-less property paths are
    deprecated (see routes/properties.py).
    """
    if not entity_slug or not property_slug:
        return []
    out: list[PropertyChartPath] = [
        PropertyChartPath(
            path=f"Assets:{entity_slug}:Property:{property_slug}",
            purpose="property asset (cost basis)",
        ),
    ]
    for cat, purpose in BASE_PROPERTY_CATEGORIES:
        out.append(PropertyChartPath(
            path=f"Expenses:{entity_slug}:Property:{property_slug}:{cat}",
            purpose=purpose,
        ))
    if is_rental:
        for cat, purpose in RENTAL_EXTRA_CATEGORIES:
            out.append(PropertyChartPath(
                path=f"Expenses:{entity_slug}:Property:{property_slug}:{cat}",
                purpose=purpose,
            ))
        out.append(PropertyChartPath(
            path=f"Income:{entity_slug}:Property:{property_slug}:Rental",
            purpose="rental income received",
        ))
    return out


def ensure_property_chart(
    *,
    conn,
    settings,
    reader,
    property_slug: str,
    entity_slug: str | None,
    is_rental: bool = False,
) -> list[PropertyChartPath]:
    """Open every missing property-chart account via AccountsWriter.
    Idempotent. Returns the newly-opened list.

    Backdates new Opens when historical postings for this property
    slug already exist on legacy or different-entity paths — matches
    the behavior of ``ensure_vehicle_chart``. Without backdating, a
    rename / migration that references a 2025 posting against a
    canonical Open dated today fails bean-check with "inactive
    account."
    """
    from datetime import date
    from beancount.core.data import Open, Transaction
    from lamella.core.registry.accounts_writer import AccountsWriter

    desired = property_chart_paths_for(
        property_slug=property_slug,
        entity_slug=entity_slug,
        is_rental=is_rental,
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

    # Find earliest posting date on any account-path mentioning this
    # slug (covers misattribution-rename + entity-transfer + legacy
    # plural-path cases). Backdate every new Open to that date so
    # bean-check accepts later override txns.
    earliest_date: date | None = None
    slug_segment = f":{property_slug}:"
    slug_suffix = f":{property_slug}"
    for entry in reader.load().entries:
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
            f"Property chart for {entity_slug}:{property_slug}"
            f" — {len(needed)} account(s)"
            + (f" (backdated to {earliest_date})" if earliest_date else "")
        ),
        existing_paths=existing,
        earliest_ref_by_path=earliest_ref,
    )
    reader.invalidate()
    return needed

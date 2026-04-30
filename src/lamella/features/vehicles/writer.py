# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 7 — writers for every vehicle-related custom directive.

One ``append_*`` helper per directive type. Each wraps
``append_custom_directive`` with the snapshot → append →
bean-check → rollback contract.

Directive roster:
  - ``vehicle``                       — full identity row
  - ``vehicle-yearly-mileage``        — Schedule C Part IV yearly row
  - ``vehicle-valuation``             — KBB/NADA/appraisal observation
  - ``vehicle-election``              — §179 / bonus / MACRS election
  - ``vehicle-credit``                — tax credit / incentive
  - ``vehicle-renewal``               — registration / inspection / ...
  - ``vehicle-trip-template``         — recurring trip template
  - ``mileage-attribution``           — per-trip entity override
  - ``mileage-attribution-revoked``   — clears an attribution override

The companion readers in `vehicles/reader.py` rebuild SQLite rows
from loaded Beancount entries.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, timezone
from pathlib import Path
from typing import Any

from lamella.core.transform.custom_directive import (
    Account,
    append_custom_directive,
    custom_arg,
    custom_meta,
)

log = logging.getLogger(__name__)


CONNECTOR_CONFIG_HEADER = (
    "; connector_config.bean — configuration state written by Lamella.\n"
    "; Paperless field-role mappings and UI-persisted settings live here.\n"
    "; Do not hand-edit; use the /settings pages.\n"
)


# -------------- writers ----------------------------------------------


def _today_ts() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def append_vehicle(
    *,
    connector_config: Path,
    main_bean: Path,
    slug: str,
    display_name: str | None,
    year: int | None,
    make: str | None,
    model: str | None,
    vin: str | None,
    license_plate: str | None,
    entity_slug: str | None,
    purchase_date: str | None,
    purchase_price: str | None,
    purchase_fees: str | None,
    asset_account: str | None,
    gvwr_lbs: int | None,
    placed_in_service: str | None,
    fuel_type: str | None,
    notes: str | None,
    is_active: bool = True,
    run_check: bool = True,
) -> str:
    meta: dict[str, Any] = {
        "lamella-vehicle-display-name": display_name or "",
        "lamella-vehicle-is-active": bool(is_active),
    }
    if year is not None:
        meta["lamella-vehicle-year"] = int(year)
    if make:
        meta["lamella-vehicle-make"] = make
    if model:
        meta["lamella-vehicle-model"] = model
    if vin:
        meta["lamella-vehicle-vin"] = vin
    if license_plate:
        meta["lamella-vehicle-plate"] = license_plate
    if entity_slug:
        meta["lamella-vehicle-entity-slug"] = entity_slug
    if purchase_date:
        meta["lamella-vehicle-purchase-date"] = purchase_date
    if purchase_price:
        meta["lamella-vehicle-purchase-price"] = purchase_price
    if purchase_fees:
        meta["lamella-vehicle-purchase-fees"] = purchase_fees
    if asset_account:
        meta["lamella-vehicle-asset-account"] = Account(asset_account)
    if gvwr_lbs is not None:
        meta["lamella-vehicle-gvwr-lbs"] = int(gvwr_lbs)
    if placed_in_service:
        meta["lamella-vehicle-placed-in-service"] = placed_in_service
    if fuel_type:
        meta["lamella-vehicle-fuel-type"] = fuel_type
    if notes:
        meta["lamella-vehicle-notes"] = notes
    return append_custom_directive(
        target=connector_config, main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=_today_ts().date(),
        directive_type="vehicle",
        args=[slug],
        meta=meta,
        run_check=run_check,
    )


def append_vehicle_deleted(
    *,
    connector_config: Path,
    main_bean: Path,
    slug: str,
    run_check: bool = True,
) -> str:
    """Append a ``custom "vehicle-deleted"`` tombstone directive.

    Phase 1.4 mirror of ``append_property_deleted`` /
    ``append_loan_deleted`` / ``append_entity_deleted``. Vehicles
    don't have a top-level user-facing delete handler today (only
    deactivate via is_active=0 + sub-resource deletes for elections,
    valuations, etc.), but the boot-time discovery via
    ``discover_vehicle_slugs`` does INSERT OR IGNORE based on the
    ledger's Open directives — so if a delete handler is ever added
    or a manual DELETE FROM vehicles is run, the tombstone path is
    in place to prevent resurrection.
    """
    return append_custom_directive(
        target=connector_config, main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=_today_ts().date(),
        directive_type="vehicle-deleted",
        args=[slug],
        meta={"lamella-deleted-at": datetime.now(UTC).isoformat(timespec="seconds")},
        run_check=run_check,
    )


def append_vehicle_yearly_mileage(
    *,
    connector_config: Path,
    main_bean: Path,
    slug: str,
    year: int,
    start_mileage: int | None,
    end_mileage: int | None,
    business_miles: int | None,
    commuting_miles: int | None,
    personal_miles: int | None,
    run_check: bool = True,
) -> str:
    meta: dict[str, Any] = {}
    if start_mileage is not None:
        meta["lamella-yearly-start-mileage"] = int(start_mileage)
    if end_mileage is not None:
        meta["lamella-yearly-end-mileage"] = int(end_mileage)
    if business_miles is not None:
        meta["lamella-yearly-business-miles"] = int(business_miles)
    if commuting_miles is not None:
        meta["lamella-yearly-commuting-miles"] = int(commuting_miles)
    if personal_miles is not None:
        meta["lamella-yearly-personal-miles"] = int(personal_miles)
    return append_custom_directive(
        target=connector_config, main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=_today_ts().date(),
        directive_type="vehicle-yearly-mileage",
        args=[slug, int(year)],
        meta=meta,
        run_check=run_check,
    )


def append_vehicle_valuation(
    *,
    connector_config: Path,
    main_bean: Path,
    slug: str,
    as_of_date: date,
    value: str,
    source: str | None = None,
    notes: str | None = None,
    run_check: bool = True,
) -> str:
    meta: dict[str, Any] = {"lamella-valuation-value": value}
    if source:
        meta["lamella-valuation-source"] = source
    if notes:
        meta["lamella-valuation-notes"] = notes
    return append_custom_directive(
        target=connector_config, main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=as_of_date,
        directive_type="vehicle-valuation",
        args=[slug, as_of_date],
        meta=meta,
        run_check=run_check,
    )


def append_vehicle_election(
    *,
    connector_config: Path,
    main_bean: Path,
    slug: str,
    tax_year: int,
    depreciation_method: str | None,
    section_179_amount: str | None,
    bonus_depreciation_amount: str | None,
    basis_at_placed_in_service: str | None,
    business_use_pct_override: float | None,
    listed_property_qualified: int | None,
    notes: str | None,
    run_check: bool = True,
) -> str:
    meta: dict[str, Any] = {}
    if depreciation_method:
        meta["lamella-election-method"] = depreciation_method
    if section_179_amount:
        meta["lamella-election-s179"] = section_179_amount
    if bonus_depreciation_amount:
        meta["lamella-election-bonus"] = bonus_depreciation_amount
    if basis_at_placed_in_service:
        meta["lamella-election-basis"] = basis_at_placed_in_service
    if business_use_pct_override is not None:
        meta["lamella-election-business-use-pct"] = str(business_use_pct_override)
    if listed_property_qualified is not None:
        meta["lamella-election-listed-property"] = bool(listed_property_qualified)
    if notes:
        meta["lamella-election-notes"] = notes
    return append_custom_directive(
        target=connector_config, main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=_today_ts().date(),
        directive_type="vehicle-election",
        args=[slug, int(tax_year)],
        meta=meta,
        run_check=run_check,
    )


def append_vehicle_credit(
    *,
    connector_config: Path,
    main_bean: Path,
    slug: str,
    tax_year: int,
    credit_label: str,
    amount: str | None = None,
    status: str | None = None,
    notes: str | None = None,
    run_check: bool = True,
) -> str:
    meta: dict[str, Any] = {}
    if amount:
        meta["lamella-credit-amount"] = amount
    if status:
        meta["lamella-credit-status"] = status
    if notes:
        meta["lamella-credit-notes"] = notes
    return append_custom_directive(
        target=connector_config, main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=_today_ts().date(),
        directive_type="vehicle-credit",
        args=[slug, int(tax_year), credit_label],
        meta=meta,
        run_check=run_check,
    )


def append_vehicle_renewal(
    *,
    connector_config: Path,
    main_bean: Path,
    slug: str,
    renewal_kind: str,
    due_date: date,
    cadence_months: int | None = None,
    last_completed: date | None = None,
    notes: str | None = None,
    is_active: bool = True,
    run_check: bool = True,
) -> str:
    meta: dict[str, Any] = {"lamella-renewal-active": bool(is_active)}
    if cadence_months is not None:
        meta["lamella-renewal-cadence-months"] = int(cadence_months)
    if last_completed is not None:
        meta["lamella-renewal-last-completed"] = last_completed
    if notes:
        meta["lamella-renewal-notes"] = notes
    return append_custom_directive(
        target=connector_config, main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=_today_ts().date(),
        directive_type="vehicle-renewal",
        args=[slug, renewal_kind, due_date],
        meta=meta,
        run_check=run_check,
    )


def append_vehicle_trip_template(
    *,
    connector_config: Path,
    main_bean: Path,
    slug: str,
    display_name: str,
    vehicle_slug: str | None,
    entity: str | None,
    default_from: str | None,
    default_to: str | None,
    default_purpose: str | None,
    default_miles: float | None,
    default_category: str | None,
    is_round_trip: bool = False,
    is_active: bool = True,
    run_check: bool = True,
) -> str:
    meta: dict[str, Any] = {
        "lamella-template-display-name": display_name,
        "lamella-template-round-trip": bool(is_round_trip),
        "lamella-template-active": bool(is_active),
    }
    if vehicle_slug:
        meta["lamella-template-vehicle-slug"] = vehicle_slug
    if entity:
        meta["lamella-template-entity"] = entity
    if default_from:
        meta["lamella-template-from"] = default_from
    if default_to:
        meta["lamella-template-to"] = default_to
    if default_purpose:
        meta["lamella-template-purpose"] = default_purpose
    if default_miles is not None:
        meta["lamella-template-miles"] = str(default_miles)
    if default_category:
        meta["lamella-template-category"] = default_category
    return append_custom_directive(
        target=connector_config, main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=_today_ts().date(),
        directive_type="vehicle-trip-template",
        args=[slug],
        meta=meta,
        run_check=run_check,
    )


def append_mileage_attribution(
    *,
    connector_config: Path,
    main_bean: Path,
    entry_date: date,
    vehicle: str,
    miles: float,
    attributed_entity: str,
    notes: str | None = None,
    run_check: bool = True,
) -> str:
    meta: dict[str, Any] = {
        "lamella-attribution-entity": attributed_entity,
    }
    if notes:
        meta["lamella-attribution-notes"] = notes
    return append_custom_directive(
        target=connector_config, main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=entry_date,
        directive_type="mileage-attribution",
        args=[entry_date, vehicle, str(miles)],
        meta=meta,
        run_check=run_check,
    )


def append_mileage_attribution_revoked(
    *,
    connector_config: Path,
    main_bean: Path,
    entry_date: date,
    vehicle: str,
    miles: float,
    run_check: bool = True,
) -> str:
    return append_custom_directive(
        target=connector_config, main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=entry_date,
        directive_type="mileage-attribution-revoked",
        args=[entry_date, vehicle, str(miles)],
        meta=None,
        run_check=run_check,
    )

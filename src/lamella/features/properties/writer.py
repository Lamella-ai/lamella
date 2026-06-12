# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Writers for `custom "property"` and `custom "property-valuation"`.

Mirrors loans/writer.py. See step10_properties.py for the reconstruct
side. All metadata keys are `lamella-*` per the repo's reconstruction
contract.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from lamella.core.transform.custom_directive import (
    Account,
    append_custom_directive,
)

log = logging.getLogger(__name__)


CONNECTOR_CONFIG_HEADER = (
    "; connector_config.bean — configuration state written by Lamella.\n"
    "; Paperless field-role mappings and UI-persisted settings live here.\n"
    "; Do not hand-edit; use the /settings pages.\n"
)


def _today() -> date:
    return datetime.now(timezone.utc).date()


def _as_date(value: str | date | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return date.fromisoformat(str(value)[:10])


def append_property(
    *,
    connector_config: Path,
    main_bean: Path,
    slug: str,
    display_name: str | None,
    property_type: str,
    entity_slug: str | None,
    address: str | None = None,
    city: str | None = None,
    state: str | None = None,
    postal_code: str | None = None,
    purchase_date: str | date | None = None,
    purchase_price: str | None = None,
    closing_costs: str | None = None,
    asset_account_path: str | None = None,
    sale_date: str | date | None = None,
    sale_price: str | None = None,
    is_primary_residence: bool = False,
    is_rental: bool = False,
    is_active: bool = True,
    notes: str | None = None,
    run_check: bool = True,
) -> str:
    meta: dict[str, Any] = {
        "lamella-property-type": property_type,
        "lamella-property-is-active": bool(is_active),
        "lamella-property-is-primary-residence": bool(is_primary_residence),
        "lamella-property-is-rental": bool(is_rental),
    }
    if display_name:
        meta["lamella-property-display-name"] = display_name
    if entity_slug:
        meta["lamella-property-entity-slug"] = entity_slug
    if address:
        meta["lamella-property-address"] = address
    if city:
        meta["lamella-property-city"] = city
    if state:
        meta["lamella-property-state"] = state
    if postal_code:
        meta["lamella-property-postal-code"] = postal_code
    if purchase_date:
        meta["lamella-property-purchase-date"] = _as_date(purchase_date)
    if purchase_price:
        meta["lamella-property-purchase-price"] = str(purchase_price)
    if closing_costs:
        meta["lamella-property-closing-costs"] = str(closing_costs)
    if asset_account_path:
        meta["lamella-property-asset-account"] = Account(asset_account_path)
    if sale_date:
        meta["lamella-property-sale-date"] = _as_date(sale_date)
    if sale_price:
        meta["lamella-property-sale-price"] = str(sale_price)
    if notes:
        meta["lamella-property-notes"] = notes

    return append_custom_directive(
        target=connector_config, main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=_today(),
        directive_type="property",
        args=[slug],
        meta=meta,
        run_check=run_check,
    )


def append_property_valuation(
    *,
    connector_config: Path,
    main_bean: Path,
    property_slug: str,
    as_of_date: str | date,
    value: str,
    source: str | None = None,
    notes: str | None = None,
    run_check: bool = True,
) -> str:
    meta: dict[str, Any] = {}
    if source:
        meta["lamella-valuation-source"] = source
    if notes:
        meta["lamella-valuation-notes"] = notes
    return append_custom_directive(
        target=connector_config, main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=_as_date(as_of_date),
        directive_type="property-valuation",
        args=[property_slug, str(value)],
        meta=meta,
        run_check=run_check,
    )


def append_property_deleted(
    *,
    connector_config: Path,
    main_bean: Path,
    slug: str,
    run_check: bool = True,
) -> str:
    return append_custom_directive(
        target=connector_config, main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=_today(),
        directive_type="property-deleted",
        args=[slug],
        meta=None,
        run_check=run_check,
    )

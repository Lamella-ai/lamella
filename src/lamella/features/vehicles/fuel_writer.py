# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Writer for `custom "vehicle-fuel-entry"` directives."""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from lamella.core.transform.custom_directive import append_custom_directive

log = logging.getLogger(__name__)


CONNECTOR_CONFIG_HEADER = (
    "; connector_config.bean — configuration state written by Lamella.\n"
    "; Paperless field-role mappings and UI-persisted settings live here.\n"
    "; Do not hand-edit; use the /settings pages.\n"
)


def _as_date(value: str | date | None) -> date:
    if value is None or value == "":
        return datetime.now(timezone.utc).date()
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return date.fromisoformat(str(value)[:10])


def append_fuel_entry(
    *,
    connector_config: Path,
    main_bean: Path,
    vehicle_slug: str,
    as_of_date: str | date,
    quantity: float,
    unit: str = "gallon",
    fuel_type: str = "gasoline",
    as_of_time: str | None = None,
    cost_cents: int | None = None,
    odometer: int | None = None,
    location: str | None = None,
    paperless_id: int | None = None,
    notes: str | None = None,
    source: str = "manual",
    run_check: bool = True,
) -> str:
    meta: dict[str, Any] = {
        "lamella-fuel-quantity": float(quantity),
        "lamella-fuel-unit": unit,
        "lamella-fuel-type": fuel_type,
        "lamella-fuel-source": source,
    }
    if as_of_time:
        meta["lamella-fuel-time"] = as_of_time
    if cost_cents is not None:
        meta["lamella-fuel-cost-cents"] = int(cost_cents)
    if odometer is not None:
        meta["lamella-fuel-odometer"] = int(odometer)
    if location:
        meta["lamella-fuel-location"] = location
    if paperless_id is not None:
        meta["lamella-fuel-paperless-id"] = int(paperless_id)
    if notes:
        meta["lamella-fuel-notes"] = notes

    return append_custom_directive(
        target=connector_config, main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=_as_date(as_of_date),
        directive_type="vehicle-fuel-entry",
        args=[vehicle_slug],
        meta=meta,
        run_check=run_check,
    )

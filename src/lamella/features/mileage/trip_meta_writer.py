# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Writer for `custom "mileage-trip-meta"` — per-trip splits.

Key is (entry_date, vehicle, miles) to match mileage_trip_meta's
UNIQUE constraint. On every save, append a fresh directive; reader
keeps the last-seen per key.
"""
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


def append_trip_meta(
    *,
    connector_config: Path,
    main_bean: Path,
    entry_date: str | date,
    vehicle: str,
    miles: float,
    business_miles: float | None = None,
    personal_miles: float | None = None,
    commuting_miles: float | None = None,
    category: str | None = None,
    purpose_parsed: str | None = None,
    entity_parsed: str | None = None,
    auto_from_ai: bool = False,
    free_text: str | None = None,
    run_check: bool = True,
) -> str:
    meta: dict[str, Any] = {
        "lamella-trip-miles": float(miles),
        "lamella-trip-auto-from-ai": bool(auto_from_ai),
    }
    if business_miles is not None:
        meta["lamella-trip-business-miles"] = float(business_miles)
    if personal_miles is not None:
        meta["lamella-trip-personal-miles"] = float(personal_miles)
    if commuting_miles is not None:
        meta["lamella-trip-commuting-miles"] = float(commuting_miles)
    if category:
        meta["lamella-trip-category"] = category
    if purpose_parsed:
        meta["lamella-trip-purpose-parsed"] = purpose_parsed
    if entity_parsed:
        meta["lamella-trip-entity-parsed"] = entity_parsed
    if free_text:
        meta["lamella-trip-free-text"] = free_text

    return append_custom_directive(
        target=connector_config, main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=_as_date(entry_date),
        directive_type="mileage-trip-meta",
        args=[vehicle, float(miles)],
        meta=meta,
        run_check=run_check,
    )

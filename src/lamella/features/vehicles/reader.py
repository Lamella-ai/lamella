# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 7 — readers for every vehicle-related custom directive.

Parallel to ``writer.py``. Each ``read_*`` function scans loaded
Beancount entries and produces dict rows ready to insert into the
corresponding SQLite table.

Ordering semantics:
  - Most directive types are upsert-by-PK — later writes override
    earlier. Reader applies entries in declaration order, keeping the
    last seen per PK.
  - `mileage-attribution-revoked` nullifies any earlier
    `mileage-attribution` at the same (date, vehicle, miles) key.
  - Disposals (Phase 4) are separate — they're ledger transactions
    tagged `#lamella-vehicle-disposal`, not `custom` directives; the
    step8_vehicles reconstruct pass reads them via a different path.
"""
from __future__ import annotations

from typing import Any, Iterable

from beancount.core.data import Custom

from lamella.core.transform.custom_directive import (
    custom_arg,
    custom_meta,
)


def _bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    s = str(value).strip().lower()
    if s in ("true", "1", "yes"):
        return True
    if s in ("false", "0", "no"):
        return False
    return None


def _int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None


def _float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _str(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value)
    return s if s else None


def read_deleted_vehicle_slugs(entries: Iterable) -> set[str]:
    """Phase 1.4. Return slugs whose latest directive in load order
    is a ``custom "vehicle-deleted"`` tombstone. Used by the
    reconstruct reader and by ``discover_vehicle_slugs`` to prevent
    boot-time resurrection from still-present Open directives."""
    deleted: set[str] = set()
    for entry in entries:
        if not isinstance(entry, Custom) or entry.type != "vehicle-deleted":
            continue
        slug = custom_arg(entry, 0)
        if isinstance(slug, str) and slug.strip():
            deleted.add(slug.strip())
    return deleted


def read_vehicles(entries: Iterable) -> list[dict]:
    state: dict[str, dict] = {}
    deleted = read_deleted_vehicle_slugs(entries)
    for entry in entries:
        if not isinstance(entry, Custom) or entry.type != "vehicle":
            continue
        slug = custom_arg(entry, 0)
        if not slug:
            continue
        if slug in deleted:
            continue
        state[slug] = {
            "slug": slug,
            "display_name": _str(custom_meta(entry, "lamella-vehicle-display-name")),
            "year": _int(custom_meta(entry, "lamella-vehicle-year")),
            "make": _str(custom_meta(entry, "lamella-vehicle-make")),
            "model": _str(custom_meta(entry, "lamella-vehicle-model")),
            "vin": _str(custom_meta(entry, "lamella-vehicle-vin")),
            "license_plate": _str(custom_meta(entry, "lamella-vehicle-plate")),
            "entity_slug": _str(custom_meta(entry, "lamella-vehicle-entity-slug")),
            "purchase_date": _str(custom_meta(entry, "lamella-vehicle-purchase-date")),
            "purchase_price": _str(custom_meta(entry, "lamella-vehicle-purchase-price")),
            "purchase_fees": _str(custom_meta(entry, "lamella-vehicle-purchase-fees")),
            "asset_account_path": _str(custom_meta(entry, "lamella-vehicle-asset-account")),
            "gvwr_lbs": _int(custom_meta(entry, "lamella-vehicle-gvwr-lbs")),
            "placed_in_service_date": _str(custom_meta(entry, "lamella-vehicle-placed-in-service")),
            "fuel_type": _str(custom_meta(entry, "lamella-vehicle-fuel-type")),
            "notes": _str(custom_meta(entry, "lamella-vehicle-notes")),
            "is_active": 1 if _bool(custom_meta(entry, "lamella-vehicle-is-active")) else 0,
        }
    return list(state.values())


def read_vehicle_yearly_mileage(entries: Iterable) -> list[dict]:
    state: dict[tuple, dict] = {}
    for entry in entries:
        if not isinstance(entry, Custom) or entry.type != "vehicle-yearly-mileage":
            continue
        slug = custom_arg(entry, 0)
        year = _int(custom_arg(entry, 1))
        if not slug or year is None:
            continue
        state[(slug, year)] = {
            "vehicle_slug": slug,
            "year": year,
            "start_mileage": _int(custom_meta(entry, "lamella-yearly-start-mileage")),
            "end_mileage": _int(custom_meta(entry, "lamella-yearly-end-mileage")),
            "business_miles": _int(custom_meta(entry, "lamella-yearly-business-miles")),
            "commuting_miles": _int(custom_meta(entry, "lamella-yearly-commuting-miles")),
            "personal_miles": _int(custom_meta(entry, "lamella-yearly-personal-miles")),
        }
    return list(state.values())


def read_vehicle_valuations(entries: Iterable) -> list[dict]:
    state: dict[tuple, dict] = {}
    for entry in entries:
        if not isinstance(entry, Custom) or entry.type != "vehicle-valuation":
            continue
        slug = custom_arg(entry, 0)
        as_of = custom_arg(entry, 1)
        if not slug or as_of is None:
            continue
        as_of_iso = as_of.isoformat() if hasattr(as_of, "isoformat") else str(as_of)
        state[(slug, as_of_iso)] = {
            "vehicle_slug": slug,
            "as_of_date": as_of_iso,
            "value": _str(custom_meta(entry, "lamella-valuation-value")),
            "source": _str(custom_meta(entry, "lamella-valuation-source")),
            "notes": _str(custom_meta(entry, "lamella-valuation-notes")),
        }
    return list(state.values())


def read_vehicle_elections(entries: Iterable) -> list[dict]:
    state: dict[tuple, dict] = {}
    for entry in entries:
        if not isinstance(entry, Custom) or entry.type != "vehicle-election":
            continue
        slug = custom_arg(entry, 0)
        year = _int(custom_arg(entry, 1))
        if not slug or year is None:
            continue
        state[(slug, year)] = {
            "vehicle_slug": slug,
            "tax_year": year,
            "depreciation_method": _str(custom_meta(entry, "lamella-election-method")),
            "section_179_amount": _str(custom_meta(entry, "lamella-election-s179")),
            "bonus_depreciation_amount": _str(custom_meta(entry, "lamella-election-bonus")),
            "basis_at_placed_in_service": _str(custom_meta(entry, "lamella-election-basis")),
            "business_use_pct_override": _float(
                custom_meta(entry, "lamella-election-business-use-pct")
            ),
            "listed_property_qualified": (
                1 if _bool(custom_meta(entry, "lamella-election-listed-property")) else
                (0 if _bool(custom_meta(entry, "lamella-election-listed-property")) is False else None)
            ),
            "notes": _str(custom_meta(entry, "lamella-election-notes")),
        }
    return list(state.values())


def read_vehicle_credits(entries: Iterable) -> list[dict]:
    """Credits have no natural unique identity — args are (slug,
    year, label) which we use as a content-hash key."""
    state: dict[tuple, dict] = {}
    for entry in entries:
        if not isinstance(entry, Custom) or entry.type != "vehicle-credit":
            continue
        slug = custom_arg(entry, 0)
        year = _int(custom_arg(entry, 1))
        label = custom_arg(entry, 2)
        if not slug or year is None or not label:
            continue
        state[(slug, year, label)] = {
            "vehicle_slug": slug,
            "tax_year": year,
            "credit_label": label,
            "amount": _str(custom_meta(entry, "lamella-credit-amount")),
            "status": _str(custom_meta(entry, "lamella-credit-status")),
            "notes": _str(custom_meta(entry, "lamella-credit-notes")),
        }
    return list(state.values())


def read_vehicle_renewals(entries: Iterable) -> list[dict]:
    state: dict[tuple, dict] = {}
    for entry in entries:
        if not isinstance(entry, Custom) or entry.type != "vehicle-renewal":
            continue
        slug = custom_arg(entry, 0)
        kind = custom_arg(entry, 1)
        due = custom_arg(entry, 2)
        if not slug or not kind or due is None:
            continue
        due_iso = due.isoformat() if hasattr(due, "isoformat") else str(due)
        last = custom_meta(entry, "lamella-renewal-last-completed")
        if hasattr(last, "isoformat"):
            last = last.isoformat()
        state[(slug, kind, due_iso)] = {
            "vehicle_slug": slug,
            "renewal_kind": kind,
            "due_date": due_iso,
            "cadence_months": _int(custom_meta(entry, "lamella-renewal-cadence-months")),
            "last_completed": last,
            "notes": _str(custom_meta(entry, "lamella-renewal-notes")),
            "is_active": 1 if _bool(custom_meta(entry, "lamella-renewal-active")) else 0,
        }
    return list(state.values())


def read_vehicle_trip_templates(entries: Iterable) -> list[dict]:
    state: dict[str, dict] = {}
    for entry in entries:
        if not isinstance(entry, Custom) or entry.type != "vehicle-trip-template":
            continue
        slug = custom_arg(entry, 0)
        if not slug:
            continue
        state[slug] = {
            "slug": slug,
            "display_name": _str(custom_meta(entry, "lamella-template-display-name")) or slug,
            "vehicle_slug": _str(custom_meta(entry, "lamella-template-vehicle-slug")),
            "entity": _str(custom_meta(entry, "lamella-template-entity")),
            "default_from": _str(custom_meta(entry, "lamella-template-from")),
            "default_to": _str(custom_meta(entry, "lamella-template-to")),
            "default_purpose": _str(custom_meta(entry, "lamella-template-purpose")),
            "default_miles": _float(custom_meta(entry, "lamella-template-miles")),
            "default_category": _str(custom_meta(entry, "lamella-template-category")),
            "is_round_trip": 1 if _bool(custom_meta(entry, "lamella-template-round-trip")) else 0,
            "is_active": 1 if _bool(custom_meta(entry, "lamella-template-active")) else 0,
        }
    return list(state.values())


def read_mileage_attributions(entries: Iterable) -> list[dict]:
    """Returns {(date, vehicle, miles): attributed_entity} rows,
    after applying `mileage-attribution-revoked` nullifications in
    declaration order."""
    state: dict[tuple, dict | None] = {}
    for entry in entries:
        if not isinstance(entry, Custom):
            continue
        if entry.type not in ("mileage-attribution", "mileage-attribution-revoked"):
            continue
        d = custom_arg(entry, 0)
        v = custom_arg(entry, 1)
        m = _float(custom_arg(entry, 2))
        if d is None or not v or m is None:
            continue
        d_iso = d.isoformat() if hasattr(d, "isoformat") else str(d)
        key = (d_iso, v, m)
        if entry.type == "mileage-attribution-revoked":
            state[key] = None
            continue
        entity = _str(custom_meta(entry, "lamella-attribution-entity"))
        if not entity:
            continue
        state[key] = {
            "entry_date": d_iso,
            "vehicle": v,
            "miles": m,
            "attributed_entity": entity,
        }
    return [row for row in state.values() if row is not None]

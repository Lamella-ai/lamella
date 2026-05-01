# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Readers for `custom "property"` and `custom "property-valuation"`."""
from __future__ import annotations

from typing import Any, Iterable

from beancount.core.data import Custom

from lamella.core.transform.custom_directive import (
    custom_arg,
    custom_meta,
)


def _str(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _bool(v: Any) -> bool | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("true", "1", "yes"):
        return True
    if s in ("false", "0", "no"):
        return False
    return None


def read_properties(entries: Iterable[Any]) -> list[dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    deleted: set[str] = set()
    for entry in entries:
        if not isinstance(entry, Custom):
            continue
        if entry.type == "property-deleted":
            slug = _str(custom_arg(entry, 0))
            if slug:
                deleted.add(slug)
                rows.pop(slug, None)
            continue
        if entry.type != "property":
            continue
        slug = _str(custom_arg(entry, 0))
        if not slug or slug in deleted:
            continue
        rows[slug] = {
            "slug": slug,
            "display_name": _str(custom_meta(entry, "lamella-property-display-name")),
            "property_type": _str(custom_meta(entry, "lamella-property-type")) or "other",
            "entity_slug": _str(custom_meta(entry, "lamella-property-entity-slug")),
            "address": _str(custom_meta(entry, "lamella-property-address")),
            "city": _str(custom_meta(entry, "lamella-property-city")),
            "state": _str(custom_meta(entry, "lamella-property-state")),
            "postal_code": _str(custom_meta(entry, "lamella-property-postal-code")),
            "purchase_date": _str(custom_meta(entry, "lamella-property-purchase-date")),
            "purchase_price": _str(custom_meta(entry, "lamella-property-purchase-price")),
            "closing_costs": _str(custom_meta(entry, "lamella-property-closing-costs")),
            "asset_account_path": _str(custom_meta(entry, "lamella-property-asset-account")),
            "sale_date": _str(custom_meta(entry, "lamella-property-sale-date")),
            "sale_price": _str(custom_meta(entry, "lamella-property-sale-price")),
            "is_primary_residence": _bool(custom_meta(entry, "lamella-property-is-primary-residence")),
            "is_rental": _bool(custom_meta(entry, "lamella-property-is-rental")),
            "is_active": _bool(custom_meta(entry, "lamella-property-is-active")),
            "notes": _str(custom_meta(entry, "lamella-property-notes")),
        }
    return list(rows.values())


def read_property_valuations(entries: Iterable[Any]) -> list[dict[str, Any]]:
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    for entry in entries:
        if not isinstance(entry, Custom) or entry.type != "property-valuation":
            continue
        slug = _str(custom_arg(entry, 0))
        value = _str(custom_arg(entry, 1))
        as_of_date = entry.date.isoformat()
        if not slug or value is None:
            continue
        rows[(slug, as_of_date)] = {
            "property_slug": slug,
            "as_of_date": as_of_date,
            "value": value,
            "source": _str(custom_meta(entry, "lamella-valuation-source")),
            "notes": _str(custom_meta(entry, "lamella-valuation-notes")),
        }
    return list(rows.values())

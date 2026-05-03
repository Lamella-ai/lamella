# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Reader for `custom "project"` directives."""
from __future__ import annotations

import json
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


def read_projects(entries: Iterable[Any]) -> list[dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    deleted: set[str] = set()
    for entry in entries:
        if not isinstance(entry, Custom):
            continue
        if entry.type == "project-deleted":
            slug = _str(custom_arg(entry, 0))
            if slug:
                deleted.add(slug)
                rows.pop(slug, None)
            continue
        if entry.type != "project":
            continue
        slug = _str(custom_arg(entry, 0))
        if not slug or slug in deleted:
            continue
        merchants_raw = _str(custom_meta(entry, "lamella-project-expected-merchants"))
        merchants_json = merchants_raw
        if merchants_raw:
            try:
                # Round-trip to ensure it's valid JSON we can serve back.
                merchants_json = json.dumps(json.loads(merchants_raw))
            except (ValueError, TypeError):
                merchants_json = merchants_raw
        rows[slug] = {
            "slug": slug,
            "display_name": _str(custom_meta(entry, "lamella-project-display-name")) or slug,
            "description": _str(custom_meta(entry, "lamella-project-description")),
            "entity_slug": _str(custom_meta(entry, "lamella-project-entity-slug")),
            "property_slug": _str(custom_meta(entry, "lamella-project-property-slug")),
            "project_type": _str(custom_meta(entry, "lamella-project-type")),
            "start_date": _str(custom_meta(entry, "lamella-project-start-date")),
            "end_date": _str(custom_meta(entry, "lamella-project-end-date")),
            "budget_amount": _str(custom_meta(entry, "lamella-project-budget-amount")),
            "expected_merchants": merchants_json,
            "previous_project_slug": _str(custom_meta(entry, "lamella-project-previous-slug")),
            "is_active": _bool(custom_meta(entry, "lamella-project-is-active")),
            "closed_at": _str(custom_meta(entry, "lamella-project-closed-at")),
            "notes": _str(custom_meta(entry, "lamella-project-notes")),
        }
    return list(rows.values())

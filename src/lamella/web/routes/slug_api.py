# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Slug availability + suggestion API.

Used by the add-modals (entities, vehicles, properties) to validate
the slug field on focus-leave: format-check, collision-check, and
disambiguated-suggestion when the candidate is taken.

Frontend wiring lives in `static/slug_helper.js`; the modal templates
call `LamellaSlug.attach({form, kind})` after render.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from lamella.web.deps import get_db
from lamella.core.registry.service import (
    disambiguate_slug,
    is_valid_slug,
    suggest_slug,
)


router = APIRouter()


# Map kind → (table, table key for disambiguate_slug or None for entities).
# Kept narrow on purpose: anything outside this set returns 400.
_KIND_TO_TABLE: dict[str, str] = {
    "entities": "entities",
    "vehicles": "vehicles",
    "properties": "properties",
    "loans": "loans",
}


@router.get("/api/slugs/check")
def check_slug(
    kind: str = Query(..., description="One of: entities, vehicles, properties, loans"),
    slug: str = Query(..., description="Candidate slug — must validate against [A-Z][A-Za-z0-9_-]*"),
    conn=Depends(get_db),
):
    """Return JSON {available, suggestion, format_ok}.

    - format_ok: false when slug doesn't match the canonical regex.
      In that case suggestion is the auto-fixed PascalCase form (or
      empty if nothing salvageable).
    - available: true when no row in the target table has this slug.
      When false, suggestion holds the next-free disambiguated form
      (e.g. "Acme" → "Acme2").
    """
    table = _KIND_TO_TABLE.get(kind)
    if table is None:
        return {
            "available": False,
            "suggestion": "",
            "format_ok": False,
            "error": f"unknown kind {kind!r}",
        }
    if not is_valid_slug(slug):
        # Try to repair via suggest_slug; the helper PascalCases and
        # X-prefixes leading-digit cases.
        repaired = suggest_slug(slug) or ""
        return {
            "available": False,
            "suggestion": repaired if is_valid_slug(repaired) else "",
            "format_ok": False,
        }
    # Format ok — check collision.
    row = conn.execute(
        f"SELECT 1 FROM {table} WHERE slug = ? LIMIT 1", (slug,),
    ).fetchone()
    if row is None:
        return {"available": True, "suggestion": None, "format_ok": True}
    # Taken — propose a disambiguated alternative.
    if table in {"vehicles", "properties", "loans"}:
        sugg = disambiguate_slug(conn, slug, table)
    else:
        # entities table not supported by disambiguate_slug; mirror the
        # 2..999 scan inline.
        sugg = slug
        for n in range(2, 1000):
            cand = f"{slug}{n}"
            r = conn.execute(
                "SELECT 1 FROM entities WHERE slug = ? LIMIT 1", (cand,),
            ).fetchone()
            if r is None:
                sugg = cand
                break
    return {"available": False, "suggestion": sugg, "format_ok": True}

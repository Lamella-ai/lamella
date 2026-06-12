# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Projects CRUD + detail + closeout routes."""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import date as date_cls
from decimal import Decimal

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from lamella.core.config import Settings
from lamella.web.deps import get_db, get_settings
from lamella.features.projects.service import (
    ProjectService,
    is_valid_project_slug,
)
from lamella.features.projects.writer import (
    append_project,
    append_project_deleted,
)


def _mirror_project_to_ledger(
    conn: sqlite3.Connection, settings: Settings, slug: str,
) -> None:
    """Emit a `custom "project"` directive reflecting the current DB
    row for ``slug``. Best-effort — logs on bean-check failure so the
    UI save still succeeds.
    """
    row = conn.execute(
        "SELECT * FROM projects WHERE slug = ?", (slug,)
    ).fetchone()
    if row is None:
        return
    r = dict(row)
    merchants: list[str] = []
    raw = r.get("expected_merchants")
    if raw:
        try:
            merchants = list(json.loads(raw))
        except (ValueError, TypeError):
            merchants = []
    try:
        append_project(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            slug=r["slug"],
            display_name=r.get("display_name") or r["slug"],
            start_date=r.get("start_date"),
            entity_slug=r.get("entity_slug"),
            property_slug=r.get("property_slug"),
            project_type=r.get("project_type"),
            end_date=r.get("end_date"),
            budget_amount=r.get("budget_amount"),
            expected_merchants=merchants,
            previous_project_slug=r.get("previous_project_slug"),
            is_active=bool(r.get("is_active", 1)),
            closed_at=r.get("closed_at"),
            description=r.get("description"),
            notes=r.get("notes"),
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("project directive write failed for %s: %s", slug, exc)

log = logging.getLogger(__name__)

router = APIRouter()


PROJECT_TYPES = (
    ("", "—"),
    ("home_improvement", "Home improvement"),
    ("home_office", "Home office"),
    ("business", "Business initiative"),
    ("medical", "Medical / health"),
    ("education", "Education / training"),
    ("travel", "Travel"),
    ("other", "Other"),
)


def _entities(conn) -> list[sqlite3.Row]:
    try:
        return conn.execute(
            "SELECT slug, display_name FROM entities "
            "WHERE COALESCE(is_active, 1) = 1 ORDER BY slug"
        ).fetchall()
    except sqlite3.Error:
        return []


def _properties(conn) -> list[sqlite3.Row]:
    try:
        return conn.execute(
            "SELECT slug, display_name FROM properties "
            "WHERE COALESCE(is_active, 1) = 1 ORDER BY slug"
        ).fetchall()
    except sqlite3.Error:
        return []


@router.get("/projects", response_class=HTMLResponse)
def projects_index(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
):
    """List all projects with progress bars + totals."""
    svc = ProjectService(conn)
    all_projects = svc.list_all()
    rows = []
    for p in all_projects:
        totals = svc.totals_for(p.slug)
        rows.append({
            "p": p,
            "spent": totals["total"],
            "txn_count": totals["n"],
        })
    return request.app.state.templates.TemplateResponse(
        request, "projects.html",
        {
            "rows": rows,
            "entities": _entities(conn),
            "properties": _properties(conn),
            "project_types": PROJECT_TYPES,
            "today": date_cls.today().isoformat(),
            "all_projects": all_projects,
        },
    )


@router.post("/projects")
def create_project(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    settings: Settings = Depends(get_settings),
    slug: str = Form(...),
    display_name: str = Form(""),
    description: str = Form(""),
    entity_slug: str = Form(""),
    property_slug: str = Form(""),
    project_type: str = Form(""),
    start_date: str = Form(...),
    end_date: str = Form(""),
    budget_amount: str = Form(""),
    expected_merchants: str = Form(""),
    notes: str = Form(""),
    previous_project_slug: str = Form(""),
):
    slug = slug.strip().lower()
    if not is_valid_project_slug(slug):
        raise HTTPException(400, f"invalid slug {slug!r}")
    merchants = [
        m.strip() for m in expected_merchants.replace("\n", ",").split(",")
        if m.strip()
    ]
    ProjectService(conn).upsert(
        slug=slug,
        display_name=display_name.strip() or slug,
        description=description.strip() or None,
        entity_slug=entity_slug.strip() or None,
        property_slug=property_slug.strip() or None,
        project_type=project_type.strip() or None,
        start_date=start_date.strip(),
        end_date=end_date.strip() or None,
        budget_amount=budget_amount.strip() or None,
        expected_merchants=merchants,
        is_active=True,
        notes=notes.strip() or None,
        previous_project_slug=previous_project_slug.strip() or None,
    )
    _mirror_project_to_ledger(conn, settings, slug)
    return RedirectResponse(f"/projects/{slug}", status_code=303)


@router.get("/projects/{slug}", response_class=HTMLResponse)
def project_detail(
    slug: str,
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
):
    svc = ProjectService(conn)
    project = svc.get(slug)
    if project is None:
        raise HTTPException(404, "project not found")
    txns = svc.txns_for(slug)
    totals = svc.totals_for(slug)
    over_budget = False
    pct_used = 0.0
    if project.budget_amount and project.budget_amount > 0:
        pct_used = float(
            (Decimal(str(totals["total"])) / project.budget_amount) * 100
        )
        over_budget = pct_used > 100.0
    # Chain (continuation) totals so a paused-then-restarted
    # project shows aggregate progress across all continuations.
    chain = svc.chain(slug)
    chain_total: Decimal = Decimal("0")
    chain_txns = 0
    for p in chain:
        t = svc.totals_for(p.slug)
        chain_total += t["total"]
        chain_txns += int(t["n"])
    all_projects = svc.list_all()
    return request.app.state.templates.TemplateResponse(
        request, "project_detail.html",
        {
            "p": project,
            "txns": txns,
            "totals": totals,
            "pct_used": pct_used,
            "over_budget": over_budget,
            "entities": _entities(conn),
            "properties": _properties(conn),
            "project_types": PROJECT_TYPES,
            "merchants_textarea":
                "\n".join(project.expected_merchants),
            "chain": chain,
            "chain_total": chain_total,
            "chain_txns": chain_txns,
            "all_projects": all_projects,
        },
    )


@router.post("/projects/{slug}")
def update_project(
    slug: str,
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    settings: Settings = Depends(get_settings),
    display_name: str = Form(""),
    description: str = Form(""),
    entity_slug: str = Form(""),
    property_slug: str = Form(""),
    project_type: str = Form(""),
    start_date: str = Form(...),
    end_date: str = Form(""),
    budget_amount: str = Form(""),
    expected_merchants: str = Form(""),
    notes: str = Form(""),
    is_active: str = Form("1"),
    previous_project_slug: str = Form(""),
):
    svc = ProjectService(conn)
    existing = svc.get(slug)
    if existing is None:
        raise HTTPException(404, "project not found")
    merchants = [
        m.strip() for m in expected_merchants.replace("\n", ",").split(",")
        if m.strip()
    ]
    svc.upsert(
        slug=slug,
        display_name=display_name.strip() or existing.display_name,
        description=description.strip() or None,
        entity_slug=entity_slug.strip() or None,
        property_slug=property_slug.strip() or None,
        project_type=project_type.strip() or None,
        start_date=start_date.strip(),
        end_date=end_date.strip() or None,
        budget_amount=budget_amount.strip() or None,
        expected_merchants=merchants,
        is_active=is_active == "1",
        notes=notes.strip() or None,
        previous_project_slug=previous_project_slug.strip() or None,
    )
    _mirror_project_to_ledger(conn, settings, slug)
    return RedirectResponse(f"/projects/{slug}", status_code=303)


@router.post("/projects/{slug}/close")
def close_project(
    slug: str,
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    settings: Settings = Depends(get_settings),
    closeout_notes: str = Form(""),
):
    svc = ProjectService(conn)
    project = svc.get(slug)
    if project is None:
        raise HTTPException(404, "project not found")
    totals = svc.totals_for(slug)
    closeout = {
        "actual_total": totals["total"],
        "txn_count": totals["n"],
        "budget": str(project.budget_amount) if project.budget_amount else None,
        "under_budget": (
            project.budget_amount - totals["total"]
            if project.budget_amount else None
        ),
        "closed_notes": closeout_notes.strip(),
    }
    svc.close(slug, closeout=closeout)
    _mirror_project_to_ledger(conn, settings, slug)
    return RedirectResponse(f"/projects/{slug}", status_code=303)


@router.post("/projects/{slug}/delete")
def delete_project(
    slug: str,
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    settings: Settings = Depends(get_settings),
):
    ProjectService(conn).delete(slug)
    try:
        append_project_deleted(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            slug=slug,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("project-deleted directive write failed for %s: %s", slug, exc)
    return RedirectResponse("/projects", status_code=303)

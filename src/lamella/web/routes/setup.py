# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""First-run setup endpoints.

Reached when the boot-time detector (``bootstrap.detection``)
classifies the ledger as missing, unparseable, or structurally
empty. Offers two paths:

1. **Start fresh** — scaffold a canonical v1 ledger in
   ``settings.ledger_dir`` per LEDGER_LAYOUT.md §8.3.
2. **Import existing** — transform an existing ledger per the
   three-bucket model (LEDGER_LAYOUT.md §7 and §9). Full flow:
   analyze → dry-run preview → apply → bean-check → seed SQLite
   via ``transform.reconstruct``. Any stage failing rolls the
   ledger and DB back to their pre-apply state (§9 steps 7 and
   8.5).
"""
from __future__ import annotations

import logging
from pathlib import Path

import sqlite3

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from lamella.core.bootstrap.classifier import analyze_import
from lamella.core.bootstrap.detection import (
    DetectionResult,
    detect_ledger_state,
)
from lamella.core.bootstrap.import_apply import (
    ImportApplyError,
    apply_import,
    copy_install_tree,
    plan_import,
)
from lamella.core.beancount_io import LedgerReader
from lamella.core.bootstrap.scaffold import ScaffoldError, scaffold_fresh
from lamella.core.config import Settings
from lamella.web.deps import get_db, get_ledger_reader, get_settings

log = logging.getLogger(__name__)

router = APIRouter()


def _bean_check_runner(path: Path) -> list[str]:
    """Wraps beancount.loader.load_file, stripping informational errors.

    Mirrors the filter in ``bootstrap.detection._fatal_error_messages``
    so a freshly-scaffolded ledger with ``auto_accounts`` loaded
    doesn't incorrectly look broken.
    """
    from beancount import loader

    _entries, errors, _opts = loader.load_file(str(path))
    out: list[str] = []
    for e in errors:
        msg = getattr(e, "message", str(e))
        if "Auto-inserted" in msg:
            continue
        source = getattr(e, "source", None)
        filename = ""
        if isinstance(source, dict):
            filename = source.get("filename", "") or ""
        if isinstance(filename, str) and filename.startswith("<"):
            continue
        out.append(msg)
    return out


def _refresh_detection(request: Request, settings: Settings) -> DetectionResult:
    """Re-run detection and update ``app.state.ledger_detection``."""
    result = detect_ledger_state(settings.ledger_main)
    request.app.state.ledger_detection = result
    return result


def _refresh_setup_required_complete(
    request: Request, settings: Settings,
) -> bool:
    """Re-run :func:`compute_setup_progress` and update
    ``app.state.setup_required_complete``.

    Without this, ``setup_required_complete`` is set once at lifespan
    startup and stays stale until the next container restart — so an
    install that just resolved its drift via ``/setup/recovery/apply``
    keeps getting bounced back to ``/setup`` by the middleware even
    though detection now reports READY. Called from the ``/setup``
    GET handler so any page load that lands there re-evaluates the
    gate, and from the recovery worker on a successful batch.

    Returns the new value so callers can branch (e.g. redirect to
    ``/`` when the gate just flipped True). On any exception, leaves
    the existing value untouched and returns the current state — we
    never silently flip to True on a compute failure.
    """
    db = getattr(request.app.state, "db", None)
    reader = getattr(request.app.state, "ledger_reader", None)
    if db is None:
        return bool(getattr(
            request.app.state, "setup_required_complete", False,
        ))
    try:
        from lamella.features.setup.setup_progress import (
            compute_setup_progress,
        )
        entries = list(reader.load().entries) if reader is not None else []
        progress = compute_setup_progress(
            db, entries,
            imports_dir=settings.import_ledger_output_dir_resolved,
        )
        request.app.state.setup_required_complete = bool(
            progress.required_complete
        )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "setup_required_complete refresh failed; gate unchanged: %s",
            exc,
        )
    return bool(getattr(
        request.app.state, "setup_required_complete", False,
    ))


@router.get("/setup/entities", response_class=HTMLResponse)
def setup_entities_page(
    request: Request,
    add: str | None = None,
    edit: str | None = None,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    """Streamlined entities editor dedicated to the setup flow.

    Shows every active entity with:
      - slug (read-only — rename is a separate merge op)
      - inline entity_type dropdown (writes both DB + ledger directive)
      - inline tax_schedule dropdown
      - display_name + notes inputs

    Everything posts to /setup/entities/{slug}/save which updates the
    DB, writes/rewrites the `custom "entity"` directive, and returns
    the updated row via HTMX so the user stays on the setup page.

    Phase 4 of /setup/recovery: + Add buttons open a modal in-page via
    ``?add=person`` / ``?add=business`` query params. The page never
    sends the user to /settings/entities to create. ``?edit={slug}``
    is reserved for an edit-flow modal (Phase 4 followup; for now
    edits still happen via the inline row form).
    """
    from lamella.web.routes.entities import ENTITY_TYPES, TAX_SCHEDULES
    open_modal = None
    if add in ("person", "business"):
        open_modal = add
    elif edit:
        open_modal = "edit"
    active_rows = [
        dict(r) for r in conn.execute(
            "SELECT slug, display_name, entity_type, tax_schedule, "
            "start_date, notes, is_active FROM entities "
            "WHERE is_active = 1 ORDER BY slug"
        ).fetchall()
    ]
    inactive_rows = [
        dict(r) for r in conn.execute(
            "SELECT slug, display_name, entity_type, tax_schedule, "
            "start_date, notes, is_active FROM entities "
            "WHERE is_active = 0 ORDER BY slug"
        ).fetchall()
    ]
    # Count posting impact per entity so the user can tell which slugs
    # are vestigial vs in heavy use — drives Delete vs Migrate vs Skip
    # decisions on the page.
    def _posting_count(slug: str) -> int:
        try:
            return len(_accounts_referencing_slug(conn, slug, only_open=True))
        except Exception:  # noqa: BLE001
            return 0

    for r in active_rows:
        r["account_count"] = _posting_count(r["slug"])
    for r in inactive_rows:
        r["account_count"] = _posting_count(r["slug"])
    # Count how many ACTIVE entities still need entity_type — skip-typed
    # entities count as labeled (the gate is "any non-empty type").
    needs_type = sum(
        1 for r in active_rows if not (r["entity_type"] or "").strip()
    )
    edit_target = None
    if edit:
        row = conn.execute(
            "SELECT slug, display_name, entity_type, tax_schedule, "
            "start_date, notes, is_active FROM entities WHERE slug = ?",
            (edit,),
        ).fetchone()
        if row is not None:
            edit_target = dict(row)
    return request.app.state.templates.TemplateResponse(
        request, "setup_entities.html",
        {
            "entities": active_rows,
            "inactive_entities": inactive_rows,
            "entity_types": ENTITY_TYPES,
            "tax_schedules": TAX_SCHEDULES,
            "needs_type": needs_type,
            "all_labeled": needs_type == 0 and len(active_rows) > 0,
            "total": len(active_rows),
            "open_modal": open_modal,
            "edit_target": edit_target,
            # field_errors / form_values populated by save handlers on
            # validation failure. Default empty so the modal renders
            # cleanly on the GET path.
            "field_errors": {},
            "form_values": {},
            # Recovery-layout progress pill — single-pill for now.
            "step_meta": (
                {"id": "entities", "label": "Entities",
                 "url": "/setup/entities"},
            ),
            "current_step": "entities",
            "step_index": 0,
        },
    )


# ---------------------------------------------------------------------------
# Phase 4 of /setup/recovery: + Add modal save handlers.
#
# These are the in-page-add path that replaces the old
# /settings/entities#add-entity punt. The modal posts here; on success
# we redirect to /setup/entities?added={slug} so the new row appears in
# the list. On validation failure we re-render setup_entities.html with
# the modal still open + field_errors populated so the user's typing
# survives.
#
# Mirrors routes/setup_wizard.py's wizard_entities_save_person /
# save-business shape (see lines 933+ for the canonical version) so a
# future Phase 7 cleanup can extract the shared validator.
# ---------------------------------------------------------------------------


_DEFAULT_TAX_SCHEDULE_BY_ENTITY_TYPE = {
    "personal": "A",
    "sole_proprietorship": "C",
    "llc": "C",
    "partnership": "",
    "s_corp": "",
    "c_corp": "",
    "trust": "",
    "estate": "",
    "nonprofit": "",
    "skip": "",
}


def _validate_new_entity_slug(
    conn,
    *,
    chosen: str,
    display_name: str,
    fallback_seed: str,
) -> tuple[str, str | None]:
    """Resolve and validate a slug. Returns (slug, error_or_None)."""
    from lamella.core.registry.service import is_valid_slug, suggest_slug
    typed = (chosen or "").strip()
    suggested = suggest_slug(display_name) if display_name else fallback_seed
    s = typed if typed else (suggested or fallback_seed)
    if not s:
        return "", "Required."
    if not is_valid_slug(s):
        return s, (
            "Slugs must start with an uppercase letter (A–Z) and "
            "contain only letters, digits, and hyphens."
        )
    existing = conn.execute(
        "SELECT slug FROM entities WHERE slug = ?", (s,),
    ).fetchone()
    if existing is not None:
        return s, (
            f'Slug "{s}" is already used by an existing entity. Pick another.'
        )
    return s, None


def _re_render_entity_modal(
    request: Request,
    *,
    conn,
    open_modal: str,
    field_errors: dict[str, str],
    form_values: dict[str, str],
    edit_target: dict | None = None,
):
    """Re-render setup_entities.html with the modal still open and the
    user's typed values preserved. Used when an add-* / edit handler
    fails validation. Mirrors the wizard's
    routes/setup_wizard.py:875-907 + 1952-1960 shape."""
    from lamella.web.routes.entities import ENTITY_TYPES, TAX_SCHEDULES
    active_rows = [
        dict(r) for r in conn.execute(
            "SELECT slug, display_name, entity_type, tax_schedule, "
            "start_date, notes, is_active FROM entities "
            "WHERE is_active = 1 ORDER BY slug"
        ).fetchall()
    ]
    inactive_rows = [
        dict(r) for r in conn.execute(
            "SELECT slug, display_name, entity_type, tax_schedule, "
            "start_date, notes, is_active FROM entities "
            "WHERE is_active = 0 ORDER BY slug"
        ).fetchall()
    ]
    for r in active_rows:
        r["account_count"] = 0  # cheap; full count only matters on the GET render
    for r in inactive_rows:
        r["account_count"] = 0
    needs_type = sum(
        1 for r in active_rows if not (r["entity_type"] or "").strip()
    )
    return request.app.state.templates.TemplateResponse(
        request, "setup_entities.html",
        {
            "entities": active_rows,
            "inactive_entities": inactive_rows,
            "entity_types": ENTITY_TYPES,
            "tax_schedules": TAX_SCHEDULES,
            "needs_type": needs_type,
            "all_labeled": needs_type == 0 and len(active_rows) > 0,
            "total": len(active_rows),
            "open_modal": open_modal,
            "edit_target": edit_target,
            "field_errors": field_errors,
            "form_values": form_values,
            "step_meta": (
                {"id": "entities", "label": "Entities",
                 "url": "/setup/entities"},
            ),
            "current_step": "entities",
            "step_index": 0,
        },
        status_code=400,
    )


@router.post("/setup/entities/add-person", response_class=HTMLResponse)
async def setup_entity_add_person(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    """Create a new personal-household entity from the modal. Form
    fields:
      - display_name (required)
      - slug (optional; auto-suggested from display_name if blank)
      - notes (optional)
    On success: redirect to /setup/entities?added={slug}. On
    validation failure: re-render the modal with field_errors +
    form_values so the user's typing survives."""
    from fastapi.responses import RedirectResponse
    from lamella.core.registry.service import upsert_entity
    from lamella.core.registry.entity_writer import append_entity_directive

    form = await request.form()
    display_name = (form.get("display_name") or "").strip()
    slug_typed = (form.get("slug") or "").strip()
    notes = (form.get("notes") or "").strip()

    field_errors: dict[str, str] = {}
    if not display_name:
        field_errors["display_name"] = "Required."

    slug, slug_err = _validate_new_entity_slug(
        conn,
        chosen=slug_typed,
        display_name=display_name,
        fallback_seed="Personal",
    )
    if slug_err:
        field_errors["slug"] = slug_err

    if field_errors:
        return _re_render_entity_modal(
            request, conn=conn, open_modal="person",
            field_errors=field_errors,
            form_values={
                "display_name": display_name,
                "slug": slug_typed,
                "notes": notes,
            },
        )

    upsert_entity(
        conn,
        slug=slug,
        display_name=display_name,
        entity_type="personal",
        tax_schedule=_DEFAULT_TAX_SCHEDULE_BY_ENTITY_TYPE["personal"] or None,
        notes=notes or None,
    )
    try:
        append_entity_directive(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            entity_slug=slug,
            display_name=display_name,
            entity_type="personal",
            tax_schedule=_DEFAULT_TAX_SCHEDULE_BY_ENTITY_TYPE["personal"] or None,
            notes=notes or None,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("entity directive write failed for %s: %s", slug, exc)
    request.app.state.setup_required_complete = False
    return RedirectResponse(
        f"/setup/entities?added={slug}", status_code=303,
    )


@router.post("/setup/entities/add-business", response_class=HTMLResponse)
async def setup_entity_add_business(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    """Create a new business entity from the modal. Form fields:
      - display_name (required)
      - slug (optional)
      - entity_type (required, e.g. sole_proprietorship | llc | …)
      - tax_schedule (optional; defaults from entity_type)
      - notes (optional)"""
    from fastapi.responses import RedirectResponse
    from lamella.core.registry.entity_structure import ENTITY_TYPES as _ETYPES
    from lamella.core.registry.service import upsert_entity
    from lamella.core.registry.entity_writer import append_entity_directive

    form = await request.form()
    display_name = (form.get("display_name") or "").strip()
    slug_typed = (form.get("slug") or "").strip()
    entity_type = (form.get("entity_type") or "").strip()
    tax_schedule = (form.get("tax_schedule") or "").strip()
    notes = (form.get("notes") or "").strip()

    field_errors: dict[str, str] = {}
    if not display_name:
        field_errors["display_name"] = "Required."

    valid_business_types = {
        v for v, _ in _ETYPES if v not in ("", "personal", "skip")
    }
    if not entity_type:
        field_errors["entity_type"] = "Pick a business type."
    elif entity_type not in valid_business_types:
        field_errors["entity_type"] = f"Unknown business type: {entity_type!r}."

    slug, slug_err = _validate_new_entity_slug(
        conn,
        chosen=slug_typed,
        display_name=display_name,
        fallback_seed="Business",
    )
    if slug_err:
        field_errors["slug"] = slug_err

    if field_errors:
        return _re_render_entity_modal(
            request, conn=conn, open_modal="business",
            field_errors=field_errors,
            form_values={
                "display_name": display_name,
                "slug": slug_typed,
                "entity_type": entity_type,
                "tax_schedule": tax_schedule,
                "notes": notes,
            },
        )

    if not tax_schedule:
        tax_schedule = _DEFAULT_TAX_SCHEDULE_BY_ENTITY_TYPE.get(
            entity_type, "",
        )

    upsert_entity(
        conn,
        slug=slug,
        display_name=display_name,
        entity_type=entity_type,
        tax_schedule=tax_schedule or None,
        notes=notes or None,
    )
    try:
        append_entity_directive(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            entity_slug=slug,
            display_name=display_name,
            entity_type=entity_type,
            tax_schedule=tax_schedule or None,
            notes=notes or None,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("entity directive write failed for %s: %s", slug, exc)
    request.app.state.setup_required_complete = False
    return RedirectResponse(
        f"/setup/entities?added={slug}", status_code=303,
    )


@router.post("/setup/entities/{slug}/save", response_class=HTMLResponse)
async def setup_entity_save(
    slug: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    """Save a single entity's setup fields. HTMX-aware: returns just
    the updated row when called with HX-Request, otherwise redirects
    back to /setup/entities."""
    from fastapi.responses import RedirectResponse
    from lamella.web.routes.entities import ENTITY_TYPES, TAX_SCHEDULES
    from lamella.core.registry.entity_writer import append_entity_directive
    form = await request.form()
    entity_type = (form.get("entity_type") or "").strip() or None
    tax_schedule = (form.get("tax_schedule") or "").strip() or None
    display_name = (form.get("display_name") or "").strip() or None
    notes = (form.get("notes") or "").strip() or None
    # Load current row first to preserve unspecified fields.
    current = conn.execute(
        "SELECT * FROM entities WHERE slug = ?", (slug,),
    ).fetchone()
    if current is None:
        raise HTTPException(status_code=404, detail="entity not found")
    # Apply updates.
    conn.execute(
        """
        UPDATE entities SET
            entity_type = COALESCE(?, entity_type),
            tax_schedule = COALESCE(?, tax_schedule),
            display_name = COALESCE(?, display_name),
            notes = COALESCE(?, notes)
          WHERE slug = ?
        """,
        (entity_type, tax_schedule, display_name, notes, slug),
    )
    # Persist directive for reconstruct round-trip.
    try:
        refreshed = conn.execute(
            "SELECT display_name, entity_type, tax_schedule, start_date, "
            "ceased_date, notes FROM entities WHERE slug = ?", (slug,),
        ).fetchone()
        append_entity_directive(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            entity_slug=slug,
            display_name=refreshed["display_name"],
            entity_type=refreshed["entity_type"],
            tax_schedule=refreshed["tax_schedule"],
            start_date=refreshed["start_date"],
            ceased_date=refreshed["ceased_date"],
            notes=refreshed["notes"],
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("entity directive write failed for %s: %s", slug, exc)
    # Invalidate cached setup-complete flag so next nav recomputes.
    request.app.state.setup_required_complete = False
    # HTMX save: form uses hx-swap="none" — we push an OOB banner
    # update so the "N still need entity_type" count decrements
    # without blowing away the form. Client JS flips the row icon
    # from ○ to ✓ locally when the form's complete-fields are set.
    if request.headers.get("hx-request", "").lower() == "true":
        banner_html = _render_entities_banner(request, conn)
        return HTMLResponse(
            f'<div hx-swap-oob="innerHTML:#setup-entities-banner">{banner_html}</div>'
        )
    return RedirectResponse("/setup/entities", status_code=303)


def _render_entities_banner(request: Request, conn) -> str:
    """Recompute + render the setup-entities banner body for OOB swap
    after a row save. Mirrors the summary math in setup_entities_page."""
    try:
        active_rows = conn.execute(
            "SELECT slug, entity_type FROM entities WHERE is_active = 1"
        ).fetchall()
    except Exception:  # noqa: BLE001
        return ""
    needs_type = sum(
        1 for r in active_rows if not (r["entity_type"] or "").strip()
    )
    total = len(active_rows)
    all_labeled = needs_type == 0 and total > 0
    return request.app.state.templates.env.get_template(
        "partials/_setup_entities_banner_body.html"
    ).render(
        request=request,
        needs_type=needs_type,
        total=total,
        all_labeled=all_labeled,
    )


def _is_htmx(request: Request) -> bool:
    return request.headers.get("hx-request", "").lower() == "true"


def _hx_aware_error_redirect(
    request: Request, list_url: str, error: str,
):
    """Build an HTMX-safe error response for setup-row endpoints.

    Vanilla form posts get a 303 to the list page (with ``?error=…``);
    HTMX requests get a 204 with an ``HX-Redirect`` header so the shim
    does a full client-side nav instead of swapping the response into
    the row's hx-target. Without this the 303 is silently followed and
    the entire list page (extending base.html) gets outerHTML-swapped
    into a single <tr> — the nested-layout class of bug.

    Preserves any pre-existing query string on ``list_url`` by picking
    the right separator (``&`` if the URL already has ``?``)."""
    from urllib.parse import quote
    from fastapi.responses import Response
    sep = "&" if "?" in list_url else "?"
    target = f"{list_url}{sep}error={quote(error)}"
    if _is_htmx(request):
        return Response(
            status_code=204,
            headers={"HX-Redirect": target},
        )
    return RedirectResponse(target, status_code=303)


def _entity_row_partial(
    request: Request, conn, slug: str,
):
    """Re-render the setup-entities row for ``slug`` so HTMX swaps it
    in place after a save / skip / deactivate / reactivate. Returns
    None when the entity row no longer exists (e.g. after delete) so
    the caller can return an empty 200 to make HTMX remove the row."""
    from lamella.web.routes.entities import ENTITY_TYPES, TAX_SCHEDULES
    row = conn.execute(
        "SELECT slug, display_name, entity_type, tax_schedule, "
        "start_date, notes, is_active FROM entities WHERE slug = ?",
        (slug,),
    ).fetchone()
    if row is None:
        return None
    e = dict(row)
    # Keep the dependency badge accurate after the action.
    try:
        e["account_count"] = len(_accounts_referencing_slug(conn, slug, only_open=True))
    except Exception:  # noqa: BLE001
        e["account_count"] = 0
    return request.app.state.templates.TemplateResponse(
        request, "partials/_setup_entity_row.html",
        {
            "e": e,
            "entity_types": ENTITY_TYPES,
            "tax_schedules": TAX_SCHEDULES,
        },
    )


@router.post("/setup/entities/{slug}/skip", response_class=HTMLResponse)
def setup_entity_skip(
    slug: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    """One-click action: mark a vestigial entity as ``entity_type='skip'``
    so the setup gate accepts it without forcing the user to invent a
    real legal/tax type. Skipped entities are excluded from chart
    scaffolding, classify whitelist, commingle resolution, and reports.

    The entity stays active (so its existing accounts keep working) —
    the user can still Delete or Migrate it later.
    """
    from lamella.core.registry.entity_writer import append_entity_directive
    row = conn.execute(
        "SELECT * FROM entities WHERE slug = ?", (slug,),
    ).fetchone()
    if row is None:
        return _hx_aware_error_redirect(request, "/setup/entities", "missing")
    conn.execute(
        "UPDATE entities SET entity_type = 'skip' WHERE slug = ?", (slug,),
    )
    try:
        refreshed = conn.execute(
            "SELECT display_name, entity_type, tax_schedule, start_date, "
            "ceased_date, notes, is_active FROM entities WHERE slug = ?",
            (slug,),
        ).fetchone()
        append_entity_directive(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            entity_slug=slug,
            display_name=refreshed["display_name"],
            entity_type=refreshed["entity_type"],
            tax_schedule=refreshed["tax_schedule"],
            start_date=refreshed["start_date"],
            ceased_date=refreshed["ceased_date"],
            notes=refreshed["notes"],
            is_active=bool(refreshed["is_active"]),
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("entity skip directive write failed for %s: %s", slug, exc)
    request.app.state.setup_required_complete = False
    if _is_htmx(request):
        partial = _entity_row_partial(request, conn, slug)
        if partial is not None:
            return partial
    return RedirectResponse(
        f"/setup/entities?skipped={slug}", status_code=303,
    )


@router.post("/setup/entities/{slug}/deactivate", response_class=HTMLResponse)
def setup_entity_deactivate(
    slug: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    """Soft-delete the entity (is_active=0). Hides it from setup,
    classify, and pickers but keeps the row + ledger directive so
    historical postings still resolve. Reversible via reactivate.
    """
    from lamella.core.registry.entity_writer import append_entity_directive
    row = conn.execute(
        "SELECT * FROM entities WHERE slug = ?", (slug,),
    ).fetchone()
    if row is None:
        return _hx_aware_error_redirect(request, "/setup/entities", "missing")
    conn.execute(
        "UPDATE entities SET is_active = 0 WHERE slug = ?", (slug,),
    )
    try:
        refreshed = conn.execute(
            "SELECT display_name, entity_type, tax_schedule, start_date, "
            "ceased_date, notes FROM entities WHERE slug = ?", (slug,),
        ).fetchone()
        append_entity_directive(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            entity_slug=slug,
            display_name=refreshed["display_name"],
            entity_type=refreshed["entity_type"],
            tax_schedule=refreshed["tax_schedule"],
            start_date=refreshed["start_date"],
            ceased_date=refreshed["ceased_date"],
            notes=refreshed["notes"],
            is_active=False,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("entity deactivate directive write failed: %s", exc)
    request.app.state.setup_required_complete = False
    if _is_htmx(request):
        partial = _entity_row_partial(request, conn, slug)
        if partial is not None:
            return partial
    return RedirectResponse(
        f"/setup/entities?deactivated={slug}", status_code=303,
    )


@router.post("/setup/entities/{slug}/reactivate", response_class=HTMLResponse)
def setup_entity_reactivate(
    slug: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    """Inverse of deactivate. Useful when the user accidentally hides
    an entity they meant to migrate or skip."""
    from lamella.core.registry.entity_writer import append_entity_directive
    row = conn.execute(
        "SELECT * FROM entities WHERE slug = ?", (slug,),
    ).fetchone()
    if row is None:
        return _hx_aware_error_redirect(request, "/setup/entities", "missing")
    conn.execute(
        "UPDATE entities SET is_active = 1 WHERE slug = ?", (slug,),
    )
    try:
        refreshed = conn.execute(
            "SELECT display_name, entity_type, tax_schedule, start_date, "
            "ceased_date, notes FROM entities WHERE slug = ?", (slug,),
        ).fetchone()
        append_entity_directive(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            entity_slug=slug,
            display_name=refreshed["display_name"],
            entity_type=refreshed["entity_type"],
            tax_schedule=refreshed["tax_schedule"],
            start_date=refreshed["start_date"],
            ceased_date=refreshed["ceased_date"],
            notes=refreshed["notes"],
            is_active=True,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("entity reactivate directive write failed: %s", exc)
    request.app.state.setup_required_complete = False
    if _is_htmx(request):
        partial = _entity_row_partial(request, conn, slug)
        if partial is not None:
            return partial
    return RedirectResponse(
        f"/setup/entities?reactivated={slug}", status_code=303,
    )


@router.post("/setup/entities/{slug}/delete", response_class=HTMLResponse)
def setup_entity_delete(
    slug: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    """Hard-delete the entity row + strip its ledger directive. Refuses
    when any account_meta still references the slug or when the entity
    appears as the second segment of any opened account path — those
    must be migrated/closed first.
    """
    from datetime import date as _date_t
    from urllib.parse import quote as _urlq
    from lamella.features.setup.posting_counts import (
        DeleteRefusal, assert_safe_to_delete_entity,
    )
    row = conn.execute(
        "SELECT slug, display_name FROM entities WHERE slug = ?", (slug,),
    ).fetchone()
    if row is None:
        return _hx_aware_error_redirect(request, "/setup/entities", "missing")
    # Refuse delete on records carrying user-typed information OR live
    # transactions. Empty scaffolding only — anything else routes
    # through deactivate / close. The refusal carries an actionable
    # message ("N transactions across M accounts; close them first")
    # that the manage template renders verbatim.
    try:
        reader = request.app.state.ledger_reader
        entries = list(reader.load().entries)
        assert_safe_to_delete_entity(
            conn, entries, slug,
            accounts_referencing_slug=_accounts_referencing_slug,
        )
    except DeleteRefusal as exc:
        return RedirectResponse(
            f"/setup/entities/{slug}/manage?error=" + _urlq(exc.message),
            status_code=303,
        )
    # Four tables FK to entities(slug): accounts_meta, loans, vehicles,
    # properties. Block the delete if any ACTIVE row in those tables
    # references this entity — those are load-bearing bindings the
    # user needs to move before we yank the entity.
    active_blockers: list[str] = []
    for table, active_col in (
        ("loans", "is_active"),
        ("vehicles", "is_active"),
        ("properties", "is_active"),
    ):
        try:
            row = conn.execute(
                f"SELECT COUNT(*) AS n FROM {table} "
                f"WHERE entity_slug = ? AND {active_col} = 1",
                (slug,),
            ).fetchone()
            if row and int(row["n"]) > 0:
                active_blockers.append(f"{int(row['n'])}-{table}")
        except Exception:  # noqa: BLE001
            pass
    if active_blockers:
        return RedirectResponse(
            f"/setup/entities/{slug}/manage?error="
            f"active-{'-and-'.join(active_blockers)}-still-reference-slug",
            status_code=303,
        )
    # Null out the FK-bearing column on every remaining reference
    # (closed accounts_meta rows, deactivated loans/vehicles/properties)
    # so SQLite's FK doesn't reject the DELETE. The column value was a
    # cached hint tied to this slug; clearing it is harmless.
    for table in ("accounts_meta", "loans", "vehicles", "properties"):
        try:
            conn.execute(
                f"UPDATE {table} SET entity_slug = NULL WHERE entity_slug = ?",
                (slug,),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "null-out %s.entity_slug for %s failed: %s", table, slug, exc,
            )
    # Strip the entity directive from connector_config.bean and append
    # an ``entity-deleted`` tombstone in a single envelope-protected
    # write. This is a line-deleting + line-appending edit on a
    # (presumed) parseable ledger; use the recovery envelope (load_file
    # + fatal-error subset diff) rather than bean-check-vs-baseline,
    # because any pre-existing non-fatal parse noise downstream of the
    # stripped block would get shifted line numbers and false-positive
    # the line-keyed diff. The envelope is also the right place to gate
    # the DB delete on the ledger write: previously the DELETE FROM
    # entities ran regardless of whether the ledger write succeeded
    # (swallowed in a broad except), which could silently diverge DB
    # state from the ledger and confuse reconstruct on the next boot.
    #
    # Phase 1.4: the tombstone is the §7 #7 fix's missing piece. Without
    # it, the next boot's discover_entity_slugs would walk the still-
    # present Open directives for Expenses:<slug>:* and re-INSERT the
    # row via seed_entities — undoing the user's delete.
    from lamella.core.registry.entity_writer import (
        _strip_existing_entity_blocks,
    )
    from lamella.core.ledger_writer import BeanCheckError
    from lamella.core.transform.custom_directive import render_directive
    from datetime import datetime as _dt
    cfg_path = settings.connector_config_path
    today_dt = _date_t.today()
    tombstone_block = render_directive(
        directive_date=today_dt,
        directive_type="entity-deleted",
        args=[slug],
        meta={
            "lamella-deleted-at": _dt.now().astimezone().isoformat(timespec="seconds"),
        },
    )

    def _do_write() -> None:
        # Strip existing entity directive(s) for this slug, then append
        # the tombstone. Both edits go through the same envelope so a
        # parse failure restores the original file bytes.
        if cfg_path.exists():
            current = cfg_path.read_text(encoding="utf-8")
            stripped = _strip_existing_entity_blocks(current, slug)
            new_text = stripped.rstrip() + "\n" + tombstone_block
        else:
            cfg_path.parent.mkdir(parents=True, exist_ok=True)
            header = (
                "; connector_config.bean — configuration state written by Lamella.\n"
                "; Paperless field-role mappings and UI-persisted settings live here.\n"
                "; Do not hand-edit; use the /settings pages.\n\n"
            )
            new_text = header + tombstone_block
        cfg_path.write_text(new_text, encoding="utf-8")

    try:
        _recovery_write_envelope(
            main_bean=settings.ledger_main,
            files_to_snapshot=[cfg_path, settings.ledger_main],
            write_fn=_do_write,
        )
    except BeanCheckError as exc:
        return RedirectResponse(
            f"/setup/entities/{slug}/manage?error=bean-check-{exc}",
            status_code=303,
        )
    except Exception as exc:  # noqa: BLE001
        log.exception(
            "entity-deleted tombstone write failed for %s", slug,
        )
        return RedirectResponse(
            f"/setup/entities/{slug}/manage?error={type(exc).__name__}",
            status_code=303,
        )
    try:
        conn.execute("DELETE FROM entities WHERE slug = ?", (slug,))
    except Exception as exc:  # noqa: BLE001
        log.exception("DELETE FROM entities failed for %s", slug)
        return RedirectResponse(
            f"/setup/entities/{slug}/manage?error={type(exc).__name__}-{exc}",
            status_code=303,
        )
    request.app.state.setup_required_complete = False
    if _is_htmx(request):
        # Row is gone — returning empty 200 makes HTMX replace the tr
        # element with nothing (outerHTML swap).
        return HTMLResponse("")
    return RedirectResponse(
        f"/setup/entities?deleted={slug}", status_code=303,
    )


def _ensure_open_covers(settings, reader, account_path: str, earliest: _date_t) -> bool:
    """Ensure the Open directive for ``account_path`` is dated on or
    before ``earliest``. If the live ledger has the Open at a later
    date, rewrite the date in-place in connector_accounts.bean and
    re-bean-check.

    Returns True if the post-condition holds (either the Open already
    covered the date, or we successfully rewrote it). False when no
    Open exists at all (caller should refuse the migrate).

    Why this exists: migrate flows write override directives dated
    as the original posting. Each override posts to a target account
    that may have been scaffolded with today's date. Bean-check
    rejects "inactive account" if the override date precedes the
    Open date. Without this helper, the migrate succeeds at the
    writer level but corrupts main.bean so thoroughly that every
    other route 500s.
    """
    import re as _re
    from beancount.core.data import Open as _Open_cov
    try:
        load = reader.load()
    except Exception:  # noqa: BLE001
        return False
    existing_open_date = None
    for e in load.entries:
        if isinstance(e, _Open_cov) and e.account == account_path:
            existing_open_date = e.date
            break
    if existing_open_date is None:
        return False
    if existing_open_date <= earliest:
        return True
    # Need to backdate. Find the Open line in connector_accounts.bean
    # and rewrite the date prefix.
    accounts_path = settings.connector_accounts_path
    if not accounts_path.exists():
        return False
    old_text = accounts_path.read_text(encoding="utf-8")
    # Match a whole line: `YYYY-MM-DD open <path>` with optional
    # trailing comment/commodity. Account path may contain : and a
    # few safe chars.
    pattern = _re.compile(
        r"^(\d{4}-\d{2}-\d{2})(\s+open\s+"
        + _re.escape(account_path)
        + r")\b",
        _re.MULTILINE,
    )
    new_date_str = earliest.isoformat()
    new_text, n = pattern.subn(f"{new_date_str}\\2", old_text, count=1)
    if n == 0:
        return False  # Open line not found in the file we control
    backup = old_text.encode("utf-8")
    try:
        accounts_path.write_text(new_text, encoding="utf-8")
        from lamella.core.ledger_writer import (
            capture_bean_check, run_bean_check_vs_baseline, BeanCheckError,
        )
        _c, baseline = capture_bean_check(settings.ledger_main)
        run_bean_check_vs_baseline(settings.ledger_main, baseline)
    except BeanCheckError:
        accounts_path.write_bytes(backup)
        return False
    except Exception:  # noqa: BLE001
        accounts_path.write_bytes(backup)
        return False
    reader.invalidate()
    return True


def _path_uses_slug(account_path: str, slug: str) -> bool:
    """True when ``slug`` is the entity-owner segment of ``account_path``
    — i.e. position 1 in the colon-split, right after the top-level
    bucket (Assets/Liabilities/Expenses/Income/Equity).

    Earlier this matched any segment, but that produced false
    positives on paths like ``Assets:Personal:Property:PinewoodHouse``
    where ``Property`` is a subcategory label owned by ``Personal``,
    not the ``Property`` entity. Entity ownership in this ledger
    convention always lives at segment 1.
    """
    segments = account_path.split(":")
    if len(segments) < 2:
        return False
    return segments[1] == slug


def _accounts_referencing_slug(conn, slug: str, *, only_open: bool = True):
    """All accounts_meta rows that reference ``slug`` either via
    ``entity_slug`` OR via the path containing the slug as a segment.

    Pulls candidates with a broad SQL filter (entity_slug match OR path
    contains the slug as a substring), then refines in Python using
    ``_path_uses_slug`` so results survive any path shape — including
    ``Equity:<slug>`` (no trailing segment) and slugs that appear deep
    in the path (e.g. ``Equity:Personal:<slug>``). The narrow
    ``LIKE 'Assets:<slug>:%'`` patterns were missing both cases.
    """
    contains = f"%:{slug}%"
    starts_with = f"{slug}:%"
    # Parens around the OR group matter: SQL AND binds tighter than
    # OR, so `A OR B OR C OR D AND E` becomes `A OR B OR C OR (D AND
    # E)` — only the last OR branch gets the closed_on filter. Wrap
    # the whole OR group so `only_open` applies to every match.
    sql = (
        "SELECT account_path, kind, entity_slug, closed_on "
        "  FROM accounts_meta "
        " WHERE (entity_slug = ? "
        "     OR account_path = ? "
        "     OR account_path LIKE ? "
        "     OR account_path LIKE ?) "
    )
    params = [slug, slug, contains, starts_with]
    if only_open:
        sql += " AND closed_on IS NULL"
    sql += " ORDER BY account_path"
    rows = conn.execute(sql, params).fetchall()
    out = []
    for r in rows:
        path = r["account_path"]
        if (r["entity_slug"] or "") == slug or _path_uses_slug(path, slug):
            out.append(r)
    return out


@router.get("/setup/entities/{slug}/manage", response_class=HTMLResponse)
def setup_entity_manage_page(
    slug: str,
    request: Request,
    error: str | None = None,
    migrated: int = 0,
    failed: int = 0,
    cleaned: int = 0,
    closed: int = 0,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Per-entity management page. For each account that references
    the entity, classifies the reference source:

      - ``path``: slug appears as a segment in the account path
      - ``meta``: only ``accounts_meta.entity_slug`` points here
      - ``both``: both — path AND meta agree

    Stale ``meta``-only references typically come from earlier
    discovery passes that set ``entity_slug`` heuristically; clearing
    them is safe and unblocks delete. Path references with zero
    postings can be Closed (write a Close directive) to also unblock
    delete. Path references with postings need migration.
    """
    from lamella.web.routes.entities import ENTITY_TYPES

    entity = conn.execute(
        "SELECT slug, display_name, entity_type, tax_schedule, is_active, "
        "notes FROM entities WHERE slug = ?", (slug,),
    ).fetchone()
    if entity is None:
        return _hx_aware_error_redirect(request, "/setup/entities", "missing")
    entity_dict = dict(entity)

    # Use the centralized helper so this query catches Equity:<slug>
    # (no trailing segment) and slug-deep-in-path cases too. The
    # narrow Assets/Liabilities/Expenses/Income LIKE-prefix patterns
    # were missing both. only_open=False so closed accounts still
    # show up in the listing for context.
    rows = _accounts_referencing_slug(conn, slug, only_open=False)

    # Walk the ledger once: collect posting counts per account AND
    # 5-most-recent samples per account so the user sees what they're
    # about to migrate.
    posting_counts: dict[str, int] = {}
    samples: dict[str, list[dict]] = {}
    paths_in_scope = {r["account_path"] for r in rows}
    try:
        load = reader.load()
        from beancount.core.data import Transaction
        from lamella.core.beancount_io.txn_hash import (
            txn_hash as _tx_hash,
        )
        from lamella.features.setup.posting_counts import (
            already_migrated_hashes, is_override_txn,
        )
        already_migrated = already_migrated_hashes(load.entries)
        for e in load.entries:
            if not isinstance(e, Transaction):
                continue
            # Skip our own migrate-overrides AND already-migrated
            # originals so a fully-migrated account reports 0 —
            # otherwise the `deletable` gate stays False after the
            # user completes a migration. Same filter the §7 #5 fix
            # applied to /setup/accounts/close. Centralized in
            # setup.posting_counts.
            if is_override_txn(e):
                continue
            if _tx_hash(e) in already_migrated:
                continue
            for p in e.postings or ():
                acct = p.account
                if acct not in paths_in_scope:
                    continue
                posting_counts[acct] = posting_counts.get(acct, 0) + 1
                if len(samples.setdefault(acct, [])) < 5:
                    amt = (
                        f"{p.units.number:.2f} {p.units.currency}"
                        if p.units and p.units.number is not None
                        else "—"
                    )
                    samples[acct].append({
                        "date": e.date.isoformat() if hasattr(e.date, "isoformat") else str(e.date),
                        "payee": e.payee or "",
                        "narration": (e.narration or "")[:80],
                        "amount": amt,
                    })
    except Exception as exc:  # noqa: BLE001
        log.warning("posting count load failed for %s manage: %s", slug, exc)

    accounts: list[dict] = []
    n_meta_only_stale = 0
    n_path_unused = 0
    for r in rows:
        path = r["account_path"]
        in_path = _path_uses_slug(path, slug)
        in_meta = (r["entity_slug"] or "") == slug
        if in_path and in_meta:
            ref_source = "both"
        elif in_path:
            ref_source = "path"
        else:
            ref_source = "meta"
        pcount = posting_counts.get(path, 0)
        is_closed = r["closed_on"] is not None
        if ref_source == "meta":
            n_meta_only_stale += 1
        if ref_source in ("path", "both") and pcount == 0 and not is_closed:
            n_path_unused += 1
        # Suggested migration target: drop the slug segment, route to
        # Personal as a safe default. Only meaningful for path-owned
        # accounts.
        suggested = path
        if in_path:
            without = [seg for seg in path.split(":") if seg != slug]
            if len(without) >= 2 and without[1] != "Personal":
                without.insert(1, "Personal")
            suggested = ":".join(without)
        accounts.append({
            "path": path,
            "kind": r["kind"] or "",
            "entity_slug": r["entity_slug"] or "",
            "closed": is_closed,
            "posting_count": pcount,
            "ref_source": ref_source,
            "suggested_target": suggested,
            "samples": samples.get(path, []),
        })

    other_entities = [
        r["slug"] for r in conn.execute(
            "SELECT slug FROM entities WHERE slug != ? AND is_active = 1 "
            "ORDER BY slug",
            (slug,),
        ).fetchall()
    ]

    total_postings = sum(a["posting_count"] for a in accounts)
    # Deletable = empty-scaffolding entity (no postings, no live
    # path-owned accounts, no stale meta refs) AND no user-typed
    # information (display_name / entity_type / tax_schedule unset).
    # Mirrors ``assert_safe_to_delete_entity`` so the page-rendered
    # button only appears when the handler will actually accept the
    # delete; otherwise the user sees a Delete button that 303s with
    # an actionable refusal — preferable to silently rejecting on
    # click. Phase 1.4 follow-up.
    has_user_info = any((
        (entity_dict.get("display_name") or "").strip(),
        (entity_dict.get("entity_type") or "").strip(),
        (entity_dict.get("tax_schedule") or "").strip(),
    ))
    deletable = (
        total_postings == 0
        and not any(
            (a["ref_source"] in ("path", "both")) and not a["closed"]
            for a in accounts
        )
        and n_meta_only_stale == 0
        and not has_user_info
    )
    return request.app.state.templates.TemplateResponse(
        request, "setup_entity_manage.html",
        {
            "entity": entity_dict,
            "accounts": accounts,
            "other_entities": other_entities,
            "entity_types": ENTITY_TYPES,
            "total_postings": total_postings,
            "deletable": deletable,
            "n_meta_only_stale": n_meta_only_stale,
            "n_path_unused": n_path_unused,
            "error": error,
            "migrated": migrated,
            "failed": failed,
            "cleaned": cleaned,
            "closed_count": closed,
        },
    )


@router.post("/setup/entities/{slug}/cleanup-stale-meta")
def setup_entity_cleanup_stale_meta(
    slug: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    """Clear ``accounts_meta.entity_slug`` for every row where the
    account_path does NOT contain the slug as a segment — i.e. stale
    references left by an earlier discovery pass that mis-attributed
    the account. Safe: only touches a SQLite cache field; no ledger
    writes (no account-meta directive existed for these — discovery
    populated entity_slug speculatively, never via user action)."""
    rows = conn.execute(
        "SELECT account_path FROM accounts_meta WHERE entity_slug = ?",
        (slug,),
    ).fetchall()
    cleared = 0
    for r in rows:
        if not _path_uses_slug(r["account_path"], slug):
            conn.execute(
                "UPDATE accounts_meta SET entity_slug = NULL WHERE account_path = ?",
                (r["account_path"],),
            )
            cleared += 1
    return RedirectResponse(
        f"/setup/entities/{slug}/manage?cleaned={cleared}",
        status_code=303,
    )


@router.post("/setup/entities/{slug}/close-unused-opens")
def setup_entity_close_unused_opens(
    slug: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Write Close directives for path-owned accounts (slug appears in
    the path segments) that have zero postings AND aren't already
    closed. Each Close goes into ``connector_accounts.bean`` next to
    the matching Open; bean-check runs after the batch."""
    from datetime import date as _date_t
    from beancount.core.data import Transaction, Open, Close

    paths_to_close: list[str] = []
    # Candidates come from TWO sources — using just accounts_meta would
    # miss Expenses:* paths, which discovery doesn't seed into the
    # cache. The ledger's Open directives are the authoritative list
    # of what exists with this slug as its entity segment.
    try:
        load = reader.load()
    except Exception as exc:  # noqa: BLE001
        log.warning("ledger load failed during close-unused: %s", exc)
        return RedirectResponse(
            f"/setup/entities/{slug}/manage?error=load-failed",
            status_code=303,
        )
    ledger_open_paths = {
        e.account for e in load.entries
        if isinstance(e, Open) and _path_uses_slug(e.account, slug)
    }
    meta_rows = _accounts_referencing_slug(conn, slug, only_open=True)
    meta_path_candidates = {
        r["account_path"] for r in meta_rows
        if _path_uses_slug(r["account_path"], slug)
    }
    candidates = ledger_open_paths | meta_path_candidates
    if not candidates:
        return RedirectResponse(
            f"/setup/entities/{slug}/manage?closed=0",
            status_code=303,
        )
    used: set[str] = set()
    opens_in_ledger: set[str] = set()
    closes_in_ledger: set[str] = set()
    for e in load.entries:
        if isinstance(e, Transaction):
            for p in e.postings or ():
                if p.account in candidates:
                    used.add(p.account)
        elif isinstance(e, Open):
            if e.account in candidates:
                opens_in_ledger.add(e.account)
        elif isinstance(e, Close):
            if e.account in candidates:
                closes_in_ledger.add(e.account)
    # Three cohorts of unused candidates:
    #  - paths already closed in the ledger → just sync the cache
    #      (writing another Close makes main.bean unparseable)
    #  - paths with an explicit Open but no Close → write Close + bean-check
    #  - paths with no Open at all → drop the stale accounts_meta row
    unused = candidates - used
    already_closed = sorted(unused & closes_in_ledger)
    needs_close = unused - closes_in_ledger
    paths_to_close = sorted(needs_close & opens_in_ledger)
    paths_meta_only = sorted(needs_close - opens_in_ledger)
    if not paths_to_close and not paths_meta_only:
        return RedirectResponse(
            f"/setup/entities/{slug}/manage?closed=0",
            status_code=303,
        )

    # If anything to actually close, append Close directives to
    # connector_accounts.bean and bean-check.
    if paths_to_close:
        accounts_path = settings.connector_accounts_path
        accounts_path.parent.mkdir(parents=True, exist_ok=True)
        if not accounts_path.exists():
            accounts_path.write_text(
                "; connector_accounts.bean — managed by Lamella.\n",
                encoding="utf-8",
            )
        today = _date_t.today().isoformat()
        # Baseline + snapshots BEFORE the write (Phase 1.2 pattern).
        from lamella.core.ledger_writer import (
            BeanCheckError, capture_bean_check, run_bean_check_vs_baseline,
        )
        backup_accounts = accounts_path.read_bytes()
        backup_main = settings.ledger_main.read_bytes()
        _baseline_count, baseline_output = capture_bean_check(
            settings.ledger_main
        )
        block = "\n".join(
            f'{today} close {p}' for p in paths_to_close
        )
        new_text = accounts_path.read_text(encoding="utf-8").rstrip() + "\n\n" + block + "\n"
        try:
            accounts_path.write_text(new_text, encoding="utf-8")
            run_bean_check_vs_baseline(settings.ledger_main, baseline_output)
        except BeanCheckError as exc:
            accounts_path.write_bytes(backup_accounts)
            settings.ledger_main.write_bytes(backup_main)
            return RedirectResponse(
                f"/setup/entities/{slug}/manage?error=bean-check-{exc}",
                status_code=303,
            )
        except Exception as exc:  # noqa: BLE001
            accounts_path.write_bytes(backup_accounts)
            settings.ledger_main.write_bytes(backup_main)
            log.exception("close-unused failed for %s", slug)
            return RedirectResponse(
                f"/setup/entities/{slug}/manage?error={type(exc).__name__}",
                status_code=303,
            )
        # Mark closed in accounts_meta so the cache lines up with the
        # ledger and the row drops out of the manage-page query
        # (only_open=True).
        for p in paths_to_close:
            conn.execute(
                "UPDATE accounts_meta SET closed_on = ? WHERE account_path = ?",
                (today, p),
            )
        reader.invalidate()

    # Stale meta-only rows: no Open exists in the ledger to Close. Just
    # drop the accounts_meta row so the dependency badge clears.
    for p in paths_meta_only:
        conn.execute(
            "DELETE FROM accounts_meta WHERE account_path = ?", (p,),
        )

    # Paths that were already Close'd in the ledger — just sync the
    # cache so the badge clears without writing a duplicate Close.
    today = _date_t.today().isoformat()
    for p in already_closed:
        conn.execute(
            "UPDATE accounts_meta SET closed_on = COALESCE(closed_on, ?) "
            "WHERE account_path = ?",
            (today, p),
        )

    total = len(paths_to_close) + len(paths_meta_only) + len(already_closed)
    return RedirectResponse(
        f"/setup/entities/{slug}/manage?closed={total}",
        status_code=303,
    )


@router.post("/setup/entities/{slug}/migrate-account")
async def setup_entity_migrate_account(
    slug: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Rewrite every posting on ``account`` (referenced by entity
    ``slug``) to ``target`` via override directives in
    ``connector_overrides.bean``. One override per posting — bean-check
    runs after each block; failures roll back the whole batch.

    Form fields:
      - account: the source account path (Assets:<slug>:...)
      - target:  the new account path (typically Assets:Personal:...)
    """
    from datetime import date as _date_t
    from decimal import Decimal as _D

    from beancount.core.data import Transaction
    from lamella.core.beancount_io import txn_hash
    from lamella.core.ledger_writer import BeanCheckError
    from lamella.features.rules.overrides import OverrideWriter

    form = await request.form()
    account = (form.get("account") or "").strip()
    target = (form.get("target") or "").strip()
    if not account or not target:
        return RedirectResponse(
            f"/setup/entities/{slug}/manage?error=missing-input",
            status_code=303,
        )
    if account == target:
        return RedirectResponse(
            f"/setup/entities/{slug}/manage?error=source-equals-target",
            status_code=303,
        )

    from lamella.features.setup.posting_counts import (
        already_migrated_hashes, is_override_txn,
    )

    load = reader.load()
    entries = list(load.entries)
    # §7 #4 shape: re-clicking migrate after a completed migration
    # would re-walk the original txns (which don't carry #lamella-override)
    # and write a second override on top of the first. Filter against
    # the existing override blocks' lamella-override-of meta so a re-click
    # is a no-op. Shared predicate: setup.posting_counts.
    already_migrated = already_migrated_hashes(entries)
    affected: list[tuple[Transaction, str, _D, str]] = []
    for e in entries:
        if not isinstance(e, Transaction):
            continue
        if is_override_txn(e):
            continue
        h = txn_hash(e)
        if h in already_migrated:
            continue
        for p in e.postings or ():
            if p.account == account and p.units and p.units.number is not None:
                affected.append((
                    e,
                    h,
                    _D(p.units.number),
                    p.units.currency or "USD",
                ))
                break  # one override per txn even if posting appears twice

    if not affected:
        return RedirectResponse(
            f"/setup/entities/{slug}/manage?error=no-postings-found",
            status_code=303,
        )

    # Backdate the target's Open if migration txns predate it. Without
    # this, bean-check rejects "inactive account" and the whole
    # migration rolls back — producing the ledger-corruption scenario
    # we hit during manual testing.
    earliest_date = min(
        (txn.date if isinstance(txn.date, _date_t)
         else _date_t.fromisoformat(str(txn.date)))
        for (txn, _, _, _) in affected
    )
    ok = _ensure_open_covers(settings, reader, target, earliest_date)
    if not ok:
        return RedirectResponse(
            f"/setup/entities/{slug}/manage?error=target-{target}"
            f"-open-too-late-(earliest-{earliest_date.isoformat()})",
            status_code=303,
        )

    writer = OverrideWriter(
        main_bean=settings.ledger_main,
        overrides=settings.connector_overrides_path,
        conn=conn,
    )
    applied = 0
    failed: list[str] = []
    for txn, h, signed_amount, currency in affected:
        txn_date_val = (
            txn.date if isinstance(txn.date, _date_t)
            else _date_t.fromisoformat(str(txn.date))
        )
        try:
            writer.append(
                txn_date=txn_date_val,
                txn_hash=h,
                amount=abs(signed_amount),
                from_account=account,
                to_account=target,
                currency=currency,
                narration=(txn.narration or f"entity migration → {target}"),
            )
            applied += 1
        except BeanCheckError as exc:
            failed.append(f"{h[:8]}… bean-check blocked: {exc}")
            break  # stop the batch — bean-check will keep failing
        except Exception as exc:  # noqa: BLE001
            log.exception("entity migration override failed for %s", h[:8])
            failed.append(f"{h[:8]}… {type(exc).__name__}: {exc}")
    reader.invalidate()
    return RedirectResponse(
        f"/setup/entities/{slug}/manage?migrated={applied}&failed={len(failed)}",
        status_code=303,
    )


def _stamp_directive_text() -> str:
    """Return the ``lamella-ledger-version`` stamp directive as a string.

    Extracted so Phase 1.2 pilot tests can monkeypatch this to an
    intentionally broken directive and verify the snapshot envelope
    rolls main.bean back instead of leaving a corrupt ledger on disk.
    """
    from lamella.core.bootstrap.detection import LATEST_LEDGER_VERSION

    return (
        f"\n; Ledger schema version — marks this ledger as "
        f"lamella-managed (v{LATEST_LEDGER_VERSION}).\n"
        f'2020-01-01 custom "lamella-ledger-version" "{LATEST_LEDGER_VERSION}"\n'
    )


def _recovery_write_envelope(
    *,
    main_bean: Path,
    files_to_snapshot: list[Path],
    write_fn,
) -> None:
    """Backward-compat shim for the in-route call sites. The
    implementation moved to :mod:`lamella.features.setup.recovery`
    in Phase 4.1 because vehicle rename needs the same envelope.
    Existing recovery handlers in this module still call the
    underscore-prefixed local — this delegates so we don't have to
    rewrite every site at once."""
    from lamella.features.setup.recovery import recovery_write_envelope
    recovery_write_envelope(
        main_bean=main_bean,
        files_to_snapshot=files_to_snapshot,
        write_fn=write_fn,
    )


@router.post("/setup/stamp-version")
def setup_stamp_version(
    request: Request,
    settings: Settings = Depends(get_settings),
):
    """Append the `custom "lamella-ledger-version" "<LATEST>"` directive to
    main.bean so detect_ledger_state reports READY instead of
    NEEDS_VERSION_STAMP. Idempotent: skips if a stamp already
    exists. Bean-check runs against a pre-write baseline; on any
    new error we restore main.bean byte-for-byte from snapshot.

    Pilot handler for the Phase 1.2 snapshot-envelope retrofit.
    Mirrors the pattern used by ``transform/*`` passes via
    ``transform/_files``.
    """
    from lamella.core.ledger_writer import BeanCheckError
    from lamella.core.transform._files import (
        baseline as bean_check_baseline,
        run_check_with_rollback,
        snapshot,
    )

    main = settings.ledger_main
    if not main.exists():
        return RedirectResponse("/setup?error=no-main-bean", status_code=303)
    original = main.read_text(encoding="utf-8")
    if 'custom "lamella-ledger-version"' in original:
        _refresh_detection(request, settings)
        return RedirectResponse(
            "/setup?info=ledger-already-stamped", status_code=303,
        )

    # Capture baseline + snapshot BEFORE the write. The previous code
    # captured the baseline after writing, which neutered the guard:
    # a broken stamp would produce errors, baseline would carry those
    # same errors, and `new_errors = curr - base` was always empty.
    snaps = [snapshot(main)]
    pre_baseline = bean_check_baseline(main, run_check=True)

    try:
        main.write_text(original + _stamp_directive_text(), encoding="utf-8")
    except Exception as exc:  # noqa: BLE001
        for snap in snaps:
            snap.restore()
        log.exception("stamp-version write failed")
        return RedirectResponse(
            f"/setup?error={type(exc).__name__}", status_code=303,
        )

    try:
        run_check_with_rollback(main, pre_baseline, snaps, run_check=True)
    except BeanCheckError as exc:
        # Rollback has already happened inside run_check_with_rollback.
        return RedirectResponse(
            f"/setup?error=bean-check-{exc}", status_code=303,
        )
    except Exception as exc:  # noqa: BLE001
        for snap in snaps:
            snap.restore()
        log.exception("stamp-version post-check failed")
        return RedirectResponse(
            f"/setup?error={type(exc).__name__}", status_code=303,
        )

    from lamella.core.bootstrap.detection import LATEST_LEDGER_VERSION
    _refresh_detection(request, settings)
    return RedirectResponse(
        f"/setup?info=ledger-stamped-as-v{LATEST_LEDGER_VERSION}",
        status_code=303,
    )


@router.post("/setup/fix-orphan-overrides")
def setup_fix_orphan_overrides(
    request: Request,
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Recovery: delete override blocks whose lamella-override-of hash
    doesn't match any non-override transaction in the ledger.

    Background: in an earlier buggy code path, the migrate flow walked
    every posting on an orphan account INCLUDING previously-written
    override txns. Each override txn has its own transaction hash, so
    the next migrate cycle wrote override-of-override blocks. The
    override writer's replace_existing=True only wipes blocks sharing
    a txn_hash, so these accumulated rather than being deduplicated.

    Visible symptom: orphan's net balance after full migration is not
    zero, and the target account shows double (or more) the correct
    amount. Every migrate click inflates further.

    This handler walks every block in connector_overrides.bean, checks
    whether its lamella-override-of hash matches an ORIGINAL (non-
    override) transaction in the ledger, and removes the block if
    not. Writes a .preclean.bak before editing. Idempotent.
    """
    import re as _re
    from beancount.core.data import Transaction
    from lamella.core.beancount_io.txn_hash import txn_hash

    overrides_path = settings.connector_overrides_path
    if not overrides_path.exists():
        return RedirectResponse("/setup?info=no-overrides-file", status_code=303)

    # Build the set of LIVE original-txn hashes (transactions that
    # don't carry #lamella-override). Any override block referencing a
    # hash not in this set is stale and gets dropped.
    from lamella.features.setup.posting_counts import is_override_txn
    valid_hashes: set[str] = set()
    try:
        for e in reader.load().entries:
            if not isinstance(e, Transaction):
                continue
            if is_override_txn(e):
                continue
            valid_hashes.add(txn_hash(e))
    except Exception as exc:  # noqa: BLE001
        log.warning("load for fix-orphan-overrides failed: %s", exc)
        return RedirectResponse("/setup?error=load-failed", status_code=303)

    original = overrides_path.read_text(encoding="utf-8")
    # Split the file into override blocks. A block starts with a
    # YYYY-MM-DD line and contains a lamella-override-of metadata line.
    # Anything between blocks (headers, blank lines) we preserve.
    block_re = _re.compile(
        r"(?m)^(\d{4}-\d{2}-\d{2}[^\n]*\n(?:[ \t]+[^\n]*\n)+)"
    )
    out_parts: list[str] = []
    removed = 0
    last_end = 0
    for m in block_re.finditer(original):
        out_parts.append(original[last_end:m.start()])
        block = m.group(1)
        last_end = m.end()
        # Accept legacy bcg-override-of for the cutover window.
        of_match = _re.search(r'(?:lamella|bcg)-override-of:\s*"([a-f0-9]+)"', block)
        if of_match is None:
            # Not an override block, keep as-is
            out_parts.append(block)
            continue
        h = of_match.group(1)
        if h in valid_hashes:
            out_parts.append(block)
        else:
            removed += 1  # drop
    out_parts.append(original[last_end:])
    if removed == 0:
        return RedirectResponse(
            "/setup?info=no-orphan-overrides-found", status_code=303,
        )

    # Keep the .preclean.bak on-disk artifact for post-hoc diffing;
    # survives rollback as a belt-and-suspenders escape hatch.
    backup_path = overrides_path.with_suffix(
        overrides_path.suffix + ".preclean.bak",
    )
    backup_path.write_bytes(original.encode("utf-8"))
    new_text = "".join(out_parts)
    # Compress 3+ consecutive blank lines to 2 (from block removal gaps)
    new_text = _re.sub(r"\n{3,}", "\n\n", new_text)

    # Recovery envelope: same helper as /setup/fix-duplicate-closes.
    # Line-deleting edit on an already-unparseable ledger — load_file
    # + fatal-error subset comparison is the right guard here;
    # run_bean_check_vs_baseline's line-keyed diff would false-
    # positive on every downstream error. See
    # _recovery_write_envelope's docstring for the full reasoning.
    from lamella.core.ledger_writer import BeanCheckError
    try:
        _recovery_write_envelope(
            main_bean=settings.ledger_main,
            files_to_snapshot=[overrides_path, settings.ledger_main],
            write_fn=lambda: overrides_path.write_text(
                new_text, encoding="utf-8",
            ),
        )
    except BeanCheckError as exc:
        return RedirectResponse(
            f"/setup?error=bean-check-after-cleanup-{exc}",
            status_code=303,
        )
    except Exception as exc:  # noqa: BLE001
        log.exception("fix-orphan-overrides write failed")
        return RedirectResponse(
            f"/setup?error={type(exc).__name__}",
            status_code=303,
        )
    reader.invalidate()
    return RedirectResponse(
        f"/setup?info=removed-{removed}-orphan-override-blocks",
        status_code=303,
    )


@router.post("/setup/fix-duplicate-closes")
def setup_fix_duplicate_closes(
    request: Request,
    settings: Settings = Depends(get_settings),
):
    """Emergency-recovery: dedupe identical 'YYYY-MM-DD close <path>'
    lines in connector_accounts.bean. This is reached from the /setup
    fallback page when detection reports the ledger is unparseable
    with 'Duplicate close directive' errors — typically because an
    earlier Close-account flow wrote a second Close on top of an
    existing one.

    Idempotent by design: collapses every exact-match duplicate line
    to one, preserves order. Writes the dedupe'd file back, then
    re-runs detection so subsequent routes unblock.
    """
    import re as _re
    accounts_path = settings.connector_accounts_path
    if not accounts_path.exists():
        return RedirectResponse("/setup?error=no-connector-accounts", status_code=303)
    original = accounts_path.read_text(encoding="utf-8")
    lines = original.splitlines(keepends=True)
    close_re = _re.compile(r'^\d{4}-\d{2}-\d{2}\s+close\s+\S+\s*$')
    seen_closes: set[str] = set()
    out_lines: list[str] = []
    removed = 0
    for line in lines:
        if close_re.match(line.rstrip("\r\n")):
            key = line.strip()
            if key in seen_closes:
                removed += 1
                continue
            seen_closes.add(key)
        out_lines.append(line)
    if removed == 0:
        return RedirectResponse("/setup?info=no-duplicates-found", status_code=303)
    # Backup before writing so a bad dedupe leaves a recoverable copy
    # on disk independent of the in-memory snapshot handled by the
    # envelope. The .predup.bak survives across rollbacks so an
    # operator can diff pre-vs-post at the filesystem level.
    backup_path = accounts_path.with_suffix(accounts_path.suffix + ".predup.bak")
    backup_path.write_bytes(original.encode("utf-8"))

    # Phase 1.2 recovery envelope: snapshot + parse-check via
    # load_file, restore on any new fatal error. See
    # _recovery_write_envelope's docstring for why this path doesn't
    # use run_bean_check_vs_baseline.
    from lamella.core.ledger_writer import BeanCheckError
    try:
        _recovery_write_envelope(
            main_bean=settings.ledger_main,
            files_to_snapshot=[accounts_path, settings.ledger_main],
            write_fn=lambda: accounts_path.write_text(
                "".join(out_lines), encoding="utf-8",
            ),
        )
    except BeanCheckError as exc:
        return RedirectResponse(
            f"/setup?error=bean-check-{exc}", status_code=303,
        )
    except Exception as exc:  # noqa: BLE001
        log.exception("fix-duplicate-closes write failed")
        return RedirectResponse(
            f"/setup?error={type(exc).__name__}", status_code=303,
        )

    # Re-run detection so the middleware stops redirecting to /setup.
    try:
        _refresh_detection(request, settings)
    except Exception:  # noqa: BLE001
        pass
    return RedirectResponse(
        f"/setup?info=removed-{removed}-duplicate-close-directives",
        status_code=303,
    )


@router.post("/setup/normalize-txn-identity")
def setup_normalize_txn_identity(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Recovery: bulk-normalize transaction identity meta on disk.

    Walks every ``.bean`` file under ``ledger_dir``, mints
    ``lamella-txn-id`` on every transaction lacking one, migrates
    legacy txn-level source keys (``lamella-simplefin-id`` /
    bare ``simplefin-id`` / ``lamella-import-txn-id``) down to the
    source-side posting as paired indexed source meta, drops retired
    keys (``lamella-import-id`` / ``lamella-import-source``), and
    backfills ``ai_decisions.input_ref`` from staging ids to lineage.

    The system already self-heals as the user touches transactions
    (see ``rewrite/txn_inplace`` on-touch normalization) and the
    read-side compat in ``_legacy_meta`` makes the legacy on-disk
    shape transparent forever. This action exists for users who
    want to clean up disk content all at once instead of letting it
    converge over time.

    Discipline: per-file snapshot under ``ledger_dir/.pre-normalize-<ISO>/``
    before any byte changes; bean-check vs baseline; restore from
    snapshot on any new error.
    """
    from lamella.core.transform.normalize_txn_identity import run as run_normalize

    try:
        result = run_normalize(
            settings, apply=True, run_check=True, db_conn=conn,
        )
    except Exception as exc:  # noqa: BLE001
        log.exception("normalize-txn-identity failed")
        return RedirectResponse(
            f"/setup/recovery?error={type(exc).__name__}", status_code=303,
        )

    if result.bean_check_error:
        return RedirectResponse(
            f"/setup/recovery?error=bean-check-{result.bean_check_error[:80]}",
            status_code=303,
        )

    reader.invalidate()
    if result.files_changed == 0 and result.ai_decisions_backfilled == 0:
        return RedirectResponse(
            "/setup/recovery?info=identity-already-normalized",
            status_code=303,
        )
    return RedirectResponse(
        (
            f"/setup/recovery?info=normalized-files={result.files_changed}"
            f"-txns={result.txns_changed}"
            f"-lineage_minted={result.lineage_minted}"
            f"-ai_backfilled={result.ai_decisions_backfilled}"
        ),
        status_code=303,
    )


@router.get("/setup/accounts", response_class=HTMLResponse)
def setup_accounts_page(
    request: Request,
    add: str | None = None,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    """Streamlined account-labeling editor for the setup flow.

    Shows every bank/card/loan account with kind, entity_slug,
    institution, last_four. Unlabeled rows surface at the top. Each
    row's form HTMX-saves in place so the user edits and moves
    without losing their position.

    Phase 4 of /setup/recovery: ``?add=account`` opens an + Add
    modal in-page for creating a new account from scratch (rare —
    most accounts arrive via import). The modal coexists with the
    label-needed inline forms; the two surfaces serve different
    jobs (bulk-labeling existing rows vs. one-off creation).
    """
    from lamella.core.registry.service import ACCOUNT_KINDS
    open_modal = "account" if add == "account" else None

    rows = [
        dict(r) for r in conn.execute(
            """
            SELECT account_path, display_name, kind, kind_source, institution,
                   last_four, entity_slug, simplefin_account_id
              FROM accounts_meta
             WHERE closed_on IS NULL
             ORDER BY
               CASE WHEN (kind IS NULL OR kind='' OR entity_slug IS NULL OR entity_slug='') THEN 0
                    WHEN kind_source = 'sibling' THEN 1
                    ELSE 2 END,
               account_path
            """
        ).fetchall()
    ]
    def _is_user_account(path: str) -> bool:
        if not path.startswith(("Assets:", "Liabilities:")):
            return False
        skip = {"Transfers", "FIXME", "OpeningBalances", "DueFrom", "DueTo"}
        return not any(seg in skip for seg in path.split(":"))

    user_rows = [r for r in rows if _is_user_account(r["account_path"])]

    # Posting counts per account from the live ledger so users see
    # which accounts are real (used) vs stale meta-only entries.
    # Use the full "unmigrated only" filter — an account that's been
    # fully migrated via overrides should report 0 postings so its
    # row counts as unused. Shared predicate: setup.posting_counts.
    posting_counts: dict[str, int] = {}
    try:
        reader = request.app.state.ledger_reader
        from lamella.features.setup.posting_counts import (
            unmigrated_postings_by_account,
        )
        target_paths = {r["account_path"] for r in user_rows}
        posting_counts = unmigrated_postings_by_account(
            reader.load().entries, target_paths,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("setup-accounts posting count load failed: %s", exc)

    # Phase 2: rationale strings for sibling-derived kinds. Built
    # on render so the DB doesn't need to store the hint text.
    from lamella.core.registry.discovery import sibling_hint_for
    for r in user_rows:
        r["posting_count"] = posting_counts.get(r["account_path"], 0)
        r["has_simplefin"] = bool((r.get("simplefin_account_id") or "").strip())
        if r.get("kind_source") == "sibling":
            r["sibling_hint"] = sibling_hint_for(conn, r["account_path"])
        else:
            r["sibling_hint"] = None

    def _needs_label(r: dict) -> bool:
        return not (
            (r["kind"] or "").strip() and (r["entity_slug"] or "").strip()
        )

    entities = [
        r["slug"] for r in conn.execute(
            "SELECT slug FROM entities WHERE is_active = 1 ORDER BY slug"
        ).fetchall()
    ]
    needs_count = sum(1 for r in user_rows if _needs_label(r))
    all_labeled = needs_count == 0 and len(user_rows) > 0
    unused_count = sum(1 for r in user_rows if r["posting_count"] == 0)

    return request.app.state.templates.TemplateResponse(
        request, "setup_accounts.html",
        {
            "accounts": user_rows,
            "account_kinds": ACCOUNT_KINDS,
            "entities": entities,
            "needs_count": needs_count,
            "total": len(user_rows),
            "all_labeled": all_labeled,
            "unused_count": unused_count,
            "open_modal": open_modal,
            "field_errors": {},
            "form_values": {},
            "step_meta": (
                {"id": "accounts", "label": "Accounts",
                 "url": "/setup/accounts"},
            ),
            "current_step": "accounts",
            "step_index": 0,
        },
    )


def _apply_account_kind_change(
    *,
    conn,
    settings: Settings,
    reader: LedgerReader,
    account_path: str,
    kind: str | None,
    entity_slug: str | None,
    institution: str | None,
    last_four: str | None,
    prior_kind: str | None,
) -> None:
    """Apply a kind=NULL→kind=X transition (or any account-meta
    update) to one account, atomically.

    Phase 4 / Phase 8 spec: this is the shared helper both the
    inline label-needed form (``setup_account_save``) and the
    modal add path (``setup_account_add``) call so the
    auto-heal-at-classify pipeline runs on both. Multi-write
    chain wrapped in ``with_bean_snapshot()`` — partial state
    mid-chain is the failure mode (kind directive written but
    companion scaffold failed mid-write would leave the ledger
    inconsistent).

    Declared snapshot path set covers the three files this chain
    can write to. ``ensure_companions()`` itself only writes to
    ``connector_accounts.bean``; ``account_kind`` and
    ``account_meta`` directives go to ``connector_config.bean``;
    main.bean is included as the bean-check target.
    """
    from lamella.features.recovery.snapshot import with_bean_snapshot
    from lamella.core.registry.account_meta_writer import (
        append_account_meta_directive,
    )
    from lamella.core.registry.companion_accounts import ensure_companions
    from lamella.core.registry.kind_writer import append_account_kind
    from lamella.core.registry.service import update_account

    declared_paths = [
        Path(settings.connector_config_path),
        Path(settings.connector_accounts_path),
        Path(settings.ledger_main),
    ]

    with with_bean_snapshot(declared_paths) as snap:
        update_account(
            conn, account_path,
            kind=kind, entity_slug=entity_slug,
            institution=institution, last_four=last_four,
        )
        if kind and kind != prior_kind:
            try:
                append_account_kind(
                    connector_config=settings.connector_config_path,
                    main_bean=settings.ledger_main,
                    account_path=account_path, kind=kind,
                )
                snap.add_touched(Path(settings.connector_config_path))
            except Exception as exc:  # noqa: BLE001
                log.warning("account-kind directive write failed: %s", exc)
        try:
            append_account_meta_directive(
                connector_config=settings.connector_config_path,
                main_bean=settings.ledger_main,
                account_path=account_path,
                institution=institution,
                last_four=last_four,
                entity_slug=entity_slug,
            )
            snap.add_touched(Path(settings.connector_config_path))
        except Exception as exc:  # noqa: BLE001
            log.warning("account-meta directive write failed: %s", exc)
        if kind and entity_slug:
            try:
                ensure_companions(
                    conn=conn, settings=settings, reader=reader,
                    account_path=account_path,
                )
                snap.add_touched(Path(settings.connector_accounts_path))
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "companion scaffold failed for %s: %s", account_path, exc,
                )


def _re_render_account_modal(
    request: Request, *,
    settings: Settings, conn,
    field_errors: dict[str, str], form_values: dict[str, str],
):
    """Re-render setup_accounts.html with the modal still open and
    typed values preserved. Mirrors the wizard's pattern from
    routes/setup_wizard.py:1952-1960."""
    from lamella.core.registry.service import ACCOUNT_KINDS
    rows = [
        dict(r) for r in conn.execute(
            """
            SELECT account_path, display_name, kind, kind_source, institution,
                   last_four, entity_slug, simplefin_account_id
              FROM accounts_meta
             WHERE closed_on IS NULL
             ORDER BY account_path
            """
        ).fetchall()
    ]
    user_rows = [
        r for r in rows
        if r["account_path"].startswith(("Assets:", "Liabilities:"))
        and not any(seg in r["account_path"].split(":") for seg in (
            "Transfers", "FIXME", "OpeningBalances", "DueFrom", "DueTo",
        ))
    ]
    for r in user_rows:
        r["posting_count"] = 0
        r["has_simplefin"] = bool((r.get("simplefin_account_id") or "").strip())
        r["sibling_hint"] = None
    entities = [
        e["slug"] for e in conn.execute(
            "SELECT slug FROM entities WHERE is_active = 1 ORDER BY slug"
        ).fetchall()
    ]
    return request.app.state.templates.TemplateResponse(
        request, "setup_accounts.html",
        {
            "accounts": user_rows,
            "account_kinds": ACCOUNT_KINDS,
            "entities": entities,
            "needs_count": 0,
            "total": len(user_rows),
            "all_labeled": False,
            "unused_count": 0,
            "open_modal": "account",
            "field_errors": field_errors,
            "form_values": form_values,
            "step_meta": (
                {"id": "accounts", "label": "Accounts",
                 "url": "/setup/accounts"},
            ),
            "current_step": "accounts",
            "step_index": 0,
        },
        status_code=400,
    )


@router.post("/setup/accounts/add", response_class=HTMLResponse)
async def setup_account_add(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Phase 4 of /setup/recovery: create a new account from the
    + Add modal. Required: account_path, kind, entity_slug.
    Optional: institution, last_four, opening_date.

    Save-path discipline:
      1. Validate the typed account_path via the same guards
         staging_review uses (parses valid, ≥3 segments, parent is
         opened or is a prefix of some opened account).
      2. INSERT accounts_meta row with kind + entity + institution
         + last_four already populated.
      3. Open directive in connector_accounts.bean (so postings
         can land on the new account immediately).
      4. Run the shared `_apply_account_kind_change` helper —
         same code path the inline label-needed forms call. This
         is the auto-heal-at-classify equivalent for the modal:
         the helper wraps account_meta directive + ensure_companions
         in a single `with_bean_snapshot()` envelope.
      5. Sibling-inference re-run on the freshly inserted row so
         the user sees the ★ suggested badge immediately if peers
         warrant it (Phase 2 contract). The user's next action is
         "confirm or override," not "wait for the next page load."

    Validation failure re-renders the modal with field_errors +
    form_values."""
    from fastapi.responses import RedirectResponse
    from datetime import date as _date
    from beancount.core import account as account_lib
    from lamella.core.registry.discovery import infer_kinds_by_sibling
    from lamella.core.registry.service import ACCOUNT_KINDS

    form = await request.form()
    account_path = (form.get("account_path") or "").strip()
    kind = (form.get("kind") or "").strip()
    entity_slug = (form.get("entity_slug") or "").strip()
    institution = (form.get("institution") or "").strip()
    last_four = (form.get("last_four") or "").strip()
    opening_date = (form.get("opening_date") or "").strip()

    field_errors: dict[str, str] = {}
    if not account_path:
        field_errors["account_path"] = "Required."
    elif not account_lib.is_valid(account_path):
        field_errors["account_path"] = (
            f"Account path {account_path!r} is not valid Beancount syntax. "
            "Each segment must start with an uppercase letter or digit."
        )
    elif len(account_path.split(":")) < 3:
        field_errors["account_path"] = (
            "Path must have at least 3 segments "
            "(Root:Entity:Leaf — e.g. Assets:Personal:Checking)."
        )
    elif not account_path.startswith(("Assets:", "Liabilities:")):
        field_errors["account_path"] = (
            "Modal-created accounts must be Assets:* or Liabilities:* — "
            "Income/Expenses/Equity accounts are scaffolded by other flows."
        )
    if not kind:
        field_errors["kind"] = "Pick a kind."
    elif kind not in ACCOUNT_KINDS:
        field_errors["kind"] = f"Unknown kind: {kind!r}."
    if not entity_slug:
        field_errors["entity_slug"] = "Required."
    elif not conn.execute(
        "SELECT 1 FROM entities WHERE slug = ? AND is_active = 1",
        (entity_slug,),
    ).fetchone():
        field_errors["entity_slug"] = (
            f"No active entity {entity_slug!r}. Pick an existing one or "
            "register the entity at /setup/entities first."
        )

    open_date_obj: _date | None = None
    if opening_date:
        try:
            open_date_obj = _date.fromisoformat(opening_date)
        except ValueError:
            field_errors["opening_date"] = "Use YYYY-MM-DD format."

    form_values_payload = {
        "account_path": account_path, "kind": kind,
        "entity_slug": entity_slug, "institution": institution,
        "last_four": last_four, "opening_date": opening_date,
    }

    if not field_errors:
        # Slug-collision check.
        existing = conn.execute(
            "SELECT account_path FROM accounts_meta WHERE account_path = ?",
            (account_path,),
        ).fetchone()
        if existing is not None:
            field_errors["account_path"] = (
                f"Account {account_path!r} already exists. Edit the row "
                "in the table below instead, or pick a different path."
            )

    if field_errors:
        return _re_render_account_modal(
            request, settings=settings, conn=conn,
            field_errors=field_errors, form_values=form_values_payload,
        )

    # 1. INSERT accounts_meta. Mirror existing accounts_admin INSERT
    # shape — kind + meta in one row, seeded_from_ledger=0 (user
    # created), kind_source=NULL (user-confirmed at creation time).
    display_name = account_path.split(":")[-1]
    open_iso = (open_date_obj or _date(1900, 1, 1)).isoformat()
    conn.execute(
        """
        INSERT INTO accounts_meta
            (account_path, display_name, kind, institution, last_four,
             entity_slug, opened_on, seeded_from_ledger, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, 0, CURRENT_TIMESTAMP)
        """,
        (
            account_path, display_name, kind,
            institution or None, last_four or None,
            entity_slug, open_iso,
        ),
    )

    # 2. Write the Open directive into connector_accounts.bean. Use
    # AccountsWriter so existing dedup + bean-check + restore
    # behaviors apply.
    try:
        from lamella.core.registry.accounts_writer import AccountsWriter
        existing_paths = {
            getattr(e, "account", None)
            for e in reader.load().entries
            if hasattr(e, "account")
        }
        writer = AccountsWriter(
            main_bean=settings.ledger_main,
            connector_accounts=settings.connector_accounts_path,
        )
        writer.write_opens(
            [account_path],
            opened_on=open_date_obj or _date(1900, 1, 1),
            comment=f"manually added via /setup/accounts ({entity_slug})",
            existing_paths=existing_paths,
        )
        reader.invalidate()
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "open-directive write failed for new account %s: %s",
            account_path, exc,
        )

    # 3. Shared kind-change helper — handles account_kind +
    # account_meta directives + ensure_companions inside a single
    # snapshot envelope. Same code path the inline label-needed
    # forms call.
    _apply_account_kind_change(
        conn=conn, settings=settings, reader=reader,
        account_path=account_path,
        kind=kind, entity_slug=entity_slug,
        institution=institution or None, last_four=last_four or None,
        prior_kind=None,
    )

    # 4. Sibling-inference re-run so the new row renders with the
    # ★ suggested badge already applied if peers warrant it. Per
    # Phase 2 contract, infer_kinds_by_sibling only updates rows
    # where kind IS NULL — the new row already has kind set, so
    # this fires for *future* sibling-derivable peers without
    # touching the just-saved row. For consistency with the wizard
    # shape we still run it.
    try:
        infer_kinds_by_sibling(conn)
    except Exception as exc:  # noqa: BLE001
        log.warning("post-add sibling inference failed: %s", exc)

    request.app.state.setup_required_complete = False
    return RedirectResponse(
        f"/setup/accounts?added={account_path}", status_code=303,
    )


@router.post("/setup/accounts/save", response_class=HTMLResponse)
async def setup_account_save(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Save one account's setup fields. Form shape:
    account_path, kind, entity_slug, institution, last_four.
    HTMX-aware: returns the updated row partial; else redirects."""
    from fastapi.responses import RedirectResponse
    form = await request.form()
    path = (form.get("account_path") or "").strip()
    if not path:
        # HTMX-callable: per ADR-0037 + routes/CLAUDE.md, never return
        # a plain 30x from an HTMX-targeted handler — fetch follows it
        # and the destination page outerHTML-swaps the action's target.
        return _hx_aware_error_redirect(request, "/setup/accounts", "no-path")
    kind = (form.get("kind") or "").strip() or None
    entity_slug = (form.get("entity_slug") or "").strip() or None
    institution = (form.get("institution") or "").strip() or None
    last_four = (form.get("last_four") or "").strip() or None
    prior = conn.execute(
        "SELECT kind FROM accounts_meta WHERE account_path = ?", (path,),
    ).fetchone()
    prior_kind = prior["kind"] if prior else None
    _apply_account_kind_change(
        conn=conn, settings=settings, reader=reader,
        account_path=path,
        kind=kind, entity_slug=entity_slug,
        institution=institution, last_four=last_four,
        prior_kind=prior_kind,
    )
    request.app.state.setup_required_complete = False
    if request.headers.get("hx-request", "").lower() == "true":
        banner_html = _render_accounts_banner(request, conn)
        return HTMLResponse(
            f'<div hx-swap-oob="innerHTML:#setup-accounts-banner">{banner_html}</div>'
        )
    return RedirectResponse("/setup/accounts", status_code=303)


def _render_accounts_banner(request: Request, conn) -> str:
    """Recompute the setup-accounts banner values + render just the
    body partial. Used as an hx-swap-oob target from the save handler
    so the stale "4 of 79 missing" count updates without forcing a
    full-page reload. Mirrors the computation in
    ``setup_accounts_page``."""
    try:
        rows = [
            dict(r) for r in conn.execute(
                "SELECT account_path, kind, entity_slug, simplefin_account_id "
                "  FROM accounts_meta WHERE closed_on IS NULL"
            ).fetchall()
        ]
    except Exception:  # noqa: BLE001
        return ""

    def _is_user_account(path: str) -> bool:
        if not path.startswith(("Assets:", "Liabilities:")):
            return False
        skip = {"Transfers", "FIXME", "OpeningBalances", "DueFrom", "DueTo"}
        return not any(seg in skip for seg in path.split(":"))

    user_rows = [r for r in rows if _is_user_account(r["account_path"])]
    # Posting counts — needed for unused_count. Full "unmigrated only"
    # filter via the shared predicate; matches the setup_accounts_page
    # computation so the OOB banner stays consistent with the page.
    posting_counts: dict[str, int] = {}
    try:
        reader = request.app.state.ledger_reader
        from lamella.features.setup.posting_counts import (
            unmigrated_postings_by_account,
        )
        target_paths = {r["account_path"] for r in user_rows}
        posting_counts = unmigrated_postings_by_account(
            reader.load().entries, target_paths,
        )
    except Exception:  # noqa: BLE001
        pass

    needs_count = sum(
        1 for r in user_rows
        if not ((r["kind"] or "").strip() and (r["entity_slug"] or "").strip())
    )
    unused_count = sum(
        1 for r in user_rows if posting_counts.get(r["account_path"], 0) == 0
    )
    total = len(user_rows)
    all_labeled = needs_count == 0 and total > 0
    return request.app.state.templates.env.get_template(
        "partials/_setup_accounts_banner_body.html"
    ).render(
        request=request,
        needs_count=needs_count,
        total=total,
        unused_count=unused_count,
        all_labeled=all_labeled,
    )


@router.post("/setup/accounts/close", response_class=HTMLResponse)
async def setup_account_close(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Close an unused path-owned account from the /setup/accounts row.
    Safety checks:
      - 0 postings on the live ledger
      - an explicit Open directive exists (else the accounts_meta row
        is just stale cache — drop it without a Close write)

    On success, the Close directive is appended to
    ``connector_accounts.bean`` and bean-check runs vs baseline.
    HTMX: returns empty 200 so the row disappears from the table.
    """
    from datetime import date as _date_t
    from beancount.core.data import Transaction, Open, Close
    from lamella.core.ledger_writer import (
        BeanCheckError, capture_bean_check, run_bean_check_vs_baseline,
    )
    from lamella.features.setup.posting_counts import (
        already_migrated_hashes, is_override_txn,
    )
    from lamella.core.beancount_io.txn_hash import txn_hash as _tx_hash
    form = await request.form()
    path = (form.get("account_path") or "").strip()
    if not path:
        # HTMX-callable per ADR-0037 + routes/CLAUDE.md.
        return _hx_aware_error_redirect(request, "/setup/accounts", "no-path")

    # Verify 0 postings and whether an explicit Open/Close exists.
    try:
        load = reader.load()
    except Exception as exc:  # noqa: BLE001
        log.warning("ledger load failed during account close: %s", exc)
        return _hx_aware_error_redirect(
            request, "/setup/accounts", "load-failed",
        )
    # Count ONLY real (non-override, non-already-migrated) postings.
    # An account with 49 originals that have all been migrated via
    # overrides would otherwise count as 49+49=98 and the close
    # would refuse with "still has 98 postings" — which is what the
    # user reported at /setup/accounts?error=account-still-has-98-postings.
    # Use the same filter as the /setup/vehicles orphan counter so
    # UI and server agree. Shared predicate: setup.posting_counts.
    already_migrated = already_migrated_hashes(load.entries)
    posting_count = 0
    has_open = False
    already_closed = False
    for e in load.entries:
        if isinstance(e, Transaction):
            if is_override_txn(e):
                continue
            if _tx_hash(e) in already_migrated:
                continue
            for p in e.postings or ():
                if p.account == path:
                    posting_count += 1
                    break
        elif isinstance(e, Open) and e.account == path:
            has_open = True
        elif isinstance(e, Close) and e.account == path:
            already_closed = True
    if posting_count > 0:
        return _hx_aware_error_redirect(
            request, "/setup/accounts",
            f"account-still-has-{posting_count}-postings",
        )

    today = _date_t.today().isoformat()
    if already_closed:
        # Ledger already has a Close for this account — just sync the
        # cache, never write a duplicate (bean-check would reject it
        # and the whole ledger goes unparseable).
        conn.execute(
            "UPDATE accounts_meta SET closed_on = COALESCE(closed_on, ?) "
            "WHERE account_path = ?",
            (today, path),
        )
    elif has_open:
        # Write a Close directive and bean-check.
        accounts_path = settings.connector_accounts_path
        accounts_path.parent.mkdir(parents=True, exist_ok=True)
        if not accounts_path.exists():
            accounts_path.write_text(
                "; connector_accounts.bean — managed by Lamella.\n",
                encoding="utf-8",
            )
        backup_accounts = accounts_path.read_bytes()
        backup_main = settings.ledger_main.read_bytes()
        # Baseline MUST be captured BEFORE the write. A post-write
        # capture neuters the guard: new errors introduced by the
        # write also land in the "baseline" and the diff comes up
        # empty. (Same pattern as Phase 1.2 pilot on stamp-version.)
        _baseline_count, baseline_output = capture_bean_check(
            settings.ledger_main
        )
        new_text = (
            accounts_path.read_text(encoding="utf-8").rstrip()
            + f"\n\n{today} close {path}\n"
        )
        try:
            accounts_path.write_text(new_text, encoding="utf-8")
            run_bean_check_vs_baseline(settings.ledger_main, baseline_output)
        except BeanCheckError as exc:
            accounts_path.write_bytes(backup_accounts)
            settings.ledger_main.write_bytes(backup_main)
            return _hx_aware_error_redirect(
                request, "/setup/accounts", f"bean-check-{exc}",
            )
        except Exception as exc:  # noqa: BLE001
            accounts_path.write_bytes(backup_accounts)
            settings.ledger_main.write_bytes(backup_main)
            log.exception("close account %s failed", path)
            return _hx_aware_error_redirect(
                request, "/setup/accounts", type(exc).__name__,
            )
        conn.execute(
            "UPDATE accounts_meta SET closed_on = ? WHERE account_path = ?",
            (today, path),
        )
        reader.invalidate()
    else:
        # No Open in the ledger — the accounts_meta row is stale cache.
        # Drop it directly; no ledger write needed.
        conn.execute(
            "DELETE FROM accounts_meta WHERE account_path = ?", (path,),
        )

    is_htmx = request.headers.get("hx-request", "").lower() == "true"
    if is_htmx:
        # Row is gone — empty response makes HTMX remove the tr.
        return HTMLResponse("")
    return RedirectResponse(
        f"/setup/accounts?closed={path}", status_code=303,
    )


@router.get("/setup/charts", response_class=HTMLResponse)
def setup_charts_page(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Streamlined per-entity chart-scaffolding check.

    For each entity whose tax_schedule / entity_type maps to a yaml
    chart (Schedule C / F / Personal), show missing-categories count
    and a one-click "Scaffold missing" per entity.
    """
    from lamella.core.registry.service import (
        load_categories_yaml_for_entity, scaffold_paths_for_entity,
    )
    from lamella.features.setup.posting_counts import open_paths as _open_paths
    entries = list(reader.load().entries)
    open_paths_set = _open_paths(entries)
    entity_rows = conn.execute(
        "SELECT slug, entity_type, tax_schedule, display_name "
        "FROM entities WHERE is_active = 1 ORDER BY slug"
    ).fetchall()
    display = []
    for row in entity_rows:
        slug = row["slug"]
        yaml_data = load_categories_yaml_for_entity(settings, row)
        if not yaml_data:
            display.append({
                "slug": slug,
                "display_name": row["display_name"],
                "schedule": "(none)",
                "applicable": False,
                "total": 0,
                "missing": 0,
                "missing_paths": [],
            })
            continue
        candidates = scaffold_paths_for_entity(yaml_data, slug)
        missing = [c["path"] for c in candidates if c["path"] not in open_paths_set]
        display.append({
            "slug": slug,
            "display_name": row["display_name"],
            "schedule": (
                "Schedule C" if (row["tax_schedule"] or "").upper() == "C"
                else "Schedule F" if (row["tax_schedule"] or "").upper() == "F"
                else "Personal / Schedule A"
            ),
            "applicable": True,
            "total": len(candidates),
            "missing": len(missing),
            "missing_paths": missing[:10],
        })
    any_missing = sum(d["missing"] for d in display if d["applicable"])
    return request.app.state.templates.TemplateResponse(
        request, "setup_charts.html",
        {
            "entities_chart": display,
            "any_missing": any_missing,
        },
    )


@router.post("/setup/charts/{slug}/scaffold")
def setup_chart_scaffold(
    slug: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Open every missing chart category for one entity."""
    from fastapi.responses import RedirectResponse
    from beancount.core.data import Open
    from lamella.core.registry.service import (
        load_categories_yaml_for_entity, scaffold_paths_for_entity,
    )
    from lamella.core.registry.accounts_writer import AccountsWriter
    from lamella.core.ledger_writer import BeanCheckError

    row = conn.execute(
        "SELECT slug, entity_type, tax_schedule, display_name "
        "FROM entities WHERE slug = ?", (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"entity {slug!r} not found")
    yaml_data = load_categories_yaml_for_entity(settings, row)
    if not yaml_data:
        return _hx_aware_error_redirect(
            request, "/setup/charts", f"no-chart-for-{slug}",
        )
    entries = list(reader.load().entries)
    # Opens-only here (NOT Opens-minus-Closes) — intentional divergence
    # from setup_charts_page's display predicate. A Close'd chart
    # account has an Open in the ledger's history, so Opens-only
    # includes it in ``existing`` and the scaffold button does NOT
    # rewrite an Open on top. Reopening a Close'd account is a
    # deliberate action that belongs in a different flow, per the
    # Phase 1.3 decision. AccountsWriter.write_opens filters the same
    # way via its own ``existing_paths`` param. Result: the page says
    # "missing" but clicking Scaffold is a deliberate no-op, not a
    # silent reopen.
    existing = {e.account for e in entries if isinstance(e, Open)}
    candidates = scaffold_paths_for_entity(yaml_data, slug)
    missing = [c["path"] for c in candidates if c["path"] not in existing]
    if not missing:
        # HTMX-callable per ADR-0037 + routes/CLAUDE.md. Use _htmx-aware
        # redirect so the shim does a client-side nav instead of fetch
        # auto-following the 303 and outerHTML-swapping the destination.
        from urllib.parse import quote as _q
        if request.headers.get("hx-request", "").lower() == "true":
            from fastapi.responses import Response
            return Response(
                status_code=204,
                headers={"HX-Redirect": f"/setup/charts?slug={_q(slug)}&opened=0"},
            )
        return RedirectResponse(
            f"/setup/charts?slug={slug}&opened=0", status_code=303,
        )
    writer = AccountsWriter(
        main_bean=settings.ledger_main,
        connector_accounts=settings.connector_accounts_path,
    )
    try:
        writer.write_opens(
            missing,
            comment=f"Setup: scaffold {len(missing)} categor{'ies' if len(missing) != 1 else 'y'} for {slug}",
            existing_paths=existing,
        )
    except BeanCheckError as exc:
        log.error("setup chart scaffold bean-check: %s", exc)
        return _hx_aware_error_redirect(
            request, "/setup/charts",
            f"bean-check-{slug}-{exc}",
        )
    reader.invalidate()
    # HTMX-aware: re-render this entity's chart-status row partial for
    # in-place swap — same pattern as the vehicles + properties
    # scaffold buttons. Phase 2.1.
    if request.headers.get("hx-request", "").lower() == "true":
        from lamella.features.setup.posting_counts import (
            open_paths as _open_paths,
        )
        # Recompute against the post-write ledger so the row reflects
        # the new "all open" state.
        fresh_entries = list(reader.load().entries)
        open_set = _open_paths(fresh_entries)
        candidates = scaffold_paths_for_entity(yaml_data, slug)
        still_missing = [
            c["path"] for c in candidates if c["path"] not in open_set
        ]
        e_payload = {
            "slug": slug,
            "display_name": row["display_name"],
            "schedule": (
                "Schedule C" if (row["tax_schedule"] or "").upper() == "C"
                else "Schedule F" if (row["tax_schedule"] or "").upper() == "F"
                else "Personal / Schedule A"
            ),
            "applicable": True,
            "total": len(candidates),
            "missing": len(still_missing),
            "missing_paths": still_missing[:10],
        }
        return request.app.state.templates.TemplateResponse(
            request, "partials/_setup_chart_row.html",
            {"e": e_payload},
        )
    return RedirectResponse(
        f"/setup/charts?slug={slug}&opened={len(missing)}",
        status_code=303,
    )


@router.get("/setup/properties", response_class=HTMLResponse)
def setup_properties_page(
    request: Request,
    add: str | None = None,
    edit: str | None = None,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Streamlined property chart check for the setup flow.

    For every registered property: show whether each canonical
    Expenses:<Entity>:Property:<Slug>:<Cat> account is open, offer
    a one-click "Scaffold missing" per property. Rentals get extras.

    Modal triggers:

    - ``?add=property`` opens the + Add modal (Phase 4); save
      delegates to ``POST /setup/properties/add``.
    - ``?edit={slug}`` opens the Edit modal prefilled with the row's
      current values (Phase 8 step 5); save delegates to
      ``POST /setup/properties/{slug}/edit``. Slug is read-only —
      slugs are immutable identifiers. Owning entity is read-only
      once set — entity changes require the dedicated
      change-ownership flow at /settings/properties/{slug}/
      change-ownership (intercompany transfer with disposal +
      re-acquisition semantics; not a field-edit operation).

    The full editor at /settings/properties/{slug} still exists for
    valuation history, change-ownership, disposal — power-user
    state-change ops the recovery modal intentionally doesn't ship.
    """
    open_modal: str | None = None
    editing_slug: str | None = None
    edit_form_values: dict[str, str] = {}
    if add == "property":
        open_modal = "property"
    elif edit:
        property_row = conn.execute(
            "SELECT slug, display_name, entity_slug, property_type, "
            "       address, city, state, postal_code, "
            "       purchase_date, purchase_price, "
            "       is_primary_residence, is_rental, notes "
            "  FROM properties WHERE slug = ? AND is_active = 1",
            (edit,),
        ).fetchone()
        if property_row is not None:
            open_modal = "property-edit"
            editing_slug = property_row["slug"]
            edit_form_values = {
                k: ("" if v is None else str(v))
                for k, v in dict(property_row).items()
            }
            # Boolean checkboxes need "1" / "" form values, not "0".
            edit_form_values["is_primary_residence"] = (
                "1" if property_row["is_primary_residence"] else ""
            )
            edit_form_values["is_rental"] = (
                "1" if property_row["is_rental"] else ""
            )
            # The linked-loan set reads from the loans.property_slug
            # back-reference, not from a properties column. Phase 8
            # multi-loan: a property may carry multiple linked loans
            # (mortgage + HELOC on one house). Surface every active
            # mortgage/heloc currently linked to this property as a
            # list so the Edit form can render the checkbox state.
            linked_rows = conn.execute(
                "SELECT slug FROM loans "
                " WHERE property_slug = ? AND is_active = 1 "
                "   AND loan_type IN ('mortgage', 'heloc') "
                " ORDER BY slug",
                (editing_slug,),
            ).fetchall()
            edit_form_values["linked_loan_slugs"] = [
                r["slug"] for r in linked_rows
            ]
    from lamella.features.properties.property_companion import (
        property_chart_paths_for,
    )
    from lamella.features.setup.posting_counts import open_paths as _open_paths
    entries = list(reader.load().entries)
    open_paths_set = _open_paths(entries)

    rows = [
        dict(r) for r in conn.execute(
            "SELECT slug, display_name, entity_slug, address, "
            "       is_rental, is_primary_residence, is_active "
            "  FROM properties WHERE is_active = 1 ORDER BY slug"
        ).fetchall()
    ]
    display: list[dict] = []
    for p in rows:
        expected = property_chart_paths_for(
            property_slug=p["slug"],
            entity_slug=p["entity_slug"],
            is_rental=bool(p["is_rental"]),
        )
        chart = [
            {
                "path": x.path,
                "purpose": x.purpose,
                "exists": x.path in open_paths_set,
            }
            for x in expected
        ]
        missing = [c for c in chart if not c["exists"]]
        display.append({
            **p,
            "chart": chart,
            "missing_count": len(missing),
            "has_entity": bool((p["entity_slug"] or "").strip()),
        })
    entity_slugs = [
        r["slug"] for r in conn.execute(
            "SELECT slug FROM entities WHERE is_active = 1 ORDER BY slug"
        ).fetchall()
    ]
    # Mortgage-shaped loans for the linked-loan dropdown. Filter to
    # mortgage / heloc kinds since those are the only loan types
    # commonly tied to a property; non-mortgage loans (auto, student)
    # are filtered out so the dropdown stays focused.
    loans_for_link = [
        dict(r) for r in conn.execute(
            "SELECT slug, display_name, institution, entity_slug, loan_type "
            "  FROM loans "
            " WHERE is_active = 1 "
            "   AND loan_type IN ('mortgage', 'heloc') "
            " ORDER BY slug"
        ).fetchall()
    ]
    return request.app.state.templates.TemplateResponse(
        request, "setup_properties.html",
        {
            "properties": display,
            "total": len(rows),
            "missing_total": sum(d["missing_count"] for d in display),
            "open_modal": open_modal,
            "editing_slug": editing_slug,
            "entity_slugs": entity_slugs,
            "loans_for_link": loans_for_link,
            "field_errors": {},
            "form_values": edit_form_values,
            "step_meta": (
                {"id": "properties", "label": "Properties",
                 "url": "/setup/properties"},
            ),
            "current_step": "properties",
            "step_index": 0,
        },
    )


# ---------------------------------------------------------------------------
# Phase 4 of /setup/recovery: + Add property modal save handler.
#
# Pair-twin of setup_vehicle_add. Differences:
#   - Address fields (free-text — no parse, mirrors /settings/properties).
#   - property_type is required (house/land/building/condo/rental/other).
#   - is_primary_residence + is_rental flags.
#   - Linked-loan dropdown filtered to mortgage/heloc loan_types.
#   - Single-loan field for Phase 4. Multi-loan (mortgage + HELOC on
#     one property) is a known incomplete edge — same shape as the
#     loans-edit punt. Tracked for Phase 8 cleanup.
#   - "No mortgage — owned outright" is the explicit no-loan label.
#   - NO auto-scaffold of per-property accounts.
# ---------------------------------------------------------------------------


_PROPERTY_TYPES = ("house", "land", "building", "condo", "rental", "other")


def _re_render_property_modal(
    request: Request,
    *,
    settings: Settings,
    conn,
    field_errors: dict[str, str],
    form_values: dict[str, str],
    editing_slug: str | None = None,
):
    """``editing_slug`` non-None → edit mode (modal renders the Edit
    title + posts to ``/setup/properties/{slug}/edit``); None → add
    mode."""
    entity_slugs = [
        r["slug"] for r in conn.execute(
            "SELECT slug FROM entities WHERE is_active = 1 ORDER BY slug"
        ).fetchall()
    ]
    loans_for_link = [
        dict(r) for r in conn.execute(
            "SELECT slug, display_name, institution, entity_slug, loan_type "
            "  FROM loans "
            " WHERE is_active = 1 "
            "   AND loan_type IN ('mortgage', 'heloc') "
            " ORDER BY slug"
        ).fetchall()
    ]
    return request.app.state.templates.TemplateResponse(
        request, "setup_properties.html",
        {
            "properties": [],
            "total": 0,
            "missing_total": 0,
            "open_modal": "property-edit" if editing_slug else "property",
            "editing_slug": editing_slug,
            "entity_slugs": entity_slugs,
            "loans_for_link": loans_for_link,
            "field_errors": field_errors,
            "form_values": form_values,
            "step_meta": (
                {"id": "properties", "label": "Properties",
                 "url": "/setup/properties"},
            ),
            "current_step": "properties",
            "step_index": 0,
        },
        status_code=400,
    )


@router.post("/setup/properties/add", response_class=HTMLResponse)
async def setup_property_add(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    """Create a new property from the modal. Required: display_name,
    entity_slug, property_type. Address fields are free-text (no
    parse). Linked-loan optional, validated against active mortgage/
    heloc loans. NO auto-scaffold of per-property accounts."""
    from fastapi.responses import RedirectResponse
    from lamella.core.registry.service import normalize_slug, disambiguate_slug

    form = await request.form()
    display_name = (form.get("display_name") or "").strip()
    slug_typed = (form.get("slug") or "").strip()
    entity_slug = (form.get("entity_slug") or "").strip()
    property_type = (form.get("property_type") or "").strip().lower()
    address = (form.get("address") or "").strip()
    city = (form.get("city") or "").strip()
    state = (form.get("state") or "").strip()
    postal_code = (form.get("postal_code") or "").strip()
    purchase_date = (form.get("purchase_date") or "").strip()
    purchase_price = (form.get("purchase_price") or "").strip()
    is_primary_residence = 1 if form.get("is_primary_residence") == "1" else 0
    is_rental = 1 if form.get("is_rental") == "1" else 0
    # Phase 8 multi-loan: the form posts one ``linked_loan_slug`` value
    # per checked loan. ``form.getlist`` collects them all; back-compat
    # with single-value clients is automatic (a lone post arrives as
    # a one-element list). De-dup + drop empties to handle stale
    # browser state.
    linked_loan_slugs = [
        s for s in (
            (v or "").strip()
            for v in form.getlist("linked_loan_slug")
        ) if s
    ]
    # Stable order for both validation iteration and form re-render.
    linked_loan_slugs = sorted(set(linked_loan_slugs))
    notes = (form.get("notes") or "").strip()

    field_errors: dict[str, str] = {}
    if not display_name:
        field_errors["display_name"] = "Required."
    if not entity_slug:
        field_errors["entity_slug"] = "Required."
    if not property_type:
        field_errors["property_type"] = "Pick a property type."
    elif property_type not in _PROPERTY_TYPES:
        field_errors["property_type"] = (
            f"Unknown property type: {property_type!r}. "
            f"Pick one of: {', '.join(_PROPERTY_TYPES)}."
        )

    slug = normalize_slug(slug_typed, fallback_display_name=display_name)
    if not slug:
        field_errors["slug"] = (
            "Couldn't derive a valid slug — type a display name or "
            "an explicit slug starting with a capital letter."
        )

    # Linked-loan validation: each selected loan must reference a real
    # mortgage/heloc row AND match the property's entity. Mixed-entity
    # links (a Personal property linking a BetaCorp mortgage) produce
    # broken account hierarchies under reconstruct.
    for candidate_slug in linked_loan_slugs:
        existing_loan = conn.execute(
            "SELECT slug, entity_slug FROM loans "
            " WHERE slug = ? AND is_active = 1 "
            "   AND loan_type IN ('mortgage', 'heloc')",
            (candidate_slug,),
        ).fetchone()
        if existing_loan is None:
            field_errors["linked_loan_slug"] = (
                f"No active mortgage/heloc loan with slug "
                f"{candidate_slug!r}. Pick from the checkbox list."
            )
            break
        if (
            entity_slug
            and existing_loan["entity_slug"]
            and existing_loan["entity_slug"] != entity_slug
        ):
            field_errors["linked_loan_slug"] = (
                f"Loan {candidate_slug!r} belongs to entity "
                f"{existing_loan['entity_slug']!r}, but this property "
                f"is owned by {entity_slug!r}. Mixed-entity links "
                "produce broken account hierarchies — pick a loan "
                "with matching entity, or change the property's "
                "owning entity above."
            )
            break

    form_values_payload = {
        "display_name": display_name, "slug": slug_typed,
        "entity_slug": entity_slug, "property_type": property_type,
        "address": address, "city": city, "state": state,
        "postal_code": postal_code,
        "purchase_date": purchase_date, "purchase_price": purchase_price,
        "is_primary_residence": "1" if is_primary_residence else "",
        "is_rental": "1" if is_rental else "",
        "linked_loan_slugs": list(linked_loan_slugs),
        "notes": notes,
    }

    if not field_errors and slug:
        existing = conn.execute(
            "SELECT slug FROM properties WHERE slug = ?", (slug,),
        ).fetchone()
        if existing is not None:
            suggested = disambiguate_slug(conn, slug, "properties")
            field_errors["slug"] = (
                f'Property slug "{slug}" is already taken. '
                f'Try "{suggested}" instead, or edit the existing property.'
            )

    if field_errors:
        return _re_render_property_modal(
            request, settings=settings, conn=conn,
            field_errors=field_errors, form_values=form_values_payload,
        )

    # INSERT property row. Per the constraint: no auto-scaffold of
    # per-property accounts. The asset_account_path is computed
    # canonical-shape so the row carries the path the future scaffold
    # button will open, but no Open directive is written here.
    canonical_asset_path = (
        f"Assets:{entity_slug}:Property:{slug}" if entity_slug else None
    )
    conn.execute(
        """
        INSERT INTO properties
            (slug, display_name, property_type, entity_slug,
             address, city, state, postal_code,
             purchase_date, purchase_price, asset_account_path,
             is_primary_residence, is_rental, is_active, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
        """,
        (
            slug, display_name, property_type, entity_slug,
            address or None, city or None, state or None, postal_code or None,
            purchase_date or None, purchase_price or None,
            canonical_asset_path,
            is_primary_residence, is_rental,
            notes or None,
        ),
    )

    # Persist linked loans on each loan row via the existing
    # loans.property_slug back-reference. Phase 8 multi-loan: write
    # to every selected loan, not just one. Failures are logged
    # individually so a bad row doesn't break the others.
    for linked_slug in linked_loan_slugs:
        try:
            conn.execute(
                "UPDATE loans SET property_slug = ? WHERE slug = ?",
                (slug, linked_slug),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "linking property %s to loan %s failed: %s",
                slug, linked_slug, exc,
            )

    request.app.state.setup_required_complete = False
    return RedirectResponse(
        f"/setup/properties?added={slug}", status_code=303,
    )


# ---------------------------------------------------------------------------
# Phase 8 step 5: + Edit property modal save handler.
#
# Twin of setup_property_add. Same field set; UPDATEs instead of
# INSERTs. Slug is path-bound (immutable identifier). Entity is
# locked once set — entity transitions are intercompany transfers
# handled by the dedicated change-ownership flow at /settings/
# properties/{slug}/change-ownership (disposal + re-acquisition
# semantics; not a field-edit operation). Same entity-mismatch
# validation as Add: linked_loan_slug must reference a loan whose
# entity matches the property's entity.
#
# Linked-loan handling: the link is stored as loans.property_slug
# (back-reference), not on the properties row. Edit handler:
#   1. Clears property_slug on the previously-linked loan (if any
#      and different from the new selection)
#   2. Sets property_slug on the new selection (if any)
# Unlinking is an explicit blank submit.
# ---------------------------------------------------------------------------


@router.post("/setup/properties/{slug}/edit", response_class=HTMLResponse)
async def setup_property_edit(
    request: Request,
    slug: str,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    """Update an existing property from the per-row Edit modal.

    Slug comes from the path (immutable). Entity is silently
    preserved if currently set (UI renders the field readonly;
    handler enforces). Validation failure re-renders with
    field_errors + form_values; success → 303 redirect to
    ``/setup/properties?updated={slug}``.

    404 if the slug doesn't match an active property.
    """
    from fastapi.responses import RedirectResponse

    existing = conn.execute(
        "SELECT slug, entity_slug FROM properties "
        " WHERE slug = ? AND is_active = 1",
        (slug,),
    ).fetchone()
    if existing is None:
        raise HTTPException(
            status_code=404,
            detail=f"Property {slug!r} not found or inactive.",
        )
    existing_entity = (existing["entity_slug"] or "").strip()

    form = await request.form()
    display_name = (form.get("display_name") or "").strip()
    entity_slug_typed = (form.get("entity_slug") or "").strip()
    property_type = (form.get("property_type") or "").strip().lower()
    address = (form.get("address") or "").strip()
    city = (form.get("city") or "").strip()
    state = (form.get("state") or "").strip()
    postal_code = (form.get("postal_code") or "").strip()
    purchase_date = (form.get("purchase_date") or "").strip()
    purchase_price = (form.get("purchase_price") or "").strip()
    is_primary_residence = 1 if form.get("is_primary_residence") == "1" else 0
    is_rental = 1 if form.get("is_rental") == "1" else 0
    # Phase 8 multi-loan: getlist + de-dup + sorted, same shape as
    # the Add handler. Backward compat with single-value posts is
    # automatic.
    linked_loan_slugs = sorted(set(
        s for s in (
            (v or "").strip()
            for v in form.getlist("linked_loan_slug")
        ) if s
    ))
    notes = (form.get("notes") or "").strip()

    # Entity-locking: once set, the field is preserved regardless of
    # what the form posted. Mirrors the test_property_save_cannot_
    # change_entity_slug invariant in test_setup_smoke.py — entity
    # transitions go through /settings/properties/{slug}/change-
    # ownership, not through field edits.
    entity_slug = existing_entity or entity_slug_typed

    field_errors: dict[str, str] = {}
    if not display_name:
        field_errors["display_name"] = "Required."
    if not entity_slug:
        field_errors["entity_slug"] = "Required."
    if not property_type:
        field_errors["property_type"] = "Pick a property type."
    elif property_type not in _PROPERTY_TYPES:
        field_errors["property_type"] = (
            f"Unknown property type: {property_type!r}. "
            f"Pick one of: {', '.join(_PROPERTY_TYPES)}."
        )

    # Linked-loan validation: per-loan, same shape as Add. Mixed-
    # entity links produce broken account hierarchies under
    # reconstruct.
    for candidate_slug in linked_loan_slugs:
        existing_loan = conn.execute(
            "SELECT slug, entity_slug FROM loans "
            " WHERE slug = ? AND is_active = 1 "
            "   AND loan_type IN ('mortgage', 'heloc')",
            (candidate_slug,),
        ).fetchone()
        if existing_loan is None:
            field_errors["linked_loan_slug"] = (
                f"No active mortgage/heloc loan with slug "
                f"{candidate_slug!r}. Pick from the checkbox list."
            )
            break
        if (
            entity_slug
            and existing_loan["entity_slug"]
            and existing_loan["entity_slug"] != entity_slug
        ):
            field_errors["linked_loan_slug"] = (
                f"Loan {candidate_slug!r} belongs to entity "
                f"{existing_loan['entity_slug']!r}, but this property "
                f"is owned by {entity_slug!r}. Mixed-entity links "
                "produce broken account hierarchies."
            )
            break

    form_values_payload = {
        "display_name": display_name, "slug": slug,
        "entity_slug": entity_slug, "property_type": property_type,
        "address": address, "city": city, "state": state,
        "postal_code": postal_code,
        "purchase_date": purchase_date, "purchase_price": purchase_price,
        "is_primary_residence": "1" if is_primary_residence else "",
        "is_rental": "1" if is_rental else "",
        "linked_loan_slugs": list(linked_loan_slugs),
        "notes": notes,
    }

    if field_errors:
        return _re_render_property_modal(
            request, settings=settings, conn=conn,
            field_errors=field_errors,
            form_values=form_values_payload,
            editing_slug=slug,
        )

    # Refresh the canonical asset path if the entity just got set
    # (was empty before — now non-empty).
    canonical_asset_path = (
        f"Assets:{entity_slug}:Property:{slug}" if entity_slug else None
    )

    conn.execute(
        """
        UPDATE properties SET
            display_name = ?,
            entity_slug = ?,
            property_type = ?,
            address = ?,
            city = ?,
            state = ?,
            postal_code = ?,
            purchase_date = ?,
            purchase_price = ?,
            asset_account_path = COALESCE(NULLIF(?, ''), asset_account_path),
            is_primary_residence = ?,
            is_rental = ?,
            notes = ?
        WHERE slug = ?
        """,
        (
            display_name, entity_slug, property_type,
            address or None, city or None, state or None, postal_code or None,
            purchase_date or None, purchase_price or None,
            canonical_asset_path or "",
            is_primary_residence, is_rental,
            notes or None,
            slug,
        ),
    )

    # Linked-loan reconciliation (Phase 8 multi-loan). Read the set
    # of currently-linked loans (mortgage/heloc only), diff against
    # the user's submitted set, then:
    #   - UNLINK loans in current ∖ submitted (set property_slug=NULL)
    #   - LINK loans in submitted ∖ current (set property_slug=slug)
    # Loans in the intersection stay as-is. Failures are logged
    # individually so a bad row doesn't break the others.
    current_links = {
        r["slug"] for r in conn.execute(
            "SELECT slug FROM loans "
            " WHERE property_slug = ? AND is_active = 1 "
            "   AND loan_type IN ('mortgage', 'heloc')",
            (slug,),
        ).fetchall()
    }
    submitted_links = set(linked_loan_slugs)
    for unlink_slug in current_links - submitted_links:
        try:
            conn.execute(
                "UPDATE loans SET property_slug = NULL WHERE slug = ?",
                (unlink_slug,),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "unlinking loan %s from property %s failed: %s",
                unlink_slug, slug, exc,
            )
    for link_slug in submitted_links - current_links:
        try:
            conn.execute(
                "UPDATE loans SET property_slug = ? WHERE slug = ?",
                (slug, link_slug),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "linking loan %s to property %s failed: %s",
                link_slug, slug, exc,
            )

    request.app.state.setup_required_complete = False
    return RedirectResponse(
        f"/setup/properties?updated={slug}", status_code=303,
    )


@router.post("/setup/properties/{slug}/scaffold")
def setup_property_scaffold(
    slug: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    from fastapi.responses import RedirectResponse
    from lamella.features.properties.property_companion import ensure_property_chart
    from lamella.core.ledger_writer import BeanCheckError

    row = conn.execute(
        "SELECT slug, entity_slug, is_rental FROM properties WHERE slug = ?",
        (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"property {slug!r} not found")
    if not (row["entity_slug"] or "").strip():
        return _hx_aware_error_redirect(
            request, "/setup/properties", f"no-entity-for-{slug}",
        )
    try:
        opened = ensure_property_chart(
            conn=conn, settings=settings, reader=reader,
            property_slug=slug, entity_slug=row["entity_slug"],
            is_rental=bool(row["is_rental"]),
        )
    except BeanCheckError as exc:
        log.error("property scaffold bean-check: %s", exc)
        return _hx_aware_error_redirect(
            request, "/setup/properties",
            f"bean-check-{slug}-{exc}",
        )
    # HTMX-aware: re-render the property's row partial for in-place
    # swap. Mirrors the vehicle pattern from commit 8b947a6 — click,
    # click, click without reloading the whole page. Phase 2.1.
    if request.headers.get("hx-request", "").lower() == "true":
        from lamella.features.properties.property_companion import (
            property_chart_paths_for,
        )
        from lamella.features.setup.posting_counts import (
            open_paths as _open_paths,
        )
        entries = list(reader.load().entries)
        open_paths_set = _open_paths(entries)
        fresh_row = conn.execute(
            "SELECT slug, display_name, entity_slug, address, "
            "       is_rental, is_primary_residence FROM properties "
            "WHERE slug = ?",
            (slug,),
        ).fetchone()
        expected = property_chart_paths_for(
            property_slug=slug,
            entity_slug=fresh_row["entity_slug"],
            is_rental=bool(fresh_row["is_rental"]),
        )
        chart = [
            {"path": x.path, "purpose": x.purpose, "exists": x.path in open_paths_set}
            for x in expected
        ]
        missing = [c for c in chart if not c["exists"]]
        p = {
            **dict(fresh_row),
            "chart": chart,
            "missing_count": len(missing),
            "has_entity": bool((fresh_row["entity_slug"] or "").strip()),
        }
        return request.app.state.templates.TemplateResponse(
            request, "partials/_setup_property_row.html",
            {"p": p},
        )
    return RedirectResponse(
        f"/setup/properties?scaffolded={slug}&opened={len(opened)}",
        status_code=303,
    )


@router.get("/setup/loans", response_class=HTMLResponse)
def setup_loans_page(
    request: Request,
    add: str | None = None,
    edit: str | None = None,
    conn = Depends(get_db),
):
    """Streamlined loan-setup check. Loans don't need chart
    scaffolding the same way vehicles/properties do (step9_loans
    already ensures the liability / interest / escrow accounts
    during loan creation), so this page is mostly a confirmation
    view: list every active loan, show whether its required
    account paths are set.

    Modal triggers:

    - ``?add=loan`` opens the + Add modal (Phase 4); save delegates
      to ``POST /setup/loans/add``.
    - ``?edit={slug}`` opens the Edit modal prefilled with the row's
      current values (Phase 8 step 4); save delegates to
      ``POST /setup/loans/{slug}/edit``. Slug field is read-only —
      slugs are immutable identifiers. The full editor at
      ``/settings/loans/{slug}/edit`` still exists for power-user
      state-change ops (record payments, escrow reconciliation,
      payoff workflow); the recovery-flow row link no longer breaks
      out to the main app for routine field edits.
    """
    open_modal: str | None = None
    editing_slug: str | None = None
    edit_form_values: dict[str, str] = {}
    if add == "loan":
        open_modal = "loan"
    elif edit:
        # Resolve the slug → loan row. If the slug doesn't match any
        # active loan, render the page WITHOUT the modal — the user
        # got here via a stale link or a typo and the surrounding
        # page is still useful.
        loan_row = conn.execute(
            "SELECT slug, display_name, loan_type, entity_slug, "
            "       institution, original_principal, funded_date, "
            "       interest_rate_apr, term_months, "
            "       monthly_payment_estimate, "
            "       liability_account_path, interest_account_path, "
            "       escrow_account_path, escrow_monthly, notes "
            "  FROM loans WHERE slug = ? AND is_active = 1",
            (edit,),
        ).fetchone()
        if loan_row is not None:
            open_modal = "loan-edit"
            editing_slug = loan_row["slug"]
            # Coerce every value to its string-form for the form,
            # NULLs become empty strings. The funded_date column is
            # already TEXT so no special handling there.
            edit_form_values = {
                k: ("" if v is None else str(v))
                for k, v in dict(loan_row).items()
            }
    rows = [
        dict(r) for r in conn.execute(
            "SELECT slug, display_name, institution, loan_type, "
            "       entity_slug, property_slug, "
            "       liability_account_path, interest_account_path, "
            "       escrow_account_path, is_active "
            "  FROM loans WHERE is_active = 1 ORDER BY slug"
        ).fetchall()
    ]
    for r in rows:
        r["vehicle_slug"] = None  # schema doesn't track vehicle-loans yet
    for r in rows:
        missing = []
        if not (r.get("liability_account_path") or "").strip():
            missing.append("liability_account_path")
        if not (r.get("interest_account_path") or "").strip():
            missing.append("interest_account_path")
        if not (r.get("entity_slug") or "").strip():
            missing.append("entity_slug")
        r["missing_fields"] = missing

    # Datalist data for the modal's autocomplete fields. Per CLAUDE.md
    # rule 5: account paths are long-tail user data and must use
    # autocomplete, not <select>.
    entity_slugs = [
        r["slug"] for r in conn.execute(
            "SELECT slug FROM entities WHERE is_active = 1 ORDER BY slug"
        ).fetchall()
    ]
    liability_paths = [
        r["account_path"] for r in conn.execute(
            "SELECT account_path FROM accounts_meta "
            "WHERE closed_on IS NULL "
            "  AND account_path LIKE 'Liabilities:%' "
            "ORDER BY account_path"
        ).fetchall()
    ]
    expense_paths = [
        r["account_path"] for r in conn.execute(
            "SELECT account_path FROM accounts_meta "
            "WHERE closed_on IS NULL "
            "  AND account_path LIKE 'Expenses:%' "
            "ORDER BY account_path"
        ).fetchall()
    ]

    return request.app.state.templates.TemplateResponse(
        request, "setup_loans.html",
        {
            "loans": rows,
            "total": len(rows),
            "incomplete": sum(1 for r in rows if r["missing_fields"]),
            "open_modal": open_modal,
            "editing_slug": editing_slug,
            "entity_slugs": entity_slugs,
            "liability_paths": liability_paths,
            "expense_paths": expense_paths,
            "field_errors": {},
            "form_values": edit_form_values,
            "step_meta": (
                {"id": "loans", "label": "Loans",
                 "url": "/setup/loans"},
            ),
            "current_step": "loans",
            "step_index": 0,
        },
    )


# ---------------------------------------------------------------------------
# Phase 4 of /setup/recovery: + Add loan modal save handler.
#
# Per the locked spec: validation lives in the existing settings-side
# writer, the route handler catches its exceptions and converts to
# field_errors. Here we inline the new-loan INSERT branch from
# routes/loans.py:194-376 (the existing /settings/loans handler) —
# extracting it into a shared helper would balloon Phase 4 scope, and
# the duplication is one branch's worth, not the full handler. The
# existing /settings/loans path stays untouched.
#
# Required-field set is intentionally smaller than the full editor:
# essentials only (display_name, loan_type, entity_slug, institution,
# original_principal, funded_date). Optional fields like APR, term,
# escrow can be filled in via the per-row Edit later. Account-path
# fields auto-scaffold from entity+institution+slug when blank,
# mirroring the existing handler's `computed_liability_path` logic.
# ---------------------------------------------------------------------------


_LOAN_TYPE_OPTIONS = (
    ("mortgage", "Mortgage"),
    ("auto", "Auto loan"),
    ("student", "Student loan"),
    ("personal", "Personal loan"),
    ("heloc", "HELOC / line of credit"),
    ("eidl", "EIDL / SBA"),
    ("other", "Other"),
)


def _re_render_loan_modal(
    request: Request,
    *,
    conn,
    field_errors: dict[str, str],
    form_values: dict[str, str],
    editing_slug: str | None = None,
):
    """Re-render setup_loans.html with the modal still open + the
    user's typed values preserved. Mirror of _re_render_entity_modal.

    ``editing_slug`` non-None → edit mode (modal renders the Edit
    title + posts to ``/setup/loans/{slug}/edit``); None → add mode.
    """
    rows = [
        dict(r) for r in conn.execute(
            "SELECT slug, display_name, institution, loan_type, "
            "       entity_slug, property_slug, "
            "       liability_account_path, interest_account_path, "
            "       escrow_account_path, is_active "
            "  FROM loans WHERE is_active = 1 ORDER BY slug"
        ).fetchall()
    ]
    for r in rows:
        r["vehicle_slug"] = None
        r["missing_fields"] = []
    entity_slugs = [
        r["slug"] for r in conn.execute(
            "SELECT slug FROM entities WHERE is_active = 1 ORDER BY slug"
        ).fetchall()
    ]
    liability_paths = [
        r["account_path"] for r in conn.execute(
            "SELECT account_path FROM accounts_meta "
            "WHERE closed_on IS NULL "
            "  AND account_path LIKE 'Liabilities:%' "
            "ORDER BY account_path"
        ).fetchall()
    ]
    expense_paths = [
        r["account_path"] for r in conn.execute(
            "SELECT account_path FROM accounts_meta "
            "WHERE closed_on IS NULL "
            "  AND account_path LIKE 'Expenses:%' "
            "ORDER BY account_path"
        ).fetchall()
    ]
    return request.app.state.templates.TemplateResponse(
        request, "setup_loans.html",
        {
            "loans": rows,
            "total": len(rows),
            "incomplete": 0,
            "open_modal": "loan-edit" if editing_slug else "loan",
            "editing_slug": editing_slug,
            "entity_slugs": entity_slugs,
            "liability_paths": liability_paths,
            "expense_paths": expense_paths,
            "field_errors": field_errors,
            "form_values": form_values,
            "step_meta": (
                {"id": "loans", "label": "Loans",
                 "url": "/setup/loans"},
            ),
            "current_step": "loans",
            "step_index": 0,
        },
        status_code=400,
    )


@router.post("/setup/loans/add", response_class=HTMLResponse)
async def setup_loan_add(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    """Create a new loan from the modal. Required: display_name,
    loan_type, entity_slug, institution, original_principal,
    funded_date. Optional: slug (auto-derived), interest_rate_apr,
    term_months, monthly_payment_estimate, account paths
    (auto-scaffolded if blank).

    Validation failure re-renders the modal with field_errors +
    form_values. Slug collisions surface as field-level errors on
    the slug field. Success → 303 redirect to /setup/loans?added=
    {slug}."""
    from fastapi.responses import RedirectResponse
    from lamella.core.registry.service import (
        is_valid_slug, suggest_slug, disambiguate_slug,
    )

    form = await request.form()
    display_name = (form.get("display_name") or "").strip()
    slug_typed = (form.get("slug") or "").strip()
    loan_type = (form.get("loan_type") or "").strip()
    entity_slug = (form.get("entity_slug") or "").strip()
    institution = (form.get("institution") or "").strip()
    original_principal = (form.get("original_principal") or "").strip()
    funded_date = (form.get("funded_date") or "").strip()
    interest_rate_apr = (form.get("interest_rate_apr") or "").strip()
    term_months = (form.get("term_months") or "").strip()
    monthly_payment_estimate = (form.get("monthly_payment_estimate") or "").strip()
    liability_account_path = (form.get("liability_account_path") or "").strip()
    interest_account_path = (form.get("interest_account_path") or "").strip()
    notes = (form.get("notes") or "").strip()

    field_errors: dict[str, str] = {}
    if not display_name:
        field_errors["display_name"] = "Required."
    if not loan_type:
        field_errors["loan_type"] = "Pick a loan type."
    elif loan_type not in {v for v, _ in _LOAN_TYPE_OPTIONS}:
        field_errors["loan_type"] = f"Unknown loan type: {loan_type!r}."
    if not entity_slug:
        field_errors["entity_slug"] = "Required."
    if not institution:
        field_errors["institution"] = "Required."
    if not original_principal:
        field_errors["original_principal"] = "Required."
    if not funded_date:
        field_errors["funded_date"] = "Required."

    # Slug derivation + validation. Match the /settings/loans handler:
    # auto-suggest from display_name when blank, allow user override.
    slug = slug_typed or (suggest_slug(display_name) if display_name else "")
    if slug and not is_valid_slug(slug):
        field_errors["slug"] = (
            "Slugs must start with an uppercase letter (A–Z) and "
            "contain only letters, digits, and hyphens."
        )

    form_values_payload = {
        "display_name": display_name, "slug": slug_typed,
        "loan_type": loan_type, "entity_slug": entity_slug,
        "institution": institution, "original_principal": original_principal,
        "funded_date": funded_date, "interest_rate_apr": interest_rate_apr,
        "term_months": term_months,
        "monthly_payment_estimate": monthly_payment_estimate,
        "liability_account_path": liability_account_path,
        "interest_account_path": interest_account_path,
        "notes": notes,
    }

    if not field_errors and slug:
        # Slug-collision check (mirrors /settings/loans:209+ shape).
        existing = conn.execute(
            "SELECT slug FROM loans WHERE slug = ?", (slug,),
        ).fetchone()
        if existing is not None:
            suggested = disambiguate_slug(conn, slug, "loans")
            field_errors["slug"] = (
                f'Loan slug "{slug}" is already taken. '
                f'Try "{suggested}" instead, or edit the existing loan.'
            )

    if field_errors:
        return _re_render_loan_modal(
            request, conn=conn,
            field_errors=field_errors,
            form_values=form_values_payload,
        )

    # Auto-scaffold the account paths if the user didn't type them —
    # mirrors /settings/loans:320-336 shape.
    inst_slug = suggest_slug(institution) or institution.replace(" ", "")
    computed_liability = (
        liability_account_path
        or f"Liabilities:{entity_slug}:{inst_slug}:{slug}"
    )
    computed_interest = (
        interest_account_path
        or f"Expenses:{entity_slug}:{slug}:Interest"
    )

    conn.execute(
        """
        INSERT INTO loans
            (slug, display_name, loan_type, entity_slug, institution,
             original_principal, funded_date,
             interest_rate_apr, term_months, monthly_payment_estimate,
             liability_account_path, interest_account_path,
             is_active, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
        """,
        (
            slug, display_name, loan_type, entity_slug, institution,
            original_principal, funded_date,
            interest_rate_apr or None,
            int(term_months) if term_months else None,
            monthly_payment_estimate or None,
            computed_liability, computed_interest,
            notes or None,
        ),
    )

    # Open the canonical liability + interest accounts so subsequent
    # postings against the loan don't trip "inactive account" errors.
    # Same dispatch /settings/loans uses; failures here are non-fatal
    # for the SQL row (the user can re-scaffold via the per-row autofix).
    try:
        from lamella.core.registry.accounts_writer import AccountsWriter
        from lamella.core.beancount_io import LedgerReader
        reader = request.app.state.ledger_reader
        existing_paths = {
            getattr(e, "account", None)
            for e in reader.load().entries
            if hasattr(e, "account")
        }
        writer = AccountsWriter(
            main_bean=settings.ledger_main,
            connector_accounts=settings.connector_accounts_path,
        )
        from datetime import date as _date
        try:
            funded = _date.fromisoformat(funded_date)
        except ValueError:
            funded = None
        writer.write_opens(
            [computed_liability, computed_interest],
            opened_on=funded,
            comment=f"loan {slug} ({display_name})",
            existing_paths=existing_paths,
        )
        reader.invalidate()
    except Exception as exc:  # noqa: BLE001
        log.warning("loan account scaffold failed for %s: %s", slug, exc)

    request.app.state.setup_required_complete = False
    return RedirectResponse(
        f"/setup/loans?added={slug}", status_code=303,
    )


# ---------------------------------------------------------------------------
# Phase 8 step 4: + Edit loan modal save handler.
#
# Twin of setup_loan_add. Same field set + escrow extras; UPDATEs
# instead of INSERTs. Slug is part of the path (immutable identifier),
# not a form field — the modal renders it readonly.
#
# Field-set scope: the recovery shell exposes essentials + escrow
# (the most-commonly-needed mortgage edit). Power-user state changes
# (record payment, escrow reconcile, payoff workflow, revolving-credit
# fields, simplefin binding, payment_due_day) stay on the full editor
# at /settings/loans/{slug}/edit. The recovery flow no longer breaks
# out to the main app for routine field edits.
#
# Validation mirrors setup_loan_add (display_name / loan_type /
# entity_slug / institution / original_principal / funded_date are
# required; everything else optional). Account paths are NOT auto-
# scaffolded on edit — the user already created the loan and may
# have intentional paths set; respecting their input means the form
# is what they typed.
# ---------------------------------------------------------------------------


@router.post("/setup/loans/{slug}/edit", response_class=HTMLResponse)
async def setup_loan_edit(
    request: Request,
    slug: str,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    """Update an existing loan from the per-row Edit modal.

    Slug comes from the path (immutable). The modal exposes the
    same fields as the Add modal plus escrow_account_path and
    escrow_monthly. Validation failure re-renders with field_errors
    + form_values; success → 303 redirect to
    ``/setup/loans?updated={slug}``.

    404 if the slug doesn't match an active loan — protects against
    a stale browser tab editing a loan that's been deleted in
    another session.
    """
    from fastapi.responses import RedirectResponse

    existing = conn.execute(
        "SELECT slug FROM loans WHERE slug = ? AND is_active = 1",
        (slug,),
    ).fetchone()
    if existing is None:
        raise HTTPException(
            status_code=404,
            detail=f"Loan {slug!r} not found or inactive.",
        )

    form = await request.form()
    display_name = (form.get("display_name") or "").strip()
    loan_type = (form.get("loan_type") or "").strip()
    entity_slug = (form.get("entity_slug") or "").strip()
    institution = (form.get("institution") or "").strip()
    original_principal = (form.get("original_principal") or "").strip()
    funded_date = (form.get("funded_date") or "").strip()
    interest_rate_apr = (form.get("interest_rate_apr") or "").strip()
    term_months = (form.get("term_months") or "").strip()
    monthly_payment_estimate = (form.get("monthly_payment_estimate") or "").strip()
    liability_account_path = (form.get("liability_account_path") or "").strip()
    interest_account_path = (form.get("interest_account_path") or "").strip()
    escrow_account_path = (form.get("escrow_account_path") or "").strip()
    escrow_monthly = (form.get("escrow_monthly") or "").strip()
    notes = (form.get("notes") or "").strip()

    field_errors: dict[str, str] = {}
    if not display_name:
        field_errors["display_name"] = "Required."
    if not loan_type:
        field_errors["loan_type"] = "Pick a loan type."
    elif loan_type not in {v for v, _ in _LOAN_TYPE_OPTIONS}:
        field_errors["loan_type"] = f"Unknown loan type: {loan_type!r}."
    if not entity_slug:
        field_errors["entity_slug"] = "Required."
    if not institution:
        field_errors["institution"] = "Required."
    if not original_principal:
        field_errors["original_principal"] = "Required."
    if not funded_date:
        field_errors["funded_date"] = "Required."

    form_values_payload = {
        "display_name": display_name, "slug": slug,
        "loan_type": loan_type, "entity_slug": entity_slug,
        "institution": institution, "original_principal": original_principal,
        "funded_date": funded_date, "interest_rate_apr": interest_rate_apr,
        "term_months": term_months,
        "monthly_payment_estimate": monthly_payment_estimate,
        "liability_account_path": liability_account_path,
        "interest_account_path": interest_account_path,
        "escrow_account_path": escrow_account_path,
        "escrow_monthly": escrow_monthly,
        "notes": notes,
    }

    if field_errors:
        return _re_render_loan_modal(
            request, conn=conn,
            field_errors=field_errors,
            form_values=form_values_payload,
            editing_slug=slug,
        )

    # Field-set-scoped UPDATE. We touch only the fields the modal
    # exposes; everything else (payoff_date, simplefin_account_id,
    # is_revolving, etc.) preserves whatever the full editor or a
    # prior save set. Account paths use COALESCE-NULLIF so blank
    # input doesn't wipe a previously-set path — the recovery flow
    # only adds, never destroys.
    conn.execute(
        """
        UPDATE loans SET
            display_name = ?,
            loan_type = ?,
            entity_slug = ?,
            institution = ?,
            original_principal = ?,
            funded_date = ?,
            interest_rate_apr = ?,
            term_months = ?,
            monthly_payment_estimate = ?,
            liability_account_path = COALESCE(NULLIF(?, ''), liability_account_path),
            interest_account_path  = COALESCE(NULLIF(?, ''), interest_account_path),
            escrow_account_path    = COALESCE(NULLIF(?, ''), escrow_account_path),
            escrow_monthly = ?,
            notes = ?
        WHERE slug = ?
        """,
        (
            display_name, loan_type, entity_slug, institution,
            original_principal, funded_date,
            interest_rate_apr or None,
            int(term_months) if term_months else None,
            monthly_payment_estimate or None,
            liability_account_path, interest_account_path,
            escrow_account_path,
            escrow_monthly or None,
            notes or None,
            slug,
        ),
    )

    request.app.state.setup_required_complete = False
    return RedirectResponse(
        f"/setup/loans?updated={slug}", status_code=303,
    )


# ---------------------------------------------------------------------------
# Phase 4 of /setup/recovery: SimpleFIN recovery wrapper.
#
# Structurally different from the CRUD editors — three-state
# machine (unconnected → connected-discovering → connected-bound),
# token-paste field treated as a security-shaped credential
# (never echoed, never logged, never round-tripped on error
# re-render), skip-for-now sets a dismissed_at timestamp that
# suppresses the recovery-progress finding for 7 days.
#
# Acceptance: zero bare /simplefin/* href references in any
# rendered page within /setup/recovery surfaces. Internal reuse
# of writers is fine; href breakout to /simplefin admin is not.
# ---------------------------------------------------------------------------


_SIMPLEFIN_DISMISS_DURATION_SECONDS = 7 * 24 * 3600  # 7 days


def _simplefin_state(
    settings: Settings, conn,
) -> tuple[str, dict]:
    """Compute the recovery wrapper's state-machine snapshot.

    Returns ``(state, ctx_extras)`` where state is one of:

    - ``unconnected``: no access URL persisted. UI shows
      paste-token form.
    - ``connected_no_accounts``: access URL set but no rows in
      simplefin_discovered_accounts. UI offers a Re-fetch button
      and explains "the bridge connected but reports zero
      accounts."
    - ``connected_unbound``: access URL set + ≥1 discovered account
      and any of them is unbound (no simplefin_account_id link).
      UI shows the binding table.
    - ``connected_bound``: access URL set + every discovered
      account is bound to a ledger account. UI shows summary +
      disconnect/re-fetch options.
    """
    has_access = bool(
        settings.simplefin_access_url
        and settings.simplefin_access_url.get_secret_value()
    )
    if not has_access:
        return "unconnected", {}

    discovered = conn.execute(
        "SELECT account_id, name, org_name, currency, balance "
        "  FROM simplefin_discovered_accounts "
        " ORDER BY COALESCE(org_name, ''), name, account_id"
    ).fetchall()
    if not discovered:
        return "connected_no_accounts", {"discovered": []}

    # Existing bindings for the dropdown options.
    bindings = {
        r["account_id"]: r["account_path"] for r in conn.execute(
            "SELECT simplefin_account_id, account_path FROM accounts_meta "
            "WHERE simplefin_account_id IS NOT NULL "
            "  AND simplefin_account_id != ''"
        ).fetchall()
    }

    rows = [
        {
            "account_id": d["account_id"],
            "name": d["name"],
            "org_name": d["org_name"],
            "currency": d["currency"],
            "balance": d["balance"],
            "bound_to": bindings.get(d["account_id"]),
        }
        for d in discovered
    ]
    bindable_paths = [
        r["account_path"] for r in conn.execute(
            "SELECT account_path FROM accounts_meta "
            "WHERE closed_on IS NULL "
            "  AND (account_path LIKE 'Assets:%' "
            "       OR account_path LIKE 'Liabilities:%') "
            "ORDER BY account_path"
        ).fetchall()
    ]
    unbound_count = sum(1 for r in rows if not r["bound_to"])
    state = "connected_bound" if unbound_count == 0 else "connected_unbound"
    return state, {
        "discovered": rows,
        "bindable_paths": bindable_paths,
        "unbound_count": unbound_count,
    }


@router.get("/setup/simplefin", response_class=HTMLResponse)
def setup_simplefin_page(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    """Recovery-shell SimpleFIN wrapper. State machine renders
    different UIs for unconnected / connected_no_accounts /
    connected_unbound / connected_bound, all on the recovery
    layout. No href breakouts to /simplefin admin."""
    state, extras = _simplefin_state(settings, conn)
    return request.app.state.templates.TemplateResponse(
        request, "setup_simplefin.html",
        {
            "sf_state": state,
            **extras,
            # The token field is intentionally absent from the
            # context — the recovery-shell template never receives
            # the raw token via render context. On error re-render
            # we pass an `error_message` only; the field is empty.
            "error_message": None,
            "step_meta": (
                {"id": "simplefin", "label": "SimpleFIN",
                 "url": "/setup/simplefin"},
            ),
            "current_step": "simplefin",
            "step_index": 0,
        },
    )


@router.post("/setup/simplefin/connect", response_class=HTMLResponse)
async def setup_simplefin_connect(
    request: Request,
    simplefin_token: str = Form(""),
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    """Claim a SimpleFIN setup token, persist the resulting access
    URL, fetch + upsert discovered accounts, redirect to
    /setup/simplefin in connected state.

    Token-handling rules per the locked spec:
      - The raw token is read from the form, used in-flight, and
        never persisted, logged, or rendered back. On error we
        return a generic message and let the user re-paste — the
        field is NOT pre-filled with the failed token.
      - The resulting access URL is what gets persisted (via
        AppSettingsStore, which writes to SQLite + ledger for
        reconstruct round-trip), not the original token.
    """
    raw = (simplefin_token or "").strip()
    if not raw:
        return _re_render_simplefin(
            request, settings=settings, conn=conn,
            error_message="Paste a setup token from SimpleFIN Bridge.",
        )

    from lamella.adapters.simplefin.client import (
        SimpleFINAuthError, SimpleFINError, SimpleFINClient,
        _looks_like_access_url, claim_setup_token,
    )
    try:
        if _looks_like_access_url(raw):
            access_url = raw
        else:
            access_url = claim_setup_token(raw)
    except (SimpleFINAuthError, SimpleFINError) as exc:
        # Generic error — don't echo the token back. The exception
        # message itself shouldn't carry the token (the SimpleFIN
        # client raises with HTTP-status-shaped messages), but we
        # truncate defensively.
        msg = str(exc)
        if len(msg) > 200:
            msg = msg[:200] + "…"
        return _re_render_simplefin(
            request, settings=settings, conn=conn,
            error_message=(
                "Couldn't claim that token. Double-check you copied "
                "the whole URL from SimpleFIN Bridge and that it "
                "hasn't been used already. Bridge said: " + msg
            ),
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("simplefin connect: bridge call failed: %s", type(exc).__name__)
        return _re_render_simplefin(
            request, settings=settings, conn=conn,
            error_message=(
                "Couldn't reach the SimpleFIN bridge right now. "
                "Try again, or paste a fresh token from "
                "https://beta-bridge.simplefin.org."
            ),
        )

    try:
        from lamella.core.settings.store import AppSettingsStore
        store = AppSettingsStore(
            conn,
            connector_config_path=settings.connector_config_path,
            main_bean_path=(
                settings.ledger_main if settings.ledger_main.exists() else None
            ),
        )
        store.set("simplefin_access_url", access_url)
        settings.apply_kv_overrides({"simplefin_access_url": access_url})
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "simplefin connect: settings persist failed: %s",
            type(exc).__name__,
        )
        return _re_render_simplefin(
            request, settings=settings, conn=conn,
            error_message=(
                "Connected to the bridge but couldn't save the access URL. "
                "Try again — if it keeps failing, check disk permissions on "
                "the data dir."
            ),
        )

    # Fetch + upsert discovered accounts. Failure here is non-fatal
    # — the user is connected; we just report 0 discovered.
    try:
        async with SimpleFINClient(access_url=access_url) as client:
            response = await client.fetch_accounts(
                lookback_days=14, include_pending=False,
            )
        from lamella.web.routes.simplefin import _upsert_discovered_accounts
        _upsert_discovered_accounts(conn, response)
        conn.commit()
    except Exception as exc:  # noqa: BLE001
        log.info("simplefin connect: post-claim fetch skipped: %s", type(exc).__name__)

    # Clear any stale dismissal — the user is acting on SimpleFIN
    # again, so the recovery finding should re-engage on its own
    # state going forward.
    try:
        from lamella.core.settings.store import AppSettingsStore
        store2 = AppSettingsStore(
            conn,
            connector_config_path=settings.connector_config_path,
            main_bean_path=(
                settings.ledger_main if settings.ledger_main.exists() else None
            ),
        )
        store2.set("simplefin_dismissed_at", "")
    except Exception:  # noqa: BLE001
        pass

    return RedirectResponse("/setup/simplefin?connected=1", status_code=303)


@router.post("/setup/simplefin/skip", response_class=HTMLResponse)
def setup_simplefin_skip(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    """Stamp a dismissed_at timestamp so the recovery-progress
    finding suppresses for 7 days. The user can revisit
    /setup/simplefin any time to opt back in; this just stops the
    nag for users who don't use SimpleFIN."""
    from datetime import datetime, timezone
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    try:
        from lamella.core.settings.store import AppSettingsStore
        store = AppSettingsStore(
            conn,
            connector_config_path=settings.connector_config_path,
            main_bean_path=(
                settings.ledger_main if settings.ledger_main.exists() else None
            ),
        )
        store.set("simplefin_dismissed_at", now_iso)
    except Exception as exc:  # noqa: BLE001
        log.warning("simplefin skip: settings persist failed: %s", exc)
    return RedirectResponse("/setup/recovery?skipped=simplefin", status_code=303)


@router.post("/setup/simplefin/disconnect", response_class=HTMLResponse)
def setup_simplefin_disconnect(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    """Clear the persisted access URL. Bindings on accounts_meta
    are kept (the user might reconnect to the same bridge); the
    discovered_accounts cache is cleared so a fresh connect
    re-discovers."""
    try:
        from lamella.core.settings.store import AppSettingsStore
        store = AppSettingsStore(
            conn,
            connector_config_path=settings.connector_config_path,
            main_bean_path=(
                settings.ledger_main if settings.ledger_main.exists() else None
            ),
        )
        store.set("simplefin_access_url", "")
        settings.apply_kv_overrides({"simplefin_access_url": ""})
    except Exception as exc:  # noqa: BLE001
        log.warning("simplefin disconnect: settings persist failed: %s", exc)
    try:
        conn.execute("DELETE FROM simplefin_discovered_accounts")
        conn.commit()
    except Exception as exc:  # noqa: BLE001
        log.warning("simplefin disconnect: discovered cleanup failed: %s", exc)
    return RedirectResponse("/setup/simplefin?disconnected=1", status_code=303)


@router.post("/setup/simplefin/bind", response_class=HTMLResponse)
async def setup_simplefin_bind(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    """Bind one SimpleFIN discovered account to a ledger account
    path. Same writer the existing /simplefin admin uses
    (accounts_meta.simplefin_account_id), but per-row instead of
    bulk so the recovery surface stays focused.

    Form: ``simplefin_id`` (required), ``account_path`` (required).
    Empty account_path clears the binding."""
    form = await request.form()
    simplefin_id = (form.get("simplefin_id") or "").strip()
    account_path = (form.get("account_path") or "").strip()
    if not simplefin_id:
        return RedirectResponse(
            "/setup/simplefin?error=missing-simplefin-id",
            status_code=303,
        )

    # Audit hardening: validate the path BEFORE releasing any prior
    # binding. The previous shape released first then validated; if
    # the path was invalid, the prior binding was already orphaned
    # (no rollback) and the user got a 303 with no fix path. New
    # shape: validate first, then run release+set as a single
    # transaction so a path-validation failure leaves prior binding
    # intact.
    #
    # IMPORTANT — do NOT "refactor" this to release-first-then-bind.
    # That ordering feels obvious ("free the slot, then claim it")
    # but it's the bug class where a transient validation failure
    # leaves the user worse off than they started: old binding gone,
    # new binding rejected. Validate-then-transaction is the correct
    # shape; keep it.
    if account_path:
        valid = conn.execute(
            "SELECT 1 FROM accounts_meta "
            "WHERE account_path = ? AND closed_on IS NULL",
            (account_path,),
        ).fetchone()
        if valid is None:
            from urllib.parse import quote as _q
            return RedirectResponse(
                f"/setup/simplefin?error=unknown-path"
                f"&path={_q(account_path)}",
                status_code=303,
            )

    try:
        conn.execute("BEGIN")
        # Always release any prior binding on this simplefin_id so
        # only one account_path ever owns it.
        conn.execute(
            "UPDATE accounts_meta SET simplefin_account_id = NULL "
            "WHERE simplefin_account_id = ?",
            (simplefin_id,),
        )
        if account_path:
            conn.execute(
                "UPDATE accounts_meta SET simplefin_account_id = ? "
                "WHERE account_path = ?",
                (simplefin_id, account_path),
            )
        conn.execute("COMMIT")
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:  # noqa: BLE001
            pass
        raise

    return RedirectResponse(
        f"/setup/simplefin?bound={simplefin_id}",
        status_code=303,
    )


def _re_render_simplefin(
    request: Request, *, settings: Settings, conn,
    error_message: str | None = None,
):
    state, extras = _simplefin_state(settings, conn)
    return request.app.state.templates.TemplateResponse(
        request, "setup_simplefin.html",
        {
            "sf_state": state,
            **extras,
            "error_message": error_message,
            "step_meta": (
                {"id": "simplefin", "label": "SimpleFIN",
                 "url": "/setup/simplefin"},
            ),
            "current_step": "simplefin",
            "step_index": 0,
        },
        status_code=400 if error_message else 200,
    )


@router.get("/setup/import-rewrite", response_class=HTMLResponse)
def setup_import_rewrite_page(
    request: Request,
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Detection-only scan for non-conforming chart paths users
    may have imported from a prior Beancount setup.

    Flags:
      - Expenses / Income / Assets / Liabilities with an unknown
        second path segment (not in the registered entities list).
      - Short account paths like `Expenses:Personal` that are
        probably expense BUCKETS the user wanted to be nested under.
      - `Expenses:<entity>:Custom:*` entries (user's old catch-all
        prefix) — flagged so the user can see them and decide
        whether they map to canonical categories.

    Does NOT rewrite — that's explicitly Phase 2 work because
    rewrites are destructive across historical postings. This page
    just surfaces what COULD be rewritten and how often each
    non-conforming path is used.
    """
    from beancount.core.data import Transaction
    from collections import Counter
    from lamella.features.setup.posting_counts import open_paths as _open_paths

    entries = list(reader.load().entries)
    open_paths_set = _open_paths(entries)
    posting_counts: Counter[str] = Counter()
    for e in entries:
        if isinstance(e, Transaction):
            for p in e.postings or ():
                if p.account:
                    posting_counts[p.account] += 1

    registered_entities = {
        r["slug"] for r in conn.execute(
            "SELECT slug FROM entities WHERE is_active = 1"
        ).fetchall()
    }
    # Skip known-system segments.
    system_second = {
        "Transfers", "OpeningBalances", "FIXME", "Uncategorized",
        "Unattributed", "Clearing", "Retained", "DueFrom", "DueTo",
    }
    findings_unknown_entity: list[dict] = []
    findings_custom: list[dict] = []
    findings_short: list[dict] = []
    for path in sorted(open_paths_set):
        parts = path.split(":")
        if len(parts) < 2:
            continue
        root = parts[0]
        if root not in ("Expenses", "Income", "Assets", "Liabilities"):
            continue
        second = parts[1] if len(parts) > 1 else ""
        # Short path: Expenses:<single segment> (no category leaf) —
        # often an imported bucket.
        if len(parts) == 2 and root == "Expenses":
            findings_short.append({
                "path": path,
                "posting_count": posting_counts.get(path, 0),
                "proposed": (
                    f"Expenses:{second}:<category>"
                    if second in registered_entities
                    else None
                ),
            })
        # Custom: segment
        if len(parts) >= 3 and parts[2] == "Custom":
            findings_custom.append({
                "path": path,
                "posting_count": posting_counts.get(path, 0),
                "entity": second,
            })
        # Unknown entity (second segment not a registered entity + not system)
        if (
            second not in registered_entities
            and second not in system_second
            and root != "Equity"
        ):
            findings_unknown_entity.append({
                "path": path,
                "posting_count": posting_counts.get(path, 0),
                "unknown_segment": second,
            })

    findings_unknown_entity.sort(key=lambda r: (-r["posting_count"], r["path"]))
    findings_custom.sort(key=lambda r: (-r["posting_count"], r["path"]))
    findings_short.sort(key=lambda r: (-r["posting_count"], r["path"]))
    return request.app.state.templates.TemplateResponse(
        request, "setup_import_rewrite.html",
        {
            "unknown_entity": findings_unknown_entity[:100],
            "custom_paths": findings_custom[:100],
            "short_paths": findings_short[:100],
            "registered_entities": sorted(registered_entities),
            "unknown_entity_total": len(findings_unknown_entity),
            "custom_total": len(findings_custom),
            "short_total": len(findings_short),
        },
    )


@router.get("/setup/vehicles", response_class=HTMLResponse)
def setup_vehicles_page(
    request: Request,
    add: str | None = None,
    edit: str | None = None,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Streamlined vehicle chart check for the setup flow.

    For every registered vehicle: show whether each canonical
    Expenses:<Entity>:Vehicle:<Slug>:<Cat> account is open, offer a
    one-click "Scaffold missing" per vehicle. Also surface LEGACY
    orphan paths (Expenses:*:Custom:Vehicle*, Expenses:*:Vehicle:X:Y
    without a vehicle slug, etc.) so the user can see what needs
    to be rewritten to the canonical per-vehicle shape.

    Modal triggers:

    - ``?add=vehicle`` opens the + Add modal (Phase 4); save
      delegates to ``POST /setup/vehicles/add``.
    - ``?edit={slug}`` opens the Edit modal prefilled with the row's
      current values (Phase 8 step 6); save delegates to
      ``POST /setup/vehicles/{slug}/edit``. Slug is read-only.
      Owning entity is read-only once set — entity changes go
      through ``/vehicles/{slug}/change-ownership`` (rename for
      misattribution fix vs intercompany transfer for a real
      ownership change; not a field-edit operation).

    The full editor at ``/vehicles/{slug}/edit`` and the change-
    ownership / mileage / elections power-user pages still exist
    for state-change ops the recovery modal doesn't ship.
    """
    open_modal: str | None = None
    editing_slug: str | None = None
    edit_form_values: dict[str, str] = {}
    if add == "vehicle":
        open_modal = "vehicle"
    elif edit:
        vehicle_row = conn.execute(
            "SELECT slug, display_name, year, make, model, vin, "
            "       license_plate, entity_slug, purchase_date, "
            "       gvwr_lbs, fuel_type, notes "
            "  FROM vehicles WHERE slug = ? AND is_active = 1",
            (edit,),
        ).fetchone()
        if vehicle_row is not None:
            open_modal = "vehicle-edit"
            editing_slug = vehicle_row["slug"]
            edit_form_values = {
                k: ("" if v is None else str(v))
                for k, v in dict(vehicle_row).items()
            }
            # Vehicles don't carry a loan-link column today (the
            # tracker's "vehicle_loans" item is still deferred); the
            # Add modal's linked_loan_slug field is fire-and-forget
            # at insert time. For Edit, leave it blank — the field
            # still renders but defaults to "no loan", which is
            # informationally correct since we have no stored value
            # to round-trip.
            edit_form_values["linked_loan_slug"] = ""
    from beancount.core.data import Transaction
    from lamella.features.vehicles.vehicle_companion import (
        vehicle_chart_paths_for,
    )
    from lamella.features.setup.posting_counts import (
        already_migrated_hashes, is_override_txn, is_vehicle_orphan,
        open_paths as _open_paths,
    )
    from lamella.core.beancount_io.txn_hash import txn_hash as _txn_hash
    entries = list(reader.load().entries)
    # Opens minus Closes — showing an account as still-open after
    # it's been Closed produced the "I clicked Close and nothing
    # happened" complaint. Shared predicate: setup.posting_counts.
    open_paths_set = _open_paths(entries)

    vehicle_rows = [
        dict(r) for r in conn.execute(
            "SELECT slug, display_name, year, make, model, entity_slug, "
            "       is_active "
            "  FROM vehicles WHERE is_active = 1 ORDER BY slug"
        ).fetchall()
    ]
    vehicles_display: list[dict] = []
    for v in vehicle_rows:
        expected = vehicle_chart_paths_for(
            vehicle_slug=v["slug"], entity_slug=v["entity_slug"],
        )
        chart = [
            {
                "path": p.path,
                "purpose": p.purpose,
                "exists": p.path in open_paths_set,
            }
            for p in expected
        ]
        missing = [c for c in chart if not c["exists"]]
        vehicles_display.append({
            **v,
            "chart": chart,
            "missing_count": len(missing),
            "has_entity": bool((v["entity_slug"] or "").strip()),
        })

    # Legacy orphan scan — paths with 'vehicle' or a vehicle-model
    # word in them that DON'T follow the canonical shape. Classifier
    # consolidated into setup.posting_counts.is_vehicle_orphan so
    # this handler and setup_vehicles_close_unused_orphans agree on
    # which paths are "orphan."
    already_migrated = already_migrated_hashes(entries)

    orphans: list[dict] = []
    for acct in sorted(open_paths_set):
        if not is_vehicle_orphan(acct):
            continue
        # Count postings that (a) aren't from override txns, and
        # (b) haven't themselves been migrated via an override.
        # Same filter the close-account handler uses (§7 #5 fix).
        posting_count = 0
        for e in entries:
            if not isinstance(e, Transaction):
                continue
            if is_override_txn(e):
                continue
            if _txn_hash(e) in already_migrated:
                continue
            for p in e.postings or ():
                if p.account == acct:
                    posting_count += 1
                    break
        orphans.append({
            "path": acct,
            "posting_count": posting_count,
        })
    orphans.sort(key=lambda o: (-o["posting_count"], o["path"]))

    # Phase 4: mileage-log presence per vehicle. The user constraint
    # was per-slug CSV files but the actual storage is one
    # mileage/vehicles.csv with a vehicle column matched against the
    # vehicle's display_name. Render a badge if mileage_entries has
    # any rows for this vehicle.
    mileage_counts: dict[str, int] = {}
    try:
        for v in vehicles_display:
            label = v.get("display_name") or v.get("slug") or ""
            if not label:
                continue
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM mileage_entries WHERE vehicle = ?",
                (label,),
            ).fetchone()
            mileage_counts[v["slug"]] = int(row["n"]) if row else 0
    except Exception:  # noqa: BLE001
        mileage_counts = {}
    for v in vehicles_display:
        v["mileage_count"] = mileage_counts.get(v["slug"], 0)

    # Modal data: entity slugs (datalist), real loans (dropdown),
    # mileage CSV presence (so the user knows their import worked).
    entity_slugs = [
        r["slug"] for r in conn.execute(
            "SELECT slug FROM entities WHERE is_active = 1 ORDER BY slug"
        ).fetchall()
    ]
    # Vehicle modal's linked-loan dropdown: filter to vehicle-shaped
    # loan kinds. Mortgages and HELOCs aren't vehicle loans; showing
    # them in this selector would be a UX weed. Mirrors the property
    # modal's loan_type filter for symmetry.
    loans_for_link = [
        dict(r) for r in conn.execute(
            "SELECT slug, display_name, institution, entity_slug, loan_type "
            "  FROM loans "
            " WHERE is_active = 1 "
            "   AND loan_type IN ('auto', 'personal', 'student', 'eidl', 'other') "
            " ORDER BY slug"
        ).fetchall()
    ]
    # Mileage CSV convention: one shared file, not per-slug.
    # ``ledger_dir/mileage/vehicles.csv`` holds rows for every
    # vehicle, with a ``vehicle`` column matched against
    # display_name. Despite the install-tree allowlist pattern
    # ``mileage/*.csv`` (in bootstrap/import_apply.py), the only
    # file actually consulted at runtime is the singular
    # vehicles.csv. If a future contributor adds per-slug files,
    # also update mileage/csv_store.py and config.py:188.
    mileage_csv_present = (
        settings.ledger_dir / "mileage" / "vehicles.csv"
    ).exists()

    return request.app.state.templates.TemplateResponse(
        request, "setup_vehicles.html",
        {
            "vehicles": vehicles_display,
            "orphans": orphans,
            "orphan_count": len(orphans),
            "total_vehicles": len(vehicle_rows),
            "missing_charts_total": sum(
                v["missing_count"] for v in vehicles_display
            ),
            "open_modal": open_modal,
            "editing_slug": editing_slug,
            "entity_slugs": entity_slugs,
            "loans_for_link": loans_for_link,
            "mileage_csv_present": mileage_csv_present,
            "field_errors": {},
            "form_values": edit_form_values,
            "step_meta": (
                {"id": "vehicles", "label": "Vehicles",
                 "url": "/setup/vehicles"},
            ),
            "current_step": "vehicles",
            "step_index": 0,
        },
    )


# ---------------------------------------------------------------------------
# Phase 4 of /setup/recovery: + Add vehicle modal save handler.
#
# Locked constraints (per the brief):
#   - linked_loan_slug is a dropdown from real loans + an explicit
#     "no loan" option. Free-text entry is rejected — the dropdown
#     pulls from the loans table on render.
#   - NO auto-scaffold of per-vehicle accounts. The
#     Assets:{Entity}:Vehicle:{slug} + Expenses subtree get created
#     by the existing Scaffold-N-missing button on the vehicle row,
#     not at vehicle-creation time. Two-step on purpose so the user
#     confirms intent before the chart materializes.
#   - Mileage CSV linkage is read-only on this page (badge shown if
#     mileage/vehicles.csv has rows for this vehicle's display_name).
# ---------------------------------------------------------------------------


_VEHICLE_FUEL_TYPES = ("gasoline", "diesel", "ev", "phev", "hybrid", "other")


def _re_render_vehicle_modal(
    request: Request,
    *,
    settings: Settings,
    conn,
    reader,
    field_errors: dict[str, str],
    form_values: dict[str, str],
    editing_slug: str | None = None,
):
    """Re-render setup_vehicles.html with the modal still open and
    typed values preserved. Mirror of _re_render_loan_modal /
    _re_render_entity_modal.

    ``editing_slug`` non-None → edit mode (modal renders the Edit
    title + posts to ``/setup/vehicles/{slug}/edit``); None → add
    mode."""
    # Cheap context — full vehicle list / orphan scan would re-query
    # the ledger. Acceptable here because validation failures are
    # rare and we want predictable timing on the modal re-render.
    vehicles_display: list[dict] = []
    orphans: list[dict] = []
    entity_slugs = [
        r["slug"] for r in conn.execute(
            "SELECT slug FROM entities WHERE is_active = 1 ORDER BY slug"
        ).fetchall()
    ]
    # Same vehicle-loan filter as the GET handler — auto/personal/etc.,
    # excluding mortgage/heloc.
    loans_for_link = [
        dict(r) for r in conn.execute(
            "SELECT slug, display_name, institution, entity_slug, loan_type "
            "  FROM loans "
            " WHERE is_active = 1 "
            "   AND loan_type IN ('auto', 'personal', 'student', 'eidl', 'other') "
            " ORDER BY slug"
        ).fetchall()
    ]
    return request.app.state.templates.TemplateResponse(
        request, "setup_vehicles.html",
        {
            "vehicles": vehicles_display,
            "orphans": orphans,
            "orphan_count": 0,
            "total_vehicles": 0,
            "missing_charts_total": 0,
            "open_modal": "vehicle-edit" if editing_slug else "vehicle",
            "editing_slug": editing_slug,
            "entity_slugs": entity_slugs,
            "loans_for_link": loans_for_link,
            "mileage_csv_present": (
                settings.ledger_dir / "mileage" / "vehicles.csv"
            ).exists(),
            "field_errors": field_errors,
            "form_values": form_values,
            "step_meta": (
                {"id": "vehicles", "label": "Vehicles",
                 "url": "/setup/vehicles"},
            ),
            "current_step": "vehicles",
            "step_index": 0,
        },
        status_code=400,
    )


@router.post("/setup/vehicles/add", response_class=HTMLResponse)
async def setup_vehicle_add(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Create a new vehicle from the modal. Required: display_name,
    entity_slug. Optional: year, make, model, vin, license_plate,
    purchase_date, fuel_type, gvwr_lbs, linked_loan_slug, notes.

    NOT auto-scaffolded: the per-vehicle chart of Expenses subtrees.
    The user clicks Scaffold-N-missing on the vehicle row when ready.
    """
    from fastapi.responses import RedirectResponse
    from lamella.core.registry.service import (
        normalize_slug, disambiguate_slug,
    )

    form = await request.form()
    display_name = (form.get("display_name") or "").strip()
    slug_typed = (form.get("slug") or "").strip()
    entity_slug = (form.get("entity_slug") or "").strip()
    year_raw = (form.get("year") or "").strip()
    make = (form.get("make") or "").strip()
    model = (form.get("model") or "").strip()
    vin = (form.get("vin") or "").strip()
    license_plate = (form.get("license_plate") or "").strip()
    purchase_date = (form.get("purchase_date") or "").strip()
    fuel_type = (form.get("fuel_type") or "").strip().lower()
    gvwr_raw = (form.get("gvwr_lbs") or "").strip()
    # linked_loan_slug uses an explicit "" sentinel for "no loan".
    # Anything else must match a real loan row (validated below).
    linked_loan_slug = (form.get("linked_loan_slug") or "").strip()
    notes = (form.get("notes") or "").strip()

    field_errors: dict[str, str] = {}
    if not display_name:
        field_errors["display_name"] = "Required."
    if not entity_slug:
        field_errors["entity_slug"] = "Required."

    slug = normalize_slug(slug_typed, fallback_display_name=display_name)
    if not slug:
        field_errors["slug"] = (
            "Couldn't derive a valid slug — type a display name or "
            "an explicit slug starting with a capital letter."
        )

    year: int | None = None
    if year_raw:
        try:
            year = int(year_raw)
            if year < 1900 or year > 2100:
                raise ValueError
        except ValueError:
            field_errors["year"] = "Year must be a 4-digit number."

    if fuel_type and fuel_type not in _VEHICLE_FUEL_TYPES:
        field_errors["fuel_type"] = (
            f"Unknown fuel type: {fuel_type!r}. "
            f"Pick one of: {', '.join(_VEHICLE_FUEL_TYPES)}."
        )

    gvwr_lbs: int | None = None
    if gvwr_raw:
        try:
            gvwr_lbs = int(gvwr_raw)
        except ValueError:
            field_errors["gvwr_lbs"] = "GVWR must be a whole number of pounds."

    # linked_loan_slug validation: blank means "no loan", otherwise
    # must reference a real loans row WHOSE TYPE IS VEHICLE-SHAPED
    # AND whose entity matches the vehicle's. Mirrors the property
    # modal's entity-match check — mixed-entity links produce broken
    # account hierarchies on reconstruct.
    if linked_loan_slug:
        existing_loan = conn.execute(
            "SELECT slug, entity_slug FROM loans "
            " WHERE slug = ? AND is_active = 1 "
            "   AND loan_type IN ('auto', 'personal', 'student', 'eidl', 'other')",
            (linked_loan_slug,),
        ).fetchone()
        if existing_loan is None:
            field_errors["linked_loan_slug"] = (
                f"No active vehicle-shaped loan with slug "
                f"{linked_loan_slug!r}. Pick an auto/personal/student "
                "loan from the dropdown, or leave it as 'No loan'."
            )
        elif (
            entity_slug
            and existing_loan["entity_slug"]
            and existing_loan["entity_slug"] != entity_slug
        ):
            field_errors["linked_loan_slug"] = (
                f"Loan {linked_loan_slug!r} belongs to entity "
                f"{existing_loan['entity_slug']!r}, but this vehicle "
                f"is owned by {entity_slug!r}. Pick a loan with "
                "matching entity, or change the vehicle's owning "
                "entity above."
            )

    form_values_payload = {
        "display_name": display_name, "slug": slug_typed,
        "entity_slug": entity_slug, "year": year_raw,
        "make": make, "model": model, "vin": vin,
        "license_plate": license_plate,
        "purchase_date": purchase_date, "fuel_type": fuel_type,
        "gvwr_lbs": gvwr_raw,
        "linked_loan_slug": linked_loan_slug, "notes": notes,
    }

    if not field_errors and slug:
        existing = conn.execute(
            "SELECT slug FROM vehicles WHERE slug = ?", (slug,),
        ).fetchone()
        if existing is not None:
            suggested = disambiguate_slug(conn, slug, "vehicles")
            field_errors["slug"] = (
                f'Vehicle slug "{slug}" is already taken. '
                f'Try "{suggested}" instead, or edit the existing vehicle.'
            )

    if field_errors:
        return _re_render_vehicle_modal(
            request, settings=settings, conn=conn, reader=reader,
            field_errors=field_errors, form_values=form_values_payload,
        )

    # INSERT vehicle row. Per the constraint: no auto-scaffold of
    # per-vehicle accounts. The asset_account_path is computed
    # canonical-shape so the row carries the path the future scaffold
    # button will open, but no Open directive is written here.
    canonical_asset_path = (
        f"Assets:{entity_slug}:Vehicle:{slug}" if entity_slug else None
    )
    conn.execute(
        """
        INSERT INTO vehicles
            (slug, display_name, year, make, model, vin, license_plate,
             entity_slug, purchase_date, asset_account_path,
             gvwr_lbs, fuel_type, is_active, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
        """,
        (
            slug, display_name, year, make or None, model or None,
            vin or None, license_plate or None, entity_slug,
            purchase_date or None, canonical_asset_path,
            gvwr_lbs, fuel_type or None, notes or None,
        ),
    )

    # Note: linked_loan_slug recording is deferred. The vehicles
    # table doesn't carry a loan FK column today; a future migration
    # adds vehicle_loans (or a property_slug-style column on loans)
    # and the link gets persisted. For now, surfacing the dropdown
    # is the user-facing affordance even if the link doesn't round-
    # trip yet — the field is in the form and validated against
    # active loans, so future code can pick it up.

    request.app.state.setup_required_complete = False
    return RedirectResponse(
        f"/setup/vehicles?added={slug}", status_code=303,
    )


# ---------------------------------------------------------------------------
# Phase 8 step 6: + Edit vehicle modal save handler.
#
# Twin of setup_vehicle_add. Same field set; UPDATEs instead of
# INSERTs. Slug is path-bound (immutable identifier). Entity is
# locked once set — entity transitions are intercompany transfers
# handled by /vehicles/{slug}/change-ownership (rename for
# misattribution fix vs intercompany transfer for a real ownership
# change). Mirrors the test_vehicle_edit_cannot_change_entity_slug
# invariant on the /vehicles save handler.
#
# Linked-loan handling: the vehicles table has no loan-link column
# today (the tracker's "vehicle_loans" item remains deferred). The
# Add modal's linked_loan_slug field is fire-and-forget; for
# parity, the Edit modal accepts it but doesn't persist it. When
# vehicle_loans ships, both Add + Edit pick it up by writing to
# the new column.
# ---------------------------------------------------------------------------


@router.post("/setup/vehicles/{slug}/edit", response_class=HTMLResponse)
async def setup_vehicle_edit(
    request: Request,
    slug: str,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Update an existing vehicle from the per-row Edit modal.

    Slug comes from the path (immutable). Entity is silently
    preserved if currently set. Validation failure re-renders;
    success → 303 redirect to ``/setup/vehicles?updated={slug}``.

    404 if the slug doesn't match an active vehicle.
    """
    from fastapi.responses import RedirectResponse

    existing = conn.execute(
        "SELECT slug, entity_slug FROM vehicles "
        " WHERE slug = ? AND is_active = 1",
        (slug,),
    ).fetchone()
    if existing is None:
        raise HTTPException(
            status_code=404,
            detail=f"Vehicle {slug!r} not found or inactive.",
        )
    existing_entity = (existing["entity_slug"] or "").strip()

    form = await request.form()
    display_name = (form.get("display_name") or "").strip()
    entity_slug_typed = (form.get("entity_slug") or "").strip()
    year_raw = (form.get("year") or "").strip()
    make = (form.get("make") or "").strip()
    model = (form.get("model") or "").strip()
    vin = (form.get("vin") or "").strip()
    license_plate = (form.get("license_plate") or "").strip()
    purchase_date = (form.get("purchase_date") or "").strip()
    fuel_type = (form.get("fuel_type") or "").strip().lower()
    gvwr_raw = (form.get("gvwr_lbs") or "").strip()
    linked_loan_slug = (form.get("linked_loan_slug") or "").strip()
    notes = (form.get("notes") or "").strip()

    # Entity-locking: once set, the field is preserved regardless of
    # what the form posted. Mirrors test_vehicle_edit_cannot_change
    # _entity_slug — entity transitions go through
    # /vehicles/{slug}/change-ownership (rename + intercompany
    # transfer flows), not field edits.
    entity_slug = existing_entity or entity_slug_typed

    field_errors: dict[str, str] = {}
    if not display_name:
        field_errors["display_name"] = "Required."
    if not entity_slug:
        field_errors["entity_slug"] = "Required."

    year: int | None = None
    if year_raw:
        try:
            year = int(year_raw)
            if year < 1900 or year > 2100:
                raise ValueError
        except ValueError:
            field_errors["year"] = "Year must be a 4-digit number."

    if fuel_type and fuel_type not in _VEHICLE_FUEL_TYPES:
        field_errors["fuel_type"] = (
            f"Unknown fuel type: {fuel_type!r}. "
            f"Pick one of: {', '.join(_VEHICLE_FUEL_TYPES)}."
        )

    gvwr_lbs: int | None = None
    if gvwr_raw:
        try:
            gvwr_lbs = int(gvwr_raw)
        except ValueError:
            field_errors["gvwr_lbs"] = "GVWR must be a whole number of pounds."

    # Linked-loan validation: same shape as Add (vehicle-shaped
    # loan kinds, entity-match required).
    if linked_loan_slug:
        existing_loan = conn.execute(
            "SELECT slug, entity_slug FROM loans "
            " WHERE slug = ? AND is_active = 1 "
            "   AND loan_type IN ('auto', 'personal', 'student', 'eidl', 'other')",
            (linked_loan_slug,),
        ).fetchone()
        if existing_loan is None:
            field_errors["linked_loan_slug"] = (
                f"No active vehicle-shaped loan with slug "
                f"{linked_loan_slug!r}. Pick an auto/personal/student "
                "loan from the dropdown, or leave it as 'No loan'."
            )
        elif (
            entity_slug
            and existing_loan["entity_slug"]
            and existing_loan["entity_slug"] != entity_slug
        ):
            field_errors["linked_loan_slug"] = (
                f"Loan {linked_loan_slug!r} belongs to entity "
                f"{existing_loan['entity_slug']!r}, but this vehicle "
                f"is owned by {entity_slug!r}. Mixed-entity links "
                "produce broken account hierarchies."
            )

    form_values_payload = {
        "display_name": display_name, "slug": slug,
        "entity_slug": entity_slug, "year": year_raw,
        "make": make, "model": model, "vin": vin,
        "license_plate": license_plate,
        "purchase_date": purchase_date, "fuel_type": fuel_type,
        "gvwr_lbs": gvwr_raw,
        "linked_loan_slug": linked_loan_slug, "notes": notes,
    }

    if field_errors:
        return _re_render_vehicle_modal(
            request, settings=settings, conn=conn, reader=reader,
            field_errors=field_errors,
            form_values=form_values_payload,
            editing_slug=slug,
        )

    canonical_asset_path = (
        f"Assets:{entity_slug}:Vehicle:{slug}" if entity_slug else None
    )

    conn.execute(
        """
        UPDATE vehicles SET
            display_name = ?,
            year = ?,
            make = ?,
            model = ?,
            vin = ?,
            license_plate = ?,
            entity_slug = ?,
            purchase_date = ?,
            asset_account_path = COALESCE(NULLIF(?, ''), asset_account_path),
            gvwr_lbs = ?,
            fuel_type = ?,
            notes = ?
        WHERE slug = ?
        """,
        (
            display_name, year, make or None, model or None,
            vin or None, license_plate or None, entity_slug,
            purchase_date or None,
            canonical_asset_path or "",
            gvwr_lbs, fuel_type or None, notes or None,
            slug,
        ),
    )

    # linked_loan_slug intentionally not persisted — see the section
    # comment above for the deferred vehicle_loans column note.

    request.app.state.setup_required_complete = False
    return RedirectResponse(
        f"/setup/vehicles?updated={slug}", status_code=303,
    )


@router.post("/setup/vehicles/close-unused-orphans")
def setup_vehicles_close_unused_orphans(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Bulk-close every orphan vehicle-ish account with 0 postings.
    Recomputes the orphan list (same logic as setup_vehicles_page),
    filters to the ones with no unmigrated postings, and closes each
    via the same safe path as /setup/accounts/close (detects existing
    Close + existing Open, writes or drops stale meta accordingly).
    Single bean-check run at the end; failure rolls back the whole
    batch."""
    from datetime import date as _date_t
    from beancount.core.data import Close, Open, Transaction
    from lamella.core.ledger_writer import (
        BeanCheckError, capture_bean_check, run_bean_check_vs_baseline,
    )
    from lamella.features.setup.posting_counts import (
        already_migrated_hashes, is_override_txn, is_vehicle_orphan,
        open_paths as _open_paths,
    )
    from lamella.core.beancount_io.txn_hash import txn_hash as _tx_hash

    try:
        load = reader.load()
    except Exception as exc:  # noqa: BLE001
        log.warning("ledger load failed during bulk close-unused-orphans: %s", exc)
        return RedirectResponse("/setup/vehicles?error=load-failed", status_code=303)

    entries = list(load.entries)
    open_paths_set = _open_paths(entries)
    close_paths = {e.account for e in entries if isinstance(e, Close)}
    # Count only real, unmigrated postings against the "used" set.
    # Including override postings caused the §7 #5-shape bug where a
    # fully-migrated orphan still appeared "used" (the override's
    # from-account posting kept it in the set), so bulk-close silently
    # did nothing. Shared filter: setup.posting_counts.
    already_migrated = already_migrated_hashes(entries)
    used: set[str] = set()
    for e in entries:
        if not isinstance(e, Transaction):
            continue
        if is_override_txn(e):
            continue
        if _tx_hash(e) in already_migrated:
            continue
        for p in e.postings or ():
            used.add(p.account)

    to_close: list[str] = []
    to_drop_meta: list[str] = []
    for acct in sorted(open_paths_set):
        if not is_vehicle_orphan(acct):
            continue
        if acct in used:
            continue
        if acct in close_paths:
            # Already closed in the ledger; just sync cache if stale.
            continue
        to_close.append(acct)
    # Also: accounts_meta rows with stale paths that never had an Open
    meta_rows = conn.execute(
        "SELECT account_path FROM accounts_meta WHERE closed_on IS NULL"
    ).fetchall()
    for r in meta_rows:
        path = r["account_path"]
        if (
            is_vehicle_orphan(path)
            and path not in open_paths_set
            and path not in used
        ):
            to_drop_meta.append(path)

    if not to_close and not to_drop_meta:
        return RedirectResponse(
            "/setup/vehicles?info=no-unused-orphans-found", status_code=303,
        )

    today = _date_t.today().isoformat()
    if to_close:
        accounts_path = settings.connector_accounts_path
        accounts_path.parent.mkdir(parents=True, exist_ok=True)
        if not accounts_path.exists():
            accounts_path.write_text(
                "; connector_accounts.bean — managed by Lamella.\n",
                encoding="utf-8",
            )
        # Baseline + snapshots BEFORE the write (Phase 1.2 pattern).
        backup_accounts = accounts_path.read_bytes()
        backup_main = settings.ledger_main.read_bytes()
        _c, baseline_output = capture_bean_check(settings.ledger_main)
        new_text = (
            accounts_path.read_text(encoding="utf-8").rstrip()
            + "\n\n; Setup: close unused vehicle orphans\n"
            + "\n".join(f"{today} close {p}" for p in to_close)
            + "\n"
        )
        try:
            accounts_path.write_text(new_text, encoding="utf-8")
            run_bean_check_vs_baseline(settings.ledger_main, baseline_output)
        except BeanCheckError as exc:
            accounts_path.write_bytes(backup_accounts)
            settings.ledger_main.write_bytes(backup_main)
            return RedirectResponse(
                f"/setup/vehicles?error=bean-check-{exc}", status_code=303,
            )
        except Exception as exc:  # noqa: BLE001
            accounts_path.write_bytes(backup_accounts)
            settings.ledger_main.write_bytes(backup_main)
            log.exception("bulk close-unused-orphans failed")
            return RedirectResponse(
                f"/setup/vehicles?error={type(exc).__name__}", status_code=303,
            )
        for p in to_close:
            conn.execute(
                "UPDATE accounts_meta SET closed_on = ? WHERE account_path = ?",
                (today, p),
            )
        reader.invalidate()

    for p in to_drop_meta:
        conn.execute("DELETE FROM accounts_meta WHERE account_path = ?", (p,))

    total = len(to_close) + len(to_drop_meta)
    return RedirectResponse(
        f"/setup/vehicles?info=closed-{total}-unused-orphans",
        status_code=303,
    )


@router.get("/setup/vehicles/{slug}/migrate", response_class=HTMLResponse)
def setup_vehicle_migrate_page(
    slug: str,
    request: Request,
    orphan: str | None = None,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Show a form to migrate postings on a legacy vehicle-ish path
    to a canonical ``Expenses:<Entity>:Vehicle:<Slug>:<Category>``
    account. User picks the category; the POST writes an override
    per txn that redirects the historical posting to the canonical
    path.
    """
    from beancount.core.data import Transaction
    from lamella.core.beancount_io.txn_hash import txn_hash
    from lamella.features.vehicles.vehicle_companion import (
        vehicle_chart_paths_for, VEHICLE_CHART_CATEGORIES,
    )
    from lamella.features.setup.posting_counts import (
        already_migrated_hashes, is_override_txn,
    )

    row = conn.execute(
        "SELECT slug, display_name, entity_slug FROM vehicles "
        "WHERE slug = ?", (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"vehicle {slug!r} not found")
    entity_slug = row["entity_slug"]
    if not entity_slug:
        return request.app.state.templates.TemplateResponse(
            request, "setup_vehicle_migrate.html",
            {
                "vehicle": dict(row),
                "orphan": orphan or "",
                "error": "Vehicle has no entity_slug set — can't scaffold canonical accounts.",
                "affected": [],
                "categories": VEHICLE_CHART_CATEGORIES,
                "target_options": [],
            },
        )
    # Find every transaction with a posting on the orphan path that
    # STILL NEEDS migration. Two-step filter:
    #   1. Skip #lamella-override txns themselves (our own prior writes
    #      would create an override-of-override chain otherwise).
    #   2. Skip txns whose hash is already referenced by an existing
    #      override block (lamella-override-of metadata) — they're
    #      already migrated, showing them in the "needs migration"
    #      list is the bug that left the user seeing 49 after
    #      migration instead of 0.
    affected: list[dict] = []
    if orphan:
        entries_list = list(reader.load().entries)
        already_migrated = already_migrated_hashes(entries_list)
        for e in entries_list:
            if not isinstance(e, Transaction):
                continue
            if is_override_txn(e):
                continue
            h = txn_hash(e)
            if h in already_migrated:
                continue
            for p in e.postings or ():
                if p.account == orphan:
                    amt = None
                    ccy = "USD"
                    if p.units and p.units.number is not None:
                        from decimal import Decimal as _D
                        amt = _D(p.units.number)
                        ccy = p.units.currency or "USD"
                    affected.append({
                        "txn_hash": h,
                        "date": str(e.date),
                        "payee": getattr(e, "payee", None) or "",
                        "narration": (e.narration or "")[:80],
                        "amount": amt,
                        "currency": ccy,
                    })
                    break
    # What canonical targets would the user pick from?
    targets = vehicle_chart_paths_for(
        vehicle_slug=slug, entity_slug=entity_slug,
    )
    expense_targets = [
        {"path": t.path, "purpose": t.purpose}
        for t in targets if t.path.startswith("Expenses:")
    ]
    return request.app.state.templates.TemplateResponse(
        request, "setup_vehicle_migrate.html",
        {
            "vehicle": dict(row),
            "orphan": orphan or "",
            "affected": affected,
            "categories": VEHICLE_CHART_CATEGORIES,
            "target_options": expense_targets,
            "error": None,
        },
    )


@router.post("/setup/vehicles/{slug}/migrate")
async def setup_vehicle_migrate_apply(
    slug: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Apply the migration: for every selected txn, write an
    override that redirects the orphan-path posting to the chosen
    canonical target.

    Each ``OverrideWriter.append`` runs a backup → write → bean-check
    → revert envelope, so the per-row cost on a real ledger is a
    couple of seconds. For a 50-row migrate that's a 1-2 minute
    handler; HTMX clients get the standard
    ``app.state.job_runner`` modal so they see live progress per
    row instead of staring at a spinning tab. Plain (non-HTMX) POSTs
    keep the synchronous redirect contract that the test suite
    relies on.
    """
    from datetime import date as _date_t
    from decimal import Decimal as _D
    from fastapi.responses import RedirectResponse
    from beancount.core.data import Transaction
    from lamella.core.beancount_io.txn_hash import txn_hash
    from urllib.parse import quote as _q

    form = await request.form()
    orphan_path = (form.get("orphan") or "").strip()
    target = (form.get("target") or "").strip()
    hashes = [
        str(v).strip() for (k, v) in form.multi_items()
        if k == "txn_hash" and str(v).strip()
    ]
    if not orphan_path or not target or not hashes:
        # HTMX-callable per ADR-0037 + routes/CLAUDE.md.
        return _hx_aware_error_redirect(
            request,
            f"/setup/vehicles/{slug}/migrate?orphan={orphan_path}",
            "missing-input",
        )

    # First pass: find the earliest date among migration txns so we
    # can backdate the target's Open if necessary. Without this, bean-
    # check rejects the whole migration when the target was scaffolded
    # with today's date but the migration txns are historical.
    entries = list(reader.load().entries)
    hashes_set = set(hashes)
    earliest_date: _date_t | None = None
    for e in entries:
        if not isinstance(e, Transaction):
            continue
        h = txn_hash(e)
        if h not in hashes_set:
            continue
        d = e.date if isinstance(e.date, _date_t) else _date_t.fromisoformat(str(e.date))
        if earliest_date is None or d < earliest_date:
            earliest_date = d
    if earliest_date is not None:
        ok = _ensure_open_covers(settings, reader, target, earliest_date)
        if not ok:
            # HTMX-callable per ADR-0037 + routes/CLAUDE.md.
            return _hx_aware_error_redirect(
                request,
                f"/setup/vehicles/{slug}/migrate?orphan={orphan_path}",
                f"target-{target}-open-too-late-(earliest-txn-{earliest_date.isoformat()})",
            )
        # Reload entries so the by_hash below uses a post-backdate view.
        entries = list(reader.load().entries)

    # Resolve every selected hash to the data the writer needs. Doing
    # this once upfront means the (possibly-backgrounded) write loop
    # doesn't have to keep the parsed ledger in scope.
    rows: list[dict] = []
    rows_for_hash: dict[str, dict] = {}
    for e in entries:
        if not isinstance(e, Transaction):
            continue
        h = txn_hash(e)
        if h not in hashes_set:
            continue
        orphan_amount: _D | None = None
        currency = "USD"
        for p in e.postings or ():
            if p.account == orphan_path and p.units and p.units.number is not None:
                orphan_amount = _D(p.units.number)
                currency = p.units.currency or "USD"
                break
        rows_for_hash[h] = {
            "txn_hash": h,
            "txn_date": (
                e.date.isoformat() if isinstance(e.date, _date_t)
                else str(e.date)
            ),
            "amount": str(orphan_amount) if orphan_amount is not None else None,
            "currency": currency,
            "narration": (e.narration or f"vehicle migration → {target}"),
            "missing_posting": orphan_amount is None,
        }
    for h in hashes:
        rows.append(rows_for_hash.get(h, {
            "txn_hash": h, "txn_date": None, "amount": None,
            "currency": "USD", "narration": "",
            "missing_posting": True, "not_in_ledger": True,
        }))

    if _is_htmx(request):
        settings_payload = {
            "ledger_main": str(settings.ledger_main),
            "connector_overrides_path": str(settings.connector_overrides_path),
            "connector_accounts_path": str(settings.connector_accounts_path),
        }
        job_runner = request.app.state.job_runner
        job_id = job_runner.submit(
            kind="vehicle-migrate",
            title=(
                f"Migrate {len(rows)} posting"
                f"{'' if len(rows) == 1 else 's'} → {target}"
            ),
            fn=lambda ctx: _vehicle_migrate_worker(
                ctx,
                rows=rows,
                orphan_path=orphan_path,
                target=target,
                slug=slug,
                settings_payload=settings_payload,
            ),
            total=len(rows),
            meta={
                "slug": slug, "orphan": orphan_path, "target": target,
                "rows_count": len(rows),
            },
            return_url="/setup/vehicles",
        )
        return request.app.state.templates.TemplateResponse(
            request,
            "partials/_job_modal.html",
            {"job_id": job_id, "on_close_url": "/setup/vehicles"},
        )

    # ---- non-HTMX (sync) path: matches the long-standing contract
    # tests + curl/scripts rely on. Same bytes touched as the worker
    # below, just inline.
    applied, failed, auto_closed, auto_close_error = _vehicle_migrate_apply_sync(
        settings=settings,
        conn=conn,
        reader=reader,
        rows=rows,
        orphan_path=orphan_path,
        target=target,
    )
    qs = f"migrated={applied}&failed={len(failed)}"
    if auto_closed:
        qs += f"&auto_closed={_q(auto_closed)}"
    if auto_close_error:
        qs += f"&auto_close_error={_q(auto_close_error)}"
    return RedirectResponse(f"/setup/vehicles?{qs}", status_code=303)


def _vehicle_migrate_apply_sync(
    *,
    settings,
    conn,
    reader,
    rows: list[dict],
    orphan_path: str,
    target: str,
    on_progress=None,
):
    """Shared write loop for the vehicle migrate flow. Returns
    ``(applied, failed, auto_closed, auto_close_error)``.

    Uses ``OverrideWriter.append_batch`` so the slow ``bean-check``
    pass runs ONCE for the whole batch instead of N times. Pre-batch
    validation (txn-not-in-ledger, missing-posting-on-orphan) is
    cheap and stays inline; only valid rows enter the batch.

    ``on_progress(idx, total, row, outcome, message)`` is called
    once per row so the job worker can stream events into the
    modal. ``outcome`` ∈ ``{"info","success","failure","error"}``;
    a final ``"info"`` event is fired around the bean-check step
    so the modal shows the slow part as an explicit phase.
    """
    from datetime import date as _date_t
    from decimal import Decimal as _D
    from lamella.features.rules.overrides import OverrideWriter
    from lamella.core.ledger_writer import BeanCheckError

    writer = OverrideWriter(
        main_bean=settings.ledger_main,
        overrides=settings.connector_overrides_path,
        conn=conn,
    )
    failed: list[str] = []
    total = len(rows)

    # Split rows into "valid for batch" vs "skip with reason".
    batch_rows: list[dict] = []
    batch_indices: list[int] = []
    for idx, row in enumerate(rows):
        h = row["txn_hash"]
        if row.get("not_in_ledger"):
            msg = f"{h[:8]}… not in ledger"
            failed.append(msg)
            if on_progress:
                on_progress(idx, total, row, "error", msg)
            continue
        if row.get("missing_posting") or row.get("amount") is None:
            msg = f"{h[:8]}… has no posting on {orphan_path}"
            failed.append(msg)
            if on_progress:
                on_progress(idx, total, row, "error", msg)
            continue
        try:
            txn_date_val = _date_t.fromisoformat(row["txn_date"])
            amount = abs(_D(row["amount"]))
        except Exception as exc:  # noqa: BLE001
            msg = f"{h[:8]}… bad row data: {type(exc).__name__}: {exc}"
            failed.append(msg)
            if on_progress:
                on_progress(idx, total, row, "error", msg)
            continue
        batch_rows.append({
            "txn_date": txn_date_val,
            "txn_hash": h,
            "amount": amount,
            "from_account": orphan_path,
            "to_account": target,
            "currency": row.get("currency") or "USD",
            "narration": row.get("narration") or f"vehicle migration → {target}",
            "_idx": idx,
            "_orig_row": row,
        })
        batch_indices.append(idx)

    applied = 0
    if batch_rows:
        def _on_staged(batch_idx, batch_total, batch_row):
            if on_progress is None:
                return
            orig_idx = batch_row["_idx"]
            orig_row = batch_row["_orig_row"]
            on_progress(
                orig_idx, total, orig_row, "success",
                (
                    f"Staged {batch_row['txn_hash'][:8]}… "
                    f"{batch_row['txn_date'].isoformat()} "
                    f"{batch_row['amount']}"
                ),
            )

        if on_progress:
            on_progress(
                0, total, batch_rows[0]["_orig_row"], "info",
                f"Running bean-check on the {len(batch_rows)}-row batch …",
            )
        try:
            applied, _blocks, _skipped = writer.append_batch(
                batch_rows, on_row_staged=_on_staged,
            )
            if on_progress:
                on_progress(
                    total - 1, total, batch_rows[-1]["_orig_row"], "info",
                    f"bean-check passed · {applied} block(s) committed",
                )
        except BeanCheckError as exc:
            # The whole batch was rolled back — report every staged row
            # as failed so the modal counters match reality.
            for br in batch_rows:
                msg = f"{br['txn_hash'][:8]}… bean-check blocked (batch rolled back)"
                failed.append(msg)
                if on_progress:
                    on_progress(
                        br["_idx"], total, br["_orig_row"], "failure", msg,
                    )
            if on_progress:
                on_progress(
                    total - 1, total, batch_rows[-1]["_orig_row"], "failure",
                    f"bean-check rejected the batch: {exc}",
                )
        except Exception as exc:  # noqa: BLE001
            log.exception("vehicle migration batch failed")
            for br in batch_rows:
                msg = (
                    f"{br['txn_hash'][:8]}… {type(exc).__name__}: "
                    f"{exc} (batch rolled back)"
                )
                failed.append(msg)
                if on_progress:
                    on_progress(
                        br["_idx"], total, br["_orig_row"], "error", msg,
                    )
    reader.invalidate()

    # Phase 3.1: if the migration was clean (everything applied, nothing
    # failed) and the orphan now has zero unmigrated postings, auto-write
    # a Close directive so the orphan stops surfacing on /setup/vehicles
    # and /setup/accounts. The user just told us the orphan was wrong by
    # migrating every posting off it — the natural next step is to close
    # the dead account, and forcing a second click for the obvious answer
    # is friction. Only fires on the all-applied / zero-failed path; a
    # partial batch leaves the close decision to the user.
    auto_closed = ""
    auto_close_error = ""
    if applied > 0 and not failed:
        try:
            outcome = _auto_close_orphan_path_if_drained(
                orphan_path, settings, reader,
            )
            if outcome == "closed":
                auto_closed = orphan_path
        except _AutoCloseFailed as exc:
            auto_close_error = str(exc)
            log.warning(
                "auto-close after vehicle migrate failed for %s: %s",
                orphan_path, exc,
            )
    return applied, failed, auto_closed, auto_close_error


def _vehicle_migrate_worker(
    ctx,
    *,
    rows: list[dict],
    orphan_path: str,
    target: str,
    slug: str,
    settings_payload: dict,
) -> dict:
    """JobRunner worker for the vehicle migrate flow. Recreates the
    settings + reader from JSON-safe payloads (the request-scoped
    objects don't survive the thread hop) and delegates to the
    shared sync loop.
    """
    from types import SimpleNamespace
    from urllib.parse import quote as _q

    settings = SimpleNamespace(
        ledger_main=Path(settings_payload["ledger_main"]),
        connector_overrides_path=Path(settings_payload["connector_overrides_path"]),
        connector_accounts_path=Path(settings_payload["connector_accounts_path"]),
    )
    reader = LedgerReader(settings.ledger_main)

    ctx.set_total(len(rows))
    ctx.emit(
        f"Migrating {len(rows)} posting"
        f"{'' if len(rows) == 1 else 's'} from {orphan_path} → {target}",
        outcome="info",
    )

    def _on_progress(idx, total, row, outcome, message):
        ctx.raise_if_cancelled()
        ctx.emit(message, outcome=outcome)
        # "info" events are phase markers (e.g. "running bean-check"),
        # not per-row outcomes — don't advance the counter on those.
        if outcome != "info":
            ctx.advance()

    applied, failed, auto_closed, auto_close_error = _vehicle_migrate_apply_sync(
        settings=settings,
        conn=None,  # cache self-heals on next boot per OverrideWriter docstring
        reader=reader,
        rows=rows,
        orphan_path=orphan_path,
        target=target,
        on_progress=_on_progress,
    )

    if auto_closed:
        ctx.emit(f"Auto-closed drained orphan {auto_closed}", outcome="success")
    if auto_close_error:
        ctx.emit(f"Auto-close warning: {auto_close_error}", outcome="failure")
    ctx.emit(
        f"Migration complete · applied={applied} · failed={len(failed)}",
        outcome="info",
    )

    qs = f"migrated={applied}&failed={len(failed)}"
    if auto_closed:
        qs += f"&auto_closed={_q(auto_closed)}"
    if auto_close_error:
        qs += f"&auto_close_error={_q(auto_close_error)}"
    ctx.set_return_url(f"/setup/vehicles?{qs}")
    return {
        "slug": slug,
        "applied": applied,
        "failed": len(failed),
        "auto_closed": auto_closed,
        "auto_close_error": auto_close_error,
    }


class _AutoCloseFailed(Exception):
    """Raised by ``_auto_close_orphan_path_if_drained`` when the Close
    write went to disk but bean-check rejected it. The migration
    overrides themselves are NOT rolled back — the auto-close is a
    convenience layered on top of an already-successful migrate, so the
    user keeps their migration and just sees a warning that the
    auto-close didn't take. They can retry the close manually from
    /setup/accounts."""


def _auto_close_orphan_path_if_drained(
    orphan_path: str,
    settings,
    reader,
) -> str:
    """If ``orphan_path`` has zero unmigrated postings AND an active
    Open (not already Close'd), append a Close directive to
    ``connector_accounts.bean`` under the standard
    backup → write → bean-check → revert envelope.

    Returns one of:

    * ``"closed"`` — wrote and bean-check passed.
    * ``"still-has-postings"`` — orphan still carries unmigrated
      postings; close is not safe.
    * ``"no-active-open"`` — orphan has no Open directive (or already
      has a matching Close); nothing to write.

    Raises :class:`_AutoCloseFailed` when bean-check rejects the write
    (the file is reverted before the raise; the migration overrides
    are untouched).
    """
    from datetime import date as _date_t
    from lamella.features.setup.posting_counts import (
        count_unmigrated_postings,
        open_paths,
    )
    from lamella.core.ledger_writer import (
        BeanCheckError, capture_bean_check, run_bean_check_vs_baseline,
    )

    entries = list(reader.load().entries)
    if count_unmigrated_postings(entries, orphan_path) > 0:
        return "still-has-postings"
    if orphan_path not in open_paths(entries):
        return "no-active-open"

    accounts_path = settings.connector_accounts_path
    accounts_path.parent.mkdir(parents=True, exist_ok=True)
    if not accounts_path.exists():
        accounts_path.write_text(
            "; connector_accounts.bean — managed by Lamella.\n",
            encoding="utf-8",
        )
    backup_accounts = accounts_path.read_bytes()
    backup_main = settings.ledger_main.read_bytes()
    _baseline_count, baseline_output = capture_bean_check(settings.ledger_main)
    today = _date_t.today().isoformat()
    new_text = (
        accounts_path.read_text(encoding="utf-8").rstrip()
        + f"\n\n{today} close {orphan_path}\n"
    )
    try:
        accounts_path.write_text(new_text, encoding="utf-8")
        run_bean_check_vs_baseline(settings.ledger_main, baseline_output)
    except BeanCheckError as exc:
        accounts_path.write_bytes(backup_accounts)
        settings.ledger_main.write_bytes(backup_main)
        raise _AutoCloseFailed(f"bean-check rejected close: {exc}") from exc
    except Exception as exc:  # noqa: BLE001
        accounts_path.write_bytes(backup_accounts)
        settings.ledger_main.write_bytes(backup_main)
        raise _AutoCloseFailed(
            f"{type(exc).__name__}: {exc}"
        ) from exc
    reader.invalidate()
    return "closed"


@router.post("/setup/vehicles/{slug}/scaffold")
def setup_vehicle_scaffold(
    slug: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """One-click: scaffold every missing Expenses:<Entity>:Vehicle:<Slug>:*
    and the Assets asset account for a vehicle."""
    from fastapi.responses import RedirectResponse
    from lamella.features.vehicles.vehicle_companion import ensure_vehicle_chart
    from lamella.core.ledger_writer import BeanCheckError

    row = conn.execute(
        "SELECT slug, entity_slug FROM vehicles WHERE slug = ?", (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"vehicle {slug!r} not found")
    entity_slug = row["entity_slug"]
    if not entity_slug:
        return _hx_aware_error_redirect(
            request, "/setup/vehicles", f"vehicle-{slug}-has-no-entity",
        )
    try:
        opened = ensure_vehicle_chart(
            conn=conn, settings=settings, reader=reader,
            vehicle_slug=slug, entity_slug=entity_slug,
        )
    except BeanCheckError as exc:
        log.error("vehicle scaffold bean-check: %s", exc)
        return _hx_aware_error_redirect(
            request, "/setup/vehicles", f"bean-check-{slug}-{exc}",
        )
    # HTMX-aware: return just this vehicle's updated row so the page
    # updates in-place (click, click, click — no page reload).
    if request.headers.get("hx-request", "").lower() == "true":
        from lamella.features.vehicles.vehicle_companion import (
            vehicle_chart_paths_for,
        )
        from lamella.features.setup.posting_counts import (
            open_paths as _open_paths,
        )
        entries = list(reader.load().entries)
        open_paths_set = _open_paths(entries)
        fresh_row = conn.execute(
            "SELECT slug, display_name, year, make, model, entity_slug, "
            "is_active FROM vehicles WHERE slug = ?", (slug,),
        ).fetchone()
        v = dict(fresh_row)
        expected = vehicle_chart_paths_for(
            vehicle_slug=v["slug"], entity_slug=v["entity_slug"],
        )
        chart = [
            {"path": p.path, "purpose": p.purpose, "exists": p.path in open_paths_set}
            for p in expected
        ]
        v["chart"] = chart
        v["missing_count"] = sum(1 for c in chart if not c["exists"])
        v["has_entity"] = bool((v["entity_slug"] or "").strip())
        return request.app.state.templates.TemplateResponse(
            request, "partials/_setup_vehicle_row.html", {"v": v},
        )
    return RedirectResponse(
        f"/setup/vehicles?scaffolded={slug}&opened={len(opened)}",
        status_code=303,
    )


@router.get("/setup/vector-progress-partial", response_class=HTMLResponse)
def setup_vector_progress_partial(request: Request):
    """HTMX partial showing vector-index rebuild status. Polled by
    the setup-progress page so the user sees a live progress bar
    during the startup rebuild instead of a silent hang."""
    progress = getattr(
        request.app.state, "vector_index_progress", None,
    ) or {"status": "unknown"}
    status = progress.get("status", "unknown")
    processed = progress.get("processed", 0) or 0
    total = progress.get("total", 0) or 0
    pct = int((processed / total) * 100) if total else 0
    return request.app.state.templates.TemplateResponse(
        request, "partials/_vector_progress.html",
        {
            "status": status,
            "processed": processed,
            "total": total,
            "pct": pct,
            "error": progress.get("error"),
            "in_progress": status in ("starting", "running"),
        },
    )


@router.post("/setup/refresh-progress")
def setup_refresh_progress(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Recompute and cache setup-required-complete on app.state.
    Called after any setup-relevant save so the gate middleware
    re-evaluates on the next request."""
    from fastapi.responses import RedirectResponse
    from lamella.features.setup.setup_progress import (
        compute_setup_progress,
    )
    entries = list(reader.load().entries) if reader else []
    progress = compute_setup_progress(
        conn, entries, imports_dir=settings.import_ledger_output_dir_resolved,
    )
    request.app.state.setup_required_complete = progress.required_complete
    return RedirectResponse("/setup/recovery", status_code=303)


@router.get("/setup/progress")
def setup_progress_page(request: Request):
    """Phase 7 URL rename: /setup/progress → /setup/recovery.

    The post-install drift surface is now /setup/recovery — the same
    page Phase 6 built as the bulk-review entry point. The compute
    function ``compute_setup_progress()`` keeps its name (internal
    API used by the gate middleware); only the URL changes.

    302 (not 303) so legacy bookmarks land on the rename target with
    the same method semantics intact, and intermediaries cache it.
    """
    from fastapi.responses import RedirectResponse
    return RedirectResponse("/setup/recovery", status_code=302)


@router.get("/setup", response_class=HTMLResponse)
def setup_page(
    request: Request,
    settings: Settings = Depends(get_settings),
):
    templates = request.app.state.templates
    detection = _refresh_detection(request, settings)
    # Recompute setup-completeness on every /setup GET so a successful
    # apply (e.g. /setup/recovery/apply that just landed) flips the
    # middleware gate without needing a container restart. Cheap relative
    # to the page render itself; bounded by ledger size.
    _refresh_setup_required_complete(request, settings)
    parse_errors = list(getattr(detection, "parse_errors", ()) or ())
    has_dup_close = any("uplicate close" in (e or "") for e in parse_errors)
    ledger_main = settings.ledger_main
    main_bean_exists = ledger_main.exists() and ledger_main.is_file()
    main_bean_bytes = ledger_main.stat().st_size if main_bean_exists else 0

    # Detect orphan-override blocks (Phase 2.2). The recovery handler at
    # POST /setup/fix-orphan-overrides cleans these up; the page surfaces
    # a one-click button when they're present. An orphan-override block
    # is a `custom "override"` in connector_overrides.bean whose
    # lamella-override-of hash doesn't match any non-override txn in the
    # ledger. Detection cost: one pass to build the valid-hash set + one
    # regex pass over connector_overrides.bean — bounded by ledger size,
    # acceptable for a setup-only render path.
    orphan_override_count = 0
    try:
        overrides_path = settings.connector_overrides_path
        reader = getattr(request.app.state, "ledger_reader", None)
        if overrides_path.exists() and reader is not None:
            import re as _re
            from beancount.core.data import Transaction as _Txn
            from lamella.core.beancount_io.txn_hash import (
                txn_hash as _txn_hash,
            )
            from lamella.features.setup.posting_counts import (
                is_override_txn as _is_override,
            )
            valid_hashes: set[str] = set()
            for e in reader.load().entries:
                if not isinstance(e, _Txn):
                    continue
                if _is_override(e):
                    continue
                valid_hashes.add(_txn_hash(e))
            text = overrides_path.read_text(encoding="utf-8")
            for m in _re.finditer(
                r'lamella-override-of:\s*"([a-f0-9]+)"', text,
            ):
                if m.group(1) not in valid_hashes:
                    orphan_override_count += 1
    except Exception as exc:  # noqa: BLE001
        log.warning("orphan-override detection skipped: %s", exc)
        orphan_override_count = 0
    has_orphan_overrides = orphan_override_count > 0

    # Scenario classification — four distinct setups the page has to
    # guide. The user doesn't care about internal states like
    # NEEDS_VERSION_STAMP; they care about "what do I click?". Turn
    # the cross-product of (detection.state, has_bcg_markers,
    # db_has_state, setup_complete) into one of four scenarios so
    # the template can render a purpose-built flow.
    from lamella.main import (
        _ledger_is_bcg_managed, _witnesses_empty,
    )
    reader = getattr(request.app.state, "ledger_reader", None)
    db = getattr(request.app.state, "db", None)
    has_bcg_markers = False
    try:
        if reader is not None:
            has_bcg_markers = _ledger_is_bcg_managed(reader)
    except Exception:  # noqa: BLE001
        pass
    db_state_empty = True
    try:
        if db is not None:
            db_state_empty = _witnesses_empty(db)
    except Exception:  # noqa: BLE001
        pass
    setup_complete = bool(getattr(
        request.app.state, "setup_required_complete", False,
    ))

    # First-run wizard takeover: a true fresh install with the wizard
    # not yet completed and no manual opt-out lands on the guided
    # wizard at /setup/wizard. Anything else (broken parse, foreign
    # import, post-stamp drift) renders the maintenance checklist
    # below. The user can still come back to /setup directly to
    # access expert mode via the "Configure myself" wizard option,
    # which sets a flag we honor here.
    try:
        from lamella.features.setup.wizard_state import (
            is_wizard_complete as _wizard_done,
            load_state as _load_wiz,
        )
        wiz = _load_wiz(request.app.state.db) if getattr(request.app.state, "db", None) else None
        wizard_completed = _wizard_done(request.app.state.db) if getattr(request.app.state, "db", None) else False
        wants_manual = bool(wiz and wiz.intent == "manual")
    except Exception:  # noqa: BLE001
        wizard_completed = False
        wants_manual = False

    # If the directory has any user-content .bean files (the user has
    # data we could import), DO NOT silently drop them into the
    # onboarding wizard — render this page with the import path
    # discoverable. Scaffolded canonical files (connector_*.bean,
    # accounts.bean, commodities.bean, etc.) don't count: those are
    # empty templates we own, written by `/setup/scaffold`. Only
    # actual non-canonical files signal "user has their own ledger."
    from lamella.core.bootstrap.templates import CANONICAL_FILES as _CF
    _canonical_names = {"main.bean"} | {f.name for f in _CF}
    other_bean_files: list[str] = []
    try:
        if settings.ledger_dir.is_dir():
            other_bean_files = sorted(
                p.name for p in settings.ledger_dir.glob("*.bean")
                if p.name not in _canonical_names
            )
    except Exception:  # noqa: BLE001
        other_bean_files = []
    has_other_bean_files = bool(other_bean_files)

    # Are there any entities yet? An empty entities table is the
    # strongest signal that this is a first-run install — even if
    # main.bean has been scaffolded with a version stamp, having
    # zero entities means the user hasn't been through onboarding.
    no_entities = True
    try:
        if request.app.state.db is not None:
            row = request.app.state.db.execute(
                "SELECT 1 FROM entities WHERE is_active = 1 LIMIT 1"
            ).fetchone()
            no_entities = row is None
    except Exception:  # noqa: BLE001
        no_entities = True

    can_offer_wizard = (
        not wizard_completed
        and not wants_manual
        and not has_other_bean_files
        and not has_bcg_markers
        and no_entities
    )

    state_val = detection.state.value
    if state_val == "missing":
        if can_offer_wizard:
            return RedirectResponse("/setup/wizard/welcome", status_code=303)
        scenario = "fresh_start"
    elif state_val == "unparseable":
        scenario = "repair_broken_ledger"
    elif state_val == "structurally_empty":
        if can_offer_wizard:
            return RedirectResponse("/setup/wizard/welcome", status_code=303)
        scenario = "fresh_start"  # ledger exists but nothing in it
    elif state_val == "needs_version_stamp":
        if has_bcg_markers:
            scenario = "fixup_existing_install"  # ours, but unstamped
        else:
            scenario = "import_foreign_ledger"
    elif state_val in ("needs_migration",):
        scenario = "fixup_existing_install"
    else:  # ready
        if setup_complete:
            # Nothing to do here — shouldn't have been redirected to
            # /setup at all. Bounce to dashboard.
            return RedirectResponse("/", status_code=303)
        # Post-scaffold: main.bean exists with v1 stamp but no
        # entities defined yet. This is the "user just clicked
        # 'Create fresh ledger'" state. Forward into the wizard
        # so they don't get dumped on the legacy checklist.
        if can_offer_wizard:
            return RedirectResponse("/setup/wizard/welcome", status_code=303)
        # Schema drift / new scaffold categories / missing refs —
        # user has a working install but needs to click through
        # setup again because something changed.
        scenario = "fixup_existing_install"

    return templates.TemplateResponse(
        request,
        "setup.html",
        {
            "detection": detection,
            "has_dup_close": has_dup_close,
            "main_bean_exists": main_bean_exists,
            "main_bean_bytes": main_bean_bytes,
            "ledger_dir": str(settings.ledger_dir),
            "ledger_main": str(settings.ledger_main),
            "import_available": True,
            "scaffold_error": None,
            # New scenario classifier the template renders from
            "scenario": scenario,
            "has_bcg_markers": has_bcg_markers,
            "db_state_empty": db_state_empty,
            "setup_complete": setup_complete,
            "has_orphan_overrides": has_orphan_overrides,
            "orphan_override_count": orphan_override_count,
        },
    )


@router.post("/setup/scaffold")
def setup_scaffold(
    request: Request,
    settings: Settings = Depends(get_settings),
):
    templates = request.app.state.templates
    detection = _refresh_detection(request, settings)

    # Idempotency guard: if the user somehow hit this while the
    # ledger is already set up, don't overwrite anything.
    if not detection.needs_setup:
        return RedirectResponse("/", status_code=303)

    # Belt-and-suspenders guard: scaffold_fresh already refuses when
    # canonical files exist, but also double-check here and return a
    # clear error instead of the scaffolder's internal message. The
    # UI already hides the button when main.bean exists — this path
    # protects against anyone posting directly (curl, dev console).
    if settings.ledger_main.exists() and settings.ledger_main.stat().st_size > 0:
        log.warning(
            "scaffold POST refused: main.bean already exists at %s "
            "(non-empty). Caller bypassed the UI guard.",
            settings.ledger_main,
        )
        return templates.TemplateResponse(
            request,
            "setup.html",
            {
                "detection": detection,
                "has_dup_close": any(
                    "uplicate close" in (e or "")
                    for e in (getattr(detection, "parse_errors", ()) or ())
                ),
                "main_bean_exists": True,
                "main_bean_bytes": settings.ledger_main.stat().st_size,
                "ledger_dir": str(settings.ledger_dir),
                "ledger_main": str(settings.ledger_main),
                "import_available": True,
                "scaffold_error": (
                    "refused: main.bean already exists. Move or delete it "
                    "manually first; the server will not overwrite existing "
                    "ledger files."
                ),
            },
            status_code=409,
        )

    try:
        settings.ledger_dir.mkdir(parents=True, exist_ok=True)
        result = scaffold_fresh(settings.ledger_dir, bean_check=_bean_check_runner)
    except ScaffoldError as err:
        log.warning("scaffold_fresh refused: %s", err)
        return templates.TemplateResponse(
            request,
            "setup.html",
            {
                "detection": detection,
                "ledger_dir": str(settings.ledger_dir),
                "ledger_main": str(settings.ledger_main),
                "import_available": False,
                "scaffold_error": str(err),
            },
            status_code=400,
        )

    log.info(
        "scaffolded fresh ledger at %s (%d files)",
        result.ledger_dir,
        len(result.created),
    )

    # Refresh detection so the middleware stops redirecting, and
    # invalidate the ledger reader cache so the new files are read
    # fresh on the next request.
    _refresh_detection(request, settings)
    reader = getattr(request.app.state, "ledger_reader", None)
    if reader is not None:
        try:
            reader.invalidate()
        except Exception:  # noqa: BLE001
            log.warning("ledger reader invalidate failed after scaffold", exc_info=True)

    return RedirectResponse("/", status_code=303)


@router.get("/setup/import", response_class=HTMLResponse)
def setup_import_page(
    request: Request,
    settings: Settings = Depends(get_settings),
    source: str | None = None,
):
    """Import form, with live preview when ``?source=/path`` is set.

    On first hit the page shows a simple text input defaulting to
    ``settings.ledger_dir`` (in-place re-canonicalization). When
    the user submits, we re-render with analysis results.
    """
    templates = request.app.state.templates
    default_source = str(settings.ledger_dir)
    analysis = None
    error: str | None = None

    if source:
        src_path = Path(source)
        if not src_path.is_dir():
            error = f"directory does not exist: {source}"
        else:
            analysis = analyze_import(src_path)

    return templates.TemplateResponse(
        request,
        "setup_import.html",
        {
            "default_source": default_source,
            "source": source or default_source,
            "analysis": analysis,
            "error": error,
        },
    )


@router.post("/setup/import/apply")
def setup_import_apply(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
    source: str = Form(...),
    dry_run: str | None = Form(default=None),
):
    """Run apply_import against ``source`` and redirect to /.

    When ``dry_run`` is set (form button "Preview changes"), the route
    re-renders with the plan but does not write. Otherwise the full
    flow runs: apply → bean-check → seed (reconstruct SQLite from the
    imported ledger). Failure at any stage rolls back file changes
    and DB state."""
    templates = request.app.state.templates
    src_path = Path(source)
    if not src_path.is_dir():
        return templates.TemplateResponse(
            request,
            "setup_import.html",
            {
                "default_source": str(settings.ledger_dir),
                "source": source,
                "analysis": None,
                "error": f"directory does not exist: {source}",
            },
            status_code=400,
        )

    # Read-only analysis against the source — preview never writes.
    analysis = analyze_import(src_path)
    if analysis.is_blocked:
        return templates.TemplateResponse(
            request,
            "setup_import.html",
            {
                "default_source": str(settings.ledger_dir),
                "source": source,
                "analysis": analysis,
                "error": "analysis is blocked — resolve errors above before applying",
            },
            status_code=400,
        )

    if dry_run:
        try:
            plan = plan_import(src_path, analysis)
        except ImportApplyError as err:
            return templates.TemplateResponse(
                request,
                "setup_import.html",
                {
                    "default_source": str(settings.ledger_dir),
                    "source": source,
                    "analysis": analysis,
                    "error": str(err),
                },
                status_code=400,
            )
        return templates.TemplateResponse(
            request,
            "setup_import.html",
            {
                "default_source": str(settings.ledger_dir),
                "source": source,
                "analysis": analysis,
                "plan": plan,
                "error": None,
            },
        )

    # Real apply. If the user typed a directory other than the active
    # ledger, copy every .bean file into settings.ledger_dir first and
    # run the rest of the flow against the copy. The originals stay
    # read-only so the user's source folder is never mutated.
    dest_path = settings.ledger_dir
    try:
        same_dir = src_path.resolve() == dest_path.resolve()
    except OSError:
        same_dir = src_path == dest_path
    apply_path = src_path if same_dir else dest_path

    install_copy = None
    if not same_dir:
        try:
            install_copy = copy_install_tree(src_path, dest_path)
        except FileExistsError as err:
            return templates.TemplateResponse(
                request,
                "setup_import.html",
                {
                    "default_source": str(settings.ledger_dir),
                    "source": source,
                    "analysis": analysis,
                    "error": (
                        f"Active ledger directory {dest_path} already "
                        "contains .bean files. Empty the active ledger "
                        "directory first, or run import against that "
                        "directory directly."
                    ),
                },
                status_code=400,
            )
        except Exception as err:  # noqa: BLE001
            return templates.TemplateResponse(
                request,
                "setup_import.html",
                {
                    "default_source": str(settings.ledger_dir),
                    "source": source,
                    "analysis": analysis,
                    "error": f"failed to copy source ledger into {dest_path}: {err}",
                },
                status_code=400,
            )
        # Re-analyze the copy so transform paths point at the
        # destination files, not the originals.
        analysis = analyze_import(apply_path)
        if analysis.is_blocked:
            return templates.TemplateResponse(
                request,
                "setup_import.html",
                {
                    "default_source": str(settings.ledger_dir),
                    "source": source,
                    "analysis": analysis,
                    "error": (
                        "copied ledger no longer parses cleanly — this "
                        "shouldn't happen; report a bug"
                    ),
                },
                status_code=500,
            )

    try:
        result = apply_import(
            apply_path,
            analysis,
            bean_check=_bean_check_runner,
            seed_conn=conn,
        )
    except ImportApplyError as err:
        log.warning("apply_import failed: %s", err)
        return templates.TemplateResponse(
            request,
            "setup_import.html",
            {
                "default_source": str(settings.ledger_dir),
                "source": source,
                "analysis": analysis,
                "error": str(err),
            },
            status_code=400,
        )

    log.info(
        "imported ledger at %s: touched=%d created=%d",
        result.ledger_dir,
        len(result.files_touched),
        len(result.files_created),
    )

    _refresh_detection(request, settings)
    reader = getattr(request.app.state, "ledger_reader", None)
    if reader is not None:
        try:
            reader.invalidate()
        except Exception:  # noqa: BLE001
            log.warning("ledger reader invalidate failed after import", exc_info=True)

    # Phase 3.2: redirect to /setup/recovery instead of /. The recovery
    # page now has a green ✓ on "Prior ledger imported" so the user sees
    # the checklist advance and can continue onboarding from a single
    # surface. Going to "/" hides the import achievement and leaves them
    # wondering if anything happened.
    from urllib.parse import quote as _q
    extras_count = len(install_copy.extra_files) if install_copy else 0
    secrets_count = len(install_copy.skipped_secrets) if install_copy else 0
    return RedirectResponse(
        f"/setup/recovery?imported={_q(str(src_path))}"
        f"&touched={len(result.files_touched)}"
        f"&created={len(result.files_created)}"
        f"&extras={extras_count}"
        f"&secrets={secrets_count}",
        status_code=303,
    )


# ---------------------------------------------------------------------
# /setup/reconstruct — visible reconstruction wizard.
#
# Reached when the ledger is parseable but the SQLite state tables are
# empty (fresh install, deleted DB, `docker volume rm`, etc.). Walks
# the user through every reconstruct pass, showing counts and "did this
# find your data?" checkboxes.
# ---------------------------------------------------------------------


def _state_tables_empty(conn: sqlite3.Connection) -> bool:
    """Heuristic: are the state tables that would normally hold the
    user's onboarded data still empty? If yes, the wizard is useful."""
    from lamella.core.transform.reconstruct import (
        _import_all_steps, _PASSES,
    )
    _import_all_steps()
    tables: set[str] = set()
    for p in _PASSES:
        tables.update(p.state_tables)
    for t in tables:
        try:
            row = conn.execute(f"SELECT 1 FROM {t} LIMIT 1").fetchone()
        except sqlite3.OperationalError:
            continue
        if row is not None:
            return False
    return True


def _ledger_counts(entries: list) -> dict[str, int]:
    """Ledger-level counts for the wizard cover page."""
    from beancount.core.data import (
        Balance, Close, Custom, Open, Pad, Transaction,
    )
    counts = {
        "entries": len(entries),
        "transactions": 0,
        "opens": 0,
        "closes": 0,
        "balances": 0,
        "pads": 0,
        "customs": 0,
    }
    for e in entries:
        if isinstance(e, Transaction):
            counts["transactions"] += 1
        elif isinstance(e, Open):
            counts["opens"] += 1
        elif isinstance(e, Close):
            counts["closes"] += 1
        elif isinstance(e, Balance):
            counts["balances"] += 1
        elif isinstance(e, Pad):
            counts["pads"] += 1
        elif isinstance(e, Custom):
            counts["customs"] += 1
    return counts


def _section_readers(entries: list) -> list[dict]:
    """Preview what each reconstruct pass will find. Runs the readers
    without touching SQLite, so the wizard can show "before" counts
    before the user clicks "Run reconstruct"."""
    from lamella.features.loans.reader import (
        read_loan_balance_anchors, read_loans,
    )
    from lamella.features.projects.reader import read_projects
    from lamella.features.properties.reader import (
        read_properties, read_property_valuations,
    )
    from lamella.core.transform.custom_directive import (
        read_custom_directives,
    )
    from lamella.features.vehicles.reader import (
        read_mileage_attributions, read_vehicle_credits,
        read_vehicle_elections, read_vehicle_renewals,
        read_vehicle_trip_templates, read_vehicle_valuations,
        read_vehicle_yearly_mileage, read_vehicles,
    )

    def _count_custom(t: str) -> int:
        return len(read_custom_directives(entries, t))

    sections: list[dict] = []
    sections.append({
        "icon": "📥",
        "title": "Settings",
        "description": "App-level preferences persisted via /settings.",
        "found": _count_custom("setting"),
        "label": "setting directives",
    })
    sections.append({
        "icon": "🧾",
        "title": "Paperless field mappings",
        "description": "Which Paperless custom field plays which role (total / tax / ...).",
        "found": _count_custom("paperless-field"),
        "label": "field mappings",
    })
    sections.append({
        "icon": "📋",
        "title": "Classification rules",
        "description": "Directional priors the AI uses when classifying transactions.",
        "found": _count_custom("classification-rule"),
        "label": "rules",
    })
    sections.append({
        "icon": "💰",
        "title": "Budgets",
        "description": "Per-account monthly/yearly budget targets.",
        "found": _count_custom("budget"),
        "label": "budgets",
    })
    sections.append({
        "icon": "🔁",
        "title": "Recurring expenses",
        "description": "User-confirmed recurring-charge patterns.",
        "found": _count_custom("recurring-confirmed") +
                 _count_custom("recurring-ignored"),
        "label": "confirmations",
    })
    sections.append({
        "icon": "📭",
        "title": "Receipt dismissals",
        "description": "Receipts the user has dismissed from the review queue.",
        "found": _count_custom("receipt-dismissed"),
        "label": "dismissals",
    })
    sections.append({
        "icon": "🚗",
        "title": "Vehicles",
        "description": "Identity (make/model/VIN), valuations, elections, credits, renewals.",
        "found": (
            len(read_vehicles(entries))
            + len(read_vehicle_yearly_mileage(entries))
            + len(read_vehicle_valuations(entries))
            + len(read_vehicle_elections(entries))
            + len(read_vehicle_credits(entries))
            + len(read_vehicle_renewals(entries))
            + len(read_vehicle_trip_templates(entries))
            + len(read_mileage_attributions(entries))
        ),
        "label": "vehicle rows",
        "detail": (
            f"{len(read_vehicles(entries))} vehicle identity, "
            f"{len(read_vehicle_yearly_mileage(entries))} yearly-mileage, "
            f"{len(read_vehicle_valuations(entries))} valuations, "
            f"{len(read_vehicle_elections(entries))} elections, "
            f"{len(read_vehicle_credits(entries))} credits, "
            f"{len(read_vehicle_renewals(entries))} renewals"
        ),
    })
    sections.append({
        "icon": "🏦",
        "title": "Loans",
        "description": "Mortgage / auto / student / HELOC metadata + balance anchors.",
        "found": len(read_loans(entries)) + len(read_loan_balance_anchors(entries)),
        "label": "loan rows",
        "detail": (
            f"{len(read_loans(entries))} loans, "
            f"{len(read_loan_balance_anchors(entries))} balance anchors"
        ),
    })
    sections.append({
        "icon": "🏠",
        "title": "Properties",
        "description": "Real-estate details + valuation history.",
        "found": len(read_properties(entries)) + len(read_property_valuations(entries)),
        "label": "property rows",
        "detail": (
            f"{len(read_properties(entries))} properties, "
            f"{len(read_property_valuations(entries))} valuations"
        ),
    })
    sections.append({
        "icon": "📅",
        "title": "Projects",
        "description": "Date-ranged classification contexts (retaining wall, remodel, etc.).",
        "found": len(read_projects(entries)),
        "label": "projects",
    })
    sections.append({
        "icon": "⛽",
        "title": "Vehicle fuel log",
        "description": "Per-fillup records (gallons, cost, odometer).",
        "found": _count_custom("vehicle-fuel-entry"),
        "label": "fuel entries",
    })
    sections.append({
        "icon": "🛣️",
        "title": "Mileage trip meta",
        "description": "Per-trip business / commuting / personal splits.",
        "found": _count_custom("mileage-trip-meta"),
        "label": "trip splits",
    })
    sections.append({
        "icon": "📚",
        "title": "Classify context",
        "description": "Account descriptions + entity contexts fed into the classify prompt.",
        "found": _count_custom("account-description") + _count_custom("entity-context"),
        "label": "context rows",
    })
    sections.append({
        "icon": "🔕",
        "title": "Audit dismissals",
        "description": "Audit items the user dismissed (won't reappear).",
        "found": _count_custom("audit-dismissed"),
        "label": "dismissals",
    })
    sections.append({
        "icon": "📝",
        "title": "Notes",
        "description": "Captured notes with AI-derived hints (merchant, entity, active window).",
        "found": _count_custom("note"),
        "label": "notes",
    })
    return sections


@router.get("/setup/reconstruct", response_class=HTMLResponse)
def setup_reconstruct_page(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Preview page: shows every reconstruct source the wizard will
    rebuild from, with counts and descriptions. The user hits "Run"
    to actually execute the reconstruction."""
    templates = request.app.state.templates
    reader = getattr(request.app.state, "ledger_reader", None)
    from lamella.core.beancount_io import LedgerReader
    if reader is None:
        reader = LedgerReader(settings.ledger_main)
    entries = list(reader.load().entries)
    return templates.TemplateResponse(
        request, "setup_reconstruct.html",
        {
            "detection": _refresh_detection(request, settings),
            "ledger_dir": str(settings.ledger_dir),
            "ledger_main": str(settings.ledger_main),
            "ledger_counts": _ledger_counts(entries),
            "sections": _section_readers(entries),
            "state_empty": _state_tables_empty(conn),
            "results": None,
        },
    )


@router.post("/setup/reconstruct", response_class=HTMLResponse)
def setup_reconstruct_run(
    request: Request,
    settings: Settings = Depends(get_settings),
    force: str | None = Form(default=None),
):
    """Kick off reconstruction as a background job so each pass emits
    a live event the modal renders as it happens. Returns the standard
    progress-modal partial; the browser polls /jobs/{id}/partial for
    per-pass updates."""
    from lamella.core.beancount_io import LedgerReader
    from lamella.core.db import connect
    from lamella.core.transform.reconstruct import (
        _import_all_steps, _PASSES, _any_state_table_has_rows,
        _wipe_state_tables,
    )

    force_flag = bool(force)
    main_bean = settings.ledger_main
    db_path = settings.db_path
    force_clear_callback = _make_needs_reconstruct_clearer(request.app)

    def _work(ctx):
        reader = LedgerReader(main_bean)
        reader.invalidate()
        entries = list(reader.load().entries)
        _import_all_steps()

        all_state_tables: list[str] = []
        for p in _PASSES:
            all_state_tables.extend(p.state_tables)

        # Each pass is one "item" on the progress bar; add a prologue
        # step for the wipe (if forced) so counts line up.
        total = len(_PASSES) + 1
        ctx.set_total(total)
        ctx.emit(
            f"Loaded ledger: {len(entries)} entries, "
            f"{sum(1 for e in entries if type(e).__name__ == 'Custom')} custom directives",
            outcome="info",
        )

        # Short-lived per-pass connection — writers inside the passes
        # don't share the app's RLock, and the whole sequence commits
        # at the end.
        conn = connect(db_path)
        try:
            populated = _any_state_table_has_rows(conn, all_state_tables)
            if populated and not force_flag:
                ctx.emit(
                    "Refused: state tables already have rows — "
                    "re-run with Force checked.",
                    outcome="failure",
                )
                ctx.advance()
                return {"status": "refused", "populated": populated}

            if populated and force_flag:
                ctx.emit(
                    f"Wiping {len(set(populated))} populated state tables: "
                    f"{', '.join(sorted(set(populated))[:6])}"
                    + (" …" if len(set(populated)) > 6 else ""),
                    outcome="info",
                )
                _wipe_state_tables(conn, sorted(set(populated)))
            else:
                ctx.emit("State tables are empty — fresh rebuild.", outcome="info")
            ctx.advance()

            prev_fk = conn.execute("PRAGMA foreign_keys").fetchone()[0]
            conn.execute("PRAGMA foreign_keys = OFF")
            reports: list = []
            try:
                for pass_ in _PASSES:
                    ctx.raise_if_cancelled()
                    try:
                        report = pass_.fn(conn, entries)
                    except Exception as exc:  # noqa: BLE001
                        ctx.emit(
                            f"{pass_.name}: crashed — {type(exc).__name__}: {exc}",
                            outcome="error",
                        )
                        ctx.advance()
                        continue
                    reports.append(report)
                    if report.rows_written:
                        ctx.emit(
                            f"{pass_.name}: wrote {report.rows_written} rows"
                            + (f" — {report.notes[0]}" if report.notes else ""),
                            outcome="success",
                        )
                    else:
                        ctx.emit(
                            f"{pass_.name}: nothing to rebuild "
                            "(no directives for this category)",
                            outcome="info",
                        )
                    ctx.advance()
                conn.commit()
            finally:
                conn.execute(
                    f"PRAGMA foreign_keys = {'ON' if prev_fk else 'OFF'}"
                )

            if prev_fk:
                violations = conn.execute("PRAGMA foreign_key_check").fetchall()
                if violations:
                    ctx.emit(
                        f"{len(violations)} foreign-key violations remain "
                        "after rebuild (first 3): " + str(violations[:3]),
                        outcome="failure",
                    )

            # Re-seed registry-discovered rows. accounts_meta + entities
            # + vehicles + properties have TWO sources: custom directives
            # (handled by the passes above) AND ledger path discovery
            # (handled by sync_from_ledger at boot). The wipe drops both;
            # the passes only repopulate the directive-backed half. Without
            # this call, /setup/accounts is empty until the next container
            # restart even though every Open directive in the ledger names
            # an account.
            try:
                from lamella.core.registry.discovery import sync_from_ledger
                sync_from_ledger(
                    conn, entries,
                    simplefin_map_path=settings.simplefin_account_map_resolved,
                )
                conn.commit()
            except Exception as exc:  # noqa: BLE001
                ctx.emit(
                    f"registry re-seed failed: {type(exc).__name__}: {exc}",
                    outcome="error",
                )

            # Re-apply account-kind overrides now that accounts_meta has
            # been re-seeded — discovery's INSERT OR IGNORE doesn't
            # update existing rows, so apply_kind_overrides has to run
            # AFTER discovery to stamp the kinds the user (or demo)
            # declared via custom "account-kind" directives.
            try:
                from lamella.core.registry.kind_writer import apply_kind_overrides
                touched = apply_kind_overrides(conn, entries)
                if touched:
                    ctx.emit(
                        f"Re-applied {touched} account-kind override(s).",
                        outcome="info",
                    )
                conn.commit()
            except Exception as exc:  # noqa: BLE001
                ctx.emit(
                    f"account-kind re-apply failed: {type(exc).__name__}: {exc}",
                    outcome="error",
                )

            total_written = sum(r.rows_written for r in reports)
            ctx.emit(
                f"Done. Rebuilt {total_written} rows across "
                f"{len(reports)} passes.",
                outcome="success",
            )
        finally:
            conn.close()

        # Clear the "needs reconstruct" flag on app.state so the user
        # can continue to the dashboard without being redirected back.
        force_clear_callback()

        return {
            "status": "ok",
            "total_written": total_written,
            "passes_run": len(reports),
        }

    job_runner = request.app.state.job_runner
    job_id = job_runner.submit(
        kind="reconstruct",
        title="Rebuilding SQLite state from the ledger",
        fn=_work,
        total=len(_PASSES) + 1 if _PASSES else 1,
        return_url="/setup/reconstruct",
    )
    return request.app.state.templates.TemplateResponse(
        request,
        "partials/_job_modal.html",
        {"job_id": job_id, "on_close_url": "/"},
    )


# ---------------------------------------------------------------------
# /setup/welcome — onboarding for raw Beancount imports
#
# Reached when the ledger parses cleanly and has transactions but
# carries no lamella-* markers anywhere. The user brought in a ledger
# from somewhere else (bean-example.beancount, a prior tool, hand-
# typed) and hasn't onboarded to this app yet. Give them the options.
# ---------------------------------------------------------------------


def _ledger_stats_for_welcome(entries: list) -> dict:
    """Compact summary so the welcome page can say "we see X accounts,
    Y transactions, earliest date …" without blocking on anything."""
    from beancount.core.data import Open, Transaction
    n_tx = 0
    n_open = 0
    dates: list = []
    accounts: set = set()
    for e in entries:
        if isinstance(e, Transaction):
            n_tx += 1
            dates.append(e.date)
            for p in e.postings:
                if p.account:
                    accounts.add(p.account)
        elif isinstance(e, Open):
            n_open += 1
            accounts.add(e.account)
    return {
        "transactions": n_tx,
        "opens": n_open,
        "accounts": len(accounts),
        "earliest": min(dates).isoformat() if dates else None,
        "latest": max(dates).isoformat() if dates else None,
    }


@router.get("/setup/welcome", response_class=HTMLResponse)
def setup_welcome_page(
    request: Request,
    settings: Settings = Depends(get_settings),
):
    """First-run onboarding for a raw Beancount ledger we've never
    touched. Offers three paths: continue and label things as you go,
    run the /setup/import analyzer, or bail out and scaffold fresh.

    Reachability: the middleware redirects to this page once at boot
    when ``app.state.needs_welcome`` is True. After the dismiss
    handler flips the flag to False, the page remains reachable via
    direct URL — render is idempotent and safe to re-load. To
    re-trigger the boot-time redirect, manually set
    ``app.state.needs_welcome = True`` (admin-only path; no UI for
    this today)."""
    from lamella.core.beancount_io import LedgerReader
    templates = request.app.state.templates
    reader = getattr(request.app.state, "ledger_reader", None)
    if reader is None:
        reader = LedgerReader(settings.ledger_main)
    entries = list(reader.load().entries)
    return templates.TemplateResponse(
        request, "setup_welcome.html",
        {
            "ledger_dir": str(settings.ledger_dir),
            "ledger_main": str(settings.ledger_main),
            "stats": _ledger_stats_for_welcome(entries),
        },
    )


@router.post("/setup/welcome/continue")
async def setup_welcome_continue(
    request: Request,
    settings: Settings = Depends(get_settings),
):
    """User acknowledges the welcome banner and wants to go straight
    to the dashboard without transforming anything. Flip the flag so
    the middleware stops redirecting.

    Also enforces the data-and-security disclaimer acknowledgement:
    the form sends `acknowledge=1` only when the user ticks the
    required checkbox. Submissions without it bounce back to the
    welcome page so we never proceed past first-run without the
    operator having read and accepted the notice. Acceptance is
    logged to auth_events with a timestamp for audit trail.

    Idempotent — calling this handler when the flag is already False
    is a harmless reset + redirect, no error."""
    form = await request.form()
    if (form.get("acknowledge") or "").strip() != "1":
        return RedirectResponse("/setup/welcome", status_code=303)

    db = getattr(request.app.state, "db", None)
    if db is not None:
        from lamella.web.auth.events import (
            EVENT_DISCLAIMER_ACK,
            record_event,
        )
        user = getattr(request.state, "user", None)
        tenant = getattr(request.state, "tenant", None)
        record_event(
            db,
            event_type=EVENT_DISCLAIMER_ACK,
            user_id=getattr(user, "id", None) if user else None,
            account_id=getattr(tenant, "id", None) if tenant else None,
            success=True,
            request=request,
            detail="setup_welcome",
        )

    request.app.state.needs_welcome = False
    return RedirectResponse("/", status_code=303)


@router.get("/disclaimer", response_class=HTMLResponse)
def disclaimer_page(request: Request):
    """Standalone page exposing the data-and-security disclaimer for
    later reference. Linked from the welcome wizard, the README,
    and (eventually) /settings. Idempotent and bookmark-safe."""
    return request.app.state.templates.TemplateResponse(
        request, "disclaimer.html", {}
    )


def _make_needs_reconstruct_clearer(app):
    """Return a callable that flips app.state.needs_reconstruct to
    False. Captures `app` so the worker thread doesn't need the request."""
    def _clear() -> None:
        try:
            app.state.needs_reconstruct = False
        except Exception:  # noqa: BLE001
            pass
    return _clear

# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Admin page for entities (businesses, farms, personal).

Lists every entity discovered from the ledger plus any the user has
added through the UI. Add/edit form lets the user fill in display name,
type, tax schedule, start/ceased dates. Optional Schedule C/F scaffold
generator creates the Expenses tree.
"""
from __future__ import annotations

import logging
import re
from pathlib import Path

import yaml
from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from lamella.core.beancount_io import LedgerReader
from lamella.core.config import Settings
from lamella.web.deps import get_db, get_ledger_reader, get_settings
from lamella.core.ledger_writer import BeanCheckError
from lamella.core.registry.accounts_writer import AccountsWriter
from lamella.core.registry.discovery import discover_entity_slugs
from lamella.core.registry.service import (
    fuzzy_match_slug,
    is_valid_slug,
    list_entities,
    scaffold_paths_for_entity,
    suggest_slug,
    upsert_entity,
)

log = logging.getLogger(__name__)

router = APIRouter()


from lamella.core.registry.entity_structure import (
    ENTITY_TYPES as _STRUCTURED_ENTITY_TYPES,
)
# Surface the structured (slug, label) tuples so the template renders
# clear options. Underlying registry helpers (commingle / 2-leg vs
# 4-leg routing) consume the slug.
ENTITY_TYPES = _STRUCTURED_ENTITY_TYPES
TAX_SCHEDULES = (
    ("", "(none)"),
    ("C", "Schedule C — self-employment business"),
    ("F", "Schedule F — farm"),
    ("A", "Schedule A — personal / itemized deductions"),
)


def _load_schedule_yaml(path: Path) -> list[dict]:
    if not path.exists():
        return []
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8")) or []
    except Exception as exc:
        log.warning("failed to parse %s: %s", path, exc)
        return []


@router.get("/settings/entities", include_in_schema=False)
def entities_settings_legacy_redirect(request: Request):
    """ADR-0047 + ADR-0048: entities is a first-class concept, not a
    setting. Old URL 301s to /entities (querystring preserved)."""
    from fastapi.responses import RedirectResponse
    qs = request.url.query
    return RedirectResponse(
        "/entities" + (f"?{qs}" if qs else ""), status_code=301,
    )


_PERSON_TYPES = {"personal"}
# Match the canonical ENTITY_TYPES from
# lamella.core.registry.entity_structure. Sole prop, LLC, partnership,
# S-Corp, C-Corp all bucket as businesses for the dashboard's
# people-first grouping. Trust / estate / nonprofit also go here —
# the dashboard split is people vs not-people, not a tax-law taxonomy.
_BUSINESS_TYPES = {
    "sole_proprietorship", "llc", "partnership", "s_corp", "c_corp",
    "trust", "estate", "nonprofit",
}
_SKIP_TYPES = {"skip"}


def _card_kind(entity_type: str) -> str:
    """Bucket an entity_type into one of {people, businesses, skip,
    other} for the people-first grouping on the dashboard.

    ``skip`` entities are scaffolded-and-ignored slugs (Clearing,
    RegularTransactionForSummariesFrom, etc.); they get their own
    section so they don't pollute the main people/businesses view
    but stay visible for users who imported them and want to clean
    them up.
    """
    t = (entity_type or "").lower().strip()
    if t in _PERSON_TYPES:
        return "people"
    if t in _BUSINESS_TYPES:
        return "businesses"
    if t in _SKIP_TYPES:
        return "skip"
    return "other"


def _card_for(conn, e) -> dict:
    """Materialize a single card dict from an entity. Accepts either
    an EntityRow attr-access object (from ``list_entities``) or a
    plain dict (from ``conn.execute(...).fetchone()``). Used by both
    the dashboard render and the post-save HTMX response so the
    in-place swap matches the original card shape exactly."""
    if isinstance(e, dict):
        slug = e.get("slug")
        display_name = e.get("display_name")
        entity_type = e.get("entity_type")
        tax_schedule = e.get("tax_schedule")
        is_active = e.get("is_active")
    else:
        slug = e.slug
        display_name = e.display_name
        entity_type = e.entity_type
        tax_schedule = e.tax_schedule
        is_active = e.is_active
    from lamella.web.routes.setup import _accounts_referencing_slug
    try:
        count = len(_accounts_referencing_slug(conn, slug, only_open=True))
    except Exception:  # noqa: BLE001
        count = 0
    return {
        "slug": slug,
        "display_name": display_name or slug,
        "entity_type": entity_type or "",
        "tax_schedule": tax_schedule or "",
        "is_active": bool(is_active),
        "account_count": count,
        "kind": _card_kind(entity_type or ""),
    }


@router.get("/entities", response_class=HTMLResponse)
def entities_page(
    request: Request,
    saved: str | None = None,
    conn = Depends(get_db),
):
    """Dashboard view: card grid grouped by kind (people first,
    then businesses, farms, other). Click a card to open a modal
    edit form. Click "+ Add person" / "+ Add business" for a focused
    create modal. ADR-0047 + the user's setup-wizard reference: no
    long-form bulk editing on this page."""
    entities = list_entities(conn, include_inactive=True)
    cards = [_card_for(conn, e) for e in entities]
    cards.sort(key=lambda c: (not c["is_active"], c["display_name"].lower()))

    grouped: dict[str, list[dict]] = {
        "people": [], "businesses": [], "other": [], "skip": [],
    }
    for c in cards:
        grouped[c["kind"]].append(c)

    ctx = {
        "grouped": grouped,
        "saved": saved,
        "entity_types": ENTITY_TYPES,
        "tax_schedules": TAX_SCHEDULES,
    }
    return request.app.state.templates.TemplateResponse(
        request, "entities.html", ctx,
    )


def _resolve_card_kind_param(value: str | None) -> str:
    """Normalize a user-supplied ``?kind=`` query-string value to
    one of the canonical buckets. Defaults to ``businesses``
    (the single most common case for new entities)."""
    v = (value or "").lower().strip()
    if v in {"person", "people", "personal", "individual"}:
        return "people"
    return "businesses"


@router.get("/entities/new-modal", response_class=HTMLResponse)
def entity_new_modal(
    request: Request,
    kind: str = "",
):
    """HTMX fragment — the "+ Add person" / "+ Add business" modal.
    Returns just the modal markup; the page-level HTMX handler swaps
    it into <body> beforeend, the global modal CSS makes it visible
    (the macro renders with .is-open). Click outside or Esc closes."""
    bucket = _resolve_card_kind_param(kind)
    # Default to canonical entity_type values from
    # lamella.core.registry.entity_structure.ENTITY_TYPES — the modal
    # uses <select> so anything outside this list would be rejected
    # client-side anyway, but matching exactly avoids a "did you
    # mean…" surprise.
    default_type = {
        "people": "personal",
        "businesses": "sole_proprietorship",  # Schedule C, most common
    }.get(bucket, "")
    default_schedule = {
        "people": "",  # personal entities usually have no schedule
        "businesses": "C",
    }.get(bucket, "")
    ctx = {
        "kind": bucket,
        "default_type": default_type,
        "default_schedule": default_schedule,
        "entity_types": ENTITY_TYPES,
        "tax_schedules": TAX_SCHEDULES,
    }
    return request.app.state.templates.TemplateResponse(
        request, "partials/_entity_modal_new.html", ctx,
    )


@router.get("/entities/{slug}/edit-modal", response_class=HTMLResponse)
def entity_edit_modal(
    slug: str,
    request: Request,
    conn = Depends(get_db),
):
    """HTMX fragment — the per-entity edit modal. Same shape as the
    create modal but pre-filled and POSTs back to the canonical save
    handler. The modal's form sets hx-target=#entity-card-{slug} and
    hx-swap=outerHTML so the dashboard card refreshes in place after
    save without a full page reload."""
    row = conn.execute(
        "SELECT * FROM entities WHERE slug = ?", (slug,)
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="entity not found")
    ctx = {
        "entity": dict(row),
        "entity_types": ENTITY_TYPES,
        "tax_schedules": TAX_SCHEDULES,
    }
    return request.app.state.templates.TemplateResponse(
        request, "partials/_entity_modal_edit.html", ctx,
    )


@router.get("/entities/legacy", response_class=HTMLResponse)
def entities_legacy_bulk_editor(
    request: Request,
    saved: str | None = None,
    conn = Depends(get_db),
):
    """Legacy "all entities, all forms, one page" editor. Reachable
    from a footer link on /entities for users who prefer the old
    bulk shape. Will be retired once the per-entity edit flow has
    feature parity."""
    entities = list_entities(conn, include_inactive=True)
    from lamella.web.routes.setup import _accounts_referencing_slug
    entity_counts: dict[str, int] = {}
    for e in entities:
        try:
            entity_counts[e.slug] = len(
                _accounts_referencing_slug(conn, e.slug, only_open=True)
            )
        except Exception:  # noqa: BLE001
            entity_counts[e.slug] = 0
    ctx = {
        "entities": entities,
        "entity_counts": entity_counts,
        "entity_types": ENTITY_TYPES,
        "tax_schedules": TAX_SCHEDULES,
        "saved": saved,
    }
    return request.app.state.templates.TemplateResponse(
        request, "settings_entities.html", ctx,
    )


@router.get("/entities/new", response_class=HTMLResponse)
def entities_new_page(
    request: Request,
    conn = Depends(get_db),
):
    """Focused "add a new entity" form — replaces the bottom-of-page
    add form on the legacy bulk editor."""
    ctx = {
        "entity_types": ENTITY_TYPES,
        "tax_schedules": TAX_SCHEDULES,
    }
    return request.app.state.templates.TemplateResponse(
        request, "entity_new.html", ctx,
    )


@router.get("/entities/{slug}/edit", response_class=HTMLResponse)
def entity_edit_page(
    slug: str,
    request: Request,
    saved: str | None = None,
    conn = Depends(get_db),
):
    """Focused single-entity editor — one entity, one form. Linked
    from the /entities dashboard's Edit button."""
    row = conn.execute(
        "SELECT * FROM entities WHERE slug = ?", (slug,)
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="entity not found")
    from lamella.web.routes.setup import _accounts_referencing_slug
    try:
        account_count = len(
            _accounts_referencing_slug(conn, slug, only_open=True)
        )
    except Exception:  # noqa: BLE001
        account_count = 0
    ctx = {
        "entity": dict(row),
        "entity_types": ENTITY_TYPES,
        "tax_schedules": TAX_SCHEDULES,
        "account_count": account_count,
        "saved": saved,
    }
    return request.app.state.templates.TemplateResponse(
        request, "entity_edit.html", ctx,
    )


@router.get("/settings/entities/suggest-slug", response_class=HTMLResponse)
def suggest_slug_endpoint(
    display_name: str,
    conn = Depends(get_db),
):
    """Autocomplete: given a typed display name, return a small HTML
    snippet suggesting a slug and (if applicable) flagging a fuzzy
    match against an existing slug. Called via htmx as the user types.
    """
    suggested = suggest_slug(display_name)
    matched = fuzzy_match_slug(conn, display_name) if suggested else None
    if not suggested:
        return HTMLResponse("")
    # Small JS to populate the slug input with the suggestion when the
    # user hasn't typed anything in there yet.
    hint = f"suggested slug: <code>{suggested}</code>"
    if matched and matched != suggested:
        hint += (
            f" &middot; <strong>already in ledger as</strong> "
            f"<code>{matched}</code> — consider linking to that"
        )
    script = (
        "<script>"
        "(function(){var s=document.getElementById('slug-input');"
        f"if (s && !s.value) s.value='{matched or suggested}';"
        "})();</script>"
    )
    return HTMLResponse(hint + script)


@router.post("/settings/entities/{slug}/delete")
def delete_entity(
    slug: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
    conn = Depends(get_db),
):
    """Delete an entity row. Refuses if the entity carries user-typed
    information (display_name, entity_type, tax_schedule) OR if any
    account it owns has live transactions. Empty scaffolding only —
    anything else routes through Deactivate / Close.

    Writes a ``custom "entity-deleted"`` tombstone before the SQL
    DELETE so boot-time discovery doesn't resurrect the row.
    """
    from lamella.core.registry.entity_writer import append_entity_deleted
    from lamella.web.routes.setup import _accounts_referencing_slug
    from lamella.features.setup.posting_counts import (
        DeleteRefusal, assert_safe_to_delete_entity,
    )
    row = conn.execute(
        "SELECT slug FROM entities WHERE slug = ?", (slug,)
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="entity not found")
    try:
        entries = list(reader.load().entries)
        assert_safe_to_delete_entity(
            conn, entries, slug,
            accounts_referencing_slug=_accounts_referencing_slug,
        )
    except DeleteRefusal as exc:
        raise HTTPException(status_code=409, detail=exc.message)
    try:
        append_entity_deleted(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            entity_slug=slug,
        )
    except BeanCheckError as exc:
        raise HTTPException(
            status_code=500,
            detail=f"bean-check rejected entity-deleted tombstone: {exc}",
        )
    conn.execute("DELETE FROM entities WHERE slug = ?", (slug,))
    if "hx-request" in {k.lower() for k in request.headers.keys()}:
        return HTMLResponse("")  # row removed from DOM via outerHTML swap
    return RedirectResponse("/entities?saved=deleted", status_code=303)


@router.post("/settings/entities-cleanup")
def cleanup_system_slugs(
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
    conn = Depends(get_db),
):
    """One-click removal of obviously-non-entity slugs left over from
    earlier discovery runs before the blocklist tightened. Removes any
    row whose slug matches a known system-slug pattern, regardless of
    whether a display_name was set — the slug itself is the
    disqualifier. Posting-count safety still applies (refuses delete
    when the slug is referenced by live ledger postings).

    Each removal writes a ``custom "entity-deleted"`` tombstone before
    the SQL DELETE so boot-time discovery doesn't re-create the row
    from still-present Open directives. Skips silently when delete is
    refused — bulk cleanup shouldn't fail on a single blocking row.
    """
    from lamella.core.registry.discovery import EXCLUDED_ENTITY_SEGMENTS
    from lamella.core.registry.entity_writer import append_entity_deleted
    from lamella.web.routes.setup import _accounts_referencing_slug
    from lamella.features.setup.posting_counts import (
        DeleteRefusal, assert_safe_to_delete_entity,
    )
    system_slugs = set(EXCLUDED_ENTITY_SEGMENTS)
    entries = list(reader.load().entries)
    # System slugs are removed regardless of whether the user set a
    # display_name on them — an entity called "Clearing" or
    # "OpeningBalances" is never a real entity, the label is leftover
    # noise from an earlier discovery pass.
    rows = conn.execute("SELECT slug FROM entities").fetchall()
    removed = 0
    skipped = 0
    for r in rows:
        slug = r["slug"]
        is_system = slug in system_slugs or re.match(r"^RegularTransa[ck]tion", slug or "")
        if not is_system:
            continue
        try:
            assert_safe_to_delete_entity(
                conn, entries, slug,
                accounts_referencing_slug=_accounts_referencing_slug,
            )
        except DeleteRefusal as exc:
            log.info("cleanup-system skip %s: %s", slug, exc.message)
            skipped += 1
            continue
        try:
            append_entity_deleted(
                connector_config=settings.connector_config_path,
                main_bean=settings.ledger_main,
                entity_slug=slug,
            )
        except BeanCheckError as exc:
            log.warning(
                "entity-deleted tombstone for %s skipped: %s — leaving "
                "DB row in place to avoid silent ledger/DB drift",
                slug, exc,
            )
            continue
        conn.execute("DELETE FROM entities WHERE slug = ?", (slug,))
        removed += 1
    return RedirectResponse(
        f"/entities?saved=cleanup-removed-{removed}", status_code=303
    )


@router.post("/settings/entities/{slug}/generate-context", response_class=HTMLResponse)
def generate_context(
    slug: str,
    request: Request,
):
    """Work-backwards: have the AI analyze this entity's ledger
    history and draft a proposed classify_context paragraph. Runs
    as a job so the user sees live progress instead of a silent
    spinner during the 10–30s AI call."""
    import asyncio
    import html as _html
    settings = request.app.state.settings
    if not settings.openrouter_api_key:
        return HTMLResponse(
            '<p class="muted">AI is disabled — set OPENROUTER_API_KEY to enable draft generation.</p>',
        )

    def _work(ctx):
        from lamella.features.ai_cascade.draft_description import (
            generate_entity_description,
        )
        from lamella.features.ai_cascade.service import AIService
        ai = AIService(settings=settings, conn=request.app.state.db)
        ctx.emit(f"Loading ledger history for entity '{slug}' …", outcome="info")
        reader = request.app.state.ledger_reader
        entries = reader.load().entries
        ctx.emit(
            "Calling AI to draft classify_context (usually 10–30s) …",
            outcome="info",
        )
        loop = asyncio.new_event_loop()
        try:
            draft = loop.run_until_complete(
                generate_entity_description(
                    ai=ai, entries=entries, entity_slug=slug,
                )
            )
        finally:
            loop.close()
        if draft is None:
            ctx.emit(
                "No ledger activity found for this entity yet — draft "
                "needs some txns to work with.",
                outcome="not_found",
            )
            return {
                "terminal_html": (
                    '<p class="muted">No ledger activity found for this '
                    'entity yet — draft needs some txns to work with.</p>'
                ),
            }
        ctx.emit(
            f"Draft ready (confidence {draft.confidence:.2f}).",
            outcome="success",
        )
        surprises_html = ""
        if draft.surprises:
            surprises_html = (
                '<p class="muted small" style="margin-top:0.5rem;">'
                '<strong>Surprises flagged:</strong></p><ul>'
                + "".join(f"<li>{_html.escape(s)}</li>" for s in draft.surprises)
                + "</ul>"
            )
        html_out = (
            f'<div class="ai-draft-result" '
            f'style="padding:0.75rem;background:#fff3cd;border-radius:4px;">'
            f'<p class="muted small"><strong>AI-proposed draft '
            f'(confidence {draft.confidence:.2f}).</strong> Copy the text '
            f'below + paste into the textarea for <code>{_html.escape(slug)}</code>.</p>'
            f'<textarea rows="8" style="width:100%;font-family:inherit;" '
            f'readonly>{_html.escape(draft.description)}</textarea>'
            f'{surprises_html}</div>'
        )
        return {"terminal_html": html_out}

    runner = request.app.state.job_runner
    job_id = runner.submit(
        kind="ai-entity-context",
        title=f"Drafting classify_context for '{slug}'",
        fn=_work,
    )
    return request.app.state.templates.TemplateResponse(
        request, "partials/_job_modal.html",
        {"job_id": job_id, "on_close_url": "/settings/entities"},
    )


def _resolve_modal_kind_for_entity_type(entity_type: str) -> str:
    """Map an entity_type back to the modal kind so a re-rendered
    error modal lands in the right grid (people / businesses / other).
    Mirrors `_card_kind` but returns the modal-routing token (which
    matches the section-grid id suffix used by the new-modal flow).
    """
    t = (entity_type or "").lower().strip()
    if t in _PERSON_TYPES:
        return "people"
    if t in _BUSINESS_TYPES:
        return "businesses"
    return "businesses"  # default to businesses for unknown


def _render_new_modal_error(
    request: Request,
    *,
    kind: str,
    error_message: str,
    prefill: dict,
):
    """Render the entity-new modal with an inline error banner and the
    user's submitted values pre-filled. HX-Retarget + HX-Reswap
    redirect the swap to the open modal-backdrop so the modal
    re-renders in place instead of polluting the destination grid.
    """
    ctx = {
        "kind": kind,
        "default_type": prefill.get("entity_type") or "",
        "default_schedule": prefill.get("tax_schedule") or "",
        "entity_types": ENTITY_TYPES,
        "tax_schedules": TAX_SCHEDULES,
        "error_message": error_message,
        "prefill": prefill,
    }
    return request.app.state.templates.TemplateResponse(
        request, "partials/_entity_modal_new.html", ctx,
        headers={
            "HX-Retarget": "#entity-new-modal",
            "HX-Reswap": "outerHTML",
        },
    )


@router.post("/settings/entities")
def save_entity(
    request: Request,
    conn = Depends(get_db),
    settings: Settings = Depends(get_settings),
    slug: str = Form(...),
    display_name: str = Form(""),
    entity_type: str = Form(""),
    tax_schedule: str = Form(""),
    start_date: str = Form(""),
    ceased_date: str = Form(""),
    is_active: str = Form("1"),
    notes: str = Form(""),
    classify_context: str = Form(""),
    auto_scaffold: str = Form(""),
):
    headers_lower = {k.lower(): v for k, v in request.headers.items()}
    is_hx = "hx-request" in headers_lower
    hx_target = headers_lower.get("hx-target", "")
    # The new-modal form sends X-Modal-Kind so we know which grid
    # ("people" / "businesses") the error redraw should target.
    modal_kind = headers_lower.get("x-modal-kind", "")
    is_new_modal = (
        is_hx and modal_kind and not hx_target.startswith("entity-card-")
        and not hx_target.startswith("entity-row-")
    )

    raw_slug = slug
    slug = slug.strip()
    prefill_for_error = {
        "slug": raw_slug,
        "display_name": display_name,
        "entity_type": entity_type,
        "tax_schedule": tax_schedule,
        "start_date": start_date,
        "notes": notes,
        "classify_context": classify_context,
        "is_active": is_active == "1",
        "auto_scaffold": auto_scaffold == "1",
    }

    def _validation_failure(msg: str):
        if is_new_modal:
            return _render_new_modal_error(
                request,
                kind=modal_kind,
                error_message=msg,
                prefill=prefill_for_error,
            )
        raise HTTPException(status_code=400, detail=msg)

    if not is_valid_slug(slug):
        return _validation_failure(
            f"slug {slug!r} is not valid — must start with a capital letter "
            f"and contain only letters, digits, hyphens, or underscores."
        )
    # Server-side validation against the canonical list. The UI uses
    # <select> elements so this should never fire for legitimate
    # form-submits, but defense-in-depth — a bare curl or a stale
    # tab with a hand-edited <input> shouldn't be able to plant
    # arbitrary entity_type / tax_schedule strings.
    valid_types = {t[0] for t in ENTITY_TYPES}
    valid_schedules = {t[0] for t in TAX_SCHEDULES}
    if entity_type and entity_type not in valid_types:
        return _validation_failure(
            f"unknown entity_type {entity_type!r}; expected one of "
            f"{sorted(t for t in valid_types if t)}"
        )
    if tax_schedule and tax_schedule not in valid_schedules:
        return _validation_failure(
            f"unknown tax_schedule {tax_schedule!r}; expected one of "
            f"{sorted(t for t in valid_schedules if t)}"
        )
    # Slug-collision guard for the new-modal create flow only.
    # Modal-edit posts back the existing slug as a hidden input and
    # legitimately UPSERTs the same row, so collision-checking there
    # would block every save. The new-modal carries X-Modal-Kind, so
    # we use that as the create-vs-update signal.
    if is_new_modal:
        existing = conn.execute(
            "SELECT 1 FROM entities WHERE slug = ? LIMIT 1", (slug,),
        ).fetchone()
        if existing is not None:
            # Suggest the next-free disambiguated form so the user
            # can copy/paste it into the slug field.
            sugg = slug
            for n in range(2, 1000):
                cand = f"{slug}{n}"
                r = conn.execute(
                    "SELECT 1 FROM entities WHERE slug = ? LIMIT 1", (cand,),
                ).fetchone()
                if r is None:
                    sugg = cand
                    break
            return _validation_failure(
                f"slug {slug!r} is already taken — try {sugg!r} or pick "
                f"another."
            )
    upsert_entity(
        conn,
        slug=slug,
        display_name=display_name.strip() or None,
        entity_type=entity_type.strip() or None,
        tax_schedule=tax_schedule.strip() or None,
        start_date=start_date.strip() or None,
        ceased_date=ceased_date.strip() or None,
        is_active=1 if is_active == "1" else 0,
        notes=notes.strip() or None,
        classify_context=classify_context.strip() or None,
    )
    ctx_text = classify_context.strip()
    if ctx_text:
        try:
            from lamella.core.transform.steps.step14_classify_context import (
                append_entity_context,
            )
            append_entity_context(
                connector_config=settings.connector_config_path,
                main_bean=settings.ledger_main,
                entity_slug=slug,
                context=ctx_text,
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("entity-context directive write failed for %s: %s", slug, exc)

    # Persist the entity registry row itself to the ledger so a DB
    # wipe can replay it. Without this the entity table is SQLite-only
    # and everything downstream (commingle resolver, classify
    # whitelist, scaffold routing) would degrade to unknown-entity on
    # recovery. The writer rewrites the block in place so per-entity
    # there's always exactly one `custom "entity"` directive.
    try:
        from lamella.core.registry.entity_writer import (
            append_entity_directive,
        )
        append_entity_directive(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            entity_slug=slug,
            display_name=display_name.strip() or None,
            entity_type=entity_type.strip() or None,
            tax_schedule=tax_schedule.strip() or None,
            start_date=start_date.strip() or None,
            ceased_date=ceased_date.strip() or None,
            notes=notes.strip() or None,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("entity directive write failed for %s: %s", slug, exc)

    # Auto-scaffold accounts for the new entity if requested. Only
    # runs for the new-modal create flow (is_new_modal); modal-edit
    # callers preserve existing scaffolding without re-applying.
    # Schedule C/F/Personal route via load_categories_yaml_for_entity;
    # if no chart resolves (e.g. unknown tax_schedule + entity_type),
    # the scaffold step silently skips.
    if is_new_modal and auto_scaffold == "1":
        try:
            row_for_scaffold = conn.execute(
                "SELECT slug, display_name, entity_type, tax_schedule "
                "FROM entities WHERE slug = ?", (slug,),
            ).fetchone()
            from lamella.core.registry.service import (
                load_categories_yaml_for_entity,
            )
            schedule_yaml = load_categories_yaml_for_entity(
                settings, row_for_scaffold,
            )
            if schedule_yaml:
                candidates = scaffold_paths_for_entity(schedule_yaml, slug)
                paths = [c["path"] for c in candidates]
                if paths:
                    writer = AccountsWriter(
                        main_bean=settings.ledger_main,
                        connector_accounts=settings.connector_accounts_path,
                    )
                    writer.write_opens(
                        paths,
                        comment=(
                            f"Auto-scaffold for {slug} "
                            f"({len(paths)} accounts)"
                        ),
                    )
                    reader.invalidate()
        except BeanCheckError as exc:
            log.warning(
                "auto-scaffold for %s failed bean-check: %s — entity "
                "still saved; user can re-run from the scaffold page",
                slug, exc,
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "auto-scaffold for %s skipped: %s — entity still saved",
                slug, exc,
            )
    # HTMX-aware: choose the response shape based on what the caller
    # is targeting.
    #
    # 1. Modal edit/create flow (the canonical /entities dashboard) →
    #    return the card partial; the modal closes via the form's
    #    HX-Trigger response. Detection:
    #      - HX-Target id starts with "entity-card-" (modal edit), or
    #      - HX-Target id starts with "entities-" (kind-grid append on
    #        modal create), or
    #      - HX-Target id equals "entities-grid".
    # 2. Legacy bulk-editor flow (/entities/legacy) → HX-Target
    #    starts with "entity-row-" and we render the row partial.
    # 3. Other HX request → return card by default (matches the
    #    dashboard pattern; legacy bulk editor is the one explicit
    #    exception above).
    # 4. No HX header → 303 to /entities with ?saved.
    headers = {k.lower(): v for k, v in request.headers.items()}
    is_hx = "hx-request" in headers
    hx_target = headers.get("hx-target", "")

    is_legacy_row_swap = hx_target.startswith("entity-row-")

    if is_hx and is_legacy_row_swap:
        # Legacy bulk-editor row partial (used by /entities/legacy).
        row = conn.execute(
            "SELECT * FROM entities WHERE slug = ?", (slug,),
        ).fetchone()
        try:
            from lamella.web.routes.setup import _accounts_referencing_slug
            count_for_slug = len(
                _accounts_referencing_slug(conn, slug, only_open=True)
            )
        except Exception:  # noqa: BLE001
            count_for_slug = 0
        ctx = {
            "e": dict(row),
            "entity_types": ENTITY_TYPES,
            "tax_schedules": TAX_SCHEDULES,
            "entity_counts": {slug: count_for_slug},
            "saved_marker": True,
        }
        return request.app.state.templates.TemplateResponse(
            request, "partials/entity_row.html", ctx,
        )

    if is_hx:
        row = conn.execute(
            "SELECT * FROM entities WHERE slug = ?", (slug,),
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=500, detail="entity vanished post-save")
        card = _card_for(conn, dict(row))
        # Trigger the modal-close client event + a "saved" toast so
        # the page-level handler closes the modal without inline JS
        # in every form template.
        return request.app.state.templates.TemplateResponse(
            request, "partials/_entity_card.html", {"c": card},
            headers={"HX-Trigger": "entity-saved"},
        )

    return RedirectResponse(f"/entities?saved={slug}", status_code=303)


@router.get("/settings/entities/{slug}/scaffold", response_class=HTMLResponse)
def scaffold_preview(
    slug: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Show the category checklist for creating the entity's Expenses
    tree. Routes to Schedule C, F, or the Personal (Schedule A +
    common-living) chart based on the entity's tax_schedule /
    entity_type / slug.
    """
    row = conn.execute(
        "SELECT * FROM entities WHERE slug = ?", (slug,)
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="entity not found")
    schedule = (row["tax_schedule"] or "").strip().upper()
    entity_type = (row["entity_type"] or "").strip().lower()
    # Pick the right YAML via the shared resolver.
    from lamella.core.registry.service import load_categories_yaml_for_entity
    schedule_yaml = load_categories_yaml_for_entity(settings, row)
    if not schedule_yaml:
        raise HTTPException(
            status_code=400,
            detail=(
                "no category chart resolves for this entity. Set "
                "tax_schedule to C (Schedule C business), F (Schedule "
                "F farm), A/Personal (itemized deductions), or entity_type "
                "to 'personal', and retry."
            ),
        )
    # For display purposes — "C"/"F" stays as-is; Personal chart
    # surfaces under a "Personal" label.
    if schedule not in ("C", "F"):
        schedule = "Personal"
    candidates = scaffold_paths_for_entity(schedule_yaml, slug)

    # Annotate existing — skip what's already open in the ledger.
    existing = set()
    for entry in reader.load().entries:
        if hasattr(entry, "account"):
            existing.add(entry.account)
    for c in candidates:
        c["exists"] = c["path"] in existing

    # Sync check — every Expenses:{slug}:* account in the ledger that
    # ISN'T in the generated candidate list. Surfaces user-created
    # categories that aren't on Schedule C/F so the user can decide
    # whether to rename, remove, or keep.
    candidate_paths = {c["path"] for c in candidates}
    prefix = f"Expenses:{slug}:"
    extras: list[str] = []
    for p in sorted(existing):
        if p.startswith(prefix) and p not in candidate_paths:
            extras.append(p)

    ctx = {
        "entity": dict(row),
        "candidates": candidates,
        "extras": extras,
        "schedule": schedule,
    }
    return request.app.state.templates.TemplateResponse(
        request, "settings_entity_scaffold.html", ctx
    )


@router.get("/settings/entities/{slug}/merge", response_class=HTMLResponse)
def merge_preview(
    slug: str,
    into: str,
    request: Request,
    settings: Settings = Depends(get_settings),
):
    """Dry-run preview of merging `slug` into `into` — lists every file
    and line that would change. No writes."""
    from lamella.core.registry.slug_rename import build_preview
    try:
        preview = build_preview(
            ledger_dir=settings.ledger_dir,
            old=slug, new=into,
            segment_index=1,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    ctx = {"preview": preview, "slug": slug, "into": into}
    return request.app.state.templates.TemplateResponse(
        request, "settings_entity_merge.html", ctx
    )


@router.post("/settings/entities/{slug}/merge")
def merge_apply(
    slug: str,
    into: str = Form(...),
    confirm: str = Form(""),
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Actually apply the merge. `confirm=yes` required to proceed."""
    from lamella.core.registry.slug_rename import apply_rename
    if confirm != "yes":
        raise HTTPException(status_code=400, detail="confirmation missing")
    try:
        preview = apply_rename(
            main_bean=settings.ledger_main,
            ledger_dir=settings.ledger_dir,
            old=slug, new=into,
            segment_index=1,
            conn=conn,
            data_dir=settings.data_dir,
        )
    except BeanCheckError as exc:
        raise HTTPException(status_code=500, detail=f"bean-check failed: {exc}")
    reader.invalidate()
    return RedirectResponse(
        f"/entities?saved=merged-{slug}-into-{into}-"
        f"({preview.file_count}files-{preview.line_count}lines)",
        status_code=303,
    )


@router.post("/settings/entities/{slug}/scaffold")
async def scaffold_apply_async(
    slug: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    form = await request.form()
    chosen = [v for (k, v) in form.multi_items() if k == "path" and v]
    if not chosen:
        return RedirectResponse(f"/entities?saved={slug}", status_code=303)
    writer = AccountsWriter(
        main_bean=settings.ledger_main,
        connector_accounts=settings.connector_accounts_path,
    )
    try:
        writer.write_opens(
            list(chosen),
            comment=f"Scaffold for {slug} ({len(chosen)} accounts)",
        )
    except BeanCheckError as exc:
        raise HTTPException(status_code=500, detail=f"bean-check failed: {exc}")
    reader.invalidate()
    return RedirectResponse(f"/entities?saved={slug}", status_code=303)

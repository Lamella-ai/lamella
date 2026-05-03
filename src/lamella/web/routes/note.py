# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import logging

from fastapi import APIRouter, BackgroundTasks, Depends, Form, Request
from fastapi.responses import HTMLResponse, Response

from lamella.features.ai_cascade.notes import parse_note as ai_parse_note
from lamella.features.ai_cascade.service import AIService
from lamella.core.config import Settings
from lamella.web.deps import get_ai_service, get_note_service, get_settings
from lamella.features.notes.service import NoteService
from lamella.features.notes.writer import append_note

log = logging.getLogger(__name__)

router = APIRouter()


@router.get("/note", include_in_schema=False)
def note_legacy_redirect(request: Request):
    """ADR-0048: collections are plural. Redirect /note → /notes."""
    from fastapi.responses import RedirectResponse
    qs = request.url.query
    return RedirectResponse(
        "/notes" + (f"?{qs}" if qs else ""), status_code=301,
    )


@router.get("/notes", response_class=HTMLResponse)
def notes_page(request: Request, notes: NoteService = Depends(get_note_service)):
    ctx = {"recent": notes.list(limit=10)}
    return request.app.state.templates.TemplateResponse(request, "note.html", ctx)


@router.post("/note", include_in_schema=False)
def create_note_legacy(
    request: Request,
    background: BackgroundTasks,
    body: str = Form(...),
    entity_hint: str | None = Form(default=None),
    merchant_hint: str | None = Form(default=None),
    captured_at: str | None = Form(default=None),
    txn_hash: str | None = Form(default=None),
    notes: NoteService = Depends(get_note_service),
    ai: AIService = Depends(get_ai_service),
    settings: Settings = Depends(get_settings),
):
    """Legacy POST alias — delegates to the canonical /notes handler.
    Kept so any cached form/HTMX action targeting /note keeps working
    until the next soak."""
    return create_note(
        request, background, body, entity_hint, merchant_hint,
        captured_at, txn_hash, notes, ai, settings,
    )


@router.post("/notes")
def create_note(
    request: Request,
    background: BackgroundTasks,
    body: str = Form(...),
    entity_hint: str | None = Form(default=None),
    merchant_hint: str | None = Form(default=None),
    captured_at: str | None = Form(default=None),
    txn_hash: str | None = Form(default=None),
    notes: NoteService = Depends(get_note_service),
    ai: AIService = Depends(get_ai_service),
    settings: Settings = Depends(get_settings),
):
    from datetime import UTC, datetime
    captured_dt = None
    th = (txn_hash or "").strip() or None
    if th:
        # Pinned memo — the date context comes from the txn itself via
        # txn_hash, so captured_at should reflect WHEN THE USER WROTE
        # THE MEMO, not midnight of the txn date. Per ADR-0023 we store
        # TZ-aware UTC at rest; the display layer converts to the user's
        # local timezone when rendering.
        captured_dt = datetime.now(UTC)
    elif captured_at and captured_at.strip():
        raw = captured_at.strip().replace("T", " ")
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                captured_dt = datetime.strptime(raw, fmt)
                break
            except ValueError:
                continue
    try:
        note_id = notes.create(
            body,
            entity_hint=entity_hint,
            merchant_hint=merchant_hint,
            captured_at=captured_dt,
            txn_hash=th,
        )
    except ValueError:
        return Response(status_code=400, content="empty note")

    # Mirror to the ledger so a DB wipe can rebuild this note. We emit
    # a fresh directive after the AI parse fills in hints too, via
    # _parse_note_in_background below — so the AI-derived merchant_hint
    # / entity_hint / active_from / etc. survive reconstruction.
    try:
        append_note(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            note_id=note_id,
            body=body.strip(),
            captured_at=captured_dt,
            merchant_hint=merchant_hint,
            entity_hint=entity_hint,
            txn_hash=th,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("note directive write failed for note %s: %s", note_id, exc)

    # Phase 3: fan out a non-blocking AI parse so the HTTP response stays fast.
    if ai.enabled and not ai.spend_cap_reached():
        background.add_task(_parse_note_in_background, note_id, ai, settings)

    if "hx-request" in {k.lower() for k in request.headers.keys()}:
        # Respond with the refreshed list (swapped into #note-recent)
        # plus an out-of-band toast so the user sees a confirmation
        # flash. HTMX processes every element with hx-swap-oob on the
        # response, regardless of the primary hx-target, so this gets
        # both UI updates from one POST.
        ctx = {"recent": notes.list(limit=10), "request": request}
        list_html = request.app.state.templates.get_template(
            "partials/note_list.html",
        ).render(ctx)
        toast_html = (
            f'<div id="toast-area" hx-swap-oob="innerHTML">'
            f'<div class="toast success">Saved note #{note_id}.</div>'
            f'</div>'
        )
        return HTMLResponse(
            list_html + toast_html,
            status_code=200,
            headers={"HX-Trigger": "note-saved"},
        )
    return Response(status_code=204)


async def _parse_note_in_background(
    note_id: int, ai: AIService, settings: Settings,
) -> None:
    try:
        notes = NoteService(ai.conn)
        note = notes.get(note_id)
        if note is None:
            return
        client = ai.new_client()
        if client is None:
            return
        try:
            # Phase 3: entity list comes from app_settings; in absence of a
            # registered list, pass an empty hint set — the model leaves
            # entity_hint null which is fine.
            known_entities = _known_entities(ai)
            annotations = await ai_parse_note(
                client,
                note_id=note.id,
                body=note.body,
                captured_at=note.captured_at,
                entities=known_entities,
                model=ai.model_for("parse_note"),
            )
        finally:
            await client.aclose()
        if annotations is None:
            return
        notes.update_hints(
            note_id,
            merchant_hint=annotations.merchant_hint,
            entity_hint=annotations.entity_hint,
            active_from=annotations.active_from,
            active_to=annotations.active_to,
            keywords=annotations.keywords if annotations.keywords else None,
            card_override=annotations.card_override,
        )
        # Re-stamp the note in the ledger now that AI has filled in
        # hints. The reader keeps the last-seen per id, so this
        # supersedes the earlier barebones directive.
        try:
            refreshed = notes.get(note_id)
            if refreshed is not None:
                append_note(
                    connector_config=settings.connector_config_path,
                    main_bean=settings.ledger_main,
                    note_id=refreshed.id,
                    body=refreshed.body,
                    captured_at=refreshed.captured_at,
                    merchant_hint=refreshed.merchant_hint,
                    entity_hint=refreshed.entity_hint,
                    active_from=refreshed.active_from,
                    active_to=refreshed.active_to,
                    keywords=list(refreshed.keywords or ()),
                    card_override=refreshed.card_override,
                    status=refreshed.status,
                )
        except Exception as exc:  # noqa: BLE001
            log.warning("note re-stamp after AI parse failed for %s: %s", note_id, exc)
    except Exception as exc:
        log.warning("ai note parse failed for note %s: %s", note_id, exc)


def _known_entities(ai: AIService) -> list[str]:
    raw = ai.settings_store.get("known_entities")
    if not raw:
        return []
    return [e.strip() for e in raw.split(",") if e.strip()]

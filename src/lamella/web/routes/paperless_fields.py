# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Settings page for the Paperless custom-field role mapping.

Shows every row in paperless_field_map with its current canonical role, and
lets the user change it. "Refresh from Paperless" pulls the latest
custom_fields list (so new Paperless fields show up) and re-runs the
keyword seeding for any field that wasn't user-overridden.

The Paperless sync job also calls `sync_fields()` on each tick, so most
users never need to hit this page — but it exists for the cases where
auto-guessing is wrong (your "Amount" field that turned out to be "item
count", etc.) and for bootstrapping a fresh Paperless instance that has
none of the canonical fields yet (see the setup panel at the top of the
page: per role, it offers to either classify an existing field that
looks like a fit, or POST a new custom field into Paperless itself).
"""
from __future__ import annotations

import logging
import json

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from lamella.core.config import Settings
from lamella.core.settings.store import AppSettingsStore
from lamella.web.deps import get_app_settings_store, get_db, get_settings
from lamella.adapters.paperless.client import PaperlessClient, PaperlessError
from lamella.features.paperless_bridge.field_map import (
    CANONICAL_ROLE_DEFAULTS,
    CANONICAL_ROLES,
    SETUP_CRITICAL_ROLES,
    SETUP_OPTIONAL_ROLES,
    get_map,
    insert_created_field,
    set_role,
    suggest_for_role,
    sync_fields,
)
from lamella.features.paperless_bridge.field_map_writer import append_field_mapping
from lamella.core.ledger_writer import BeanCheckError

log = logging.getLogger(__name__)

router = APIRouter()


def _build_setup_panel(conn, mapping) -> list[dict]:
    """One row per canonical role on the setup panel at the top of the
    page. Each row tells the template which of three states to render:

      * ``status='mapped'``      — at least one field maps to this role
      * ``status='suggest'``     — no mapping, but an ignored field has
                                   a name that looks like it would fit;
                                   offer a one-click classify button
      * ``status='create'``      — no mapping, no candidate; offer the
                                   "Create in Paperless" button using
                                   CANONICAL_ROLE_DEFAULTS
    """
    panel: list[dict] = []
    mapped_by_role: dict[str, list[dict]] = {}
    for row in mapping.rows:
        role = row["canonical_role"]
        if role == "ignore":
            continue
        mapped_by_role.setdefault(role, []).append(row)

    for role in (*SETUP_CRITICAL_ROLES, *SETUP_OPTIONAL_ROLES):
        item: dict = {
            "role": role,
            "critical": role in SETUP_CRITICAL_ROLES,
        }
        if mapped_by_role.get(role):
            item["status"] = "mapped"
            item["mapped_fields"] = mapped_by_role[role]
            panel.append(item)
            continue
        suggestions = suggest_for_role(conn, role)
        if suggestions:
            item["status"] = "suggest"
            item["suggestions"] = suggestions
        else:
            default_name, data_type = CANONICAL_ROLE_DEFAULTS.get(
                role, (role.replace("_", " ").title(), "string"),
            )
            item["status"] = "create"
            item["default_name"] = default_name
            item["data_type"] = data_type
        panel.append(item)
    return panel


@router.get("/settings/paperless-fields", response_class=HTMLResponse)
def page(
    request: Request,
    saved: bool = False,
    refreshed: bool = False,
    created: str | None = None,
    classified: str | None = None,
    doc_types_saved: bool = False,
    settings: Settings = Depends(get_settings),
    store: AppSettingsStore = Depends(get_app_settings_store),
    conn = Depends(get_db),
):
    mapping = get_map(conn)
    doc_type_rows = conn.execute(
        "SELECT DISTINCT document_type_id, document_type_name "
        "FROM paperless_doc_index "
        "WHERE document_type_id IS NOT NULL "
        "ORDER BY LOWER(COALESCE(document_type_name, '')) ASC"
    ).fetchall()
    raw_doc_roles = store.get("paperless_doc_type_roles")
    doc_type_roles: dict[int, str] = {}
    if raw_doc_roles:
        try:
            parsed = json.loads(raw_doc_roles)
            if isinstance(parsed, dict):
                for k, v in parsed.items():
                    if str(k).strip().isdigit() and str(v) in {"receipt", "invoice", "ignore"}:
                        doc_type_roles[int(k)] = str(v)
        except Exception:
            doc_type_roles = {}

    # Writeback status panel. The four ``Lamella_*`` fields are write
    # targets, not read sources; the user setting
    # ``paperless_writeback_enabled`` is the master gate (defaults OFF
    # per ADR-0044). Surface the live state plus recent activity from
    # ``paperless_writeback_log`` so the user can see whether writes
    # are actually happening, instead of having to guess from the
    # role label alone.
    try:
        raw = store.get("paperless_writeback_enabled")
        if raw is not None:
            writeback_enabled = str(raw).strip().lower() in {"1", "true", "yes", "on"}
        else:
            writeback_enabled = bool(settings.paperless_writeback_enabled)
    except Exception:  # noqa: BLE001
        writeback_enabled = bool(settings.paperless_writeback_enabled)

    writeback_recent: list[dict] = []
    writeback_total = 0
    try:
        writeback_total = int(
            conn.execute(
                "SELECT COUNT(*) FROM paperless_writeback_log"
            ).fetchone()[0]
        )
        writeback_recent = [
            dict(r) for r in conn.execute(
                "SELECT paperless_id, kind, written_at, fields_count "
                "FROM paperless_writeback_log "
                "ORDER BY written_at DESC LIMIT 5"
            ).fetchall()
        ]
    except Exception:  # noqa: BLE001
        # Table may not exist yet on a fresh install.
        pass

    ctx = {
        "rows": mapping.rows,
        "roles": CANONICAL_ROLES,
        "saved": saved,
        "refreshed": refreshed,
        "created": created,
        "classified": classified,
        "setup_panel": _build_setup_panel(conn, mapping),
        "paperless_configured": settings.paperless_configured,
        "doc_types": [dict(r) for r in doc_type_rows],
        "doc_type_roles": doc_type_roles,
        "doc_types_saved": doc_types_saved,
        "writeback_enabled": writeback_enabled,
        "writeback_total": writeback_total,
        "writeback_recent": writeback_recent,
    }
    return request.app.state.templates.TemplateResponse(
        request, "settings_paperless_fields.html", ctx
    )


@router.post("/settings/paperless-fields/document-types")
async def save_document_type_roles(
    request: Request,
    store: AppSettingsStore = Depends(get_app_settings_store),
):
    form = await request.form()
    roles: dict[str, str] = {}
    for key, value in form.multi_items():
        if not key.startswith("doc_type_role_"):
            continue
        type_id = key[len("doc_type_role_"):].strip()
        role = str(value).strip().lower()
        if not type_id.isdigit() or role not in {"receipt", "invoice", "ignore"}:
            continue
        roles[type_id] = role
    store.set("paperless_doc_type_roles", json.dumps(roles))
    return RedirectResponse("/settings/paperless-fields?doc_types_saved=1", status_code=303)


@router.post("/settings/paperless-fields/refresh", response_class=HTMLResponse)
def refresh(
    request: Request,
    settings: Settings = Depends(get_settings),
):
    """Pull latest custom_fields from Paperless. Runs as a job so
    the user sees the API call happen instead of a frozen page."""
    if not settings.paperless_configured:
        raise HTTPException(
            status_code=400,
            detail="Paperless is not configured (PAPERLESS_URL / PAPERLESS_API_TOKEN).",
        )

    def _work(ctx):
        import asyncio
        ctx.emit("Opening Paperless client …", outcome="info")
        client = PaperlessClient(
            base_url=settings.paperless_url,  # type: ignore[arg-type]
            api_token=settings.paperless_api_token.get_secret_value(),  # type: ignore[union-attr]
            extra_headers=settings.paperless_extra_headers(),
        )
        loop = asyncio.new_event_loop()
        try:
            ctx.emit(
                "Fetching custom_fields from Paperless …", outcome="info",
            )
            try:
                stats = loop.run_until_complete(
                    sync_fields(request.app.state.db, client)
                )
            except PaperlessError as exc:
                ctx.emit(f"Paperless error: {exc}", outcome="error")
                raise
            finally:
                try:
                    loop.run_until_complete(client.aclose())
                except Exception:  # noqa: BLE001
                    pass
        finally:
            loop.close()
        log.info("paperless field map refresh: %s", stats)
        ctx.emit(
            f"Refreshed field map · added {stats.get('added', 0)}, "
            f"updated {stats.get('updated', 0)}, unchanged "
            f"{stats.get('unchanged', 0)}.",
            outcome="success",
        )
        return {"stats": stats}

    runner = request.app.state.job_runner
    job_id = runner.submit(
        kind="paperless-fields-refresh",
        title="Refreshing Paperless field map",
        fn=_work,
        return_url="/settings/paperless-fields?refreshed=1",
    )
    return request.app.state.templates.TemplateResponse(
        request, "partials/_job_modal.html",
        {"job_id": job_id, "on_close_url": "/settings/paperless-fields"},
    )


@router.post("/settings/paperless-fields/create", response_class=HTMLResponse)
def create_field(
    request: Request,
    role: str = Form(...),
    name: str = Form(""),
    settings: Settings = Depends(get_settings),
):
    """Create a custom field in Paperless for `role` and record the
    mapping (both in paperless_field_map AND as a ``custom
    "paperless-field"`` directive so reconstruct rebuilds from the
    ledger). Runs as a job so the user sees: list existing Paperless
    fields → check for duplicates → create (or map existing) → write
    ledger → bean-check."""
    if not settings.paperless_configured:
        raise HTTPException(
            status_code=400,
            detail="Paperless is not configured (PAPERLESS_URL / PAPERLESS_API_TOKEN).",
        )
    if role not in CANONICAL_ROLE_DEFAULTS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"cannot auto-create a field for role {role!r}; no default "
                f"data_type is defined. Create it in Paperless manually "
                f"and assign the role on this page."
            ),
        )
    default_name, data_type = CANONICAL_ROLE_DEFAULTS[role]
    chosen_name = (name or "").strip() or default_name
    conn = request.app.state.db

    def _work(ctx):
        import asyncio
        import urllib.parse as _up
        from lamella.features.paperless_bridge.field_map import _guess_role

        ctx.emit("Opening Paperless client …", outcome="info")
        client = PaperlessClient(
            base_url=settings.paperless_url,  # type: ignore[arg-type]
            api_token=settings.paperless_api_token.get_secret_value(),  # type: ignore[union-attr]
            extra_headers=settings.paperless_extra_headers(),
        )
        loop = asyncio.new_event_loop()
        try:
            ctx.emit(
                "Listing existing Paperless fields to check for duplicates …",
                outcome="info",
            )
            try:
                existing_fields = loop.run_until_complete(
                    client._load_field_cache()
                )
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "paperless-fields/create: pre-check list failed: %s", exc,
                )
                ctx.emit(
                    f"Existing-fields pre-check failed ({exc}) — will create.",
                    outcome="info",
                )
                existing_fields = {}

            for existing in existing_fields.values():
                exact_match = (existing.name or "").strip().lower() == chosen_name.strip().lower()
                role_match = _guess_role(existing.name or "") == role
                if exact_match or role_match:
                    ctx.emit(
                        f"Found existing Paperless field '{existing.name}' "
                        f"— mapping it to role '{role}' instead of creating "
                        f"a duplicate.",
                        outcome="info",
                    )
                    try:
                        append_field_mapping(
                            connector_config=settings.connector_config_path,
                            main_bean=settings.ledger_main,
                            paperless_field_id=existing.id,
                            paperless_field_name=existing.name,
                            canonical_role=role,
                        )
                    except BeanCheckError as exc:
                        log.warning(
                            "paperless-fields/create: ledger write for existing "
                            "field %r failed: %s", existing.name, exc,
                        )
                        ctx.emit(
                            f"Ledger write refused by bean-check: {exc}",
                            outcome="error",
                        )
                    insert_created_field(
                        conn,
                        field_id=existing.id,
                        field_name=existing.name,
                        canonical_role=role,
                    )
                    ctx.emit(
                        f"Mapped existing field #{existing.id} "
                        f"'{existing.name}' → {role}",
                        outcome="success",
                    )
                    ctx.set_return_url(
                        f"/settings/paperless-fields?"
                        f"mapped_existing={_up.quote(existing.name)}"
                        f"&role={_up.quote(role)}"
                    )
                    return {"mapped_existing": existing.name, "role": role}

            ctx.emit(
                f"Creating new Paperless custom field '{chosen_name}' "
                f"(data_type={data_type}) …",
                outcome="info",
            )
            try:
                field = loop.run_until_complete(
                    client.create_custom_field(
                        name=chosen_name, data_type=data_type,
                    )
                )
            except PaperlessError as exc:
                ctx.emit(f"Paperless rejected the create: {exc}", outcome="error")
                ctx.set_return_url(
                    f"/settings/paperless-fields?"
                    f"create_failed={_up.quote(chosen_name)}"
                    f"&reason={_up.quote(str(exc)[:400])}"
                )
                raise

            ctx.emit(
                f"Created Paperless field #{field.id} — writing ledger "
                f"directive + running bean-check …",
                outcome="info",
            )
            try:
                append_field_mapping(
                    connector_config=settings.connector_config_path,
                    main_bean=settings.ledger_main,
                    paperless_field_id=field.id,
                    paperless_field_name=field.name,
                    canonical_role=role,
                )
            except BeanCheckError as exc:
                log.error(
                    "paperless create-field ledger write rejected: %s", exc,
                )
                ctx.emit(
                    f"bean-check blocked the ledger write: {exc}",
                    outcome="error",
                )
                raise
            insert_created_field(
                conn,
                field_id=field.id,
                field_name=field.name,
                canonical_role=role,
            )
            log.info(
                "paperless field created: id=%d name=%r data_type=%s role=%s",
                field.id, field.name, data_type, role,
            )
            ctx.emit(
                f"Mapped new field #{field.id} '{field.name}' → {role}.",
                outcome="success",
            )
            ctx.set_return_url(
                f"/settings/paperless-fields?created={role}"
            )
            return {"created_id": field.id, "created_name": field.name, "role": role}
        finally:
            try:
                loop.run_until_complete(client.aclose())
            except Exception:  # noqa: BLE001
                pass
            loop.close()

    runner = request.app.state.job_runner
    job_id = runner.submit(
        kind="paperless-field-create",
        title=f"Creating Paperless field for role '{role}' ({chosen_name})",
        fn=_work,
    )
    return request.app.state.templates.TemplateResponse(
        request, "partials/_job_modal.html",
        {"job_id": job_id, "on_close_url": "/settings/paperless-fields"},
    )


@router.post("/settings/paperless-fields/classify")
async def classify_field(
    role: str = Form(...),
    field_id: int = Form(...),
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    """One-click classify: the setup panel suggested an existing
    ignored field as a fit for `role`; this records that assignment
    in both the ledger and the DB."""
    if role not in CANONICAL_ROLES or role == "ignore":
        raise HTTPException(status_code=400, detail=f"invalid role {role!r}")
    row = conn.execute(
        "SELECT paperless_field_name FROM paperless_field_map "
        "WHERE paperless_field_id = ?",
        (field_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"Paperless field id {field_id} not in local map; refresh first.",
        )
    try:
        append_field_mapping(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            paperless_field_id=field_id,
            paperless_field_name=row["paperless_field_name"] or "",
            canonical_role=role,
        )
    except BeanCheckError as exc:
        log.error("paperless classify ledger write rejected: %s", exc)
        raise HTTPException(status_code=500, detail=f"bean-check: {exc}")
    set_role(conn, field_id, role)
    log.info(
        "paperless field classified: id=%d name=%r role=%s",
        field_id, row["paperless_field_name"], role,
    )
    return RedirectResponse(
        url=f"/settings/paperless-fields?classified={role}",
        status_code=303,
    )


@router.post("/settings/paperless-fields")
async def save_roles(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    """Save per-row role assignments. The form has one <select name="role_{id}">
    per field; we read the raw form dict because the row set is dynamic."""
    form = await request.form()
    mapping = get_map(conn)
    existing_names = {row.paperless_field_id: row.paperless_field_name for row in mapping.rows}
    changed = 0
    for key, value in form.multi_items():
        if not key.startswith("role_"):
            continue
        try:
            fid = int(key[len("role_"):])
        except ValueError:
            continue
        role = str(value).strip()
        if role not in CANONICAL_ROLES:
            continue
        # Dual-write: stamp the user's explicit choice into the ledger
        # before updating the cache. If bean-check rejects we abort the
        # whole save — partial writes are worse than a re-render with
        # the old roles still shown.
        try:
            append_field_mapping(
                connector_config=settings.connector_config_path,
                main_bean=settings.ledger_main,
                paperless_field_id=fid,
                paperless_field_name=existing_names.get(fid, ""),
                canonical_role=role,
            )
        except BeanCheckError as exc:
            log.error("paperless field ledger write rejected: %s", exc)
            raise HTTPException(status_code=500, detail=f"bean-check: {exc}")
        set_role(conn, fid, role)
        changed += 1
    log.info("paperless field map: %d rows updated by user", changed)
    return RedirectResponse(url="/settings/paperless-fields?saved=1", status_code=303)

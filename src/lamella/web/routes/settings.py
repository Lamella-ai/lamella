# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from lamella.features.ai_cascade.service import AIService
from lamella.core.config import Settings
from lamella.web.deps import get_ai_service, get_app_settings_store, get_settings
from lamella.features.notifications.digests import WEEKDAYS
from lamella.core.settings.store import AppSettingsStore

router = APIRouter()

AI_MODEL_KEYS = (
    "openrouter_model",
    "openrouter_model_classify_txn",
    "openrouter_model_match_receipt",
    "openrouter_model_parse_note",
    "openrouter_model_fallback",
    "openrouter_model_classify_txn_fallback",
    "openrouter_model_receipt_verify",
)


def _render(
    request: Request,
    settings: Settings,
    store: AppSettingsStore,
    ai: AIService,
    *,
    saved: bool = False,
) -> HTMLResponse:
    models = {k: store.get(k) or "" for k in AI_MODEL_KEYS}
    spend_cap_raw = store.get("ai_max_monthly_spend_usd") or ""
    prompt_price_raw = store.get("ai_price_usd_per_1k_prompt") or ""
    completion_price_raw = store.get("ai_price_usd_per_1k_completion") or ""
    cost_summary = ai.cost_summary()
    ctx = {
        "settings": settings,
        "masked_token": settings.masked_paperless_token(),
        "version": request.app.state.version,
        "saved": saved,
        "ai_enabled": ai.enabled,
        "models": models,
        "spend_cap_raw": spend_cap_raw,
        "prompt_price_raw": prompt_price_raw,
        "completion_price_raw": completion_price_raw,
        "cost_summary": cost_summary,
        "cap_usd": ai.monthly_cap_usd(),
        "prompt_price": ai.price_prompt_per_1k(),
        "completion_price": ai.price_completion_per_1k(),
        "summary": cost_summary,
        # Phase 5 — notification + mileage surface.
        "ntfy_base_url": store.get("ntfy_base_url") or settings.ntfy_base_url,
        "ntfy_topic": store.get("ntfy_topic") or settings.ntfy_topic or "",
        "ntfy_token_set": bool(settings.ntfy_token),
        "pushover_configured": settings.pushover_enabled,
        "mileage_rate_raw": store.get("mileage_rate") or f"{settings.mileage_rate:.3f}",
        "notify_min_fixme_usd_raw": store.get("notify_min_fixme_usd") or f"{settings.notify_min_fixme_usd:g}",
        "notify_digest_day": store.get("notify_digest_day") or settings.notify_digest_day,
        "weekdays": WEEKDAYS,
        # Phase 6.
        "reports_output_dir": str(settings.reports_output_resolved),
        "audit_max_receipt_bytes_raw": store.get("audit_max_receipt_bytes") or str(settings.audit_max_receipt_bytes),
        "estimated_tax_flat_rate_raw": store.get("estimated_tax_flat_rate") or f"{settings.estimated_tax_flat_rate:.3f}",
        "recurring_scan_window_days_raw": store.get("recurring_scan_window_days") or str(settings.recurring_scan_window_days),
        "recurring_min_occurrences_raw": store.get("recurring_min_occurrences") or str(settings.recurring_min_occurrences),
        "budget_alert_channels_raw": store.get("budget_alert_channels") or settings.budget_alert_channels,
        # Phase 7 — spreadsheet import.
        "import_upload_dir": str(settings.import_upload_dir_resolved),
        "import_ledger_output_dir": str(settings.import_ledger_output_dir_resolved),
        "import_retention_days_raw": store.get("import_retention_days") or str(settings.import_retention_days),
        "import_max_upload_bytes_raw": store.get("import_max_upload_bytes") or str(settings.import_max_upload_bytes),
        "import_ai_column_map_model_raw": store.get("import_ai_column_map_model") or (settings.import_ai_column_map_model or ""),
        "import_ai_confidence_threshold_raw": store.get("import_ai_confidence_threshold") or f"{settings.import_ai_confidence_threshold:.2f}",
        # Phase H — vector-search toggle. Default True in config;
        # an app_settings override (shaped 'false'/'0'/'no'/'off')
        # turns it off without touching the ledger or the bean files.
        "ai_vector_search_enabled": (
            (str(store.get("ai_vector_search_enabled") or "").strip().lower()
                not in ("0", "false", "no", "off"))
            if store.get("ai_vector_search_enabled") is not None
            else settings.ai_vector_search_enabled
        ),
        # Two-agent cascade — Haiku → Opus-on-low-confidence.
        "ai_fallback_enabled": (
            (str(store.get("ai_fallback_enabled") or "").strip().lower()
                not in ("0", "false", "no", "off"))
            if store.get("ai_fallback_enabled") is not None
            else settings.ai_fallback_enabled
        ),
        "fallback_threshold_raw": (
            store.get("ai_fallback_confidence_threshold")
            or f"{settings.ai_fallback_confidence_threshold:.2f}"
        ),
        # Paperless verify + writeback.
        "paperless_writeback_enabled": (
            (str(store.get("paperless_writeback_enabled") or "").strip().lower()
                not in ("0", "false", "no", "off"))
            if store.get("paperless_writeback_enabled") is not None
            else settings.paperless_writeback_enabled
        ),
        "import_known_classes": (
            "wf_annotated, paypal, amazon_seller, amazon_merch, "
            "amazon_purchases, amex, costco_citibank, chase, ebay, eidl, "
            "check_writing, generic_csv, generic_xlsx"
        ),
    }
    return request.app.state.templates.TemplateResponse(request, "settings.html", ctx)


def _build_settings_context(
    request: Request,
    settings: Settings,
    store: AppSettingsStore,
    ai: AIService,
    *,
    saved: bool = False,
) -> dict:
    """Same context dict _render() builds, returned as a plain dict so
    the per-area sub-pages (ADR-0047) can pass it to whichever
    settings_<area>.html template they own."""
    models = {k: store.get(k) or "" for k in AI_MODEL_KEYS}
    spend_cap_raw = store.get("ai_max_monthly_spend_usd") or ""
    prompt_price_raw = store.get("ai_price_usd_per_1k_prompt") or ""
    completion_price_raw = store.get("ai_price_usd_per_1k_completion") or ""
    cost_summary = ai.cost_summary()
    return {
        "settings": settings,
        "masked_token": settings.masked_paperless_token(),
        "version": request.app.state.version,
        "saved": saved,
        "ai_enabled": ai.enabled,
        "models": models,
        "spend_cap_raw": spend_cap_raw,
        "prompt_price_raw": prompt_price_raw,
        "completion_price_raw": completion_price_raw,
        "cost_summary": cost_summary,
        "cap_usd": ai.monthly_cap_usd(),
        "prompt_price": ai.price_prompt_per_1k(),
        "completion_price": ai.price_completion_per_1k(),
        "summary": cost_summary,
        "ntfy_base_url": store.get("ntfy_base_url") or settings.ntfy_base_url,
        "ntfy_topic": store.get("ntfy_topic") or settings.ntfy_topic or "",
        "ntfy_token_set": bool(settings.ntfy_token),
        "pushover_configured": settings.pushover_enabled,
        "mileage_rate_raw": store.get("mileage_rate") or f"{settings.mileage_rate:.3f}",
        "notify_min_fixme_usd_raw": store.get("notify_min_fixme_usd") or f"{settings.notify_min_fixme_usd:g}",
        "notify_digest_day": store.get("notify_digest_day") or settings.notify_digest_day,
        "weekdays": WEEKDAYS,
        "reports_output_dir": str(settings.reports_output_resolved),
        "audit_max_receipt_bytes_raw": store.get("audit_max_receipt_bytes") or str(settings.audit_max_receipt_bytes),
        "estimated_tax_flat_rate_raw": store.get("estimated_tax_flat_rate") or f"{settings.estimated_tax_flat_rate:.3f}",
        "recurring_scan_window_days_raw": store.get("recurring_scan_window_days") or str(settings.recurring_scan_window_days),
        "recurring_min_occurrences_raw": store.get("recurring_min_occurrences") or str(settings.recurring_min_occurrences),
        "budget_alert_channels_raw": store.get("budget_alert_channels") or settings.budget_alert_channels,
        "import_upload_dir": str(settings.import_upload_dir_resolved),
        "import_ledger_output_dir": str(settings.import_ledger_output_dir_resolved),
        "import_retention_days_raw": store.get("import_retention_days") or str(settings.import_retention_days),
        "import_max_upload_bytes_raw": store.get("import_max_upload_bytes") or str(settings.import_max_upload_bytes),
        "import_ai_column_map_model_raw": store.get("import_ai_column_map_model") or (settings.import_ai_column_map_model or ""),
        "import_ai_confidence_threshold_raw": store.get("import_ai_confidence_threshold") or f"{settings.import_ai_confidence_threshold:.2f}",
        "ai_vector_search_enabled": (
            (str(store.get("ai_vector_search_enabled") or "").strip().lower()
                not in ("0", "false", "no", "off"))
            if store.get("ai_vector_search_enabled") is not None
            else settings.ai_vector_search_enabled
        ),
        "ai_fallback_enabled": (
            (str(store.get("ai_fallback_enabled") or "").strip().lower()
                not in ("0", "false", "no", "off"))
            if store.get("ai_fallback_enabled") is not None
            else settings.ai_fallback_enabled
        ),
        "fallback_threshold_raw": (
            store.get("ai_fallback_confidence_threshold")
            or f"{settings.ai_fallback_confidence_threshold:.2f}"
        ),
        "paperless_writeback_enabled": (
            (str(store.get("paperless_writeback_enabled") or "").strip().lower()
                not in ("0", "false", "no", "off"))
            if store.get("paperless_writeback_enabled") is not None
            else settings.paperless_writeback_enabled
        ),
        "import_known_classes": (
            "wf_annotated, paypal, amazon_seller, amazon_merch, "
            "amazon_purchases, amex, costco_citibank, chase, ebay, eidl, "
            "check_writing, generic_csv, generic_xlsx"
        ),
    }


def _render_subpage(
    request: Request,
    template_name: str,
    settings: Settings,
    store: AppSettingsStore,
    ai: AIService,
    *,
    saved: bool = False,
) -> HTMLResponse:
    ctx = _build_settings_context(
        request, settings, store, ai, saved=saved
    )
    return request.app.state.templates.TemplateResponse(
        request, template_name, ctx
    )


@router.get("/settings", response_class=HTMLResponse)
def settings_page(
    request: Request,
    settings: Settings = Depends(get_settings),
    store: AppSettingsStore = Depends(get_app_settings_store),
    ai: AIService = Depends(get_ai_service),
):
    return _render(request, settings, store, ai)


_NUMBER_LOCALES = (
    ("en_US", "1,234.56", "comma thousands · period decimal"),
    ("en_EU", "1.234,56", "period thousands · comma decimal"),
)


@router.get("/settings/paperless", response_class=HTMLResponse)
def settings_paperless_page(
    request: Request,
    saved: str | None = None,
    settings: Settings = Depends(get_settings),
    store: AppSettingsStore = Depends(get_app_settings_store),
    ai: AIService = Depends(get_ai_service),
):
    """ADR-0047 sub-page — Paperless URL + API token."""
    return _render_subpage(
        request, "settings_paperless.html",
        settings, store, ai, saved=bool(saved),
    )


@router.get("/settings/ai", response_class=HTMLResponse)
def settings_ai_page(
    request: Request,
    saved: str | None = None,
    settings: Settings = Depends(get_settings),
    store: AppSettingsStore = Depends(get_app_settings_store),
    ai: AIService = Depends(get_ai_service),
):
    """ADR-0047 sub-page — AI cascade, models, spend cap, writeback."""
    return _render_subpage(
        request, "settings_ai.html",
        settings, store, ai, saved=bool(saved),
    )


@router.get("/settings/notifications", response_class=HTMLResponse)
def settings_notifications_page(
    request: Request,
    saved: str | None = None,
    settings: Settings = Depends(get_settings),
    store: AppSettingsStore = Depends(get_app_settings_store),
    ai: AIService = Depends(get_ai_service),
):
    """ADR-0047 sub-page — ntfy + Pushover channels and thresholds."""
    return _render_subpage(
        request, "settings_notifications.html",
        settings, store, ai, saved=bool(saved),
    )


@router.get("/settings/reports", response_class=HTMLResponse)
def settings_reports_page(
    request: Request,
    saved: str | None = None,
    settings: Settings = Depends(get_settings),
    store: AppSettingsStore = Depends(get_app_settings_store),
    ai: AIService = Depends(get_ai_service),
):
    """ADR-0047 sub-page — reports / budgets / recurring / import tuning."""
    return _render_subpage(
        request, "settings_reports.html",
        settings, store, ai, saved=bool(saved),
    )


@router.get("/settings/general", response_class=HTMLResponse)
def general_settings_page(
    request: Request,
    saved: str | None = None,
    settings: Settings = Depends(get_settings),
    store: AppSettingsStore = Depends(get_app_settings_store),
):
    """ADR-0047 sub-page — focused settings that don't fit anywhere
    else (number formatting today; date format / first-day-of-week
    will land here too as they're added). Lives at its own route per
    the dashboard pattern; the /settings landing only links here, no
    inline form."""
    return request.app.state.templates.TemplateResponse(
        request, "settings_general.html",
        {
            "settings": settings,
            "saved": saved,
            "number_locale": getattr(settings, "number_locale", "en_US"),
            "number_locale_options": _NUMBER_LOCALES,
        },
    )


@router.post("/settings/general")
def general_settings_save(
    request: Request,
    number_locale: str = Form("en_US"),
    settings: Settings = Depends(get_settings),
    store: AppSettingsStore = Depends(get_app_settings_store),
):
    from fastapi.responses import RedirectResponse
    valid_locales = {code for code, _label, _hint in _NUMBER_LOCALES}
    if number_locale not in valid_locales:
        number_locale = "en_US"
    store.set("number_locale", number_locale)
    # Apply to the live Settings object so the next page render uses
    # it without an app restart (fmt_amount reads from app.state.settings).
    try:
        settings.number_locale = number_locale  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001
        pass
    return RedirectResponse("/settings/general?saved=1", status_code=303)


def _finalize(request: Request, settings: Settings, store: AppSettingsStore) -> None:
    """Apply store overrides to the live Settings object and rebuild
    the notification dispatcher. Shared by every per-area POST handler
    (ADR-0047) so the side effects of saving stay in one place."""
    overrides = store.all()
    settings.apply_kv_overrides(overrides)
    rebuild = getattr(request.app.state, "_notify_rebuild", None)
    if rebuild is not None:
        try:
            rebuild()
        except Exception:  # noqa: BLE001
            pass


@router.post("/settings/paperless")
def settings_paperless_save(
    request: Request,
    paperless_url: str | None = Form(default=None),
    paperless_api_token: str | None = Form(default=None),
    paperless_writeback_enabled: str | None = Form(default=None),
    settings: Settings = Depends(get_settings),
    store: AppSettingsStore = Depends(get_app_settings_store),
):
    """ADR-0047 — Paperless URL + API token + writeback toggle.

    Writeback was historically wired into the AI page because it
    drives the verify-cascade PATCHes; users looking for it on the
    Paperless settings page found it missing and assumed it had
    been deleted. The toggle lives in BOTH places so it can be
    found from either entry point. The store is the single source
    of truth so flipping it from either page has the same effect.
    """
    if paperless_url is not None:
        store.set("paperless_url", paperless_url.strip())
    if paperless_api_token:
        store.set("paperless_api_token", paperless_api_token)
    # Checkbox semantics: present in form data = on, absent = off.
    # ``paperless_writeback_enabled`` may be ``None`` (form posted
    # without the field, e.g. partial save) — distinguish from
    # explicit "off" via the always-posted hidden marker.
    if paperless_writeback_enabled is not None:
        store.set(
            "paperless_writeback_enabled",
            "1" if paperless_writeback_enabled else "0",
        )
    _finalize(request, settings, store)
    return RedirectResponse("/settings/paperless?saved=1", status_code=303)


@router.post("/settings/ai")
def settings_ai_save(
    request: Request,
    openrouter_model: str | None = Form(default=None),
    openrouter_model_classify_txn: str | None = Form(default=None),
    openrouter_model_match_receipt: str | None = Form(default=None),
    openrouter_model_parse_note: str | None = Form(default=None),
    openrouter_model_fallback: str | None = Form(default=None),
    openrouter_model_classify_txn_fallback: str | None = Form(default=None),
    openrouter_model_receipt_verify: str | None = Form(default=None),
    ai_fallback_enabled: str | None = Form(default=None),
    ai_fallback_confidence_threshold: str | None = Form(default=None),
    paperless_writeback_enabled: str | None = Form(default=None),
    ai_max_monthly_spend_usd: str | None = Form(default=None),
    ai_price_usd_per_1k_prompt: str | None = Form(default=None),
    ai_price_usd_per_1k_completion: str | None = Form(default=None),
    ai_vector_search_enabled: str | None = Form(default=None),
    settings: Settings = Depends(get_settings),
    store: AppSettingsStore = Depends(get_app_settings_store),
):
    """ADR-0047 — AI cascade, models, spend cap, vector search,
    writeback toggle, fallback toggle + threshold."""
    model_updates = {
        "openrouter_model": openrouter_model,
        "openrouter_model_classify_txn": openrouter_model_classify_txn,
        "openrouter_model_match_receipt": openrouter_model_match_receipt,
        "openrouter_model_parse_note": openrouter_model_parse_note,
        "openrouter_model_fallback": openrouter_model_fallback,
        "openrouter_model_classify_txn_fallback": openrouter_model_classify_txn_fallback,
        "openrouter_model_receipt_verify": openrouter_model_receipt_verify,
    }
    for key, value in model_updates.items():
        if value is not None:
            v = value.strip()
            if v:
                store.set(key, v)
            else:
                store.delete(key)

    for key, raw in (
        ("ai_max_monthly_spend_usd", ai_max_monthly_spend_usd),
        ("ai_price_usd_per_1k_prompt", ai_price_usd_per_1k_prompt),
        ("ai_price_usd_per_1k_completion", ai_price_usd_per_1k_completion),
    ):
        if raw is not None:
            v = raw.strip()
            if v:
                try:
                    float(v)
                except ValueError:
                    continue
                store.set(key, v)
            else:
                store.delete(key)

    # Phase H — vector-search toggle. Standard HTML checkbox semantics:
    # field present == "1" (on); field absent == off. We write the
    # app_settings row explicitly on both branches so a user who
    # turned it off and then back on doesn't end up with a stale
    # override. Now that this lives on the AI form only, we don't
    # need the cross-form guard anymore.
    if ai_vector_search_enabled is not None:
        store.set("ai_vector_search_enabled", "1")
    else:
        store.set("ai_vector_search_enabled", "false")

    # Two-agent cascade toggle + paperless writeback toggle. Both
    # ride the AI form, so we always write them on submit (no need
    # for the openrouter_model presence guard the legacy handler used
    # to gate against cross-form submissions).
    if ai_fallback_enabled is not None:
        store.set("ai_fallback_enabled", "1")
    else:
        store.set("ai_fallback_enabled", "false")
    if paperless_writeback_enabled is not None:
        store.set("paperless_writeback_enabled", "1")
    else:
        store.set("paperless_writeback_enabled", "false")

    if ai_fallback_confidence_threshold is not None:
        v = ai_fallback_confidence_threshold.strip()
        if v:
            try:
                float(v)
            except ValueError:
                pass
            else:
                store.set("ai_fallback_confidence_threshold", v)

    _finalize(request, settings, store)
    return RedirectResponse("/settings/ai?saved=1", status_code=303)


@router.post("/settings/notifications")
def settings_notifications_save(
    request: Request,
    ntfy_base_url: str | None = Form(default=None),
    ntfy_topic: str | None = Form(default=None),
    ntfy_token: str | None = Form(default=None),
    pushover_user_key: str | None = Form(default=None),
    pushover_api_token: str | None = Form(default=None),
    mileage_rate: str | None = Form(default=None),
    notify_min_fixme_usd: str | None = Form(default=None),
    notify_digest_day: str | None = Form(default=None),
    settings: Settings = Depends(get_settings),
    store: AppSettingsStore = Depends(get_app_settings_store),
):
    """ADR-0047 — ntfy + Pushover channel creds and digest thresholds.
    Two forms post here (channels + thresholds); each leaves the other
    half's fields as None and the per-key None-checks skip them."""
    if ntfy_base_url is not None:
        v = ntfy_base_url.strip()
        if v:
            store.set("ntfy_base_url", v)
    if ntfy_topic is not None:
        v = ntfy_topic.strip()
        if v:
            store.set("ntfy_topic", v)
        else:
            store.delete("ntfy_topic")
    if ntfy_token:
        store.set("ntfy_token", ntfy_token)
    if pushover_user_key:
        store.set("pushover_user_key", pushover_user_key)
    if pushover_api_token:
        store.set("pushover_api_token", pushover_api_token)

    if mileage_rate is not None:
        v = mileage_rate.strip()
        if v:
            try:
                float(v)
            except ValueError:
                pass
            else:
                store.set("mileage_rate", v)

    if notify_min_fixme_usd is not None:
        v = notify_min_fixme_usd.strip()
        if v:
            try:
                float(v)
            except ValueError:
                pass
            else:
                store.set("notify_min_fixme_usd", v)
    if notify_digest_day is not None:
        v = notify_digest_day.strip().capitalize()
        if v in WEEKDAYS:
            store.set("notify_digest_day", v)

    _finalize(request, settings, store)
    return RedirectResponse("/settings/notifications?saved=1", status_code=303)


@router.post("/settings/reports")
def settings_reports_save(
    request: Request,
    audit_max_receipt_bytes: str | None = Form(default=None),
    estimated_tax_flat_rate: str | None = Form(default=None),
    recurring_scan_window_days: str | None = Form(default=None),
    recurring_min_occurrences: str | None = Form(default=None),
    budget_alert_channels: str | None = Form(default=None),
    import_retention_days: str | None = Form(default=None),
    import_max_upload_bytes: str | None = Form(default=None),
    import_ai_column_map_model: str | None = Form(default=None),
    import_ai_confidence_threshold: str | None = Form(default=None),
    settings: Settings = Depends(get_settings),
    store: AppSettingsStore = Depends(get_app_settings_store),
):
    """ADR-0047 — reports / budgets / recurring scan / spreadsheet
    import tuning."""
    # Phase 6 — reports + budgets + recurring.
    for key, raw in (
        ("audit_max_receipt_bytes", audit_max_receipt_bytes),
        ("recurring_scan_window_days", recurring_scan_window_days),
        ("recurring_min_occurrences", recurring_min_occurrences),
    ):
        if raw is not None:
            v = raw.strip()
            if v:
                try:
                    int(v)
                except ValueError:
                    continue
                store.set(key, v)
    if estimated_tax_flat_rate is not None:
        v = estimated_tax_flat_rate.strip()
        if v:
            try:
                float(v)
            except ValueError:
                pass
            else:
                store.set("estimated_tax_flat_rate", v)
    if budget_alert_channels is not None:
        store.set("budget_alert_channels", budget_alert_channels.strip())

    # Phase 7 — spreadsheet import.
    for key, raw in (
        ("import_retention_days", import_retention_days),
        ("import_max_upload_bytes", import_max_upload_bytes),
    ):
        if raw is not None:
            v = raw.strip()
            if v:
                try:
                    int(v)
                except ValueError:
                    continue
                store.set(key, v)
    if import_ai_column_map_model is not None:
        v = import_ai_column_map_model.strip()
        if v:
            store.set("import_ai_column_map_model", v)
        else:
            store.delete("import_ai_column_map_model")
    if import_ai_confidence_threshold is not None:
        v = import_ai_confidence_threshold.strip()
        if v:
            try:
                float(v)
            except ValueError:
                pass
            else:
                store.set("import_ai_confidence_threshold", v)

    _finalize(request, settings, store)
    return RedirectResponse("/settings/reports?saved=1", status_code=303)


@router.post("/settings")
def update_settings_legacy_redirect(request: Request):
    """ADR-0047 — POST /settings is no longer the universal write
    endpoint; the four per-area POSTs at /settings/{paperless,ai,
    notifications,reports} replaced it. A stale browser tab POSTing
    here gets a 303 back to the dashboard (303 forces GET on the
    follow-up; a 308 would re-POST and infinite-loop on this same
    handler)."""
    return RedirectResponse("/settings", status_code=303)

# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Routes that act on payout-source suggestion cards.

The cards themselves are built by ``lamella.features.review_queue.suggestions``
and rendered by ``_components/cards.html::suggestion_card``. The
two endpoints here turn an accepted suggestion into ledger writes
or persist the user's "not a payout source" dismissal.

Scaffold (POST /settings/payout-sources/scaffold):
  1. Open the suggested account (e.g. ``Assets:Acme:eBay``) via
     ``AccountsWriter.write_opens``.
  2. Stamp ``custom "account-kind" "Assets:Acme:eBay" "payout"``
     into ``connector_config.bean`` so the rest of the system
     treats the account as a payout source.
  3. Append a ``custom "classification-rule"`` mapping
     ``merchant_contains:<pattern>`` → the new account, so future
     payouts route there as transfers without prompting.

Each write follows the snapshot / bean-check / rollback
discipline already baked into the connector-owned-file writers.
On failure the route bails before subsequent writes so a
half-scaffolded account never lands in the ledger.

Dismiss (POST /settings/payout-sources/dismiss):
  Writes a ``custom "payout-source-dismissed"`` directive to
  ``connector_rules.bean`` so the suggestion stays gone across
  restarts and reconstructs.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from lamella.core.beancount_io import LedgerReader
from lamella.core.config import Settings
from lamella.web.deps import get_db, get_ledger_reader, get_settings
from lamella.core.ledger_writer import BeanCheckError
from lamella.core.registry.accounts_writer import AccountsWriter
from lamella.core.registry.kind_writer import append_account_kind
from lamella.web.routes import _htmx
from lamella.web.routes._htmx import is_htmx
from lamella.features.rules.rule_writer import CONNECTOR_RULES_HEADER, append_rule
from lamella.core.transform.custom_directive import append_custom_directive

log = logging.getLogger(__name__)

router = APIRouter()


def _done_partial(
    request: Request,
    *,
    tone: str = "ok",
    title: str,
    body: str = "",
    undo_label: str | None = None,
    undo_action: str | None = None,
    undo_form_data: dict | None = None,
) -> HTMLResponse:
    """Render the in-place "done" replacement for a suggestion card."""
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "partials/_suggestion_card_done.html",
        {
            "tone": tone,
            "title": title,
            "body": body,
            "undo_label": undo_label,
            "undo_action": undo_action,
            "undo_form_data": undo_form_data or {},
        },
    )


def _scaffold_error(
    request: Request,
    next_path: str,
    *,
    stage: str,
    failure: str,
    detail: str,
    suggested_path: str,
):
    """Surface a scaffold-flow failure in-place for HTMX clients
    (replaces the suggestion card with a tone='err' done-partial
    that includes the actual exception text), falling back to a
    redirect with a generic message code for vanilla form posts.

    Without this helper, every error path silently redirected the
    HTMX action to next_path with a query-param message that the
    destination page didn't render — the user saw "you ended up
    on the dashboard with nothing done" and zero diagnostic info.

    `stage`: which step of the 3-step scaffold flow failed
        (open / kind / rule).
    `failure`: "check" (bean-check rejection) or "error" (other
        exception). Drives the message-code fallback for the
        non-HTMX redirect.
    `detail`: the actual exception text (already truncated by
        the caller). Rendered verbatim in the in-place error tile.
    """
    if is_htmx(request):
        stage_label = {
            "open": "opening the account",
            "kind": "stamping the account kind",
            "rule": "writing the routing rule",
        }.get(stage, "scaffolding")
        title = f"Scaffold failed while {stage_label}"
        body = (
            f"Tried to scaffold {suggested_path}. The bean-check "
            f"rejected the change — see detail below. Nothing was "
            f"left half-done in the ledger."
            if failure == "check"
            else
            f"Tried to scaffold {suggested_path}. An unexpected "
            f"error occurred — see detail below."
        )
        return _done_partial(
            request,
            tone="err",
            title=title,
            body=f"{body}\n\n{detail}",
        )
    code = (
        "payout_scaffold_check_failed"
        if failure == "check" else "payout_scaffold_failed"
    )
    return _redirect(request, next_path, code)


def _safe_redirect_back(request: Request, default: str = "/") -> str:
    """Pick a redirect target that keeps the user where they were.

    Honors an explicit ``next`` form field; falls back to the
    referer; finally to ``default``. All values must start with
    ``/`` — never honor an absolute URL from the client.
    """
    return default


@router.post("/settings/payout-sources/scaffold")
def scaffold_payout_source(
    request: Request,
    pattern_id: str = Form(...),
    entity: str = Form(...),
    leaf: str = Form(...),
    suggested_path: str = Form(...),
    receiving_account: str = Form(default=""),
    next_path: str = Form(default=""),
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Accept a payout-source suggestion: open the account, stamp
    its kind, and write the merchant-routing rule.

    The form data comes from the suggestion card's hidden fields,
    so ``suggested_path`` is the authoritative target — we don't
    re-derive it from ``entity`` + ``leaf`` to avoid a drift
    between the displayed CTA and what actually lands.
    """
    # Sanity: the path the card suggested must be entity-first
    # Assets and start with the entity we were given. Reject
    # anything else with a redirect-banner so a tampered POST
    # can't open arbitrary accounts.
    if not suggested_path.startswith(f"Assets:{entity}:"):
        log.warning(
            "payout-source scaffold rejected: path %r doesn't match entity %r",
            suggested_path, entity,
        )
        if is_htmx(request):
            return _done_partial(
                request, tone="err",
                title="Scaffold rejected",
                body=(
                    f"Path {suggested_path!r} doesn't match the expected "
                    f"shape Assets:{entity}:… — refusing to open."
                ),
            )
        return _redirect(request, next_path, "payout_scaffold_invalid")

    main_bean = settings.ledger_main
    accounts_path = settings.connector_accounts_path
    config_path = settings.connector_config_path
    rules_path = settings.connector_rules_path

    # 1. Open. Snapshot existing-paths first so write_opens skips
    #    when an earlier scaffold already opened the account.
    try:
        existing_paths = {
            e.account for e in reader.entries
            if hasattr(e, "account")
        }
    except Exception:  # noqa: BLE001 — ledger read failure shouldn't block
        existing_paths = set()

    writer = AccountsWriter(
        main_bean=main_bean,
        connector_accounts=accounts_path,
    )
    try:
        writer.write_opens(
            paths=[suggested_path],
            opened_on=date.today(),
            comment=(
                f"Auto-scaffolded by Lamella: detected payout source "
                f"({pattern_id})."
            ),
            existing_paths=existing_paths,
        )
    except BeanCheckError as exc:
        log.error("payout-source open failed bean-check: %s", exc)
        return _scaffold_error(
            request, next_path,
            stage="open",
            failure="check",
            detail=str(exc)[:600],
            suggested_path=suggested_path,
        )
    except Exception as exc:  # noqa: BLE001
        log.exception("payout-source open failed: %s", exc)
        return _scaffold_error(
            request, next_path,
            stage="open",
            failure="error",
            detail=f"{type(exc).__name__}: {exc}"[:600],
            suggested_path=suggested_path,
        )

    # 2. Account-kind override → 'payout'.
    try:
        append_account_kind(
            connector_config=config_path,
            main_bean=main_bean,
            account_path=suggested_path,
            kind="payout",
        )
    except BeanCheckError as exc:
        log.error("payout-source kind append failed bean-check: %s", exc)
        return _scaffold_error(
            request, next_path,
            stage="kind",
            failure="check",
            detail=str(exc)[:600],
            suggested_path=suggested_path,
        )
    except Exception as exc:  # noqa: BLE001
        log.exception("payout-source kind append failed: %s", exc)
        return _scaffold_error(
            request, next_path,
            stage="kind",
            failure="error",
            detail=f"{type(exc).__name__}: {exc}"[:600],
            suggested_path=suggested_path,
        )

    # Update accounts_meta.kind in SQLite so the rest of the in-
    # process system sees the new kind without waiting for a
    # ledger reread.
    try:
        conn.execute(
            "UPDATE accounts_meta SET kind = ?, kind_source = 'override' "
            " WHERE account_path = ?",
            ("payout", suggested_path),
        )
        # If the account isn't in accounts_meta yet (the writer just
        # opened it; the registry sync hasn't run), insert a stub
        # row so subsequent reads see it. The next discovery pass
        # will fill in the rest.
        if conn.total_changes == 0:
            if entity:
                conn.execute(
                    "INSERT OR IGNORE INTO entities (slug, display_name) "
                    "VALUES (?, ?)",
                    (entity, entity),
                )
            conn.execute(
                "INSERT OR IGNORE INTO accounts_meta "
                "    (account_path, display_name, entity_slug, "
                "     kind, kind_source, is_active, seeded_from_ledger) "
                " VALUES (?, ?, ?, 'payout', 'override', 1, 1)",
                (suggested_path, leaf, entity or None),
            )
        conn.commit()
    except Exception as exc:  # noqa: BLE001 — SQLite drift isn't fatal
        log.warning("accounts_meta update for %s failed: %s",
                    suggested_path, exc)

    # 3. Classification rule routing matching merchants to the new
    #    account. The pattern value matches the brand pattern's id
    #    (lowercased substring) — same shape the rule engine
    #    consumes.
    try:
        append_rule(
            connector_rules=rules_path,
            main_bean=main_bean,
            pattern_type="merchant_contains",
            pattern_value=pattern_id.replace("_", " ")
                              if pattern_id == "amazon_seller"
                              else pattern_id,
            target_account=suggested_path,
            created_by="payout-detector",
        )
    except BeanCheckError as exc:
        log.error("payout-source rule append failed bean-check: %s", exc)
        return _scaffold_error(
            request, next_path,
            stage="rule",
            failure="check",
            detail=str(exc)[:600],
            suggested_path=suggested_path,
        )
    except Exception as exc:  # noqa: BLE001
        log.exception("payout-source rule append failed: %s", exc)
        return _scaffold_error(
            request, next_path,
            stage="rule",
            failure="error",
            detail=f"{type(exc).__name__}: {exc}"[:600],
            suggested_path=suggested_path,
        )

    # 4. Re-evaluate pending staged rows so the user sees the
    #    payout-source target on /review immediately, instead of
    #    waiting for the next ingest sweep to apply the new rule.
    #    Only updates the proposal — actual ledger writes stay
    #    user-driven via the existing Accept-Proposed flow.
    try:
        from lamella.features.bank_sync.payout_sources import (
            reclassify_pending_rows_for_pattern,
        )
        touched = reclassify_pending_rows_for_pattern(
            conn,
            pattern_id=pattern_id,
            entity=entity,
            target_account=suggested_path,
        )
    except Exception as exc:  # noqa: BLE001 — non-fatal
        log.warning("payout-source reclassify-pending failed: %s", exc)
        touched = 0

    log.info(
        "payout-source scaffolded: %s (pattern=%s, entity=%s, "
        "reclassified=%d pending row%s)",
        suggested_path, pattern_id, entity,
        touched, "" if touched == 1 else "s",
    )
    if is_htmx(request):
        body_msg = f"Opened {suggested_path} and routed future "
        body_msg += f"{pattern_id} payouts there."
        if touched:
            body_msg += (
                f" Re-classified {touched} pending row"
                f"{'' if touched == 1 else 's'}."
            )
        return _done_partial(
            request,
            tone="ok",
            title=f"Scaffolded {suggested_path}",
            body=body_msg,
        )
    msg = "payout_scaffold_ok"
    if touched:
        msg = f"payout_scaffold_ok_{touched}"
    return _redirect(request, next_path, msg)


@router.post("/settings/payout-sources/dismiss")
def dismiss_payout_source(
    request: Request,
    pattern_id: str = Form(...),
    entity: str = Form(...),
    next_path: str = Form(default=""),
    settings: Settings = Depends(get_settings),
):
    """Record "user said this isn't a payout source" so the
    suggestion stops re-firing on every page load.

    Persisted as a ``custom "payout-source-dismissed"`` directive
    in ``connector_rules.bean`` (sibling of recurring-ignored
    and the rest of the rule-layer dismissals). The dismissal is
    keyed on ``(pattern_id, entity)`` so dismissing eBay for one
    entity doesn't suppress the suggestion for another.
    """
    main_bean = settings.ledger_main
    rules_path = settings.connector_rules_path
    ts = datetime.now(timezone.utc).replace(tzinfo=None)
    try:
        append_custom_directive(
            target=rules_path,
            main_bean=main_bean,
            header=CONNECTOR_RULES_HEADER,
            directive_date=ts.date(),
            directive_type="payout-source-dismissed",
            args=[f"{pattern_id}:{entity}"],
            meta={
                "lamella-pattern-id": pattern_id,
                "lamella-entity": entity,
                "lamella-dismissed-at": ts.isoformat(),
            },
        )
    except BeanCheckError as exc:
        log.error("payout-source dismiss bean-check failed: %s", exc)
        return _redirect(request, next_path, "payout_dismiss_check_failed")
    except Exception as exc:  # noqa: BLE001
        log.exception("payout-source dismiss failed: %s", exc)
        return _redirect(request, next_path, "payout_dismiss_failed")
    log.info(
        "payout-source dismissed: pattern=%s entity=%s", pattern_id, entity,
    )
    if is_htmx(request):
        return _done_partial(
            request,
            tone="info",
            title="Dismissed",
            body=(
                f"This payout-source suggestion ({pattern_id}) is hidden for "
                f"{entity}. Re-run the detector from /settings to bring it "
                f"back."
            ),
        )
    return _redirect(request, next_path, "payout_dismiss_ok")


def _redirect(request: Request, next_path: str, message: str):
    """Redirect back to the page the user was on with a message hint.

    HTMX-safe: for HTMX requests returns 204 + HX-Redirect (client-side
    nav) so the shim doesn't auto-follow into a full-page swap that
    would dump the destination's layout into the action's hx-target.
    See ADR-0037 + routes/CLAUDE.md. For vanilla form submits returns
    a regular 303."""
    target = next_path or "/"
    if not target.startswith("/"):
        target = "/"
    sep = "&" if "?" in target else "?"
    return _htmx.redirect(
        request, f"{target}{sep}msg={message}",
    )

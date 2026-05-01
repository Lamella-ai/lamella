# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Edit plain-English descriptions for Expense/Income accounts.

Complements /settings/accounts (which covers Assets/Liabilities
via accounts_meta). Backed by the account_classify_context table
— the AI sees whatever you write here alongside the account name
in the classify prompt, making narrow or newly-created accounts
immediately useful without needing transaction history.
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from lamella.core.beancount_io import LedgerReader
from lamella.core.config import Settings
from lamella.web.deps import get_db, get_ledger_reader, get_settings

log = logging.getLogger(__name__)

router = APIRouter()


@dataclass
class AccountDescRow:
    account_path: str
    entity: str | None
    description: str = ""
    txn_count: int = 0
    has_description: bool = False


def _load_expense_income_accounts(entries) -> dict[str, int]:
    """Every Expenses:* or Income:* account that has either an
    Open directive OR at least one posting. Returned as
    {account_path: txn_count}."""
    from beancount.core import data as bdata
    counts: dict[str, int] = {}
    for e in entries:
        if isinstance(e, bdata.Open):
            if e.account.startswith(("Expenses:", "Income:")):
                counts.setdefault(e.account, 0)
        elif isinstance(e, bdata.Transaction):
            for p in e.postings or []:
                acct = p.account or ""
                if acct.startswith(("Expenses:", "Income:")):
                    counts[acct] = counts.get(acct, 0) + 1
    return counts


def _load_descriptions(conn) -> dict[str, str]:
    try:
        rows = conn.execute(
            "SELECT account_path, description "
            "FROM account_classify_context"
        ).fetchall()
    except sqlite3.Error:
        return {}
    return {r["account_path"]: (r["description"] or "") for r in rows}


def _entity_from_path(account: str) -> str | None:
    """Entity-first ordering: Expenses:<Entity>:… where <Entity> is
    the second segment. Personal / Acme / etc."""
    parts = account.split(":")
    if len(parts) >= 2 and parts[1] not in {"FIXME", "UNKNOWN", "UNCATEGORIZED"}:
        return parts[1]
    return None


@router.get("/settings/account-descriptions", response_class=HTMLResponse)
def descriptions_page(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    entity: str = "",
    only_missing: str = "",
):
    """List every Expense/Income account with its current
    description (if any) and an inline editor. Optional filters:
    `entity` (Acme, Personal, etc.) and `only_missing=1`
    (accounts with no description yet)."""
    try:
        entries = reader.load().entries
    except Exception as exc:  # noqa: BLE001
        entries = []
        log.warning("account-descriptions: ledger load failed: %s", exc)

    counts = _load_expense_income_accounts(entries)
    descriptions = _load_descriptions(conn)

    rows: list[AccountDescRow] = []
    for account_path in sorted(counts.keys()):
        entity_slug = _entity_from_path(account_path)
        desc = descriptions.get(account_path, "")
        rows.append(AccountDescRow(
            account_path=account_path,
            entity=entity_slug,
            description=desc,
            txn_count=counts[account_path],
            has_description=bool(desc.strip()),
        ))

    # Filters.
    if entity:
        rows = [r for r in rows if r.entity == entity]
    if only_missing:
        rows = [r for r in rows if not r.has_description]

    entities_seen = sorted({r.entity for r in rows if r.entity})
    total_accounts = len(counts)
    described = sum(1 for d in descriptions.values() if d.strip())

    return request.app.state.templates.TemplateResponse(
        request, "account_descriptions.html",
        {
            "rows": rows,
            "entity": entity,
            "only_missing": bool(only_missing),
            "entities_seen": entities_seen,
            "total_accounts": total_accounts,
            "described": described,
            "saved": request.query_params.get("saved"),
        },
    )


@router.post("/settings/account-descriptions/generate", response_class=HTMLResponse)
def generate_description(
    request: Request,
    account_path: str = Form(...),
):
    """Work-backwards AI draft for a specific account. Runs as a
    background job so the user sees live progress (fetching history
    → calling AI → building draft) instead of a silent spinner."""
    import asyncio
    import html as _html
    path = account_path.strip()
    if not path:
        return HTMLResponse("account_path required", status_code=400)
    settings = request.app.state.settings
    if not settings.openrouter_api_key:
        return HTMLResponse(
            '<p class="muted">AI is disabled — set OPENROUTER_API_KEY to enable.</p>',
        )

    def _work(ctx):
        from lamella.features.ai_cascade.draft_description import (
            generate_account_description,
        )
        from lamella.features.ai_cascade.service import AIService
        ai = AIService(settings=settings, conn=request.app.state.db)
        ctx.emit(f"Loading ledger history for {path} …", outcome="info")
        reader = request.app.state.ledger_reader
        entries = reader.load().entries
        ctx.emit(
            "Calling AI to draft description (this usually takes 10–30s) …",
            outcome="info",
        )
        loop = asyncio.new_event_loop()
        try:
            draft = loop.run_until_complete(
                generate_account_description(
                    ai=ai, entries=entries, account_path=path,
                )
            )
        finally:
            loop.close()
        if draft is None:
            ctx.emit(
                "No ledger activity found — draft needs txns to work with.",
                outcome="not_found",
            )
            return {
                "terminal_html": (
                    '<p class="muted">No ledger activity found for this '
                    'account — draft needs txns to work with.</p>'
                ),
            }
        ctx.emit(
            f"Draft ready (confidence {draft.confidence:.2f}).",
            outcome="success",
        )
        surprises_html = ""
        if draft.surprises:
            surprises_html = (
                '<p class="muted small" style="margin-top:0.3rem;">'
                '<strong>Surprises:</strong></p><ul class="small">'
                + "".join(f"<li>{_html.escape(s)}</li>" for s in draft.surprises)
                + "</ul>"
            )
        html_out = (
            f'<div class="ai-draft-result" '
            f'style="padding:0.5rem;background:#fff3cd;border-radius:4px;">'
            f'<p class="muted small"><strong>Drafted (conf '
            f'{draft.confidence:.2f}).</strong> '
            f'Copy the text below + paste into the textarea for <code>'
            f'{_html.escape(path)}</code>.</p>'
            f'<textarea rows="4" style="width:100%;font-family:inherit;" '
            f'readonly>{_html.escape(draft.description)}</textarea>'
            f'{surprises_html}</div>'
        )
        return {"terminal_html": html_out}

    runner = request.app.state.job_runner
    job_id = runner.submit(
        kind="ai-account-description",
        title=f"Drafting description for {path}",
        fn=_work,
    )
    return request.app.state.templates.TemplateResponse(
        request, "partials/_job_modal.html",
        {"job_id": job_id, "on_close_url": "/settings/account-descriptions"},
    )


@router.post("/settings/account-descriptions/mine", response_class=HTMLResponse)
def mine_subcategories(
    request: Request,
    account_path: str = Form(...),
):
    """Run the emergent subcategory miner on a given account. Runs
    as a background job so the user sees per-phase progress (load →
    cluster → AI propose) instead of a blank spinner."""
    import asyncio
    import html as _html
    path = account_path.strip()
    if not path:
        return HTMLResponse("account_path required", status_code=400)
    settings = request.app.state.settings
    if not settings.openrouter_api_key:
        return HTMLResponse(
            '<p class="muted">AI is disabled — set OPENROUTER_API_KEY to enable mining.</p>',
        )

    def _work(ctx):
        from lamella.features.ai_cascade.service import AIService
        from lamella.features.ai_cascade.subcategory_miner import (
            build_miner_input,
            propose_subcategories,
        )
        ai = AIService(settings=settings, conn=request.app.state.db)
        ctx.emit(f"Loading ledger history for {path} …", outcome="info")
        reader = request.app.state.ledger_reader
        entries = reader.load().entries
        entity = _entity_from_path(path)
        miner_input = build_miner_input(
            entries, account_path=path, entity_slug=entity,
        )
        if miner_input.total_txns == 0:
            ctx.emit(
                "No transactions found in this account — nothing to mine.",
                outcome="not_found",
            )
            return {
                "terminal_html": (
                    '<p class="muted">No transactions found in this '
                    'account — nothing to mine.</p>'
                ),
            }
        ctx.emit(
            f"Clustering {miner_input.total_txns} txn(s) — asking AI to propose "
            f"sub-categories (usually 10–30s) …",
            outcome="info",
        )
        loop = asyncio.new_event_loop()
        try:
            proposal = loop.run_until_complete(
                propose_subcategories(ai=ai, miner_input=miner_input)
            )
        finally:
            loop.close()
        if proposal is None:
            ctx.emit("AI call failed or was skipped.", outcome="error")
            return {
                "terminal_html": '<p class="muted">AI call failed or was skipped.</p>',
            }
        if not proposal.clusters:
            ctx.emit("No useful clusters found.", outcome="not_found")
            return {
                "terminal_html": (
                    f'<div class="muted" '
                    f'style="padding:0.5rem;background:#e2e3e5;border-radius:4px;">'
                    f'<strong>No useful clusters found.</strong> '
                    f'{_html.escape(proposal.reasoning or "")}</div>'
                ),
            }
        ctx.emit(
            f"Proposed {len(proposal.clusters)} sub-category cluster(s).",
            outcome="success",
        )
        parts = [
            '<div class="miner-result" '
            'style="padding:0.75rem;background:#fff3cd;border-radius:4px;">'
            '<p class="muted small"><strong>Proposed sub-categories.</strong> '
            'Review, create the ones you like via the Create buttons below, '
            "then use the audit page's 'Focus on account' to scan the parent "
            'for candidates to move into the new children.</p>'
        ]
        for c in proposal.clusters:
            parts.append(
                f'<section style="border-top:1px solid #ccc;margin-top:0.5rem;padding-top:0.5rem;">'
                f'<h4 style="margin:0;"><code>{_html.escape(path)}:{_html.escape(c.proposed_leaf)}</code></h4>'
                f'<p class="muted small" style="margin:0.2rem 0;">{_html.escape(c.rationale)}</p>'
                f'<p class="small">~{c.estimated_txn_count} txns · example merchants: '
                f'{_html.escape(", ".join(c.example_merchants))}</p>'
                f'<form method="post" action="/settings/accounts/add-subcategory" '
                f'      style="display:inline;">'
                f'<input type="hidden" name="parent" value="{_html.escape(path)}" />'
                f'<input type="hidden" name="leaf" value="{_html.escape(c.proposed_leaf)}" />'
                f'<input type="hidden" name="redirect_to" value="/settings/account-descriptions" />'
                f'<button type="submit" class="small primary-action">Create {_html.escape(c.proposed_leaf)}</button>'
                f'</form>'
                f'</section>'
            )
        if proposal.unclassifiable:
            parts.append(
                f'<p class="muted small" style="margin-top:0.5rem;">'
                f'<strong>Left unclassified:</strong> '
                f'{_html.escape(", ".join(proposal.unclassifiable[:10]))}'
                f'{"…" if len(proposal.unclassifiable) > 10 else ""}</p>'
            )
        parts.append('</div>')
        return {"terminal_html": "".join(parts)}

    runner = request.app.state.job_runner
    job_id = runner.submit(
        kind="ai-mine-subcategories",
        title=f"Mining sub-categories for {path}",
        fn=_work,
    )
    return request.app.state.templates.TemplateResponse(
        request, "partials/_job_modal.html",
        {"job_id": job_id, "on_close_url": "/settings/account-descriptions"},
    )


@router.post("/settings/account-descriptions/save", response_class=HTMLResponse)
def save_description(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    settings: Settings = Depends(get_settings),
    account_path: str = Form(...),
    description: str = Form(""),
):
    """Upsert a description for a single account."""
    path = account_path.strip()
    desc = description.strip()
    if not path:
        return HTMLResponse("account_path required", status_code=400)
    if desc:
        conn.execute(
            """
            INSERT INTO account_classify_context
                (account_path, description, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(account_path) DO UPDATE SET
                description = excluded.description,
                updated_at = CURRENT_TIMESTAMP
            """,
            (path, desc),
        )
        try:
            from lamella.core.transform.steps.step14_classify_context import (
                append_account_description,
            )
            append_account_description(
                connector_config=settings.connector_config_path,
                main_bean=settings.ledger_main,
                account_path=path,
                description=desc,
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("account-description directive write failed for %s: %s", path, exc)
    else:
        # Empty description = clear the row.
        conn.execute(
            "DELETE FROM account_classify_context WHERE account_path = ?",
            (path,),
        )
    if "hx-request" in {k.lower() for k in request.headers.keys()}:
        return HTMLResponse(
            f'<div class="saved-marker">✓ saved'
            f'{" (cleared)" if not desc else ""}</div>',
        )
    return RedirectResponse(
        f"/settings/account-descriptions?saved={path}", status_code=303,
    )

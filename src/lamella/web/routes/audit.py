# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Classification audit UI.

GET /audit — shows open audit items (disagreements not yet
decided on), a recent-runs list, and a Run button.

POST /audit/run — spawns a new audit pass. Form: sample_mode
(random | recent | high_dollar), sample_size (default 20).

POST /audit/items/{id}/accept — writes an override moving the
txn to the AI's proposed account, marks the decision as
`user_corrected` on ai_decisions, closes the audit item.

POST /audit/items/{id}/dismiss — records an audit_dismissal
(silencing the merchant/current-account pair from future
audits), closes the audit item.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import date as date_cls
from decimal import Decimal

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from beancount.core.data import Transaction

from lamella.features.ai_cascade.audit import AuditRunner
from lamella.features.ai_cascade.service import AIService
from lamella.core.beancount_io import LedgerReader
from lamella.core.beancount_io.txn_hash import txn_hash
from lamella.core.config import Settings
from lamella.web.deps import (
    get_ai_service,
    get_db,
    get_ledger_reader,
    get_settings,
)
from lamella.core.ledger_writer import BeanCheckError
from lamella.features.rules.overrides import OverrideWriter

log = logging.getLogger(__name__)

router = APIRouter()


def _scan_synthetic_legs(reader: LedgerReader) -> dict:
    """ADR-0046 Phase 4 — enumerate every synthetic counterpart leg in
    the ledger. Phase 4b adds per-leg detail so /audit can render
    Promote / Demote buttons on each row.

    Returns::

        {
          "total":         <count of replaceable legs (back-compat)>,
          "by_account":    [(account, count), ...]  # replaceable legs only
          "replaceable":   [<leg dict>, ...],       # for Promote
          "confirmed":     [<leg dict>, ...],       # for Demote
        }

    Each leg dict carries lamella_txn_id, posting_account, date,
    amount, currency, payee, replaceable. ``replaceable=True`` rows
    appear in the ``replaceable`` list; rows the user previously
    promoted appear in ``confirmed`` so a misfire is recoverable
    (Phase 4b)."""
    from collections import defaultdict
    counts: dict[str, int] = defaultdict(int)
    replaceable_legs: list[dict] = []
    confirmed_legs: list[dict] = []
    try:
        entries = reader.load().entries
    except Exception:  # noqa: BLE001
        return {
            "total": 0,
            "by_account": [],
            "replaceable": [],
            "confirmed": [],
        }
    from lamella.core.identity import get_txn_id as _get_txn_id
    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        lamella_txn_id = _get_txn_id(entry)
        for posting in entry.postings:
            meta = getattr(posting, "meta", None) or {}
            replaceable = meta.get("lamella-synthetic-replaceable")
            # Only emit a row when the posting carries the synthetic
            # marker at all — replaceable IS the discriminator.
            if replaceable is None:
                continue
            is_replaceable = replaceable is True or (
                isinstance(replaceable, str)
                and replaceable.strip().upper() == "TRUE"
            )
            is_confirmed = replaceable is False or (
                isinstance(replaceable, str)
                and replaceable.strip().upper() == "FALSE"
            )
            # Skip rows where we couldn't parse the marker either way
            # (defensive — the writer always emits one of the two).
            if not (is_replaceable or is_confirmed):
                continue
            amount = None
            currency = "USD"
            if posting.units and posting.units.number is not None:
                amount = Decimal(posting.units.number)
                currency = posting.units.currency or "USD"
            leg = {
                "lamella_txn_id": str(lamella_txn_id) if lamella_txn_id else None,
                "posting_account": posting.account,
                "date": entry.date.isoformat() if entry.date else None,
                "amount": amount,
                "currency": currency,
                "payee": getattr(entry, "payee", None) or entry.narration or "",
                "narration": entry.narration or "",
                "replaceable": is_replaceable,
            }
            if is_replaceable:
                replaceable_legs.append(leg)
                counts[posting.account] += 1
            else:
                confirmed_legs.append(leg)
    return {
        "total": len(replaceable_legs),
        "by_account": sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])),
        "replaceable": replaceable_legs,
        "confirmed": confirmed_legs,
    }


@router.get("/audit", response_class=HTMLResponse)
def audit_page(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    target_account: str = "",
):
    """Main audit view: open items (disagreements awaiting
    user decision) + recent-runs stats."""
    items = conn.execute(
        """
        SELECT id, txn_hash, txn_date, txn_amount, merchant_text,
               current_account, ai_proposed_account, ai_confidence,
               ai_reasoning
          FROM audit_items
         WHERE status = 'open'
         ORDER BY ai_confidence DESC, id DESC
         LIMIT 100
        """
    ).fetchall()
    # Most-recent run — if the user just kicked a pass off,
    # show the result summary ("sampled 20, 17 agreed with
    # current, 3 surfaced as disagreements") so they know what
    # happened.
    last_run = conn.execute(
        """
        SELECT id, started_at, finished_at, sampled, classified,
               disagreements, errors, sample_mode, sample_size, notes
          FROM audit_runs
         ORDER BY id DESC LIMIT 1
        """
    ).fetchone()
    runs = conn.execute(
        """
        SELECT id, started_at, finished_at, sampled, classified,
               disagreements, errors, sample_mode, notes
          FROM audit_runs
         ORDER BY id DESC
         LIMIT 10
        """
    ).fetchall()
    totals = {
        "open": conn.execute(
            "SELECT COUNT(*) AS n FROM audit_items WHERE status = 'open'"
        ).fetchone()["n"],
        "accepted": conn.execute(
            "SELECT COUNT(*) AS n FROM audit_items WHERE status = 'accepted'"
        ).fetchone()["n"],
        "dismissed": conn.execute(
            "SELECT COUNT(*) AS n FROM audit_items WHERE status = 'dismissed'"
        ).fetchone()["n"],
    }
    synthetic = _scan_synthetic_legs(reader)
    return request.app.state.templates.TemplateResponse(
        request, "audit.html",
        {
            "items": [dict(r) for r in items],
            "runs": [dict(r) for r in runs],
            "totals": totals,
            "target_account_prefill": target_account,
            "last_run": dict(last_run) if last_run else None,
            "synthetic": synthetic,
        },
    )


@router.post("/audit/run", response_class=HTMLResponse)
async def audit_run(
    request: Request,
    ai: AIService = Depends(get_ai_service),
    reader: LedgerReader = Depends(get_ledger_reader),
    conn: sqlite3.Connection = Depends(get_db),
    sample_mode: str = Form("random"),
    sample_size: int = Form(20),
    target_account: str = Form(""),
):
    """Kick off an audit pass. Each sample AI call takes ~30-60s, so
    a 20-item pass used to block for 10+ minutes with no feedback.
    Now runs as a background job — route returns the progress modal
    immediately, worker streams per-sample events (Agree / Disagree /
    Skipped / Error) and the user sees live progress."""
    import asyncio

    if sample_mode not in {"random", "recent", "high_dollar"}:
        sample_mode = "random"
    sample_size = max(1, min(int(sample_size or 20), 200))
    target = (target_account or "").strip() or None

    def _work(ctx):
        total_set = {"done": False}

        def _on_progress(idx, total, txn, merchant, outcome):
            ctx.raise_if_cancelled()
            if total and not total_set["done"]:
                ctx.set_total(total)
                total_set["done"] = True
            label = (merchant or (txn.narration or "")).strip()[:60]
            if outcome == "disagree":
                ctx.emit(f"Disagreement: {label}", outcome="failure")
            elif outcome == "agree":
                ctx.emit(f"Agreed: {label}", outcome="success")
            elif outcome == "skipped":
                ctx.emit(f"Skipped (dismissed): {label}", outcome="info")
            elif outcome == "error":
                ctx.emit(f"Error on: {label}", outcome="error")
            elif outcome == "no_context":
                ctx.emit(f"No context: {label}", outcome="info")
            ctx.advance()

        runner = AuditRunner(
            ai=ai, reader=reader, conn=conn,
            sample_mode=sample_mode, sample_size=sample_size,
            target_account=target,
            progress_callback=_on_progress,
        )
        # Initial total hint — refined by the callback as soon as the
        # actual sample size is known (which may be smaller than
        # sample_size if the pool is thinner).
        ctx.set_total(sample_size)
        ctx.emit(
            f"Starting audit · sample_mode={sample_mode} · size={sample_size}"
            + (f" · target={target}" if target else ""),
            outcome="info",
        )
        loop = asyncio.new_event_loop()
        try:
            result = loop.run_until_complete(runner.run())
        finally:
            loop.close()
        ctx.emit(
            f"Audit complete · sampled={result.sampled} · "
            f"classified={result.classified} · "
            f"disagreements={result.disagreements} · "
            f"errors={result.errors}",
            outcome="info",
        )
        return {
            "run_id": result.id,
            "sampled": result.sampled,
            "classified": result.classified,
            "disagreements": result.disagreements,
            "errors": result.errors,
        }

    job_runner = request.app.state.job_runner
    job_id = job_runner.submit(
        kind="audit-run",
        title=f"Audit pass · {sample_mode} · {sample_size} sample(s)",
        fn=_work,
        total=sample_size,
        meta={"sample_mode": sample_mode, "target": target},
        return_url="/audit",
    )
    return request.app.state.templates.TemplateResponse(
        request,
        "partials/_job_modal.html",
        {"job_id": job_id, "on_close_url": "/audit"},
    )


@router.post("/audit/items/{item_id}/accept", response_class=HTMLResponse)
def audit_accept(
    item_id: int,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Accept the AI's proposed account. Writes an override and
    marks the source decision as user_corrected so the vector
    index picks up the correction on next build."""
    row = conn.execute(
        "SELECT * FROM audit_items WHERE id = ? AND status = 'open'",
        (item_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(404, "audit item not found or already decided")
    target_hash = row["txn_hash"]
    new_account = row["ai_proposed_account"]
    current_account = row["current_account"]
    txn = _find_txn(reader, target_hash)
    if txn is None:
        raise HTTPException(410, "transaction no longer in ledger")

    # Per CLAUDE.md "in-place rewrites are the default" — try
    # rewriting the source posting line first, fall back to
    # OverrideWriter only if the in-place path can't proceed
    # (no filename/lineno meta, path safety refusal, etc.).
    from pathlib import Path as _P
    from lamella.core.rewrite.txn_inplace import (
        InPlaceRewriteError,
        rewrite_fixme_to_account,
    )
    amount = _amount_on_account(txn, current_account)
    if amount is None:
        raise HTTPException(409, f"can't find ${current_account} posting")
    currency = _currency_on_account(txn, current_account) or "USD"
    writer = OverrideWriter(
        main_bean=settings.ledger_main,
        overrides=settings.connector_overrides_path,
        conn=conn,
    )

    in_place_done = False
    meta = getattr(txn, "meta", None) or {}
    src_file = meta.get("filename")
    lineno = meta.get("lineno")
    if src_file and lineno is not None:
        try:
            try:
                writer.rewrite_without_hash(target_hash)
            except BeanCheckError:
                raise InPlaceRewriteError("override-strip blocked")
            # Find the signed amount on the current_account
            # posting (the in-place line check uses signed
            # decimal, not abs).
            posting_amount = None
            for p in txn.postings or ():
                if (p.account or "") == current_account and p.units:
                    posting_amount = (
                        Decimal(p.units.number)
                        if p.units.number is not None else None
                    )
                    break
            rewrite_fixme_to_account(
                source_file=_P(src_file),
                line_number=int(lineno),
                old_account=current_account,
                new_account=new_account,
                expected_amount=posting_amount,
                ledger_dir=settings.ledger_dir,
                main_bean=settings.ledger_main,
            )
            in_place_done = True
        except InPlaceRewriteError as exc:
            log.info(
                "audit accept: in-place refused for %s: %s — "
                "falling back to override",
                target_hash[:12], exc,
            )

    if not in_place_done:
        try:
            writer.append(
                txn_date=txn.date if isinstance(txn.date, date_cls)
                         else date_cls.fromisoformat(str(txn.date)),
                txn_hash=target_hash,
                amount=amount,
                from_account=current_account,
                to_account=new_account,
                currency=currency,
                narration=(txn.narration or f"audit accept → {new_account}"),
            )
        except BeanCheckError as exc:
            raise HTTPException(409, f"bean-check blocked: {exc}")
    reader.invalidate()

    # Mark the AI decision as user-corrected so the vector index
    # picks it up (weight 2) on next rebuild.
    if row["ai_decision_id"]:
        try:
            conn.execute(
                "UPDATE ai_decisions "
                "SET user_corrected = 1, "
                "    user_correction = ? "
                "WHERE id = ?",
                (f'{{"target_account": "{new_account}"}}',
                 int(row["ai_decision_id"])),
            )
        except sqlite3.Error:
            pass

    conn.execute(
        "UPDATE audit_items SET status = 'accepted', "
        "decided_at = CURRENT_TIMESTAMP WHERE id = ?",
        (item_id,),
    )
    if "hx-request" in {k.lower() for k in request.headers.keys()}:
        return HTMLResponse(
            f'<div class="audit-decision accepted">'
            f'✓ Accepted → <code>{new_account}</code></div>',
        )
    return RedirectResponse(url="/audit", status_code=303)


@router.post("/audit/items/{item_id}/dismiss", response_class=HTMLResponse)
def audit_dismiss(
    item_id: int,
    request: Request,
    reason: str = Form(""),
    conn: sqlite3.Connection = Depends(get_db),
    settings: Settings = Depends(get_settings),
):
    """Mark an audit disagreement as "original was correct" —
    silences future audits on the merchant/current-account pair."""
    row = conn.execute(
        "SELECT * FROM audit_items WHERE id = ? AND status = 'open'",
        (item_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(404, "audit item not found or already decided")
    reason_clean = reason.strip() or None
    conn.execute(
        """
        INSERT OR IGNORE INTO audit_dismissals
            (merchant_text, current_account, reason)
        VALUES (?, ?, ?)
        """,
        (row["merchant_text"], row["current_account"], reason_clean),
    )
    conn.execute(
        "UPDATE audit_items SET status = 'dismissed', "
        "decided_at = CURRENT_TIMESTAMP WHERE id = ?",
        (item_id,),
    )
    try:
        from lamella.core.transform.steps.step15_audit_dismissals import (
            append_audit_dismissed,
        )
        append_audit_dismissed(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            fingerprint=f"{row['merchant_text']}|{row['current_account']}",
            reason=reason_clean,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("audit-dismissed directive write failed: %s", exc)
    if "hx-request" in {k.lower() for k in request.headers.keys()}:
        return HTMLResponse(
            '<div class="audit-decision dismissed">⏭ Dismissed — original stands</div>',
        )
    return RedirectResponse(url="/audit", status_code=303)


@router.post("/audit/synthetic/{lamella_txn_id}/demote", response_class=HTMLResponse)
def audit_synthetic_demote(
    lamella_txn_id: str,
    request: Request,
    posting_account: str = Form(...),
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """ADR-0046 Phase 4b — inverse of /audit/synthetic/.../promote.

    Flips ``lamella-synthetic-replaceable`` back to TRUE and drops
    confidence to ``"guessed"`` so the strict matcher resumes auto-
    replace behavior. Used when the user realizes they promoted
    prematurely.

    Idempotent: posting twice is a no-op."""
    from lamella.features.bank_sync.synthetic_replace import (
        demote_synthetic_to_replaceable,
    )
    bean_file = (
        settings.ledger_main.parent / "simplefin_transactions.bean"
    )
    try:
        modified = demote_synthetic_to_replaceable(
            bean_file=bean_file,
            lamella_txn_id=lamella_txn_id,
            posting_account=posting_account,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "demote_synthetic failed for %s on %s: %s",
            lamella_txn_id, posting_account, exc,
        )
        return HTMLResponse(
            f'<div class="error">Demote failed: {exc}</div>',
            status_code=500,
        )
    reader.invalidate()
    if "hx-request" in {k.lower() for k in request.headers.keys()}:
        # HX-Refresh moves the row from the Confirmed table to the
        # Replaceable table by reloading /audit. Cheaper than crafting
        # a multi-row swap that has to know about both tables.
        from fastapi.responses import Response as _Resp
        return _Resp(status_code=204, headers={"HX-Refresh": "true"})
    return RedirectResponse(url="/audit", status_code=303)


@router.post("/audit/synthetic/{lamella_txn_id}/promote", response_class=HTMLResponse)
def audit_synthetic_promote(
    lamella_txn_id: str,
    request: Request,
    posting_account: str = Form(...),
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """ADR-0046 Phase 4 — Promote a synthetic counterpart leg to
    confirmed. Flips ``lamella-synthetic-replaceable`` from TRUE to
    FALSE and bumps confidence to ``"confirmed"`` so the matcher
    leaves the leg alone going forward.

    Used by the "Promote synthetic leg" action on the per-txn detail
    page. Idempotent: posting twice is a no-op."""
    from lamella.features.bank_sync.synthetic_replace import (
        promote_synthetic_to_confirmed,
    )
    bean_file = (
        settings.ledger_main.parent / "simplefin_transactions.bean"
    )
    try:
        modified = promote_synthetic_to_confirmed(
            bean_file=bean_file,
            lamella_txn_id=lamella_txn_id,
            posting_account=posting_account,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "promote_synthetic failed for %s on %s: %s",
            lamella_txn_id, posting_account, exc,
        )
        return HTMLResponse(
            f'<div class="error">Promote failed: {exc}</div>',
            status_code=500,
        )
    reader.invalidate()
    if "hx-request" in {k.lower() for k in request.headers.keys()}:
        # HX-Refresh: simplest correct way to move the row from the
        # Replaceable table to the Confirmed table without inventing a
        # cross-table swap.
        from fastapi.responses import Response as _Resp
        return _Resp(status_code=204, headers={"HX-Refresh": "true"})
    return RedirectResponse(url="/audit", status_code=303)


def _find_txn(reader: LedgerReader, target: str) -> Transaction | None:
    for e in reader.load().entries:
        if isinstance(e, Transaction) and txn_hash(e) == target:
            return e
    return None


def _amount_on_account(txn: Transaction, account: str) -> Decimal | None:
    for p in txn.postings or []:
        if p.account == account and p.units and p.units.number is not None:
            return abs(Decimal(p.units.number))
    return None


def _currency_on_account(txn: Transaction, account: str) -> str | None:
    for p in txn.postings or []:
        if p.account == account and p.units:
            return p.units.currency
    return None

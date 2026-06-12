# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import json
import logging
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

from beancount.core.data import Transaction
from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from lamella.features.ai_cascade.decisions import DecisionsLog
from lamella.core.beancount_io import LedgerReader, txn_hash
from lamella.core.config import Settings
from lamella.web.deps import (
    get_db,
    get_fixme_scanner,
    get_ledger_reader,
    get_review_service,
    get_rule_service,
    get_settings,
)
from lamella.features.paperless_bridge.lookups import cached_paperless_hash
from lamella.adapters.paperless.schemas import paperless_url_for
from lamella.core.ledger_writer import BeanCheckError
from lamella.features.receipts.linker import DocumentLinker
from lamella.features.review_queue.pair_detector import PairInfo, detect_pairs
from lamella.features.review_queue.service import ReviewItem, ReviewService
from lamella.features.import_.staging import count_pending_items
from lamella.features.rules.overrides import OverrideWriter
from lamella.features.rules.scanner import FixmeScanner, _fixme_amount, _is_fixme
from lamella.features.rules.service import RuleService

log = logging.getLogger(__name__)

router = APIRouter()


@dataclass(frozen=True)
class RuleSuggestion:
    rule_id: int | None
    target_account: str
    pattern_type: str | None
    pattern_value: str | None
    confidence: float | None
    created_by: str | None


@dataclass(frozen=True)
class AISuggestion:
    target_account: str
    confidence: float
    reasoning: str | None
    decision_id: int | None
    model: str | None


def _parse_suggestion(raw: str | None) -> tuple[RuleSuggestion | None, AISuggestion | None]:
    if not raw:
        return None, None
    try:
        data = json.loads(raw)
    except ValueError:
        return None, None
    if not isinstance(data, dict):
        return None, None
    # Tolerate the Phase-2 `{"source": "rule", ...}` shape.
    if data.get("source") == "rule":
        data = {"rule": data}
    rule_data = data.get("rule")
    ai_data = data.get("ai")
    rule = None
    if isinstance(rule_data, dict):
        rule = RuleSuggestion(
            rule_id=rule_data.get("rule_id"),
            target_account=rule_data.get("target_account", ""),
            pattern_type=rule_data.get("pattern_type"),
            pattern_value=rule_data.get("pattern_value"),
            confidence=rule_data.get("confidence"),
            created_by=rule_data.get("created_by"),
        )
    ai = None
    if isinstance(ai_data, dict):
        ai = AISuggestion(
            target_account=ai_data.get("target_account", ""),
            confidence=float(ai_data.get("confidence") or 0.0),
            reasoning=ai_data.get("reasoning"),
            decision_id=ai_data.get("decision_id"),
            model=ai_data.get("model"),
        )
    return rule, ai


def _fixme_txn(reader: LedgerReader, target_hash: str) -> Transaction | None:
    for entry in reader.load().entries:
        if isinstance(entry, Transaction) and txn_hash(entry) == target_hash:
            return entry
    return None


def _build_txn_index(reader: LedgerReader) -> dict[str, Transaction]:
    index: dict[str, Transaction] = {}
    for entry in reader.load().entries:
        if isinstance(entry, Transaction):
            index[txn_hash(entry)] = entry
    return index


def _txn_context(txn: Transaction | None) -> dict[str, Any] | None:
    if txn is None:
        return None
    fixme_posting = None
    other_postings = []
    for posting in txn.postings:
        if _is_fixme(posting.account):
            fixme_posting = posting
        else:
            other_postings.append(posting)
    amount: Decimal | None = None
    currency = ""
    from_account = ""
    if fixme_posting is not None:
        from_account = fixme_posting.account or ""
        units = fixme_posting.units
        if units is not None:
            if units.number is not None:
                amount = Decimal(units.number)
            if units.currency:
                currency = units.currency
    # If the FIXME posting has no number (interpolated), fall back to any other posting.
    if amount is None:
        for p in other_postings:
            if p.units and p.units.number is not None:
                amount = abs(Decimal(p.units.number))
                if not currency and p.units.currency:
                    currency = p.units.currency
                break
    card_account = ""
    for p in other_postings:
        acct = p.account or ""
        if acct.startswith("Liabilities:") or acct.startswith("Assets:"):
            card_account = acct
            break
    meta = getattr(txn, "meta", None) or {}
    filename = meta.get("filename") if isinstance(meta, dict) else None
    lineno = meta.get("lineno") if isinstance(meta, dict) else None
    return {
        "date": txn.date,
        "payee": getattr(txn, "payee", None),
        "narration": txn.narration or "",
        "amount": amount,
        "currency": currency or "USD",
        "from_account": from_account,
        "card_account": card_account,
        "other_accounts": [p.account for p in other_postings if p.account],
        "filename": filename.rsplit("/", 1)[-1] if isinstance(filename, str) else None,
        "lineno": lineno,
    }


def _decorate(
    item: ReviewItem,
    txn_index: dict[str, Transaction],
    pair_map: dict[str, PairInfo] | None = None,
    receipt_link_index: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    rule_sugg: RuleSuggestion | None = None
    ai_sugg: AISuggestion | None = None
    if item.kind == "fixme":
        rule_sugg, ai_sugg = _parse_suggestion(item.ai_suggestion)
    hash_from_ref = (
        item.source_ref.split(":", 1)[1] if item.source_ref.startswith("fixme:") else None
    )
    txn = txn_index.get(hash_from_ref) if hash_from_ref else None
    pair: PairInfo | None = None
    if pair_map and hash_from_ref:
        pair = pair_map.get(hash_from_ref)
    receipt_link: dict[str, Any] | None = None
    if receipt_link_index and hash_from_ref:
        receipt_link = receipt_link_index.get(hash_from_ref)
    from lamella.core.identity import get_txn_id
    return {
        "item": item,
        "suggestion": rule_sugg,
        "ai": ai_sugg,
        "txn_hash": hash_from_ref,
        # Lineage UUIDv7 — preferred for /txn/{id} link-building.
        # None when the source_ref doesn't resolve to a current ledger
        # entry (rare; the template falls back to ``txn_hash`` then).
        "lamella_txn_id": get_txn_id(txn) if txn is not None else None,
        "txn": _txn_context(txn),
        "pair": pair,
        "receipt_link": receipt_link,
    }


def _build_receipt_link_index(
    conn, txn_hashes: list[str],
) -> dict[str, dict[str, Any]]:
    """Resolve document_links for a batch of txn hashes, joined with
    paperless_doc_index so the review template can render a
    "Verify" button with the paperless_id + know whether the OCR
    looked implausible.

    Key: txn_hash. Value: {paperless_id, document_date, total_amount,
    date_mismatch_likely}."""
    if not txn_hashes:
        return {}
    from datetime import date as _date
    placeholders = ",".join("?" * len(txn_hashes))
    # ADR-0061 Phase 5: column + dict key + template reads all use
    # ``document_date``; the legacy ``receipt_date`` alias was dropped
    # alongside the template rename in this phase.
    rows = conn.execute(
        f"""
        SELECT rl.txn_hash, rl.paperless_id, rl.txn_date,
               pdi.document_date, pdi.total_amount, pdi.vendor,
               pdi.mime_type
          FROM document_links rl
          LEFT JOIN paperless_doc_index pdi
                 ON pdi.paperless_id = rl.paperless_id
         WHERE rl.txn_hash IN ({placeholders})
        """,
        tuple(txn_hashes),
    ).fetchall()
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        # Quick "does this OCR date look implausible" hint: >30 days
        # off from the txn posting date. Shown as a visual cue on
        # the review row so the user knows to hit Verify.
        mismatch = False
        try:
            if row["document_date"] and row["txn_date"]:
                d1 = _date.fromisoformat(str(row["document_date"])[:10])
                d2 = _date.fromisoformat(str(row["txn_date"])[:10])
                mismatch = abs((d1 - d2).days) > 30
        except Exception:  # noqa: BLE001
            mismatch = False
        out[row["txn_hash"]] = {
            "paperless_id": int(row["paperless_id"]),
            "document_date": row["document_date"],
            "total_amount": row["total_amount"],
            "vendor": row["vendor"],
            "mime_type": row["mime_type"],
            "date_mismatch_likely": mismatch,
        }
    return out


def review_page(
    request: Request,
    source: str | None = None,
    hide_transfers: bool = False,
    sort: str = "groups",
    page: int = 1,
    service: ReviewService = Depends(get_review_service),
    reader: LedgerReader = Depends(get_ledger_reader),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Canonical review surface — renders the unified staged queue.
    Delegates context-building + template choice to the staging
    review module so /review and the legacy /review/staged URL show
    identical markup. Falls back to the legacy review_items render
    only when staging is empty AND legacy items still exist."""
    from lamella.web.routes.staging_review import _staged_list_context

    legacy_items = service.list_open()
    staging_pending = count_pending_items(conn)
    if staging_pending > 0 or not legacy_items:
        # Load entries lazily so the empty-queue case stays fast,
        # but pass them through so the suggestion-cards layer can
        # filter dismissals.
        try:
            ledger_entries = reader.load().entries if staging_pending > 0 else []
        except Exception:  # noqa: BLE001
            ledger_entries = []
        ctx = _staged_list_context(
            conn,
            source=source,
            hide_transfers=hide_transfers,
            sort=sort,
            page=page,
            entries=ledger_entries,
        )
        templates = request.app.state.templates
        if request.headers.get("HX-Request"):
            return templates.TemplateResponse(
                request, "partials/_staged_list.html", ctx,
            )
        return templates.TemplateResponse(
            request, "staging_review.html", ctx,
        )
    items = legacy_items
    entries = reader.load().entries if items else []
    txn_index = _build_txn_index(reader) if items else {}
    pair_map = detect_pairs(entries) if items else {}
    # Resolve linked Paperless docs for every FIXME so the review
    # UI can render a per-row Verify button against the doc.
    txn_hashes_for_links = [
        it.source_ref.split(":", 1)[1]
        for it in items
        if it.source_ref.startswith("fixme:")
    ]
    receipt_link_index = _build_receipt_link_index(
        service.conn, txn_hashes_for_links,
    )

    # Build a hash → item-id index so the template can find the partner
    # review row (for rendering / for the "mark as transfer" POST target).
    item_by_hash: dict[str, int] = {}
    for it in items:
        if it.source_ref.startswith("fixme:"):
            item_by_hash[it.source_ref.split(":", 1)[1]] = it.id

    # Determine which half of each pair is the "primary" (rendered) one.
    # Pick the earlier-dated, tiebreaking on hash. The partner is suppressed
    # to avoid double rendering.
    suppressed: set[str] = set()
    primary: set[str] = set()
    for h, info in pair_map.items():
        if h in suppressed or h in primary:
            continue
        partner = info.partner_hash
        if partner not in pair_map:
            continue
        # Pick primary by the earlier txn date, fallback to lexicographic hash.
        left_txn = txn_index.get(h)
        right_txn = txn_index.get(partner)
        if left_txn is None or right_txn is None:
            continue
        if left_txn.date < right_txn.date or (
            left_txn.date == right_txn.date and h < partner
        ):
            primary.add(h)
            suppressed.add(partner)
        else:
            primary.add(partner)
            suppressed.add(h)

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    pair_count = 0
    for item in items:
        hash_from_ref = (
            item.source_ref.split(":", 1)[1] if item.source_ref.startswith("fixme:") else None
        )
        if hash_from_ref and hash_from_ref in suppressed:
            # Skip — already rendered as the partner of a pair.
            continue
        decorated = _decorate(item, txn_index, pair_map, receipt_link_index)
        if decorated.get("pair") and hash_from_ref in primary:
            partner_hash = decorated["pair"].partner_hash
            partner_item_id = item_by_hash.get(partner_hash)
            partner_txn = txn_index.get(partner_hash)
            decorated["partner_item_id"] = partner_item_id
            decorated["partner_txn"] = _txn_context(partner_txn)
            # Determine transfer direction from the primary's FIXME sign.
            # FIXME +ve → primary's real account LOST money → primary is source.
            # FIXME -ve → primary's real account GAINED money → primary is destination.
            primary_fixme = decorated["txn"]["amount"] if decorated.get("txn") else None
            primary_account = (
                decorated["txn"]["card_account"] if decorated.get("txn") else None
            ) or decorated["pair"].partner_account
            partner_account = decorated["pair"].partner_account
            if primary_fixme is not None and primary_fixme < 0:
                decorated["pair_from_account"] = partner_account
                decorated["pair_to_account"] = primary_account
            else:
                decorated["pair_from_account"] = primary_account
                decorated["pair_to_account"] = partner_account
            pair_count += 1
        grouped[item.kind].append(decorated)
    ctx = {
        "grouped": dict(grouped),
        "total": len(items),
        "pair_count": pair_count,
        "suppressed_count": len(suppressed),
    }
    return request.app.state.templates.TemplateResponse(request, "review.html", ctx)


@router.post("/review/rescan")
def rescan(
    request: Request,
    scanner: FixmeScanner = Depends(get_fixme_scanner),
):
    added = scanner.scan()
    if "hx-request" in {k.lower() for k in request.headers.keys()}:
        return HTMLResponse(f"<span>Re-scan enqueued {added} item(s).</span>")
    return RedirectResponse(url="/review", status_code=303)


@router.post("/review/{item_id}/resolve")
async def resolve_item(
    item_id: int,
    request: Request,
    user_decision: str | None = Form(default=None),
    action: str = Form(default="note"),
    target_account: str | None = Form(default=None),
    rule_pattern_type: str = Form(default="merchant_contains"),
    rule_pattern_value: str | None = Form(default=None),
    rule_card_account: str | None = Form(default=None),
    service: ReviewService = Depends(get_review_service),
    rule_service: RuleService = Depends(get_rule_service),
    reader: LedgerReader = Depends(get_ledger_reader),
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    """Resolve a review item.

    Actions:
    - `note` (default, Phase 1 compat): record `user_decision` only.
    - `accept_rule`: write an override that moves the FIXME onto
      `target_account`, bump the backing rule's hit counter, and mark the
      review item resolved.
    - `accept_new_rule`: like `accept_rule` but also persist a new
      `merchant_contains` rule (or the explicit pattern fields from the
      form).
    - `accept_ai`: apply the AI suggestion, stamp the AI decision as
      accepted (not user-corrected), and insert a `created_by='ai'` rule
      so future txns benefit.
    - `reject`: leave the FIXME in place; just resolve the review row.
    """
    item = _get_open_item(service, item_id)

    decisions_log = DecisionsLog(conn)

    if action in {"accept_rule", "accept_new_rule", "accept_ai"}:
        if not target_account:
            raise HTTPException(status_code=400, detail="target_account required")
        if item.kind != "fixme":
            raise HTTPException(status_code=400, detail="rule acceptance only applies to fixme items")
        rule_sugg, ai_sugg = _parse_suggestion(item.ai_suggestion)
        th = item.source_ref.split(":", 1)[1] if item.source_ref.startswith("fixme:") else ""
        try:
            _apply_fixme_override(
                reader=reader,
                settings=settings,
                txn_hash_target=th,
                target_account=target_account,
                conn=conn,
            )
        except BeanCheckError as exc:
            log.error("override rejected by bean-check: %s", exc)
            raise HTTPException(status_code=500, detail=f"bean-check failed: {exc}")

        if action == "accept_new_rule":
            value = (rule_pattern_value or "").strip()
            if not value:
                raise HTTPException(status_code=400, detail="rule_pattern_value required")
            rule_service.learn_from_decision(
                matched_rule_id=None,
                user_target_account=target_account,
                pattern_type=rule_pattern_type,
                pattern_value=value,
                card_account=rule_card_account or None,
                create_if_missing=True,
            )
        elif action == "accept_ai":
            pattern_value = (rule_pattern_value or "").strip()
            if not pattern_value:
                raise HTTPException(status_code=400, detail="rule_pattern_value required")
            rule_service.learn_from_decision(
                matched_rule_id=None,
                user_target_account=target_account,
                pattern_type=rule_pattern_type,
                pattern_value=pattern_value,
                card_account=rule_card_account or None,
                create_if_missing=True,
                source="ai",
            )
            if ai_sugg and ai_sugg.decision_id is not None:
                corrected = ai_sugg.target_account != target_account
                decisions_log.mark_correction(
                    ai_sugg.decision_id,
                    user_correction=(
                        f"user set target_account={target_account}" if corrected else None
                    ),
                )
                # When the user accepts the AI suggestion cleanly,
                # `user_corrected` stays FALSE; the update above only runs
                # when they edited it.
                if corrected:
                    # mark_correction above already flipped the flag; nothing else.
                    pass
        else:
            rule_service.learn_from_decision(
                matched_rule_id=rule_sugg.rule_id if rule_sugg else None,
                user_target_account=target_account,
                pattern_type=(rule_sugg.pattern_type if rule_sugg else "merchant_contains"),
                pattern_value=(rule_sugg.pattern_value or "") if rule_sugg else "",
                card_account=rule_card_account or None,
                create_if_missing=False,
            )
            if ai_sugg and ai_sugg.decision_id is not None and ai_sugg.target_account != target_account:
                decisions_log.mark_correction(
                    ai_sugg.decision_id,
                    user_correction=f"user picked rule target {target_account}",
                )
        reader.invalidate()
        decision_note = user_decision or f"accepted→{target_account}"
    elif action == "reject":
        _, ai_sugg = _parse_suggestion(item.ai_suggestion)
        if ai_sugg and ai_sugg.decision_id is not None:
            decisions_log.mark_correction(
                ai_sugg.decision_id, user_correction="rejected"
            )
        decision_note = user_decision or "rejected"
    elif action == "accept_ai_link":
        await _apply_ai_link(
            conn=conn,
            settings=settings,
            item=item,
            reader=reader,
        )
        decision_note = user_decision or "ai_confirmed"
    else:
        decision_note = user_decision

    ok = service.resolve(item_id, decision_note)
    if not ok:
        raise HTTPException(status_code=404, detail="review item not found or already resolved")

    if "hx-request" in {k.lower() for k in request.headers.keys()}:
        return HTMLResponse("")
    return Response(status_code=204)


@router.post("/review/{item_id}/mark_transfer")
def mark_as_transfer(
    item_id: int,
    request: Request,
    service: ReviewService = Depends(get_review_service),
    reader: LedgerReader = Depends(get_ledger_reader),
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    """Resolve BOTH halves of a detected transfer pair in one click.

    Writes two overrides: each transaction's FIXME posting is routed to the
    other half's non-FIXME account. bean-check runs after each write and
    rolls back on failure. Marks both review items resolved.
    """
    item = _get_open_item(service, item_id)
    if not item.source_ref.startswith("fixme:"):
        raise HTTPException(status_code=400, detail="only fixme items can be marked as transfer")
    my_hash = item.source_ref.split(":", 1)[1]

    entries = list(reader.load().entries)
    pair_map = detect_pairs(entries)
    info = pair_map.get(my_hash)
    if info is None:
        raise HTTPException(
            status_code=400,
            detail="no transfer partner detected for this transaction",
        )

    my_txn = None
    partner_txn = None
    for e in entries:
        if not isinstance(e, Transaction):
            continue
        h = txn_hash(e)
        if h == my_hash:
            my_txn = e
        elif h == info.partner_hash:
            partner_txn = e
        if my_txn and partner_txn:
            break
    if my_txn is None or partner_txn is None:
        raise HTTPException(status_code=404, detail="transaction or partner not in ledger")

    is_htmx = "hx-request" in {k.lower() for k in request.headers.keys()}

    def _inline_error(msg: str) -> HTMLResponse:
        return HTMLResponse(
            f'<li id="review-item-{item_id}" class="review-item link-error">'
            f'<div class="row-error"><strong>✗ Transfer write failed.</strong>'
            f'<details open><summary>Error detail</summary>'
            f'<pre class="excerpt">{msg}</pre></details>'
            f'<p class="muted small">The override was reverted; the row stays '
            f'so you can try again, fix the underlying bean-check issue, or '
            f'skip it.</p></div></li>',
            status_code=200,
        )

    # Self's FIXME routes to partner's non-FIXME account.
    try:
        _apply_fixme_override(
            reader=reader,
            settings=settings,
            txn_hash_target=my_hash,
            target_account=info.partner_account,
            conn=conn,
        )
    except BeanCheckError as exc:
        log.error("transfer override (self) rejected by bean-check: %s", exc)
        if is_htmx:
            return _inline_error(str(exc))
        raise HTTPException(status_code=500, detail=f"bean-check failed: {exc}")

    # Partner's FIXME routes to self's non-FIXME account.
    partner_info = pair_map.get(info.partner_hash)
    if partner_info is None:
        if is_htmx:
            return _inline_error("partner pair info missing")
        raise HTTPException(status_code=500, detail="partner pair info missing")
    try:
        _apply_fixme_override(
            reader=reader,
            settings=settings,
            txn_hash_target=info.partner_hash,
            target_account=partner_info.partner_account,
            conn=conn,
        )
    except BeanCheckError as exc:
        log.error("transfer override (partner) rejected by bean-check: %s", exc)
        if is_htmx:
            return _inline_error(str(exc))
        raise HTTPException(status_code=500, detail=f"bean-check failed: {exc}")

    # Resolve both review items (autocommit semantics depend on the
    # pool; call commit defensively so the resolution persists even if
    # a later handler raises).
    service.resolve(item_id, f"transfer → {info.partner_account}")
    for it in service.list_open():
        if it.source_ref == f"fixme:{info.partner_hash}":
            service.resolve(it.id, f"transfer → {partner_info.partner_account}")
            break
    try:
        service.conn.commit()
    except Exception:  # noqa: BLE001
        pass

    reader.invalidate()

    if is_htmx:
        return HTMLResponse(
            f'<li id="review-item-{item_id}" class="review-item row-resolved">'
            f'<div><strong>✓ Marked as transfer.</strong> '
            f'{info.partner_account}</div></li>',
            status_code=200,
        )
    return Response(status_code=204)


@router.post("/review/{item_id}/mark_transfer_to")
async def mark_transfer_to_account(
    item_id: int,
    request: Request,
    service: ReviewService = Depends(get_review_service),
    reader: LedgerReader = Depends(get_ledger_reader),
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    """Manually categorize a FIXME txn as a transfer to an account the
    user picks (brokerage sweeps, intra-bank moves where only one side
    is in the ledger, etc.). Writes a single override, resolves the
    item. Unlike `/mark_transfer`, no partner row is touched."""
    form = await request.form()
    target_account = str(form.get("target_account") or "").strip()
    if not target_account:
        raise HTTPException(status_code=400, detail="target_account is required")
    item = _get_open_item(service, item_id)
    if not item.source_ref.startswith("fixme:"):
        raise HTTPException(status_code=400, detail="only fixme items can be marked as transfer")
    my_hash = item.source_ref.split(":", 1)[1]
    try:
        _apply_fixme_override(
            reader=reader,
            settings=settings,
            txn_hash_target=my_hash,
            target_account=target_account,
            conn=conn,
        )
    except BeanCheckError as exc:
        log.error("transfer-to override rejected by bean-check: %s", exc)
        raise HTTPException(status_code=500, detail=f"bean-check failed: {exc}")
    service.resolve(item_id, f"transfer → {target_account}")
    reader.invalidate()
    if "hx-request" in {k.lower() for k in request.headers.keys()}:
        return HTMLResponse("")
    return Response(status_code=204)


async def _apply_ai_link(
    *,
    conn,
    settings: Settings,
    item: ReviewItem,
    reader: LedgerReader,
) -> None:
    """Accept the AI's receipt-match suggestion: write the link the same
    way the webhook does, so `match_method='ai_confirmed'`."""
    raw = item.ai_suggestion or ""
    try:
        data = json.loads(raw) if raw else {}
    except ValueError:
        data = {}
    ai = data.get("ai") if isinstance(data, dict) else None
    if not isinstance(ai, dict) or not ai.get("best_match_hash"):
        raise HTTPException(status_code=400, detail="no AI match to accept")
    paperless_id = data.get("paperless_id")
    if paperless_id is None:
        raise HTTPException(status_code=400, detail="paperless_id missing from suggestion")
    txn_hash_target = ai["best_match_hash"]
    txn = _fixme_txn(reader, txn_hash_target)
    if txn is None:
        raise HTTPException(status_code=404, detail="matched txn no longer in ledger")
    # Find the expense/liability amount for the link stamp.
    amount = Decimal("0")
    for posting in txn.postings:
        if posting.units and posting.units.number is not None:
            acct = posting.account or ""
            if acct.startswith("Expenses:") or acct.startswith("Liabilities:"):
                amount = abs(Decimal(posting.units.number))
                break
    linker = DocumentLinker(
        conn=conn,
        main_bean=settings.ledger_main,
        connector_links=settings.connector_links_path,
    )
    try:
        linker.link(
            paperless_id=int(paperless_id),
            txn_hash=txn_hash_target,
            txn_date=_to_date(txn.date),
            txn_amount=amount,
            match_method="ai_confirmed",
            match_confidence=float(ai.get("confidence") or 0.9),
            paperless_hash=cached_paperless_hash(conn, int(paperless_id)),
            paperless_url=paperless_url_for(settings.paperless_url, int(paperless_id)),
        )
    except BeanCheckError as exc:
        log.error("ai link rejected by bean-check: %s", exc)
        raise HTTPException(status_code=500, detail=f"bean-check failed: {exc}")
    reader.invalidate()

    # ADR-0044: write the four canonical Lamella_* fields back to
    # Paperless. Best-effort: never breaks the link.
    try:
        from lamella.features.paperless_bridge.writeback import (
            writeback_after_link,
        )
        await writeback_after_link(
            paperless_id=int(paperless_id),
            txn_hash=txn_hash_target,
            settings=settings,
            reader=reader,
            conn=conn,
        )
    except Exception:  # noqa: BLE001
        pass


def _get_open_item(service: ReviewService, item_id: int) -> ReviewItem:
    for item in service.list_open():
        if item.id == item_id:
            return item
    raise HTTPException(status_code=404, detail="review item not found or already resolved")


def _apply_fixme_override(
    *,
    reader: LedgerReader,
    settings: Settings,
    txn_hash_target: str,
    target_account: str,
    conn: sqlite3.Connection | None = None,
) -> None:
    """Resolve a single FIXME → target. Per CLAUDE.md
    "in-place rewrites are the default, overrides are the
    fallback" — try the in-place path first, fall back to
    OverrideWriter only when filename/lineno is missing or
    txn_inplace refuses (path safety, etc.).
    """
    from pathlib import Path as _P
    from lamella.core.rewrite.txn_inplace import (
        InPlaceRewriteError,
        rewrite_fixme_to_account,
    )

    txn = _fixme_txn(reader, txn_hash_target)
    if txn is None:
        raise HTTPException(status_code=404, detail="FIXME transaction not found for this item")

    amount: Decimal | None = _fixme_amount(txn)
    from_account: str | None = next(
        (p.account for p in txn.postings if _is_fixme(p.account)), None
    )
    if amount is None or from_account is None:
        raise HTTPException(status_code=400, detail="transaction has no FIXME posting with amount")

    currency = "USD"
    fixme_decimal: Decimal | None = None
    for posting in txn.postings:
        if _is_fixme(posting.account) and posting.units:
            if posting.units.currency:
                currency = posting.units.currency
            if posting.units.number is not None:
                fixme_decimal = Decimal(posting.units.number)
            break

    writer = OverrideWriter(
        main_bean=settings.ledger_main,
        overrides=settings.connector_overrides_path,
        conn=conn,
    )

    # Try in-place rewrite. Strip any prior override on this
    # hash first so we don't end up with both representations.
    meta = getattr(txn, "meta", None) or {}
    src_file = meta.get("filename")
    lineno = meta.get("lineno")
    if src_file and lineno is not None:
        try:
            try:
                writer.rewrite_without_hash(txn_hash_target)
            except BeanCheckError:
                # If just clearing the prior override breaks the
                # ledger, the override fallback is safer.
                raise InPlaceRewriteError("override-strip blocked")
            rewrite_fixme_to_account(
                source_file=_P(src_file),
                line_number=int(lineno),
                old_account=from_account,
                new_account=target_account,
                expected_amount=fixme_decimal,
                ledger_dir=settings.ledger_dir,
                main_bean=settings.ledger_main,
            )
            return
        except InPlaceRewriteError as exc:
            log.info(
                "review resolve: in-place refused for %s: %s — "
                "falling back to override",
                txn_hash_target[:12], exc,
            )

    writer.append(
        txn_date=_to_date(txn.date),
        txn_hash=txn_hash_target,
        amount=amount,
        from_account=from_account,
        to_account=target_account,
        currency=currency,
        narration=(txn.narration or "FIXME override"),
    )


def _to_date(value) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))

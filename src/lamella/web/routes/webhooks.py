# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from lamella.features.ai_cascade.context import ReceiptFacts, candidate_from_txn
from lamella.features.ai_cascade.gating import GateAction
from lamella.features.ai_cascade.match import rank_candidates
from lamella.features.ai_cascade.service import AIService
from lamella.core.beancount_io import LedgerReader
from lamella.core.config import Settings
from lamella.web.deps import (
    get_ai_service,
    get_db,
    get_ledger_reader,
    get_review_service,
    get_settings,
)
from lamella.adapters.paperless.client import PaperlessClient, PaperlessError
from lamella.features.paperless_bridge.lookups import resolve_and_cache_paperless_hash
from lamella.adapters.paperless.schemas import paperless_url_for
from lamella.core.ledger_writer import BeanCheckError
from lamella.features.receipts.linker import ReceiptLinker
from lamella.features.receipts.matcher import find_candidates
from lamella.features.review_queue.service import ReviewService

log = logging.getLogger(__name__)

router = APIRouter()


class PaperlessWebhookPayload(BaseModel):
    document_id: int


def _priority_for_amount(amount: Decimal) -> int:
    value = abs(amount)
    if value >= 500:
        return 3
    if value >= 100:
        return 2
    if value >= 25:
        return 1
    return 0


@router.post("/webhooks/paperless/new")
async def paperless_new_document(
    payload: PaperlessWebhookPayload,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    reviews: ReviewService = Depends(get_review_service),
    ai: AIService = Depends(get_ai_service),
) -> dict:
    if not settings.paperless_configured:
        raise HTTPException(
            status_code=503,
            detail="Paperless is not configured.",
        )

    client = PaperlessClient(
        base_url=settings.paperless_url,  # type: ignore[arg-type]
        api_token=settings.paperless_api_token.get_secret_value(),  # type: ignore[union-attr]
        extra_headers=settings.paperless_extra_headers(),
    )
    try:
        try:
            doc = await client.get_document(payload.document_id)
            fields = await client.get_custom_fields(doc)
            # Resolve the content-hash now (while the client is still
            # open) so the link stamp gets it even on Paperless versions
            # that omit checksums from the main document response.
            paperless_hash = await resolve_and_cache_paperless_hash(
                client,
                conn,
                doc.id,
                doc_original_checksum=doc.original_checksum,
            )
        except PaperlessError as exc:
            raise HTTPException(status_code=502, detail=str(exc))
    finally:
        await client.aclose()

    request.app.state.last_webhook_at = datetime.now(timezone.utc)

    total = fields.get("receipt_total")
    r_date = fields.get("receipt_date")
    last_four = fields.get("payment_last_four")
    vendor = fields.get("vendor") or doc.title

    ledger = reader.load()

    exact = find_candidates(
        ledger.entries,
        receipt_total=total,
        receipt_date=r_date,
        last_four=last_four if isinstance(last_four, str) else None,
        date_window_days=0,
    )
    fuzzy = find_candidates(
        ledger.entries,
        receipt_total=total,
        receipt_date=r_date,
        last_four=last_four if isinstance(last_four, str) else None,
        date_window_days=1,
    )

    chosen = None
    match_method: str | None = None
    if len(exact) == 1:
        chosen = exact[0]
        match_method = "exact"
    elif len(fuzzy) == 1:
        chosen = fuzzy[0]
        match_method = "fuzzy_date"

    if chosen is not None and match_method is not None:
        linker = ReceiptLinker(
            conn=conn,
            main_bean=settings.ledger_main,
            connector_links=settings.connector_links_path,
        )
        try:
            linker.link(
                paperless_id=doc.id,
                txn_hash=chosen.txn_hash,
                txn_date=chosen.date,
                txn_amount=chosen.amount,
                match_method=match_method,
                match_confidence=1.0 if match_method == "exact" else 0.9,
                paperless_hash=paperless_hash,
                paperless_url=paperless_url_for(settings.paperless_url, doc.id),
            )
        except BeanCheckError as exc:
            log.error("bean-check rejected link for doc %s: %s", doc.id, exc)
            raise HTTPException(status_code=500, detail=f"bean-check failed: {exc}")
        reader.invalidate()
        # ADR-0044: write Lamella_* fields back to Paperless.
        # Best-effort; never breaks the link.
        try:
            from lamella.features.paperless_bridge.writeback import (
                writeback_after_link,
            )
            await writeback_after_link(
                paperless_id=doc.id,
                txn_hash=chosen.txn_hash,
                settings=settings,
                reader=reader,
                conn=conn,
            )
        except Exception:  # noqa: BLE001
            pass
        return {
            "status": "linked",
            "match": match_method,
            "txn_hash": chosen.txn_hash,
        }

    # Phase 3: AI disambiguation on ambiguity. Pre-compute the review
    # payload so we can attach the ranking even when auto-link doesn't fire.
    ai_payload: dict | None = None
    if ai.enabled and not ai.spend_cap_reached() and fuzzy:
        try:
            ranking, ai_payload = await _rank_with_ai(
                ai=ai,
                paperless_id=doc.id,
                vendor=vendor if isinstance(vendor, str) else None,
                total=total,
                r_date=r_date,
                last_four=last_four if isinstance(last_four, str) else None,
                candidates=fuzzy,
            )
        except Exception as exc:  # defensive
            log.warning("ai match failed for doc %s: %s", doc.id, exc)
            ranking = None

        if ranking is not None:
            gate_action = ai.gate.decide_match(
                ranking=ranking,
                candidates_present=bool(fuzzy),
            )
            if gate_action == GateAction.AUTO_LINK and ranking.best_match_hash:
                winning = next(
                    (c for c in fuzzy if c.txn_hash == ranking.best_match_hash),
                    None,
                )
                if winning is not None:
                    linker = ReceiptLinker(
                        conn=conn,
                        main_bean=settings.ledger_main,
                        connector_links=settings.connector_links_path,
                    )
                    try:
                        linker.link(
                            paperless_id=doc.id,
                            txn_hash=winning.txn_hash,
                            txn_date=winning.date,
                            txn_amount=winning.amount,
                            match_method="ai_confirmed",
                            match_confidence=float(ranking.confidence),
                            paperless_hash=paperless_hash,
                            paperless_url=paperless_url_for(settings.paperless_url, doc.id),
                        )
                    except BeanCheckError as exc:
                        log.error("bean-check rejected ai link for doc %s: %s", doc.id, exc)
                        raise HTTPException(
                            status_code=500, detail=f"bean-check failed: {exc}"
                        )
                    reader.invalidate()
                    # ADR-0044: write Lamella_* fields back. Best-effort.
                    try:
                        from lamella.features.paperless_bridge.writeback import (
                            writeback_after_link,
                        )
                        await writeback_after_link(
                            paperless_id=doc.id,
                            txn_hash=winning.txn_hash,
                            settings=settings,
                            reader=reader,
                            conn=conn,
                        )
                    except Exception:  # noqa: BLE001
                        pass
                    return {
                        "status": "linked",
                        "match": "ai_confirmed",
                        "txn_hash": winning.txn_hash,
                        "confidence": float(ranking.confidence),
                    }

    kind = "ambiguous_match" if fuzzy else "receipt_unmatched"
    suggestion = {
        "paperless_id": doc.id,
        "title": doc.title,
        "receipt_total": str(total) if total is not None else None,
        "receipt_date": str(r_date) if r_date is not None else None,
        "last_four": last_four,
        "candidates": [
            {
                "txn_hash": c.txn_hash,
                "date": c.date.isoformat(),
                "narration": c.txn.narration,
                "day_delta": c.day_delta,
            }
            for c in fuzzy
        ],
    }
    if ai_payload is not None:
        suggestion["ai"] = ai_payload
    priority = _priority_for_amount(Decimal(str(total))) if total is not None else 0
    reviews.enqueue(
        kind=kind,
        source_ref=f"paperless:{doc.id}",
        priority=priority,
        ai_suggestion=json.dumps(suggestion),
        ai_model=ai.model_for("match_receipt") if ai_payload else None,
    )
    return {
        "status": "queued",
        "kind": kind,
        "candidates": len(fuzzy),
        "ai": bool(ai_payload),
    }


async def _rank_with_ai(
    *,
    ai: AIService,
    paperless_id: int,
    vendor: str | None,
    total,
    r_date,
    last_four: str | None,
    candidates,
):
    client = ai.new_client()
    if client is None:
        return None, None
    try:
        total_dec = Decimal(str(total)) if total is not None else Decimal("0")
        rdate = r_date
        if isinstance(rdate, str):
            from datetime import date as _date

            try:
                rdate = _date.fromisoformat(rdate[:10])
            except ValueError:
                rdate = datetime.now(timezone.utc).date()
        receipt = ReceiptFacts(
            vendor=vendor,
            total=total_dec,
            currency="USD",
            date=rdate,
            last4=last_four,
        )
        cand_facts = [candidate_from_txn(c.txn, receipt_date=rdate) for c in candidates]
        ranking = await rank_candidates(
            client,
            paperless_id=paperless_id,
            receipt=receipt,
            candidates=cand_facts,
            model=ai.model_for("match_receipt"),
        )
        if ranking is None:
            return None, None
        payload = {
            "best_match_hash": ranking.best_match_hash,
            "confidence": ranking.confidence,
            "reasoning": ranking.reasoning,
            "alternate_date_hypothesis": ranking.alternate_date_hypothesis,
            "decision_id": ranking.decision_id,
            "runners_up": list(ranking.runners_up),
        }
        return ranking, payload
    finally:
        await client.aclose()

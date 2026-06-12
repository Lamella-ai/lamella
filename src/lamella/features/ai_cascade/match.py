# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import logging
from typing import Iterable

from pydantic import BaseModel, Field

from lamella.adapters.openrouter.client import AIError, AIResult, OpenRouterClient
from lamella.features.ai_cascade.context import CandidateFacts, ReceiptFacts, render
from lamella.features.ai_cascade.gating import MatchRanking

log = logging.getLogger(__name__)

SYSTEM = (
    "You are a meticulous bookkeeper. You disambiguate which bank "
    "transaction a receipt belongs to. Never invent a transaction hash. "
    "If nothing fits, say so."
)


class MatchResponse(BaseModel):
    best_match: str | None = None
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: str = ""
    alternate_date_hypothesis: str | None = None


async def rank_candidates(
    client: OpenRouterClient,
    *,
    paperless_id: int,
    receipt: ReceiptFacts,
    candidates: Iterable[CandidateFacts],
    model: str | None = None,
) -> MatchRanking | None:
    cand_list = list(candidates)
    if not cand_list:
        return None

    valid_hashes = {c.txn_hash for c in cand_list}
    user_prompt = render(
        "match_receipt.j2",
        receipt=receipt,
        candidates=cand_list,
    )

    try:
        result: AIResult[MatchResponse] = await client.chat(
            decision_type="match_receipt",
            input_ref=f"paperless:{paperless_id}",
            system=SYSTEM,
            user=user_prompt,
            schema=MatchResponse,
            model=model,
        )
    except AIError as exc:
        log.warning("match_receipt failed for doc %s: %s", paperless_id, exc)
        return None

    data = result.data
    best = (data.best_match or "").strip() or None
    if best is not None and best not in valid_hashes:
        log.info(
            "match_receipt returned off-list hash %r for doc %s — suppressing",
            best,
            paperless_id,
        )
        best = None

    # Build a runners-up list from remaining candidates by a simple heuristic
    # (day_delta asc); the briefing's gate needs the top runner-up score,
    # which we approximate with (1 - day_delta / 14) clipped to [0, 0.6].
    runners: list[tuple[str, float]] = []
    for c in sorted(cand_list, key=lambda x: x.day_delta):
        if c.txn_hash == best:
            continue
        score = max(0.0, min(0.6, 0.6 - 0.05 * c.day_delta))
        runners.append((c.txn_hash, score))

    return MatchRanking(
        best_match_hash=best,
        confidence=float(data.confidence),
        runners_up=tuple(runners),
        reasoning=data.reasoning,
        alternate_date_hypothesis=data.alternate_date_hypothesis,
        decision_id=result.decision_id,
    )

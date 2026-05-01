# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Iterable

from pydantic import BaseModel, Field

from lamella.adapters.openrouter.client import AIError, AIResult, OpenRouterClient
from lamella.features.ai_cascade.context import render

log = logging.getLogger(__name__)

SYSTEM = (
    "You extract structured hints from free-text spending notes. "
    "Never invent fields — leave them null when the note doesn't say. "
    "Return only values grounded in the note body."
)


class ParseNoteResponse(BaseModel):
    merchant_hint: str | None = None
    entity_hint: str | None = None
    amount_hint: Decimal | None = None
    date_hint: str | None = None
    # Active-window fields — when the note implies a date range
    # (e.g. "in Atlanta April 14–20"), the model returns the endpoints
    # so the note becomes active for every transaction in that range.
    # If only a single date is implied, both should equal it.
    active_from_hint: str | None = None
    active_to_hint: str | None = None
    # Card-override hint (G7 precursor) — when the note says "I'm
    # using my personal Visa for Acme business this week," the
    # model should set card_override_hint: true and entity_hint:
    # Acme. The immediate fix persists this flag; Phase G7 teaches
    # the classifier to act on it.
    card_override_hint: bool = False
    keywords: list[str] = Field(default_factory=list)


@dataclass(frozen=True)
class NoteAnnotations:
    merchant_hint: str | None
    entity_hint: str | None
    amount_hint: Decimal | None
    date_hint: date | None
    active_from: date | None
    active_to: date | None
    card_override: bool
    keywords: tuple[str, ...]
    decision_id: int


def _coerce_date(raw: str | None) -> date | None:
    if not raw:
        return None
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        return None


async def parse_note(
    client: OpenRouterClient,
    *,
    note_id: int,
    body: str,
    captured_at: datetime,
    entities: Iterable[str],
    model: str | None = None,
) -> NoteAnnotations | None:
    user_prompt = render(
        "parse_note.j2",
        body=body,
        entities=list(entities),
        captured_at=captured_at.date().isoformat(),
    )
    entity_list = {e.strip() for e in entities if e}

    try:
        result: AIResult[ParseNoteResponse] = await client.chat(
            decision_type="parse_note",
            input_ref=f"note:{note_id}",
            system=SYSTEM,
            user=user_prompt,
            schema=ParseNoteResponse,
            model=model,
        )
    except AIError as exc:
        log.warning("parse_note failed for note %s: %s", note_id, exc)
        return None

    data = result.data

    entity = (data.entity_hint or "").strip() or None
    if entity is not None and entity_list and entity not in entity_list:
        entity = None

    # Resolve the active window. Three fallbacks so legacy prompts
    # and model outputs all end up producing SOMETHING useful:
    #   1. If the model returned both endpoints, use them.
    #   2. Else if only a single date_hint is present, single-day
    #      window anchored there.
    #   3. Else leave both null — notes_active_on treats that as a
    #      fallback single-day window on captured_at.
    from_raw = _coerce_date(data.active_from_hint)
    to_raw = _coerce_date(data.active_to_hint)
    if from_raw is None and to_raw is None:
        d = _coerce_date(data.date_hint)
        if d is not None:
            from_raw = to_raw = d
    elif from_raw is None:
        from_raw = to_raw
    elif to_raw is None:
        to_raw = from_raw
    # Guard against inverted ranges.
    if from_raw and to_raw and from_raw > to_raw:
        from_raw, to_raw = to_raw, from_raw

    return NoteAnnotations(
        merchant_hint=(data.merchant_hint or "").strip() or None,
        entity_hint=entity,
        amount_hint=data.amount_hint,
        date_hint=_coerce_date(data.date_hint),
        active_from=from_raw,
        active_to=to_raw,
        card_override=bool(data.card_override_hint),
        keywords=tuple(k.strip().lower() for k in data.keywords if k),
        decision_id=result.decision_id,
    )

# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""SuggestionCard data shape + the registry that builds them.

A ``SuggestionCard`` is a small, host-page-agnostic record:
title + body + CTA. The dashboard renders cards in a strip at
the top; /review renders them above the staged-list; /card
renders them per-row inline. The card itself doesn't know which
surface it's on — the host pages filter by ``contexts``.

Adding a new suggestion type:
  1. Write the detector / data source (something that returns
     candidates the user should act on).
  2. Add a ``_build_<kind>_cards()`` helper here that turns
     candidates into ``SuggestionCard`` records and tags them
     with the right ``contexts``.
  3. Call it from ``build_suggestion_cards`` under the contexts
     it applies to.

Cards intentionally do not persist between requests — they are
rebuilt every time the host page loads. Dismissals (when added)
will live as ``custom "suggestion-dismissed"`` directives the
ledger keeps so reconstruct can re-derive them.
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from typing import Iterable, Literal

log = logging.getLogger(__name__)

__all__ = [
    "SuggestionCard",
    "SuggestionContext",
    "build_suggestion_cards",
]


# Where a card is allowed to render. A card with `contexts={"global"}`
# only shows on dashboard / /review top-strip; one with `{"row"}`
# only shows inline on /card or inside a /review row. A card may
# carry multiple contexts when the same suggestion makes sense
# in more than one place.
SuggestionContext = Literal["global", "row"]


@dataclass(frozen=True)
class SuggestionCard:
    """One actionable suggestion the system observed.

    The macro at ``templates/_components/cards.html::suggestion_card``
    renders this directly — keep field names stable and the
    presence of ``cta_*`` fields well-defined (any of them None
    means "no CTA, info-only").
    """
    id: str
    """Stable identifier — e.g. ``payout_source:ebay:Acme``. Used
    for dismissal + de-duplication when the same suggestion fires
    from multiple data sources (ledger + staging)."""

    kind: str
    """Suggestion type. ``payout_source``, future kinds: ``recurring``,
    ``intercompany_review``, etc."""

    tone: Literal["info", "ok", "warn"] = "info"
    """Visual treatment. Maps to ``card--ok`` / ``card--warn`` /
    plain ``card`` (info)."""

    icon: str = "sparkles"
    """Macro name in ``_components/_icons.html`` (e.g. ``repeat``,
    ``sparkles``, ``creditcard``)."""

    title: str = ""
    body: str = ""

    # CTA — any of cta_label / cta_action None means "no button."
    cta_label: str | None = None
    cta_action: str | None = None
    cta_method: Literal["GET", "POST"] = "POST"
    cta_form_data: dict[str, str] = field(default_factory=dict)

    # Optional secondary action — typically "Dismiss" / "Not now."
    secondary_label: str | None = None
    secondary_action: str | None = None
    secondary_method: Literal["GET", "POST"] = "POST"
    secondary_form_data: dict[str, str] = field(default_factory=dict)

    contexts: frozenset[SuggestionContext] = field(
        default_factory=lambda: frozenset({"global"}),
    )
    """Where this card may render. The host page filters by this."""

    # Free-form data the template can use for richer rendering
    # without bloating the core fields. Keep keys stable per kind.
    details: dict = field(default_factory=dict)


# --- payout-source cards ---------------------------------------------------


def _build_payout_source_cards(
    conn: sqlite3.Connection,
    entries: Iterable,
    *,
    context: SuggestionContext,
    row_payee_text: str | None = None,
    row_account_path: str | None = None,
) -> list[SuggestionCard]:
    """Build payout-source suggestion cards from the detector output.

    For ``context='global'`` we surface every detected candidate
    that isn't already scaffolded.

    For ``context='row'`` we narrow to the candidate matching the
    specific row the user is looking at — the row's payee +
    description picks the pattern, the row's receiving account
    picks the entity. This is what the per-row inline nudge on
    /card uses.
    """
    from lamella.features.bank_sync.payout_sources import (
        detect_payout_sources,
        match_payout_pattern,
        read_payout_dismissals,
        suggested_account_path,
    )

    cards: list[SuggestionCard] = []

    if context == "row":
        if not row_payee_text or not row_account_path:
            return cards
        pat = match_payout_pattern(row_payee_text)
        if pat is None:
            return cards
        # Row context bypasses the frequency threshold — the user is
        # looking at this specific row right now and asking "what
        # is this?" The detector's threshold is for surfacing
        # unprompted; once the user is in the row, even a single
        # match is informative. We still skip when the suggested
        # account already exists (no duplicate prompts) OR when the
        # user has dismissed this (pattern, entity) pair.
        parts = row_account_path.split(":")
        if len(parts) < 3 or parts[0] != "Assets":
            return cards
        entity = parts[1]
        if entries:
            dismissed = read_payout_dismissals(entries)
            if (pat.id, entity) in dismissed:
                return cards
        suggested = suggested_account_path(entity, pat.leaf)
        try:
            existing = conn.execute(
                "SELECT 1 FROM accounts_meta WHERE account_path = ? LIMIT 1",
                (suggested,),
            ).fetchone()
        except sqlite3.OperationalError:
            existing = None
        if existing is not None:
            return cards
        cards.append(SuggestionCard(
            id=f"payout_source:{pat.id}:{entity}:row",
            kind="payout_source",
            tone="info",
            icon="repeat",
            title=f"This looks like a payout from {pat.display}",
            body=(
                f"{pat.display} can't hold cash on your books — payouts "
                f"to {row_account_path} are the disbursement leg of a "
                f"transfer from a {pat.display} clearing account. "
                f"Set up {suggested} so this and future "
                f"{pat.display} payouts route there as transfers, with "
                f"sales / fees imported separately to reconcile."
            ),
            cta_label=f"Scaffold {suggested}",
            cta_action="/settings/payout-sources/scaffold",
            cta_form_data={
                "pattern_id": pat.id,
                "entity": entity,
                "leaf": pat.leaf,
                "suggested_path": suggested,
                "receiving_account": row_account_path,
            },
            secondary_label="Not a payout source",
            secondary_action="/settings/payout-sources/dismiss",
            secondary_form_data={
                "pattern_id": pat.id,
                "entity": entity,
            },
            contexts=frozenset({"row"}),
            details={
                "pattern_id": pat.id,
                "display_name": pat.display,
                "suggested_path": suggested,
                "entity": entity,
            },
        ))
        return cards

    # Global context — full detector output, undismissed and
    # unscaffolded.
    candidates = detect_payout_sources(conn, entries)
    for c in candidates:
        if c.already_scaffolded:
            continue
        share_pct = int(round(c.inbound_share * 100))
        cards.append(SuggestionCard(
            id=f"payout_source:{c.pattern_id}:{c.entity}",
            kind="payout_source",
            tone="info",
            icon="repeat",
            title=f"{c.display_name} looks like a payout source",
            body=(
                f"{c.hits} deposit{'s' if c.hits != 1 else ''} matching "
                f"'{c.display_name}' have hit "
                f"{c.receiving_account} ({share_pct}% inbound). "
                f"Scaffold {c.suggested_path} so future "
                f"{c.display_name} payouts route there as transfers."
            ),
            cta_label=f"Scaffold {c.suggested_path}",
            cta_action="/settings/payout-sources/scaffold",
            cta_form_data={
                "pattern_id": c.pattern_id,
                "entity": c.entity,
                "leaf": c.suggested_leaf,
                "suggested_path": c.suggested_path,
                "receiving_account": c.receiving_account,
            },
            secondary_label="Dismiss",
            secondary_action="/settings/payout-sources/dismiss",
            secondary_form_data={
                "pattern_id": c.pattern_id,
                "entity": c.entity,
            },
            contexts=frozenset({"global"}),
            details={
                "pattern_id": c.pattern_id,
                "display_name": c.display_name,
                "suggested_path": c.suggested_path,
                "entity": c.entity,
                "hits": c.hits,
                "inbound_count": c.inbound_count,
                "outbound_count": c.outbound_count,
                "inbound_share": c.inbound_share,
                "sample_dates": list(c.sample_dates),
            },
        ))
    return cards


# --- public API -----------------------------------------------------------


def build_suggestion_cards(
    conn: sqlite3.Connection,
    entries: Iterable | None = None,
    *,
    context: SuggestionContext = "global",
    row_payee_text: str | None = None,
    row_account_path: str | None = None,
) -> list[SuggestionCard]:
    """Build the list of suggestion cards applicable to ``context``.

    Args:
        conn: SQLite connection.
        entries: Beancount entries (only required for global; row
            context derives everything from the row).
        context: ``'global'`` for dashboard / top-of-page strips,
            ``'row'`` for per-row inline cards on /card and inside
            /review groups.
        row_payee_text: payee + description text of the row, used
            only when ``context='row'``.
        row_account_path: receiving account of the row (used only
            for ``context='row'``).

    Returns: list of cards. The host page filters by
    ``card.contexts`` so a global-only card doesn't accidentally
    render in a row slot.
    """
    cards: list[SuggestionCard] = []
    cards.extend(
        _build_payout_source_cards(
            conn, entries or [],
            context=context,
            row_payee_text=row_payee_text,
            row_account_path=row_account_path,
        )
    )
    # Final filter so callers never see cards outside the requested
    # context (defense in depth — the helpers should only return
    # context-correct cards but the contract is clearer this way).
    return [c for c in cards if context in c.contexts]

# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Staging-backed review surface — NEXTGEN.md Phase B2 full swing.

``GET /review/staged`` renders pending items from the unified
staging surface via ``list_pending_items``.

``POST /review/staged/dismiss`` marks a row dismissed (terminal).

``POST /review/staged/classify`` (Phase B2 full swing) writes a
clean CLASSIFIED Beancount transaction for a staged row — no
FIXME leg, no override block. The staged row flips to ``promoted``
and the review list refreshes. Accepts an explicit
``target_account`` or an ``accept_proposed`` toggle that uses the
row's existing staged_decision proposal.

Compatible with the ongoing SimpleFIN FIXME emission until B2's
paired refactor lands — a row that's still waiting for user
attention stays classifiable from here even if it was also
written as a FIXME in the source bean file. The classify action
writes a new clean entry AND strips the FIXME override via the
in-place rewrite path.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from collections import Counter
from datetime import date as _date
from decimal import Decimal
from pathlib import Path

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from typing import List

from lamella.features.ai_cascade.service import AIService
from lamella.core.beancount_io import LedgerReader
from lamella.core.config import Settings
from lamella.web.deps import get_db, get_ledger_reader, get_settings
from lamella.web.routes import _htmx
from lamella.features.review_queue.grouping import group_staged_rows
from lamella.features.review_queue.service import ReviewService
from lamella.features.rules.overrides import OverrideWriter
from lamella.features.rules.scanner import FixmeScanner
from lamella.features.rules.service import RuleService
from lamella.features.bank_sync.writer import PendingEntry, SimpleFINWriter
from lamella.features.import_.staging import (
    StagingService,
    count_pending_items,
    list_pending_items,
)

log = logging.getLogger(__name__)

# Cap on Ask-AI retries before the modal short-circuits to the manual
# fallback. Mirrors api_txn.py's local constant; lifted to module level
# here because the legacy /review/staged/ask-ai-modal handler reads it
# inside its body and the local-only definition was a NameError waiting
# to fire (it did, in production).
_ASK_AI_MAX_ATTEMPTS = 2

router = APIRouter()


def _redirect_to_list(
    request: Request,
    *,
    source: str | None = None,
    message: str | None = None,
    next_path: str | None = None,
):
    """Build a redirect after a /review action.

    Default target is ``/review?source=…&message=…``. ``next_path``
    lets /card etc. divert the redirect to its own surface so the same
    action endpoints serve every consumer.

    HTMX-safe: delegates to ``_htmx.redirect`` so HTMX requests get a
    204 + ``HX-Redirect`` (client-side nav) instead of a 303 that the
    shim's underlying fetch auto-follows. Without this, the
    destination page (``/review`` or ``/card``) gets outerHTML-swapped
    into the action's hx-target — the layout-nesting bug ADR-0037 +
    routes/CLAUDE.md mandate against."""
    base = (next_path or "/review").strip() or "/review"
    parts: list[str] = []
    if source and base == "/review":
        parts.append(f"source={source}")
    if message:
        parts.append(f"message={message}")
    qs = "&".join(parts)
    sep = "&" if "?" in base else "?"
    target = base + (f"{sep}{qs}" if qs else "")
    return _htmx.redirect(request, target)


# ─── Transfer-heuristic helpers ────────────────────────────────────
#
# A staged row "looks like a transfer" when its narration mentions
# transfer/xfer keywords AND the matcher hasn't paired it yet. Surfaced
# as a hint band on the row + as a soft filter on /review
# (toggle "hide transfers") + as a hard exclusion on /card (only show
# transfer-looking rows when literally nothing else is pending).
#
# The detection is plain-text on payee / description so it stays in
# the route layer without an extra DB column.

# The transfer-suspect heuristic is defined once in
# lamella.core.transfer_heuristic and reused here, in the AI
# cascade enricher, and in the synchronous Ask-AI path. The shared
# regex covers literal "transfer" / "xfer" plus liability-payment
# language (credit card payment, online pmt, "to <Bank> credit
# account payment", etc.) — the broader set was needed because
# bank narrations like "To Mercury Credit Account Payment" are
# unambiguously transfers but didn't match the original
# `\b(transfer|xfer)\b` regex, letting AI confidently misclassify
# them as Expenses (user-reported bug).
from lamella.core.transfer_heuristic import (
    looks_like_transfer_item as _looks_like_transfer,
)


def _is_balance_sheet_target(account: str | None) -> bool:
    """ADR-0046: True when ``account`` is an ``Assets:`` /
    ``Liabilities:`` path — i.e. a balance-sheet account that can be
    the receiving leg of a transfer. Expenses / Income / Equity targets
    fall through to normal classify (no synthetic-counterpart marking)."""
    if not account:
        return False
    return account.startswith("Assets:") or account.startswith("Liabilities:")


def _should_emit_synthetic_counterpart(
    item, target_account: str, card_kind: str | None,
) -> bool:
    """ADR-0046 Phase 1 trigger: True when the user is classifying a
    transfer-suspect single-leg row to an Assets:/Liabilities: target.

    Both predicates must agree:
      * ``target_account`` is a balance-sheet account (Assets:/Liab:)
      * the row matches ``_looks_like_transfer`` (transfer narration
        OR single-leg liability payment)

    When True, the writer stamps the destination posting with the
    four lamella-synthetic-* meta keys so the matcher (Phase 2+) can
    recognize and replace the leg when the real other half arrives.
    """
    if not _is_balance_sheet_target(target_account):
        return False
    return _looks_like_transfer(item, card_kind=card_kind)


def _build_row_extras(
    conn: sqlite3.Connection, items
) -> dict[int, dict]:
    """For each StagingReviewItem, resolve the card account it was
    paid on and the owning entity, then attach human-readable
    display strings via ``account_label`` / entities.display_name.
    Returns ``{staged_id: {card_account, card_display, card_secondary,
    entity_slug, entity_display}}``.

    The review UI is unusable without these fields — a row's payee
    + amount alone don't tell the user *which card* paid or *whose
    books* it should land in. Resolved in the route so the template
    stays declarative."""
    from lamella.core.registry.alias import account_label
    out: dict[int, dict] = {}
    for item in items:
        card_account = _resolve_account_path(
            conn, item.source, item.source_ref,
        )
        card_display = ""
        card_secondary = ""
        card_kind: str | None = None
        entity_slug: str | None = None
        entity_display: str | None = None
        if card_account:
            try:
                card_display, card_secondary = account_label(
                    conn, card_account,
                )
            except Exception:  # noqa: BLE001 — display fallback
                card_display = card_account
            try:
                row = conn.execute(
                    "SELECT entity_slug, kind FROM accounts_meta "
                    "WHERE account_path = ?",
                    (card_account,),
                ).fetchone()
                if row and row["entity_slug"]:
                    entity_slug = str(row["entity_slug"])
                if row and row["kind"]:
                    card_kind = str(row["kind"])
            except Exception:  # noqa: BLE001
                pass
            if entity_slug:
                try:
                    erow = conn.execute(
                        "SELECT display_name FROM entities WHERE slug = ?",
                        (entity_slug,),
                    ).fetchone()
                    if erow and erow["display_name"]:
                        entity_display = str(erow["display_name"])
                    else:
                        entity_display = entity_slug
                except Exception:  # noqa: BLE001
                    entity_display = entity_slug
        # Link to /txn/ via the immutable lamella-txn-id when the
        # row has one — Phase 3 of the immutable-URL invariant: the
        # same URL renders the staged shape pre-promotion and the
        # ledger shape post-promotion. Reboot rows fall back to
        # source_ref_hash (= existing ledger txn_hash) for the small
        # gap of pre-Phase-1 rows that haven't been re-staged.
        txn_link: str | None = None
        if getattr(item, "lamella_txn_id", None):
            txn_link = f"/txn/{item.lamella_txn_id}"
        elif item.source == "reboot" and item.source_ref_hash:
            # source_ref_hash is the canonical txn_hash for reboot.
            txn_link = f"/txn/{item.source_ref_hash}"
        # Most-recent AI decision against this row's identifier, if
        # any. SimpleFIN rows: the decision's input_ref is the
        # SimpleFIN id from source_ref. Reboot rows: input_ref is
        # the txn_hash (= source_ref_hash). Both join cleanly to
        # ai_decisions.
        ai_decision_id: int | None = None
        ai_input_ref: str | None = None
        if item.source == "simplefin" and isinstance(item.source_ref, dict):
            ai_input_ref = item.source_ref.get("txn_id")
        elif item.source == "reboot":
            ai_input_ref = item.source_ref_hash
        if ai_input_ref:
            try:
                arow = conn.execute(
                    "SELECT id FROM ai_decisions "
                    " WHERE decision_type = 'classify_txn' "
                    "   AND input_ref = ? "
                    " ORDER BY decided_at DESC LIMIT 1",
                    (ai_input_ref,),
                ).fetchone()
                if arow:
                    ai_decision_id = int(arow["id"])
            except Exception:  # noqa: BLE001
                pass
        out[item.staged_id] = {
            "card_account": card_account or "",
            "card_display": card_display or "",
            "card_secondary": card_secondary or "",
            "card_kind": card_kind or "",
            "entity_slug": entity_slug or "",
            "entity_display": entity_display or "",
            "txn_link": txn_link,
            "ai_decision_id": ai_decision_id,
        }
    return out


_GROUPS_PER_PAGE = 25


def _aggregate_group_proposal(group) -> dict | None:
    """Roll the per-row AI / rule proposals up to the group level.

    Each row in a group MAY carry a ``proposed_account`` (the
    staged_decisions.account string for that row). This helper
    inspects every row in ``group.items`` and decides whether the
    group as a whole has a coherent proposal:

      * **None of the rows have a usable proposal** → returns
        ``None``. The template renders nothing.
      * **All proposals agree** (same target account on every
        row that has one) → returns
        ``{"kind": "consensus", "account": ..., "covers": N,
           "total": M}``. The template surfaces an "AI proposed X
        for N/M rows · Apply to all" CTA wired into the existing
        classify-group bulk action.
      * **Proposals disagree** (rows split between two or more
        target accounts) → returns
        ``{"kind": "conflict", "options": [...sorted unique...],
           "counts": {acct: n, ...}}``. The template surfaces a
        warning band; no auto-apply CTA (the user has to decide).

    Proposals that look like ``Expenses:FIXME`` /
    ``Equity:Uncategorized`` are filtered out — those are
    placeholder targets, not real classification suggestions.
    """
    # Collect (target, confidence_bucket, rationale, decided_by)
    # tuples per row that has a usable proposal so the aggregator
    # can surface the strongest evidence to the user — not just
    # "what was proposed" but "how confident the AI was AND why".
    _CONF_RANK = {"high": 3, "medium": 2, "low": 1}
    rows: list[dict] = []
    for item in group.items:
        target = getattr(item, "proposed_account", None)
        if not target:
            continue
        upper = target.upper()
        if "FIXME" in upper or "Uncategorized" in target:
            continue
        rows.append({
            "account": target,
            "confidence": (getattr(item, "proposed_confidence", None) or "").lower(),
            "rationale": getattr(item, "proposed_rationale", None) or "",
            "decided_by": (getattr(item, "proposed_by", None) or "").lower(),
        })
    if not rows:
        return None

    def _best(rs: list[dict]) -> dict:
        """Pick the row with the highest confidence, breaking ties by
        the existence of a rationale (a low-conf with reasoning beats
        a low-conf with nothing). Returns the chosen dict."""
        return max(
            rs,
            key=lambda r: (
                _CONF_RANK.get(r["confidence"], 0),
                1 if r["rationale"] else 0,
            ),
        )

    targets = [r["account"] for r in rows]
    unique = set(targets)
    if len(unique) == 1:
        only = next(iter(unique))
        best = _best(rows)
        return {
            "kind": "consensus",
            "account": only,
            "covers": len(rows),
            "total": group.size,
            "confidence": best["confidence"] or None,
            "rationale": best["rationale"] or None,
            "decided_by": best["decided_by"] or None,
        }
    counts = Counter(targets)
    # Per-option summary so the conflict band can show confidence +
    # reasoning per choice. Sorted by row-count desc, ties broken by
    # confidence desc.
    options: list[dict] = []
    for acct in sorted(
        unique,
        key=lambda a: (-counts[a], -_CONF_RANK.get(_best([r for r in rows if r["account"] == a])["confidence"], 0)),
    ):
        best = _best([r for r in rows if r["account"] == acct])
        options.append({
            "account": acct,
            "count": counts[acct],
            "confidence": best["confidence"] or None,
            "rationale": best["rationale"] or None,
            "decided_by": best["decided_by"] or None,
        })
    return {
        "kind": "conflict",
        "options": options,
        # Backward-compat: old templates indexed counts by account
        # string. Keep that map alongside the new options[] list.
        "counts": dict(counts),
        "covers": len(rows),
        "total": group.size,
    }


def _staged_list_context(
    conn: sqlite3.Connection,
    *,
    source: str | None = None,
    hide_transfers: bool = False,
    sort: str = "groups",
    page: int = 1,
    entries: list | None = None,
) -> dict:
    """Assemble data for both the full-page render and the HTMX
    partial-swap path. Single source of truth so /review and the
    legacy /review/staged URL stay in lockstep.

    ``hide_transfers`` filters out transfer-flagged rows server-side
    (the matcher handles those; surfacing them clutters the queue).
    Counts the hidden total separately so the toggle knows what it
    will reveal.

    ``sort`` controls group ordering:
      - ``groups`` (default): group size desc — biggest blast-radius first
      - ``amount``:           group total desc — largest dollar first
      - ``date``:             most-recent posting date desc

    ``page`` is 1-indexed; up to ``_GROUPS_PER_PAGE`` groups per page."""
    items = list_pending_items(conn, source=source)

    # Build extras up-front so the transfer-hint check has access to
    # each row's source-account kind (credit_card / loan / line_of_credit
    # / mortgage). Single-leg liability payments get flagged as
    # transfers regardless of narration.
    all_extras = _build_row_extras(conn, items)

    def _is_transfer(it) -> bool:
        ex = all_extras.get(it.staged_id, {})
        return _looks_like_transfer(it, card_kind=ex.get("card_kind") or None)

    # Count the transfer-flagged rows BEFORE filtering so the toggle
    # label can preview what's hidden ("Hide N transfers").
    transfer_count = sum(1 for it in items if _is_transfer(it))
    visible_items = (
        [it for it in items if not _is_transfer(it)]
        if hide_transfers else items
    )

    extras = {
        it.staged_id: all_extras.get(it.staged_id, {})
        for it in visible_items
    }
    groups = group_staged_rows(visible_items)

    if sort == "amount":
        groups.sort(
            key=lambda g: (
                -sum((it.amount for it in g.items), Decimal("0")),
                g.prototype.staged_id,
            ),
        )
    elif sort == "date":
        groups.sort(
            key=lambda g: max(it.posting_date for it in g.items),
            reverse=True,
        )
    else:  # "groups" — default
        groups.sort(key=lambda g: (-g.size, g.prototype.staged_id))

    total_groups = len(groups)
    total_pages = max(
        1, (total_groups + _GROUPS_PER_PAGE - 1) // _GROUPS_PER_PAGE,
    )
    page = max(1, min(page, total_pages))
    start = (page - 1) * _GROUPS_PER_PAGE
    page_groups = groups[start:start + _GROUPS_PER_PAGE]

    # Group-level proposal aggregation. The template surfaces the
    # consensus / conflict band on the group header — if every row
    # in the group has the same AI suggestion, "Apply to all" is a
    # one-click commit to the whole bucket. Keyed by prototype
    # staged_id so the template can pull it up by group.
    group_proposals: dict[int, dict] = {}
    # Group-level transfer-suspect signal — when most rows in a
    # group are flagged "looks like a transfer" by the same heuristic
    # the per-row hint uses, the group itself shouldn't be one-click
    # classified into Expenses. The template surfaces a warning band
    # on these groups and adds a confirm-prompt to any Expenses-target
    # Apply-to-all action on them. Per user feedback: "the group
    # should also have the notice saying these look like transfers"
    # and "It should not allow you to just one click accept ... without
    # a confirmation warning".
    group_transfer_suspect: dict[int, dict] = {}
    for g in page_groups:
        prop = _aggregate_group_proposal(g)
        if prop is not None:
            group_proposals[g.prototype.staged_id] = prop
        flagged = 0
        for item in g.items:
            row_extras = extras.get(item.staged_id, {}) or {}
            card_kind = row_extras.get("card_kind")
            if _looks_like_transfer(item, card_kind):
                flagged += 1
        # Majority rule (>=50%) + minimum-2 floor so a single
        # anomalous row in a 100-row group doesn't trip the warning.
        # In practice transfer-suspect groups are 90%+ flagged because
        # the heuristic matches on narration + source-account-kind
        # and similar rows cluster together.
        if flagged >= max(2, (g.size + 1) // 2):
            group_transfer_suspect[g.prototype.staged_id] = {
                "flagged": flagged,
                "total": g.size,
            }

    total = count_pending_items(conn)
    by_source = {
        s: count_pending_items(conn, source=s)
        for s in ("simplefin", "csv", "paste", "reboot")
    }

    # Suggestion cards — the same observed-state nudges the dashboard
    # surfaces. /review is the highest-attention surface for them
    # because the user is already in classify-mode and a "this looks
    # like a payout source" prompt converts directly to an action.
    # The route passes through real ledger entries so the detector
    # can filter dismissed candidates; passing [] still works (the
    # detector just won't see dismissals).
    suggestion_cards: list = []
    try:
        from lamella.features.review_queue.suggestions import build_suggestion_cards
        if total > 0:
            suggestion_cards = build_suggestion_cards(
                conn, entries or [], context="global",
            )
    except Exception:  # noqa: BLE001
        log.exception("/review build_suggestion_cards failed")
        suggestion_cards = []

    return {
        "items": visible_items,
        "groups": page_groups,
        "extras": extras,
        "group_proposals": group_proposals,
        "group_transfer_suspect": group_transfer_suspect,
        "filter_source": source,
        "hide_transfers": hide_transfers,
        "transfer_count": transfer_count,
        "sort": sort,
        "page": page,
        "total_pages": total_pages,
        "total_groups": total_groups,
        "groups_per_page": _GROUPS_PER_PAGE,
        "total": total,
        "by_source": by_source,
        "suggestion_cards": suggestion_cards,
        "message": None,
    }


@router.get("/review/staged", response_class=HTMLResponse)
def staged_review_page(
    request: Request,
    source: str | None = None,
):
    """Back-compat redirect. /review is the canonical URL now."""
    qs = f"?source={source}" if source else ""
    return RedirectResponse(f"/review{qs}", status_code=301)


@router.post("/review/staged/dismiss", response_class=HTMLResponse)
def staged_review_dismiss(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    staged_id: int = Form(...),
    reason: str = Form(default=""),
    source: str | None = Form(default=None),
    next_path: str | None = Form(default=None),
):
    """Mark a staged row as ignored. Reversible via /review/ignored
    or POST /review/staged/restore. Drops off the pending list;
    downstream pair records are unaffected."""
    svc = StagingService(conn)
    svc.dismiss(staged_id, reason=reason or "ignored from review")
    conn.commit()
    return _redirect_to_list(
        request,
        source=source, message=f"ignored_{staged_id}", next_path=next_path,
    )


@router.post(
    "/review/staged/{staged_id}/promote-synthetic",
    response_class=HTMLResponse,
)
def staged_review_promote_synthetic(
    request: Request,
    staged_id: int,
    conn: sqlite3.Connection = Depends(get_db),
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
    next_path: str | None = Form(default=None),
):
    """ADR-0046 Phase 3b — confirm a wrong-account synthetic match.

    Reads ``synthetic_match_meta`` off the staged row, calls
    ``rewrite_synthetic_account_in_place`` to flip the synthetic
    posting from ``wrong_account`` → ``right_account`` AND swap
    synthetic-* meta for paired source meta in one pass, then marks
    the staged row promoted (the row has been absorbed into the
    existing transaction).

    Failure modes:
      * marker missing / row not found → 404
      * helper finds no matching block → 409 (the ledger moved under us)
      * bean-check fails after rewrite → 500, snapshot/restore handled
        by the caller's snapshot wrapper (TODO once the wrapper is
        unified across phases).
    """
    from lamella.features.bank_sync.synthetic_replace import (
        rewrite_synthetic_account_in_place,
    )

    svc = StagingService(conn)
    row = svc.get(staged_id)
    if row is None:
        raise HTTPException(status_code=404, detail="staged row not found")

    raw_marker = conn.execute(
        "SELECT synthetic_match_meta FROM staged_transactions WHERE id = ?",
        (staged_id,),
    ).fetchone()
    if raw_marker is None or not raw_marker["synthetic_match_meta"]:
        raise HTTPException(
            status_code=404,
            detail="row carries no synthetic-match marker",
        )
    try:
        marker = json.loads(raw_marker["synthetic_match_meta"])
    except (TypeError, ValueError):
        raise HTTPException(
            status_code=500, detail="synthetic_match_meta corrupted",
        )

    lamella_txn_id = marker.get("lamella_txn_id")
    wrong_account = marker.get("wrong_account")
    right_account = marker.get("right_account")
    if not (lamella_txn_id and wrong_account and right_account):
        raise HTTPException(
            status_code=500,
            detail="synthetic_match_meta missing required fields",
        )

    # Resolve the source-reference id from the row's source_ref blob
    # so the synthetic→source meta swap stamps the right id.
    source = row.source
    source_reference_id: str = ""
    try:
        ref = json.loads(row.source_ref) if isinstance(row.source_ref, str) else (row.source_ref or {})
        if source == "simplefin":
            source_reference_id = str(ref.get("txn_id") or "")
    except (TypeError, ValueError):
        source_reference_id = ""
    if not source_reference_id:
        raise HTTPException(
            status_code=500,
            detail="cannot resolve source_reference_id from staged row",
        )

    bean_file = settings.ledger_main.parent / "simplefin_transactions.bean"
    rewrote = rewrite_synthetic_account_in_place(
        bean_file=bean_file,
        lamella_txn_id=lamella_txn_id,
        wrong_account=wrong_account,
        right_account=right_account,
        source=source,
        source_reference_id=source_reference_id,
    )
    if not rewrote:
        raise HTTPException(
            status_code=409,
            detail=(
                "couldn't locate synthetic posting on "
                f"{wrong_account!r} for txn {lamella_txn_id!r} — "
                "the ledger may have been edited since detection"
            ),
        )

    # Mark the staged row promoted (absorbed) and clear the marker.
    conn.execute(
        "UPDATE staged_transactions "
        "SET status = 'promoted', synthetic_match_meta = NULL, "
        "    updated_at = ? "
        "WHERE id = ?",
        (
            _now_iso(),
            staged_id,
        ),
    )
    conn.commit()
    try:
        reader.invalidate()
    except Exception:  # noqa: BLE001
        pass

    response = _htmx.redirect(
        request,
        next_path or f"/review?message=synthetic_rewrote_{staged_id}",
    )
    # Trigger same event the classify path emits so any open detail
    # panes / row swappers refresh.
    response.headers["HX-Trigger"] = "lamella:txn-classified"
    return response


@router.post(
    "/review/staged/{staged_id}/clear-synthetic-marker",
    response_class=HTMLResponse,
)
def staged_review_clear_synthetic_marker(
    request: Request,
    staged_id: int,
    conn: sqlite3.Connection = Depends(get_db),
    next_path: str | None = Form(default=None),
):
    """ADR-0046 Phase 3b — "No, classify normally" branch.

    Clears the loose-match marker without rewriting the ledger. The
    row falls back to the standard classify flow on the next /review
    interaction. Idempotent."""
    conn.execute(
        "UPDATE staged_transactions "
        "SET synthetic_match_meta = NULL, updated_at = ? "
        "WHERE id = ?",
        (_now_iso(), staged_id),
    )
    conn.commit()
    return _htmx.redirect(
        request,
        next_path or f"/review?message=synthetic_marker_cleared_{staged_id}",
    )


def _now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@router.post("/review/staged/restore", response_class=HTMLResponse)
def staged_review_restore(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    staged_id: int = Form(...),
    next_path: str | None = Form(default=None),
):
    """Bring an ignored row back to ``pending``. Reverses
    :func:`staged_review_dismiss`."""
    svc = StagingService(conn)
    svc.restore(staged_id)
    conn.commit()
    target = (next_path or "").strip() or "/review/ignored"
    sep = "&" if "?" in target else "?"
    return _htmx.redirect(
        request,
        f"{target}{sep}message=restored_{staged_id}",
    )


@router.get("/review/ignored", response_class=HTMLResponse)
def staged_review_ignored(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
):
    """Surface every ignored staged row so the user can review what
    was hidden and restore any back to pending."""
    svc = StagingService(conn)
    rows = svc.list_by_status("dismissed", limit=500)
    items = []
    for r in rows:
        decision = svc.get_decision(r.id)
        rationale = decision.rationale if decision else None
        items.append({
            "staged_id": r.id,
            "posting_date": r.posting_date,
            "amount": r.amount,
            "currency": r.currency,
            "payee": r.payee,
            "description": r.description,
            "memo": r.memo,
            "source": r.source,
            "reason": rationale,
            "updated_at": r.updated_at,
        })
    msg = request.query_params.get("message", "")
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request, "staging_ignored.html",
        {"items": items, "message": msg},
    )


def _resolve_account_path(
    conn: sqlite3.Connection, source: str, source_ref: dict,
) -> str | None:
    """Look up the ledger account_path backing a staged row."""
    if not isinstance(source_ref, dict):
        return None
    if source == "simplefin":
        account_id = source_ref.get("account_id")
        if not account_id:
            return None
        row = conn.execute(
            "SELECT account_path FROM accounts_meta "
            "WHERE simplefin_account_id = ?",
            (str(account_id),),
        ).fetchone()
        if row and row["account_path"]:
            return str(row["account_path"])
    elif isinstance(source_ref.get("account_path"), str):
        return source_ref["account_path"]
    return None


def _resolve_card_kind(
    conn: sqlite3.Connection, account_path: str | None,
) -> str | None:
    """Look up the ``accounts_meta.kind`` for a resolved account path.

    ADR-0046 detection needs this so single-leg liability payments
    (credit_card / line_of_credit / loan / mortgage) can be flagged
    as transfer-suspect even when their narration lacks the word
    ``transfer`` (banks rarely use it on the liability-side row)."""
    if not account_path:
        return None
    try:
        row = conn.execute(
            "SELECT kind FROM accounts_meta WHERE account_path = ?",
            (account_path,),
        ).fetchone()
    except Exception:  # noqa: BLE001
        return None
    if row and row["kind"]:
        return str(row["kind"])
    return None


from lamella.core.registry.account_open_guard import (
    check_account_open_on as _check_account_open_on,
    ensure_target_account_open as _ensure_target_account_open,
)
from lamella.core.registry.service import (
    InvalidAccountSegmentError,
    validate_beancount_account,
)


@router.post("/review/staged/classify", response_class=HTMLResponse)
def staged_review_classify(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    settings: Settings = Depends(get_settings),
    staged_id: int = Form(...),
    target_account: str | None = Form(default=None),
    accept_proposed: str | None = Form(default=None),
    source: str | None = Form(default=None),
    next_path: str | None = Form(default=None),
    refund_of_txn_id: str | None = Form(default=None),
):
    """Write a clean classified Beancount transaction for one
    pending staged row. No FIXME leg. Flips the staged row
    'promoted' on success.

    Two modes:
      * ``accept_proposed=1`` — use the row's existing
        staged_decision.account (the rule / AI suggestion).
      * ``target_account`` explicitly set — override with the
        user's pick.

    Only SimpleFIN rows are supported in this first pass. Paste /
    CSV / reboot rows stay with their existing commit paths until
    their own classify-from-staging writers land.
    """
    svc = StagingService(conn)
    row = svc.get(staged_id)
    if row is None:
        raise HTTPException(status_code=404, detail="staged row not found")
    if row.status == "promoted":
        return _redirect_to_list(
            request,
            source=source, message="already_promoted", next_path=next_path,
        )

    # Resolve the target account.
    target = (target_account or "").strip() or None
    if not target and accept_proposed:
        dec = conn.execute(
            "SELECT account FROM staged_decisions WHERE staged_id = ?",
            (staged_id,),
        ).fetchone()
        if dec and dec["account"]:
            proposed = str(dec["account"])
            # Skip proposals that are still FIXME-ish.
            if "FIXME" not in proposed.upper():
                target = proposed
    if not target:
        raise HTTPException(
            status_code=400,
            detail="target_account required (or row has no acceptable proposal)",
        )

    # ADR-0045: validate the destination path before any write hits
    # the ledger. Catches "eBay" / "1stStreet" / "X" leaves that
    # bean-check would reject post-write, leaving a half-written file.
    # ADR-0042: also enforce the entity-first rule (segment 1 must be
    # a registered entity slug) so paths like Expenses:Vehicles:Foo
    # surface a 400 here instead of a 500 from bean-check downstream.
    try:
        validate_beancount_account(target)
        from lamella.core.registry.service import (
            validate_entity_first_path,
        )
        _entity_slugs = frozenset(
            r["slug"] for r in conn.execute("SELECT slug FROM entities")
        )
        validate_entity_first_path(target, _entity_slugs)
    except InvalidAccountSegmentError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    # Resolve the source account path.
    source_account = _resolve_account_path(conn, row.source, row.source_ref)
    if not source_account:
        raise HTTPException(
            status_code=400,
            detail=f"can't resolve backing account for source={row.source} "
                   f"ref={row.source_ref}",
        )

    # Pre-write validation: target account must be open on the
    # txn date. Catches the case where setup-progress just opened
    # an account today (2026-04-24) but the user is classifying a
    # row from before that — bean-check would reject the write
    # AFTER it lands and break the ledger; a pre-flight check
    # produces a clean 400 instead.
    txn_date_for_check = _date.fromisoformat(row.posting_date[:10])
    ext_err = _ensure_target_account_open(
        reader, settings, target, txn_date_for_check,
    )
    if ext_err:
        raise HTTPException(status_code=400, detail=ext_err)
    open_err = _check_account_open_on(
        reader, target, txn_date_for_check, settings=settings,
    )
    if open_err:
        raise HTTPException(status_code=400, detail=open_err)

    if row.source == "reboot":
        # Reboot rows are scanned out of historical bean files —
        # the txn already exists in the ledger with a FIXME posting.
        # Write an override rerouting that FIXME to the user's pick;
        # don't append a new clean transaction.
        result_path = _classify_reboot_row(
            conn=conn, reader=reader, settings=settings,
            row=row, target_account=target,
        )
        try:
            svc.mark_promoted(staged_id, promoted_to_file=str(result_path))
        except Exception as exc:  # noqa: BLE001
            log.warning("staged classify: mark_promoted failed: %s", exc)
        reader.invalidate()
        conn.commit()
        return _redirect_to_list(
            request,
            source=source,
            message=f"classified_{staged_id}_to_{target}",
            next_path=next_path,
        )

    if row.source != "simplefin":
        raise HTTPException(
            status_code=501,
            detail=f"classify-from-staging for source={row.source} "
                   "not yet implemented — use the existing commit flow",
        )

    # Build the clean categorized PendingEntry.
    txn_id = row.source_ref.get("txn_id") or row.source_ref_hash
    # ADR-0046 Phase 1: when the row is transfer-suspect AND the
    # user picked an Assets:/Liab: target, mark the destination leg
    # synthetic so the matcher can replace it when the real other
    # half arrives.
    card_kind = _resolve_card_kind(conn, source_account)
    is_synthetic = _should_emit_synthetic_counterpart(
        row, target, card_kind,
    )
    # Refund-of: stripped/validated form value. None semantics =
    # ordinary deposit (no link); a UUIDv7-shaped string means the
    # user accepted a refund-of-expense candidate. The writer renders
    # ``lamella-refund-of: "<value>"`` at txn-meta when set.
    refund_of = (refund_of_txn_id or "").strip() or None
    # ADR-0059 promotion-path synthesis: build the canonical
    # narration from the staged row's per-source description before
    # the entry is rendered. Today the staged row is one source's
    # observation; the synthesizer picks longest-non-empty
    # description, falls back to payee, falls back to a placeholder.
    # This unifies the narration shape across the confirm-as-dup
    # path (already wired) and the promote path (this code), so
    # every new ledger entry gets a deterministic narration line
    # plus the ``lamella-narration-synthesized: TRUE`` marker.
    from lamella.features.ai_cascade.narration_synthesizer import (
        DeterministicNarrationSynthesizer,
        SourceObservation,
        build_synthesis_input,
    )
    _staged_description = row.description or row.memo
    _synth_input = build_synthesis_input(
        signed_amount=Decimal(row.amount),
        currency=row.currency or "USD",
        source_account=source_account,
        target_account=target,
        observations=[
            SourceObservation(
                source=(row.source or "simplefin"),
                reference_id=str(txn_id) if txn_id else None,
                description=_staged_description,
                payee=row.payee,
            ),
        ],
        existing_narration=_staged_description,
    )
    _synth_result = DeterministicNarrationSynthesizer().synthesize(_synth_input)
    _synth_narration = _synth_result.narration
    entry = PendingEntry(
        date=_date.fromisoformat(row.posting_date[:10]),
        simplefin_id=str(txn_id),
        payee=row.payee,
        narration=_synth_narration,
        amount=Decimal(row.amount),
        currency=row.currency or "USD",
        source_account=source_account,
        target_account=target,
        ai_classified=False,
        staged_id=staged_id,
        lamella_txn_id=row.lamella_txn_id,
        synthetic_kind=(
            "user-classified-counterpart" if is_synthetic else None
        ),
        synthetic_confidence="guessed" if is_synthetic else None,
        synthetic_replaceable=True,
        refund_of_txn_id=refund_of,
        source_description=_staged_description,
    )

    writer = SimpleFINWriter(
        main_bean=settings.ledger_main,
        simplefin_path=settings.simplefin_transactions_path,
    )
    # ADR-0043 P5 — when staged-txn directives are enabled and this
    # row's directive is in the ledger, promote-in-place (replace
    # the directive with staged-txn-promoted + append a balanced
    # txn under one bean-check pass). Falls back to plain append
    # for rows that staged before the flag was turned on (no
    # directive to find — the directive write was skipped at
    # ingest).
    use_promotion = bool(
        getattr(settings, "enable_staged_txn_directives", False)
        and entry.lamella_txn_id
    )
    try:
        if use_promotion:
            from lamella.features.bank_sync.writer import (
                StagedDirectiveNotFoundError,
            )
            try:
                writer.promote_staged_txn(
                    promoted_entry=entry,
                    promoted_by="manual",
                    source="simplefin",
                )
            except StagedDirectiveNotFoundError:
                # Pre-flag-on row — no directive in the ledger. Fall
                # back to plain append for parity with v0.3.0
                # behaviour.
                writer.append_entries([entry])
        else:
            writer.append_entries([entry])
    except Exception as exc:  # noqa: BLE001
        log.warning("staged classify: writer failed for %s: %s", staged_id, exc)
        raise HTTPException(
            status_code=500,
            detail=f"failed to write classified entry: {exc}",
        )

    # Promote the staged row.
    try:
        svc.mark_promoted(
            staged_id,
            promoted_to_file=str(settings.simplefin_transactions_path),
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("staged classify: mark_promoted failed: %s", exc)

    # ADR-0059 — stamp the synthesizer marker on the freshly-written
    # entry. The narration is already the synthesized value (set
    # above); rewrite_narration_in_place is a no-op on the header but
    # adds the txn-meta line ``lamella-narration-synthesized: TRUE``
    # so future synthesis passes know this narration is theirs to
    # rewrite. Best-effort — don't fail the classify if marker write
    # crashes (e.g. file locked, rare race).
    if entry.lamella_txn_id and _synth_narration:
        try:
            from pathlib import Path as _Path
            from lamella.features.bank_sync.synthetic_replace import (
                rewrite_narration_in_place,
            )
            rewrite_narration_in_place(
                bean_file=_Path(settings.simplefin_transactions_path),
                lamella_txn_id=entry.lamella_txn_id,
                new_narration=_synth_narration,
                mark_synthesized=True,
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "staged classify: narration-synthesized marker failed for %s: %s",
                staged_id, exc,
            )

    reader.invalidate()
    conn.commit()
    # HTMX-driven classify: when called from the AI modal (or any other
    # surface that hx-swap="none"s its main response), return the tile
    # via OOB swap so the originating row visibly transitions to the
    # "Classified" state in place — same UX as the inline row Classify
    # button. Each staged-list group section carries data-rsg-staged-ids
    # listing its member staged_ids; the OOB selector matches by that.
    #
    # Falls back to _redirect_to_list for vanilla (non-HTMX) form posts
    # — those need the 303 + Location chain, not OOB.
    if _htmx.is_htmx(request):
        from lamella.core.registry.alias import account_label
        try:
            alias = account_label(conn, target)
        except Exception:  # noqa: BLE001
            alias = target
        templates = request.app.state.templates
        # The OOB-shaped tile: outerHTML swap targeting the group
        # section that contains this staged_id. The shim's processOob
        # honors `hx-swap-oob="outerHTML:<selector>"` and removes the
        # node from the main response body, so the modal's
        # hx-swap="none" still discards everything else.
        oob_html = templates.get_template(
            "partials/_classify_group_done.html"
        ).render({
            "count": 1,
            "account": target,
            "account_alias": alias,
            "undo_url": None,
            "undo_form_data": None,
        })
        # Inject the OOB swap directive on the rendered tile's outer
        # element. The tile's outermost element is <section ...>; we
        # add hx-swap-oob with a selector that matches by the data
        # attribute on the original group section.
        oob_attr = (
            f'hx-swap-oob="outerHTML:'
            f'[data-rsg-staged-ids~=&quot;{staged_id}&quot;]"'
        )
        oob_html = oob_html.replace(
            '<section class="rsg-group rsg-group--done"',
            f'<section class="rsg-group rsg-group--done" {oob_attr}',
            1,
        )
        # Toast confirmation as a second OOB target. Belt-and-suspenders:
        # if the row anchor isn't visible (rare — staged-list renders
        # everything), the toast still confirms the action.
        toast_html = (
            f'<div hx-swap-oob="beforeend:#toast-area">'
            f'<div class="toast toast--ok" role="status" aria-live="polite">'
            f'Classified to <code>{alias}</code></div></div>'
        )
        return HTMLResponse(oob_html + toast_html)
    return _redirect_to_list(
        request,
        source=source,
        message=f"classified_{staged_id}_to_{target}",
        next_path=next_path,
    )


def _classify_reboot_row(
    *,
    conn: sqlite3.Connection,
    reader: LedgerReader,
    settings: Settings,
    row,
    target_account: str,
):
    """Reboot rows were scanned out of existing ledger files — the
    txn is already present with a FIXME posting. Classify by writing
    an OVERRIDE that reroutes that FIXME leg to the user's chosen
    target. No new transaction is appended.

    Locates the underlying ledger entry by source_ref ``file +
    lineno`` (the reboot scanner stamps both). Falls back to
    matching by ``source_ref_hash`` against ``txn_hash`` if file +
    lineno don't resolve cleanly. Raises HTTPException(404) when
    the row can't be matched to any ledger transaction."""
    from beancount.core.data import Transaction
    from lamella.core.beancount_io.txn_hash import txn_hash
    from decimal import Decimal as _D
    entries = reader.load().entries
    target_file = (
        row.source_ref.get("file")
        if isinstance(row.source_ref, dict) else None
    )
    target_lineno = (
        row.source_ref.get("lineno")
        if isinstance(row.source_ref, dict) else None
    )
    matched_txn = None
    for e in entries:
        if not isinstance(e, Transaction):
            continue
        meta = e.meta or {}
        if (
            target_file
            and target_lineno
            and str(meta.get("filename", "")).endswith(
                str(target_file).lstrip("/")
            )
            and meta.get("lineno") == target_lineno
        ):
            matched_txn = e
            break
        # Fallback: match by content hash.
        if row.source_ref_hash and txn_hash(e) == row.source_ref_hash:
            matched_txn = e
            break
    if matched_txn is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"reboot row {row.id}: source_ref={row.source_ref} "
                "did not match any ledger transaction; the file may "
                "have been edited or rewritten since the reboot scan"
            ),
        )

    # Identify the FIXME leg + the card / source-account leg.
    fixme_leg = None
    card_leg = None
    for p in matched_txn.postings or ():
        acct = p.account or ""
        if acct.split(":")[-1].upper() == "FIXME":
            fixme_leg = p
        elif acct.startswith(("Assets:", "Liabilities:")):
            card_leg = p
    # When the matched ledger txn has no FIXME leg, it's already
    # classified end-to-end. The reboot writer pulled an existing
    # fully-classified entry into a staged-txn directive (intended
    # for the "migrate legacy FIXMEs" path), but there's nothing
    # left to override. Self-heal: bump the staged row to
    # ``promoted`` so it stops surfacing as pending, and return a
    # success-shaped response telling the user the txn was already
    # classified. This also clears the duplicate-list state where
    # /search showed the txn under both Staged AND Ledger.
    if fixme_leg is None:
        # Self-heal: the underlying ledger entry is already fully
        # classified — there's nothing to override. Mark the staged row
        # promoted and return the matched ledger file as the
        # ``promoted_to_file`` so the caller's normal post-classify
        # redirect runs (mark_promoted + commit + _redirect_to_list).
        # Earlier code tried to short-circuit with its own HTMX/redirect
        # response, but this function doesn't have ``request`` or
        # ``next_path`` in scope — that path raised NameError on every
        # already-classified reboot row. The caller already handles the
        # HTMX-aware redirect after we return.
        from pathlib import Path as _Pheal
        try:
            conn.execute(
                "UPDATE staged_transactions SET status = 'promoted', "
                "promoted_at = COALESCE(promoted_at, datetime('now')), "
                "updated_at = datetime('now') WHERE id = ?",
                (row.id,),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "reboot self-heal failed for staged id=%s: %s",
                row.id, exc,
            )
        existing_targets = [
            p.account for p in matched_txn.postings or ()
            if p.account and p.account.startswith(("Expenses:", "Income:"))
        ]
        existing = (
            ", ".join(existing_targets)
            if existing_targets
            else "(no Expenses/Income leg found)"
        )
        matched_meta_heal = getattr(matched_txn, "meta", None) or {}
        src_file_heal = matched_meta_heal.get("filename")
        log.info(
            "reboot row %s: matched ledger txn already classified to %s; "
            "marked staged row promoted and skipping override.",
            row.id, existing,
        )
        return (
            _Pheal(str(src_file_heal))
            if src_file_heal
            else settings.ledger_main
        )
    if card_leg is None:
        raise HTTPException(
            status_code=400,
            detail=(
                f"reboot row {row.id}: matched ledger txn has a FIXME "
                "leg but no Assets/Liabilities card leg to balance the "
                "override against"
            ),
        )

    from lamella.features.rules.overrides import OverrideWriter
    from lamella.core.ledger_writer import BeanCheckError
    from lamella.core.rewrite.txn_inplace import (
        InPlaceRewriteError,
        rewrite_fixme_to_account,
    )
    from datetime import date as _date2
    from pathlib import Path as _P

    target_hash = txn_hash(matched_txn)
    txn_date = (
        matched_txn.date if isinstance(matched_txn.date, _date2)
        else _date2.fromisoformat(str(matched_txn.date))
    )
    amount = abs(_D(fixme_leg.units.number)) if fixme_leg.units else _D("0")
    currency = (
        fixme_leg.units.currency if fixme_leg.units else "USD"
    ) or "USD"
    writer = OverrideWriter(
        main_bean=settings.ledger_main,
        overrides=settings.connector_overrides_path,
        conn=conn,
    )

    # Per CLAUDE.md "in-place rewrites are the default" — reboot
    # rows reference an existing ledger entry by its file+lineno,
    # so we have everything needed to edit the FIXME posting line
    # in place. Override fallback only kicks in when the path
    # safety check refuses (e.g. archive/reboot subdir) or
    # filename/lineno is missing on the matched txn.
    matched_meta = getattr(matched_txn, "meta", None) or {}
    src_file = matched_meta.get("filename")
    src_lineno = matched_meta.get("lineno")
    fixme_signed = (
        _D(fixme_leg.units.number)
        if fixme_leg.units and fixme_leg.units.number is not None
        else None
    )
    if src_file and src_lineno is not None:
        try:
            try:
                writer.rewrite_without_hash(target_hash)
            except BeanCheckError:
                raise InPlaceRewriteError("override-strip blocked")
            rewrite_fixme_to_account(
                source_file=_P(src_file),
                line_number=int(src_lineno),
                old_account=fixme_leg.account,
                new_account=target_account,
                expected_amount=fixme_signed,
                ledger_dir=settings.ledger_dir,
                main_bean=settings.ledger_main,
            )
            return _P(src_file)
        except InPlaceRewriteError as exc:
            log.info(
                "_classify_reboot_row: in-place refused for %s: %s — "
                "falling back to override",
                target_hash[:12], exc,
            )

    writer.append(
        txn_date=txn_date,
        txn_hash=target_hash,
        amount=amount,
        from_account=fixme_leg.account,
        to_account=target_account,
        currency=currency,
        narration=(
            matched_txn.narration or "reboot classify"
        ),
    )
    return settings.connector_overrides_path


def _build_pending_entry(
    conn: sqlite3.Connection,
    row,
    target_account: str,
) -> PendingEntry:
    """Shared single-row classify payload — for SOURCES that produce
    new clean Beancount transactions (currently just SimpleFIN).
    Reboot-source rows have their own path via
    `_classify_reboot_row` because they override an existing entry
    rather than appending a new one."""
    if row.source != "simplefin":
        raise HTTPException(
            status_code=501,
            detail=f"_build_pending_entry: source={row.source} "
                   "uses a different write path",
        )
    source_account = _resolve_account_path(conn, row.source, row.source_ref)
    if not source_account:
        raise HTTPException(
            status_code=400,
            detail=f"can't resolve backing account for source={row.source} "
                   f"ref={row.source_ref}",
        )
    txn_id = (
        row.source_ref.get("txn_id")
        if isinstance(row.source_ref, dict) else None
    ) or row.source_ref_hash
    # ADR-0046 Phase 1: per-row synthetic-counterpart decision. The
    # group-level decision is the union — every row in the group that
    # individually passes the heuristic AND lands on an Assets:/Liab:
    # target gets the synthetic marker. Rows where the heuristic
    # disagrees with the group's target shape stay unmarked.
    card_kind = _resolve_card_kind(conn, source_account)
    is_synthetic = _should_emit_synthetic_counterpart(
        row, target_account, card_kind,
    )
    return PendingEntry(
        date=_date.fromisoformat(row.posting_date[:10]),
        simplefin_id=str(txn_id),
        payee=row.payee,
        narration=row.description or row.memo,
        amount=Decimal(row.amount),
        currency=row.currency or "USD",
        source_account=source_account,
        target_account=target_account,
        ai_classified=False,
        staged_id=row.id,
        lamella_txn_id=row.lamella_txn_id,
        synthetic_kind=(
            "user-classified-counterpart" if is_synthetic else None
        ),
        synthetic_confidence="guessed" if is_synthetic else None,
        synthetic_replaceable=True,
    )


@router.post("/review/staged/classify-group", response_class=HTMLResponse)
def staged_review_classify_group(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    settings: Settings = Depends(get_settings),
    staged_ids: List[int] = Form(...),
    target_account: str = Form(...),
    source: str | None = Form(default=None),
    next_path: str | None = Form(default=None),
):
    """Apply one target_account to N staged rows in a single action.

    The tier-2 payoff from docs/specs/AI-CLASSIFICATION.md: the user
    confirms ONE decision; `learn_from_decision` records that as a
    user-tier rule so any future (or pre-existing) FIXMEs that
    match the group's pattern auto-apply via `FixmeScanner.scan`
    without another LLM call.

    Contract:
      * Every staged_id in the group gets a clean Beancount
        transaction written (no FIXME leg). All writes land in one
        writer.append_entries call — single bean-check run for the
        whole group.
      * `learn_from_decision` is called **exactly once**, with the
        prototype (first) row's payee/card. The other N-1 rows
        inherit the classification via the rule that call creates;
        they do not each re-record the decision. Bumping hit_count
        per-row would game the confidence scoring — one rule, one
        decision.
    """
    target = target_account.strip()
    if not target:
        raise HTTPException(
            status_code=400, detail="target_account required",
        )
    if not staged_ids:
        raise HTTPException(
            status_code=400, detail="at least one staged_id required",
        )

    # ADR-0045: validate the path before any write hits the ledger.
    # ADR-0042: also enforce entity-first segment 1.
    try:
        validate_beancount_account(target)
        from lamella.core.registry.service import (
            validate_entity_first_path,
        )
        _entity_slugs = frozenset(
            r["slug"] for r in conn.execute("SELECT slug FROM entities")
        )
        validate_entity_first_path(target, _entity_slugs)
    except InvalidAccountSegmentError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    from lamella.features.import_.staging import StagingError
    svc = StagingService(conn)
    rows = []
    for sid in staged_ids:
        try:
            row = svc.get(sid)
        except StagingError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        if row.status == "promoted":
            # Skip already-promoted rows rather than error — the user
            # may have double-submitted or the group may have
            # partially landed from a prior retry.
            continue
        rows.append(row)

    if not rows:
        return _redirect_to_list(
            request,
            source=source,
            message="classify_group_nothing_to_do",
            next_path=next_path,
        )

    # Dispatch by source. SimpleFIN rows append new clean
    # transactions via SimpleFINWriter (one bean-check pass for the
    # batch). Reboot rows already exist in the ledger; each gets a
    # standalone override write via OverrideWriter. Other sources
    # are not yet supported — the per-row 501 in
    # `_build_pending_entry` will surface them.
    simplefin_rows = [r for r in rows if r.source == "simplefin"]
    reboot_rows = [r for r in rows if r.source == "reboot"]
    unknown_rows = [
        r for r in rows if r.source not in ("simplefin", "reboot")
    ]
    if unknown_rows:
        raise HTTPException(
            status_code=501,
            detail=(
                f"classify-group for source(s) "
                f"{sorted({r.source for r in unknown_rows})!r} "
                "not yet implemented"
            ),
        )

    # Pre-write validation: target account must be open on every
    # row's txn date. Catches the case where the user picks an
    # account opened after some of the older rows in the group —
    # bean-check would reject the write and we'd have to roll
    # back per-row, but the cleaner error is up front before any
    # bytes hit the ledger.
    #
    # If the user typed a new account that legitimately extends an
    # existing branch, auto-scaffold it once dated on or before the
    # earliest txn date in the group so every member-row classifies
    # cleanly without a per-row bean-check rollback.
    earliest_td = min(
        _date.fromisoformat(r.posting_date[:10]) for r in rows
    )
    ext_err = _ensure_target_account_open(
        reader, settings, target, earliest_td,
    )
    if ext_err:
        raise HTTPException(status_code=400, detail=ext_err)
    for row in rows:
        td = _date.fromisoformat(row.posting_date[:10])
        err = _check_account_open_on(reader, target, td, settings=settings)
        if err:
            raise HTTPException(
                status_code=400,
                detail=f"row {row.id} dated {td}: {err}",
            )

    if simplefin_rows:
        entries = [
            _build_pending_entry(conn, row, target) for row in simplefin_rows
        ]
        writer = SimpleFINWriter(
            main_bean=settings.ledger_main,
            simplefin_path=settings.simplefin_transactions_path,
        )
        # ADR-0043 P5 — when staged-txn directives are enabled, promote
        # each row in place: the directive flips to staged-txn-promoted
        # and a balanced txn is appended in one bean-check pass per
        # entry. Plain append fallback for rows whose directive isn't
        # in the ledger (staged before the flag flipped).
        use_promotion = bool(
            getattr(settings, "enable_staged_txn_directives", False)
        )
        try:
            if use_promotion:
                from lamella.features.bank_sync.writer import (
                    StagedDirectiveNotFoundError,
                )
                fallback_entries: list = []
                for entry in entries:
                    if not entry.lamella_txn_id:
                        fallback_entries.append(entry)
                        continue
                    try:
                        writer.promote_staged_txn(
                            promoted_entry=entry,
                            promoted_by="manual",
                            source="simplefin",
                        )
                    except StagedDirectiveNotFoundError:
                        fallback_entries.append(entry)
                if fallback_entries:
                    writer.append_entries(fallback_entries)
            else:
                writer.append_entries(entries)
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "classify_group: SimpleFINWriter failed for ids=%s: %s",
                [r.id for r in simplefin_rows], exc,
            )
            raise HTTPException(
                status_code=500,
                detail=f"failed to write classified group (simplefin): {exc}",
            )

    if reboot_rows:
        for row in reboot_rows:
            try:
                _classify_reboot_row(
                    conn=conn, reader=reader, settings=settings,
                    row=row, target_account=target,
                )
            except HTTPException:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "classify_group: reboot override failed id=%d: %s",
                    row.id, exc,
                )
                raise HTTPException(
                    status_code=500,
                    detail=f"failed to override reboot row {row.id}: {exc}",
                )

    # Promote every row that made it into the write.
    for r in rows:
        try:
            promoted_to = (
                str(settings.simplefin_transactions_path)
                if r.source == "simplefin"
                else str(settings.connector_overrides_path)
            )
            svc.mark_promoted(r.id, promoted_to_file=promoted_to)
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "classify_group: mark_promoted(%d) failed: %s", r.id, exc,
            )

    # ONE rule per group. Prototype = first row; its payee/source
    # become the learned pattern. Subsequent rows in the group
    # inherit the outcome via that rule — they don't re-trigger
    # learn_from_decision. See C2.2 notes in AI-IMPLEMENTATION.md.
    prototype = rows[0]
    pattern_value = (prototype.payee or prototype.description or "").strip()
    card_account = _resolve_account_path(
        conn, prototype.source, prototype.source_ref,
    )
    if pattern_value:
        try:
            RuleService(conn).learn_from_decision(
                matched_rule_id=None,
                user_target_account=target,
                pattern_type="merchant_contains",
                pattern_value=pattern_value,
                card_account=card_account,
                create_if_missing=True,
                source="user",
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "classify_group: learn_from_decision failed: %s", exc,
            )

    # Group-size KPI (AI-PLAN.md observability). One line per
    # classify_group invocation; no new table, no dashboard. Read
    # the log on day 1 / week 1 / month 1.
    log.info(
        "classify_group: staged_ids=%d target=%s pattern=%r",
        len(rows), target, pattern_value,
    )

    reader.invalidate()
    conn.commit()

    # C2.3 — "confirm one, next 99 free" scan. The user-rule just
    # created can now auto-apply to any pre-existing FIXME in the
    # ledger that matches the group's pattern. Run the scan once on
    # the invalidated reader so those siblings resolve without
    # another LLM call.
    scan_applied = 0
    try:
        scanner = FixmeScanner(
            reader=reader,
            reviews=ReviewService(conn),
            rules=RuleService(conn),
            override_writer=OverrideWriter(
                main_bean=settings.ledger_main,
                overrides=settings.connector_overrides_path,
                conn=conn,
            ),
        )
        scan_applied = scanner.scan()
        conn.commit()
    except Exception as exc:  # noqa: BLE001
        # The write already landed — scan failure is a
        # non-critical second-order effect. Log and move on.
        log.warning("classify_group: post-write scan failed: %s", exc)

    log.info(
        "classify_group: post-scan enqueued=%d (pre-existing FIXMEs "
        "resolved via the new rule)", scan_applied,
    )

    # In-place HTMX response per ADR-0036/ADR-0037: when the form was
    # submitted via HTMX (hx-target="closest section.rsg-group"), the
    # response replaces just THAT group's section with a same-height
    # "Classified" tile — no full-list re-render, no scroll reset, no
    # layout shift. Vanilla form posts still get the redirect-to-list
    # so the page reload reflects the change.
    if _htmx.is_htmx(request):
        from lamella.core.registry.alias import account_label
        try:
            alias = account_label(conn, target)
        except Exception:  # noqa: BLE001
            alias = target
        return request.app.state.templates.TemplateResponse(
            request,
            "partials/_classify_group_done.html",
            {
                "count": len(rows),
                "account": target,
                "account_alias": alias,
                # Undo endpoint not yet implemented — Phase 2 follow-up.
                # Omit the button until the endpoint exists; rendering
                # a non-functional Undo would be worse than no Undo.
                "undo_url": None,
                "undo_form_data": None,
            },
        )
    return _redirect_to_list(
        request,
        source=source,
        message=f"classify_group_{len(rows)}_to_{target}",
        next_path=next_path,
    )


# /review/staged/ask-ai (synchronous variant) retired — every UI
# consumer now uses POST /api/txn/staged:<id>/ask-ai which
# returns the job-modal partial. The blocking sync handler
# was a Phase B2 leftover.

@router.post("/review/staged/ask-ai-modal", response_class=HTMLResponse)
def staged_review_ask_ai_modal(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    settings: Settings = Depends(get_settings),
    staged_id: int = Form(...),
    context_hint: str | None = Form(default=None),
    rejection_reason: str | None = Form(default=None),
    attempt: int = Form(default=1),
    source: str | None = Form(default=None),
):
    """Submit an AI classification job for one staged row, return
    the job-modal partial. See header comment for the full flow."""
    from lamella.features.import_.staging import StagingError

    svc = StagingService(conn)
    try:
        row = svc.get(staged_id)
    except StagingError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    if row.status == "promoted":
        raise HTTPException(
            status_code=409,
            detail="row already promoted — close the modal and refresh",
        )

    runner = getattr(request.app.state, "job_runner", None)
    if runner is None:
        raise HTTPException(status_code=503, detail="job runner not ready")

    templates = request.app.state.templates
    blocked = attempt > _ASK_AI_MAX_ATTEMPTS

    # Build the worker as a closure over the request-scoped conn /
    # reader / settings. The threadpool runs us off-request, so any
    # writes go through short-lived connections via JobContext (we
    # just need reads for this worker; conn.execute on the same
    # connection from a worker thread is ok with WAL mode).
    payee_label = (row.payee or row.description or f"row #{staged_id}")[:50]
    title = f"Asking AI · {payee_label}"
    if attempt > 1:
        title = f"Retry #{attempt - 1} · {payee_label}"

    def _render_terminal(proposal: dict | None, *, blocked_flag: bool) -> str:
        # Mirror the unified /api/txn/{ref}/ask-ai context shape so
        # the same template renders correctly whichever endpoint
        # produced the modal. This staged-only legacy endpoint sets
        # mode="staged"; the unified endpoint sets mode="ledger" or
        # "staged" based on the parsed ref.
        return templates.get_template(
            "partials/_ask_ai_result.html"
        ).render({
            "mode": "staged",
            "ref": f"staged:{staged_id}",
            "staged_id": staged_id,
            "txn_hash": None,
            "proposal": proposal,
            "attempt": attempt,
            "reason": rejection_reason,
            "blocked": blocked_flag,
            "return_url": (
                f"/review?source={source}" if source else "/review"
            ),
            "source": source or "",
        })

    def _worker(ctx):
        ctx.set_total(1)

        # Block path — short-circuit the AI call, render the
        # "may need more categories" panel.
        if blocked:
            ctx.emit(
                "Blocked after 2 failed attempts — surfacing manual fallback.",
                outcome="info",
            )
            ctx.advance(1)
            return {
                "terminal_html": _render_terminal(None, blocked_flag=True),
            }

        ctx.emit("Building classify context …", outcome="info")

        # Resolve the source account for this row. Same resolver
        # the synchronous handler uses.
        source_account = _resolve_account_path(
            conn, row.source, row.source_ref,
        )
        if not source_account:
            ctx.emit(
                "Can’t resolve the card/account for this row. The classifier "
                "needs a source-account hint to score targets — classify manually.",
                outcome="failure",
            )
            ctx.advance(1)
            return {
                "terminal_html": _render_terminal(None, blocked_flag=False),
            }

        ai = AIService(settings=settings, conn=conn)
        if not ai.enabled:
            ctx.emit(
                "AI is disabled — set OPENROUTER_API_KEY to enable.",
                outcome="error",
            )
            ctx.advance(1)
            return {
                "terminal_html": _render_terminal(None, blocked_flag=False),
            }

        # Compose the memo from base + user hint + last rejection
        # reason. The classifier reads narration alongside payee,
        # similar-history, active notes, and receipt context — the
        # rejection reason therefore steers the next attempt without
        # any prompt-template branching.
        from datetime import datetime, timezone
        from lamella.adapters.simplefin.schemas import SimpleFINTransaction
        from lamella.features.bank_sync.ingest import SimpleFINIngest
        from lamella.features.ai_cascade.gating import ConfidenceGate

        posted_epoch = int(
            datetime.fromisoformat(row.posting_date[:10])
            .replace(tzinfo=timezone.utc).timestamp()
        )
        txn_id_inner = (
            row.source_ref.get("txn_id")
            if isinstance(row.source_ref, dict) else None
        ) or row.source_ref_hash

        memo_parts: list[str] = []
        if rejection_reason and rejection_reason.strip():
            memo_parts.append(
                f"User rejected the previous AI guess. Their reason: "
                f"{rejection_reason.strip()}"
            )
        hint = (context_hint or "").strip()
        if hint:
            memo_parts.append(f"User hint: {hint}")
        if row.memo:
            memo_parts.append(row.memo)
        composed_memo = "\n".join(memo_parts) if memo_parts else None

        sf_txn = SimpleFINTransaction(
            id=str(txn_id_inner),
            posted=posted_epoch,
            amount=Decimal(row.amount),
            description=row.description or "",
            payee=row.payee,
            memo=composed_memo,
        )

        writer = SimpleFINWriter(
            main_bean=settings.ledger_main,
            simplefin_path=settings.simplefin_transactions_path,
        )
        ingest = SimpleFINIngest(
            conn=conn,
            settings=settings,
            reader=reader,
            rules=RuleService(conn),
            reviews=ReviewService(conn),
            writer=writer,
            ai=ai,
            gate=ConfidenceGate(),
        )

        ctx.emit("Calling AI classifier …", outcome="info")

        # The classifier is async; run it on a fresh event loop in
        # this worker thread. _maybe_ai_classify already handles
        # both the Haiku primary call and the Opus fallback.
        # Thread the staged row's lamella-txn-id so the receipt-
        # context lookup hits the linked branch (ADR-0056) — without
        # this the classifier runs with the candidate-by-amount
        # fallback only and misses receipts the user explicitly
        # attached to this staged row.
        import asyncio
        loop = asyncio.new_event_loop()
        try:
            inline = loop.run_until_complete(
                ingest._maybe_ai_classify(  # noqa: SLF001
                    txn=sf_txn,
                    source_account=source_account,
                    lamella_txn_id=row.lamella_txn_id,
                )
            )
        finally:
            loop.close()

        if inline is None or inline.proposal is None:
            ctx.emit(
                "AI returned no confident proposal.",
                outcome="failure",
            )
            ctx.advance(1)
            return {
                "terminal_html": _render_terminal(None, blocked_flag=False),
            }

        proposal = inline.proposal
        conf_score = float(proposal.confidence or 0.0)
        confidence_bucket = (
            "high" if conf_score >= 0.90
            else "medium" if conf_score >= 0.50
            else "low"
        )

        # Mirror the synchronous handler — record the decision so the
        # row's existing Proposed band reflects the latest AI guess
        # if the user closes the modal without accepting.
        try:
            svc.record_decision(
                staged_id=staged_id,
                account=proposal.target_account,
                confidence=confidence_bucket,
                confidence_score=conf_score,
                decided_by="ai",
                ai_decision_id=proposal.decision_id,
                rationale=proposal.reasoning,
                needs_review=True,
            )
            conn.commit()
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "ask_ai_modal: record_decision failed for %d: %s",
                staged_id, exc,
            )

        ctx.emit(
            f"AI proposed {proposal.target_account} ({confidence_bucket}).",
            outcome="success",
        )
        ctx.advance(1)
        return {
            "terminal_html": _render_terminal(
                {
                    "target": proposal.target_account,
                    "confidence": confidence_bucket,
                    "score": conf_score,
                    "rationale": proposal.reasoning,
                },
                blocked_flag=False,
            ),
        }

    job_id = runner.submit(
        kind="ask-ai-classify",
        title=title,
        fn=_worker,
        total=1,
        meta={
            "staged_id": staged_id,
            "attempt": attempt,
            "source": source,
        },
    )
    return templates.TemplateResponse(
        request,
        "partials/_job_modal.html",
        {
            "job_id": job_id,
            "on_close_url": (
                f"/review?source={source}" if source
                else "/review"
            ),
        },
    )

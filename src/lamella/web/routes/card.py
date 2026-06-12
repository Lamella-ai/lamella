# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""The Card UX — primary interface for categorizing uncategorized
transactions.

Replaces the raw-path review form with a single-card layout showing
each FIXME transaction with pre-filled business / category / receipt /
note. The user confirms or edits, hits save, and the card animates
out. Undo is first-class (10-second window + "Recently categorized"
drawer). Splits are first-class (inline multi-line categorize).

Related modules:
  - registry.service: merchant_memory, entity + account lookup
  - rules.overrides: write the FIXME override(s)
  - receipts.linker: attach receipt
  - receipts.txn_matcher: suggest receipt candidates
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

import yaml
from beancount.core.data import Transaction
from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response

from lamella.core.beancount_io import LedgerReader
from lamella.core.beancount_io.txn_hash import txn_hash as compute_hash
from lamella.core.config import Settings
from lamella.web.deps import (
    get_db,
    get_ledger_reader,
    get_review_service,
    get_settings,
)
from lamella.features.paperless_bridge.lookups import cached_paperless_hash
from lamella.adapters.paperless.schemas import paperless_url_for
from lamella.core.ledger_writer import BeanCheckError
from lamella.features.receipts.linker import DocumentLinker
from lamella.features.receipts.txn_matcher import find_document_candidates
from lamella.core.registry.alias import alias_for
from lamella.core.registry.service import (
    bump_merchant_memory,
    decrement_merchant_memory,
    list_entities,
    merchant_key_for,
    recent_for_merchant,
    scaffold_paths_for_entity,
)
from lamella.features.review_queue.pair_detector import detect_pairs
from lamella.features.review_queue.service import ReviewService
from lamella.features.rules.overrides import OverrideWriter
from lamella.features.rules.scanner import _is_fixme

log = logging.getLogger(__name__)

router = APIRouter()


RECEIPT_AUTO_ATTACH_SCORE = 0.85


@dataclass
class CardContext:
    item_id: int
    txn_hash: str
    txn_date: date
    amount: Decimal
    currency: str
    merchant: str
    narration: str
    from_account: str            # raw path
    from_account_display: str    # alias
    charged_to_entity: str | None
    defaults: dict
    candidate_receipt: dict | None
    candidate_receipts: list[dict]
    pair: dict | None
    categories_by_entity: dict[str, list[dict]]
    entities: list[Any]
    transfer_suggestion: dict | None = None


def _load_schedule(settings: Settings, schedule: str) -> list[dict]:
    """Back-compat shim. New code should call
    ``load_categories_yaml_for_entity(settings, entity)`` directly
    so Personal entities pick up ``personal_categories.yml`` without
    the caller having to know the mapping.
    """
    if schedule == "C":
        p = settings.schedule_c_lines_path
    elif schedule == "F":
        p = settings.schedule_f_lines_path
    elif schedule in ("A", "Personal"):
        p = settings.personal_categories_path
    else:
        return []
    if not p.exists():
        return []
    try:
        return yaml.safe_load(p.read_text(encoding="utf-8")) or []
    except Exception:
        return []


def _txn_by_hash(entries, target_hash: str) -> Transaction | None:
    for e in entries:
        if isinstance(e, Transaction) and compute_hash(e) == target_hash:
            return e
    return None


def _fixme_amount_and_currency(txn: Transaction) -> tuple[Decimal, str, str]:
    """Return (amount, currency, from_account) of the FIXME leg."""
    for p in txn.postings:
        if _is_fixme(p.account) and p.units and p.units.number is not None:
            return (
                Decimal(p.units.number),
                p.units.currency or "USD",
                p.account,
            )
    # Fallback: use first posting with a number.
    for p in txn.postings:
        if p.units and p.units.number is not None:
            return (
                abs(Decimal(p.units.number)),
                p.units.currency or "USD",
                "Expenses:FIXME",
            )
    return (Decimal("0"), "USD", "Expenses:FIXME")


def _charged_to_account(txn: Transaction) -> str | None:
    """The non-FIXME Assets/Liabilities account (the card / bank)."""
    for p in txn.postings:
        acct = p.account or ""
        if _is_fixme(acct):
            continue
        if acct.startswith(("Assets:", "Liabilities:")):
            return acct
    return None


def _build_card(
    *,
    conn,
    settings: Settings,
    reader: LedgerReader,
    review: ReviewService,
    prefer_item_id: int | None = None,
) -> CardContext | None:
    """Build the CardContext for the next item in the review queue."""
    items = review.list_open()
    if not items:
        return None

    # If user provided a specific item_id (e.g. from the drawer "reopen"),
    # pick that one; otherwise the first non-deferred fixme item.
    chosen = None
    if prefer_item_id is not None:
        for it in items:
            if it.id == prefer_item_id:
                chosen = it
                break
    if chosen is None:
        for it in items:
            if it.kind != "fixme":
                continue
            if not it.source_ref.startswith("fixme:"):
                continue
            chosen = it
            break
    if chosen is None:
        return None

    th = chosen.source_ref.split(":", 1)[1]
    entries = list(reader.load().entries)
    txn = _txn_by_hash(entries, th)
    if txn is None:
        return None

    amount, currency, from_account = _fixme_amount_and_currency(txn)
    charged_to = _charged_to_account(txn)

    # Determine default entity from the charged_to account's meta.
    entity_slug_default = None
    if charged_to:
        row = conn.execute(
            "SELECT entity_slug FROM accounts_meta WHERE account_path = ?",
            (charged_to,),
        ).fetchone()
        if row:
            entity_slug_default = row["entity_slug"]
    # Fallback: second path segment of the charged-to account.
    if entity_slug_default is None and charged_to:
        parts = charged_to.split(":")
        if len(parts) >= 2 and parts[1] not in ("Vehicles", "FIXME"):
            entity_slug_default = parts[1]

    # Build categories_by_entity. Entities with a known tax schedule
    # (or the Personal entity) load their yaml-defined chart; others
    # fall back to scanning the ledger for whatever they already have.
    from lamella.core.registry.service import load_categories_yaml_for_entity
    entities = list_entities(conn, include_inactive=False)
    categories_by_entity: dict[str, list[dict]] = {}
    for e in entities:
        yaml_data = load_categories_yaml_for_entity(settings, e)
        if yaml_data:
            cats = scaffold_paths_for_entity(yaml_data, e.slug)
            categories_by_entity[e.slug] = cats
        else:
            # Entity without a schedule: fall back to scanning the ledger
            # for existing Expenses:{slug}:* paths.
            cats = []
            prefix = f"Expenses:{e.slug}:"
            seen = set()
            for entry in entries:
                if hasattr(entry, "account"):
                    acct = entry.account
                    if isinstance(acct, str) and acct.startswith(prefix) and acct not in seen:
                        seen.add(acct)
                        cats.append({"path": acct, "description": acct.split(":")[-1], "line": None})
            cats.sort(key=lambda c: c["path"])
            categories_by_entity[e.slug] = cats

    # Merchant memory → top categories for this merchant.
    merchant = getattr(txn, "payee", None) or ""
    narration = txn.narration or ""
    mkey = merchant_key_for(narration, merchant) or ""
    recent = recent_for_merchant(conn, mkey, limit=3) if mkey else []

    # AI suggestion: parse review_item.ai_suggestion if present (rule engine
    # or Phase 3 AI already populated it).
    ai_suggestion = None
    try:
        if chosen.ai_suggestion:
            ai_suggestion = json.loads(chosen.ai_suggestion)
    except Exception:
        ai_suggestion = None

    default_target = None
    if recent:
        default_target = recent[0]["target_account"]
    elif ai_suggestion:
        if isinstance(ai_suggestion, dict):
            for key in ("rule", "ai"):
                leaf = ai_suggestion.get(key) if isinstance(ai_suggestion.get(key), dict) else None
                if leaf and leaf.get("target_account"):
                    default_target = leaf["target_account"]
                    break

    # Candidate receipts via paperless matcher. Return top 3 so the user
    # can attach multiple documents (invoice + receipt + packing slip
    # all for the same transaction) on one save.
    candidates_list: list[dict] = []
    try:
        cands = find_document_candidates(
            conn,
            txn_amount=abs(amount),
            txn_date=txn.date,
            narration=narration,
            payee=merchant or None,
            limit=5,
            min_score=0.55,
        )
        for top in cands:
            candidates_list.append({
                "paperless_id": top.paperless_id,
                "title": top.title,
                "score": top.score,
                "auto_attach": top.score >= RECEIPT_AUTO_ATTACH_SCORE,
                "reasons": list(top.reasons),
                "total": str(top.total_amount) if top.total_amount else None,
                "subtotal": str(top.subtotal_amount) if top.subtotal_amount else None,
                "tax": str(top.tax_amount) if top.tax_amount else None,
                "last_four": top.payment_last_four,
                "correspondent": top.correspondent_name,
                "vendor": top.vendor,
                "document_type": top.document_type_name,
                "date": top.effective_date.isoformat() if top.effective_date else None,
                "content_excerpt": top.content_excerpt,
            })
    except Exception as exc:  # noqa: BLE001
        log.warning("receipt candidate lookup failed: %s", exc)
    # Keep single-candidate for backward-compat with older templates.
    candidate = candidates_list[0] if candidates_list else None

    # Pair detection.
    pair_info = None
    try:
        pair_map = detect_pairs(entries)
        p = pair_map.get(th)
        if p is not None:
            pair_info = {
                "partner_account": p.partner_account,
                "partner_display": alias_for(conn, p.partner_account),
                "ref": p.ref,
                "strength": p.strength,
            }
    except Exception:  # noqa: BLE001
        pair_info = None

    from_display = alias_for(conn, charged_to) if charged_to else "Unknown account"

    # Smart transfer-target prefill. Scores every Assets/Liabilities
    # account against the narration on multiple signals — a
    # 4-digit token matching accounts_meta.last_four, path-segment
    # words appearing in the text, institution name hits — so
    # "PRIME CHECKING ...5040" picks the :PrimeChecking account even
    # when no last_four is stored. Highest-scoring row wins; "transfer"
    # in the haystack auto-opens the UI at high confidence.
    transfer_suggestion: dict | None = None
    try:
        import re as _re
        haystack = f"{narration} {merchant or ''}"
        haystack_lc = haystack.lower()
        has_transfer_keyword = bool(
            _re.search(r"\btransfer\b", haystack, _re.IGNORECASE)
        )
        last_fours = set(_re.findall(r"(?:^|[^\d])(\d{4})(?!\d)", haystack))

        source_path = charged_to or from_account
        candidate_rows = conn.execute(
            "SELECT account_path, display_name, last_four, institution "
            "FROM accounts_meta "
            "WHERE (account_path LIKE 'Assets:%' OR account_path LIKE 'Liabilities:%') "
            "AND closed_on IS NULL"
        ).fetchall()
        best: tuple[int, dict] | None = None
        for r in candidate_rows:
            if r["account_path"] == source_path:
                continue
            score = 0
            # +5 when stored last_four matches a token in the text.
            if r["last_four"] and r["last_four"] in last_fours:
                score += 5
            # +1 per path-segment word (>=4 chars, alphabetic) that
            # appears in the text. Also break camelCase so "PrimeChecking"
            # matches "Prime Checking" — scoring the sub-words
            # independently. Use a set so "checking" only counts once
            # even when multiple path segments share the suffix.
            tokens = set()
            for seg in r["account_path"].split(":"):
                # Split seg on camelCase + digit boundaries.
                for m in _re.findall(r"[A-Z][a-z]+|[a-z]+|[0-9]+", seg):
                    tokens.add(m.lower())
                tokens.add(seg.lower())  # also the un-split form
            for t in tokens:
                if len(t) >= 4 and t.isalpha() and t in haystack_lc:
                    score += 1
            # +2 when the institution name hits (strong signal).
            inst = (r["institution"] or "").lower()
            if len(inst) >= 4 and inst in haystack_lc:
                score += 2
            # +1 when the (non-path) display_name hits.
            disp = (r["display_name"] or "").lower()
            if len(disp) >= 4 and disp in haystack_lc:
                score += 1
            if score == 0:
                continue
            if best is None or score > best[0]:
                best = (score, dict(r))
        if best is not None:
            transfer_suggestion = {
                "target_account": best[1]["account_path"],
                "display_name": best[1]["display_name"] or best[1]["account_path"],
                "last_four": best[1]["last_four"],
                "score": best[0],
                # High confidence = "transfer" keyword AND 2+ signal
                # points (so a single weak token-match doesn't auto-open).
                "confident": has_transfer_keyword and best[0] >= 2,
            }
    except Exception as exc:  # noqa: BLE001
        log.warning("transfer-suggestion detection failed: %s", exc)
        transfer_suggestion = None

    return CardContext(
        item_id=chosen.id,
        txn_hash=th,
        txn_date=txn.date,
        amount=abs(amount),
        currency=currency,
        merchant=merchant or "",
        narration=narration,
        from_account=charged_to or from_account,
        from_account_display=from_display,
        charged_to_entity=entity_slug_default,
        defaults={
            "target_account": default_target,
            "business": entity_slug_default,
            "recent": recent,
        },
        candidate_receipt=candidate,
        candidate_receipts=candidates_list,
        pair=pair_info,
        categories_by_entity=categories_by_entity,
        entities=entities,
        transfer_suggestion=transfer_suggestion,
    )


def _pick_next_staged(
    conn,
    exclude_ids: set[int] | None = None,
    focus_id: int | None = None,
    skip_transfers: bool = True,
):
    """Return ``(chosen_item, chosen_group, groups)`` for the highest-
    impact pending staged row, or ``None`` when nothing pends.

    Ranking — group size desc, then oldest staged_id (so older work
    surfaces first within a tie). Picking the prototype of the largest
    group is the explicit user-visible behavior of /card: "show me
    the row whose classification will help most others fall into
    place." If only singletons remain, this still returns the
    earliest-staged one — random would be lossy.

    ``exclude_ids`` lets the caller honor a Skip — those staged_ids
    are filtered out before grouping. The skip is per-render, not
    persisted; reload the page and the row is back.

    ``focus_id`` lets the up-next carousel jump directly to a
    specific group: if any group contains that staged_id, that group
    becomes the chosen one regardless of size ranking. The remaining
    groups are still sorted by impact so the carousel always offers
    its biggest options first."""
    from lamella.features.review_queue.grouping import group_staged_rows
    from lamella.web.routes.staging_review import (
        _build_row_extras,
        _looks_like_transfer,
    )
    from lamella.features.import_.staging import list_pending_items

    items = list_pending_items(conn)
    if exclude_ids:
        items = [it for it in items if it.staged_id not in exclude_ids]
    if not items:
        return None
    # Transfer-flagged rows distract from /card's "show me one row at a
    # time" promise — they're often waiting on the other half of the
    # transfer to land, and classifying just one leg would write a
    # half-formed entry. Hide them by default; only fall back to the
    # full pool when literally nothing else is pending. The hint check
    # consults each row's source-account kind so a CC / loan / LoC
    # payment is recognized as a transfer leg without needing the word
    # "transfer" in the bank's narration.
    if skip_transfers:
        kind_extras = _build_row_extras(conn, items)

        def _is_transfer(it) -> bool:
            ex = kind_extras.get(it.staged_id, {})
            return _looks_like_transfer(
                it, card_kind=ex.get("card_kind") or None,
            )

        non_transfer = [it for it in items if not _is_transfer(it)]
        if non_transfer:
            items = non_transfer
    groups = group_staged_rows(items)

    if focus_id is not None:
        for g in groups:
            if any(it.staged_id == focus_id for it in g.items):
                others = sorted(
                    [x for x in groups if x is not g],
                    key=lambda x: (-x.size, x.prototype.staged_id),
                )
                return g.prototype, g, [g] + others

    groups_sorted = sorted(groups, key=lambda g: (-g.size, g.prototype.staged_id))
    chosen_group = groups_sorted[0]
    return chosen_group.prototype, chosen_group, groups_sorted


@router.get("/card", response_class=HTMLResponse)
def card_page(
    request: Request,
    item_id: int | None = None,
    skip: int | None = None,
    focus: int | None = None,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    review: ReviewService = Depends(get_review_service),
):
    # Staging-first IA. The unified staged_transactions surface is the
    # canonical pending-work queue; the legacy review_items flow only
    # surfaces FIXME-in-ledger items (which are now rare since ingest
    # writes classified directly when confident). When anything pends
    # in staging, render the slim single-row staged-card template,
    # prioritizing the row whose classification has the largest
    # blast radius (largest group prototype). Falls through to the
    # legacy flow only when staging is empty.
    from lamella.web.routes.staging_review import _build_row_extras
    from lamella.features.import_.staging import count_pending_items

    if count_pending_items(conn) > 0:
        exclude = {skip} if skip else None
        picked = _pick_next_staged(
            conn, exclude_ids=exclude, focus_id=focus,
        )
        if picked is not None:
            chosen_item, chosen_group, all_groups = picked
            other_groups = [g for g in all_groups if g is not chosen_group]
            # Build extras for the chosen group's siblings (so the
            # sibling preview can label cards/entities) + each other
            # group's prototype (so the up-next carousel can label
            # each tile). One pass through _build_row_extras keeps
            # the SQL bounded — sibling rows + N prototypes, not the
            # full pending list.
            extras_input = list(chosen_group.items) + [
                g.prototype for g in other_groups
            ]
            extras = _build_row_extras(conn, extras_input)
            total_pending = count_pending_items(conn)

            # Per-row suggestion cards — does the row in front of the
            # user match a known payout source we haven't scaffolded
            # yet? Real ledger entries pass through so the registry
            # can filter dismissed (pattern, entity) pairs.
            suggestion_cards: list = []
            try:
                from lamella.features.review_queue.suggestions import build_suggestion_cards
                row_extras = extras.get(chosen_item.staged_id, {})
                row_text = " ".join(filter(None, [
                    chosen_item.payee or "",
                    chosen_item.description or "",
                ]))
                try:
                    ledger_entries = reader.load().entries
                except Exception:  # noqa: BLE001
                    ledger_entries = []
                suggestion_cards = build_suggestion_cards(
                    conn, entries=ledger_entries,
                    context="row",
                    row_payee_text=row_text or None,
                    row_account_path=row_extras.get("card_account") or None,
                )
            except Exception:  # noqa: BLE001
                log.exception("/card build_suggestion_cards failed")
                suggestion_cards = []

            ctx = {
                "item": chosen_item,
                "group": chosen_group,
                "other_groups": other_groups,
                "extras": extras,
                "total_pending": total_pending,
                "group_count": len(all_groups),
                "suggestion_cards": suggestion_cards,
                "message": request.query_params.get("message"),
            }
            # On HX-Request, return only the swappable partial so the
            # outerHTML swap targets #card-pane cleanly. Full-page
            # nav still gets the wrapped template (page_head + script
            # + styles + the partial).
            tpl = (
                "partials/_card_pane.html"
                if request.headers.get("HX-Request")
                else "card_staged.html"
            )
            return request.app.state.templates.TemplateResponse(
                request, tpl, ctx,
            )

    ctx = _build_card(
        conn=conn, settings=settings, reader=reader, review=review,
        prefer_item_id=item_id,
    )
    if ctx is None:
        recent = conn.execute(
            "SELECT * FROM review_actions WHERE undone_at IS NULL "
            "ORDER BY created_at DESC LIMIT 20"
        ).fetchall()
        return request.app.state.templates.TemplateResponse(
            request, "card_empty.html", {"recent_actions": [dict(r) for r in recent]}
        )

    open_count = len(review.list_open())
    recent_actions = conn.execute(
        "SELECT * FROM review_actions WHERE undone_at IS NULL "
        "ORDER BY created_at DESC LIMIT 10"
    ).fetchall()
    ctx_dict = {
        "card": ctx,
        "open_count": open_count,
        "threshold_usd": settings.receipt_required_threshold_usd,
        "recent_actions": [dict(r) for r in recent_actions],
    }
    return request.app.state.templates.TemplateResponse(
        request, "card.html", ctx_dict
    )


@router.get("/card/categories-for/{slug}")
def categories_for_entity(
    slug: str,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """HTMX endpoint — returns category options for a given entity."""
    row = conn.execute(
        "SELECT slug, tax_schedule, entity_type FROM entities WHERE slug = ?",
        (slug,),
    ).fetchone()
    if row is None:
        return HTMLResponse("<option value=''>(no entity)</option>")
    from lamella.core.registry.service import load_categories_yaml_for_entity
    cats: list[dict] = []
    yaml_data = load_categories_yaml_for_entity(settings, row)
    if yaml_data:
        cats = scaffold_paths_for_entity(yaml_data, slug)
    if not cats:
        # Fallback: scan ledger for existing Expenses:{slug}:*
        prefix = f"Expenses:{slug}:"
        seen = set()
        for entry in reader.load().entries:
            acct = getattr(entry, "account", None)
            if isinstance(acct, str) and acct.startswith(prefix) and acct not in seen:
                seen.add(acct)
                cats.append({"path": acct, "description": acct.split(":")[-1], "line": None})
    options = ["<option value=''>—</option>"]
    for c in cats:
        options.append(f"<option value='{c['path']}'>{c['description']}</option>")
    return HTMLResponse("".join(options))


@router.post("/card/{item_id}/save")
async def card_save(
    item_id: int,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    review: ReviewService = Depends(get_review_service),
):
    form = await request.form()
    items = review.list_open()
    item = next((i for i in items if i.id == item_id), None)
    if item is None:
        raise HTTPException(status_code=404, detail="review item not found or already resolved")
    if not item.source_ref.startswith("fixme:"):
        raise HTTPException(status_code=400, detail="not a fixme item")
    th = item.source_ref.split(":", 1)[1]

    entries = list(reader.load().entries)
    txn = _txn_by_hash(entries, th)
    if txn is None:
        raise HTTPException(status_code=404, detail="transaction not found in ledger")
    amount, currency, from_account = _fixme_amount_and_currency(txn)
    abs_amount = abs(amount)

    # Parse split lines if any. Form shape: target_account[], split_amount[]
    target_accounts = [v for (k, v) in form.multi_items() if k == "target_account" and v]
    split_amounts_raw = [v for (k, v) in form.multi_items() if k == "split_amount" and v]
    # Receipt IDs as a list so the user can attach multiple Paperless
    # documents (invoice + receipt + packing slip) to one transaction.
    receipt_ids_raw = [
        v for (k, v) in form.multi_items() if k == "receipt_paperless_id" and v
    ]
    note_text = (form.get("note") or "").strip()

    if not target_accounts:
        raise HTTPException(status_code=400, detail="target_account is required")

    # Build split list from form (1 entry = single leg, N entries = splits).
    splits: list[tuple[str, Decimal]] = []
    if len(target_accounts) == 1:
        splits = [(target_accounts[0], abs_amount)]
    else:
        if len(split_amounts_raw) != len(target_accounts):
            raise HTTPException(
                status_code=400,
                detail="split_amount count must match target_account count",
            )
        total = Decimal("0")
        for acct, amt_s in zip(target_accounts, split_amounts_raw):
            try:
                amt = Decimal(amt_s)
            except Exception:
                raise HTTPException(status_code=400, detail=f"bad split amount {amt_s!r}")
            splits.append((acct, amt))
            total += amt
        if total != abs_amount:
            raise HTTPException(
                status_code=400,
                detail=f"splits sum to {total}, expected {abs_amount}",
            )

    # Write the override (single-leg or multi-leg).
    # Per CLAUDE.md, single-leg = in-place rewrite by default.
    # Multi-leg splits are a legitimate overlay (one txn → many
    # postings) and stay on the override layer.
    overrider = OverrideWriter(
        main_bean=settings.ledger_main,
        overrides=settings.connector_overrides_path,
        conn=conn,
    )
    try:
        if len(splits) == 1:
            from pathlib import Path as _P
            from lamella.core.rewrite.txn_inplace import (
                InPlaceRewriteError,
                rewrite_fixme_to_account,
            )
            in_place_done = False
            meta = getattr(txn, "meta", None) or {}
            src_file = meta.get("filename")
            src_lineno = meta.get("lineno")
            fixme_signed = None
            for _p in txn.postings or ():
                if (_p.account or "") == from_account and _p.units \
                        and _p.units.number is not None:
                    fixme_signed = Decimal(_p.units.number)
                    break
            if src_file and src_lineno is not None:
                try:
                    try:
                        overrider.rewrite_without_hash(th)
                    except BeanCheckError:
                        raise InPlaceRewriteError(
                            "override-strip blocked"
                        )
                    rewrite_fixme_to_account(
                        source_file=_P(src_file),
                        line_number=int(src_lineno),
                        old_account=from_account,
                        new_account=splits[0][0],
                        expected_amount=fixme_signed,
                        ledger_dir=settings.ledger_dir,
                        main_bean=settings.ledger_main,
                    )
                    in_place_done = True
                except InPlaceRewriteError as exc:
                    log.info(
                        "card classify: in-place refused for %s: %s "
                        "— falling back to override", th[:12], exc,
                    )
            if not in_place_done:
                overrider.append(
                    txn_date=txn.date,
                    txn_hash=th,
                    amount=splits[0][1],
                    from_account=from_account,
                    to_account=splits[0][0],
                    currency=currency,
                    narration=(txn.narration or "FIXME override"),
                )
        else:
            # Multi-leg split — N target accounts. Try in-place
            # first (one FIXME line → N posting lines, sums must
            # equal the original FIXME amount). Override fallback
            # only when filename/lineno is missing or the path-
            # safety check refuses.
            from pathlib import Path as _P
            from lamella.core.rewrite.txn_inplace import (
                InPlaceRewriteError,
                rewrite_fixme_to_multiple_postings,
            )
            in_place_done = False
            meta = getattr(txn, "meta", None) or {}
            src_file = meta.get("filename")
            src_lineno = meta.get("lineno")
            fixme_signed = None
            for _p in txn.postings or ():
                if (_p.account or "") == from_account and _p.units \
                        and _p.units.number is not None:
                    fixme_signed = Decimal(_p.units.number)
                    break
            if src_file and src_lineno is not None:
                try:
                    try:
                        overrider.rewrite_without_hash(th)
                    except BeanCheckError:
                        raise InPlaceRewriteError(
                            "override-strip blocked"
                        )
                    rewrite_fixme_to_multiple_postings(
                        source_file=_P(src_file),
                        line_number=int(src_lineno),
                        old_account=from_account,
                        splits=splits,
                        expected_amount=fixme_signed,
                        currency=currency,
                        ledger_dir=settings.ledger_dir,
                        main_bean=settings.ledger_main,
                    )
                    in_place_done = True
                except InPlaceRewriteError as exc:
                    log.info(
                        "card split-classify: in-place refused for %s: %s "
                        "— falling back to override",
                        th[:12], exc,
                    )
            if not in_place_done:
                overrider.append_split(
                    txn_date=txn.date,
                    txn_hash=th,
                    from_account=from_account,
                    splits=splits,
                    currency=currency,
                    narration=(txn.narration or "FIXME override (split)"),
                )
            for idx, (acct, amt) in enumerate(splits):
                conn.execute(
                    "INSERT OR REPLACE INTO fixme_override_splits "
                    "(txn_hash, leg_idx, target_account, amount) "
                    "VALUES (?, ?, ?, ?)",
                    (th, idx, acct, str(amt)),
                )
    except BeanCheckError as exc:
        raise HTTPException(status_code=500, detail=f"bean-check failed: {exc}")

    # Receipt link(s) — loop over every checked receipt id. The schema's
    # UNIQUE(paperless_id, txn_hash) prevents duplicate links for the
    # same pair while allowing multiple docs per txn.
    receipt_ids_int: list[int] = []
    for raw in receipt_ids_raw:
        try:
            receipt_ids_int.append(int(raw))
        except ValueError:
            continue
    linked_ids: list[int] = []
    if receipt_ids_int:
        linker = DocumentLinker(
            conn=conn,
            main_bean=settings.ledger_main,
            connector_links=settings.connector_links_path,
        )
        for pid in receipt_ids_int:
            try:
                linker.link(
                    paperless_id=pid,
                    txn_hash=th,
                    txn_date=txn.date,
                    txn_amount=abs_amount,
                    match_method="card_ui",
                    match_confidence=0.95,
                    paperless_hash=cached_paperless_hash(conn, pid),
                    paperless_url=paperless_url_for(settings.paperless_url, pid),
                )
                linked_ids.append(pid)
            except BeanCheckError as exc:
                log.warning("receipt link skipped for #%s: %s", pid, exc)
        # ADR-0044: write the four canonical Lamella_* fields back
        # to Paperless for each linked doc. Best-effort: writeback
        # failures never break the link / card-save flow.
        if linked_ids:
            try:
                from lamella.features.paperless_bridge.writeback import (
                    writeback_after_link,
                )
                for pid in linked_ids:
                    await writeback_after_link(
                        paperless_id=pid,
                        txn_hash=th,
                        settings=settings,
                        reader=reader,
                        conn=conn,
                    )
            except Exception:  # noqa: BLE001
                pass
    receipt_id_int = linked_ids[0] if linked_ids else None

    # Merchant memory bump (one bump per unique target).
    mkey = merchant_key_for(txn.narration, getattr(txn, "payee", None))
    if mkey:
        for acct, _ in splits:
            entity_slug = None
            row = conn.execute(
                "SELECT entity_slug FROM accounts_meta WHERE account_path = ?",
                (acct,),
            ).fetchone()
            if row:
                entity_slug = row["entity_slug"]
            # Fallback: second path segment for Expenses:Entity:*
            if entity_slug is None:
                parts = acct.split(":")
                if len(parts) >= 2 and parts[0] == "Expenses":
                    entity_slug = parts[1]
            bump_merchant_memory(
                conn,
                merchant_key=mkey,
                target_account=acct,
                entity_slug=entity_slug,
            )

    # Record undoable action.
    payload = {
        "txn_hash": th,
        "splits": [[acct, str(amt)] for acct, amt in splits],
        "receipt_paperless_id": receipt_id_int,
        "receipt_paperless_ids": linked_ids,
        "note": note_text or None,
        "from_account": from_account,
        # Captured so card_undo can reverse the bump on the exact
        # (merchant_key, target_account) row this action touched.
        "merchant_key": mkey,
    }
    conn.execute(
        "INSERT INTO review_actions (review_item_id, txn_hash, action_type, payload_json) "
        "VALUES (?, ?, ?, ?)",
        (
            item_id, th,
            "split_categorize" if len(splits) > 1 else "categorize",
            json.dumps(payload),
        ),
    )

    review.resolve(item_id, f"card → {splits[0][0]}" + (" (split)" if len(splits) > 1 else ""))
    reader.invalidate()

    if "hx-request" in {k.lower() for k in request.headers.keys()}:
        # HTMX swap: return an empty response so caller redirects.
        return HTMLResponse("", headers={"HX-Redirect": "/card"})
    return Response(status_code=204, headers={"Location": "/card"})


@router.post("/card/{item_id}/skip")
def card_skip(
    item_id: int,
    request: Request,
    conn = Depends(get_db),
    review: ReviewService = Depends(get_review_service),
):
    # Bump deferred_count and set deferred_until = 24h from now.
    until = (date.today() + timedelta(days=1)).isoformat()
    conn.execute(
        "UPDATE review_queue SET deferred_count = deferred_count + 1, deferred_until = ? "
        "WHERE id = ?",
        (until, item_id),
    )
    # Record undoable action.
    item = next((i for i in review.list_open() if i.id == item_id), None)
    if item is not None:
        th = item.source_ref.split(":", 1)[1] if item.source_ref.startswith("fixme:") else None
        conn.execute(
            "INSERT INTO review_actions (review_item_id, txn_hash, action_type, payload_json) "
            "VALUES (?, ?, 'skip', ?)",
            (item_id, th, json.dumps({"deferred_until": until})),
        )
    if "hx-request" in {k.lower() for k in request.headers.keys()}:
        return HTMLResponse("", headers={"HX-Redirect": "/card"})
    return Response(status_code=204, headers={"Location": "/card"})


@router.post("/card/undo/{action_id}")
def card_undo(
    action_id: int,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    review: ReviewService = Depends(get_review_service),
):
    row = conn.execute(
        "SELECT * FROM review_actions WHERE id = ? AND undone_at IS NULL",
        (action_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="action not found or already undone")

    action_type = row["action_type"]
    payload = json.loads(row["payload_json"]) if row["payload_json"] else {}
    review_item_id = row["review_item_id"]
    th = row["txn_hash"]

    if action_type in ("categorize", "split_categorize"):
        # Remove the override block from connector_overrides.bean.
        overrider = OverrideWriter(
            main_bean=settings.ledger_main,
            overrides=settings.connector_overrides_path,
        )
        try:
            overrider.rewrite_without_hash(th)
        except BeanCheckError as exc:
            raise HTTPException(status_code=500, detail=f"bean-check failed: {exc}")
        # Remove any receipt link written by this action.
        if payload.get("receipt_paperless_id"):
            conn.execute(
                "DELETE FROM document_links WHERE paperless_id = ? AND txn_hash = ?",
                (payload["receipt_paperless_id"], th),
            )
        # Remove splits.
        conn.execute("DELETE FROM fixme_override_splits WHERE txn_hash = ?", (th,))
        # Decrement merchant memory on the exact (merchant_key,
        # target_account) row that was bumped at categorize time.
        # Older actions (pre-payload-merchant_key) skip this — better
        # to leave use_count one too high than to stomp the wrong row.
        mkey = payload.get("merchant_key")
        if mkey:
            for leg in payload.get("splits") or []:
                acct = leg[0] if isinstance(leg, list) else leg
                decrement_merchant_memory(
                    conn, merchant_key=mkey, target_account=acct,
                )
        # Re-open the review item.
        conn.execute(
            "UPDATE review_queue SET resolved_at = NULL, user_decision = NULL WHERE id = ?",
            (review_item_id,),
        )
    elif action_type == "skip":
        conn.execute(
            "UPDATE review_queue SET deferred_count = MAX(0, deferred_count - 1), "
            "deferred_until = NULL WHERE id = ?",
            (review_item_id,),
        )
    elif action_type == "transfer":
        # Remove transfer overrides (both halves).
        overrider = OverrideWriter(
            main_bean=settings.ledger_main,
            overrides=settings.connector_overrides_path,
        )
        partner_hash = payload.get("partner_hash")
        try:
            overrider.rewrite_without_hash(th)
            if partner_hash:
                overrider.rewrite_without_hash(partner_hash)
        except BeanCheckError as exc:
            raise HTTPException(status_code=500, detail=f"bean-check failed: {exc}")
        conn.execute(
            "UPDATE review_queue SET resolved_at = NULL, user_decision = NULL WHERE id = ?",
            (review_item_id,),
        )

    conn.execute(
        "UPDATE review_actions SET undone_at = CURRENT_TIMESTAMP WHERE id = ?",
        (action_id,),
    )
    reader.invalidate()

    if "hx-request" in {k.lower() for k in request.headers.keys()}:
        return HTMLResponse("", headers={"HX-Redirect": "/card"})
    return Response(status_code=204, headers={"Location": "/card"})


@router.get("/card/recent")
def card_recent_actions(
    request: Request,
    conn = Depends(get_db),
):
    rows = conn.execute(
        "SELECT id, created_at, action_type, payload_json, txn_hash, undone_at "
        "FROM review_actions WHERE undone_at IS NULL "
        "ORDER BY created_at DESC LIMIT 20"
    ).fetchall()
    return JSONResponse([dict(r) for r in rows])

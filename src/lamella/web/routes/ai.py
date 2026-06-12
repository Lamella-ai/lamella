# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse

from lamella.features.ai_cascade.decisions import DECISION_TYPES, DecisionsLog
from lamella.features.ai_cascade.service import AIService
from lamella.core.config import Settings
from lamella.web.deps import get_ai_service, get_db, get_settings
from lamella.adapters.paperless.client import PaperlessClient

log = logging.getLogger(__name__)

router = APIRouter()


@router.get("/ai/audit", include_in_schema=False)
def ai_audit_legacy_redirect(request: Request):
    """301 from the legacy /ai/audit URL to /ai/logs. The page was
    misnamed: it's a chronological log of every AI call (read-only),
    not a quality-correctness "audit" — that's the /audit surface.
    Redirect preserves any querystring filters."""
    from fastapi.responses import RedirectResponse
    qs = request.url.query
    target = "/ai/logs" + (f"?{qs}" if qs else "")
    return RedirectResponse(target, status_code=301)


@router.get("/ai/logs", response_class=HTMLResponse)
def logs_page(
    request: Request,
    decision_type: str | None = Query(default=None),
    user_corrected: bool | None = Query(default=None),
    days: int = Query(default=30, ge=1, le=365),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    ai: AIService = Depends(get_ai_service),
    conn: sqlite3.Connection = Depends(get_db),
):
    dlog = DecisionsLog(conn)
    if decision_type and decision_type not in DECISION_TYPES:
        raise HTTPException(status_code=400, detail=f"unknown decision_type: {decision_type}")
    since = datetime.now(timezone.utc) - timedelta(days=days)
    rows = dlog.recent(
        limit=limit,
        offset=offset,
        decision_type=decision_type,
        user_corrected=user_corrected,
        since=since,
    )
    # Some classify_txn rows store the SimpleFIN bridge id as
    # input_ref (when the AI was called at ingest time, pre-C1, or
    # on demand from /review where the ledger entry doesn't
    # exist yet). Build the SimpleFIN id → txn_hash map once so
    # source_href can resolve the link without N ledger walks.
    sf_id_to_hash = _build_simplefin_id_to_hash_map(request.app.state)
    ctx = {
        "rows": [
            _decorate(r, sf_id_to_hash=sf_id_to_hash) for r in rows
        ],
        "decision_type": decision_type,
        "user_corrected": user_corrected,
        "days": days,
        "ai_enabled": ai.enabled,
        "spend_cap_reached": ai.spend_cap_reached(),
        "decision_types": sorted(DECISION_TYPES),
        "limit": limit,
        "offset": offset,
    }
    return request.app.state.templates.TemplateResponse(request, "ai_logs.html", ctx)


@router.get("/ai/cost", response_class=HTMLResponse)
def cost_page(
    request: Request,
    ai: AIService = Depends(get_ai_service),
):
    summary = ai.cost_summary()
    ctx = {
        "summary": summary,
        "cap_usd": ai.monthly_cap_usd(),
        "prompt_price": ai.price_prompt_per_1k(),
        "completion_price": ai.price_completion_per_1k(),
        "ai_enabled": ai.enabled,
    }
    return request.app.state.templates.TemplateResponse(request, "ai_cost.html", ctx)


@router.get("/ai/suggestions", response_class=HTMLResponse)
def suggestions_page(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    limit: int = Query(default=50, ge=1, le=500),
):
    """Review queue for classify_txn suggestions the AI proposed but
    didn't auto-apply (below the auto-apply threshold) or that the
    user hasn't yet confirmed.

    Shape: one row per pending decision. Approve writes the override
    and marks user_corrected=1 with user_correction="approved". Reject
    marks user_corrected=1 with user_correction="rejected: <reason>"
    so the FIXME stays open for the next AI pass with negative-
    reinforcement context. Edit lets the user pick a different
    target account — writes the override using the corrected target.
    """
    import json as _json
    # Every classify_txn decision that isn't yet user-corrected.
    #
    # Note: `user_corrected = 0` is the SQLite-side acceptance signal,
    # but per ADR-0001 SQLite is disposable — after a DB reset or
    # `lamella reconstruct`, every accepted suggestion would come back
    # as `user_corrected = 0` because reconstruct doesn't replay
    # acceptance state. The authoritative signal is the override block
    # in `connector_overrides.bean` carrying `lamella-override-of:
    # "<txn_hash>"`. We layer that filter on top below.
    rows = conn.execute(
        """
        SELECT id, decided_at, input_ref, model, prompt_tokens,
               completion_tokens, result
          FROM ai_decisions
         WHERE decision_type = 'classify_txn'
           AND user_corrected = 0
         ORDER BY decided_at DESC
         LIMIT ?
        """,
        (limit,),
    ).fetchall()

    from lamella.core.config import Settings as _S
    settings = _S()

    # Build the override set from the ledger (the durable acceptance
    # signal — see ADR-0001). The set contains txn_hashes that already
    # have an `override-of:` block, meaning the user (or auto-apply
    # path) has already classified that FIXME.
    overrides_text = (
        settings.connector_overrides_path.read_text(encoding="utf-8")
        if settings.connector_overrides_path.exists() else ""
    )
    import re as _re
    already_overridden = set(
        _re.findall(r'(?:lamella-)?override-of:\s*"([^"]+)"', overrides_text)
    )

    # ai_decisions.input_ref can be ANY of three shapes:
    #   * Beancount txn_hash               — bulk-classify path
    #   * lineage UUID (`lamella-txn-id`)  — post-Phase-3 default
    #   * SimpleFIN bridge id              — ingest-time AI calls
    # The override-of set only contains txn_hashes, so we MUST resolve
    # lineage/SimpleFIN inputs to their canonical txn_hash before the
    # `in already_overridden` check — otherwise an accepted SimpleFIN-
    # source suggestion (input_ref="TRN-…") never matches the override
    # block (override-of="<hash>") and the suggestion stays in the queue
    # forever after a SQLite wipe / reconstruct.
    #
    # One ledger walk builds both the alias map (for the override-state
    # filter) AND the by_hash map (for hydration below). Without this
    # consolidation the route walked the ledger twice.
    from lamella.core.beancount_io import LedgerReader as _LR
    from lamella.core.beancount_io.txn_hash import txn_hash as _th
    from beancount.core.data import Transaction as _Txn
    from decimal import Decimal as _D
    from lamella.core.identity import find_source_reference, get_txn_id
    reader = _LR(settings.ledger_main)
    entries = list(reader.load().entries)
    # alias_to_hash maps every non-txn-hash id we might see in input_ref
    # to its canonical ledger txn_hash, so the override-state check can
    # collapse all three shapes onto one comparison key.
    alias_to_hash: dict[str, str] = {}
    by_hash: dict[str, _Txn] = {}
    for e in entries:
        if not isinstance(e, _Txn):
            continue
        h = _th(e)
        by_hash[h] = e
        sf_ref = find_source_reference(e, "simplefin")
        if sf_ref:
            alias_to_hash[str(sf_ref)] = h
        lineage = get_txn_id(e)
        if lineage:
            alias_to_hash[lineage] = h

    # alias_to_staged maps SimpleFIN bridge ids (TRN-…) to staged_id
    # for rows that have NOT yet been promoted to the ledger. This
    # covers the AI-at-ingest-time case where the proposal references
    # a bank-side id that has no ledger txn yet — the user accepting
    # such a suggestion needs to land on the staged row, not get the
    # "could not load — may have been deleted" error from a ledger
    # lookup that will never succeed.
    alias_to_staged: dict[str, int] = {}
    try:
        # Statuses to include: any non-terminal state — `new` (just
        # ingested), `classified` (has a proposal but not yet
        # promoted), `matched` (matched to a pair). Exclude
        # `promoted` (already in ledger; alias_to_hash handles it),
        # `dismissed` / `failed` (terminal, no actionable surface).
        # Earlier WHERE used status='pending' which doesn't exist as
        # a real status — the resulting map was empty and Accept on
        # an unpromoted row continued to land on ledger:TRN-… and
        # 404'd.
        cur = conn.execute(
            "SELECT id, source_ref FROM staged_transactions "
            "WHERE source = 'simplefin' "
            "  AND status IN ('new', 'classified', 'matched')"
        )
        for row in cur.fetchall():
            try:
                ref_obj = _json.loads(row["source_ref"]) if row["source_ref"] else {}
            except Exception:  # noqa: BLE001
                continue
            txn_id = ref_obj.get("txn_id") if isinstance(ref_obj, dict) else None
            if txn_id:
                alias_to_staged[str(txn_id)] = row["id"]
    except Exception as exc:  # noqa: BLE001
        log.warning("ai/suggestions: staged-alias build failed: %s", exc)

    def _canonical_hash(ref: str) -> str:
        """Return the canonical txn_hash for a decision input_ref.
        Falls back to the input_ref itself when the ledger has no
        matching alias (the txn might already be a hash, or might
        have been removed from the ledger)."""
        return alias_to_hash.get(ref, ref)

    # The AUTHORITATIVE "already-classified" signal is the resolved
    # ledger txn no longer carrying a FIXME posting. Two paths classify
    # a FIXME and we need to detect both:
    #   1. Override-block path → writes `override-of:` in
    #      connector_overrides.bean. The regex above catches it.
    #   2. In-place rewrite path → replaces the FIXME posting line
    #      directly in the source .bean. NO override block is written.
    # Pre-build a set of hashes whose current ledger entry has at
    # least one FIXME posting; suggestions whose canonical hash is
    # NOT in this set are stale (txn classified by either path) and
    # auto-supersede so the queue doesn't show un-acceptable rows.
    def _is_fixme_acct(acct: str | None) -> bool:
        if not acct:
            return False
        return acct.split(":")[-1].upper() == "FIXME"
    hashes_with_fixme: set[str] = set()
    for h, e in by_hash.items():
        if any(_is_fixme_acct(p.account) for p in (e.postings or ())):
            hashes_with_fixme.add(h)

    # Deduplicate by canonical txn_hash — when the AI was re-run on
    # the same FIXME multiple times (e.g., the user triggered repeated
    # bulk-classify passes or we manually tested), only the newest
    # proposal should be reviewable. The older ones are
    # automatically superseded: we stamp user_corrected=1 with
    # user_correction="superseded by #N" so they drop out of the
    # queue AND land in the audit log as resolved-by-supersession,
    # not left-as-pending-forever.
    pending = []
    seen_txns: set[str] = set()
    superseded_ids: list[tuple[int, int]] = []  # (old_id, newer_id)
    classified_skip_ids: list[int] = []
    # Rows come back decided_at DESC, so the first time we see a
    # canonical hash it's the newest. Everything after for the same
    # hash is older and gets superseded.
    newest_by_hash: dict[str, int] = {}
    for r in rows:
        d = dict(r)
        canonical = _canonical_hash(d["input_ref"])
        # Hide already-accepted suggestions: an `override-of` block
        # exists in the ledger for this txn (authoritative signal for
        # the override path).
        if canonical in already_overridden:
            classified_skip_ids.append(d["id"])
            continue
        # Hide stale suggestions whose target txn no longer has a
        # FIXME posting (in-place rewrite path — no override block
        # written, but the txn IS classified). Only applies when we
        # actually resolved the ref to a ledger txn; staged-only
        # rows (canonical not in by_hash) skip this check.
        if canonical in by_hash and canonical not in hashes_with_fixme:
            classified_skip_ids.append(d["id"])
            continue
        if canonical in seen_txns:
            # Older duplicate for the same txn — supersede it.
            superseded_ids.append((d["id"], newest_by_hash[canonical]))
            continue
        seen_txns.add(canonical)
        newest_by_hash[canonical] = d["id"]
        try:
            d["result_parsed"] = _json.loads(d["result"]) if d.get("result") else {}
        except Exception:  # noqa: BLE001
            d["result_parsed"] = {}
        pending.append(d)
    # Apply the supersession flags in a single batch so the queue
    # is clean for subsequent visits (and so the AI audit log shows
    # these were resolved, not ignored).
    for old_id, new_id in superseded_ids:
        try:
            conn.execute(
                "UPDATE ai_decisions SET user_corrected = 1, "
                "user_correction = ? WHERE id = ? AND user_corrected = 0",
                (f"superseded by #{new_id}", old_id),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "supersede ai_decisions id=%s failed: %s", old_id, exc,
            )
    # Auto-stamp suggestions whose target txn is already classified.
    # Without this, after a SQLite reset the queue would re-fill with
    # stale rows on every page load (the cache lost what was accepted)
    # and the user would see Accept fail with 400 "no FIXME posting".
    for stale_id in classified_skip_ids:
        try:
            conn.execute(
                "UPDATE ai_decisions SET user_corrected = 1, "
                "user_correction = 'classified outside this queue' "
                "WHERE id = ? AND user_corrected = 0",
                (stale_id,),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "stale-flag ai_decisions id=%s failed: %s", stale_id, exc,
            )

    # Hydrate every pending row with the underlying transaction's
    # essentials (date, amount, payee, narration, source account,
    # source entity) so the user can decide approve/reject without
    # clicking through to /txn for each. Without this the card is
    # an unreviewable wall of AI reasoning with zero context.
    #
    # Two flavors of input_ref need to resolve to a ledger txn:
    #  * Bulk-classify path uses the real txn_hash → direct match
    #    against by_hash.
    #  * SimpleFIN ingest path stores the bank's transaction id
    #    (txn.id) because at the time of the AI call the txn isn't
    #    in the ledger yet. After the entry lands it carries
    #    `lamella-simplefin-id` metadata — alias_to_hash bridges it.
    # We populate `txn_hash_for_link` per row so the template links
    # to the correct hash regardless of which path created the
    # decision.
    # Pre-load the staged rows we'll need for hydration in one query
    # rather than N. Keys are the staged_ids referenced by alias_to_staged
    # (i.e. the ones we'll fall back to when the ledger lookup misses).
    staged_by_id: dict[int, dict] = {}
    if alias_to_staged:
        try:
            placeholders = ",".join("?" * len(alias_to_staged))
            cur = conn.execute(
                f"SELECT id, posting_date, amount, currency, payee, "
                f"       description, memo, source, source_ref, raw_json "
                f"FROM staged_transactions WHERE id IN ({placeholders})",
                tuple(alias_to_staged.values()),
            )
            for row in cur.fetchall():
                staged_by_id[row["id"]] = dict(row)
        except Exception as exc:  # noqa: BLE001
            log.warning("ai/suggestions: staged hydration failed: %s", exc)

    # Two separate fields:
    #  * ``lamella_txn_id`` — UUIDv7 lineage id for the immutable
    #    /txn/{id} link target (post-v3 every ledger Transaction has
    #    one; the migration backfilled lineage for every entry that
    #    lacked it).
    #  * ``txn_hash_for_link`` — the resolved content-hash used to
    #    build the ``ledger:<hash>`` action ref. The unified
    #    /api/txn/ledger:<x> endpoint matches by content hash, so the
    #    action-ref must stay a hash.
    hash_to_lineage: dict[str, str] = {
        h: get_txn_id(e) for h, e in by_hash.items() if get_txn_id(e)
    }
    if pending:
        for d in pending:
            ref = d["input_ref"]
            resolved = _canonical_hash(ref)
            txn = by_hash.get(resolved)
            # Action-ref token (ledger:<hash>): always a content hash.
            d["txn_hash_for_link"] = resolved
            # /txn/{id} link target: lineage UUID when available,
            # else the resolved hex (legacy entries pre-lineage), else
            # the raw input_ref (covers staged-only rows whose link
            # falls back to the staged surface below).
            d["lamella_txn_id"] = (
                hash_to_lineage.get(resolved) or resolved or ref
            )
            # Staged-row fallback: if no ledger txn matched but the
            # input_ref maps to a still-pending staged row (AI was
            # called at ingest before promotion), point Accept /
            # Classify at the staged surface so the action lands on
            # something real instead of the "could not load" error.
            staged_id = alias_to_staged.get(ref)
            d["staged_id_for_link"] = staged_id
            if txn is None:
                # Hydrate from the staged row when we have one — without
                # this, the card shows "could not load" even though the
                # row is sitting in the staging queue waiting for the
                # user. Source-account / entity resolution mirrors the
                # /review row hydration shape so the card looks the
                # same as a /review row.
                staged_row = staged_by_id.get(staged_id) if staged_id else None
                if staged_row:
                    src_acct = None
                    src_entity = None
                    try:
                        from lamella.web.routes.staging_review import (
                            _resolve_account_path,
                        )
                        ref_obj = _json.loads(staged_row["source_ref"]) if staged_row["source_ref"] else {}
                        # Parse raw_json so the reboot resolver branch
                        # can derive entity from captured ledger postings.
                        # All other call sites pass raw=getattr(row, "raw", None)
                        # (commit 470de1e8); this was the one missed call.
                        _raw_json_str = staged_row.get("raw_json")
                        _raw_for_resolver = (
                            _json.loads(_raw_json_str) if _raw_json_str else None
                        )
                        src_acct = _resolve_account_path(
                            conn, staged_row["source"], ref_obj,
                            raw=_raw_for_resolver,
                        )
                        if src_acct:
                            parts = src_acct.split(":")
                            if len(parts) >= 2:
                                src_entity = parts[1]
                    except Exception:  # noqa: BLE001
                        pass
                    try:
                        amt_decimal = abs(_D(str(staged_row["amount"])))
                        amt_str = f"{amt_decimal:.2f}"
                    except Exception:  # noqa: BLE001
                        amt_str = str(staged_row["amount"]) or "?"
                    d["txn_summary"] = {
                        "date": (staged_row["posting_date"] or "")[:10],
                        "amount": amt_str,
                        "currency": staged_row["currency"] or "USD",
                        "payee": staged_row["payee"] or "",
                        "narration": (staged_row["description"] or "")[:240],
                        "source_account": src_acct,
                        "source_entity": src_entity,
                        "_from_staged": True,  # flag for template if it cares
                    }
                else:
                    d["txn_summary"] = None
                continue
            primary_amt = None
            primary_currency = "USD"
            source_acct = None
            for p in txn.postings or ():
                acct = p.account or ""
                # Skip FIXME — find the real bank/card leg.
                if acct.split(":")[-1].upper() == "FIXME":
                    continue
                if not acct.startswith(("Assets:", "Liabilities:")):
                    continue
                if source_acct is None:
                    source_acct = acct
                if p.units and p.units.number is not None and primary_amt is None:
                    primary_amt = abs(_D(p.units.number))
                    primary_currency = p.units.currency or "USD"
            # Fall back to ANY posting amount if we never found a
            # bank-side leg (shouldn't happen for a normal FIXME).
            if primary_amt is None:
                for p in txn.postings or ():
                    if p.units and p.units.number is not None:
                        primary_amt = abs(_D(p.units.number))
                        primary_currency = p.units.currency or "USD"
                        break
            # Source entity = second segment of the source account
            # path (Assets:Personal:..., Liabilities:ZetaGen:...).
            source_entity = None
            if source_acct:
                parts = source_acct.split(":")
                if len(parts) >= 2:
                    source_entity = parts[1]
            d["txn_summary"] = {
                "date": str(txn.date),
                "amount": f"{primary_amt:.2f}" if primary_amt is not None else "?",
                "currency": primary_currency,
                "payee": getattr(txn, "payee", None) or "",
                "narration": (txn.narration or "")[:240],
                "source_account": source_acct,
                "source_entity": source_entity,
            }

    return request.app.state.templates.TemplateResponse(
        request, "ai_suggestions.html",
        {
            "rows": pending,
            "total": len(pending),
        },
    )


def _is_htmx_request(request: Request) -> bool:
    return request.headers.get("hx-request", "").lower() == "true"


def _render_resolved(
    request: Request, *,
    decision_id: int,
    status: str,
    summary: str,
    txn_hash: str | None,
    lamella_txn_id: str | None = None,
):
    """Same-shape replacement card for the in-place HTMX swap so the
    page doesn't scroll when the user clicks down the list. The
    "open txn →" link prefers the immutable UUIDv7 lineage id when
    available (post-v3 every Transaction has one) and falls back to
    the content-hash for the rare unresolved case."""
    return request.app.state.templates.TemplateResponse(
        request,
        "partials/_suggestion_resolved.html",
        {
            "decision_id": decision_id,
            "status": status,
            "summary": summary,
            "txn_hash": txn_hash,
            "lamella_txn_id": lamella_txn_id,
        },
    )


# /ai/suggestions/{id}/approve and /replace were retired in favor of
# the unified resource layer:
#
#   Approve  → POST /api/txn/ledger:<hash>/classify
#              with target_account = the AI's proposed target
#              (T.actions(..., proposal=...) renders the Accept button)
#
#   Replace  → POST /api/txn/ledger:<hash>/classify
#              with target_account = the user's corrected target
#              (T.actions Classify popover handles this)
#
# Both legacy endpoints are gone. The unified classify path also
# stamps any matching ai_decisions row as user_corrected so the
# suggestion drops out of the queue automatically.


@router.post("/ai/suggestions/{decision_id}/reject", response_class=HTMLResponse)
async def suggestion_reject(
    decision_id: int,
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
):
    """User rejects an AI suggestion → record the reason, don't write.

    When the user supplies a reason, fire-and-forget marks the
    decision rejected AND triggers the unified Ask-AI modal flow
    (/api/txn/ledger:<hash>/ask-ai) so the AI gets a 2nd opinion
    with the reason in its prompt context. When no reason is given,
    same as before — silent dismissal, the FIXME stays open for the
    next AI pass.
    """
    from fastapi.responses import RedirectResponse
    form = await request.form()
    raw_reason = (form.get("reason") or "").strip()
    reason = raw_reason or "(no reason)"
    try:
        conn.execute(
            "UPDATE ai_decisions SET user_corrected = 1, user_correction = ? "
            "WHERE id = ?",
            (f"rejected: {reason}", decision_id),
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("reject suggestion %s failed: %s", decision_id, exc)

    # If the user provided a reason AND we're being called via HTMX,
    # auto-open the Ask-AI modal so they get a 2nd opinion. The
    # `_is_htmx_request` gate keeps the vanilla form-post path
    # backwards-compatible (still redirects to /ai/suggestions).
    if raw_reason and _is_htmx_request(request):
        row = conn.execute(
            "SELECT input_ref FROM ai_decisions WHERE id = ?", (decision_id,),
        ).fetchone()
        target_hash = (row["input_ref"] if row else None) or ""
        # Resolve SimpleFIN ids to real txn_hashes via metadata, the
        # same way the suggestions hydration does — otherwise a
        # SimpleFIN-source decision would point Ask-AI at an opaque
        # bank id instead of the ledger entry's hash.
        resolved_pair = _resolve_decision_to_ids(target_hash, request)
        resolved = resolved_pair[0] if resolved_pair else None
        resolved_lid = resolved_pair[1] if resolved_pair else None
        if resolved:
            from fastapi.responses import HTMLResponse
            # Pass the rejection reason to the unified endpoint via
            # a synthetic form scope. Cleanest path: redirect the
            # browser to POST it themselves via a tiny inline form
            # the modal swaps in. But we have direct access — call
            # the handler.
            #
            # Easier: return a 200 with a small bootstrap that
            # triggers htmx.ajax to the unified endpoint. Keeps the
            # action-side dispatch logic in api_txn.py.
            return HTMLResponse(
                f'<div id="reject-bootstrap" '
                f'  hx-post="/api/txn/ledger:{resolved}/ask-ai" '
                f'  hx-trigger="load" '
                f'  hx-target="body" hx-swap="beforeend" '
                f'  hx-vals=\'{{"rejection_reason": "{_jsq(raw_reason)}", '
                f'  "attempt": 2, "return_url": "/ai/suggestions"}}\'>'
                f'<span class="muted small">Re-asking AI with your reason …</span>'
                f'</div>'
            )
        # Fall through to the existing resolved card if we couldn't
        # find a matching txn (input_ref isn't a hash and no metadata
        # match in the ledger).
        return _render_resolved(
            request, decision_id=decision_id, status="rejected",
            summary=f"Reason: {reason}. Couldn't auto-rerun AI — the "
                    "underlying transaction wasn't findable in the ledger.",
            txn_hash=target_hash if target_hash else None,
            lamella_txn_id=resolved_lid,
        )

    if _is_htmx_request(request):
        row = conn.execute(
            "SELECT input_ref FROM ai_decisions WHERE id = ?", (decision_id,),
        ).fetchone()
        ref = row["input_ref"] if row else None
        # Try to resolve to lineage id so the "open txn →" link can
        # use the immutable /txn/{uuid} URL. Falls back to the raw
        # input_ref (legacy hex hash or SimpleFIN id) if the entry
        # isn't findable in the current ledger snapshot.
        ref_pair = _resolve_decision_to_ids(ref or "", request) if ref else None
        return _render_resolved(
            request, decision_id=decision_id, status="rejected",
            summary=f"Reason: {reason}. The FIXME stays open and the next "
                    "AI pass will see your reason as negative-example context.",
            txn_hash=ref,
            lamella_txn_id=ref_pair[1] if ref_pair else None,
        )
    return RedirectResponse("/ai/suggestions?rejected=1", status_code=303)


def _jsq(s: str) -> str:
    """JSON-string-safe escape for embedding inside an inline JSON
    literal in an attribute value. Sufficient for hx-vals — escapes
    backslashes, double-quotes, and ASCII control chars."""
    return (
        s.replace("\\", "\\\\")
         .replace('"', '\\"')
         .replace("\n", " ")
         .replace("\r", " ")
    )


def _resolve_decision_to_txn_hash(
    input_ref: str, request: Request,
) -> str | None:
    """Resolve an ai_decisions.input_ref to a real ledger txn_hash.
    Mirrors the lookup in the /ai/suggestions hydration path:
    bulk-classify decisions store the txn_hash directly; SimpleFIN
    ingest decisions store the bank's txn_id and the hash is reachable
    via `lamella-simplefin-id` metadata on the ledger entry that
    eventually got written.

    Returns None when no match — the caller falls back to a no-rerun
    rejection."""
    pair = _resolve_decision_to_ids(input_ref, request)
    return pair[0] if pair else None


def _resolve_decision_to_ids(
    input_ref: str, request: Request,
) -> tuple[str, str | None] | None:
    """Same as ``_resolve_decision_to_txn_hash`` but also returns the
    matched entry's ``lamella-txn-id`` (UUIDv7 lineage) when present.
    Returns ``(txn_hash, lamella_txn_id_or_None)`` on match, ``None``
    on no match. Callers building /txn/ links should use the lineage
    id; callers that need to do content-hash joins should use the
    hash. Post-v3 every Transaction has a lineage id, so the second
    element is rarely None in practice."""
    if not input_ref:
        return None
    try:
        reader = request.app.state.reader
    except AttributeError:
        from lamella.core.config import Settings
        from lamella.core.beancount_io import LedgerReader
        reader = LedgerReader(Settings().ledger_main)
    from beancount.core.data import Transaction
    from lamella.core.beancount_io.txn_hash import txn_hash as _th
    from lamella.core.identity import find_source_reference, get_txn_id
    entries = list(reader.load().entries)
    for e in entries:
        if not isinstance(e, Transaction):
            continue
        h = _th(e)
        if h == input_ref:
            return (h, get_txn_id(e))
        # Lineage UUID match — every post-Phase-3 decision logs
        # input_ref=lamella-txn-id when the entry has a lineage.
        if get_txn_id(e) == input_ref:
            return (h, get_txn_id(e))
        # SimpleFIN bridge id match — ingest-time decisions.
        if find_source_reference(e, "simplefin") == input_ref:
            return (h, get_txn_id(e))
    return None


# /ai/suggestions/{id}/replace retired — /api/txn/ledger:<hash>/classify
# is the canonical write path now. The shared _apply_suggestion writer
# was deleted along with its callers.


@router.get("/ai/decisions/{decision_id}", response_class=HTMLResponse)
def decision_detail(
    decision_id: int,
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
):
    """Show the full prompt + response + metadata for one AI call.

    The user asked "how does it actually know, what request was sent,
    was vector data included, was cluster context passed" — this is
    the answer. Renders the exact system + user strings that were
    sent to OpenRouter, the structured response that came back, token
    counts, model, and cache status. Captures started with migration
    046; older rows show "(pre-capture)" for the prompt fields.
    """
    import json as _json
    row = conn.execute(
        "SELECT * FROM ai_decisions WHERE id = ?", (decision_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="decision not found")
    d = dict(row)
    try:
        d["result_parsed"] = _json.loads(d["result"]) if d.get("result") else None
    except Exception:  # noqa: BLE001
        d["result_parsed"] = None
    # Resolve the input_ref to a usable link. Some classify_txn rows
    # carry a SimpleFIN id (TRN-…) that won't 404-cleanly at /txn/.
    # Build the same SimpleFIN-id → txn_hash map the audit page uses
    # so the "open transaction →" link goes somewhere real.
    sf_id_to_hash: dict[str, str] = {}
    resolved_hash: str | None = None
    txn_summary: dict | None = None
    if d.get("decision_type") == "classify_txn" and d.get("input_ref"):
        ref = d["input_ref"]
        if _looks_like_txn_hash(ref):
            resolved_hash = ref
        else:
            sf_id_to_hash = _build_simplefin_id_to_hash_map(
                request.app.state,
            )
            resolved_hash = sf_id_to_hash.get(ref)
        if resolved_hash:
            txn_summary = _lookup_txn_summary(
                request.app.state, resolved_hash,
            )
    d["source_href"] = _source_href(
        d.get("decision_type"), d.get("input_ref") or "",
        sf_id_to_hash=sf_id_to_hash,
    )
    d["resolved_hash"] = resolved_hash
    d["txn_summary"] = txn_summary
    return request.app.state.templates.TemplateResponse(
        request, "ai_decision_detail.html", {"d": d},
    )


def _lookup_txn_summary(app_state, txn_hash_value: str) -> dict | None:
    """Pull the date / amount / payee / narration / accounts for one
    ledger txn so the audit-detail page can show what the AI was
    classifying — without making the user click through and trust
    the link."""
    reader = getattr(app_state, "ledger_reader", None)
    if reader is None:
        return None
    try:
        entries = reader.load().entries
    except Exception:  # noqa: BLE001
        return None
    from beancount.core.data import Transaction
    from lamella.core.beancount_io.txn_hash import txn_hash
    for e in entries:
        if not isinstance(e, Transaction):
            continue
        if txn_hash(e) != txn_hash_value:
            continue
        # Summarise. Pick a representative non-zero amount on the
        # first non-FIXME posting; the user just wants "what was
        # it" not the full posting structure.
        amount_str = ""
        currency = ""
        accounts: list[str] = []
        for p in e.postings or ():
            if p.account:
                accounts.append(p.account)
            if (
                not amount_str and p.units is not None
                and p.units.number is not None
            ):
                amount_str = f"{abs(p.units.number):.2f}"
                currency = p.units.currency or "USD"
        return {
            "date": e.date,
            "payee": getattr(e, "payee", None) or "",
            "narration": getattr(e, "narration", None) or "",
            "amount": amount_str,
            "currency": currency,
            "accounts": accounts,
        }
    return None


@router.post("/ai/retry/{decision_id}")
async def retry_decision(
    decision_id: int,
    ai: AIService = Depends(get_ai_service),
    conn: sqlite3.Connection = Depends(get_db),
    settings: Settings = Depends(get_settings),
):
    """Re-run a past AI decision. Writes a new `ai_decisions` row and
    leaves the original untouched. For ``receipt_verify``, this
    actually re-invokes the verify-and-correct flow against the
    same Paperless document so a prior failure (bad model ID, OCR
    provider flake, schema mismatch) can be recovered in one
    click. Other decision types still log a placeholder row."""
    if not ai.enabled:
        raise HTTPException(status_code=503, detail="AI is disabled.")
    if ai.spend_cap_reached():
        raise HTTPException(status_code=429, detail="AI budget exhausted.")
    dlog = DecisionsLog(conn)
    original = dlog.get(decision_id)
    if original is None:
        raise HTTPException(status_code=404, detail="decision not found")

    if original.decision_type == "receipt_verify":
        if not settings.paperless_configured:
            raise HTTPException(
                status_code=503,
                detail="Paperless is not configured; cannot re-run receipt_verify.",
            )
        paperless_id = _parse_paperless_id(original.input_ref)
        if paperless_id is None:
            raise HTTPException(
                status_code=400,
                detail=f"cannot parse paperless id from input_ref={original.input_ref!r}",
            )
        from lamella.features.paperless_bridge.verify import VerifyService
        paperless = PaperlessClient(
            base_url=settings.paperless_url,  # type: ignore[arg-type]
            api_token=settings.paperless_api_token.get_secret_value(),  # type: ignore[union-attr]
            extra_headers=settings.paperless_extra_headers(),
        )
        try:
            service = VerifyService(ai=ai, paperless=paperless, conn=conn)
            outcome = await service.verify_and_correct(paperless_id)
        finally:
            await paperless.aclose()
        return {
            "status": "ran",
            "of": original.id,
            "paperless_id": paperless_id,
            "verified": outcome.verified,
            "fields_patched": outcome.fields_patched,
            "skipped_reason": outcome.skipped_reason,
        }

    # Other decision types: echo a new decision row with the same
    # input_ref pointing at the original so the audit UI can link them.
    dlog.log(
        decision_type=original.decision_type,
        input_ref=original.input_ref,
        model=ai.model_for(original.decision_type),
        result={"retry_of": original.id, "note": "queued for re-run"},
    )
    return {"status": "queued", "of": original.id}


def _parse_paperless_id(input_ref: str) -> int | None:
    """Input_refs for receipt_verify look like ``paperless:17140``
    (vision tier) or ``paperless:17140:ocr_text`` (ocr-text tier).
    Return the integer id, or None when the format doesn't match."""
    if not input_ref or not input_ref.startswith("paperless:"):
        return None
    try:
        return int(input_ref.split(":", 2)[1])
    except (IndexError, ValueError):
        return None


def _decorate(
    row,
    *,
    sf_id_to_hash: dict[str, str] | None = None,
) -> dict[str, Any]:
    return {
        "id": row.id,
        "decided_at": row.decided_at,
        "decision_type": row.decision_type,
        "input_ref": row.input_ref,
        "model": row.model,
        "prompt_tokens": row.prompt_tokens or 0,
        "completion_tokens": row.completion_tokens or 0,
        "user_corrected": row.user_corrected,
        "user_correction": row.user_correction,
        "result": row.result,
        "source_href": _source_href(
            row.decision_type, row.input_ref,
            sf_id_to_hash=sf_id_to_hash,
        ),
    }


_HEX_CHARS = frozenset("0123456789abcdefABCDEF")


def _looks_like_txn_hash(value: str) -> bool:
    """A Beancount txn_hash in this repo is a hex digest (sha-1 →
    40 chars, sha-256 → 64). Anything else — SimpleFIN ids
    (TRN-<uuid>), staged-row prefixes (staged:42), paperless
    composite keys (paperless:1234) — is NOT a hash and must not
    be routed to /txn/, which would 404."""
    if not value or len(value) not in (40, 64):
        return False
    return all(c in _HEX_CHARS for c in value)


def _resolve_txn_hash_for_input_ref(
    input_ref: str, app_state
) -> str | None:
    """When the input_ref isn't itself a txn_hash, try to find a
    ledger entry that carries it as its `lamella-simplefin-id` (or
    legacy `simplefin-id`) metadata. Returns the canonical
    txn_hash when matched, else None.

    The audit page renders 100 rows by default; this lookup is
    cheap because LedgerReader caches the parsed entries across
    requests, but we still scan once and build a mapping the
    caller can reuse — see audit_page().
    """
    reader = getattr(app_state, "ledger_reader", None)
    if reader is None:
        return None
    try:
        entries = reader.load().entries
    except Exception:  # noqa: BLE001
        return None
    from beancount.core.data import Transaction
    from lamella.core.beancount_io.txn_hash import txn_hash
    from lamella.core.identity import find_source_reference
    for e in entries:
        if not isinstance(e, Transaction):
            continue
        sf_id = find_source_reference(e, "simplefin")
        if sf_id and str(sf_id) == input_ref:
            return txn_hash(e)
    return None


def _build_simplefin_id_to_hash_map(app_state) -> dict[str, str]:
    """One-shot scan of the ledger producing
    ``{any_alias_id: txn_hash}`` so the audit page can resolve N
    rows in one pass instead of N walks.

    Despite the historical name, this map covers every non-txn-hash
    input_ref shape an AI decision can carry:
      * lineage UUID (`lamella-txn-id`)  — post-Phase-3 default
      * SimpleFIN bridge id              — ingest-time calls
    Callers look up `input_ref` directly; if it matches either
    alias type the resolved value is the entry's txn_hash.
    """
    reader = getattr(app_state, "ledger_reader", None)
    if reader is None:
        return {}
    try:
        entries = reader.load().entries
    except Exception:  # noqa: BLE001
        return {}
    from beancount.core.data import Transaction
    from lamella.core.beancount_io.txn_hash import txn_hash
    from lamella.core.identity import find_source_reference, get_txn_id
    out: dict[str, str] = {}
    for e in entries:
        if not isinstance(e, Transaction):
            continue
        h = txn_hash(e)
        sf_id = find_source_reference(e, "simplefin")
        if sf_id:
            out[str(sf_id)] = h
        lineage = get_txn_id(e)
        if lineage:
            out[lineage] = h
    return out


def _source_href(
    decision_type: str,
    input_ref: str,
    *,
    sf_id_to_hash: dict[str, str] | None = None,
) -> str | None:
    if decision_type == "classify_txn":
        if not input_ref:
            return "/inbox"
        # Real Beancount txn_hash → link directly.
        if _looks_like_txn_hash(input_ref):
            return f"/txn/{input_ref}"
        # SimpleFIN id (TRN-<uuid> or similar) — try to resolve to
        # the canonical txn_hash via ledger metadata. Falls back
        # to the staged review page when the row never made it
        # into the ledger (post-C1, on-demand Ask-AI proposals,
        # etc.) so the user lands somewhere useful instead of a
        # 404.
        if sf_id_to_hash is not None and input_ref in sf_id_to_hash:
            return f"/txn/{sf_id_to_hash[input_ref]}"
        return "/inbox"
    if decision_type == "match_receipt":
        return "/documents"
    if decision_type == "parse_note":
        return "/notes"
    if decision_type in ("receipt_verify", "receipt_enrich"):
        # input_ref is "paperless:<id>" or "paperless:<id>:<tier>".
        pid = _parse_paperless_id(input_ref)
        if pid is not None:
            return f"/paperless/preview/{pid}"
    return None

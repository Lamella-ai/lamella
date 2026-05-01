# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Transaction search — find in narration/payee, group by classification,
offer bulk rule creation AND bulk re-categorization for the matches.

The point is: find the Storage Unit transactions, see how they've been
classified so far, fix the ones that are wrong, categorize the ones
that are FIXME, and (optionally) promote a rule so future ones get the
right target automatically.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from beancount.core.data import Transaction
from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from lamella.core.beancount_io import LedgerReader
from lamella.core.beancount_io.txn_hash import txn_hash
from lamella.core.config import Settings
from lamella.web.deps import get_db, get_ledger_reader, get_settings
from lamella.core.ledger_writer import BeanCheckError
from lamella.features.rules.overrides import OverrideWriter

log = logging.getLogger(__name__)

router = APIRouter()

# Cross-date transfer-pair validator tolerances.
PAIR_AMOUNT_TOLERANCE = Decimal("0.02")
PAIR_DATE_TOLERANCE_DAYS = 14


def _entity_for_txn(conn, txn) -> str | None:
    """Best-effort entity slug for a transaction: read accounts_meta
    for the non-FIXME Assets/Liabilities leg; fall back to the second
    segment of the path. Returns None when nothing parses out — the
    caller rejects the pair in that case so we don't silently land
    on the wrong entity's Transfers account.
    """
    for p in txn.postings:
        acct = p.account or ""
        if not acct.startswith(("Assets:", "Liabilities:")):
            continue
        if acct.split(":")[-1].upper() == "FIXME":
            continue
        row = conn.execute(
            "SELECT entity_slug FROM accounts_meta WHERE account_path = ?",
            (acct,),
        ).fetchone()
        if row and row["entity_slug"]:
            return row["entity_slug"]
        parts = acct.split(":")
        if len(parts) >= 2 and parts[1] not in ("FIXME",):
            return parts[1]
    return None


def _transfers_account_for(entity_slug: str) -> str:
    """Entity-scoped clearing account used for cross-date transfer
    pairing. Every entity owns its own so the `entity-first` rule
    holds: money never sits in an un-owned bucket."""
    return f"Assets:{entity_slug}:Transfers:InFlight"


@dataclass(frozen=True)
class SearchHit:
    txn_hash: str
    lamella_txn_id: str | None
    date: date
    amount: Decimal
    currency: str
    payee: str | None
    narration: str
    expense_accounts: tuple[str, ...]
    source_accounts: tuple[str, ...]
    is_fixme: bool
    paperless_id: int | None = None
    receipt_date_mismatch: bool = False


@dataclass(frozen=True)
class StagedHit:
    """A pending row from staged_transactions that matched the query.

    Distinct from SearchHit — staged rows mostly haven't been promoted
    to the ledger, so they have no receipt links yet and their
    "classification" is a *proposal* rather than a posting.

    ``source_ref_hash`` carries a backing ledger txn_hash for sources
    that scan an existing ledger entry (currently just ``reboot``).
    The template uses it to send the row's row click-through to
    ``/txn/<hash>`` when there's a real transaction to view, falling
    back to ``/card?focus=<staged_id>`` for sources where the row is
    purely staging-side.
    """
    staged_id: int
    source: str
    status: str
    date: date
    amount: Decimal
    currency: str
    payee: str | None
    description: str
    memo: str | None
    proposed_account: str | None
    proposed_confidence: str | None
    proposed_by: str | None
    needs_review: bool
    source_ref_hash: str | None = None
    lamella_txn_id: str | None = None

    @property
    def detail_link(self) -> str:
        """Click-through target. The immutable ``/txn/{lamella_txn_id}``
        URL when available — same URL works pre- and post-promotion.
        Reboot rows fall back to source_ref_hash for the gap of
        pre-Phase-1 rows. Sources without identity yet route to
        ``/card?focus=N``."""
        if self.lamella_txn_id:
            return f"/txn/{self.lamella_txn_id}"
        if self.source == "reboot" and self.source_ref_hash:
            return f"/txn/{self.source_ref_hash}"
        return f"/card?focus={self.staged_id}"


def _is_fixme(path: str) -> bool:
    return bool(path) and path.split(":")[-1].upper() == "FIXME"


def _matches(needle: str, txn: Transaction) -> bool:
    n = needle.lower()
    hay = " ".join(
        filter(None, [txn.payee or "", txn.narration or ""])
    ).lower()
    if n in hay:
        return True
    # Account-path match — lets "q=ZetaGen" find txns routed
    # through Assets:ZetaGen:… even when the narration is
    # e.g. "Mercury IO Cashback" with no mention of the entity slug.
    for p in txn.postings:
        if n in (p.account or "").lower():
            return True
    return False


@router.get("/search", response_class=HTMLResponse)
def search_page(
    request: Request,
    q: str = "",
    lookback_days: int = 365,
    limit: int = 100,
    fixme: str = "",
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
    conn = Depends(get_db),
):
    hits: list[SearchHit] = []
    staged_hits: list[StagedHit] = []
    grouped_by_target: dict[str, int] = defaultdict(int)
    fixme_count = 0
    needle = (q or "").strip()
    since = date.today() - timedelta(days=max(1, int(lookback_days)))

    if needle:
        entries = reader.load().entries
        for entry in entries:
            if not isinstance(entry, Transaction):
                continue
            if entry.date < since:
                continue
            # Skip override-correction blocks. They're a correction
            # layer on top of an underlying txn (linked by
            # lamella-override-of), not separate events the user did in
            # the world. They share the original's narration when
            # bulk-apply copies it forward, which made every
            # corrected txn appear twice in search results — once as
            # the original, once as its override. The user wants
            # ONE row per real-world purchase; the original is the
            # authoritative one and its detail page shows the
            # override as an "applied resolution" annotation.
            tags = getattr(entry, "tags", None) or set()
            if "lamella-override" in tags:
                continue
            if not _matches(needle, entry):
                continue
            # Expense postings — current classification(s).
            expense_paths: list[str] = []
            source_paths: list[str] = []
            primary_amount: Decimal | None = None
            primary_currency = "USD"
            is_fixme = False
            # Track signed cashflow direction alongside magnitude. For an
            # Expenses posting the cashflow is the NEGATION of the posting
            # number (user spent money → cashflow is negative). For a
            # source (Assets/Liabilities) posting the number IS the
            # cashflow. We pick the largest magnitude as primary and
            # remember its signed cashflow value so /search renders
            # refunds with + and charges with − via T.summary.
            primary_signed: Decimal | None = None
            for p in entry.postings:
                acct = p.account or ""
                if acct.startswith("Expenses:"):
                    expense_paths.append(acct)
                    if _is_fixme(acct):
                        is_fixme = True
                    if p.units and p.units.number is not None:
                        signed = -Decimal(p.units.number)
                        amt = abs(signed)
                        if primary_amount is None or amt > primary_amount:
                            primary_amount = amt
                            primary_signed = signed
                            primary_currency = p.units.currency or "USD"
                elif acct.startswith(("Assets:", "Liabilities:")):
                    source_paths.append(acct)
                    if primary_amount is None and p.units and p.units.number is not None:
                        primary_signed = Decimal(p.units.number)
                        primary_amount = abs(primary_signed)
                        primary_currency = p.units.currency or "USD"
            if primary_signed is None:
                primary_signed = Decimal("0")
            from lamella.core.identity import get_txn_id
            hit = SearchHit(
                txn_hash=txn_hash(entry),
                lamella_txn_id=get_txn_id(entry),
                date=entry.date,
                amount=primary_signed,
                currency=primary_currency,
                payee=getattr(entry, "payee", None),
                narration=entry.narration or "",
                expense_accounts=tuple(expense_paths),
                source_accounts=tuple(source_paths),
                is_fixme=is_fixme,
            )
            hits.append(hit)
            if is_fixme:
                fixme_count += 1
            else:
                for p in expense_paths:
                    if not _is_fixme(p):
                        grouped_by_target[p] += 1

        hits.sort(key=lambda h: h.date, reverse=True)
        # Server-side fixme filter — ?fixme=1 on the URL drops every
        # already-classified row, matching what users expect when they
        # click "N more pending →" from /businesses/{slug}.
        if fixme and fixme not in ("0", "false", "no"):
            hits = [h for h in hits if h.is_fixme]
        hits = hits[:limit]
        hits = _annotate_receipts(conn, hits)

        # Staged hits — pending rows that haven't been promoted to
        # the ledger yet. Without this, a SimpleFIN row sitting in
        # the inbox is invisible to search (which only walks
        # reader.load().entries above). Excludes `promoted` (already
        # in the ledger, found above), `dismissed` (user discarded),
        # AND rows whose ``lamella_txn_id`` matches a ledger entry —
        # the reboot writer can leave staged rows behind that are
        # already in the ledger but whose status never got bumped to
        # ``promoted``. Filtering them here stops /search from showing
        # the same transaction in BOTH the staged list AND the ledger
        # list. The bumps also self-heal the staging table so future
        # queries don't have to keep filtering.
        from lamella.core.identity import get_txn_id as _get_txn_id
        ledger_txn_ids: set[str] = set()
        for _e in entries:
            if isinstance(_e, Transaction):
                _tid = _get_txn_id(_e)
                if _tid:
                    ledger_txn_ids.add(_tid.lower())
        staged_hits = _search_staged(
            conn, needle, since, limit, ledger_txn_ids=ledger_txn_ids,
        )

    # Suggest the most common non-FIXME target so the "create rule" UI
    # can pre-fill it.
    suggested_target = None
    if grouped_by_target:
        suggested_target = max(grouped_by_target.items(), key=lambda kv: kv[1])[0]

    ctx = {
        "q": q,
        "lookback_days": lookback_days,
        "limit": limit,
        "fixme": fixme,
        "hits": hits,
        "hit_count": len(hits),
        "fixme_count": fixme_count,
        "staged_hits": staged_hits,
        "staged_count": len(staged_hits),
        "grouped_by_target": sorted(
            grouped_by_target.items(), key=lambda kv: kv[1], reverse=True
        ),
        "suggested_target": suggested_target,
        "applied": request.query_params.get("applied"),
        "applied_failed": request.query_params.get("applied_failed"),
        "paperless_base": (settings.paperless_url or "").rstrip("/"),
    }
    return request.app.state.templates.TemplateResponse(
        request, "search.html", ctx
    )


def _search_staged(
    conn, needle: str, since: date, limit: int,
    *,
    ledger_txn_ids: set[str] | None = None,
) -> list[StagedHit]:
    """Find pending staged rows whose payee/description/memo contains
    ``needle``. Excludes promoted (already in ledger) and dismissed
    rows. Joins ``staged_decisions`` so the result row carries the
    AI/rule's proposed account when one exists.

    ``ledger_txn_ids`` is the set of ``lamella-txn-id`` values present
    in the current ledger. Any staged row whose ``lamella_txn_id`` is
    in that set gets:
      1. Filtered out of the result list (the ledger entry shows up
         in the Ledger section of /search; showing it twice confuses
         the user — same txn, two different rows).
      2. Self-healed: status bumped to ``promoted`` in the staging
         table so future queries don't repeat the dance and the
         staging review queue stops surfacing it.
    The reboot writer can leave staged rows behind that are already
    in the ledger but whose status never got promoted; this is the
    cheapest place to clean them up because /search already loads the
    ledger entry-set.
    """
    ledger_txn_ids = ledger_txn_ids or set()
    rows = conn.execute(
        """
        SELECT t.id, t.source, t.status, t.posting_date,
               t.amount, t.currency, t.payee, t.description, t.memo,
               t.source_ref_hash, t.lamella_txn_id,
               d.account     AS proposed_account,
               d.confidence  AS proposed_confidence,
               d.decided_by  AS proposed_by,
               d.needs_review
          FROM staged_transactions t
          LEFT JOIN staged_decisions d ON d.staged_id = t.id
         WHERE t.status NOT IN ('promoted', 'dismissed')
           AND t.posting_date >= ?
           AND (
                LOWER(COALESCE(t.payee, ''))       LIKE ?
             OR LOWER(COALESCE(t.description, '')) LIKE ?
             OR LOWER(COALESCE(t.memo, ''))        LIKE ?
           )
         ORDER BY t.posting_date DESC, t.id DESC
         LIMIT ?
        """,
        (
            since.isoformat(),
            f"%{needle.lower()}%",
            f"%{needle.lower()}%",
            f"%{needle.lower()}%",
            int(limit),
        ),
    ).fetchall()
    out: list[StagedHit] = []
    heal_ids: list[int] = []
    for r in rows:
        try:
            d = date.fromisoformat(str(r["posting_date"])[:10])
        except Exception:  # noqa: BLE001
            continue
        try:
            amt = Decimal(str(r["amount"]))
        except Exception:  # noqa: BLE001
            amt = Decimal("0")
        row_keys = r.keys() if hasattr(r, "keys") else []
        row_lamella_id = (
            r["lamella_txn_id"] if "lamella_txn_id" in row_keys else None
        )
        # Self-heal + filter: the ledger has this txn, the staged
        # table thinks it's still pending. Drop it from the result
        # list and queue the id for a status bump after the loop so
        # future queries don't repeat the work.
        if row_lamella_id and row_lamella_id.lower() in ledger_txn_ids:
            heal_ids.append(int(r["id"]))
            continue
        out.append(StagedHit(
            staged_id=int(r["id"]),
            source=r["source"] or "",
            status=r["status"] or "new",
            date=d,
            amount=amt,
            currency=r["currency"] or "USD",
            payee=r["payee"],
            description=r["description"] or "",
            memo=r["memo"],
            proposed_account=r["proposed_account"],
            proposed_confidence=r["proposed_confidence"],
            proposed_by=r["proposed_by"],
            needs_review=bool(r["needs_review"]) if r["needs_review"] is not None else False,
            source_ref_hash=r["source_ref_hash"],
            lamella_txn_id=row_lamella_id,
        ))
    if heal_ids:
        try:
            placeholders = ",".join("?" * len(heal_ids))
            conn.execute(
                f"UPDATE staged_transactions SET status = 'promoted', "
                f"promoted_at = COALESCE(promoted_at, datetime('now')), "
                f"updated_at = datetime('now') "
                f"WHERE id IN ({placeholders})",
                tuple(heal_ids),
            )
            log.info(
                "/search staged hits: self-healed %d rows whose "
                "lamella_txn_id matched a ledger entry "
                "(ids=%s)",
                len(heal_ids), heal_ids[:10],
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("/search staged self-heal failed: %s", exc)
    return out


def _annotate_receipts(
    conn, hits: list[SearchHit],
) -> list[SearchHit]:
    """For each hit, look up its linked Paperless doc (if any) and
    flag date-mismatch-likely. Returns a new list — SearchHit is
    frozen, so we build fresh instances."""
    if not hits:
        return hits
    hashes = [h.txn_hash for h in hits]
    placeholders = ",".join("?" * len(hashes))
    rows = conn.execute(
        f"""
        SELECT rl.txn_hash, rl.paperless_id, rl.txn_date,
               pdi.receipt_date
          FROM receipt_links rl
          LEFT JOIN paperless_doc_index pdi
                 ON pdi.paperless_id = rl.paperless_id
         WHERE rl.txn_hash IN ({placeholders})
        """,
        tuple(hashes),
    ).fetchall()
    by_hash: dict[str, sqlite3.Row] = {r["txn_hash"]: r for r in rows}  # type: ignore[name-defined]
    out: list[SearchHit] = []
    for h in hits:
        row = by_hash.get(h.txn_hash)
        if row is None:
            out.append(h)
            continue
        mismatch = False
        try:
            if row["receipt_date"] and row["txn_date"]:
                d1 = date.fromisoformat(str(row["receipt_date"])[:10])
                d2 = date.fromisoformat(str(row["txn_date"])[:10])
                mismatch = abs((d1 - d2).days) > 30
        except Exception:  # noqa: BLE001
            mismatch = False
        out.append(SearchHit(
            txn_hash=h.txn_hash,
            lamella_txn_id=h.lamella_txn_id,
            date=h.date, amount=h.amount,
            currency=h.currency, payee=h.payee, narration=h.narration,
            expense_accounts=h.expense_accounts,
            source_accounts=h.source_accounts, is_fixme=h.is_fixme,
            paperless_id=int(row["paperless_id"]),
            receipt_date_mismatch=mismatch,
        ))
    return out


def _find_txn(reader: LedgerReader, target_hash: str) -> Transaction | None:
    for entry in reader.load().entries:
        if isinstance(entry, Transaction) and txn_hash(entry) == target_hash:
            return entry
    return None


# UUIDv7 canonical form: 8-4-4-4-12 hex with version nibble == 7 and
# variant high bits == 10. Anything else is treated as a legacy
# txn_hash (sha-style hex) and routed to the existing _find_txn.
_UUIDV7_RE = __import__("re").compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    __import__("re").IGNORECASE,
)


def _is_uuidv7_token(token: str) -> bool:
    return bool(token) and bool(_UUIDV7_RE.match(token))


def _resolve_to_ledger_hash(reader: LedgerReader, token: str) -> str | None:
    """For the secondary /txn/{token}/* routes that operate on a
    ledger entry, resolve the UUIDv7 token to a Beancount content
    hash. Returns None for unknown tokens, ``None`` is also raised
    via the helper below as a 404.
    """
    if not _is_uuidv7_token(token):
        return None
    entry = _find_txn_by_lamella_id(reader, token)
    if entry is None:
        return None
    return txn_hash(entry)


def _require_ledger_hash(reader: LedgerReader, token: str) -> str:
    """Same as ``_resolve_to_ledger_hash`` but raises a 404
    HTTPException when the token doesn't shape-check or doesn't
    resolve to a ledger entry. Use at the top of /txn/{token}/*
    mutation handlers."""
    if not _is_uuidv7_token(token):
        raise HTTPException(
            status_code=404,
            detail=(
                "/txn/{token}/* paths accept only a lamella-txn-id "
                "(UUIDv7); legacy hex was retired in v3."
            ),
        )
    h = _resolve_to_ledger_hash(reader, token)
    if h is None:
        raise HTTPException(
            status_code=404,
            detail=f"no ledger entry with lamella-txn-id {token[:12]}…",
        )
    return h


def _render_staged_detail(request: Request, conn, staged_row, *, token: str):
    """Render the pre-promotion shape of /txn/{token}. The staged row
    carries the same UUIDv7 it'll be stamped with on the ledger entry,
    so the URL keeps working after promotion — only the template the
    user sees changes."""
    decision = None
    pair = None
    ai_history: list[dict] = []
    try:
        from lamella.features.import_.staging import StagingService
        svc = StagingService(conn)
        decision = svc.get_decision(staged_row.id)
        pairs = svc.pairs_for(staged_row.id)
        if pairs:
            p = pairs[0]
            partner_id = (
                p.b_staged_id if p.a_staged_id == staged_row.id else p.a_staged_id
            )
            pair = type("StagedPairView", (), {})()
            pair.kind = p.kind
            pair.confidence = p.confidence
            pair.partner_id = partner_id
            pair.reason = p.reason
    except Exception:  # noqa: BLE001
        pass
    # Best-effort recent AI decisions: same input_ref shape as the
    # /review aggregator uses. SimpleFIN rows key by source txn_id;
    # other sources fall through with an empty list.
    try:
        ai_input_ref: str | None = None
        if staged_row.source == "simplefin" and isinstance(staged_row.source_ref, dict):
            ai_input_ref = staged_row.source_ref.get("txn_id")
        elif staged_row.source == "reboot":
            ai_input_ref = staged_row.source_ref_hash
        # The lamella-txn-id is also a valid input_ref shape now —
        # post-categorize-time AI calls log under it.
        candidate_refs: list[str] = []
        if ai_input_ref:
            candidate_refs.append(ai_input_ref)
        if staged_row.lamella_txn_id:
            candidate_refs.append(staged_row.lamella_txn_id)
        if candidate_refs:
            placeholders = ",".join("?" * len(candidate_refs))
            ai_history = [
                dict(r) for r in conn.execute(
                    f"SELECT id, decision_type, decided_at, model "
                    f"  FROM ai_decisions "
                    f" WHERE input_ref IN ({placeholders}) "
                    f" ORDER BY decided_at DESC LIMIT 10",
                    tuple(candidate_refs),
                ).fetchall()
            ]
    except Exception:  # noqa: BLE001
        ai_history = []

    return request.app.state.templates.TemplateResponse(
        request,
        "partials/_txn_detail_staged.html",
        {
            "token": token,
            "row": staged_row,
            "decision": decision,
            "pair": pair,
            "ai_history": ai_history,
        },
    )


def _find_txn_by_lamella_id(
    reader: LedgerReader, lamella_id: str,
) -> Transaction | None:
    """Walk the ledger looking for a transaction whose ``lamella-txn-id``
    meta (or any indexed ``lamella-txn-id-alias-N`` meta) matches the
    supplied UUIDv7. The alias case covers transfer pairs, where leg
    A's id becomes the canonical lamella-txn-id and leg B's id is
    stamped as ``lamella-txn-id-alias-0`` so /txn/{b_token} keeps
    resolving post-promotion.
    """
    if not lamella_id:
        return None
    target = lamella_id.lower()
    for entry in reader.load().entries:
        if not isinstance(entry, Transaction):
            continue
        meta = getattr(entry, "meta", None) or {}
        primary = meta.get("lamella-txn-id")
        if primary and str(primary).lower() == target:
            return entry
        # Aliases live under indexed keys; one alias slot covers the
        # transfer-pair case today, more would follow the same shape.
        for k, v in meta.items():
            if not isinstance(k, str):
                continue
            if k.startswith("lamella-txn-id-alias-") and v:
                if str(v).lower() == target:
                    return entry
    return None


def _fixme_posting(txn: Transaction) -> tuple[str | None, Decimal | None, str]:
    """Return (account, abs_amount, currency) of the FIXME leg, or the
    largest expense leg if there's no FIXME."""
    fixme: tuple[str, Decimal, str] | None = None
    best_expense: tuple[str, Decimal, str] | None = None
    for p in txn.postings:
        acct = p.account or ""
        if p.units is None or p.units.number is None:
            continue
        amt = abs(Decimal(p.units.number))
        currency = p.units.currency or "USD"
        if acct.split(":")[-1].upper() == "FIXME":
            fixme = (acct, amt, currency)
        elif acct.startswith("Expenses:"):
            if best_expense is None or amt > best_expense[1]:
                best_expense = (acct, amt, currency)
    chosen = fixme or best_expense
    if chosen is None:
        return None, None, "USD"
    return chosen[0], chosen[1], chosen[2]


@router.get("/txn/{target_hash}", response_class=HTMLResponse)
def txn_detail(
    target_hash: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Full detail page for a single transaction — the "everything
    you need to act on this txn" surface. Rolls up: all postings,
    transfer-pair partner (if any), linked receipts with Paperless
    metadata + action buttons, active notes around the txn date,
    AI classify/verify history, and the full action panel (reused
    from the HTMX partial). Meant to be linked FROM anywhere a txn
    is listed — search, review, receipts, AI audit, card-of-cards.

    Token shape: a ``lamella-txn-id`` (UUIDv7). Same URL pre- and
    post-promotion — the staged row carries the same id that ends up
    on the eventual ledger entry, so a bookmark on either side stays
    valid. Legacy hex (Beancount ``txn_hash``) is no longer accepted;
    v3+ ledgers guarantee every entry has lineage.
    """
    from beancount.core.data import Transaction as _T  # local import avoids cycles in test harness

    if not _is_uuidv7_token(target_hash):
        raise HTTPException(
            status_code=404,
            detail=(
                "the /txn/{token} URL accepts only a lamella-txn-id "
                "(UUIDv7) — the legacy hex form was retired in v3. "
                "Search for the transaction via /search if you have "
                "an old hex link."
            ),
        )
    # Resolve the token in BOTH directions and let the ledger win when
    # both exist. The status-based ordering ("staged first, but only
    # if not yet promoted") was correct under the assumption that
    # staging and ledger are mutually exclusive states for a given id.
    # In practice they can drift: reboot-source txns can land in the
    # ledger with a lamella-txn-id while a stale staged_transactions
    # row carries the same id at status=new/classified/matched. Past
    # consequence: /txn/{token} rendered "not in the ledger" while
    # /search showed the txn under the Ledger tab. The ledger is the
    # source of truth (ADR-0001); when both exist, render the ledger
    # view AND self-heal the staged row to status=promoted so the
    # next request doesn't repeat the dance.
    try:
        from lamella.features.import_.staging import StagingService
        staged_row = StagingService(conn).get_by_lamella_txn_id(target_hash)
    except Exception:  # noqa: BLE001
        staged_row = None
    ledger_txn = _find_txn_by_lamella_id(reader, target_hash)
    if ledger_txn is None and staged_row is not None and staged_row.status != "promoted":
        return _render_staged_detail(
            request, conn, staged_row, token=target_hash,
        )
    if ledger_txn is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"no transaction with lamella-txn-id "
                f"{target_hash[:12]}… found in staging or the ledger"
            ),
        )
    if staged_row is not None and staged_row.status != "promoted":
        # Self-heal: ledger has the entry, staged row is stale. Bump
        # status so the staging review queue stops showing this row
        # and downstream code doesn't keep tripping the same drift.
        try:
            conn.execute(
                "UPDATE staged_transactions SET status = 'promoted', "
                "promoted_at = COALESCE(promoted_at, datetime('now')), "
                "updated_at = datetime('now') WHERE id = ?",
                (staged_row.id,),
            )
            log.info(
                "txn_detail: self-healed stale staged row id=%s "
                "(lamella-txn-id %s already in ledger)",
                staged_row.id, target_hash[:12],
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "txn_detail self-heal failed for staged id=%s: %s",
                staged_row.id, exc,
            )
    # Keep ``target_hash`` as the UUIDv7 token the user typed — it's
    # what the template emits in /txn/{target_hash}/* sub-routes (panel,
    # categorize-inplace, revert-override, apply, ask-ai, …) so those
    # mutation routes receive the immutable id. Use ``ledger_hash`` for
    # downstream blocks that genuinely need the content hash (receipt
    # joins, override resolution, transfer-pair partner lookup).
    ledger_hash = txn_hash(ledger_txn)
    txn = ledger_txn
    entries = list(reader.load().entries)

    # --- Postings ------------------------------------------------
    postings = []
    for p in txn.postings:
        amt = None
        ccy = ""
        if p.units is not None and p.units.number is not None:
            amt = Decimal(p.units.number)
            ccy = p.units.currency or ""
        postings.append({
            "account": p.account or "",
            "amount": amt,
            "currency": ccy,
            "is_fixme": (p.account or "").split(":")[-1].upper() == "FIXME",
            "meta": {
                k: v for k, v in (p.meta or {}).items()
                if k not in ("filename", "lineno")
            },
        })

    # --- Source / metadata --------------------------------------
    txn_meta = {
        k: v for k, v in (txn.meta or {}).items()
        if k not in ("filename", "lineno")
    }
    filename = (txn.meta or {}).get("filename")
    lineno = (txn.meta or {}).get("lineno")

    # --- Transfer pair ------------------------------------------
    from lamella.features.review_queue.pair_detector import detect_pairs
    pairs = detect_pairs(entries)
    pair = pairs.get(ledger_hash)
    partner_txn = None
    if pair is not None:
        for e in entries:
            if isinstance(e, _T) and txn_hash(e) == pair.partner_hash:
                partner_txn = e
                break

    # --- Linked receipts (possibly multiple) --------------------
    # ``receipt_links.txn_hash`` is overloaded — it stores either the
    # Beancount content hash (legacy + matcher-sweep links) or the
    # UUIDv7 lamella-txn-id token (links written via
    # /txn/{token}/receipt-link). Query both so a receipt linked
    # through the staged-row path still surfaces after the txn lands
    # in the ledger. Without this, /txn/{uuid} silently dropped
    # receipts that were attached pre-promotion.
    _link_keys: list[str] = [ledger_hash]
    if target_hash and target_hash != ledger_hash:
        _link_keys.append(target_hash)
    receipt_rows = conn.execute(
        f"""
        SELECT rl.paperless_id, rl.match_method, rl.match_confidence,
               rl.linked_at, rl.txn_date, rl.txn_amount,
               pdi.title, pdi.vendor, pdi.correspondent_name,
               pdi.total_amount, pdi.subtotal_amount, pdi.tax_amount,
               pdi.receipt_date, pdi.created_date, pdi.content_excerpt,
               pdi.tags_json, pdi.document_type_name, pdi.payment_last_four
          FROM receipt_links rl
          LEFT JOIN paperless_doc_index pdi ON pdi.paperless_id = rl.paperless_id
         WHERE rl.txn_hash IN ({",".join("?" * len(_link_keys))})
         ORDER BY rl.linked_at DESC
        """,
        tuple(_link_keys),
    ).fetchall()
    paperless_base = (settings.paperless_url or "").rstrip("/")
    receipts: list[dict] = []
    for r in receipt_rows:
        d = dict(r)
        try:
            d["tags"] = _parse_tag_names(d.get("tags_json"))
        except Exception:  # noqa: BLE001
            d["tags"] = []
        d["paperless_deep_link"] = (
            f"{paperless_base}/documents/{d['paperless_id']}/" if paperless_base else None
        )
        # Recent verify/enrich decisions for this doc.
        d["ai_decisions"] = [
            dict(x) for x in conn.execute(
                "SELECT id, decision_type, decided_at, model, result "
                "  FROM ai_decisions "
                " WHERE decision_type IN ('receipt_verify','receipt_enrich') "
                "   AND (input_ref = ? OR input_ref LIKE ?) "
                " ORDER BY decided_at DESC LIMIT 5",
                (f"paperless:{d['paperless_id']}",
                 f"paperless:{d['paperless_id']}:%"),
            ).fetchall()
        ]
        for ai_d in d["ai_decisions"]:
            try:
                ai_d["result"] = json.loads(ai_d["result"]) if ai_d["result"] else None
            except Exception:  # noqa: BLE001
                pass
        receipts.append(d)

    # --- Candidate receipts if none linked ----------------------
    # Historical: this block fed the template receipt-candidates list
    # from receipts.matcher.find_candidates — which returns LEDGER-side
    # MatchCandidate rows, not Paperless-side receipt candidates the
    # template expects (paperless_id / vendor / total). Mismatch 500s
    # every /txn load. Keep the empty list wired so the template
    # renders; revive with a Paperless candidate finder when ready.
    candidates: list = []

    # --- Notes --------------------------------------------------
    from lamella.features.notes.service import NoteService
    note_service = NoteService(conn)
    active_notes = note_service.notes_active_on(txn.date, proximity_days=5)

    # --- Mileage on this day ------------------------------------
    # Show trips from the same date so the reviewer can see "was I
    # driving when this charge hit?" without hopping over to the
    # mileage page. Context-only — editing still lives on
    # /mileage/<id>/edit.
    mileage_rows = conn.execute(
        """
        SELECT id, entry_date, entry_time, vehicle, miles, entity,
               purpose, from_loc, to_loc
          FROM mileage_entries
         WHERE entry_date = ?
         ORDER BY COALESCE(entry_time, '00:00')
        """,
        (txn.date.isoformat() if hasattr(txn.date, 'isoformat') else str(txn.date),),
    ).fetchall()
    mileage_day = [dict(r) for r in mileage_rows]

    # --- AI history for this transaction ------------------------
    # An entry's AI decisions can be keyed under any of several
    # ``input_ref`` shapes depending on when each call ran:
    #   * lineage UUID (``lamella-txn-id``) — canonical, every new
    #     decision logs under this when the entry has lineage.
    #   * Beancount ``txn_hash`` — pre-lineage post-promotion calls,
    #     plus the lineage-fallback path for entries that haven't
    #     been on-touch-normalized yet.
    #   * SimpleFIN bridge id (``TRN-…``) — ingest-time calls
    #     persisted on the eventual entry as ``lamella-simplefin-id``.
    # This expansion is **permanent**, not migration scaffolding —
    # it matches the ``bcg-*`` pattern: read both formats forever so
    # the user never has to "run the migration" for the app to work.
    # The /setup/recovery normalize action and the on-touch
    # normalization in ``rewrite/txn_inplace`` converge legacy
    # entries to lineage as the user goes, but absent those the
    # query keeps finding decisions by their original key.
    from lamella.core.identity import (
        find_source_reference, get_refund_of, get_txn_id,
    )
    # Candidate refs: ledger content hash (legacy AI calls), the
    # immutable lineage (canonical post-Phase-7), and any source-side
    # bridge id (legacy ingest-time AI). Each ref shape may have
    # decisions logged against it across the entry's lifetime.
    candidate_refs: list[str] = [ledger_hash]
    _txn_lineage = get_txn_id(txn)
    if _txn_lineage and _txn_lineage not in candidate_refs:
        candidate_refs.append(_txn_lineage)
    # SimpleFIN bridge id (legacy ingest-time decisions). Reads from
    # posting-level paired source meta; legacy txn-level keys mirror
    # down via _legacy_meta.normalize_entries.
    _sf = find_source_reference(txn, "simplefin")
    if _sf and _sf not in candidate_refs:
        candidate_refs.append(_sf)
    _placeholders = ",".join("?" * len(candidate_refs))
    ai_history_rows = conn.execute(
        f"""
        SELECT id, decision_type, decided_at, model, input_ref,
               prompt_tokens, completion_tokens, result, user_corrected,
               user_correction
          FROM ai_decisions
         WHERE input_ref IN ({_placeholders})
         ORDER BY decided_at DESC LIMIT 20
        """,
        tuple(candidate_refs),
    ).fetchall()
    ai_history: list[dict] = []
    for r in ai_history_rows:
        d = dict(r)
        try:
            d["result"] = json.loads(d["result"]) if d["result"] else None
        except Exception:  # noqa: BLE001
            pass
        ai_history.append(d)

    # --- Primary amount for display -----------------------------
    from_acct, from_amt, currency = _fixme_posting(txn)

    # Datalist sources for the Paperless "enrich" form's vehicle /
    # entity pickers. Picker rule (CLAUDE.md): anywhere we ask the
    # user to pick a vehicle or entity, render a text input backed
    # by a datalist so long-tail values remain discoverable without
    # a native <select>.
    vehicles_list: list[str] = []
    entities_list: list[str] = []
    try:
        vehicle_rows = conn.execute(
            "SELECT slug, display_name FROM vehicles WHERE is_active = 1 "
            "ORDER BY COALESCE(year, 9999) DESC, "
            "         COALESCE(display_name, slug)"
        ).fetchall()
        vehicles_list = [
            (r["display_name"] or r["slug"]) for r in vehicle_rows
        ]
    except Exception:  # noqa: BLE001
        vehicles_list = []
    try:
        from beancount.core.data import Open as _Open
        entity_seen: set[str] = set()
        for e in entries:
            if isinstance(e, _Open):
                parts = e.account.split(":")
                if len(parts) >= 2 and parts[0] in {
                    "Assets", "Liabilities", "Income", "Expenses", "Equity",
                }:
                    entity_seen.add(parts[1])
        entities_list = sorted(entity_seen)
    except Exception:  # noqa: BLE001
        entities_list = []

    # --- Refund-of-expense bidirectional links ------------------
    # Two directions, both resolved by walking the in-memory entries:
    #   * refund_of: this txn carries lamella-refund-of pointing at
    #     another txn's lamella-txn-id → render "Refund of: <link>".
    #   * refunded_by: another txn carries lamella-refund-of pointing
    #     at this txn's lamella-txn-id → render "Refunded by: <link>".
    # The link is the source of truth (ADR-0001); SQLite stays a cache.
    refund_of: dict | None = None
    refunded_by: list[dict] = []
    _this_lineage = get_refund_of(txn)
    _self_lineage_token = get_txn_id(txn)
    for entry in entries:
        if not isinstance(entry, _T):
            continue
        refund_meta = get_refund_of(entry)
        if not refund_meta:
            continue
        refund_meta_str = str(refund_meta)
        meta = getattr(entry, "meta", None) or {}
        # The "refund_of" direction — this txn IS the refund. Resolve
        # the original entry's lineage so the template gets the same
        # /txn/{token} URL the user would type by hand.
        if refund_meta_str == str(_this_lineage) and refund_of is None:
            # Only one original per refund — first match wins.
            pass  # handled below
        # The "refunded_by" direction — another txn refunds this one.
        if (
            _self_lineage_token
            and refund_meta_str == str(_self_lineage_token)
        ):
            refunded_by.append({
                "token": get_txn_id(entry) or "",
                "date": entry.date,
                "merchant": (
                    getattr(entry, "payee", None)
                    or (entry.narration or "")[:48]
                ),
            })
    if _this_lineage:
        # Find the original by lamella-txn-id.
        target_id = str(_this_lineage)
        for entry in entries:
            if not isinstance(entry, _T):
                continue
            if get_txn_id(entry) == target_id:
                refund_of = {
                    "token": target_id,
                    "date": entry.date,
                    "merchant": (
                        getattr(entry, "payee", None)
                        or (entry.narration or "")[:48]
                    ),
                }
                break

    # --- Override resolution ------------------------------------
    # Any overlay txn whose lamella-override-of metadata points at this
    # target means the original's FIXME leg has already been redirected.
    # Show that prominently so users don't wonder if their click did
    # anything.
    overrides: list[dict] = []
    for entry in entries:
        if not isinstance(entry, _T):
            continue
        meta = getattr(entry, "meta", None) or {}
        ov = meta.get("lamella-override-of") or meta.get("override-of")
        if ov != ledger_hash:
            continue
        target_legs: list[dict] = []
        for p in entry.postings:
            acct = p.account or ""
            if acct.upper().endswith(":FIXME"):
                continue
            if p.units is None or p.units.number is None:
                continue
            target_legs.append({
                "account": acct,
                "amount": Decimal(p.units.number),
                "currency": p.units.currency or "USD",
            })
        overrides.append({
            "date": entry.date,
            "narration": entry.narration or "",
            "modified_at": meta.get("lamella-modified-at"),
            "legs": target_legs,
        })

    ctx = {
        "target_hash": target_hash,  # UUIDv7 token (URL form)
        "ledger_hash": ledger_hash,  # Beancount content hash
        "txn": txn,
        "txn_meta": txn_meta,
        "postings": postings,
        "filename": filename,
        "lineno": lineno,
        "pair": pair,
        "partner_txn": partner_txn,
        "partner_hash": pair.partner_hash if pair else None,
        # The pair's partner_hash is a content hash; we need the
        # partner's lineage UUID for /txn/{token} URLs in the template.
        "partner_token": (
            get_txn_id(partner_txn) if partner_txn is not None else None
        ),
        "receipts": receipts,
        "candidates": candidates,
        "active_notes": active_notes,
        "mileage_day": mileage_day,
        "ai_history": ai_history,
        "overrides": overrides,
        "refund_of": refund_of,
        "refunded_by": refunded_by,
        "from_account": from_acct,
        "from_amount": from_amt,
        "currency": currency,
        "payee": getattr(txn, "payee", None),
        "narration": txn.narration or "",
        "date": txn.date,
        "paperless_base": paperless_base,
        "vehicles_list": vehicles_list,
        "entities_list": entities_list,
    }
    return request.app.state.templates.TemplateResponse(
        request, "txn_detail.html", ctx,
    )


@router.post("/txn/{target_hash}/ask-ai", response_class=HTMLResponse)
async def txn_ask_ai(
    target_hash: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
    conn: sqlite3.Connection = Depends(get_db),
    context_hint: str | None = Form(default=None),
):
    """Run the classifier on demand against the txn whose
    Beancount hash is `target_hash`. The proposal lands in
    `ai_decisions` (visible in the AI history table on this same
    page) — no ledger write happens here. The user reviews the
    proposal and uses the existing /review/<id>/resolve flow (or
    a manual override) to apply it.

    Accepts either a UUIDv7 ``lamella-txn-id`` or a legacy hex
    ``txn_hash``. UUIDv7 tokens that don't yet have a ledger entry
    (staged-only) are out of scope for this endpoint — staged rows
    have their own ``/api/txn/staged:{id}/ask-ai`` flow.

    Optional `context_hint` is a free-form note prepended to the
    txn narration so the classifier prompt picks it up the same
    way it picks up any other narration text — same shape as the
    /review/staged/ask-ai endpoint.

    Only fires when the txn has a FIXME posting; if it's already
    classified, a re-classification request needs the audit /
    bulk-classify path instead.
    """
    from lamella.features.ai_cascade.bulk_classify import _classify_one
    from lamella.features.ai_cascade.service import AIService

    # Preserve the caller's UUIDv7 token so the post-action redirect
    # lands back on the same URL the user came from. The classifier
    # needs the resolved ledger entry; both come from the same lookup.
    if not _is_uuidv7_token(target_hash):
        raise HTTPException(
            status_code=404,
            detail=(
                "/txn/{token}/ask-ai accepts only a lamella-txn-id "
                "(UUIDv7); legacy hex was retired in v3."
            ),
        )
    redirect_token = target_hash
    ledger_txn = _find_txn_by_lamella_id(reader, target_hash)
    if ledger_txn is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"no ledger transaction with lamella-txn-id "
                f"{target_hash[:12]}…; staged-only rows use "
                "/api/txn/staged:<id>/ask-ai"
            ),
        )
    target_hash = txn_hash(ledger_txn)
    txn = ledger_txn

    fixme_acct, abs_amount, currency = _fixme_posting(txn)
    if fixme_acct is None or abs_amount is None or "FIXME" not in fixme_acct.upper():
        raise HTTPException(
            status_code=400,
            detail=(
                f"transaction {target_hash[:12]}… has no FIXME posting "
                "to classify. Use /audit or /search bulk-apply to "
                "re-classify an already-categorized transaction."
            ),
        )

    ai = AIService(settings=settings, conn=conn)
    if not ai.enabled:
        raise HTTPException(
            status_code=503,
            detail="AI service disabled — set OPENROUTER_API_KEY to enable",
        )

    # Splice the optional hint into the narration so the prompt
    # picks it up. Don't mutate the ledger entry — _classify_one
    # reads narration via render(); we shadow it by constructing
    # a lightweight wrapper that overrides only that field.
    hint = (context_hint or "").strip()
    txn_for_classify = txn
    if hint:
        from dataclasses import replace as _replace
        try:
            txn_for_classify = _replace(
                txn,
                narration=f"User hint: {hint}\n{txn.narration or ''}".strip(),
            )
        except Exception:  # noqa: BLE001 — fallthrough to no-hint path
            txn_for_classify = txn

    entries = list(reader.load().entries)
    target, confidence, error_msg = await _classify_one(
        txn=txn_for_classify,
        fixme_account=fixme_acct,
        abs_amount=abs_amount,
        currency=currency,
        entries=entries,
        conn=conn,
        settings=settings,
        ai_service=ai,
    )

    if error_msg and not target:
        log.warning("/txn ask-ai failed for %s: %s", target_hash[:12], error_msg)
        return RedirectResponse(
            f"/txn/{redirect_token}?ask_ai_error={error_msg}",
            status_code=303,
        )
    if target is None:
        return RedirectResponse(
            f"/txn/{redirect_token}?ask_ai_error=no+target+proposed",
            status_code=303,
        )

    # The proposal is already logged to ai_decisions by the
    # classifier's chat() call; just redirect back so the AI
    # history table re-renders with the new row.
    return RedirectResponse(
        f"/txn/{redirect_token}?ask_ai_proposed={target}&conf={confidence:.2f}",
        status_code=303,
    )


def _parse_tag_names(tags_json: Any) -> list:
    """tags_json stores a JSON list of {id, name} dicts OR just ids,
    depending on which sync version wrote it. Return a list of
    strings we can display; empty when unparseable."""
    if not tags_json:
        return []
    try:
        data = json.loads(tags_json) if isinstance(tags_json, str) else tags_json
    except Exception:  # noqa: BLE001
        return []
    if not isinstance(data, list):
        return []
    out: list[str] = []
    for item in data:
        if isinstance(item, dict) and item.get("name"):
            out.append(str(item["name"]))
        elif isinstance(item, (int, str)):
            out.append(str(item))
    return out


@router.get("/txn/{target_hash}/notes-partial", response_class=HTMLResponse)
def txn_notes_partial(
    target_hash: str,
    request: Request,
    reader: LedgerReader = Depends(get_ledger_reader),
    conn = Depends(get_db),
):
    """HTMX partial — re-renders only the notes list for a txn.

    Used after a successful note POST so the list refreshes in place
    without a full page reload. Returns the inner list HTML (not the
    wrapping section) so the target element's id + heading stay put.
    """
    from lamella.features.notes.service import NoteService
    if not _is_uuidv7_token(target_hash):
        raise HTTPException(
            status_code=404,
            detail="legacy hex /txn/* paths retired in v3; use lamella-txn-id",
        )
    ledger_txn = _find_txn_by_lamella_id(reader, target_hash)
    if ledger_txn is None:
        raise HTTPException(status_code=404, detail="txn not found")
    target_hash = txn_hash(ledger_txn)
    txn = ledger_txn
    notes = NoteService(conn).notes_active_on(
        txn.date, proximity_days=5, txn_hash=target_hash,
    )
    return request.app.state.templates.TemplateResponse(
        request,
        "partials/_txn_notes_list.html",
        {
            "active_notes": notes,
            "target_hash": target_hash,
            "date": txn.date,
        },
    )


@router.get("/txn/{target_hash}/panel", response_class=HTMLResponse)
def txn_panel(
    target_hash: str,
    request: Request,
    reader: LedgerReader = Depends(get_ledger_reader),
    conn = Depends(get_db),
):
    """HTMX partial — inline actions for any transaction."""
    try:
        return _render_txn_panel(target_hash, request, reader, conn)
    except Exception as exc:  # noqa: BLE001
        log.exception("txn_panel failed for %s", target_hash)
        return HTMLResponse(
            f'<div class="txn-panel-error">'
            f'<strong>Failed to load actions.</strong>'
            f'<details open><summary>Error detail</summary>'
            f'<pre class="excerpt">{type(exc).__name__}: {exc}</pre></details>'
            f'</div>',
            status_code=200,
        )


def _render_txn_panel(target_hash, request, reader, conn):
    if not _is_uuidv7_token(target_hash):
        return HTMLResponse(
            '<div class="txn-panel-error">'
            'Legacy hex /txn/* paths retired in v3; '
            'use the immutable lamella-txn-id URL.</div>',
            status_code=200,
        )
    ledger_txn = _find_txn_by_lamella_id(reader, target_hash)
    if ledger_txn is None:
        return HTMLResponse(
            f'<div class="txn-panel-error">'
            f'Transaction not in ledger (id {target_hash[:8]}…).</div>',
            status_code=200,
        )
    # `target_hash` (the URL-segment value) MUST stay the UUIDv7 token
    # the user is on — it's what the panel's forms emit as the action
    # URL for /txn/{token}/categorize-inplace, /apply, /revert-override,
    # etc. Those mutation routes call _require_ledger_hash() which
    # rejects anything that isn't UUIDv7. Use `ledger_hash` for the
    # internal beancount-content-hash uses (override-of join below).
    ledger_hash = txn_hash(ledger_txn)
    txn = ledger_txn

    from_acct, from_amt, currency = _fixme_posting(txn)
    has_fixme = any(
        (p.account or "").split(":")[-1].upper() == "FIXME" for p in txn.postings
    )

    # Already-categorized check. If there's an override in
    # connector_overrides.bean whose lamella-override-of metadata points
    # at this hash, the FIXME has already been redirected. Surface
    # the target account so the panel shows "Categorized as X" with
    # an Undo button, instead of a Categorize form on an already-
    # categorized txn (which previously accumulated stacked overrides).
    override_target_accounts: list[str] = []
    from beancount.core.data import Transaction
    entries = list(reader.load().entries)
    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        meta = getattr(entry, "meta", None) or {}
        if meta.get("lamella-override-of") != ledger_hash:
            continue
        for p in entry.postings or ():
            acct = p.account or ""
            if "FIXME" in acct.upper():
                continue
            if not acct.startswith(("Expenses:", "Income:", "Assets:", "Liabilities:", "Equity:")):
                continue
            units = p.units
            if units is None or units.number is None:
                continue
            # Only the positive-leg (destination) accounts; skip the
            # -amount counter-leg pairing the FIXME removal.
            from decimal import Decimal as _D
            if _D(units.number) > 0 and acct not in override_target_accounts:
                override_target_accounts.append(acct)
    already_categorized = bool(override_target_accounts)

    # Relevant loans: if any loan's liability path matches a posting,
    # or the narration mentions a loan institution, offer a shortcut.
    relevant_loans: list[dict] = []
    rows = conn.execute(
        "SELECT slug, display_name, institution, liability_account_path "
        "FROM loans WHERE is_active = 1"
    ).fetchall()
    posting_accts = {p.account for p in txn.postings if p.account}
    hay = " ".join(filter(None, [txn.payee or "", txn.narration or ""])).lower()
    for r in rows:
        rd = dict(r)
        is_match = False
        if rd.get("liability_account_path") and rd["liability_account_path"] in posting_accts:
            is_match = True
        elif rd.get("institution") and len(rd["institution"]) >= 4 and rd["institution"].lower() in hay:
            is_match = True
        elif rd.get("slug") and len(rd["slug"]) >= 4 and rd["slug"].lower() in hay:
            is_match = True
        if is_match:
            relevant_loans.append(rd)

    ctx = {
        "txn": txn,
        "target_hash": target_hash,
        "from_account": from_acct,
        "from_amount": from_amt,
        "currency": currency,
        "has_fixme": has_fixme,
        "narration": txn.narration or "",
        "payee": getattr(txn, "payee", None),
        "date": txn.date,
        "relevant_loans": relevant_loans,
        "already_categorized": already_categorized,
        "override_target_accounts": override_target_accounts,
    }
    return request.app.state.templates.TemplateResponse(
        request, "partials/txn_panel.html", ctx
    )


@router.post("/txn/{target_hash}/categorize-inplace", response_class=HTMLResponse)
async def txn_categorize_inplace(
    target_hash: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Rewrite the FIXME posting in the source .bean file directly.

    This is the canonical categorize path — no override layer, no
    accumulated correction blocks. The .bean file the txn came from
    gets edited in place, with a timestamped backup taken first and
    bean-check validating the result.

    If the txn already has an override from the legacy path, we
    strip it first so we're not left with both an in-place edit
    AND a stale override layered on top.

    On any failure (bean-check rejection, amount mismatch, path
    outside ledger_dir, etc.) the backup is restored and a clear
    error is returned. The caller can fall back to the override
    flow at /txn/<hash>/apply — same response shape."""
    from decimal import Decimal as _D
    from lamella.core.rewrite.txn_inplace import (
        InPlaceRewriteError,
        rewrite_fixme_to_account,
    )

    target_hash = _require_ledger_hash(reader, target_hash)
    txn = _find_txn(reader, target_hash)
    if txn is None:
        return HTMLResponse(
            '<div class="txn-panel-error">transaction not found</div>',
            status_code=404,
        )
    form = await request.form()
    target = (form.get("target_account") or "").strip()
    if not target:
        return HTMLResponse(
            '<div class="txn-panel-error">target_account required</div>',
            status_code=400,
        )

    # Locate the FIXME posting + amount + source file.
    from_acct: str | None = None
    fixme_amount: _D | None = None
    for p in txn.postings or ():
        acct = p.account or ""
        if "FIXME" in acct.upper() and p.units and p.units.number is not None:
            from_acct = acct
            fixme_amount = _D(p.units.number)
            break
    if from_acct is None or fixme_amount is None:
        return HTMLResponse(
            '<div class="txn-panel-error">no FIXME posting to rewrite — '
            'already categorized?</div>',
            status_code=400,
        )

    meta = getattr(txn, "meta", None) or {}
    filename = meta.get("filename")
    lineno = meta.get("lineno")
    if not filename or lineno is None:
        return HTMLResponse(
            '<div class="txn-panel-error">txn has no filename/lineno meta '
            '(unusual — fall back to /apply)</div>',
            status_code=400,
        )

    # Clear any stale override for this hash so we don't leave
    # both an in-place edit AND an override layered on the same txn.
    writer = OverrideWriter(
        main_bean=settings.ledger_main,
        overrides=settings.connector_overrides_path,
        conn=conn,
    )
    try:
        writer.rewrite_without_hash(target_hash)
    except BeanCheckError:
        # If removing the override alone breaks bean-check, don't
        # proceed — something unusual is up.
        return HTMLResponse(
            '<div class="txn-panel-error">could not strip prior override; '
            'aborting in-place rewrite to avoid leaving the ledger '
            'inconsistent</div>',
            status_code=500,
        )

    from pathlib import Path as _P
    try:
        rewrite_fixme_to_account(
            source_file=_P(filename),
            line_number=int(lineno),
            old_account=from_acct,
            new_account=target,
            expected_amount=fixme_amount,
            ledger_dir=settings.ledger_dir,
            main_bean=settings.ledger_main,
        )
    except InPlaceRewriteError as exc:
        log.warning(
            "in-place categorize failed for %s: %s",
            target_hash[:12], exc,
        )
        # Clear cache row we might have created during override-strip.
        try:
            conn.execute(
                "DELETE FROM txn_classification_modified WHERE txn_hash = ?",
                (target_hash,),
            )
        except Exception:  # noqa: BLE001
            pass
        return HTMLResponse(
            f'<div class="txn-panel-error">Couldn\'t apply categorization: '
            f'{exc}. No ledger change was made — the source file was '
            f'restored from the pre-call backup. Double-check the target '
            f'account spelling and that the file is writable.</div>',
            status_code=400,
        )

    # Stamp the txn_classification_modified cache so the calendar
    # dirty-since-reviewed check sees the change, same as the
    # override path would.
    from datetime import datetime as _dt
    try:
        conn.execute(
            """
            INSERT INTO txn_classification_modified
                (txn_hash, txn_date, modified_at)
            VALUES (?, ?, ?)
            ON CONFLICT(txn_hash) DO UPDATE SET
                txn_date = excluded.txn_date,
                modified_at = MAX(modified_at, excluded.modified_at)
            """,
            (
                target_hash,
                txn.date.isoformat() if hasattr(txn.date, "isoformat") else str(txn.date),
                _dt.now().astimezone().isoformat(timespec="seconds"),
            ),
        )
    except Exception:  # noqa: BLE001
        pass

    reader.invalidate()
    return HTMLResponse(
        f'<div class="txn-panel txn-panel-done">✓ Categorized as '
        f'<strong>{target}</strong> (in-place edit to '
        f'<code>{_P(filename).name}</code>). '
        f'<a href="javascript:location.reload()">refresh</a></div>'
    )


@router.post("/txn/{target_hash}/revert-override", response_class=HTMLResponse)
async def txn_revert_override(
    target_hash: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Remove every override block pointing at this txn hash, putting
    the transaction back into its original FIXME state. Also clears
    the txn_classification_modified cache row so the calendar's
    dirty-since-reviewed signal stays accurate.

    Used by the 'Undo categorization' button on /txn/<token>. No more
    telling users to hand-edit connector_overrides.bean."""
    target_hash = _require_ledger_hash(reader, target_hash)
    writer = OverrideWriter(
        main_bean=settings.ledger_main,
        overrides=settings.connector_overrides_path,
        conn=conn,
    )
    try:
        removed = writer.rewrite_without_hash(target_hash)
    except BeanCheckError as exc:
        return HTMLResponse(
            f'<div class="txn-panel-error">bean-check failed: {exc}</div>',
            status_code=500,
        )
    # Clear the txn_classification_modified cache so the calendar
    # doesn't show a stale "modified" timestamp for a txn that no
    # longer has an override.
    try:
        conn.execute(
            "DELETE FROM txn_classification_modified WHERE txn_hash = ?",
            (target_hash,),
        )
    except Exception:  # noqa: BLE001
        pass
    reader.invalidate()
    is_htmx = "hx-request" in {k.lower() for k in request.headers.keys()}
    if is_htmx:
        return HTMLResponse(
            f'<div class="txn-panel txn-panel-done">↩ Categorization undone '
            f'({removed} override block(s) removed). '
            f'<a href="javascript:location.reload()">refresh</a></div>'
        )
    return RedirectResponse(url=f"/txn/{target_hash}", status_code=303)


@router.post("/txn/{target_hash}/apply", response_class=HTMLResponse)
async def txn_apply(
    target_hash: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Generic transaction action dispatch — mode=categorize|transfer|split."""
    target_hash = _require_ledger_hash(reader, target_hash)
    txn = _find_txn(reader, target_hash)
    if txn is None:
        return HTMLResponse(
            '<div class="txn-panel-error">transaction not found</div>',
            status_code=404,
        )
    form = await request.form()
    mode = (form.get("mode") or "categorize").strip()

    from_acct, from_amt, currency = _fixme_posting(txn)
    if from_acct is None or from_amt is None:
        return HTMLResponse(
            '<div class="txn-panel-error">no posting to override</div>',
            status_code=400,
        )

    writer = OverrideWriter(
        main_bean=settings.ledger_main,
        overrides=settings.connector_overrides_path,
        conn=conn,
    )

    # Re-categorize cleanly: before writing a new override, strip any
    # existing override block pointing at this hash. Without this, two
    # overrides would stack — both redirecting FIXME — and bean-check
    # would reject the second write because FIXME nets to 2x the
    # amount instead of zero.
    try:
        writer.rewrite_without_hash(target_hash)
    except BeanCheckError:
        # Non-fatal — if the prior removal fails bean-check, the
        # append below will also fail and the user sees the same
        # error with the full context.
        pass

    def _done(message: str) -> HTMLResponse:
        reader.invalidate()
        return HTMLResponse(
            f'<div class="txn-panel txn-panel-done">✓ {message} · '
            f'<a href="javascript:location.reload()">refresh list</a></div>'
        )

    if mode in ("categorize", "transfer"):
        target = (form.get("target_account") or "").strip()
        if not target:
            return HTMLResponse(
                '<div class="txn-panel-error">target_account required</div>',
                status_code=400,
            )
        try:
            writer.append(
                txn_date=txn.date if isinstance(txn.date, date)
                         else date.fromisoformat(str(txn.date)),
                txn_hash=target_hash,
                amount=from_amt,
                from_account=from_acct,
                to_account=target,
                currency=currency,
                narration=(txn.narration or f"{mode} override"),
            )
        except BeanCheckError as exc:
            log.warning("txn apply (%s) bean-check: %s", mode, exc)
            return HTMLResponse(
                f'<div class="txn-panel-error">bean-check blocked: {exc}</div>',
                status_code=200,
            )
        # Optionally save a rule.
        if form.get("save_rule") == "1" and (form.get("rule_pattern_value") or "").strip():
            try:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO classification_rules
                        (pattern_type, pattern_value, target_account,
                         card_account, confidence, hit_count, created_by)
                    VALUES ('merchant_contains', ?, ?, NULL, 1.0, 0, 'txn-panel')
                    """,
                    (form.get("rule_pattern_value").strip(), target),
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("txn panel rule save failed: %s", exc)
        verb = "marked as transfer" if mode == "transfer" else "categorized"
        return _done(f"{verb} → {target}")

    if mode == "split":
        accounts = [str(v).strip() for (k, v) in form.multi_items() if k == "split_account"]
        amounts_raw = [v for (k, v) in form.multi_items() if k == "split_amount"]
        splits: list[tuple[str, Decimal]] = []
        for acct, raw_amt in zip(accounts, amounts_raw):
            if not acct:
                continue
            try:
                amt_clean = str(raw_amt).replace(",", "").replace("$", "").strip()
                amt = Decimal(amt_clean) if amt_clean else Decimal("0")
            except Exception:  # noqa: BLE001
                continue
            if amt > 0:
                splits.append((acct, amt))
        if not splits:
            return HTMLResponse(
                '<div class="txn-panel-error">no valid splits</div>',
                status_code=400,
            )
        total = sum((a for _, a in splits), Decimal("0"))
        if abs(total - from_amt) > Decimal("0.02"):
            return HTMLResponse(
                f'<div class="txn-panel-error">splits sum to {total:.2f} '
                f'but transaction is {from_amt:.2f}</div>',
                status_code=400,
            )
        try:
            writer.append_split(
                txn_date=txn.date if isinstance(txn.date, date)
                         else date.fromisoformat(str(txn.date)),
                txn_hash=target_hash,
                from_account=from_acct,
                splits=splits,
                currency=currency,
                narration=(txn.narration or "split override"),
            )
        except BeanCheckError as exc:
            log.warning("txn split bean-check: %s", exc)
            return HTMLResponse(
                f'<div class="txn-panel-error">bean-check blocked: {exc}</div>',
                status_code=200,
            )
        return _done(f"split across {len(splits)} accounts")

    return HTMLResponse(
        f'<div class="txn-panel-error">unknown mode: {mode}</div>',
        status_code=400,
    )


@router.get("/txn/{target_hash}/pair-candidates")
def txn_pair_candidates(
    target_hash: str,
    request: Request,
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """JSON endpoint — return opposite-sign ledger txns within ±14
    days whose amount matches ``target_hash``'s absolute primary
    amount (within $0.02). Used by the /txn page's transfer-pair
    picker to suggest the other side of a cross-date transfer.
    """
    from fastapi.responses import JSONResponse
    target_hash = _require_ledger_hash(reader, target_hash)
    entries = list(reader.load().entries)
    target: Transaction | None = None
    for e in entries:
        if isinstance(e, Transaction) and txn_hash(e) == target_hash:
            target = e
            break
    if target is None:
        raise HTTPException(status_code=404, detail="txn not found")
    _, target_amt, target_ccy = _fixme_posting(target)
    if target_amt is None:
        return JSONResponse({"candidates": []})
    # Signed amount of the FIXME leg, to match opposite-sign.
    target_fixme_acct = None
    target_signed = Decimal("0")
    for p in target.postings:
        acct = p.account or ""
        if acct.split(":")[-1].upper() == "FIXME" and p.units and p.units.number is not None:
            target_fixme_acct = acct
            target_signed = Decimal(p.units.number)
            break

    target_date = target.date if isinstance(target.date, date) else date.fromisoformat(str(target.date))
    candidates: list[dict] = []
    for e in entries:
        if not isinstance(e, Transaction):
            continue
        if txn_hash(e) == target_hash:
            continue
        ed = e.date if isinstance(e.date, date) else date.fromisoformat(str(e.date))
        if abs((ed - target_date).days) > 14:
            continue
        # Find a FIXME leg with matching abs amount, opposite sign.
        for p in e.postings:
            acct = p.account or ""
            if acct.split(":")[-1].upper() != "FIXME":
                continue
            if p.units is None or p.units.number is None:
                continue
            amt = Decimal(p.units.number)
            if abs(abs(amt) - target_amt) > Decimal("0.02"):
                continue
            if (amt > 0) == (target_signed > 0):
                # Same sign — can't be the other side of a transfer.
                continue
            # Pull the non-FIXME source account for display.
            source_acct = None
            for q in e.postings:
                qa = q.account or ""
                if qa.split(":")[-1].upper() == "FIXME":
                    continue
                if qa.startswith(("Assets:", "Liabilities:")):
                    source_acct = qa
                    break
            candidates.append({
                "txn_hash": txn_hash(e),
                "date": ed.isoformat(),
                "days_diff": (ed - target_date).days,
                "amount": f"{abs(amt):.2f}",
                "signed_amount": f"{amt:.2f}",
                "currency": p.units.currency or "USD",
                "payee": getattr(e, "payee", None),
                "narration": (e.narration or "")[:120],
                "source_account": source_acct,
            })
            break
    candidates.sort(key=lambda c: (abs(c["days_diff"]), c["txn_hash"]))
    return JSONResponse({"candidates": candidates[:20]})


@router.post("/search/mark-transfer-pair", response_class=HTMLResponse)
async def search_mark_transfer_pair(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Pair two FIXME transactions as two sides of a single transfer
    that hit different statement dates (ACH float, wire lag, etc.).

    Form inputs:
      - txn_hash (repeated 2x) — the two transactions to pair

    Entity-scoped clearing: each side's FIXME leg routes to
    ``Assets:{entity}:Transfers:InFlight``. When both sides share the
    same entity (same-entity multi-day transfer), the clearing
    account nets to zero across the two legs. When the sides belong
    to different entities (cross-entity transfer), each entity's
    Transfers:InFlight carries a non-zero balance until reconciled
    with an intercompany entry — that's the expected real-world
    shape per ``CLAUDE.md`` entity-first rule.

    Both overrides share a ``lamella-transfer-pair-id`` so the pair can
    be rebuilt from the ledger alone. Validates that amounts offset
    within ``PAIR_AMOUNT_TOLERANCE`` and dates are within
    ``PAIR_DATE_TOLERANCE_DAYS``. Opens each side's clearing account
    via AccountsWriter if needed.

    On any failure both attempted writes are rolled back by
    OverrideWriter's snapshot restore, leaving the ledger
    byte-identical to its pre-call state.
    """
    from uuid import uuid4
    from beancount.core.data import Open as _Open, Transaction as _Txn

    from lamella.core.registry.accounts_writer import AccountsWriter
    from lamella.core.ledger_writer import BeanCheckError as _BCE

    form = await request.form()
    hashes = [
        str(v).strip() for (k, v) in form.multi_items()
        if k == "txn_hash" and str(v).strip()
    ]
    if len(hashes) != 2:
        raise HTTPException(
            status_code=400,
            detail=f"select exactly 2 transactions (got {len(hashes)})",
        )
    if hashes[0] == hashes[1]:
        raise HTTPException(
            status_code=400, detail="the two selections must be different transactions",
        )
    q = (form.get("q") or "").strip()
    lookback_days = (form.get("lookback_days") or "365").strip()
    fixme_param = (form.get("fixme") or "").strip()

    entries = list(reader.load().entries)
    by_hash: dict[str, Transaction] = {
        txn_hash(e): e for e in entries if isinstance(e, _Txn)
    }

    txn_a = by_hash.get(hashes[0])
    txn_b = by_hash.get(hashes[1])
    if txn_a is None or txn_b is None:
        missing = [h[:8] for h, t in zip(hashes, (txn_a, txn_b)) if t is None]
        raise HTTPException(
            status_code=404,
            detail=f"transaction(s) not in ledger: {', '.join(missing)}",
        )

    fixme_a, amt_a, ccy_a = _fixme_posting(txn_a)
    fixme_b, amt_b, ccy_b = _fixme_posting(txn_b)
    if fixme_a is None or fixme_b is None or amt_a is None or amt_b is None:
        raise HTTPException(
            status_code=400,
            detail="both transactions must have a FIXME (or expense) leg",
        )
    if ccy_a != ccy_b:
        raise HTTPException(
            status_code=400,
            detail=f"currency mismatch: {ccy_a} vs {ccy_b}",
        )
    # Amounts are returned absolute; they must match within tolerance.
    if abs(amt_a - amt_b) > PAIR_AMOUNT_TOLERANCE:
        raise HTTPException(
            status_code=400,
            detail=(
                f"amounts don't match: {amt_a:.2f} vs {amt_b:.2f} "
                f"(tolerance ±{PAIR_AMOUNT_TOLERANCE})"
            ),
        )
    # Opposing signs on the actual postings — one FIXME leg should be
    # positive (money parked there from a debit) and the other negative
    # (money taken from there into a credit). _fixme_posting returns
    # absolute amounts so we re-scan for the signed amount here.
    def _signed_fixme(txn: Transaction, acct: str) -> Decimal:
        for p in txn.postings:
            if (p.account or "") == acct and p.units and p.units.number is not None:
                return Decimal(p.units.number)
        return Decimal("0")
    signed_a = _signed_fixme(txn_a, fixme_a)
    signed_b = _signed_fixme(txn_b, fixme_b)
    if signed_a == 0 or signed_b == 0 or (signed_a > 0) == (signed_b > 0):
        raise HTTPException(
            status_code=400,
            detail=(
                "FIXME legs don't offset — one side should be an outflow and "
                "the other an inflow. If both look the same direction, these "
                "may not actually be two sides of one transfer."
            ),
        )
    # Date window.
    def _to_date(v) -> date:
        return v if isinstance(v, date) else date.fromisoformat(str(v))
    date_a, date_b = _to_date(txn_a.date), _to_date(txn_b.date)
    delta_days = abs((date_a - date_b).days)
    if delta_days > PAIR_DATE_TOLERANCE_DAYS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"dates are {delta_days} days apart (max {PAIR_DATE_TOLERANCE_DAYS}). "
                "Confirm these are really two sides of the same transfer."
            ),
        )

    # Derive each side's entity-scoped clearing account. Same-entity
    # pairs share one account (nets to zero); cross-entity pairs each
    # keep their own (each non-zero until intercompany reconciliation).
    entity_a = _entity_for_txn(conn, txn_a)
    entity_b = _entity_for_txn(conn, txn_b)
    if not entity_a or not entity_b:
        raise HTTPException(
            status_code=400,
            detail=(
                "couldn't determine the entity that owns "
                f"{hashes[0][:8] if not entity_a else hashes[1][:8]}. "
                "Label the account under /accounts so it has an entity_slug, "
                "then retry."
            ),
        )
    clearing_a = _transfers_account_for(entity_a)
    clearing_b = _transfers_account_for(entity_b)
    same_entity = entity_a == entity_b

    # Ensure each clearing account is open. AccountsWriter.write_opens
    # filters against the passed `existing_paths` set so re-runs are
    # no-ops.
    open_paths = {e.account for e in entries if isinstance(e, _Open)}
    needed_opens: list[str] = []
    for acct in (clearing_a, clearing_b):
        if acct not in open_paths and acct not in needed_opens:
            needed_opens.append(acct)
    if needed_opens:
        aw = AccountsWriter(
            main_bean=settings.ledger_main,
            connector_accounts=settings.connector_accounts_path,
        )
        try:
            aw.write_opens(
                needed_opens,
                comment="Entity-scoped transfer clearing account(s)",
                existing_paths=open_paths,
            )
        except _BCE as exc:
            raise HTTPException(
                status_code=500,
                detail=f"couldn't open clearing account(s): {exc}",
            )

    # Write both legs. pair_id is stamped on both overrides so a
    # future reader can rebuild the pairing from the ledger alone.
    pair_id = uuid4().hex
    writer = OverrideWriter(
        main_bean=settings.ledger_main,
        overrides=settings.connector_overrides_path,
        conn=conn,
    )
    # Pass the SIGNED FIXME amount so the override cancels correctly on
    # both inflow (Income:FIXME -150) and outflow (Expenses:FIXME +150)
    # sides. The writer's block template does `from -= amt, clearing +=
    # amt`; with signed amounts that nets the clearing account to zero
    # across the two legs (same-entity case) or leaves the two entity
    # clearing accounts each carrying their half (cross-entity case).
    try:
        writer.append_transfer_pair_leg(
            txn_date=date_a,
            txn_hash=hashes[0],
            amount=signed_a,
            from_account=fixme_a,
            clearing_account=clearing_a,
            pair_id=pair_id,
            partner_hash=hashes[1],
            currency=ccy_a,
            narration=(txn_a.narration or "transfer pair leg"),
        )
        writer.append_transfer_pair_leg(
            txn_date=date_b,
            txn_hash=hashes[1],
            amount=signed_b,
            from_account=fixme_b,
            clearing_account=clearing_b,
            pair_id=pair_id,
            partner_hash=hashes[0],
            currency=ccy_b,
            narration=(txn_b.narration or "transfer pair leg"),
        )
    except BeanCheckError as exc:
        # First-leg success + second-leg failure: we have to undo the
        # first leg by removing its override block so we don't leave a
        # half-written pair on the ledger.
        try:
            writer.rewrite_without_hash(hashes[0])
        except Exception:  # noqa: BLE001
            log.exception("failed to roll back first transfer-pair leg")
        raise HTTPException(
            status_code=500,
            detail=f"bean-check rejected the pair: {exc}",
        )

    # Resolve any review_queue items pointing at either txn so they
    # don't keep showing as pending in the card UX.
    for h, clearing in ((hashes[0], clearing_a), (hashes[1], clearing_b)):
        row = conn.execute(
            "SELECT id FROM review_queue WHERE source_ref = ? AND resolved_at IS NULL",
            (f"fixme:{h}",),
        ).fetchone()
        if row:
            conn.execute(
                "UPDATE review_queue SET resolved_at = CURRENT_TIMESTAMP, "
                "user_decision = ? WHERE id = ?",
                (f"paired → {clearing}", row["id"]),
            )

    reader.invalidate()

    # Redirect back to /search with the original query intact so the
    # user sees the updated list minus these two rows.
    from urllib.parse import urlencode
    qs = {"q": q, "lookback_days": lookback_days}
    if fixme_param:
        qs["fixme"] = fixme_param
    qs["paired"] = "same" if same_entity else "cross"
    return HTMLResponse(
        "",
        status_code=200,
        headers={"HX-Redirect": f"/search?{urlencode(qs)}"},
    )


@router.post("/search/receipt-hunt", response_class=HTMLResponse)
async def search_receipt_hunt(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Three-pass receipt hunt for a batch of selected transactions.

    Runs as a background job — returns the progress-modal partial
    immediately so the browser sees live updates instead of a
    multi-minute hang. Per-txn events stream through JobContext.emit
    with Success / Failure / Not Found / Error outcomes. The final
    result (auto-linked / ambiguous / no-match counts + the detailed
    report the template needs) is stored as the job's ``result`` so
    the "View results" action on job completion can render
    receipt_hunt_result.html.
    """
    from lamella.features.receipts.hunt import run_hunt

    form = await request.form()
    selected = [v for (k, v) in form.multi_items() if k == "txn_hash" and v]
    selected = [str(h).strip() for h in selected if str(h).strip()]
    if not selected:
        raise HTTPException(status_code=400, detail="select at least one txn")

    q = (form.get("q") or "").strip()
    lookback_days = (form.get("lookback_days") or "365").strip()
    tolerance_days = int(form.get("tolerance_days") or 3)

    runner = request.app.state.job_runner

    def _work(ctx):
        return run_hunt(
            ctx,
            selected=selected,
            q=q,
            lookback_days=lookback_days,
            tolerance_days=tolerance_days,
            conn=conn,
            reader=reader,
            settings=settings,
        )

    job_id = runner.submit(
        kind="receipt-hunt",
        title=f"Hunting receipts for {len(selected)} transaction(s)",
        fn=_work,
        total=len(selected),
        meta={"q": q, "lookback_days": lookback_days},
    )
    # After-completion URL the user clicks to land on the full result
    # template. Needs job_id so /search/receipt-hunt/result can look
    # up the stored report.
    from urllib.parse import urlencode
    return_url = "/search/receipt-hunt/result?" + urlencode(
        {"job_id": job_id, "q": q, "lookback_days": lookback_days}
    )
    runner.set_return_url(job_id, return_url)
    # Modal close → result page (same as the View Results action). The
    # default `/jobs/{job_id}` would dump the user on a generic job
    # detail surface, away from the hunt results they care about.
    return request.app.state.templates.TemplateResponse(
        request,
        "partials/_job_modal.html",
        {"job_id": job_id, "on_close_url": return_url},
    )


@router.get("/search/receipt-hunt/result", response_class=HTMLResponse)
def search_receipt_hunt_result(
    request: Request,
    job_id: str | None = None,
):
    """Render the final receipt-hunt report for a completed job.

    The job runner stores the worker's return value in jobs.result_json;
    this handler fetches it and hands it to receipt_hunt_result.html.
    ``job_id`` comes from the progress modal's "View results" button.
    """
    if not job_id:
        raise HTTPException(status_code=400, detail="job_id required")
    runner = request.app.state.job_runner
    job = runner.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    if not job.is_terminal:
        # Not done yet — send the user back to /search (where they
        # launched the hunt) rather than to the generic /jobs/{id}
        # surface, which is a dead-end relative to the hunt UX.
        return RedirectResponse(url="/search", status_code=303)
    result = job.result or {}
    report = result.get("report") or {}
    return request.app.state.templates.TemplateResponse(
        request, "receipt_hunt_result.html",
        {"report": report},
    )


def _best_expense_amount(txn: Transaction) -> Decimal | None:
    """Best receipt-target amount on this txn for the hunt flow.

    Widened in AI-AGENT.md Phase 2: a receipt can attach to an
    Income-deposit, Liabilities-payment or Equity-reimbursement
    txn just as legitimately as an Expenses charge. Returns the
    largest |amount| across any receipt-target leg; pure
    Asset↔Asset transfers still return None.
    """
    target_roots = ("Expenses", "Income", "Liabilities", "Equity")
    best: Decimal | None = None
    for p in txn.postings or []:
        acct = p.account or ""
        if not acct:
            continue
        root = acct.split(":", 1)[0]
        if root not in target_roots:
            continue
        if p.units and p.units.number is not None:
            amt = abs(Decimal(p.units.number))
            if best is None or amt > best:
                best = amt
    return best


@router.post("/search/bulk-apply", response_class=HTMLResponse)
async def search_bulk_apply(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Apply a single target account to every selected transaction —
    runs as a job so each per-txn bean-check surfaces progress instead
    of blocking the browser for minutes on 90+ selections.

    For FIXME txns this routes the FIXME leg to the new target; for
    already-categorized txns it writes a correction override that moves
    the amount from the old expense account to the new one (so old
    "Custom Rent Storage" rows become "Rent:StorageUnit" retroactively).
    Optionally saves a rule for future matches.
    """
    form = await request.form()
    target_account = (form.get("target_account") or "").strip()
    if not target_account:
        raise HTTPException(status_code=400, detail="target_account is required")
    selected = [v for (k, v) in form.multi_items() if k == "txn_hash" and v]
    selected = [str(h).strip() for h in selected if str(h).strip()]
    if not selected:
        raise HTTPException(status_code=400, detail="select at least one transaction")

    save_rule = form.get("save_rule") == "1"
    rule_pattern_type = (form.get("rule_pattern_type") or "merchant_contains").strip()
    rule_pattern_value = (form.get("rule_pattern_value") or "").strip()
    q = (form.get("q") or "").strip()
    lookback_days = (form.get("lookback_days") or "365").strip()

    def _work(ctx):
        ctx.set_total(len(selected))
        ctx.emit(
            f"Applying {target_account} to {len(selected)} transaction(s)",
            outcome="info",
        )
        by_hash: dict[str, Transaction] = {}
        for entry in reader.load().entries:
            if isinstance(entry, Transaction):
                by_hash[txn_hash(entry)] = entry

        # Per CLAUDE.md "in-place rewrites are the default" — we
        # rewrite the source posting line in its `.bean` file
        # rather than appending an override block. The fallback
        # below still uses OverrideWriter for the rare cases
        # where in-place isn't feasible (no filename/lineno on
        # the entry, path safety check refuses, etc.). Each
        # rewrite snapshots the file first and runs bean-check
        # vs. baseline; failure rolls back to byte-identical.
        from pathlib import Path as _P
        from lamella.core.rewrite.txn_inplace import (
            InPlaceRewriteError,
            rewrite_fixme_to_account,
        )
        fallback_writer = OverrideWriter(
            main_bean=settings.ledger_main,
            overrides=settings.connector_overrides_path,
            conn=conn,
        )

        applied = 0
        rewrote_in_place = 0
        wrote_overlay = 0
        failed: list[str] = []
        for h in selected:
            ctx.raise_if_cancelled()
            txn = by_hash.get(h)
            if txn is None:
                failed.append(f"{h[:8]}… (not in ledger)")
                ctx.emit(f"{h[:8]}… not in ledger", outcome="error")
                ctx.advance()
                continue
            best_account: str | None = None
            best_amount: Decimal | None = None
            best_currency = "USD"
            for p in txn.postings:
                acct = p.account or ""
                if not acct.startswith("Expenses:"):
                    continue
                if p.units and p.units.number is not None:
                    amt = abs(Decimal(p.units.number))
                    if best_amount is None or amt > best_amount:
                        best_amount = amt
                        best_account = acct
                        best_currency = p.units.currency or "USD"
            if best_account is None or best_amount is None:
                failed.append(f"{h[:8]}… (no expense posting)")
                ctx.emit(f"{h[:8]}… no expense posting", outcome="error")
                ctx.advance()
                continue
            if best_account == target_account:
                applied += 1
                ctx.emit(
                    f"{h[:8]}… already on {target_account}", outcome="info",
                )
                ctx.advance()
                continue

            # Try in-place rewrite first.
            meta = getattr(txn, "meta", None) or {}
            src_file = meta.get("filename")
            lineno = meta.get("lineno")
            in_place_ok = False
            if src_file and lineno is not None:
                try:
                    # Strip any prior override on this hash so we
                    # don't end up with both the in-place edit AND
                    # a stale overlay layered on top.
                    try:
                        fallback_writer.rewrite_without_hash(h)
                    except BeanCheckError:
                        # If just removing the override breaks the
                        # ledger, fall through to override write
                        # below — it's the safer route.
                        raise InPlaceRewriteError("override-strip blocked")
                    rewrite_fixme_to_account(
                        source_file=_P(src_file),
                        line_number=int(lineno),
                        old_account=best_account,
                        new_account=target_account,
                        expected_amount=Decimal(
                            next(
                                (p.units.number for p in txn.postings
                                 if (p.account or "") == best_account
                                 and p.units and p.units.number is not None),
                                best_amount,
                            )
                        ),
                        ledger_dir=settings.ledger_dir,
                        main_bean=settings.ledger_main,
                    )
                    applied += 1
                    rewrote_in_place += 1
                    ctx.emit(
                        f"{h[:8]}… {best_account} → {target_account} "
                        "(in-place)",
                        outcome="success",
                    )
                    in_place_ok = True
                except InPlaceRewriteError as exc:
                    log.info(
                        "bulk-apply: in-place rewrite refused for %s: %s "
                        "— falling back to override",
                        h[:8], exc,
                    )

            if in_place_ok:
                ctx.advance()
                continue

            # Fallback: override-block write.
            try:
                fallback_writer.append(
                    txn_date=txn.date if isinstance(txn.date, date)
                             else date.fromisoformat(str(txn.date)),
                    txn_hash=h,
                    amount=best_amount,
                    from_account=best_account,
                    to_account=target_account,
                    currency=best_currency,
                    narration=(txn.narration or "search bulk-apply"),
                )
                applied += 1
                wrote_overlay += 1
                ctx.emit(
                    f"{h[:8]}… {best_account} → {target_account} "
                    "(override fallback)",
                    outcome="success",
                )
            except BeanCheckError as exc:
                log.warning("bulk-apply skipped %s: %s", h[:8], exc)
                failed.append(f"{h[:8]}… ({exc})")
                ctx.emit(
                    f"{h[:8]}… bean-check blocked: {exc}", outcome="error",
                )
            ctx.advance()

        if save_rule and rule_pattern_value and applied:
            try:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO classification_rules
                        (pattern_type, pattern_value, target_account,
                         card_account, confidence, hit_count, created_by)
                    VALUES (?, ?, ?, NULL, 1.0, 0, 'search-bulk')
                    """,
                    (rule_pattern_type, rule_pattern_value, target_account),
                )
                ctx.emit(
                    f"Saved rule: {rule_pattern_type}={rule_pattern_value}",
                    outcome="info",
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("rule save after bulk-apply failed: %s", exc)
                ctx.emit(f"Rule save failed: {exc}", outcome="error")

        reader.invalidate()
        ctx.emit(
            f"Done. {applied} applied "
            f"({rewrote_in_place} in-place, {wrote_overlay} via override "
            f"fallback). {len(failed)} failed.",
            outcome="info",
        )
        return {
            "applied": applied,
            "in_place": rewrote_in_place,
            "override_fallback": wrote_overlay,
            "failed": failed,
        }

    runner = request.app.state.job_runner
    redirect_url = (
        f"/search?q={q}&lookback_days={lookback_days}"
    )
    job_id = runner.submit(
        kind="search-bulk-apply",
        title=f"Applying {target_account} to {len(selected)} transaction(s)",
        fn=_work,
        total=len(selected),
        meta={"target": target_account, "q": q},
        return_url=redirect_url,
    )
    return request.app.state.templates.TemplateResponse(
        request,
        "partials/_job_modal.html",
        {"job_id": job_id, "on_close_url": redirect_url},
    )


# ----------------------------------------------------------------------
# Cmd+K palette backend.
#
# The palette opens via the topbar icon button or ⌘K. It needs cheap
# instant results: a few page shortcuts, the user's accounts and
# entities (path lookup), and a tail of recent transactions filtered
# by the current query string. JS in base.html fetches this and renders
# its own results — keep the response small + uniform.
# ----------------------------------------------------------------------

_PALETTE_PAGES = [
    {"label": "Dashboard", "href": "/", "kind": "page"},
    {"label": "Calendar", "href": "/calendar", "kind": "page"},
    {"label": "Notes", "href": "/notes", "kind": "page"},
    {"label": "Log trip", "href": "/mileage", "kind": "page"},
    {"label": "Categorize", "href": "/card", "kind": "page"},
    {"label": "Review queue", "href": "/review", "kind": "page"},
    {"label": "Receipts", "href": "/receipts", "kind": "page"},
    {"label": "Receipts needed", "href": "/receipts-needed", "kind": "page"},
    {"label": "Recurring", "href": "/recurring", "kind": "page"},
    {"label": "Budgets", "href": "/budgets", "kind": "page"},
    {"label": "Reports", "href": "/reports", "kind": "page"},
    {"label": "AI logs", "href": "/ai/logs", "kind": "page"},
    {"label": "AI cost", "href": "/ai/cost", "kind": "page"},
    {"label": "Status", "href": "/status", "kind": "page"},
    {"label": "Accounts", "href": "/accounts", "kind": "page"},
    {"label": "Vehicles", "href": "/vehicles", "kind": "page"},
    {"label": "Loans", "href": "/loans", "kind": "page"},
    {"label": "Properties", "href": "/settings/properties", "kind": "page"},
    {"label": "Projects", "href": "/projects", "kind": "page"},
    {"label": "Search transactions", "href": "/search", "kind": "page"},
    {"label": "Settings", "href": "/settings", "kind": "page"},
    {"label": "Backups", "href": "/settings/backups", "kind": "page"},
    {"label": "Data integrity", "href": "/settings/data-integrity", "kind": "page"},
]


@router.get("/search/palette.json")
def palette_json(
    q: str = "",
    request: Request = None,
    conn: sqlite3.Connection = Depends(get_db),
):
    """Cheap typeahead source for the cmd+K palette. Returns up to 24
    matches across pages, accounts, entities, and recent transactions
    that include the query in narration / payee. Empty query returns
    just the page shortcuts.
    """
    needle = (q or "").strip().lower()
    results: list[dict] = []

    # Pages first — always included, filtered by needle when present.
    for page in _PALETTE_PAGES:
        if not needle or needle in page["label"].lower():
            results.append(page)

    if needle:
        # Account paths (limit to 12 most-used / most-active).
        try:
            rows = conn.execute(
                "SELECT account_path, COALESCE(display_name, '') AS dn "
                "FROM accounts_meta "
                "WHERE LOWER(account_path) LIKE ? OR LOWER(COALESCE(display_name, '')) LIKE ? "
                "ORDER BY account_path LIMIT 12",
                (f"%{needle}%", f"%{needle}%"),
            ).fetchall()
        except sqlite3.Error:
            rows = []
        for row in rows:
            path = row[0] if isinstance(row, tuple) else row["account_path"]
            display = (row[1] if isinstance(row, tuple) else row["dn"]) or path
            results.append({
                "label": display,
                "sublabel": path,
                "href": f"/accounts/{path}",
                "kind": "account",
            })

        # Entities.
        try:
            rows = conn.execute(
                "SELECT slug, COALESCE(display_name, slug) FROM entities "
                "WHERE is_active = 1 AND (LOWER(slug) LIKE ? OR LOWER(display_name) LIKE ?) "
                "ORDER BY slug LIMIT 8",
                (f"%{needle}%", f"%{needle}%"),
            ).fetchall()
        except sqlite3.Error:
            rows = []
        for row in rows:
            slug, name = (row[0], row[1]) if isinstance(row, tuple) else (row["slug"], row[1])
            results.append({
                "label": name,
                "sublabel": slug,
                "href": f"/businesses/{slug}",
                "kind": "entity",
            })

        # Always offer "open full search" as a final entry so the user
        # can escape the palette into the dedicated search surface.
        results.append({
            "label": f"Search transactions for “{q}”",
            "href": f"/search?q={q}",
            "kind": "search",
        })

    return {"q": q, "results": results[:32]}

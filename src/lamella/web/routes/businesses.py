# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Per-business hub — landing page for each entity.

Phase 1 of BUSINESS_SECTION_IMPROVEMENTS.md: a read-only dashboard
that answers 'how much did I make this month?' in the first screen.
The page is mostly KPI tiles, charts, and per-domain summary cards
that link out to existing editor pages. The single piece of inline
interactivity is the 'classify newest FIXME' button.

Routes:
    GET /businesses                              — index (entity cards)
    GET /businesses/{slug}                       — full dashboard
    GET /businesses/{slug}/period                — HTMX partial swap when
                                                   the period selector changes
    GET /businesses/{slug}/chart/pnl-monthly.json
    GET /businesses/{slug}/chart/expense-trend.json
    GET /assets                                  — unified asset view
"""
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal

from beancount.core.data import Transaction
from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from lamella.core.beancount_io import LedgerReader
from lamella.core.beancount_io.txn_hash import txn_hash
from lamella.core.identity import get_txn_id
from lamella.core.config import Settings
from lamella.web.deps import get_db, get_ledger_reader, get_settings
from lamella.features.dashboard import service as dash
from lamella.core.registry.service import list_entities

log = logging.getLogger(__name__)

router = APIRouter()


# Entities split into two top-level surfaces: /personal/* for
# entity_type == "personal" (Schedule A households / individuals)
# and /businesses/* for everything else (sole-prop, llc, partnership,
# s-corp, c-corp, trust, estate, nonprofit). The split is by
# entity_type, NOT by slug — a user's personal slug could be
# anything ("AJ", "Smith", "Household", etc.) and the routing has
# to follow the type.
def _is_personal_entity(entity_type: str | None) -> bool:
    return (entity_type or "").strip().lower() == "personal"


def _canonical_prefix(entity_type: str | None) -> str:
    """Return ``/personal`` for personal entities, ``/businesses``
    for everything else. Used by the redirect guard so a stale URL
    pointing at the wrong prefix bounces to its canonical home."""
    return "/personal" if _is_personal_entity(entity_type) else "/businesses"


def _redirect_if_wrong_prefix(
    request: Request, slug: str, entity_type: str | None,
) -> RedirectResponse | None:
    """If the request hit a prefix that doesn't match the entity's
    type (a personal entity under /businesses, or vice versa),
    return a 301 to the canonical path. Returns ``None`` when the
    prefix is already correct.

    Path detection is by `request.url.path` startswith — we don't
    rely on the route function knowing which prefix matched. The
    {slug} segment is replaced inline rather than re-deriving the
    full URL because the trailing path may carry sub-routes
    (`/expenses`, `/transactions/...`). Query string is preserved.
    """
    canonical = _canonical_prefix(entity_type)
    path = request.url.path
    if path.startswith(canonical + "/") or path == canonical:
        return None
    other = "/personal" if canonical == "/businesses" else "/businesses"
    if path.startswith(other + "/") or path == other:
        new_path = canonical + path[len(other):]
        if request.url.query:
            new_path = f"{new_path}?{request.url.query}"
        return RedirectResponse(new_path, status_code=301)
    return None


def _is_fixme(acct: str | None) -> bool:
    return bool(acct) and acct.split(":")[-1].upper() == "FIXME"


def _account_belongs_to(acct: str, slug: str) -> bool:
    if not acct or not slug:
        return False
    return slug in acct.split(":")[1:]


def _period_context(loaded, conn, slug: str, period_label: str) -> dict:
    """Build the period-dependent slice of the page context — KPIs,
    expense composition, top payees. Used both for full-page render and
    for the HTMX partial swap."""
    period = dash.resolve_period(period_label)
    kpis = dash.compute_period_kpis(conn, loaded, slug, period)
    composition = dash.compute_expense_composition(conn, loaded, slug, period)
    payees = dash.compute_top_payees(conn, loaded, slug, period)
    return {
        "period": {
            "label": period.label,
            "current_start": period.current_start,
            "current_end": period.current_end,
            "prior_start": period.prior_start,
            "prior_end": period.prior_end,
            "is_all_time": period.is_all_time,
        },
        "kpis": kpis,
        "composition": composition,
        "payees": payees,
    }


@router.get("/businesses", response_class=HTMLResponse)
@router.get("/personal", response_class=HTMLResponse)
def businesses_index(
    request: Request,
    reader: LedgerReader = Depends(get_ledger_reader),
    conn=Depends(get_db),
):
    is_personal_view = request.url.path == "/personal"
    all_entities = list_entities(conn, include_inactive=False)
    entities = [
        e for e in all_entities
        if _is_personal_entity(e.entity_type) == is_personal_view
    ]
    loaded = reader.load()
    entries = loaded.entries

    now = date.today()
    recent_since = now - timedelta(days=90)
    fixme_counts: dict[str, int] = defaultdict(int)
    recent_tx_counts: dict[str, int] = defaultdict(int)

    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        touched_slugs: set[str] = set()
        has_fixme = any(_is_fixme(p.account) for p in entry.postings)
        for p in entry.postings:
            acct = p.account or ""
            for e in entities:
                if _account_belongs_to(acct, e.slug):
                    touched_slugs.add(e.slug)
        for slug in touched_slugs:
            if has_fixme:
                fixme_counts[slug] += 1
            if entry.date >= recent_since:
                recent_tx_counts[slug] += 1

    loan_counts = {
        r["entity_slug"]: r["n"]
        for r in conn.execute(
            "SELECT entity_slug, COUNT(*) AS n FROM loans "
            "WHERE is_active = 1 AND entity_slug IS NOT NULL GROUP BY entity_slug"
        ).fetchall()
    }
    vehicle_counts = {
        r["entity_slug"]: r["n"]
        for r in conn.execute(
            "SELECT entity_slug, COUNT(*) AS n FROM vehicles "
            "WHERE COALESCE(is_active, 1) = 1 AND entity_slug IS NOT NULL GROUP BY entity_slug"
        ).fetchall()
    }
    property_counts = {
        r["entity_slug"]: r["n"]
        for r in conn.execute(
            "SELECT entity_slug, COUNT(*) AS n FROM properties "
            "WHERE is_active = 1 AND entity_slug IS NOT NULL GROUP BY entity_slug"
        ).fetchall()
    }

    # Per-entity headline KPI: net (1mo) and liquid cash. Cached, so
    # walking N entities here is cheap on the warm path.
    one_mo = dash.resolve_period(dash.DEFAULT_PERIOD)
    cards = []
    for e in entities:
        kpis = dash.compute_period_kpis(conn, loaded, e.slug, one_mo)
        cards.append({
            "slug": e.slug,
            "display_name": e.display_name or e.slug,
            "tax_schedule": e.tax_schedule,
            "entity_type": e.entity_type,
            "fixme_count": fixme_counts.get(e.slug, 0),
            "recent_tx_count": recent_tx_counts.get(e.slug, 0),
            "loan_count": loan_counts.get(e.slug, 0),
            "vehicle_count": vehicle_counts.get(e.slug, 0),
            "property_count": property_counts.get(e.slug, 0),
            "net_1mo": Decimal(kpis["net_current"]),
            "cash": Decimal(kpis["cash"]),
        })
    cards.sort(key=lambda c: (-c["fixme_count"], c["display_name"]))

    if is_personal_view:
        ctx = {
            "cards": cards,
            "view_kind": "personal",
            "page_title": "Personal",
            "page_subtitle": (
                "Schedule A households / individuals. Pick one for "
                "transactions, accounts, loans, vehicles, and "
                "properties in one place."
            ),
            "new_url": "/entities/new?entity_type=personal",
            "new_label": "+ New personal entity",
            "card_prefix": "/personal",
            "empty_label": "personal entities",
        }
    else:
        ctx = {
            "cards": cards,
            "view_kind": "business",
            "page_title": "Businesses",
            "page_subtitle": (
                "Each card is an entity in your ledger — pick one "
                "for transactions, accounts, loans, vehicles, and "
                "properties in one place. Entities with the most "
                "uncategorized activity float to the top."
            ),
            "new_url": "/entities/new",
            "new_label": "+ New business",
            "card_prefix": "/businesses",
            "empty_label": "businesses",
        }
    return request.app.state.templates.TemplateResponse(
        request, "businesses_index.html", ctx,
    )


@router.get("/businesses/{slug}", response_class=HTMLResponse)
@router.get("/personal/{slug}", response_class=HTMLResponse)
def business_detail(
    slug: str,
    request: Request,
    period: str = Query(default=dash.DEFAULT_PERIOD),
    reader: LedgerReader = Depends(get_ledger_reader),
    conn=Depends(get_db),
    settings: Settings = Depends(get_settings),
):
    entity_row = conn.execute(
        "SELECT * FROM entities WHERE slug = ?", (slug,)
    ).fetchone()
    if entity_row is None:
        raise HTTPException(status_code=404, detail="entity not found")
    entity = dict(entity_row)
    redirect = _redirect_if_wrong_prefix(request, slug, entity.get("entity_type"))
    if redirect is not None:
        return redirect

    loaded = reader.load()
    entries = loaded.entries

    # Single ledger walk for the page-level bits that the cache layer
    # doesn't own (recent transactions, latest FIXME, account balances).
    recent_txns: list[dict] = []
    fixme_txns: list[dict] = []
    balance_accum: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    cutoff = date.today() - timedelta(days=120)

    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        touches = any(_account_belongs_to(p.account or "", slug) for p in entry.postings)
        if not touches:
            continue
        has_fixme = any(_is_fixme(p.account) for p in entry.postings)
        primary_amount: Decimal | None = None
        currency = "USD"
        for p in entry.postings:
            acct = p.account or ""
            if not _account_belongs_to(acct, slug):
                continue
            if p.units is None or p.units.number is None:
                continue
            amt = Decimal(p.units.number)
            currency = p.units.currency or "USD"
            balance_accum[acct] += amt
            if primary_amount is None or abs(amt) > abs(primary_amount):
                primary_amount = amt
        row = {
            "hash": txn_hash(entry),
            "lamella_txn_id": get_txn_id(entry),
            "date": entry.date,
            "amount": primary_amount or Decimal("0"),
            "currency": currency,
            "payee": getattr(entry, "payee", None),
            "narration": entry.narration or "",
            "has_fixme": has_fixme,
        }
        if has_fixme:
            fixme_txns.append(row)
        if entry.date >= cutoff:
            recent_txns.append(row)

    recent_txns.sort(key=lambda r: r["date"], reverse=True)
    recent_txns = recent_txns[:25]
    fixme_txns.sort(key=lambda r: r["date"], reverse=True)
    needs_attention = fixme_txns[0] if fixme_txns else None
    fixme_remaining = max(len(fixme_txns) - 1, 0)
    # Attach the review-queue id for the newest-FIXME row so the
    # "Classify →" button can deep-link /card?item_id=<id> and open
    # that specific transaction (instead of "next open item"). Keyed
    # off source_ref = "fixme:<txn_hash>".
    if needs_attention:
        row = conn.execute(
            "SELECT id FROM review_queue "
            "WHERE kind = 'fixme' AND source_ref = ? AND resolved_at IS NULL "
            "LIMIT 1",
            (f"fixme:{needs_attention['hash']}",),
        ).fetchone()
        needs_attention["review_item_id"] = row["id"] if row else None

    # Period-dependent block (cached).
    period_ctx = _period_context(loaded, conn, slug, period)

    # 12-month series (cached, period-independent).
    pnl_monthly = dash.compute_monthly_pnl(conn, loaded, slug)
    expense_trend = dash.compute_expense_trend(conn, loaded, slug)

    # Specialized cards — only render when data exists.
    inventory_slugs = dash.discover_inventory_entities(entries)
    inventory = (
        dash.compute_inventory_summary(conn, loaded, slug)
        if slug in inventory_slugs else None
    )
    vehicles_summary = dash.compute_vehicle_summary(
        conn, loaded, slug, settings.mileage_rate,
    )
    properties_summary = dash.compute_property_summary(conn, loaded, slug)

    # Related-records grids (kept from the old layout).
    loans = [
        dict(r) for r in conn.execute(
            "SELECT * FROM loans WHERE entity_slug = ? "
            "ORDER BY is_active DESC, display_name",
            (slug,),
        ).fetchall()
    ]
    vehicles = [
        dict(r) for r in conn.execute(
            "SELECT * FROM vehicles WHERE entity_slug = ? ORDER BY slug", (slug,),
        ).fetchall()
    ]
    properties = [
        dict(r) for r in conn.execute(
            "SELECT * FROM properties WHERE entity_slug = ? ORDER BY slug", (slug,),
        ).fetchall()
    ]
    accounts = [
        dict(r) for r in conn.execute(
            "SELECT account_path, display_name, kind, institution, last_four "
            "FROM accounts_meta WHERE entity_slug = ? ORDER BY kind, display_name",
            (slug,),
        ).fetchall()
    ]

    ctx = {
        "entity": entity,
        "needs_attention": needs_attention,
        "fixme_remaining": fixme_remaining,
        "recent_txns": recent_txns,
        "balances": sorted(balance_accum.items()),
        "loans": loans,
        "vehicles": vehicles,
        "properties": properties,
        "accounts": accounts,
        "pnl_monthly": pnl_monthly,
        "expense_trend": expense_trend,
        "inventory": inventory,
        "vehicles_summary": vehicles_summary,
        "properties_summary": properties_summary,
        "period_options": dash.PERIOD_LABELS,
        **period_ctx,
    }
    return request.app.state.templates.TemplateResponse(
        request, "business_detail.html", ctx,
    )


@router.get("/businesses/{slug}/period", response_class=HTMLResponse)
@router.get("/personal/{slug}/period", response_class=HTMLResponse)
def business_period_swap(
    slug: str,
    request: Request,
    period: str = Query(default=dash.DEFAULT_PERIOD),
    reader: LedgerReader = Depends(get_ledger_reader),
    conn=Depends(get_db),
):
    """HTMX partial: re-renders just the KPI strip + composition + top
    payees + period label. Period-independent widgets (12-mo P&L, trend,
    recent txns, related grids) stay put."""
    entity_row = conn.execute(
        "SELECT entity_type FROM entities WHERE slug = ?", (slug,)
    ).fetchone()
    if entity_row is None:
        raise HTTPException(status_code=404, detail="entity not found")
    redirect = _redirect_if_wrong_prefix(request, slug, entity_row["entity_type"])
    if redirect is not None:
        return redirect
    loaded = reader.load()
    ctx = {
        "entity": {
            "slug": slug,
            "entity_type": entity_row["entity_type"],
        },
        **_period_context(loaded, conn, slug, period),
    }
    ctx["period_options"] = dash.PERIOD_LABELS
    return request.app.state.templates.TemplateResponse(
        request, "_business_period_block.html", ctx,
    )


@router.get("/businesses/{slug}/expenses", response_class=HTMLResponse)
@router.get("/personal/{slug}/expenses", response_class=HTMLResponse)
def business_expenses(
    slug: str,
    request: Request,
    period: str = Query(default=dash.DEFAULT_PERIOD),
    q: str = Query(default=""),
    category: str = Query(default=""),
    fixme: str = Query(default=""),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    reader: LedgerReader = Depends(get_ledger_reader),
    conn=Depends(get_db),
):
    """Per-business expense transaction list. The dashboard at
    /businesses/{slug} is read-only KPIs + charts; this is the
    drill-down — every expense leg in the selected period, with
    text search, per-category filter, and a FIXME-only toggle.

    Each row links to /txn/{lamella_txn_id}. The Expense category
    select doubles as both a filter and a useful catalog of every
    Expenses:{slug}:* leaf the entity has actually used.
    """
    entity_row = conn.execute(
        "SELECT * FROM entities WHERE slug = ?", (slug,)
    ).fetchone()
    if entity_row is None:
        raise HTTPException(status_code=404, detail="entity not found")
    entity = dict(entity_row)
    redirect = _redirect_if_wrong_prefix(request, slug, entity.get("entity_type"))
    if redirect is not None:
        return redirect

    period_window = dash.resolve_period(period)
    needle = (q or "").strip().lower()
    category_filter = (category or "").strip()
    only_fixme = fixme not in ("", "0", "false", "no")

    expense_prefix = f"Expenses:{slug}:"
    loaded = reader.load()
    entries = loaded.entries

    rows: list[dict] = []
    # Track every Expenses:{slug}:* path the entity touches inside the
    # window so the filter <select> in the template lists exactly the
    # categories the user has data for, not every Open directive.
    category_totals: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))

    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        if entry.date < period_window.current_start:
            continue
        if entry.date > period_window.current_end:
            continue
        # Quick reject: skip transactions that don't touch the entity at
        # all. Cheap loop guard before the heavier per-posting work.
        if not any(
            _account_belongs_to(p.account or "", slug)
            for p in (entry.postings or ())
        ):
            continue

        # Collect every leg this entity-touching txn contributes to
        # the expenses surface. Two shapes land here:
        #
        # 1. A real ``Expenses:{slug}:*`` posting — one row per
        #    posting (a split txn shows twice, by design — the
        #    per-category filter expects per-leg granularity).
        # 2. A bare ``Expenses:FIXME`` leg on a txn that touches the
        #    entity via a non-expense leg (the source-side
        #    Assets/Liabilities posting). The FIXME itself isn't
        #    namespaced to the entity, but the user definitely wants
        #    to see uncategorized rows here so they can triage them.
        #    Bucketed under the synthetic ``Expenses:FIXME`` category.
        legs: list[tuple[str, Decimal, str]] = []
        for posting in entry.postings:
            acct = posting.account or ""
            if posting.units is None or posting.units.number is None:
                continue
            amount = Decimal(posting.units.number)
            currency = posting.units.currency or "USD"
            if acct.startswith(expense_prefix):
                legs.append((acct, amount, currency))
            elif _is_fixme(acct):
                # Tag the txn's FIXME leg so the user can find it
                # via category=Expenses:FIXME.
                legs.append((acct, amount, currency))
        has_fixme = any(_is_fixme(a) for a, _, _ in legs)

        for acct, amount, currency in legs:
            category_totals[acct] += amount

            if category_filter and acct != category_filter:
                continue
            if only_fixme and not _is_fixme(acct):
                continue
            if needle:
                hay = " ".join(
                    filter(None, [
                        entry.payee or "",
                        entry.narration or "",
                    ])
                ).lower()
                if needle not in hay:
                    continue

            rows.append({
                "lamella_txn_id": get_txn_id(entry),
                "txn_hash": txn_hash(entry),
                "date": entry.date,
                "amount": amount,
                "currency": currency,
                "account": acct,
                "payee": getattr(entry, "payee", None),
                "narration": entry.narration or "",
                "has_fixme": _is_fixme(acct),
            })

    rows.sort(key=lambda r: (r["date"], r["lamella_txn_id"] or ""), reverse=True)
    total_rows = len(rows)
    page_rows = rows[offset:offset + limit]

    # Filter dropdown: every category the entity touched in the
    # window, sorted by spend desc so the most-used ones land on top.
    categories = [
        {
            "account": acct,
            "amount": amt,
        }
        for acct, amt in sorted(
            category_totals.items(), key=lambda kv: kv[1], reverse=True,
        )
    ]

    ctx = {
        "entity": entity,
        "rows": page_rows,
        "total_rows": total_rows,
        "limit": limit,
        "offset": offset,
        "categories": categories,
        "category_filter": category_filter,
        "q": q,
        "only_fixme": only_fixme,
        "period": {
            "label": period_window.label,
            "current_start": period_window.current_start,
            "current_end": period_window.current_end,
            "is_all_time": period_window.is_all_time,
        },
        "period_options": dash.PERIOD_LABELS,
    }
    return request.app.state.templates.TemplateResponse(
        request, "business_expenses.html", ctx,
    )


@router.get("/businesses/{slug}/transactions", response_class=HTMLResponse)
@router.get("/personal/{slug}/transactions", response_class=HTMLResponse)
def business_transactions(
    slug: str,
    request: Request,
    period: str = Query(default=dash.DEFAULT_PERIOD),
    q: str = Query(default=""),
    kind: str = Query(default=""),
    fixme: str = Query(default=""),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    reader: LedgerReader = Depends(get_ledger_reader),
    conn=Depends(get_db),
):
    """All transactions that touch the entity, regardless of leg type
    — deposits, transfers, expenses, income, equity moves. Companion
    to /businesses/{slug}/expenses, which shows only expense legs.

    One row per transaction (not per leg, unlike /expenses). The
    primary amount is the largest absolute-value leg on the entity
    side; the ``kind`` filter narrows by root account
    (Assets/Liabilities/Income/Expenses/Equity).
    """
    entity_row = conn.execute(
        "SELECT * FROM entities WHERE slug = ?", (slug,)
    ).fetchone()
    if entity_row is None:
        raise HTTPException(status_code=404, detail="entity not found")
    entity = dict(entity_row)
    redirect = _redirect_if_wrong_prefix(request, slug, entity.get("entity_type"))
    if redirect is not None:
        return redirect

    period_window = dash.resolve_period(period)
    needle = (q or "").strip().lower()
    kind_filter = (kind or "").strip()
    only_fixme = fixme not in ("", "0", "false", "no")

    loaded = reader.load()
    entries = loaded.entries
    rows: list[dict] = []
    # Counts per root for the kind dropdown ("Assets (32) / Expenses
    # (412) / …"). Useful to surface the actual distribution rather
    # than guessing what the entity has.
    kind_counts: dict[str, int] = defaultdict(int)

    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        if entry.date < period_window.current_start:
            continue
        if entry.date > period_window.current_end:
            continue
        # Pick out the leg(s) that touch the entity. The "primary"
        # amount + root for the row is the largest-by-abs leg.
        primary_amount: Decimal | None = None
        primary_currency = "USD"
        primary_root: str | None = None
        primary_account: str | None = None
        seen_roots: set[str] = set()
        for posting in entry.postings:
            acct = posting.account or ""
            if not _account_belongs_to(acct, slug):
                continue
            root = acct.split(":")[0] if acct else ""
            if root:
                seen_roots.add(root)
            if posting.units is None or posting.units.number is None:
                continue
            amt = Decimal(posting.units.number)
            if (
                primary_amount is None
                or abs(amt) > abs(primary_amount)
            ):
                primary_amount = amt
                primary_currency = posting.units.currency or "USD"
                primary_root = root
                primary_account = acct
        if primary_account is None:
            # No entity leg with a numeric amount — skip the
            # interpolated-balance edge case.
            continue
        for r in seen_roots:
            kind_counts[r] += 1

        has_fixme = any(_is_fixme(p.account) for p in entry.postings)

        # Filter by ANY entity-touching leg's root, not just the
        # largest-by-abs one. A deposit txn has both an Assets leg
        # (where money lands) and an Income leg (where it comes
        # from); kind=Income should match it regardless of which
        # leg is primary.
        if kind_filter and kind_filter not in seen_roots:
            continue
        if only_fixme and not has_fixme:
            continue
        if needle:
            hay = " ".join(
                filter(None, [entry.payee or "", entry.narration or ""])
            ).lower()
            if needle not in hay:
                continue

        rows.append({
            "lamella_txn_id": get_txn_id(entry),
            "txn_hash": txn_hash(entry),
            "date": entry.date,
            "amount": primary_amount,
            "currency": primary_currency,
            "account": primary_account,
            "root": primary_root,
            "payee": getattr(entry, "payee", None),
            "narration": entry.narration or "",
            "has_fixme": has_fixme,
        })

    rows.sort(
        key=lambda r: (r["date"], r["lamella_txn_id"] or ""), reverse=True,
    )
    total_rows = len(rows)
    page_rows = rows[offset:offset + limit]

    kinds = [
        {"root": k, "count": v}
        for k, v in sorted(kind_counts.items(), key=lambda kv: kv[1], reverse=True)
    ]

    ctx = {
        "entity": entity,
        "rows": page_rows,
        "total_rows": total_rows,
        "limit": limit,
        "offset": offset,
        "kinds": kinds,
        "kind_filter": kind_filter,
        "q": q,
        "only_fixme": only_fixme,
        "period": {
            "label": period_window.label,
            "current_start": period_window.current_start,
            "current_end": period_window.current_end,
            "is_all_time": period_window.is_all_time,
        },
        "period_options": dash.PERIOD_LABELS,
    }
    return request.app.state.templates.TemplateResponse(
        request, "business_transactions.html", ctx,
    )


@router.get("/businesses/{slug}/accounts", response_class=HTMLResponse)
@router.get("/personal/{slug}/accounts", response_class=HTMLResponse)
def business_accounts(
    slug: str,
    request: Request,
    reader: LedgerReader = Depends(get_ledger_reader),
    conn=Depends(get_db),
):
    """Every account this entity owns, grouped by root (Assets,
    Liabilities, Equity, Income, Expenses) with current balance.
    Each row links to the per-account /accounts/{path} detail.
    """
    entity_row = conn.execute(
        "SELECT * FROM entities WHERE slug = ?", (slug,)
    ).fetchone()
    if entity_row is None:
        raise HTTPException(status_code=404, detail="entity not found")
    entity = dict(entity_row)
    redirect = _redirect_if_wrong_prefix(request, slug, entity.get("entity_type"))
    if redirect is not None:
        return redirect

    # Walk the ledger once, tally balance per account whose entity
    # segment matches. Use accounts_meta for display details (alias,
    # institution, last-four) when registered.
    loaded = reader.load()
    entries = loaded.entries
    balances: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    currencies: dict[str, str] = {}
    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        for p in entry.postings:
            acct = p.account or ""
            if not _account_belongs_to(acct, slug):
                continue
            if p.units is None or p.units.number is None:
                continue
            balances[acct] += Decimal(p.units.number)
            currencies.setdefault(acct, p.units.currency or "USD")

    meta_rows = {
        r["account_path"]: dict(r) for r in conn.execute(
            "SELECT account_path, display_name, kind, institution, "
            "       last_four, closed_on "
            "FROM accounts_meta WHERE entity_slug = ?",
            (slug,),
        ).fetchall()
    }

    groups: dict[str, list[dict]] = defaultdict(list)
    for acct, bal in balances.items():
        meta = meta_rows.get(acct, {})
        root = acct.split(":")[0] if acct else "Other"
        groups[root].append({
            "account_path": acct,
            "balance": bal,
            "currency": currencies.get(acct, "USD"),
            "display_name": meta.get("display_name") or "",
            "institution": meta.get("institution") or "",
            "last_four": meta.get("last_four") or "",
            "kind": meta.get("kind") or "",
            "closed_on": meta.get("closed_on"),
            "registered": acct in meta_rows,
        })

    # Stable root ordering: Assets, Liabilities, Equity, Income, Expenses, then anything else alphabetically.
    canonical_order = ["Assets", "Liabilities", "Equity", "Income", "Expenses"]
    ordered_roots = [r for r in canonical_order if r in groups] + sorted(
        [r for r in groups if r not in canonical_order]
    )
    sections = []
    grand_total: Decimal = Decimal("0")
    for root in ordered_roots:
        accts = sorted(groups[root], key=lambda a: a["account_path"])
        subtotal = sum((a["balance"] for a in accts), Decimal("0"))
        if root in {"Assets", "Liabilities"}:
            grand_total += subtotal
        sections.append({"root": root, "accounts": accts, "subtotal": subtotal})

    ctx = {
        "entity": entity,
        "sections": sections,
        "grand_total": grand_total,
        "registered_count": sum(1 for a in balances if a in meta_rows),
        "unregistered_count": sum(1 for a in balances if a not in meta_rows),
    }
    return request.app.state.templates.TemplateResponse(
        request, "business_accounts.html", ctx,
    )


@router.get("/businesses/{slug}/chart/pnl-monthly.json")
@router.get("/personal/{slug}/chart/pnl-monthly.json")
def business_chart_pnl_monthly(
    slug: str,
    reader: LedgerReader = Depends(get_ledger_reader),
    conn=Depends(get_db),
):
    if conn.execute("SELECT 1 FROM entities WHERE slug = ?", (slug,)).fetchone() is None:
        raise HTTPException(status_code=404, detail="entity not found")
    return JSONResponse(dash.compute_monthly_pnl(conn, reader.load(), slug))


@router.get("/businesses/{slug}/chart/expense-trend.json")
@router.get("/personal/{slug}/chart/expense-trend.json")
def business_chart_expense_trend(
    slug: str,
    reader: LedgerReader = Depends(get_ledger_reader),
    conn=Depends(get_db),
):
    if conn.execute("SELECT 1 FROM entities WHERE slug = ?", (slug,)).fetchone() is None:
        raise HTTPException(status_code=404, detail="entity not found")
    return JSONResponse(dash.compute_expense_trend(conn, reader.load(), slug))


@router.post("/businesses/{slug}/seed-cogs", response_class=HTMLResponse)
@router.post("/personal/{slug}/seed-cogs", response_class=HTMLResponse)
def business_seed_cogs(
    slug: str,
    request: Request,
    settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
    conn = Depends(get_db),
):
    """Scaffold the four standard Schedule C Part III COGS leaves
    plus the inventory asset for a business that's just starting to
    track inventory.

    Per IRS Schedule C Part III, the four canonical COGS lines are:
      35 — Inventory at beginning of year
      36 — Purchases (less cost of items withdrawn for personal use)
      37 — Cost of labor (do not include amounts paid to yourself)
      38 — Materials and supplies
      39 — Other costs

    We open Purchases / Labor / MaterialsAndSupplies / OtherCosts
    under Expenses:{slug}:COGS:* (lines 36–39). Beginning/ending
    inventory anchors are the BALANCE side and live on
    Assets:{slug}:Inventory which we also open. The user runs this
    once when they answer "Yes — this business carries inventory"
    on /businesses/{slug}.

    Idempotent: passes ``existing_paths`` through write_opens so
    re-running the seeder on a partially-set-up business adds only
    the missing leaves without re-opening existing ones.
    """
    if conn.execute(
        "SELECT 1 FROM entities WHERE slug = ?", (slug,),
    ).fetchone() is None:
        raise HTTPException(status_code=404, detail="entity not found")

    from lamella.core.registry.accounts_writer import AccountsWriter
    from beancount.core.data import Open

    cogs_paths = [
        f"Expenses:{slug}:COGS:Purchases",
        f"Expenses:{slug}:COGS:MaterialsAndSupplies",
        f"Expenses:{slug}:COGS:Labor",
        f"Expenses:{slug}:COGS:OtherCosts",
        f"Assets:{slug}:Inventory",
    ]
    entries = list(reader.load().entries)
    opened = {e.account for e in entries if isinstance(e, Open)}
    writer = AccountsWriter(
        main_bean=settings.ledger_main,
        connector_accounts=settings.connector_accounts_path,
    )
    try:
        writer.write_opens(
            cogs_paths,
            comment=f"COGS scaffold for {slug} (Schedule C Part III lines 35–39)",
            existing_paths=opened,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "business %s: COGS seeder failed: %s", slug, exc,
        )
        return RedirectResponse(
            f"/businesses/{slug}?error=cogs_seed_failed",
            status_code=303,
        )
    reader.invalidate()
    return RedirectResponse(
        f"/businesses/{slug}?message=cogs_seeded",
        status_code=303,
    )


@router.get("/assets", response_class=HTMLResponse)
def assets_index(
    request: Request,
    conn=Depends(get_db),
):
    """Unified asset view — every vehicle + property the user tracks.
    Clicking drills into the admin detail for that asset."""
    vehicles = [
        dict(r) for r in conn.execute(
            "SELECT v.*, e.display_name AS entity_display_name "
            "FROM vehicles v LEFT JOIN entities e ON e.slug = v.entity_slug "
            "ORDER BY COALESCE(v.is_active, 1) DESC, v.slug"
        ).fetchall()
    ]
    properties = [
        dict(r) for r in conn.execute(
            "SELECT p.*, e.display_name AS entity_display_name "
            "FROM properties p LEFT JOIN entities e ON e.slug = p.entity_slug "
            "ORDER BY p.is_active DESC, p.slug"
        ).fetchall()
    ]
    return request.app.state.templates.TemplateResponse(
        request, "assets_index.html",
        {"vehicles": vehicles, "properties": properties},
    )

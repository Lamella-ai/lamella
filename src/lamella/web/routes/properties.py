# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Admin page for real properties (houses, land, rentals).

Parallel to /settings/vehicles. Each property auto-creates:
  - Assets:{Entity}:Property:{slug}  (book value) — or Assets:Property:{slug} if personal.
  - Expenses:{Entity}:Property:{slug}:Tax
  - Expenses:{Entity}:Property:{slug}:Insurance
  - Expenses:{Entity}:Property:{slug}:Maintenance
  - Expenses:{Entity}:Property:{slug}:Utilities
  - (for rentals) Income:{Entity}:Property:{slug}:Rent
"""
from __future__ import annotations

import logging
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from lamella.core.beancount_io import LedgerReader
from lamella.core.beancount_io.txn_hash import txn_hash
from lamella.core.config import Settings
from lamella.web.deps import get_db, get_ledger_reader, get_settings
from lamella.core.ledger_writer import BeanCheckError
from lamella.features.properties.loans_summary import loans_for_property
from lamella.features.properties.writer import (
    append_property,
    append_property_valuation,
)
from lamella.core.registry.accounts_writer import AccountsWriter
from lamella.core.registry.service import (
    list_entities,
    normalize_slug,
)

log = logging.getLogger(__name__)

router = APIRouter()

PROPERTY_TYPES = ("house", "land", "building", "condo", "rental", "other")


_DEFAULT_EXPENSE_CATEGORIES = ("Tax", "Insurance", "Maintenance", "Utilities", "HOA")


# Narration / payee tokens that strongly suggest a transaction is part of a
# property purchase timeline (pre-closing due diligence, earnest money, or
# the closing itself). Used to surface FIXME candidates the user probably
# wants re-classified into the property's asset account.
_ACQUISITION_TOKENS = (
    "earnest",
    "escrow deposit",
    "title",
    "closing",
    "inspection",
    "appraisal",
    "survey",
    "home warranty",
    "settlement",
    "recording fee",
    "attorney",
    "notary",
)


def _property_paths(slug: str, entity_slug: str | None, is_rental: bool) -> list[str]:
    """Return the canonical account paths a property scaffolds.

    Requires ``entity_slug`` — properties belong to entities
    (personal, LLC, etc.) and the old-style entity-less
    ``Assets:Property:{slug}`` convention is deprecated. Callers
    that don't have an entity must refuse to scaffold the property
    and ask the user to set one first; silently minting an
    entity-less path violates the `entity-first` rule and leaves
    the chart with orphan accounts nobody can report on.
    """
    if not entity_slug:
        raise ValueError(
            "property requires entity_slug — Assets:Property:... "
            "(no entity segment) is deprecated. Set the property's "
            "owning entity and retry."
        )
    asset = f"Assets:{entity_slug}:Property:{slug}"
    exp_root = f"Expenses:{entity_slug}:Property:{slug}"
    income_root = f"Income:{entity_slug}:Property:{slug}"
    paths = [asset]
    paths.extend(f"{exp_root}:{c}" for c in _DEFAULT_EXPENSE_CATEGORIES)
    if is_rental:
        paths.append(f"{income_root}:Rent")
    return paths


@router.get("/settings/properties", include_in_schema=False)
def properties_settings_legacy_redirect(request: Request):
    """ADR-0047 + ADR-0048: properties is a first-class concept, not
    a setting. Old URL 301s to /properties (querystring preserved)."""
    from fastapi.responses import RedirectResponse
    qs = request.url.query
    return RedirectResponse(
        "/properties" + (f"?{qs}" if qs else ""), status_code=301,
    )


@router.get("/properties/{slug}/edit-modal", response_class=HTMLResponse)
def property_edit_modal(
    slug: str,
    request: Request,
    conn = Depends(get_db),
):
    """HTMX fragment — quick-edit modal for one property. Mirrors the
    /entities and /vehicles patterns: form posts to /settings/properties,
    modal targets the dashboard card by id, server fires
    HX-Trigger=property-saved to close the modal. Valuations, dispose,
    refi history stay on /settings/properties/{slug}."""
    row = conn.execute(
        "SELECT * FROM properties WHERE slug = ?", (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="property not found")
    return request.app.state.templates.TemplateResponse(
        request, "partials/_property_modal_edit.html",
        {"property": dict(row), "property_types": PROPERTY_TYPES},
    )


@router.get("/properties/new-modal", response_class=HTMLResponse)
def property_new_modal(
    request: Request,
    conn = Depends(get_db),
):
    """HTMX fragment — "+ Add property" modal. Slim form: display
    name, slug, property_type, owning entity. Detailed fields
    (address, purchase_date / purchase_price, asset_account_path,
    valuations, etc.) live on /properties/{slug}."""
    entities = [
        dict(r) for r in conn.execute(
            "SELECT slug, display_name FROM entities "
            "WHERE is_active = 1 ORDER BY display_name, slug"
        ).fetchall()
    ]
    return request.app.state.templates.TemplateResponse(
        request, "partials/_property_modal_new.html",
        {"entities": entities, "property_types": PROPERTY_TYPES},
    )


@router.get("/properties", response_class=HTMLResponse)
def properties_page(
    request: Request,
    saved: str | None = None,
    conn = Depends(get_db),
):
    rows = conn.execute(
        "SELECT * FROM properties ORDER BY is_active DESC, display_name, slug"
    ).fetchall()
    properties = [dict(r) for r in rows]
    entities = list_entities(conn, include_inactive=False)
    ctx = {
        "properties": properties,
        "entities": entities,
        "property_types": PROPERTY_TYPES,
        "saved": saved,
    }
    return request.app.state.templates.TemplateResponse(
        request, "settings_properties.html", ctx
    )


@router.post("/settings/properties")
async def save_property(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    form = await request.form()
    display_name = (form.get("display_name") or "").strip() or None
    raw_slug = (form.get("slug") or "").strip()
    slug = normalize_slug(raw_slug, fallback_display_name=display_name)
    if not slug:
        raise HTTPException(
            status_code=400,
            detail="Couldn't derive a valid slug — type a display name or an explicit slug starting with a capital letter.",
        )
    property_type = (form.get("property_type") or "other").strip() or "other"
    entity_slug = (form.get("entity_slug") or "").strip() or None
    address = (form.get("address") or "").strip() or None
    city = (form.get("city") or "").strip() or None
    state = (form.get("state") or "").strip() or None
    postal_code = (form.get("postal_code") or "").strip() or None
    purchase_date = (form.get("purchase_date") or "").strip() or None
    purchase_price = (form.get("purchase_price") or "").strip() or None
    closing_costs = (form.get("closing_costs") or "").strip() or None
    asset_account_path = (form.get("asset_account_path") or "").strip() or None
    sale_date = (form.get("sale_date") or "").strip() or None
    sale_price = (form.get("sale_price") or "").strip() or None
    is_primary = 1 if form.get("is_primary_residence") == "1" else 0
    is_rental = 1 if form.get("is_rental") == "1" else 0
    is_active = form.get("is_active", "1")
    notes = (form.get("notes") or "").strip() or None

    # If the POST came from the New-Property form (carries
    # intent=create), refuse to silently clobber an existing record.
    # Auto-suggest a numeric suffix so the user can confirm a second
    # identical-slug property is intentional. Mirrors the vehicle slug-
    # collision pattern from commit d68dc1e.
    intent = (form.get("intent") or "").strip().lower()
    existing = conn.execute(
        "SELECT slug, entity_slug FROM properties WHERE slug = ?", (slug,),
    ).fetchone()
    if existing and intent == "create":
        from lamella.core.registry.service import disambiguate_slug
        suggested = disambiguate_slug(conn, slug, "properties")
        raise HTTPException(
            status_code=409,
            detail=(
                f"Property slug {slug!r} is already taken. "
                f"Try {suggested!r} instead — or use the edit page "
                f"if you meant to update the existing record."
            ),
        )
    if existing:
        # Entity reassignment on properties needs a deliberate
        # disposal + re-acquisition flow (real-estate transfers have
        # tax-basis consequences). Preserve the existing entity_slug
        # silently if the form tries to change it.
        locked = existing["entity_slug"]
        if locked and entity_slug and entity_slug != locked:
            log.warning(
                "save_property refused entity-slug change on %s: "
                "tried %r, kept %r (use property change-ownership flow)",
                slug, entity_slug, locked,
            )
        entity_slug = locked or entity_slug
        conn.execute(
            """
            UPDATE properties SET
                display_name=?, property_type=?, entity_slug=?, address=?,
                city=?, state=?, postal_code=?, purchase_date=?, purchase_price=?,
                closing_costs=?, asset_account_path=COALESCE(NULLIF(?, ''), asset_account_path),
                sale_date=?, sale_price=?, is_primary_residence=?, is_rental=?,
                is_active=?, notes=?
            WHERE slug=?
            """,
            (
                display_name, property_type, entity_slug, address, city, state,
                postal_code, purchase_date, purchase_price, closing_costs,
                asset_account_path or "",
                sale_date, sale_price, is_primary, is_rental,
                1 if is_active == "1" else 0, notes, slug,
            ),
        )
    else:
        if not entity_slug and not asset_account_path:
            raise HTTPException(
                status_code=400,
                detail=(
                    "New properties require entity_slug (or an explicit "
                    "asset_account_path). Entity-less "
                    "Assets:Property:... is deprecated — properties "
                    "belong to entities (Personal, LLC, etc.)."
                ),
            )
        computed_asset = asset_account_path or f"Assets:{entity_slug}:Property:{slug}"
        conn.execute(
            """
            INSERT INTO properties
                (slug, display_name, property_type, entity_slug, address,
                 city, state, postal_code, purchase_date, purchase_price,
                 closing_costs, asset_account_path,
                 sale_date, sale_price, is_primary_residence, is_rental,
                 is_active, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                slug, display_name, property_type, entity_slug, address, city,
                state, postal_code, purchase_date, purchase_price,
                closing_costs, computed_asset,
                sale_date, sale_price, is_primary, is_rental,
                1 if is_active == "1" else 0, notes,
            ),
        )
        create_tree = form.get("create_expense_tree") == "1"
        if create_tree:
            paths = _property_paths(slug, entity_slug, bool(is_rental))
            existing_paths: set[str] = set()
            for entry in reader.load().entries:
                acct = getattr(entry, "account", None)
                if isinstance(acct, str):
                    existing_paths.add(acct)
            writer = AccountsWriter(
                main_bean=settings.ledger_main,
                connector_accounts=settings.connector_accounts_path,
            )
            try:
                writer.write_opens(
                    paths,
                    comment=f"Property scaffold for {slug}",
                    existing_paths=existing_paths,
                )
            except BeanCheckError as exc:
                conn.execute("DELETE FROM properties WHERE slug = ?", (slug,))
                raise HTTPException(status_code=500, detail=f"bean-check failed: {exc}")
            reader.invalidate()

    # Mirror the state to the ledger so a DB wipe can rebuild it.
    # Best-effort: on bean-check failure we log and let the UI save
    # succeed (the SQLite row is authoritative in memory).
    saved = conn.execute(
        "SELECT * FROM properties WHERE slug = ?", (slug,)
    ).fetchone()
    if saved is not None:
        s = dict(saved)
        try:
            append_property(
                connector_config=settings.connector_config_path,
                main_bean=settings.ledger_main,
                slug=s["slug"],
                display_name=s.get("display_name"),
                property_type=s.get("property_type") or "other",
                entity_slug=s.get("entity_slug"),
                address=s.get("address"), city=s.get("city"),
                state=s.get("state"), postal_code=s.get("postal_code"),
                purchase_date=s.get("purchase_date"),
                purchase_price=s.get("purchase_price"),
                closing_costs=s.get("closing_costs"),
                asset_account_path=s.get("asset_account_path"),
                sale_date=s.get("sale_date"),
                sale_price=s.get("sale_price"),
                is_primary_residence=bool(s.get("is_primary_residence", 0)),
                is_rental=bool(s.get("is_rental", 0)),
                is_active=bool(s.get("is_active", 1)),
                notes=s.get("notes"),
            )
            reader.invalidate()
        except Exception as exc:  # noqa: BLE001
            log.warning("property directive write failed for %s: %s", slug, exc)
    # HTMX response shape:
    # - Modal-EDIT (HX-Target prefix property-card-) → return the card
    #   partial + HX-Trigger=property-saved so the page-level handler
    #   on /properties closes the modal and the card swaps in place.
    # - Modal-ADD (any other HTMX caller) → HX-Refresh so the dashboard
    #   reloads cleanly with the new card.
    # - Non-HTMX form post → 303 to /properties.
    headers = {k.lower(): v for k, v in request.headers.items()}
    is_hx = "hx-request" in headers
    hx_target = headers.get("hx-target", "")
    if is_hx and hx_target.startswith("property-card-"):
        row = conn.execute(
            "SELECT * FROM properties WHERE slug = ?", (slug,),
        ).fetchone()
        return request.app.state.templates.TemplateResponse(
            request, "partials/_property_card.html",
            {"c": dict(row)},
            headers={"HX-Trigger": "property-saved"},
        )
    if is_hx:
        return HTMLResponse(
            "", status_code=200, headers={"HX-Refresh": "true"},
        )
    return RedirectResponse(f"/properties?saved={slug}", status_code=303)


def _decimal(s) -> "Decimal | None":  # noqa: F821
    from decimal import Decimal as _D
    if s is None or s == "":
        return None
    raw = str(s).strip().replace(",", "").replace("$", "").replace(" ", "")
    if not raw:
        return None
    try:
        return _D(raw)
    except Exception:
        return None


@router.get("/settings/properties/{slug}", response_class=HTMLResponse)
def property_detail(
    slug: str,
    request: Request,
    saved: str | None = None,
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    from decimal import Decimal as _D
    from beancount.core.data import Transaction

    row = conn.execute("SELECT * FROM properties WHERE slug = ?", (slug,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="property not found")
    prop = dict(row)

    purchase_price = _decimal(prop.get("purchase_price")) or _D("0")
    closing_costs = _decimal(prop.get("closing_costs")) or _D("0")
    cost_basis = purchase_price + closing_costs

    # Linked loans + per-loan current balance + revolving headroom roll-up.
    # `loans_for_property` walks entries once and returns enriched rows
    # plus combined totals; the property panel uses both. The legacy
    # `loans` list (template-shaped) is built from summary.loans so the
    # existing template loop keeps working.
    entries = reader.load().entries
    loans_summary = loans_for_property(
        property_slug=slug, conn=conn, entries=entries,
        include_inactive=True,
    )
    loans = [
        {
            "slug": l.slug,
            "display_name": l.display_name,
            "loan_type": l.loan_type,
            "institution": l.institution,
            "original_principal": l.original_principal,
            "liability_account_path": l.liability_account_path,
            "is_revolving": l.is_revolving,
            "is_active": l.is_active,
            "credit_limit": l.credit_limit,
            "current_balance": l.current_balance,
            "available_headroom": l.available_headroom,
            "monthly_payment_estimate": l.monthly_payment_estimate,
        }
        for l in loans_summary.loans
    ]

    liability_paths = {l.get("liability_account_path") for l in loans if l.get("liability_account_path")}
    expense_rollup: dict[str, _D] = {}
    asset_path = prop.get("asset_account_path")
    book_value = _D("0")

    # Per-posting cost-basis ledger: every transaction that credits or
    # debits the property's asset account, ordered by date, with running
    # total. This is the core dashboard the user asked for — it makes the
    # gap between expected and actual cost basis visible.
    cost_basis_postings: list[dict] = []

    # Transactions that look acquisition-related but are still sitting on
    # FIXME. The user can click through to re-classify them into the asset
    # account (cost-basis) via the existing override flow.
    acquisition_fixme_candidates: list[dict] = []

    asset_account_seen_hashes: set[str] = set()

    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        entry_hash = None
        entry_asset_amt = _D("0")
        has_fixme = False
        fixme_amt: _D | None = None
        for p in entry.postings:
            if p.units is None or p.units.number is None:
                continue
            amt = _D(p.units.number)
            acct = p.account or ""
            if asset_path and acct == asset_path:
                book_value += amt
                entry_asset_amt += amt
            # Expense rollup for accounts under this property.
            if f":{slug}:" in f":{acct}:" and acct.startswith("Expenses:"):
                expense_rollup[acct] = expense_rollup.get(acct, _D("0")) + abs(amt)
            if acct.split(":")[-1].upper() == "FIXME":
                has_fixme = True
                fixme_amt = abs(amt)

        if asset_path and entry_asset_amt != 0:
            entry_hash = txn_hash(entry)
            asset_account_seen_hashes.add(entry_hash)
            cost_basis_postings.append({
                "txn_hash": entry_hash,
                "date": entry.date,
                "payee": getattr(entry, "payee", None),
                "narration": entry.narration or "",
                "amount": entry_asset_amt,
                "running": _D("0"),  # filled after sorting
            })

        if has_fixme and fixme_amt is not None:
            hay = " ".join(filter(None, [
                (entry.payee or ""),
                (entry.narration or ""),
            ])).lower()
            hit_tokens = [tok for tok in _ACQUISITION_TOKENS if tok in hay]
            if hit_tokens:
                # Skip anything already reclassified onto the asset
                # account — those show up in cost_basis_postings above.
                acquisition_fixme_candidates.append({
                    "txn_hash": txn_hash(entry),
                    "date": entry.date,
                    "payee": getattr(entry, "payee", None),
                    "narration": entry.narration or "",
                    "amount": fixme_amt,
                    "hits": hit_tokens,
                })

    # Liability balances are computed per-loan inside loans_for_property
    # (same magnitude semantics) and rolled up into combined_balance.
    current_debt = loans_summary.combined_balance

    # Sort the cost-basis list by date and fill running totals so the
    # template can render a running-balance column.
    cost_basis_postings.sort(key=lambda r: (r["date"], r["txn_hash"]))
    running = _D("0")
    for row in cost_basis_postings:
        running += row["amount"]
        row["running"] = running
    cost_basis_gap = cost_basis - book_value
    acquisition_fixme_candidates.sort(key=lambda r: r["date"], reverse=True)

    # Valuations
    valuations = [
        dict(r) for r in conn.execute(
            "SELECT id, as_of_date, value, source, notes "
            "FROM property_valuations WHERE property_slug = ? "
            "ORDER BY as_of_date DESC",
            (slug,),
        ).fetchall()
    ]
    current_value: _D | None = None
    if valuations:
        current_value = _decimal(valuations[0]["value"])
    equity: _D | None = None
    if current_value is not None:
        equity = current_value - current_debt

    entities = list_entities(conn, include_inactive=False)
    ctx = {
        "property": prop,
        "entities": entities,
        "property_types": PROPERTY_TYPES,
        "loans": loans,
        "loans_summary": loans_summary,
        "valuations": valuations,
        "current_value": current_value,
        "current_debt": current_debt,
        "cost_basis": cost_basis,
        "book_value": book_value,
        "cost_basis_gap": cost_basis_gap,
        "cost_basis_postings": cost_basis_postings,
        "acquisition_fixme_candidates": acquisition_fixme_candidates,
        "equity": equity,
        "expense_rollup": sorted(expense_rollup.items(), key=lambda kv: kv[1], reverse=True),
        "saved": saved,
    }
    return request.app.state.templates.TemplateResponse(
        request, "settings_property_detail.html", ctx,
    )


@router.post("/settings/properties/{slug}/valuations")
async def add_property_valuation(
    slug: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    row = conn.execute("SELECT slug FROM properties WHERE slug = ?", (slug,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="property not found")
    form = await request.form()
    as_of_date = (form.get("as_of_date") or "").strip()
    value = (form.get("value") or "").strip()
    if not as_of_date or not value:
        raise HTTPException(status_code=400, detail="as_of_date and value required")
    source = (form.get("source") or "").strip() or None
    notes = (form.get("notes") or "").strip() or None
    try:
        conn.execute(
            """
            INSERT INTO property_valuations (property_slug, as_of_date, value, source, notes)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (property_slug, as_of_date) DO UPDATE SET
                value = excluded.value,
                source = excluded.source,
                notes = excluded.notes
            """,
            (slug, as_of_date, value, source, notes),
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    try:
        append_property_valuation(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            property_slug=slug, as_of_date=as_of_date,
            value=value, source=source, notes=notes,
        )
        reader.invalidate()
    except Exception as exc:  # noqa: BLE001
        log.warning("property-valuation directive write failed for %s: %s", slug, exc)
    return RedirectResponse(f"/settings/properties/{slug}?saved=valuation", status_code=303)


@router.post("/settings/properties/{slug}/valuations/{valuation_id}/delete")
def delete_property_valuation(
    slug: str,
    valuation_id: int,
    conn = Depends(get_db),
):
    conn.execute(
        "DELETE FROM property_valuations WHERE id = ? AND property_slug = ?",
        (valuation_id, slug),
    )
    return RedirectResponse(f"/settings/properties/{slug}?saved=valuation-removed", status_code=303)


# ---------------------------------------------------------------------------
# Property outright sale (disposal). Vehicle has /vehicles/{slug}/dispose;
# this is the property analog. Writes a single 3-leg disposal block
# (asset out at book value, proceeds in to user-selected account,
# gain/loss plug). Updates properties row to is_active=0 + sale meta.
#
# Use case: vehicle/property leaves the user's books entirely
# (sold to outside party, gifted, scrapped). For intercompany
# transfer between two of the user's own entities, use the change-
# ownership transfer flow instead.
# ---------------------------------------------------------------------------


def _default_property_proceeds_account(p: dict) -> str:
    """Default landing for sale proceeds. Most users sell real estate
    via wire/check that lands in a primary checking account; the
    autocomplete picker lets them override. We don't try to detect
    "the right" bank — just suggest a plausible default the user can
    override."""
    entity = p.get("entity_slug") or "Personal"
    return f"Assets:{entity}:Bank:Checking"


def _default_property_gain_loss_account(p: dict) -> str:
    """Default plug account. Slug-embedded so the CPA can grep the
    chart of accounts and tie the disposal entry to its property."""
    entity = p.get("entity_slug") or "Personal"
    slug = p["slug"]
    return f"Income:{entity}:Property:{slug}:DisposalGainLoss"


@router.get(
    "/settings/properties/{slug}/dispose", response_class=HTMLResponse,
)
def property_dispose_form(
    slug: str, request: Request, conn=Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    row = conn.execute(
        "SELECT * FROM properties WHERE slug = ?", (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"property '{slug}' not found")
    if row["is_active"] == 0:
        raise HTTPException(
            status_code=400,
            detail=f"property '{slug}' is already inactive (sale_date "
                   f"{row['sale_date'] or '?'}); use the edit form to "
                   f"adjust if needed",
        )
    p = dict(row)

    # Compute current book value to pre-fill the form.
    from decimal import Decimal as _D
    from beancount.core.data import Transaction
    asset_path = (
        p.get("asset_account_path")
        or f"Assets:{p['entity_slug']}:Property:{slug}"
    )
    book_value = _D("0")
    for e in reader.load().entries:
        if not isinstance(e, Transaction):
            continue
        for posting in e.postings or ():
            if (
                posting.account == asset_path
                and posting.units
                and posting.units.number is not None
            ):
                book_value += _D(posting.units.number)

    from lamella.features.properties.disposal_writer import (
        VALID_DISPOSAL_TYPES,
    )
    return request.app.state.templates.TemplateResponse(
        request, "property_disposal_form.html",
        {
            "property": p,
            "today": date.today().isoformat(),
            "asset_path": asset_path,
            "current_book_value": book_value,
            "default_proceeds_account": _default_property_proceeds_account(p),
            "default_gain_loss_account": _default_property_gain_loss_account(p),
            "valid_types": sorted(VALID_DISPOSAL_TYPES),
        },
    )


@router.post("/settings/properties/{slug}/dispose")
async def property_dispose_commit(
    slug: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Single-shot disposal commit. (Vehicle has form → preview →
    commit; property is simpler — confirm modal on the form button
    handles the "are you sure?" gate without a separate preview
    page.)"""
    from datetime import date as _date_t
    from decimal import Decimal as _D, InvalidOperation as _DInv
    from urllib.parse import quote as _q
    from beancount.core.data import Open, Transaction
    from lamella.features.properties.disposal_writer import (
        PropertyDisposalDraft, VALID_DISPOSAL_TYPES, compute_gain_loss,
        new_disposal_id, write_disposal,
    )
    from lamella.core.registry.accounts_writer import AccountsWriter

    form = await request.form()
    base_redirect = f"/settings/properties/{slug}/dispose"

    def _err(code: str, detail: str | None = None) -> RedirectResponse:
        url = f"{base_redirect}?error={code}"
        if detail:
            url += f"&detail={_q(detail[:200])}"
        return RedirectResponse(url, status_code=303)

    row = conn.execute(
        "SELECT * FROM properties WHERE slug = ?", (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"property '{slug}' not found")
    if row["is_active"] == 0:
        return _err(
            "already-disposed",
            f"property is already marked inactive (sale_date "
            f"{row['sale_date'] or '?'})",
        )

    disposal_date_raw = (form.get("disposal_date") or "").strip()
    disposal_type = (form.get("disposal_type") or "sale").strip().lower()
    proceeds_raw = (form.get("proceeds_amount") or "").strip()
    proceeds_account = (form.get("proceeds_account") or "").strip()
    gain_loss_account = (form.get("gain_loss_account") or "").strip()
    buyer = (form.get("buyer_or_party") or "").strip() or None
    notes = (form.get("notes") or "").strip() or None

    if disposal_type not in VALID_DISPOSAL_TYPES:
        return _err("invalid-disposal-type", disposal_type)
    if not disposal_date_raw:
        return _err("missing-disposal-date")
    try:
        disposal_date = _date_t.fromisoformat(disposal_date_raw)
    except ValueError:
        return _err("invalid-disposal-date", disposal_date_raw)
    try:
        proceeds = _D(proceeds_raw or "0")
    except _DInv:
        return _err("non-numeric-proceeds")
    if proceeds < 0:
        return _err("negative-proceeds")
    if not proceeds_account:
        return _err("missing-proceeds-account")
    if not gain_loss_account:
        return _err("missing-gain-loss-account")

    asset_account = (
        row["asset_account_path"]
        or f"Assets:{row['entity_slug']}:Property:{slug}"
    )

    # Compute book value at disposal_date (postings on or before).
    entries = list(reader.load().entries)
    book_value = _D("0")
    for e in entries:
        if not isinstance(e, Transaction):
            continue
        e_date = (
            e.date if isinstance(e.date, _date_t)
            else _date_t.fromisoformat(str(e.date))
        )
        if e_date > disposal_date:
            continue
        for posting in e.postings or ():
            if (
                posting.account == asset_account
                and posting.units
                and posting.units.number is not None
            ):
                book_value += _D(posting.units.number)
    if book_value < 0:
        return _err(
            "negative-book-value",
            f"book value on {asset_account} is {book_value}; manual "
            "adjustment required before disposal",
        )

    gain_loss = compute_gain_loss(proceeds=proceeds, book_value=book_value)
    # Beancount Income accounts naturally carry credit (negative)
    # balances. A positive `proceeds - book_value` is a gain, which
    # belongs on the credit side of an Income account → we negate
    # the post amount when the user picked an Income account.
    if gain_loss_account.startswith("Income:"):
        gain_loss_post = -gain_loss
    else:
        gain_loss_post = gain_loss

    disposal_id = new_disposal_id()
    draft = PropertyDisposalDraft(
        disposal_id=disposal_id,
        property_slug=slug,
        property_display_name=row["display_name"],
        disposal_date=disposal_date,
        disposal_type=disposal_type,
        proceeds_amount=proceeds,
        proceeds_account=proceeds_account,
        asset_account=asset_account,
        asset_amount_out=book_value,
        gain_loss_account=gain_loss_account,
        gain_loss_amount=gain_loss_post,
        buyer_or_party=buyer,
        notes=notes,
    )

    # Open proceeds + gain/loss accounts on or before disposal_date if
    # not already. AccountsWriter is idempotent against the live set.
    existing_paths = {
        getattr(e, "account", None)
        for e in entries
        if isinstance(e, Open)
    }
    wanted = {proceeds_account, gain_loss_account}
    to_open = [p for p in wanted if p and p not in existing_paths]
    if to_open:
        opener = AccountsWriter(
            main_bean=settings.ledger_main,
            connector_accounts=settings.connector_accounts_path,
        )
        try:
            opener.write_opens(
                to_open,
                comment=f"Property disposal accounts for {slug}",
                existing_paths=existing_paths,
                earliest_ref_by_path={p: disposal_date for p in to_open},
            )
        except BeanCheckError as exc:
            return _err("bean-check-on-account-open", str(exc))
        reader.invalidate()

    try:
        write_disposal(
            draft=draft,
            main_bean=settings.ledger_main,
            overrides_path=settings.connector_overrides_path,
        )
    except BeanCheckError as exc:
        return _err("bean-check-rejected", str(exc))
    except Exception as exc:  # noqa: BLE001
        log.exception("property disposal failed for %s", slug)
        return _err(type(exc).__name__, str(exc))

    # Update properties row.
    conn.execute(
        "UPDATE properties SET is_active = 0, sale_date = ?, "
        "sale_price = ? WHERE slug = ?",
        (disposal_date.isoformat(), str(proceeds), slug),
    )
    conn.commit()
    reader.invalidate()
    return RedirectResponse(
        f"/settings/properties/{slug}?disposed=1&disposal_id={disposal_id}",
        status_code=303,
    )


# ---------------------------------------------------------------------------
# Property rename (misattribution-fix). Mirrors the vehicle rename
# flow at /vehicles/{slug}/change-ownership/rename. Use case: user
# labeled a property under the wrong entity (set up Personal but
# should have been Acme LLC). Rewrites postings on
# Expenses:<old>:Property:<slug>:*, Assets:<old>:Property:<slug>,
# AND Income:<old>:Property:<slug>:* (rentals) to the new entity.
# No real-world event recorded — just labels corrected.
# ---------------------------------------------------------------------------


@router.post("/settings/properties/{slug}/change-ownership/rename")
async def property_change_ownership_rename(
    slug: str,
    request: Request,
    conn = Depends(get_db),
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Misattribution-fix for properties. Same shape as
    `vehicle_change_ownership_rename` — Case B textual rewrite of
    pre-existing migration overrides + Case A new overrides for
    direct postings — extended to include the rental Income path
    and a refuse-if-custom-asset-path guard.

    Refuses if the property's `asset_account_path` is custom (not
    `Assets:<old_entity>:Property:<slug>`). User would need to clear
    the custom path manually first; the rewrite logic is path-
    pattern-based and a hand-set custom path falls outside its
    coverage."""
    import re
    from datetime import date as _date_t
    from decimal import Decimal as _D
    from urllib.parse import quote as _q

    from beancount.core.data import Transaction
    from lamella.core.beancount_io.txn_hash import txn_hash as _txn_hash
    from lamella.features.properties.property_companion import (
        ensure_property_chart,
    )
    from lamella.features.rules.overrides import OverrideWriter
    from lamella.features.setup.posting_counts import (
        is_override_txn, open_paths,
    )
    from lamella.features.setup.recovery import recovery_write_envelope

    form = await request.form()
    new_entity_slug = (form.get("new_entity_slug") or "").strip()

    base_redirect = f"/settings/properties/{slug}/change-ownership"

    def _err(code: str, detail: str | None = None) -> RedirectResponse:
        url = f"{base_redirect}?error={code}"
        if detail:
            url += f"&detail={_q(detail[:200])}"
        return RedirectResponse(url, status_code=303)

    if not new_entity_slug:
        return _err("missing-new-entity")

    row = conn.execute(
        "SELECT slug, display_name, entity_slug, is_rental, "
        "asset_account_path FROM properties WHERE slug = ?",
        (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"property '{slug}' not found")
    old_entity = row["entity_slug"]
    if not old_entity:
        return _err("current-entity-missing")
    if old_entity == new_entity_slug:
        return _err("same-entity")
    if not conn.execute(
        "SELECT 1 FROM entities WHERE slug = ? AND is_active = 1",
        (new_entity_slug,),
    ).fetchone():
        return _err("unknown-new-entity", new_entity_slug)

    canonical_asset = f"Assets:{old_entity}:Property:{slug}"
    if (
        row["asset_account_path"]
        and row["asset_account_path"] != canonical_asset
    ):
        return _err(
            "custom-asset-path",
            f"Property has a custom asset_account_path "
            f"({row['asset_account_path']!r}); rename rewrites the "
            f"canonical {canonical_asset!r} only. Clear the custom "
            f"path or use the intercompany transfer flow.",
        )

    old_expense_prefix = f"Expenses:{old_entity}:Property:{slug}:"
    new_expense_prefix = f"Expenses:{new_entity_slug}:Property:{slug}:"
    old_income_prefix = f"Income:{old_entity}:Property:{slug}:"
    new_income_prefix = f"Income:{new_entity_slug}:Property:{slug}:"
    old_asset = canonical_asset
    new_asset = f"Assets:{new_entity_slug}:Property:{slug}"

    overrides_path = settings.connector_overrides_path
    accounts_path = settings.connector_accounts_path

    counters = {"case_a": 0, "case_b": 0, "closed": 0}

    def _do_rename() -> None:
        # 1. Open canonical chart on new entity (idempotent, backdates).
        ensure_property_chart(
            conn=conn, settings=settings, reader=reader,
            property_slug=slug, entity_slug=new_entity_slug,
            is_rental=bool(row["is_rental"]),
        )
        reader.invalidate()

        # 2. Case B — textual rewrite of existing override blocks.
        # Three patterns (asset, expense prefix, income prefix).
        if overrides_path.exists():
            text = overrides_path.read_text(encoding="utf-8")
            asset_re = re.compile(
                rf"(?<![A-Za-z0-9:_\-]){re.escape(old_asset)}"
                rf"(?![A-Za-z0-9:_\-])"
            )
            expense_re = re.compile(
                rf"(?<![A-Za-z0-9:_\-]){re.escape(old_expense_prefix)}"
            )
            income_re = re.compile(
                rf"(?<![A-Za-z0-9:_\-]){re.escape(old_income_prefix)}"
            )
            new_text, n_a = asset_re.subn(new_asset, text)
            new_text, n_e = expense_re.subn(new_expense_prefix, new_text)
            new_text, n_i = income_re.subn(new_income_prefix, new_text)
            if new_text != text:
                overrides_path.write_text(new_text, encoding="utf-8")
                counters["case_b"] = n_a + n_e + n_i
                reader.invalidate()

        # 3. Case A — direct postings on old paths in original txns.
        entries = list(reader.load().entries)
        writer = OverrideWriter(
            main_bean=settings.ledger_main,
            overrides=overrides_path,
            conn=conn,
            run_check=False,
        )
        for entry in entries:
            if not isinstance(entry, Transaction):
                continue
            if is_override_txn(entry):
                continue
            for p in entry.postings or ():
                acct = p.account or ""
                if acct == old_asset:
                    new_acct = new_asset
                elif acct.startswith(old_expense_prefix):
                    new_acct = new_expense_prefix + acct[len(old_expense_prefix):]
                elif acct.startswith(old_income_prefix):
                    new_acct = new_income_prefix + acct[len(old_income_prefix):]
                else:
                    continue
                if p.units is None or p.units.number is None:
                    continue
                amt = _D(p.units.number)
                # OverrideWriter emits ``from: -amt, to: +amt``. For
                # debit-side originals (Asset/Expense, positive amt)
                # that cancels old + replicates on new. For credit-
                # side originals (Income/Liability, negative amt) the
                # same emission would DOUBLE the original. Fix: swap
                # from/to when the source posting is negative — the
                # override now reads ``new: -amt, old: +amt``, which
                # cancels the credit on old and replicates the credit
                # on new.
                if amt >= 0:
                    swap_from, swap_to = acct, new_acct
                else:
                    swap_from, swap_to = new_acct, acct
                writer.append(
                    txn_date=(
                        entry.date
                        if isinstance(entry.date, _date_t)
                        else _date_t.fromisoformat(str(entry.date))
                    ),
                    txn_hash=_txn_hash(entry),
                    amount=abs(amt),
                    from_account=swap_from,
                    to_account=swap_to,
                    currency=p.units.currency or "USD",
                    narration=(
                        entry.narration
                        or f"property rename {old_entity}→{new_entity_slug}"
                    ),
                    replace_existing=False,
                )
                counters["case_a"] += 1

        # 4. Close old-entity property accounts that are still Open.
        reader.invalidate()
        entries = list(reader.load().entries)
        opens = open_paths(entries)
        old_paths = {
            p for p in opens
            if p == old_asset
            or p.startswith(old_expense_prefix)
            or p.startswith(old_income_prefix)
        }
        if old_paths:
            accounts_path.parent.mkdir(parents=True, exist_ok=True)
            if not accounts_path.exists():
                accounts_path.write_text(
                    "; connector_accounts.bean — managed by Lamella.\n",
                    encoding="utf-8",
                )
            today = _date_t.today().isoformat()
            close_block = "\n".join(
                f"{today} close {p}" for p in sorted(old_paths)
            )
            with accounts_path.open("a", encoding="utf-8") as fh:
                fh.write(f"\n; rename — close old-entity paths for {slug}\n")
                fh.write(close_block + "\n")
            counters["closed"] = len(old_paths)

        # 5. SQLite update — reassign entity + update asset_account_path.
        conn.execute(
            "UPDATE properties SET entity_slug = ?, "
            "asset_account_path = ? WHERE slug = ?",
            (new_entity_slug, new_asset, slug),
        )
        conn.commit()

    try:
        recovery_write_envelope(
            main_bean=settings.ledger_main,
            files_to_snapshot=[
                settings.ledger_main, accounts_path, overrides_path,
            ],
            write_fn=_do_rename,
        )
    except BeanCheckError as exc:
        conn.execute(
            "UPDATE properties SET entity_slug = ?, "
            "asset_account_path = ? WHERE slug = ?",
            (old_entity, canonical_asset, slug),
        )
        conn.commit()
        return _err("bean-check-rejected", str(exc))
    except Exception as exc:  # noqa: BLE001
        conn.execute(
            "UPDATE properties SET entity_slug = ?, "
            "asset_account_path = ? WHERE slug = ?",
            (old_entity, canonical_asset, slug),
        )
        conn.commit()
        log.exception("property rename failed for %s", slug)
        return _err(type(exc).__name__, str(exc))

    reader.invalidate()
    return RedirectResponse(
        f"/settings/properties/{slug}?renamed_to={_q(new_entity_slug)}"
        f"&case_a={counters['case_a']}"
        f"&case_b={counters['case_b']}"
        f"&closed={counters['closed']}",
        status_code=303,
    )


# ---------------------------------------------------------------------------
# Phase 4.3 — property change-ownership (intercompany transfer).
#
# Mirrors the vehicle change-ownership flow at
# /vehicles/{slug}/change-ownership. Records a real-event intercompany
# transfer: disposal entry on the old entity + acquisition entry on
# the new, written atomically through recovery_write_envelope.
#
# Bookkeeping mode — the system records facts the user states. CPA
# decides §1031 vs taxable sale vs §721 partnership-capital
# treatment. SaleRecapture is the visible CPA-touchpoint when
# transaction value differs from book value.
# ---------------------------------------------------------------------------


@router.get(
    "/settings/properties/{slug}/change-ownership",
    response_class=HTMLResponse,
)
def property_change_ownership_page(
    slug: str, request: Request, conn=Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Form page for property change-ownership. Mirrors the
    vehicle equivalent."""
    row = conn.execute(
        "SELECT * FROM properties WHERE slug = ?", (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"property '{slug}' not found")
    entities = list_entities(conn)
    # Compute current book value for the carryover-NBV display.
    from decimal import Decimal as _D
    from beancount.core.data import Transaction
    asset_path = (
        row["asset_account_path"]
        or f"Assets:{row['entity_slug']}:Property:{slug}"
    )
    book_value = _D("0")
    for e in reader.load().entries:
        if not isinstance(e, Transaction):
            continue
        for p in e.postings or ():
            if p.account == asset_path and p.units and p.units.number is not None:
                book_value += _D(p.units.number)
    return request.app.state.templates.TemplateResponse(
        request, "property_change_ownership.html",
        {
            "property": dict(row),
            "entities": entities,
            "asset_path": asset_path,
            "current_book_value": book_value,
        },
    )


@router.post(
    "/settings/properties/{slug}/change-ownership/transfer",
)
async def property_change_ownership_transfer(
    slug: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Intercompany property transfer. See docstring on the vehicle
    counterpart (`vehicle_change_ownership_transfer` in
    routes/vehicles.py) — same shape, same bookkeeper-not-tax
    philosophy, slug-embedded scaffolds."""
    from datetime import date as _date_t
    from decimal import Decimal as _D, InvalidOperation as _DInv
    from urllib.parse import quote as _q
    from beancount.core.data import Open, Transaction
    from lamella.core.registry.accounts_writer import AccountsWriter
    from lamella.features.properties.property_companion import (
        ensure_property_chart,
    )
    from lamella.features.setup.recovery import recovery_write_envelope
    from lamella.features.properties.transfer_writer import (
        PropertyTransferDraft, new_transfer_id, render_acquisition_block,
        render_disposal_block, required_open_paths,
        property_asset_path,
    )

    form = await request.form()
    new_entity_slug = (form.get("new_entity_slug") or "").strip()
    transfer_date_raw = (form.get("transfer_date") or "").strip()
    cash_raw = (form.get("cash_amount") or "0").strip() or "0"
    equity_raw = (form.get("equity_amount") or "0").strip() or "0"
    basis_choice = (form.get("basis_choice") or "").strip().lower()
    basis_explicit_raw = (form.get("basis_explicit") or "").strip()
    notes = (form.get("notes") or "").strip() or None

    base_redirect = f"/settings/properties/{slug}/change-ownership"

    def _err(code: str, detail: str | None = None) -> RedirectResponse:
        url = f"{base_redirect}?error={code}"
        if detail:
            url += f"&detail={_q(detail[:200])}"
        return RedirectResponse(url, status_code=303)

    if not new_entity_slug:
        return _err("missing-new-entity")
    if not transfer_date_raw:
        return _err("missing-transfer-date")
    try:
        transfer_date = _date_t.fromisoformat(transfer_date_raw)
    except ValueError:
        return _err("invalid-transfer-date", transfer_date_raw)
    try:
        cash_amount = _D(cash_raw)
        equity_amount = _D(equity_raw)
    except _DInv:
        return _err("non-numeric-amount")
    if cash_amount < 0 or equity_amount < 0:
        return _err("negative-amount")
    if cash_amount + equity_amount == 0:
        return _err(
            "zero-transaction-value",
            "Either cash or equity must be > 0.",
        )
    if basis_choice not in ("carryover", "sale_price", "explicit"):
        return _err("missing-basis-choice")

    row = conn.execute(
        "SELECT slug, display_name, entity_slug, is_rental, "
        "asset_account_path FROM properties WHERE slug = ?",
        (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"property '{slug}' not found")
    old_entity = row["entity_slug"]
    if not old_entity:
        return _err("current-entity-missing")
    if old_entity == new_entity_slug:
        return _err("same-entity")
    if not conn.execute(
        "SELECT 1 FROM entities WHERE slug = ? AND is_active = 1",
        (new_entity_slug,),
    ).fetchone():
        return _err("unknown-new-entity", new_entity_slug)

    # Read current NBV from the ledger.
    asset_account = (
        row["asset_account_path"]
        or property_asset_path(old_entity, slug)
    )
    entries = list(reader.load().entries)
    book_value = _D("0")
    for e in entries:
        if not isinstance(e, Transaction):
            continue
        e_date = (
            e.date if isinstance(e.date, _date_t)
            else _date_t.fromisoformat(str(e.date))
        )
        if e_date > transfer_date:
            continue
        for p in e.postings or ():
            if p.account == asset_account and p.units and p.units.number is not None:
                book_value += _D(p.units.number)
    if book_value < 0:
        return _err(
            "negative-book-value",
            f"book value on {asset_account} is {book_value}; manual "
            "adjustment required before transfer",
        )

    if basis_choice == "carryover":
        new_basis = book_value
    elif basis_choice == "sale_price":
        new_basis = cash_amount + equity_amount
    else:  # explicit
        try:
            new_basis = _D(basis_explicit_raw or "0")
        except _DInv:
            return _err("non-numeric-basis")
        if new_basis < 0:
            return _err("negative-basis")

    transfer_id = new_transfer_id()
    draft = PropertyTransferDraft(
        transfer_id=transfer_id,
        property_slug=slug,
        property_display_name=row["display_name"],
        transfer_date=transfer_date,
        old_entity=old_entity,
        new_entity=new_entity_slug,
        book_value=book_value,
        cash_amount=cash_amount,
        equity_amount=equity_amount,
        new_basis=new_basis,
        notes=notes,
    )

    overrides_path = settings.connector_overrides_path
    accounts_path = settings.connector_accounts_path

    def _do_transfer() -> None:
        # 1. Open canonical property chart on new entity.
        ensure_property_chart(
            conn=conn, settings=settings, reader=reader,
            property_slug=slug, entity_slug=new_entity_slug,
            is_rental=bool(row["is_rental"]),
        )
        reader.invalidate()

        # 2. Open every disposal/acquisition sub-account this draft uses.
        live = list(reader.load().entries)
        existing_paths = {
            getattr(e, "account", None)
            for e in live
            if isinstance(e, Open)
        }
        wanted = required_open_paths(draft)
        to_open = [p for p in wanted if p and p not in existing_paths]
        if to_open:
            opener = AccountsWriter(
                main_bean=settings.ledger_main,
                connector_accounts=accounts_path,
            )
            opener.write_opens(
                to_open,
                comment=f"Property transfer scaffolds for {slug}",
                existing_paths=existing_paths,
            )
            reader.invalidate()

        # 3. Append both ledger transactions.
        from lamella.features.rules.overrides import (
            ensure_overrides_exists,
        )
        from lamella.core.ledger_writer import ensure_include_in_main
        ensure_overrides_exists(overrides_path)
        ensure_include_in_main(settings.ledger_main, overrides_path)
        with overrides_path.open("a", encoding="utf-8") as fh:
            fh.write(render_disposal_block(draft))
            fh.write(render_acquisition_block(draft))

        # 4. Mark old property inactive; create new property row on
        # target entity. Re-use display_name; disambiguate slug.
        transaction_value = cash_amount + equity_amount
        conn.execute(
            "UPDATE properties SET is_active = 0, sale_date = ?, "
            "sale_price = ? WHERE slug = ?",
            (
                transfer_date.isoformat(),
                str(transaction_value),
                slug,
            ),
        )
        from lamella.core.registry.service import disambiguate_slug
        new_row_slug = disambiguate_slug(conn, slug, "properties") or (slug + "B")
        cols = conn.execute(
            "PRAGMA table_info(properties)",
        ).fetchall()
        col_names = [c["name"] for c in cols]
        source = conn.execute(
            "SELECT * FROM properties WHERE slug = ?", (slug,),
        ).fetchone()
        new_values = {c: source[c] for c in col_names if c != "id"}
        new_values["slug"] = new_row_slug
        new_values["entity_slug"] = new_entity_slug
        new_values["is_active"] = 1
        new_values["sale_date"] = None
        new_values["sale_price"] = None
        new_values["asset_account_path"] = property_asset_path(
            new_entity_slug, slug,
        )
        placeholders = ", ".join("?" for _ in new_values)
        col_list = ", ".join(new_values.keys())
        conn.execute(
            f"INSERT INTO properties ({col_list}) VALUES ({placeholders})",
            tuple(new_values.values()),
        )
        conn.commit()

    try:
        recovery_write_envelope(
            main_bean=settings.ledger_main,
            files_to_snapshot=[
                settings.ledger_main, accounts_path, overrides_path,
            ],
            write_fn=_do_transfer,
        )
    except BeanCheckError as exc:
        # Roll back SQLite mutations; envelope reverted file writes.
        conn.execute(
            "UPDATE properties SET is_active = 1, sale_date = NULL, "
            "sale_price = NULL WHERE slug = ?",
            (slug,),
        )
        conn.execute(
            "DELETE FROM properties WHERE entity_slug = ? "
            "AND display_name = ? AND is_active = 1 "
            "AND sale_date IS NULL",
            (new_entity_slug, row["display_name"]),
        )
        conn.commit()
        return _err("bean-check-rejected", str(exc))
    except Exception as exc:  # noqa: BLE001
        log.exception("property transfer failed for %s", slug)
        conn.execute(
            "UPDATE properties SET is_active = 1, sale_date = NULL, "
            "sale_price = NULL WHERE slug = ?",
            (slug,),
        )
        conn.execute(
            "DELETE FROM properties WHERE entity_slug = ? "
            "AND display_name = ? AND is_active = 1 "
            "AND sale_date IS NULL",
            (new_entity_slug, row["display_name"]),
        )
        conn.commit()
        return _err(type(exc).__name__, str(exc))

    reader.invalidate()
    return RedirectResponse(
        f"/settings/properties/{slug}?transferred_to={_q(new_entity_slug)}"
        f"&transfer_id={transfer_id}",
        status_code=303,
    )

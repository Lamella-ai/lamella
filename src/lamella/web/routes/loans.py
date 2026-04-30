# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Admin page for loans (mortgage / auto / student / HELOC).

List / create / edit loans with institution, principal, APR, term.
Adding a new loan auto-scaffolds the ledger accounts:
  - Liabilities:{entity}:{institution}:{slug}
  - Expenses:{entity}:{slug}:Interest
  - Expenses:{entity}:{slug}:Escrow (when escrow_monthly is set)
  - Expenses:{entity}:{slug}:PropertyTax (when property_tax_monthly is set)
  - Expenses:{entity}:{slug}:Insurance   (when insurance_monthly is set)

Loan detail page `/settings/loans/{slug}` shows balance / interest
YTD / payoff projection via the amortization calculator.
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from lamella.core.beancount_io import LedgerReader
from lamella.core.beancount_io.txn_hash import txn_hash
from lamella.core.config import Settings
from lamella.web.deps import get_db, get_ledger_reader, get_settings
from lamella.features.loans.amortization import (
    amortization_schedule,
    monthly_payment,
    payment_number_on,
    split_for_payment_number,
)
from lamella.features.loans.scaffolding import (
    ScaffoldingError,
    autofix as scaffolding_autofix,
    check as scaffolding_check,
    ensure_open_on_or_before,
)
from lamella.features.loans.writer import (
    append_loan,
    append_loan_balance_anchor,
    write_loan_funding,
    write_synthesized_payment,
)
from lamella.core.ledger_writer import BeanCheckError
from lamella.core.registry.accounts_writer import AccountsWriter
from lamella.core.registry.service import is_valid_slug, list_entities, suggest_slug
from lamella.features.rules.overrides import OverrideWriter

log = logging.getLogger(__name__)

router = APIRouter()


LOAN_TYPES = ("mortgage", "auto", "student", "personal", "heloc", "other")


def _to_decimal(s: str | None) -> Decimal | None:
    if s is None or s == "":
        return None
    # Users paste values straight off statements ("$546,234.52"), so
    # strip the common noise before parsing — otherwise Decimal raises
    # and the caller silently falls back to 0, which produced the
    # "balance is $0.00" bug on anchored mortgages.
    raw = str(s).strip()
    raw = raw.replace(",", "").replace("$", "").replace(" ", "")
    if raw.startswith("(") and raw.endswith(")"):  # "(1,234.56)" = -1234.56
        raw = "-" + raw[1:-1]
    if not raw:
        return None
    try:
        return Decimal(raw)
    except Exception:
        return None


def _to_date_safe(value) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    try:
        return datetime.fromisoformat(str(value)[:10]).date()
    except Exception:
        return None


def _default_escrow_path(
    entity_slug: str | None,
    institution: str | None,
    slug: str,
) -> str | None:
    """Build the default Assets path for a mortgage escrow account.

    Escrow is money the servicer holds on your behalf to pay property
    tax / insurance — conceptually an Asset, not an expense. Modeling
    it as Assets lets you track the balance the servicer is holding
    and match their statements. Mirrors the liability-path shape so
    every account for this loan lives under a consistent subtree.
    """
    if not entity_slug or not slug:
        return None
    if institution:
        inst_slug = suggest_slug(institution) or institution.replace(" ", "")
        return f"Assets:{entity_slug}:{inst_slug}:{slug}:Escrow"
    return f"Assets:{entity_slug}:{slug}:Escrow"


def _ensure_open_on_or_before(
    reader: LedgerReader,
    opener: AccountsWriter,
    path: str,
    on_or_before: date | None,
    *,
    connector_accounts_path,
    comment_tag: str,
) -> None:
    """Route-layer wrapper around `loans.scaffolding.ensure_open_on_or_before`.

    The real logic lives in `loans/scaffolding.py` so both the existing
    funding/record-payment paths and the autofix endpoint share one
    implementation. This wrapper translates `ScaffoldingError` into
    `HTTPException` for FastAPI routes.
    """
    try:
        ensure_open_on_or_before(
            reader, opener, path, on_or_before,
            connector_accounts_path=connector_accounts_path,
            comment_tag=comment_tag,
        )
    except ScaffoldingError as exc:
        raise HTTPException(status_code=exc.status, detail=str(exc))


@router.get("/settings/loans", include_in_schema=False)
def loans_settings_legacy_redirect(request: Request):
    """ADR-0047 + ADR-0048: loans is a first-class user concept, not
    a setting. Old URL 301s to /loans (querystring preserved)."""
    from fastapi.responses import RedirectResponse
    qs = request.url.query
    return RedirectResponse(
        "/loans" + (f"?{qs}" if qs else ""), status_code=301,
    )


@router.get("/loans", response_class=HTMLResponse)
def loans_page(
    request: Request,
    saved: str | None = None,
    prefill_slug: str | None = None,
    prefill_display_name: str | None = None,
    prefill_institution: str | None = None,
    prefill_entity: str | None = None,
    prefill_type: str | None = None,
    prefill_revolving: str | None = None,
    prefill_account_path: str | None = None,
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    from lamella.core.registry.discovery import discover_loan_candidates

    rows = conn.execute(
        "SELECT * FROM loans ORDER BY is_active DESC, display_name, slug"
    ).fetchall()
    loans = [dict(r) for r in rows]
    entities = list_entities(conn, include_inactive=False)

    # Pull candidate Liabilities accounts from the ledger that look like
    # loans. Filter out any the user has already linked (by slug match
    # against existing loans table).
    candidates = discover_loan_candidates(reader.load().entries)
    existing_slugs = {r["slug"] for r in rows if r["slug"]}
    candidates = [c for c in candidates if c["suggested_slug"] not in existing_slugs]

    # Gather pre-fill values from query params (set by "Create loan from this"
    # button on a candidate row).
    prefill = None
    if prefill_slug or prefill_account_path:
        prefill = {
            "slug": prefill_slug or "",
            "display_name": prefill_display_name or "",
            "institution": prefill_institution or "",
            "entity_slug": prefill_entity or "",
            "loan_type": prefill_type or "mortgage",
            "is_revolving": prefill_revolving == "1",
            "account_path": prefill_account_path or "",
        }

    ctx = {
        "loans": loans,
        "entities": entities,
        "loan_types": LOAN_TYPES,
        "candidates": candidates,
        "saved": saved,
        "prefill": prefill,
    }
    return request.app.state.templates.TemplateResponse(
        request, "settings_loans.html", ctx
    )


@router.post("/settings/loans")
async def save_loan(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    form = await request.form()
    display_name = (form.get("display_name") or "").strip() or None
    slug = (form.get("slug") or "").strip()
    if not slug and display_name:
        slug = suggest_slug(display_name)
    if not is_valid_slug(slug):
        raise HTTPException(status_code=400, detail=f"invalid slug {slug!r}")

    # Phase 4.4: slug-collision check runs BEFORE field validation so
    # the user's "second mortgage with the same slug" submission gets
    # an actionable 409 + suggestion instead of a confusing
    # "original_principal required" 400 that doesn't mention the slug
    # is taken. Mirrors d68dc1e for vehicles.
    intent_early = (form.get("intent") or "").strip().lower()
    if intent_early == "create":
        existing_early = conn.execute(
            "SELECT slug FROM loans WHERE slug = ?", (slug,),
        ).fetchone()
        if existing_early:
            from lamella.core.registry.service import disambiguate_slug
            suggested = disambiguate_slug(conn, slug, "loans")
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Loan slug {slug!r} is already taken. "
                    f"Try {suggested!r} instead — or use the edit page "
                    f"if you meant to update the existing record."
                ),
            )

    loan_type = (form.get("loan_type") or "").strip() or "other"
    entity_slug = (form.get("entity_slug") or "").strip() or None
    institution = (form.get("institution") or "").strip() or None
    original_principal = (form.get("original_principal") or "").strip()
    if not original_principal:
        raise HTTPException(status_code=400, detail="original_principal required")
    funded_date = (form.get("funded_date") or "").strip()
    if not funded_date:
        raise HTTPException(status_code=400, detail="funded_date required")
    first_payment_date = (form.get("first_payment_date") or "").strip() or None
    payment_due_day = form.get("payment_due_day")
    term_months = form.get("term_months")
    apr = (form.get("interest_rate_apr") or "").strip() or None
    monthly_payment_estimate = (form.get("monthly_payment_estimate") or "").strip() or None
    escrow_monthly = (form.get("escrow_monthly") or "").strip() or None
    property_tax_monthly = (form.get("property_tax_monthly") or "").strip() or None
    insurance_monthly = (form.get("insurance_monthly") or "").strip() or None
    liability_account_path = (form.get("liability_account_path") or "").strip() or None
    interest_account_path = (form.get("interest_account_path") or "").strip() or None
    escrow_account_path = (form.get("escrow_account_path") or "").strip() or None
    simplefin_account_id = (form.get("simplefin_account_id") or "").strip() or None
    property_slug = (form.get("property_slug") or "").strip() or None
    payoff_date = (form.get("payoff_date") or "").strip() or None
    payoff_amount = (form.get("payoff_amount") or "").strip() or None
    is_active = form.get("is_active", "1")
    # Auto-close when a payoff_date is supplied with no explicit is_active:
    # the user indicated the loan has ended.
    if payoff_date and form.get("is_active") is None:
        is_active = "0"
    is_revolving_form = form.get("is_revolving")
    is_revolving = 1 if (is_revolving_form == "1" or is_revolving_form == "on") else 0
    credit_limit = (form.get("credit_limit") or "").strip() or None
    notes = (form.get("notes") or "").strip() or None

    # The intent=create / 409 gate already ran above (before field
    # validation) — by here, either intent != create or no existing
    # row was found. Either way the upsert below is the correct
    # behavior: no-intent edit submissions UPDATE; new-form creates
    # land in INSERT.
    existing = conn.execute("SELECT slug FROM loans WHERE slug = ?", (slug,)).fetchone()
    if existing:
        # Auto-fill an escrow account path when the user adds
        # escrow_monthly on edit but leaves the path blank — otherwise
        # the loan sits in a confusing half-configured state (monthly
        # amount recorded, but record-payment has nowhere to post).
        if escrow_monthly and not escrow_account_path:
            existing_escrow = conn.execute(
                "SELECT escrow_account_path FROM loans WHERE slug = ?",
                (slug,),
            ).fetchone()
            if not (existing_escrow and existing_escrow[0]):
                escrow_account_path = _default_escrow_path(
                    entity_slug, institution, slug,
                )
        conn.execute(
            """
            UPDATE loans SET
                display_name = ?, loan_type = ?, entity_slug = ?, institution = ?,
                original_principal = ?, funded_date = ?, first_payment_date = ?,
                payment_due_day = ?, term_months = ?, interest_rate_apr = ?,
                monthly_payment_estimate = ?, escrow_monthly = ?,
                property_tax_monthly = ?, insurance_monthly = ?,
                liability_account_path = COALESCE(NULLIF(?, ''), liability_account_path),
                interest_account_path  = COALESCE(NULLIF(?, ''), interest_account_path),
                escrow_account_path    = COALESCE(NULLIF(?, ''), escrow_account_path),
                simplefin_account_id = ?, property_slug = ?,
                payoff_date = ?, payoff_amount = ?,
                is_active = ?, is_revolving = ?, credit_limit = ?, notes = ?
            WHERE slug = ?
            """,
            (
                display_name, loan_type, entity_slug, institution,
                original_principal, funded_date, first_payment_date,
                int(payment_due_day) if payment_due_day else None,
                int(term_months) if term_months else None, apr,
                monthly_payment_estimate, escrow_monthly,
                property_tax_monthly, insurance_monthly,
                liability_account_path or "",
                interest_account_path or "",
                escrow_account_path or "",
                simplefin_account_id, property_slug,
                payoff_date, payoff_amount,
                1 if is_active == "1" else 0, is_revolving, credit_limit,
                notes, slug,
            ),
        )
    else:
        # Figure the account paths we plan to scaffold (or link to
        # existing accounts), so we can store them on the loan row.
        from lamella.core.registry.service import suggest_slug as _sug
        existing_liability = (form.get("existing_liability_path") or "").strip() or None
        computed_liability_path: str | None = None
        if liability_account_path:
            computed_liability_path = liability_account_path
        elif existing_liability:
            computed_liability_path = existing_liability
        elif entity_slug and institution:
            inst_slug = _sug(institution) or institution.replace(" ", "")
            computed_liability_path = f"Liabilities:{entity_slug}:{inst_slug}:{slug}"
        # Entity-first, loan-slug before category: every expense tied to
        # this mortgage rolls up under the loan's own subtree so reports
        # and drill-downs stay clean. Matches the :Vehicles:{slug}:*
        # convention — slug owns the subtree, category is a leaf.
        computed_interest_path = interest_account_path or (
            f"Expenses:{entity_slug}:{slug}:Interest" if entity_slug else None
        )
        computed_escrow_path = escrow_account_path or (
            _default_escrow_path(entity_slug, institution, slug)
            if escrow_monthly else None
        )
        # Optional tax / insurance sub-accounts so reporting can split
        # mortgage payments cleanly (helpful for rentals on Schedule E).
        property_tax_path = (
            f"Expenses:{entity_slug}:{slug}:PropertyTax"
            if (entity_slug and property_tax_monthly) else None
        )
        insurance_path = (
            f"Expenses:{entity_slug}:{slug}:Insurance"
            if (entity_slug and insurance_monthly) else None
        )
        conn.execute(
            """
            INSERT INTO loans
                (slug, display_name, loan_type, entity_slug, institution,
                 original_principal, funded_date, first_payment_date,
                 payment_due_day, term_months, interest_rate_apr,
                 monthly_payment_estimate, escrow_monthly, property_tax_monthly,
                 insurance_monthly, liability_account_path, interest_account_path,
                 escrow_account_path, simplefin_account_id, property_slug,
                 payoff_date, payoff_amount, is_active, is_revolving,
                 credit_limit, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                slug, display_name, loan_type, entity_slug, institution,
                original_principal, funded_date, first_payment_date,
                int(payment_due_day) if payment_due_day else None,
                int(term_months) if term_months else None, apr,
                monthly_payment_estimate, escrow_monthly,
                property_tax_monthly, insurance_monthly,
                computed_liability_path, computed_interest_path, computed_escrow_path,
                simplefin_account_id, property_slug,
                payoff_date, payoff_amount,
                1 if is_active == "1" else 0, is_revolving, credit_limit, notes,
            ),
        )
        # Auto-scaffold the ledger accounts. If the user came in via
        # "Create loan from this" (existing_liability_path set), skip
        # writing the liability account since it already exists.
        create_tree = form.get("create_expense_tree") == "1"
        if create_tree:
            paths: list[str] = []
            if computed_liability_path and not existing_liability:
                # Always emit — AccountsWriter.write_opens dedups against
                # `existing_paths`, so if the user typed a path that
                # already exists in the ledger it's a no-op. The old
                # "skip if liability_account_path set" check left the
                # account missing when the user typed a desired path
                # that hadn't yet been opened.
                paths.append(computed_liability_path)
            if computed_interest_path:
                paths.append(computed_interest_path)
            if computed_escrow_path:
                paths.append(computed_escrow_path)
            if property_tax_path:
                paths.append(property_tax_path)
            if insurance_path:
                paths.append(insurance_path)
            # Gather ledger paths so duplicates (e.g. existing EIDL liability)
            # are skipped rather than written again.
            existing_paths: set[str] = set()
            for entry in reader.load().entries:
                acct = getattr(entry, "account", None)
                if isinstance(acct, str):
                    existing_paths.add(acct)
            if paths:
                writer = AccountsWriter(
                    main_bean=settings.ledger_main,
                    connector_accounts=settings.connector_accounts_path,
                )
                # Use funded_date so historical payments against these
                # accounts don't get rejected as "inactive."
                opened_on = _to_date_safe(funded_date)
                try:
                    writer.write_opens(
                        paths,
                        opened_on=opened_on,
                        comment=f"Loan scaffold for {slug}",
                        existing_paths=existing_paths,
                    )
                except BeanCheckError as exc:
                    conn.execute("DELETE FROM loans WHERE slug = ?", (slug,))
                    raise HTTPException(status_code=500, detail=f"bean-check failed: {exc}")
                reader.invalidate()

    # Persist to the ledger too so reconstruct from a wiped DB can
    # rebuild this loan. Best-effort: on bean-check failure we log
    # and let the UI save succeed — the SQLite row is already committed
    # and reflects the user's intent. A later edit will re-try.
    saved_row = conn.execute(
        "SELECT * FROM loans WHERE slug = ?", (slug,)
    ).fetchone()
    if saved_row is not None:
        s = dict(saved_row)
        try:
            append_loan(
                connector_config=settings.connector_config_path,
                main_bean=settings.ledger_main,
                slug=s["slug"],
                display_name=s.get("display_name"),
                loan_type=s.get("loan_type") or "other",
                entity_slug=s.get("entity_slug"),
                institution=s.get("institution"),
                original_principal=s.get("original_principal") or "0",
                funded_date=s.get("funded_date"),
                first_payment_date=s.get("first_payment_date"),
                payment_due_day=s.get("payment_due_day"),
                term_months=s.get("term_months"),
                interest_rate_apr=s.get("interest_rate_apr"),
                monthly_payment_estimate=s.get("monthly_payment_estimate"),
                escrow_monthly=s.get("escrow_monthly"),
                property_tax_monthly=s.get("property_tax_monthly"),
                insurance_monthly=s.get("insurance_monthly"),
                liability_account_path=s.get("liability_account_path"),
                interest_account_path=s.get("interest_account_path"),
                escrow_account_path=s.get("escrow_account_path"),
                simplefin_account_id=s.get("simplefin_account_id"),
                property_slug=s.get("property_slug"),
                payoff_date=s.get("payoff_date"),
                payoff_amount=s.get("payoff_amount"),
                is_active=bool(s.get("is_active", 1)),
                is_revolving=bool(s.get("is_revolving", 0)),
                credit_limit=s.get("credit_limit"),
                notes=s.get("notes"),
            )
            reader.invalidate()
        except Exception as exc:  # noqa: BLE001
            log.warning("loan directive write failed for %s: %s", slug, exc)

    # When an edit is submitted from the detail page, return to it;
    # the list page otherwise.
    redirect_to = (form.get("redirect_to") or "").strip()
    if redirect_to and redirect_to.startswith("/settings/loans/"):
        return RedirectResponse(f"{redirect_to}?saved={slug}", status_code=303)
    return RedirectResponse(f"/settings/loans?saved={slug}", status_code=303)


@router.get("/settings/loans/{slug}/edit", response_class=HTMLResponse)
def loan_edit_page(
    slug: str,
    request: Request,
    saved: str | None = None,
    conn = Depends(get_db),
):
    """Dedicated edit view so the detail page stays summary-focused."""
    row = conn.execute("SELECT * FROM loans WHERE slug = ?", (slug,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="loan not found")
    loan = dict(row)
    entities = list_entities(conn, include_inactive=False)
    properties = [
        dict(r) for r in conn.execute(
            "SELECT slug, display_name FROM properties "
            "WHERE is_active = 1 OR slug = ? "
            "ORDER BY display_name, slug",
            (loan.get("property_slug") or "",),
        ).fetchall()
    ]
    ctx = {
        "loan": loan,
        "loan_types": LOAN_TYPES,
        "entities": entities,
        "properties": properties,
        "saved": saved,
    }
    return request.app.state.templates.TemplateResponse(
        request, "settings_loan_edit.html", ctx,
    )


@router.post("/settings/loans/{slug}/fund-initial")
async def fund_initial_balance(
    slug: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Post the opening-balance transaction for a loan.

    Shape:
        2025-10-27 * "Loan funding — {display_name}"
          Liabilities:…:Mortgage2025       -{principal}.00 USD
          {offset_account}                  +{principal}.00 USD

    The offset is usually:
      - Assets:{Entity}:{PropertySlug}:CostBasis (new purchase)
      - Equity:{Entity}:OpeningBalances         (historical loan, pre-ledger)
      - Assets:{Entity}:{Bank}:Checking         (cash-out refi / proceeds)
      - A custom path the user picks.
    """
    row = conn.execute("SELECT * FROM loans WHERE slug = ?", (slug,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="loan not found")
    loan = dict(row)

    liability_path = loan.get("liability_account_path")
    if not liability_path:
        raise HTTPException(
            status_code=400,
            detail="Set liability_account_path on the loan first.",
        )
    principal = _to_decimal(loan.get("original_principal"))
    if not principal or principal <= 0:
        raise HTTPException(status_code=400, detail="original_principal is missing.")

    form = await request.form()
    offset_account = (form.get("offset_account") or "").strip()
    if not offset_account:
        raise HTTPException(status_code=400, detail="offset_account is required")
    funded_date = (form.get("funded_date") or loan.get("funded_date") or "").strip()
    if not funded_date:
        raise HTTPException(status_code=400, detail="funded_date is required")
    narration = (
        (form.get("narration") or "").strip()
        or f"Loan funding — {loan.get('display_name') or slug}"
    )

    funded_date_dt = _to_date_safe(funded_date)

    # Ensure both the offset and the liability are Open on or before
    # funded_date. A loan's liability is auto-scaffolded with an Open
    # dated `date.today()` at creation time; if the user's funded_date
    # is historical, the funding posting lands before the Open and
    # bean-check rejects it as "inactive account."
    opener = AccountsWriter(
        main_bean=settings.ledger_main,
        connector_accounts=settings.connector_accounts_path,
    )
    try:
        for path in (liability_path, offset_account):
            _ensure_open_on_or_before(
                reader, opener, path, funded_date_dt,
                connector_accounts_path=settings.connector_accounts_path,
                comment_tag=f"Loan funding for {slug}",
            )
    except BeanCheckError as exc:
        raise HTTPException(status_code=500, detail=f"bean-check failed opening account: {exc}")
    reader.invalidate()

    try:
        write_loan_funding(
            loan=loan,
            settings=settings,
            funded_date=funded_date_dt,
            principal=principal,
            offset_account=offset_account,
            narration=narration,
        )
    except BeanCheckError as exc:
        raise HTTPException(status_code=500, detail=f"bean-check failed: {exc}")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    reader.invalidate()
    return RedirectResponse(
        f"/settings/loans/{slug}?saved=funded", status_code=303,
    )


@router.post("/settings/loans/{slug}/open-accounts")
def open_loan_accounts(
    slug: str,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Scaffold any of this loan's configured account paths that aren't
    yet `open`'d in the ledger. Used to heal loans where the initial
    scaffold was skipped (pre-fix) or where a path was typed in after
    the fact."""
    row = conn.execute("SELECT * FROM loans WHERE slug = ?", (slug,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="loan not found")
    loan = dict(row)

    wanted: list[str] = []
    for col in ("liability_account_path", "interest_account_path", "escrow_account_path"):
        v = loan.get(col)
        if v:
            wanted.append(v)
    entity = loan.get("entity_slug")
    if entity and loan.get("property_tax_monthly"):
        wanted.append(f"Expenses:{entity}:{slug}:PropertyTax")
    if entity and loan.get("insurance_monthly"):
        wanted.append(f"Expenses:{entity}:{slug}:Insurance")

    if not wanted:
        return RedirectResponse(f"/settings/loans/{slug}", status_code=303)

    existing: set[str] = set()
    for entry in reader.load().entries:
        acct = getattr(entry, "account", None)
        if isinstance(acct, str):
            existing.add(acct)

    writer = AccountsWriter(
        main_bean=settings.ledger_main,
        connector_accounts=settings.connector_accounts_path,
    )
    # Open accounts on the loan's funded_date (not today) so historical
    # payments posted against them don't fail bean-check with "inactive
    # account."
    opened_on = _to_date_safe(loan.get("funded_date"))
    try:
        writer.write_opens(
            wanted,
            opened_on=opened_on,
            comment=f"Loan scaffold for {slug} (heal)",
            existing_paths=existing,
        )
    except BeanCheckError as exc:
        raise HTTPException(status_code=500, detail=f"bean-check failed: {exc}")
    reader.invalidate()
    return RedirectResponse(
        f"/settings/loans/{slug}?saved=accounts-opened", status_code=303,
    )


@router.post("/settings/loans/{slug}/autofix")
async def loan_autofix(
    slug: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Apply the one-click fix for a scaffolding issue.

    Stale-click guard: re-runs `scaffolding.check()` before writing so a
    user clicking an autofix button for an issue that's already been
    resolved (by an earlier click, a reconstruct, or a manual edit)
    returns a clean 303 without writing anything.
    """
    row = conn.execute(
        "SELECT * FROM loans WHERE slug = ?", (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="loan not found")
    loan = dict(row)

    form = await request.form()
    kind = (form.get("kind") or "").strip()
    path = (form.get("path") or "").strip() or None
    if not kind:
        raise HTTPException(status_code=400, detail="kind is required")

    # Re-check from current ledger state; a stale UI click for a
    # fixed issue is a no-op redirect, not a 500.
    current = scaffolding_check(loan, reader.load().entries, conn, settings)
    matching = [
        i for i in current
        if i.kind == kind and (path is None or i.path == path)
    ]
    if not matching:
        return RedirectResponse(
            f"/settings/loans/{slug}?saved=autofix-noop", status_code=303,
        )

    try:
        scaffolding_autofix(
            kind, loan, path,
            settings=settings, reader=reader, conn=conn,
        )
    except ScaffoldingError as exc:
        raise HTTPException(status_code=exc.status, detail=str(exc))

    return RedirectResponse(
        f"/settings/loans/{slug}?saved=autofix", status_code=303,
    )


@router.post("/settings/loans/{slug}/categorize-draw")
async def loan_categorize_draw(
    slug: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """WP13 — categorize a draw on a revolving loan.

    A draw is a FIXME-bearing transaction on a revolving loan where
    the liability posting is negative (more debt). The user picks
    where the drawn money went (typically a checking account or a
    contractor expense) and we write an override re-classifying the
    FIXME leg.

    Stale-click guard: re-loads the entries, re-checks that the txn
    is still a draw (FIXME still present, liability still negative).
    If the FIXME has already been categorized between page render
    and form submit, the endpoint returns 303 saved=draw-noop
    instead of writing a duplicate override. Same shape as
    loan_autofix.
    """
    from beancount.core.data import Transaction
    from lamella.features.loans.revolving import is_draw_txn
    from lamella.features.rules.overrides import OverrideWriter

    row = conn.execute(
        "SELECT * FROM loans WHERE slug = ?", (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="loan not found")
    loan = dict(row)
    if not loan.get("is_revolving"):
        raise HTTPException(
            status_code=400,
            detail="Loan is not configured as revolving.",
        )
    liability_path = loan.get("liability_account_path")
    if not liability_path:
        raise HTTPException(
            status_code=400,
            detail="Loan has no liability_account_path configured.",
        )

    form = await request.form()
    target_hash = (form.get("txn_hash") or "").strip()
    destination = (form.get("destination_account") or "").strip()
    if not target_hash or not destination:
        raise HTTPException(
            status_code=400,
            detail="txn_hash and destination_account are required.",
        )

    # Stale-click guard: walk current entries to find the txn by
    # hash and confirm it's still a draw.
    target_entry = None
    for entry in reader.load().entries:
        if not isinstance(entry, Transaction):
            continue
        if txn_hash(entry) == target_hash:
            target_entry = entry
            break
    if target_entry is None:
        # Already overridden / removed — pre-existing override stripped
        # the FIXME-bearing block, or a parallel session got there first.
        return RedirectResponse(
            f"/settings/loans/{slug}?saved=draw-noop", status_code=303,
        )
    if not is_draw_txn(target_entry, liability_path):
        return RedirectResponse(
            f"/settings/loans/{slug}?saved=draw-noop", status_code=303,
        )

    # Find the FIXME leg + the magnitude of the draw.
    fixme_account = None
    draw_amount = Decimal("0")
    for p in target_entry.postings or []:
        acct = getattr(p, "account", None) or ""
        units = getattr(p, "units", None)
        if units is None or getattr(units, "number", None) is None:
            continue
        if acct == liability_path:
            draw_amount = Decimal(str(units.number)).copy_abs()
        if acct.split(":")[-1].upper() == "FIXME":
            fixme_account = acct
    if fixme_account is None or draw_amount <= 0:
        return RedirectResponse(
            f"/settings/loans/{slug}?saved=draw-noop", status_code=303,
        )

    writer = OverrideWriter(
        main_bean=settings.ledger_main,
        overrides=settings.connector_overrides_path,
        conn=conn,
    )
    try:
        writer.append(
            txn_date=target_entry.date,
            txn_hash=target_hash,
            amount=draw_amount,
            from_account=fixme_account,
            to_account=destination,
            narration="WP13 categorize-draw",
        )
    except BeanCheckError as exc:
        raise HTTPException(
            status_code=500,
            detail=f"bean-check failed on draw override: {exc}",
        )
    reader.invalidate()
    return RedirectResponse(
        f"/settings/loans/{slug}?saved=draw-categorized", status_code=303,
    )


@router.post("/settings/loans/{slug}/escrow/reconcile")
async def loan_escrow_reconcile(
    slug: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """WP7 — post an escrow-reconciliation adjustment transaction.

    User enters the statement balance + date; the server compares
    against the ledger-walked balance at that date; if the delta
    exceeds tolerance, writes a zero-sum transaction tagged
    ``#lamella-loan-escrow-reconcile`` that moves the delta between
    the escrow asset and a user-picked offset account (defaults to
    ``Expenses:{Entity}:{Slug}:EscrowAdjustment``).
    """
    from lamella.features.loans.escrow import (
        build_reconciliation_block, escrow_flows, reconcile,
    )

    row = conn.execute(
        "SELECT * FROM loans WHERE slug = ?", (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="loan not found")
    loan = dict(row)
    if not loan.get("escrow_account_path"):
        raise HTTPException(
            status_code=400,
            detail="Loan has no escrow account configured.",
        )

    form = await request.form()
    statement_balance = _to_decimal(form.get("statement_balance"))
    statement_date_str = (form.get("statement_date") or "").strip()
    offset_account = (form.get("offset_account") or "").strip()
    statement_date = _to_date_safe(statement_date_str)

    if statement_balance is None or not statement_date:
        raise HTTPException(
            status_code=400,
            detail="statement_balance and statement_date are required.",
        )

    default_offset = (
        f"Expenses:{loan.get('entity_slug')}:{slug}:EscrowAdjustment"
        if loan.get("entity_slug") else "Expenses:EscrowAdjustment"
    )
    offset_account = offset_account or default_offset

    entries = reader.load().entries
    flows = escrow_flows(loan, entries)
    result = reconcile(
        flows, statement_balance, statement_date,
        default_offset_path=default_offset,
    )
    if not result.needs_adjustment:
        return RedirectResponse(
            f"/settings/loans/{slug}?saved=escrow-in-sync", status_code=303,
        )

    # Scaffold the offset account Open if needed.
    opener = AccountsWriter(
        main_bean=settings.ledger_main,
        connector_accounts=settings.connector_accounts_path,
    )
    try:
        _ensure_open_on_or_before(
            reader, opener, offset_account, statement_date,
            connector_accounts_path=settings.connector_accounts_path,
            comment_tag=f"Escrow reconciliation for {slug}",
        )
    except BeanCheckError as exc:
        raise HTTPException(status_code=500, detail=f"bean-check failed: {exc}")
    reader.invalidate()

    block = build_reconciliation_block(
        loan,
        statement_date=statement_date,
        delta=result.delta,
        offset_account=offset_account,
    )
    from lamella.features.rules.overrides import ensure_overrides_exists
    from lamella.core.ledger_writer import (
        capture_bean_check,
        ensure_include_in_main,
        run_bean_check_vs_baseline,
    )
    overrides_path = settings.connector_overrides_path
    main_bean = settings.ledger_main
    backup_main = main_bean.read_bytes() if main_bean.exists() else b""
    backup_ov = overrides_path.read_bytes() if overrides_path.exists() else None
    _, baseline = capture_bean_check(main_bean)
    ensure_overrides_exists(overrides_path)
    ensure_include_in_main(main_bean, overrides_path)
    with overrides_path.open("a", encoding="utf-8") as fh:
        fh.write(block)
    try:
        run_bean_check_vs_baseline(main_bean, baseline)
    except BeanCheckError as exc:
        main_bean.write_bytes(backup_main)
        if backup_ov is None:
            overrides_path.unlink(missing_ok=True)
        else:
            overrides_path.write_bytes(backup_ov)
        raise HTTPException(status_code=500, detail=f"bean-check failed: {exc}")

    reader.invalidate()
    return RedirectResponse(
        f"/settings/loans/{slug}?saved=escrow-reconciled", status_code=303,
    )


@router.post("/settings/loans/{slug}/groups/{group_id}/confirm")
async def loan_group_confirm(
    slug: str,
    group_id: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """WP5 — confirm a proposed multi-leg payment group.

    The proposer surfaces groups as candidates; this endpoint commits
    the chosen one. The form posts:
      * ``primary_hash`` — which member carries the real split
      * ``split_<account_path>`` — amount for each split leg of the
        aggregate, in dollars (positive)

    Stale-click guard: the proposer re-runs from current ledger state.
    If the group_id no longer appears (because a member was already
    classified since the user saw the proposal), we return a no-op
    303 with ``?saved=group-stale`` rather than writing a bad block.
    """
    from lamella.features.loans.groups import (
        MemberPosting,
        apply_group,
        ensure_in_flight_account,
        from_transactions,
        propose_groups,
    )

    row = conn.execute(
        "SELECT * FROM loans WHERE slug = ?", (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="loan not found")
    loan = dict(row)

    monthly = _to_decimal(loan.get("monthly_payment_estimate"))
    if monthly is None or monthly <= 0:
        raise HTTPException(
            status_code=400,
            detail="Loan has no monthly_payment_estimate; cannot group.",
        )

    # Already-confirmed group_ids come from the ledger via the
    # reader — keep them excluded from the proposer so the live
    # proposal state reflects what's still open.
    already_confirmed = {
        r["group_id"] for r in conn.execute(
            "SELECT group_id FROM loan_payment_groups "
            "WHERE loan_slug = ? AND status = 'confirmed'",
            (slug,),
        ).fetchall()
    }

    entries = reader.load().entries
    legs = from_transactions(
        entries,
        fixme_account_prefix="Expenses:FIXME",
        liability_path=loan.get("liability_account_path"),
    )
    # Exclude members already in a confirmed group.
    grouped_hashes: set[str] = set()
    for r in conn.execute(
        "SELECT member_hashes FROM loan_payment_groups "
        "WHERE loan_slug = ? AND status = 'confirmed'",
        (slug,),
    ).fetchall():
        grouped_hashes.update(
            h.strip() for h in (r["member_hashes"] or "").split(",") if h.strip()
        )

    report = propose_groups(
        loan, legs, monthly,
        already_grouped_hashes=grouped_hashes,
    )
    proposal = next(
        (g for g in report.groups if g.group_id == group_id), None,
    )
    if proposal is None or group_id in already_confirmed:
        return RedirectResponse(
            f"/settings/loans/{slug}?saved=group-stale", status_code=303,
        )

    form = await request.form()
    primary_hash = (form.get("primary_hash") or "").strip()
    if primary_hash not in proposal.member_hashes:
        raise HTTPException(
            status_code=400,
            detail="primary_hash must be one of the group's members.",
        )

    # Harvest split_<account>=amount fields into a list.
    primary_splits: list[tuple[str, Decimal]] = []
    for key in form.keys():
        if not key.startswith("split_"):
            continue
        acct = key[len("split_"):]
        amt = _to_decimal(form.get(key))
        if not acct or amt is None or amt == 0:
            continue
        primary_splits.append((acct, amt))
    if not primary_splits:
        raise HTTPException(
            status_code=400,
            detail="at least one split_<account> field is required.",
        )

    split_total = sum((amt for _, amt in primary_splits), Decimal("0"))
    if abs(split_total - proposal.aggregate_amount) > Decimal("0.02"):
        raise HTTPException(
            status_code=400,
            detail=(
                f"split total {split_total} does not match group "
                f"aggregate {proposal.aggregate_amount}."
            ),
        )

    # Build MemberPosting list from live ledger txns so we know each
    # member's from_account and date without trusting the form.
    members: list[MemberPosting] = []
    fixme_by_hash = {l.txn_hash: l for l in legs if l.txn_hash in proposal.member_hashes}
    # Walk entries once to capture the from_account per member.
    from_accounts: dict[str, str] = {}
    for entry in entries:
        h = txn_hash(entry)
        if h not in proposal.member_hashes:
            continue
        for p in entry.postings or []:
            acct = getattr(p, "account", None) or ""
            if acct.startswith("Expenses:FIXME"):
                from_accounts[h] = acct
                break
    for h in proposal.member_hashes:
        leg = fixme_by_hash.get(h)
        acct = from_accounts.get(h)
        if leg is None or acct is None:
            raise HTTPException(
                status_code=409,
                detail=f"member {h[:10]} no longer on an Expenses:FIXME leg.",
            )
        members.append(MemberPosting(
            txn_hash=h, txn_date=leg.date,
            amount=leg.amount, from_account=acct,
        ))

    # Ensure the in-flight staging account is open before writing.
    earliest = min(m.txn_date for m in members)
    try:
        in_flight_path = ensure_in_flight_account(
            loan, entries,
            main_bean=settings.ledger_main,
            connector_accounts=settings.connector_accounts_path,
            earliest_member_date=earliest,
        )
    except BeanCheckError as exc:
        raise HTTPException(
            status_code=500,
            detail=f"bean-check blocked in-flight Open: {exc}",
        )
    reader.invalidate()

    # Ensure every split account is open by the primary txn date.
    opener = AccountsWriter(
        main_bean=settings.ledger_main,
        connector_accounts=settings.connector_accounts_path,
    )
    primary_leg = next(m for m in members if m.txn_hash == primary_hash)
    for acct, _ in primary_splits:
        try:
            _ensure_open_on_or_before(
                reader, opener, acct, primary_leg.txn_date,
                connector_accounts_path=settings.connector_accounts_path,
                comment_tag=f"Loan group confirm for {slug}",
            )
        except BeanCheckError as exc:
            raise HTTPException(
                status_code=500,
                detail=f"bean-check blocked Open for {acct}: {exc}",
            )
    reader.invalidate()

    writer = OverrideWriter(
        main_bean=settings.ledger_main,
        overrides=settings.connector_overrides_path,
        conn=conn,
    )
    try:
        result = apply_group(
            loan,
            group_id=group_id,
            members=members,
            primary_hash=primary_hash,
            primary_splits=primary_splits,
            in_flight_path=in_flight_path,
            writer=writer,
        )
    except BeanCheckError as exc:
        raise HTTPException(
            status_code=409,
            detail=f"bean-check blocked group write: {exc}",
        )
    reader.invalidate()

    # Upsert the cache row as confirmed.
    conn.execute(
        """
        INSERT INTO loan_payment_groups
            (group_id, loan_slug, member_hashes, aggregate_amount,
             date_span_start, date_span_end, primary_hash, status,
             confirmed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'confirmed', CURRENT_TIMESTAMP)
        ON CONFLICT (group_id) DO UPDATE SET
            status       = 'confirmed',
            primary_hash = excluded.primary_hash,
            confirmed_at = CURRENT_TIMESTAMP
        """,
        (
            group_id, slug, ",".join(result.member_hashes),
            str(proposal.aggregate_amount),
            proposal.date_span_start.isoformat(),
            proposal.date_span_end.isoformat(),
            primary_hash,
        ),
    )

    return RedirectResponse(
        f"/settings/loans/{slug}?saved=group-confirmed", status_code=303,
    )


@router.get("/settings/loans/{slug}/projection.json")
def loan_projection_json(
    slug: str,
    extra: str = "0",
    lump: str = "0",
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """WP9 — prepayment / payoff projection as JSON.

    Called by the detail-page projection panel's sliders. Pure
    read; no writes. Returns baseline-vs-scenario summary plus
    monthly balance points for the chart.

    Starting balance resolution matches the detail page's anchored-
    balance path: walk-forward from most-recent anchor ≤ today when
    any anchor exists, else use the amortization model's remaining
    balance at the current payment number.
    """
    from fastapi.responses import JSONResponse

    from lamella.features.loans.coverage import extract_actuals
    from lamella.features.loans.projection import (
        project, resolve_starting_balance,
    )

    row = conn.execute(
        "SELECT * FROM loans WHERE slug = ?", (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="loan not found")
    loan = dict(row)

    apr = _to_decimal(loan.get("interest_rate_apr")) or Decimal("0")
    monthly = _to_decimal(loan.get("monthly_payment_estimate"))
    term = int(loan.get("term_months") or 0)
    principal = _to_decimal(loan.get("original_principal")) or Decimal("0")

    # Fall back to computing monthly_payment from terms when the
    # user didn't stamp one explicitly.
    if not monthly and principal > 0 and term > 0:
        from lamella.features.loans.amortization import monthly_payment as _mpay
        monthly = _mpay(principal, apr, term)
    if not monthly or monthly <= 0:
        raise HTTPException(
            status_code=400,
            detail="Loan is missing monthly_payment_estimate (and term to derive one).",
        )

    extra_d = _to_decimal(extra) or Decimal("0")
    lump_d = _to_decimal(lump) or Decimal("0")
    if extra_d < 0 or lump_d < 0:
        raise HTTPException(status_code=400, detail="extras must be non-negative")

    anchors = [
        dict(r) for r in conn.execute(
            "SELECT as_of_date, balance FROM loan_balance_anchors "
            "WHERE loan_slug = ? ORDER BY as_of_date DESC",
            (slug,),
        ).fetchall()
    ]
    entries = reader.load().entries
    actuals = extract_actuals(loan, entries)

    starting = resolve_starting_balance(loan, anchors, actuals)
    result = project(
        starting, apr, monthly,
        extra_monthly=extra_d, lump_sum=lump_d,
    )

    return JSONResponse({
        "starting_balance": str(result.starting_balance),
        "monthly_payment": str(result.monthly_payment),
        "apr": str(result.apr),
        "baseline_payoff_date": (
            result.baseline_payoff_date.isoformat()
            if result.baseline_payoff_date else None
        ),
        "baseline_total_interest": str(result.baseline_total_interest),
        "baseline_months": result.baseline_months,
        "scenario_payoff_date": (
            result.scenario_payoff_date.isoformat()
            if result.scenario_payoff_date else None
        ),
        "scenario_total_interest": str(result.scenario_total_interest),
        "scenario_months": result.scenario_months,
        "months_saved": result.months_saved,
        "interest_saved": str(result.interest_saved),
        "points": [
            [d.isoformat(), str(b)] for d, b in result.monthly_points
        ],
    })


@router.post("/settings/loans/{slug}/record-missing-payment")
async def record_missing_payment(
    slug: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """WP3 — record a payment that SHOULD have happened but isn't in
    the ledger (SimpleFIN missed it, or user paid manually offline).

    Unlike /record-payment (which splits an existing FIXME), this
    synthesizes the whole transaction from the amortization-model
    split plus a user-picked offset account. Writes to
    connector_overrides.bean tagged ``#lamella-loan-backfill`` with
    ``lamella-loan-slug`` + ``lamella-loan-expected-n`` metadata so
    reconstruct can rebuild the row and WP8 anomaly detection can
    tell which payments were user-synthesized vs observed.
    """
    row = conn.execute(
        "SELECT * FROM loans WHERE slug = ?", (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="loan not found")
    loan = dict(row)

    liability_path = loan.get("liability_account_path")
    if not liability_path:
        raise HTTPException(
            status_code=400,
            detail="Set the liability account path on the loan first.",
        )
    interest_path = loan.get("interest_account_path")

    form = await request.form()
    expected_date = (form.get("expected_date") or "").strip()
    expected_n = (form.get("expected_n") or "").strip()
    principal = _to_decimal(form.get("principal")) or Decimal("0")
    interest = _to_decimal(form.get("interest")) or Decimal("0")
    escrow = _to_decimal(form.get("escrow")) or Decimal("0")
    tax = _to_decimal(form.get("tax")) or Decimal("0")
    insurance = _to_decimal(form.get("insurance")) or Decimal("0")
    late_fee = _to_decimal(form.get("late_fee")) or Decimal("0")
    offset_account = (form.get("offset_account") or "").strip()
    if not expected_date or not offset_account:
        raise HTTPException(
            status_code=400,
            detail="expected_date and offset_account are required.",
        )
    if principal <= 0 and interest <= 0:
        raise HTTPException(
            status_code=400,
            detail="At least one of principal or interest must be > 0.",
        )
    total = principal + interest + escrow + tax + insurance + late_fee
    if interest > 0 and not interest_path:
        raise HTTPException(
            status_code=400,
            detail="Interest provided but no interest_account_path on loan.",
        )

    # Ensure all referenced accounts are open on or before the
    # expected date. _ensure_open_on_or_before routes through the
    # extracted scaffolding helper, rewriting our own Open directive
    # dates where needed and refusing (with a useful message) when a
    # user-authored file would need editing.
    expected_date_dt = _to_date_safe(expected_date)
    opener = AccountsWriter(
        main_bean=settings.ledger_main,
        connector_accounts=settings.connector_accounts_path,
    )
    leg_accounts: list[str] = [liability_path, offset_account]
    if interest > 0 and interest_path:
        leg_accounts.append(interest_path)
    if escrow > 0:
        escrow_path = loan.get("escrow_account_path")
        if not escrow_path:
            raise HTTPException(
                status_code=400,
                detail="Escrow provided but no escrow_account_path on loan.",
            )
        leg_accounts.append(escrow_path)
    # Tax / insurance / late-fee paths are derived from the loan's
    # entity_slug + slug by write_synthesized_payment. Pre-open
    # them here so the synthesizer's per-leg writes don't fail
    # bean-check on accounts that hadn't been Open'd yet at the
    # expected_date.
    entity_for_paths = loan.get("entity_slug") or ""
    if tax > 0 and entity_for_paths and slug:
        leg_accounts.append(f"Expenses:{entity_for_paths}:{slug}:PropertyTax")
    if insurance > 0 and entity_for_paths and slug:
        leg_accounts.append(f"Expenses:{entity_for_paths}:{slug}:Insurance")
    if late_fee > 0 and entity_for_paths and slug:
        leg_accounts.append(f"Expenses:{entity_for_paths}:{slug}:LateFees")
    try:
        for path in leg_accounts:
            _ensure_open_on_or_before(
                reader, opener, path, expected_date_dt,
                connector_accounts_path=settings.connector_accounts_path,
                comment_tag=f"Missing payment for {slug} on {expected_date}",
            )
    except BeanCheckError as exc:
        raise HTTPException(status_code=500, detail=f"bean-check failed opening account: {exc}")
    reader.invalidate()

    expected_n_int: int | None = int(expected_n) if expected_n else None
    try:
        write_synthesized_payment(
            loan=loan,
            settings=settings,
            txn_date=expected_date_dt,
            expected_n=expected_n_int,
            principal=principal,
            interest=interest,
            escrow=escrow,
            tax=tax,
            insurance=insurance,
            late_fee=late_fee,
            offset_account=offset_account,
            narration=(
                f"Mortgage payment (recorded for expected #{expected_n})"
                if expected_n else "Mortgage payment (recorded)"
            ),
        )
    except BeanCheckError as exc:
        raise HTTPException(status_code=500, detail=f"bean-check failed: {exc}")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    reader.invalidate()
    return RedirectResponse(
        f"/settings/loans/{slug}?saved=missing-recorded", status_code=303,
    )


@router.post("/settings/loans/{slug}/record-payment")
async def record_mortgage_payment(
    slug: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Split a FIXME transaction into principal / interest / escrow /
    (optionally) property tax + insurance legs. Writes one connector-
    override that zeroes out the FIXME amount and lays it down across
    the loan's accounts."""
    from beancount.core.data import Transaction

    loan_row = conn.execute(
        "SELECT * FROM loans WHERE slug = ?", (slug,)
    ).fetchone()
    if loan_row is None:
        raise HTTPException(status_code=404, detail="loan not found")
    loan = dict(loan_row)

    liability_path = loan.get("liability_account_path")
    if not liability_path:
        raise HTTPException(
            status_code=400,
            detail="Set the liability account path on the loan before recording payments.",
        )

    form = await request.form()
    target_hash = (form.get("txn_hash") or "").strip()
    if not target_hash:
        raise HTTPException(status_code=400, detail="txn_hash required")

    # Fetch the transaction from the ledger.
    txn: Transaction | None = None
    for entry in reader.load().entries:
        if isinstance(entry, Transaction) and txn_hash(entry) == target_hash:
            txn = entry
            break
    if txn is None:
        raise HTTPException(status_code=404, detail="transaction not in ledger")

    # FIXME leg: where we're taking money off. Must exist.
    fixme_account: str | None = None
    fixme_amount: Decimal | None = None
    currency = "USD"
    for p in txn.postings:
        acct = p.account or ""
        if acct.endswith(":FIXME") or acct.split(":")[-1].upper() == "FIXME":
            fixme_account = acct
            if p.units and p.units.number is not None:
                fixme_amount = abs(Decimal(p.units.number))
                currency = p.units.currency or "USD"
    if fixme_account is None or fixme_amount is None:
        raise HTTPException(
            status_code=400,
            detail="Transaction has no FIXME posting; nothing to split.",
        )

    # Collect split amounts from the form.
    principal = _to_decimal(form.get("principal")) or Decimal("0")
    interest = _to_decimal(form.get("interest")) or Decimal("0")
    escrow = _to_decimal(form.get("escrow")) or Decimal("0")
    extra_principal = _to_decimal(form.get("extra_principal")) or Decimal("0")
    property_tax = _to_decimal(form.get("property_tax")) or Decimal("0")
    insurance = _to_decimal(form.get("insurance")) or Decimal("0")
    principal_total = principal + extra_principal

    splits: list[tuple[str, Decimal]] = []
    if principal_total > 0:
        splits.append((liability_path, principal_total))
    if interest > 0:
        interest_path = loan.get("interest_account_path") or (
            f"Expenses:{loan['entity_slug']}:{slug}:Interest"
            if loan.get("entity_slug") else None
        )
        if not interest_path:
            raise HTTPException(
                status_code=400,
                detail="Interest amount provided but no interest_account_path on loan.",
            )
        splits.append((interest_path, interest))
    if escrow > 0:
        escrow_path = loan.get("escrow_account_path") or _default_escrow_path(
            loan.get("entity_slug"), loan.get("institution"), slug,
        )
        if not escrow_path:
            raise HTTPException(
                status_code=400,
                detail="Escrow amount provided but no escrow_account_path on loan.",
            )
        splits.append((escrow_path, escrow))
    if property_tax > 0 and loan.get("entity_slug"):
        splits.append((
            f"Expenses:{loan['entity_slug']}:{slug}:PropertyTax",
            property_tax,
        ))
    if insurance > 0 and loan.get("entity_slug"):
        splits.append((
            f"Expenses:{loan['entity_slug']}:{slug}:Insurance",
            insurance,
        ))
    # WP12: optional late-fee leg routed to LateFees expense account.
    # Path auto-derives from entity + slug; scaffolding's Open guard
    # below will create the Open directive on first use.
    late_fee = _to_decimal(form.get("late_fee")) or Decimal("0")
    if late_fee > 0 and loan.get("entity_slug"):
        splits.append((
            f"Expenses:{loan['entity_slug']}:{slug}:LateFees",
            late_fee,
        ))

    # Free-form "other" rows (repeated pairs: other_account + other_amount).
    other_accts = [str(v).strip() for (k, v) in form.multi_items() if k == "other_account"]
    other_amts = [_to_decimal(v) for (k, v) in form.multi_items() if k == "other_amount"]
    for acct, amt in zip(other_accts, other_amts):
        if acct and amt and amt > 0:
            splits.append((acct, amt))

    if not splits:
        raise HTTPException(status_code=400, detail="No split amounts provided.")

    total = sum((a for _, a in splits), Decimal("0"))
    diff = abs(total - fixme_amount)
    if diff > Decimal("0.02"):
        raise HTTPException(
            status_code=400,
            detail=(
                f"Split total {total:.2f} does not match transaction amount "
                f"{fixme_amount:.2f} (diff {diff:.2f}). Adjust so the splits sum."
            ),
        )

    # Each split posts into an account that may have been auto-scaffolded
    # at `date.today()`. If this txn is older than the Open date, bean-
    # check would reject the write. Roll back the Open date to the txn
    # date where needed (only touches our own connector_accounts.bean).
    txn_date_dt = _to_date_safe(txn.date) or date.today()
    opener = AccountsWriter(
        main_bean=settings.ledger_main,
        connector_accounts=settings.connector_accounts_path,
    )
    try:
        for acct, _amt in splits:
            _ensure_open_on_or_before(
                reader, opener, acct, txn_date_dt,
                connector_accounts_path=settings.connector_accounts_path,
                comment_tag=f"Mortgage payment split for {slug}",
            )
    except BeanCheckError as exc:
        raise HTTPException(status_code=500, detail=f"bean-check failed opening account: {exc}")
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        log.exception("mortgage split: failed to open split accounts")
        raise HTTPException(
            status_code=500,
            detail=f"Could not open split account: {type(exc).__name__}: {exc}",
        )
    reader.invalidate()

    writer = OverrideWriter(
        main_bean=settings.ledger_main,
        overrides=settings.connector_overrides_path,
        conn=conn,
    )
    narration = txn.narration or f"Mortgage payment ({slug})"

    # In-place first: rewrite the FIXME txn's whole posting block to the
    # categorized split. Cleaner than stacking an override on top —
    # the FIXME line literally goes away, and re-submits don't
    # accumulate. The override-overlay path is the fallback when
    # in-place can't proceed (no source leg, txn lives outside
    # ledger_dir, an existing override on this hash blocks the
    # strip, bean-check rejects the rewrite, …).
    in_place_done = False
    txn_meta = getattr(txn, "meta", None) or {}
    src_file = txn_meta.get("filename")
    src_lineno = txn_meta.get("lineno")

    # Find the source-of-funds leg (non-FIXME) on the original txn.
    source_leg_acct: str | None = None
    source_leg_amount: Decimal | None = None
    for p in txn.postings or []:
        acct = getattr(p, "account", None) or ""
        if not acct or acct == fixme_account:
            continue
        if acct.split(":")[-1].upper() == "FIXME":
            continue
        units = getattr(p, "units", None)
        if units is None or getattr(units, "number", None) is None:
            continue
        source_leg_acct = acct
        source_leg_amount = Decimal(str(units.number))
        break

    if (
        src_file and src_lineno is not None
        and source_leg_acct and source_leg_amount is not None
    ):
        try:
            from pathlib import Path as _P
            from lamella.core.rewrite.txn_inplace import (
                InPlaceRewriteError,
                rewrite_txn_postings,
            )
            try:
                writer.rewrite_without_hash(target_hash)
            except BeanCheckError:
                raise InPlaceRewriteError("override-strip blocked")

            # Source leg keeps its original signed amount; splits
            # add up to the magnitude on the FIXME side. By
            # construction, source_leg_amount + sum(splits) = 0
            # because the original txn balanced and splits sum to
            # fixme_amount = -source_leg_amount.
            new_postings: list[tuple[str, Decimal, str]] = [
                (source_leg_acct, source_leg_amount, currency),
            ]
            for acct, amt in splits:
                new_postings.append((acct, amt, currency))

            rewrite_txn_postings(
                source_file=_P(src_file),
                txn_start_line=int(src_lineno),
                new_postings=new_postings,
                ledger_dir=settings.ledger_dir,
                main_bean=settings.ledger_main,
            )
            in_place_done = True
        except InPlaceRewriteError as exc:
            log.info(
                "mortgage split: in-place refused for %s: %s "
                "— falling back to override",
                target_hash[:12], exc,
            )

    if not in_place_done:
        try:
            writer.append_split(
                txn_date=txn_date_dt,
                txn_hash=target_hash,
                from_account=fixme_account,
                splits=splits,
                currency=currency,
                narration=narration,
            )
        except BeanCheckError as exc:
            log.error("mortgage split override rejected: %s", exc)
            raise HTTPException(status_code=500, detail=f"bean-check failed: {exc}")
        except HTTPException:
            raise
        except Exception as exc:  # noqa: BLE001
            # Any other write-path failure (WriteError, OSError, etc.) —
            # surface a useful message instead of an empty 500.
            log.exception("mortgage split override failed unexpectedly")
            raise HTTPException(
                status_code=500,
                detail=f"Could not write mortgage split: {type(exc).__name__}: {exc}",
            )

    reader.invalidate()
    return RedirectResponse(f"/settings/loans/{slug}?saved=payment", status_code=303)


@router.post("/settings/loans/{slug}/anchors")
async def add_balance_anchor(
    slug: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    """Record a statement-observed balance for this loan on a given
    date. Used to pin reality when amortization model drifts from the
    servicer's number (bonus principal payments, refi, pre-ledger
    history, etc.)."""
    row = conn.execute("SELECT slug FROM loans WHERE slug = ?", (slug,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="loan not found")
    form = await request.form()
    as_of_date = (form.get("as_of_date") or "").strip()
    balance = (form.get("balance") or "").strip()
    if not as_of_date or not balance:
        raise HTTPException(status_code=400, detail="as_of_date and balance are required")
    source = (form.get("source") or "").strip() or None
    notes = (form.get("notes") or "").strip() or None
    try:
        conn.execute(
            """
            INSERT INTO loan_balance_anchors (loan_slug, as_of_date, balance, source, notes)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (loan_slug, as_of_date) DO UPDATE SET
                balance = excluded.balance,
                source  = excluded.source,
                notes   = excluded.notes
            """,
            (slug, as_of_date, balance, source, notes),
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    try:
        append_loan_balance_anchor(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            loan_slug=slug, as_of_date=as_of_date,
            balance=balance, source=source, notes=notes,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("loan-balance-anchor directive write failed for %s: %s", slug, exc)
    return RedirectResponse(f"/settings/loans/{slug}?saved=anchor", status_code=303)


@router.post("/settings/loans/{slug}/anchors/{anchor_id}/delete")
def delete_balance_anchor(
    slug: str,
    anchor_id: int,
    conn = Depends(get_db),
):
    conn.execute(
        "DELETE FROM loan_balance_anchors WHERE id = ? AND loan_slug = ?",
        (anchor_id, slug),
    )
    return RedirectResponse(f"/settings/loans/{slug}?saved=anchor-removed", status_code=303)


# ----------------------------------------------------------- WP12 pauses


@router.post("/settings/loans/{slug}/pauses")
async def add_pause(
    slug: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Record a forbearance / payment-pause window for this loan.

    The coverage engine reads ``loan_pauses`` (rebuilt from the ledger
    via read_loan_pauses) and skips expected-row generation for any
    month falling inside an open or closed pause window. Without this,
    the WP3 graceful-degradation collapse-to-attention path fires on
    legitimate forbearance gaps.
    """
    from lamella.features.loans.pauses import PauseError, create_pause

    row = conn.execute(
        "SELECT slug FROM loans WHERE slug = ?", (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="loan not found")

    form = await request.form()
    start_date = _to_date_safe((form.get("start_date") or "").strip())
    end_date = _to_date_safe((form.get("end_date") or "").strip())
    reason = (form.get("reason") or "").strip() or None
    notes = (form.get("notes") or "").strip() or None
    accrued_str = (form.get("accrued_interest") or "").strip()
    accrued = _to_decimal(accrued_str) if accrued_str else None

    if start_date is None:
        raise HTTPException(
            status_code=400, detail="start_date is required.",
        )

    try:
        create_pause(
            conn,
            settings=settings,
            loan_slug=slug,
            start_date=start_date,
            end_date=end_date,
            reason=reason,
            notes=notes,
            accrued_interest=accrued,
        )
    except PauseError as exc:
        raise HTTPException(status_code=exc.status, detail=str(exc))

    reader.invalidate()
    return RedirectResponse(
        f"/settings/loans/{slug}?saved=pause-added", status_code=303,
    )


@router.post("/settings/loans/{slug}/pauses/{pause_id}/end")
async def end_pause_route(
    slug: str,
    pause_id: int,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Set an end_date on an open (still-active) pause."""
    from lamella.features.loans.pauses import PauseError, end_pause

    form = await request.form()
    end_date = _to_date_safe((form.get("end_date") or "").strip())
    if end_date is None:
        raise HTTPException(
            status_code=400, detail="end_date is required.",
        )
    try:
        end_pause(
            conn, settings=settings, pause_id=pause_id, end_date=end_date,
        )
    except PauseError as exc:
        raise HTTPException(status_code=exc.status, detail=str(exc))
    reader.invalidate()
    return RedirectResponse(
        f"/settings/loans/{slug}?saved=pause-ended", status_code=303,
    )


@router.post("/settings/loans/{slug}/pauses/{pause_id}/delete")
def delete_pause_route(
    slug: str,
    pause_id: int,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Tombstone the pause directive and remove the SQLite cache row."""
    from lamella.features.loans.pauses import PauseError, delete_pause

    try:
        delete_pause(conn, settings=settings, pause_id=pause_id)
    except PauseError as exc:
        raise HTTPException(status_code=exc.status, detail=str(exc))
    reader.invalidate()
    return RedirectResponse(
        f"/settings/loans/{slug}?saved=pause-deleted", status_code=303,
    )


@router.get("/settings/loans/{slug}", response_class=HTMLResponse)
def loan_detail(
    slug: str,
    request: Request,
    saved: str | None = None,
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    settings: Settings = Depends(get_settings),
):
    from beancount.core.data import Transaction

    row = conn.execute("SELECT * FROM loans WHERE slug = ?", (slug,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="loan not found")
    loan = dict(row)

    principal = _to_decimal(loan["original_principal"]) or Decimal("0")
    apr = _to_decimal(loan["interest_rate_apr"]) or Decimal("0")
    term_months = int(loan["term_months"] or 0)
    escrow = _to_decimal(loan["escrow_monthly"])
    schedule = []
    current_split = None
    est_pmt = None
    total_principal_paid = Decimal("0")
    total_interest_paid = Decimal("0")
    remaining = principal
    if principal and term_months:
        schedule = amortization_schedule(principal, apr, term_months)
        est_pmt = monthly_payment(principal, apr, term_months)
        first = loan.get("first_payment_date")
        if first:
            try:
                first_dt = datetime.fromisoformat(first).date() if isinstance(first, str) else first
                n = payment_number_on(first_dt, date.today(), term_months)
                current_split = split_for_payment_number(
                    principal, apr, term_months, n, escrow_monthly=escrow,
                )
                for row2 in schedule[:n - 1]:
                    total_principal_paid += row2.principal
                    total_interest_paid += row2.interest
                remaining = principal - total_principal_paid
            except Exception:  # noqa: BLE001
                pass

    entities = list_entities(conn, include_inactive=False)
    properties = [
        dict(r) for r in conn.execute(
            "SELECT slug, display_name FROM properties "
            "WHERE is_active = 1 OR slug = ? "
            "ORDER BY display_name, slug",
            (loan.get("property_slug") or "",),
        ).fetchall()
    ]

    # Pull payment history: every transaction that touches the liability,
    # interest, or escrow account for this loan. Each shows date, total
    # debit to the loan (liability-side positive = principal paid), the
    # interest portion, escrow portion, and other legs (property tax,
    # insurance, from-account).
    liability_path = loan.get("liability_account_path")
    interest_path = loan.get("interest_account_path")
    escrow_path = loan.get("escrow_account_path")

    # If the loan was created before this page stored account paths,
    # suggest plausible ones from the convention so the user just clicks
    # save rather than typing three paths from scratch.
    from lamella.core.registry.service import suggest_slug as _sug
    entity = loan.get("entity_slug") or ""
    inst = loan.get("institution") or ""
    inst_slug = _sug(inst) or (inst.replace(" ", "") if inst else "")
    suggested_liability = loan.get("liability_account_path")
    if not suggested_liability and entity and inst_slug:
        suggested_liability = f"Liabilities:{entity}:{inst_slug}:{slug}"
    suggested_interest = loan.get("interest_account_path") or (
        f"Expenses:{entity}:{slug}:Interest" if entity else ""
    )
    suggested_escrow = loan.get("escrow_account_path") or (
        _default_escrow_path(entity, inst, slug) if (entity and loan.get("escrow_monthly")) else ""
    ) or ""

    payments: list[dict] = []
    escrow_flows: list[dict] = []
    actual_principal_paid = Decimal("0")
    actual_interest_paid = Decimal("0")
    actual_escrow_paid = Decimal("0")

    if liability_path or interest_path or escrow_path:
        tracked = {p for p in (liability_path, interest_path, escrow_path) if p}

        # Build a hash index so override txns can follow their
        # lamella-override-of meta back to the original to retrieve the
        # true source-of-funds (Checking / CreditCard). Without this
        # the payment row shows "Uncategorized" because the override
        # block's own legs are FIXME + targets, not the source.
        all_entries = list(reader.load().entries)
        by_hash: dict[str, Transaction] = {}
        for e in all_entries:
            if isinstance(e, Transaction):
                by_hash[txn_hash(e)] = e

        def _find_source_leg(txn: Transaction) -> tuple[str | None, Decimal | None]:
            """Return (account, amount) of the Assets / Liabilities:Credit
            leg that's not a FIXME — the real source-of-funds side."""
            for p in txn.postings:
                acct = p.account or ""
                if not acct:
                    continue
                if acct.split(":")[-1].upper() == "FIXME":
                    continue
                if p.units is None or p.units.number is None:
                    continue
                if acct.startswith("Assets:") or acct.startswith("Liabilities:Credit"):
                    return acct, Decimal(p.units.number)
            return None, None

        for entry in all_entries:
            if not isinstance(entry, Transaction):
                continue
            if not any(
                (p.account in tracked) for p in entry.postings if p.account
            ):
                continue
            # Is this entry an override for a prior FIXME txn? If so,
            # its source-of-funds lives on the original entry.
            meta = getattr(entry, "meta", None) or {}
            override_of = meta.get("lamella-override-of") or meta.get("override-of")
            source_txn: Transaction | None = None
            if override_of and override_of in by_hash:
                source_txn = by_hash[override_of]

            legs: list[dict] = []
            principal_leg = Decimal("0")
            interest_leg = Decimal("0")
            escrow_leg = Decimal("0")
            from_leg: str | None = None
            from_amt: Decimal | None = None
            for p in entry.postings:
                if not p.account or p.units is None or p.units.number is None:
                    continue
                acct = p.account
                # Skip the FIXME cancellation leg from display — it's an
                # accounting mechanism, not user-facing info. The leg is
                # still present on the underlying txn; hiding it from the
                # "All legs" expander means the list shows the actual
                # effect (source → targets) instead of (FIXME → targets).
                is_fixme_leg = acct.split(":")[-1].upper() == "FIXME"
                amt = Decimal(p.units.number)
                if not is_fixme_leg:
                    legs.append({
                        "account": acct,
                        "amount": amt,
                        "currency": p.units.currency or "USD",
                    })
                if acct == liability_path:
                    principal_leg += amt
                elif acct == interest_path:
                    interest_leg += amt
                elif acct == escrow_path:
                    escrow_leg += amt
                elif (
                    not is_fixme_leg
                    and (acct.startswith("Assets:") or acct.startswith("Liabilities:Credit"))
                ):
                    if from_leg is None:
                        from_leg = acct
                        from_amt = amt

            # Override txns carry no direct source leg — follow the
            # override-of pointer back to the original and prepend its
            # non-FIXME source as the authoritative "from" side.
            if from_leg is None and source_txn is not None:
                src_acct, src_amt = _find_source_leg(source_txn)
                if src_acct is not None:
                    from_leg = src_acct
                    from_amt = src_amt
                    legs.insert(0, {
                        "account": src_acct,
                        "amount": src_amt,
                        "currency": "USD",
                    })

            actual_principal_paid += principal_leg
            actual_interest_paid += interest_leg
            actual_escrow_paid += escrow_leg
            payments.append({
                "date": entry.date,
                "narration": entry.narration or "",
                "payee": getattr(entry, "payee", None),
                "principal": principal_leg,
                "interest": interest_leg,
                "escrow": escrow_leg,
                "from_account": from_leg,
                "from_amount": from_amt,
                "source_txn_hash": override_of if override_of else txn_hash(entry),
                # Real issue flag: after override-join, still no source →
                # something is genuinely miscategorized.
                "unresolved_source": (from_leg is None),
                "legs": legs,
            })

        # Escrow disbursements: transactions that touch escrow but NOT
        # the liability (property tax / insurance payouts from escrow).
        if escrow_path:
            for entry in reader.load().entries:
                if not isinstance(entry, Transaction):
                    continue
                touches_escrow = any(
                    (p.account == escrow_path) for p in entry.postings if p.account
                )
                touches_liability = any(
                    (p.account == liability_path) for p in entry.postings if p.account
                ) if liability_path else False
                if not touches_escrow or touches_liability:
                    continue
                for p in entry.postings:
                    if not p.account or p.units is None or p.units.number is None:
                        continue
                    if p.account == escrow_path:
                        continue
                    escrow_flows.append({
                        "date": entry.date,
                        "narration": entry.narration or "",
                        "account": p.account,
                        "amount": Decimal(p.units.number),
                    })

    payments.sort(key=lambda r: r["date"], reverse=True)
    escrow_flows.sort(key=lambda r: r["date"], reverse=True)

    # Balance anchors: pin reality to a specific date when the textbook
    # amortization model drifts (bonus payments, refi) or when the loan
    # predates the ledger.
    anchor_rows = conn.execute(
        "SELECT id, as_of_date, balance, source, notes FROM loan_balance_anchors "
        "WHERE loan_slug = ? ORDER BY as_of_date DESC",
        (slug,),
    ).fetchall()
    anchors = [dict(r) for r in anchor_rows]

    # Anchored current balance: take the most recent anchor with
    # as_of_date <= today, then subtract principal paid AFTER that date
    # per the ledger. Gives a ledger-true remaining balance that
    # respects bonus/extra principal payments.
    anchored_balance: Decimal | None = None
    anchor_used: dict | None = None
    principal_paid_since_anchor = Decimal("0")
    today = date.today()
    past_anchors = [
        a for a in anchors
        if a["as_of_date"] and _to_date_safe(a["as_of_date"]) and
        _to_date_safe(a["as_of_date"]) <= today
    ]
    if past_anchors:
        # Rows already DESC by as_of_date.
        anchor_used = past_anchors[0]
        anchor_date = _to_date_safe(anchor_used["as_of_date"])
        anchor_balance = _to_decimal(anchor_used["balance"]) or Decimal("0")
        if liability_path and anchor_date:
            for p in payments:
                if p["date"] > anchor_date:
                    principal_paid_since_anchor += p["principal"]
        # Paydown posts positive to the liability (balance goes from
        # -182k toward 0). So "remaining" = anchor_balance - principal_paid_since_anchor.
        anchored_balance = anchor_balance - principal_paid_since_anchor

    # Schedule-vs-reality delta for the expand-your-schedule UI.
    model_vs_actual = None
    if anchored_balance is not None and remaining is not None:
        model_vs_actual = remaining - anchored_balance  # >0 = ahead of schedule

    # Outstanding FIXME transactions that look like payments against
    # this loan. Narration/payee contains institution, slug, display
    # name, or the amount matches the expected monthly payment ±5%.
    from beancount.core.data import Transaction

    # Tokenize institution / slug / display name into discriminating
    # words. A single 4-char substring like "sunflower" in a narration
    # is only a real match if the rest of the transaction lines up;
    # tokenizing avoids matching random words that share a prefix.
    _STOP = {"bank", "the", "llc", "inc", "corp", "co", "company", "and", "of", "credit",
             "union", "loan", "loans", "mortgage", "mtg", "auto", "payments",
             "payment", "services", "service"}
    tokens: set[str] = set()
    for key in ("institution", "slug", "display_name"):
        v = loan.get(key)
        if not v:
            continue
        for tok in re.findall(r"[A-Za-z0-9]{4,}", str(v).lower()):
            if tok not in _STOP:
                tokens.add(tok)

    est_monthly = _to_decimal(loan.get("monthly_payment_estimate")) or est_pmt
    # Candidate filter: narration-token alone was flagging $20
    # transactions as mortgage payments when the only surviving token
    # was a generic word like the institution's name. When we know the
    # expected monthly payment, require the amount to be within a
    # reasonably wide band so extra-principal and escrow-heavy months
    # still match. Without an estimate, we require 2+ distinct token
    # hits instead.
    amount_tolerance = Decimal("0.30")  # ±30% envelope around est_monthly
    linked_hashes_for_loan: set[str] = set()
    # Already-categorized (override applied). Hash is stamped in the
    # override's `lamella-override-of:` line (or legacy `override-of:`) — we
    # treat a FIXME with a matching override as resolved.
    try:
        overrides_text = (
            settings.connector_overrides_path.read_text(encoding="utf-8")
            if settings.connector_overrides_path.exists() else ""
        )
        linked_hashes_for_loan = set(
            re.findall(r'(?:lamella-)?override-of: "([^"]+)"', overrides_text)
        )
    except Exception:  # noqa: BLE001
        linked_hashes_for_loan = set()

    fixme_candidates: list[dict] = []
    seen_candidate_hashes: set[str] = set()
    if tokens:  # without a narration fingerprint we can't be confident
        for entry in reader.load().entries:
            if not isinstance(entry, Transaction):
                continue
            h = txn_hash(entry)
            if h in linked_hashes_for_loan:
                continue
            # Dedup by txn_hash. A real duplicate in the ledger (e.g.
            # SimpleFIN double-fetch surfaced two days apart) would
            # otherwise render the same FIXME form twice on this
            # page, confusing the user about which one to split.
            if h in seen_candidate_hashes:
                continue
            has_fixme = False
            fixme_amt: Decimal | None = None
            for p in entry.postings:
                acct = p.account or ""
                if acct.split(":")[-1].upper() == "FIXME":
                    has_fixme = True
                    if p.units and p.units.number is not None:
                        fixme_amt = abs(Decimal(p.units.number))
                        break
            if not has_fixme or fixme_amt is None:
                continue
            hay = " ".join(
                filter(None, [entry.payee or "", entry.narration or ""])
            ).lower()
            token_hits = sum(1 for t in tokens if t in hay)
            narration_match = token_hits > 0
            if not narration_match:
                continue
            amount_match = False
            if est_monthly and est_monthly > 0:
                band = est_monthly * amount_tolerance
                if abs(fixme_amt - est_monthly) <= max(band, Decimal("20.00")):
                    amount_match = True
            # Narration alone is too noisy (one generic token hit can
            # flag a $20 grocery run as a mortgage payment). Require
            # either an amount-band match OR multiple token hits.
            if est_monthly and est_monthly > 0:
                if not amount_match:
                    continue
            else:
                if token_hits < 2:
                    continue
            # Pre-computed amortization split for this exact month —
            # helps the user confirm principal/interest defaults.
            model_split = None
            if principal and term_months and loan.get("first_payment_date"):
                try:
                    first_dt = _to_date_safe(loan["first_payment_date"])
                    if first_dt:
                        n_payment = payment_number_on(
                            first_dt, entry.date if isinstance(entry.date, date)
                            else date.fromisoformat(str(entry.date)),
                            term_months,
                        )
                        model_split = split_for_payment_number(
                            principal, apr, term_months,
                            max(1, n_payment),
                            escrow_monthly=escrow,
                        )
                except Exception:  # noqa: BLE001
                    model_split = None
            fixme_candidates.append({
                "txn_hash": h,
                "date": entry.date,
                "amount": fixme_amt,
                "narration": entry.narration or "",
                "payee": getattr(entry, "payee", None),
                "model_split": model_split,
                "narration_match": narration_match,
                "amount_match": amount_match,
            })
            seen_candidate_hashes.add(h)
    fixme_candidates.sort(key=lambda c: c["date"], reverse=True)

    # Offset-account suggestions for the initial-funding form.
    # Ranked by likelihood given what we know about the loan.
    offset_suggestions: list[dict] = []
    entity_slug_for_opts = loan.get("entity_slug") or ""
    if loan.get("property_slug"):
        prop_row = conn.execute(
            "SELECT slug, display_name, asset_account_path, purchase_date "
            "FROM properties WHERE slug = ?",
            (loan["property_slug"],),
        ).fetchone()
        if prop_row:
            prop = dict(prop_row)
            p_slug = prop["slug"]
            p_entity = (
                prop.get("entity_slug") or entity_slug_for_opts or ""
            )
            # Prefer the property's own asset_account_path (set when
            # scaffolded correctly); else only fall back to an
            # entity-scoped path — the old entity-less
            # `Assets:Property:{slug}` shape is deprecated and gets
            # skipped here so we never suggest it.
            asset_path = prop.get("asset_account_path")
            if not asset_path and p_entity:
                asset_path = f"Assets:{p_entity}:Property:{p_slug}"
            if asset_path:
                offset_suggestions.append({
                    "path": asset_path,
                    "label": f"Asset account for {prop.get('display_name') or p_slug}",
                    "why": "New-purchase mortgage — proceeds went straight to the house as cost basis.",
                })
    if entity_slug_for_opts:
        offset_suggestions.append({
            "path": f"Equity:{entity_slug_for_opts}:OpeningBalances",
            "label": f"Equity:{entity_slug_for_opts}:OpeningBalances",
            "why": "Historical / pre-ledger loan — use when your ledger starts AFTER the loan was funded.",
        })
    offset_suggestions.append({
        "path": "Equity:Opening-Balances",
        "label": "Equity:Opening-Balances",
        "why": "Default opening-balance bucket if you don't track per-entity equity.",
    })
    # Checking account guess — pick an Assets:*Checking* path tied to
    # the loan's entity if one exists in the ledger.
    for entry in reader.load().entries:
        acct = getattr(entry, "account", None) or ""
        if (acct.startswith(f"Assets:{entity_slug_for_opts}:") and "Checking" in acct) \
                or (not entity_slug_for_opts and acct.startswith("Assets:") and "Checking" in acct):
            offset_suggestions.append({
                "path": acct,
                "label": acct,
                "why": "Cash-out refi or proceeds-to-checking.",
            })
            break

    # Initial funding detection: a mortgage (or any loan) starts life
    # with a posting that CREDITS the liability account by the original
    # principal. Without that posting, the liability has a zero opening
    # balance and every amortization calc is stranded. Walk the ledger
    # for any transaction whose liability-leg amount is within 0.5% of
    # original_principal.
    original_principal = _to_decimal(loan.get("original_principal"))
    initial_funding_found = False
    initial_funding_txn: dict | None = None
    if liability_path and original_principal:
        tolerance = max(original_principal * Decimal("0.005"), Decimal("100"))
        for entry in reader.load().entries:
            if not isinstance(entry, Transaction):
                continue
            for p in entry.postings:
                if p.account != liability_path or p.units is None or p.units.number is None:
                    continue
                amt = Decimal(p.units.number)
                if abs(abs(amt) - original_principal) <= tolerance:
                    initial_funding_found = True
                    initial_funding_txn = {
                        "date": entry.date,
                        "narration": entry.narration or "",
                        "amount": abs(amt),
                    }
                    break
            if initial_funding_found:
                break

    # Which configured account paths are NOT yet in the ledger? Offer
    # the user a one-click "open these" heal button.
    ledger_accounts: set[str] = set()
    for entry in reader.load().entries:
        acct = getattr(entry, "account", None)
        if isinstance(acct, str):
            ledger_accounts.add(acct)
    wanted_for_open = [
        p for p in (
            loan.get("liability_account_path"),
            loan.get("interest_account_path"),
            loan.get("escrow_account_path"),
        ) if p
    ]
    missing_accounts = [p for p in wanted_for_open if p not in ledger_accounts]

    # WP7 — escrow dashboard data. Only compute when escrow is
    # configured; the adaptive layout hides the panel otherwise.
    escrow_all_flows: list = []
    escrow_running: list = []
    escrow_ytd_obj = None
    if loan.get("escrow_account_path"):
        from lamella.features.loans.escrow import (
            escrow_flows as _escrow_flows,
            running_balance as _running_balance,
            ytd_summary as _ytd_summary,
        )
        escrow_all_flows = _escrow_flows(loan, reader.load().entries)
        escrow_running = _running_balance(escrow_all_flows)
        escrow_ytd_obj = _ytd_summary(escrow_all_flows, date.today().year)

    # Adaptive layout is the page now. The classic template was
    # the rollout cover during WP1-WP13; once CSS landed for the
    # adaptive class names (summary-grid, panel, panel-stack,
    # loan-panel, chip*, severity-chip, coverage-row,
    # escrow-sparkline, anomaly-list, group-*, projection-*) the
    # flag and the classic template were retired together.
    show_all = request.query_params.get("show_all") == "1"

    # Health model drives the adaptive layout's next-actions + panel
    # visibility. Safe to compute even for the classic template —
    # the classic template ignores these context keys.
    from lamella.features.loans.health import assess as _loan_assess
    from lamella.features.loans.layout import panels_for
    health = _loan_assess(loan, reader.load().entries, conn, settings)

    # Live proposer for the groups panel — populates the "proposed"
    # list with whatever groups can still form from current
    # FIXME-candidate set. health.payment_groups["confirmed"] is
    # already populated from the SQLite cache by health.assess.
    try:
        from lamella.features.loans.groups import (
            from_transactions, propose_groups,
        )
        monthly = _to_decimal(loan.get("monthly_payment_estimate"))
        if monthly is not None and monthly > 0:
            legs = from_transactions(
                reader.load().entries,
                fixme_account_prefix="Expenses:FIXME",
                liability_path=loan.get("liability_account_path"),
            )
            grouped_hashes: set[str] = set()
            for g in health.payment_groups.get("confirmed", []):
                grouped_hashes.update(
                    h.strip() for h in (g.get("member_hashes") or "").split(",")
                    if h.strip()
                )
            report = propose_groups(
                loan, legs, monthly,
                already_grouped_hashes=grouped_hashes,
            )
            health.payment_groups["proposed"] = report.groups
    except Exception as exc:  # noqa: BLE001 — panel is nice-to-have
        log.warning("loans detail: group proposer skipped for %s: %s",
                    loan.get("slug"), exc)

    panels = panels_for(loan, health, show_all=show_all)

    # WP13 — recent uncategorized draws on revolving loans, available
    # headroom (limit minus current balance). Only computed when
    # is_revolving=True; the panel itself is hidden otherwise via
    # layout.py's _is_panel_relevant.
    revolving_draws: list = []
    revolving_available: Decimal | None = None
    if loan.get("is_revolving"):
        from lamella.features.loans.revolving import recent_draws
        revolving_draws = recent_draws(
            loan=loan, entries=reader.load().entries, limit=10,
        )
        if loan.get("credit_limit") and anchored_balance is not None:
            try:
                revolving_available = (
                    Decimal(str(loan["credit_limit"])) - anchored_balance
                )
            except Exception:  # noqa: BLE001
                revolving_available = None

    ctx = {
        "loan": loan,
        "loan_types": LOAN_TYPES,
        "entities": entities,
        "properties": properties,
        "schedule": schedule,
        "est_pmt": est_pmt,
        "current_split": current_split,
        "total_principal_paid": total_principal_paid,
        "total_interest_paid": total_interest_paid,
        "remaining": remaining,
        "payments": payments,
        "escrow_flows": escrow_flows,
        "actual_principal_paid": actual_principal_paid,
        "actual_interest_paid": actual_interest_paid,
        "actual_escrow_paid": actual_escrow_paid,
        "suggested_liability_path": suggested_liability or "",
        "suggested_interest_path": suggested_interest or "",
        "suggested_escrow_path": suggested_escrow or "",
        "anchors": anchors,
        "anchor_used": anchor_used,
        "anchored_balance": anchored_balance,
        "principal_paid_since_anchor": principal_paid_since_anchor,
        "model_vs_actual": model_vs_actual,
        "fixme_candidates": fixme_candidates,
        "missing_accounts": missing_accounts,
        "initial_funding_found": initial_funding_found,
        "initial_funding_txn": initial_funding_txn,
        "offset_suggestions": offset_suggestions,
        "saved": saved,
        # WP2 additions — classic template ignores these.
        "health": health,
        "panels": panels,
        "show_all": show_all,
        # WP7 additions.
        "escrow_all_flows": escrow_all_flows,
        "escrow_running": escrow_running,
        "escrow_ytd": escrow_ytd_obj,
        # WP13 additions — only populated for revolving loans.
        "revolving_draws": revolving_draws,
        "revolving_available": revolving_available,
    }
    return request.app.state.templates.TemplateResponse(
        request, "settings_loan_detail_adaptive.html", ctx
    )

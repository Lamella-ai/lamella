# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""WP10 — Purchase wizard flow.

"I bought a property and got a mortgage on it." Linear in the
common case; one branch when the user is buying a property already
in their registry (e.g., a refi-replacing-a-cash-purchase scenario).

Steps:
  1. choose_property        — pick existing property OR "create new"
  2. new_property_details   — only when choice == 'new'
  3. loan_terms             — institution, principal, APR, term
  4. accounts               — liability/interest/escrow paths
  5. funding                — funded date, offset account, narration
  6. preview                — (handled by dispatcher; not a flow step)
  7. commit                 — (handled by dispatcher)

The flow's commit produces this write plan:
  - PlannedPropertyWrite       (only when property_choice == 'new')
  - PlannedAccountsOpen        (the loan's tracked paths)
  - PlannedLoanWrite           (the custom "loan" directive)
  - PlannedLoanFunding         (the #lamella-loan-funding transaction)

All four execute under one WizardCommitTxn — atomic.
"""
from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from lamella.features.loans.wizard._base import (
    FlowResult,
    PlannedAccountsOpen,
    PlannedLoanFunding,
    PlannedLoanWrite,
    PlannedPropertyWrite,
    PlannedWrite,
    ValidationError,
    WizardCommitTxn,
    WizardStep,
    err,
)

log = logging.getLogger(__name__)


_LOAN_TYPES = ("mortgage", "auto", "student", "personal", "heloc", "other")
_PROPERTY_TYPES = (
    "primary_residence", "rental", "investment", "land", "other",
)


def _to_decimal(s: str | None) -> Decimal | None:
    if s is None or s == "":
        return None
    raw = str(s).strip().replace(",", "").replace("$", "")
    if not raw:
        return None
    try:
        return Decimal(raw)
    except (InvalidOperation, ValueError):
        return None


def _to_date(s: str | None) -> date | None:
    if s is None or s == "":
        return None
    try:
        return date.fromisoformat(str(s)[:10])
    except (TypeError, ValueError):
        return None


def _to_int(s: str | None) -> int | None:
    if s is None or s == "":
        return None
    try:
        return int(str(s).strip())
    except (TypeError, ValueError):
        return None


def _is_valid_slug(s: str) -> bool:
    """Slug rules: alphanumeric + underscores, starts with a letter,
    3-40 chars. Matches the existing registry slug convention."""
    if not s or len(s) < 3 or len(s) > 40:
        return False
    if not s[0].isalpha():
        return False
    return all(c.isalnum() or c == "_" for c in s)


# --------------------------------------------------------------------- flow


class PurchaseFlow:
    name = "purchase"
    title = "Purchase a property"

    def steps(self) -> dict[str, WizardStep]:
        return {
            "choose_property": WizardStep(
                name="choose_property",
                template="loans_wizard_purchase_step_choose_property.html",
                title="Choose a property",
            ),
            "new_property_details": WizardStep(
                name="new_property_details",
                template="loans_wizard_purchase_step_new_property.html",
                title="New property details",
            ),
            "loan_terms": WizardStep(
                name="loan_terms",
                template="loans_wizard_purchase_step_loan_terms.html",
                title="Loan terms",
            ),
            "accounts": WizardStep(
                name="accounts",
                template="loans_wizard_purchase_step_accounts.html",
                title="Account paths",
            ),
            "funding": WizardStep(
                name="funding",
                template="loans_wizard_purchase_step_funding.html",
                title="Funding",
            ),
        }

    def initial_step(self) -> str:
        return "choose_property"

    # -------------------------------------------------------- validate

    def validate(
        self, step_name: str, params: dict, conn: Any,
    ) -> list[ValidationError]:
        """Defensive: every value here was round-tripped through hidden
        inputs. View Source can edit them. Re-coerce + re-check.

        validate() examines ALL accumulated params, not just
        step_name's, because a back-edit on step 1 should surface as
        an error on step 1 even if the user is currently on step 4.
        """
        errs: list[ValidationError] = []

        # Step 1: choose_property
        choice = (params.get("property_choice") or "").strip()
        if step_name in (
            "choose_property", "new_property_details", "loan_terms",
            "accounts", "funding",
        ):
            if choice not in ("new", "existing"):
                errs.append(err(
                    "Pick whether the property is new or existing.",
                    field="property_choice",
                ))
            elif choice == "existing":
                existing_slug = (params.get("existing_property_slug") or "").strip()
                if not existing_slug:
                    errs.append(err(
                        "Select an existing property.",
                        field="existing_property_slug",
                    ))
                elif conn is not None:
                    row = conn.execute(
                        "SELECT slug FROM properties WHERE slug = ?",
                        (existing_slug,),
                    ).fetchone()
                    if row is None:
                        errs.append(err(
                            f"Property '{existing_slug}' is not in the "
                            f"registry. Pick another, or create a new one.",
                            field="existing_property_slug",
                        ))

        # Step 2: new_property_details (only when choice == 'new')
        if choice == "new" and step_name in (
            "new_property_details", "loan_terms", "accounts", "funding",
        ):
            slug = (params.get("new_property_slug") or "").strip()
            if not _is_valid_slug(slug):
                errs.append(err(
                    "Property slug: 3–40 chars, starts with a letter, "
                    "alphanumeric + underscores only.",
                    field="new_property_slug",
                ))
            elif conn is not None:
                row = conn.execute(
                    "SELECT slug FROM properties WHERE slug = ?", (slug,),
                ).fetchone()
                if row is not None:
                    errs.append(err(
                        f"Property '{slug}' already exists. Pick a "
                        f"different slug or use 'existing'.",
                        field="new_property_slug",
                    ))
            display_name = (params.get("new_property_display_name") or "").strip()
            if not display_name:
                errs.append(err(
                    "Display name is required.",
                    field="new_property_display_name",
                ))
            ptype = (params.get("new_property_type") or "").strip()
            if ptype not in _PROPERTY_TYPES:
                errs.append(err(
                    f"Property type must be one of: "
                    f"{', '.join(_PROPERTY_TYPES)}.",
                    field="new_property_type",
                ))

        # Step 3: loan_terms
        if step_name in ("loan_terms", "accounts", "funding"):
            slug = (params.get("loan_slug") or "").strip()
            if not _is_valid_slug(slug):
                errs.append(err(
                    "Loan slug: 3–40 chars, starts with a letter, "
                    "alphanumeric + underscores only.",
                    field="loan_slug",
                ))
            elif conn is not None:
                row = conn.execute(
                    "SELECT slug FROM loans WHERE slug = ?", (slug,),
                ).fetchone()
                if row is not None:
                    errs.append(err(
                        f"Loan '{slug}' already exists.",
                        field="loan_slug",
                    ))
            ltype = (params.get("loan_type") or "").strip()
            if ltype not in _LOAN_TYPES:
                errs.append(err(
                    f"Loan type must be one of: {', '.join(_LOAN_TYPES)}.",
                    field="loan_type",
                ))
            principal = _to_decimal(params.get("original_principal"))
            if principal is None or principal <= 0:
                errs.append(err(
                    "Principal must be a positive number.",
                    field="original_principal",
                ))
            term = _to_int(params.get("term_months"))
            if term is None or term <= 0 or term > 600:
                errs.append(err(
                    "Term must be 1–600 months.",
                    field="term_months",
                ))
            apr = _to_decimal(params.get("interest_rate_apr"))
            if apr is None or apr < 0 or apr > 50:
                errs.append(err(
                    "APR must be 0–50%.",
                    field="interest_rate_apr",
                ))

        # Step 4: accounts
        if step_name in ("accounts", "funding"):
            liability = (params.get("liability_account_path") or "").strip()
            if not liability or not liability.startswith("Liabilities:"):
                errs.append(err(
                    "Liability account path must start with 'Liabilities:'.",
                    field="liability_account_path",
                ))

        # Step 5: funding
        if step_name == "funding":
            funded = _to_date(params.get("funded_date"))
            if funded is None:
                errs.append(err(
                    "Funded date is required.", field="funded_date",
                ))
            offset = (params.get("offset_account") or "").strip()
            if not offset:
                errs.append(err(
                    "Offset account is required.",
                    field="offset_account",
                ))

        return errs

    # -------------------------------------------------------- next_step

    def next_step(
        self, current_step: str, params: dict, conn: Any,
    ) -> str | None:
        """Branching: choose_property → new_property_details (if 'new')
        OR straight to loan_terms (if 'existing'). All other transitions
        are linear."""
        choice = (params.get("property_choice") or "").strip()
        if current_step == "choose_property":
            if choice == "new":
                return "new_property_details"
            return "loan_terms"
        if current_step == "new_property_details":
            return "loan_terms"
        if current_step == "loan_terms":
            return "accounts"
        if current_step == "accounts":
            return "funding"
        if current_step == "funding":
            return None  # ready to preview
        return None

    # -------------------------------------------------------- write_plan

    def write_plan(
        self, params: dict, conn: Any,
    ) -> list[PlannedWrite]:
        plan: list[PlannedWrite] = []

        choice = (params.get("property_choice") or "").strip()
        loan_slug = (params.get("loan_slug") or "").strip()
        loan_type = (params.get("loan_type") or "").strip()
        principal = _to_decimal(params.get("original_principal")) or Decimal("0")
        funded_date = _to_date(params.get("funded_date"))
        funded_iso = funded_date.isoformat() if funded_date else ""

        # Property write (only when 'new').
        property_slug: str | None = None
        if choice == "new":
            property_slug = (params.get("new_property_slug") or "").strip()
            ptype = (params.get("new_property_type") or "").strip()
            plan.append(PlannedPropertyWrite(
                slug=property_slug,
                display_name=(params.get("new_property_display_name") or "").strip(),
                property_type=ptype,
                entity_slug=(params.get("new_property_entity_slug") or "").strip()
                            or None,
                address=(params.get("new_property_address") or "").strip() or None,
                purchase_date=funded_iso or None,
                purchase_price=str(principal) if principal > 0 else None,
                is_primary_residence=(ptype == "primary_residence"),
                is_rental=(ptype == "rental"),
            ))
        else:
            property_slug = (params.get("existing_property_slug") or "").strip()

        # Account opens. Only the paths the user actually filled in
        # land in the open list — bean-check rejects opening accounts
        # we never post to.
        liability = (params.get("liability_account_path") or "").strip()
        interest = (params.get("interest_account_path") or "").strip()
        escrow = (params.get("escrow_account_path") or "").strip()
        offset = (params.get("offset_account") or "").strip()
        paths: list[str] = []
        for p in (liability, interest, escrow, offset):
            if p and p not in paths:
                paths.append(p)
        if paths:
            plan.append(PlannedAccountsOpen(
                paths=tuple(paths),
                opened_on=funded_iso,
                comment=f"Purchase wizard scaffold for {loan_slug}",
            ))

        # Loan directive write.
        plan.append(PlannedLoanWrite(
            slug=loan_slug,
            display_name=(params.get("loan_display_name") or loan_slug).strip(),
            loan_type=loan_type,
            entity_slug=(params.get("loan_entity_slug") or "").strip() or None,
            institution=(params.get("loan_institution") or "").strip() or None,
            original_principal=str(principal),
            funded_date=funded_iso,
            first_payment_date=(params.get("first_payment_date") or "").strip()
                               or None,
            payment_due_day=_to_int(params.get("payment_due_day")),
            term_months=_to_int(params.get("term_months")),
            interest_rate_apr=str(_to_decimal(params.get("interest_rate_apr"))
                                  or Decimal("0")),
            monthly_payment_estimate=(
                params.get("monthly_payment_estimate") or "").strip() or None,
            escrow_monthly=(params.get("escrow_monthly") or "").strip() or None,
            property_tax_monthly=(
                params.get("property_tax_monthly") or "").strip() or None,
            insurance_monthly=(
                params.get("insurance_monthly") or "").strip() or None,
            liability_account_path=liability or None,
            interest_account_path=interest or None,
            escrow_account_path=escrow or None,
            property_slug=property_slug or None,
        ))

        # Funding transaction.
        plan.append(PlannedLoanFunding(
            slug=loan_slug,
            display_name=(params.get("loan_display_name") or loan_slug).strip(),
            funded_date=funded_iso,
            principal=str(principal),
            offset_account=offset,
            liability_account_path=liability,
            narration=(params.get("funding_narration") or "").strip() or None,
        ))

        return plan

    # ------------------------------------------------- template_context

    def template_context(
        self, step_name: str, params: dict, conn: Any,
    ) -> dict:
        """Surface registry data the templates need: list of existing
        property slugs for the choose_property datalist, entity slugs
        for entity pickers, etc."""
        ctx: dict = {}
        if step_name == "choose_property" and conn is not None:
            try:
                rows = conn.execute(
                    "SELECT slug, display_name FROM properties "
                    "WHERE COALESCE(is_active, 1) = 1 "
                    "ORDER BY slug"
                ).fetchall()
                ctx["existing_properties"] = [
                    {
                        "slug": r["slug"] if hasattr(r, "keys") else r[0],
                        "display_name": (
                            r["display_name"] if hasattr(r, "keys") else r[1]
                        ),
                    }
                    for r in rows
                ]
            except Exception as exc:  # noqa: BLE001
                log.warning("purchase wizard: properties lookup failed: %s", exc)
                ctx["existing_properties"] = []
        return ctx

    # -------------------------------------------------------- commit

    def commit(
        self, params: dict, settings: Any, conn: Any, reader: Any,
    ) -> FlowResult:
        plan = self.write_plan(params, conn)
        with WizardCommitTxn(settings, conn=conn):
            for planned in plan:
                planned.execute(settings=settings, conn=conn, reader=reader)

        slug = (params.get("loan_slug") or "").strip()
        return FlowResult(
            redirect_to=f"/settings/loans/{slug}",
            saved_message="purchase-wizard",
        )


# Register at import time so main.py's wizard-flow import wires
# this flow into the dispatcher's registry without a separate
# bookkeeping step.
def _register():
    from lamella.web.routes.loans_wizard import register_flow
    register_flow(PurchaseFlow())


_register()

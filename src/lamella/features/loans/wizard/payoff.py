# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""WP10 — Payoff wizard flow.

"I paid off this loan." Records the payoff balance as an anchor,
re-emits the loan directive with is_active=False + payoff_date +
payoff_amount stamped, and flips the SQLite is_active column to 0.

Steps:
  1. select_loan      — pick existing active loan
  2. payoff_details   — payoff_date, payoff_amount, source

Plan:
  - PlannedLoanBalanceAnchor (final anchor at payoff_amount,
    source='payoff' or user-chosen)
  - PlannedLoanWrite (re-emit with is_active=False + payoff_date +
    payoff_amount stamped on the directive)
  - PlannedSqliteRowUpdate (loans row: is_active=0 + payoff_date +
    payoff_amount; rolled back by SAVEPOINT if anything fails)

Structurally simpler than refi (3 writes vs 6, no new loan), but
the SQLite/ledger atomicity property is the same: if bean-check
rejects the directive write, the SQLite UPDATE on is_active must
also undo. Named test asserts both states roll back together.
"""
from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from lamella.features.loans.wizard._base import (
    FlowResult,
    PlannedLoanBalanceAnchor,
    PlannedLoanWrite,
    PlannedSqliteRowUpdate,
    PlannedWrite,
    ValidationError,
    WizardCommitTxn,
    WizardStep,
    err,
)

log = logging.getLogger(__name__)


_PAYOFF_SOURCES = (
    "cash", "refi", "lender_paid", "insurance_payout",
    "sale_proceeds", "other",
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


def _load_loan(conn: Any, slug: str) -> dict | None:
    if conn is None or not slug:
        return None
    try:
        row = conn.execute(
            "SELECT * FROM loans WHERE slug = ?", (slug,),
        ).fetchone()
    except Exception:  # noqa: BLE001
        return None
    return dict(row) if row else None


# --------------------------------------------------------------------- flow


class PayoffFlow:
    name = "payoff"
    title = "Record a payoff"

    def steps(self) -> dict[str, WizardStep]:
        return {
            "select_loan": WizardStep(
                name="select_loan",
                template="loans_wizard_payoff_step_select_loan.html",
                title="Pick the loan to mark paid off",
            ),
            "payoff_details": WizardStep(
                name="payoff_details",
                template="loans_wizard_payoff_step_details.html",
                title="Payoff details",
            ),
        }

    def initial_step(self) -> str:
        return "select_loan"

    # -------------------------------------------------------- validate

    def validate(
        self, step_name: str, params: dict, conn: Any,
    ) -> list[ValidationError]:
        errs: list[ValidationError] = []

        if step_name in ("select_loan", "payoff_details"):
            slug = (params.get("loan_slug") or "").strip()
            if not slug:
                errs.append(err(
                    "Pick which loan you're marking paid off.",
                    field="loan_slug",
                ))
            elif conn is not None:
                row = conn.execute(
                    "SELECT slug, is_active FROM loans WHERE slug = ?",
                    (slug,),
                ).fetchone()
                if row is None:
                    errs.append(err(
                        f"Loan '{slug}' is not in the registry.",
                        field="loan_slug",
                    ))
                elif not (row["is_active"] if hasattr(row, "keys") else row[1]):
                    errs.append(err(
                        f"Loan '{slug}' is already inactive.",
                        field="loan_slug",
                    ))

        if step_name == "payoff_details":
            payoff_date = _to_date(params.get("payoff_date"))
            if payoff_date is None:
                errs.append(err(
                    "Payoff date is required.", field="payoff_date",
                ))
            payoff_amount = _to_decimal(params.get("payoff_amount"))
            if payoff_amount is None or payoff_amount < 0:
                errs.append(err(
                    "Payoff amount must be a non-negative number "
                    "(use 0 if the loan was forgiven / written off).",
                    field="payoff_amount",
                ))
            source = (params.get("payoff_source") or "").strip()
            if source not in _PAYOFF_SOURCES:
                errs.append(err(
                    f"Pick a payoff source: {', '.join(_PAYOFF_SOURCES)}.",
                    field="payoff_source",
                ))

        return errs

    # -------------------------------------------------------- next_step

    def next_step(
        self, current_step: str, params: dict, conn: Any,
    ) -> str | None:
        if current_step == "select_loan":
            return "payoff_details"
        if current_step == "payoff_details":
            return None  # → preview
        return None

    # ------------------------------------------------- template_context

    def template_context(
        self, step_name: str, params: dict, conn: Any,
    ) -> dict:
        ctx: dict = {}
        if step_name == "select_loan" and conn is not None:
            try:
                rows = conn.execute(
                    "SELECT slug, display_name, loan_type, institution "
                    "FROM loans WHERE COALESCE(is_active, 1) = 1 "
                    "ORDER BY slug"
                ).fetchall()
                ctx["active_loans"] = [
                    dict(r) if hasattr(r, "keys") else r for r in rows
                ]
            except Exception as exc:  # noqa: BLE001
                log.warning("payoff wizard: active-loans lookup failed: %s", exc)
                ctx["active_loans"] = []
        return ctx

    # -------------------------------------------------------- write_plan

    def write_plan(
        self, params: dict, conn: Any,
    ) -> list[PlannedWrite]:
        plan: list[PlannedWrite] = []

        slug = (params.get("loan_slug") or "").strip()
        loan = _load_loan(conn, slug) or {}
        payoff_date = (params.get("payoff_date") or "").strip()
        payoff_amount = (
            _to_decimal(params.get("payoff_amount")) or Decimal("0")
        )
        payoff_source = (params.get("payoff_source") or "cash").strip()
        notes = (params.get("payoff_notes") or "").strip() or None

        # 1. Final balance anchor — pins the payoff balance on the
        # payoff date so coverage and amortization-drift detection
        # have a known endpoint.
        plan.append(PlannedLoanBalanceAnchor(
            loan_slug=slug,
            as_of_date=payoff_date,
            balance=str(payoff_amount),
            source=payoff_source,
            notes=notes,
        ))

        # 2. Re-emit the loan directive with is_active=False +
        # payoff metadata stamped. Reader's last-seen-wins
        # semantics make this the new state.
        plan.append(PlannedLoanWrite(
            slug=slug,
            display_name=loan.get("display_name") or slug,
            loan_type=loan.get("loan_type") or "other",
            entity_slug=loan.get("entity_slug"),
            institution=loan.get("institution"),
            original_principal=str(loan.get("original_principal") or "0"),
            funded_date=loan.get("funded_date") or payoff_date,
            first_payment_date=loan.get("first_payment_date"),
            payment_due_day=(
                int(loan["payment_due_day"])
                if loan.get("payment_due_day") else None
            ),
            term_months=(
                int(loan["term_months"])
                if loan.get("term_months") else None
            ),
            interest_rate_apr=loan.get("interest_rate_apr"),
            monthly_payment_estimate=loan.get("monthly_payment_estimate"),
            escrow_monthly=loan.get("escrow_monthly"),
            property_tax_monthly=loan.get("property_tax_monthly"),
            insurance_monthly=loan.get("insurance_monthly"),
            liability_account_path=loan.get("liability_account_path"),
            interest_account_path=loan.get("interest_account_path"),
            escrow_account_path=loan.get("escrow_account_path"),
            property_slug=loan.get("property_slug"),
            notes=(
                f"Paid off {payoff_date} via {payoff_source}. "
                + (loan.get("notes") or "")
            ).strip(),
        ))

        # 3. SQLite UPDATE — stamps is_active=0 + payoff metadata
        # on the live cache row so the UI reflects the payoff
        # immediately. SAVEPOINT discipline means this rolls back
        # alongside the ledger writes if anything fails.
        plan.append(PlannedSqliteRowUpdate(
            table="loans",
            set_columns=(
                ("is_active", 0),
                ("payoff_date", payoff_date),
                ("payoff_amount", str(payoff_amount)),
            ),
            where_clause="slug = ?",
            where_values=(slug,),
            summary_text=f"Mark loan '{slug}' inactive (SQLite cache)",
        ))

        return plan

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
            saved_message="loan-paid-off",
        )


def _register():
    from lamella.web.routes.loans_wizard import register_flow
    register_flow(PayoffFlow())


_register()

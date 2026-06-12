# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""WP10 — Refi wizard flow.

"I refinanced loan A as loan B." Closes out the old loan
(payoff_date + payoff_amount stamped, is_active flipped to 0,
cross-ref anchor written) and creates the new loan with its
funding transaction. All five writes execute under one
WizardCommitTxn — atomic.

Steps:
  1. select_old        — pick existing active loan to refinance
  2. payoff_terms      — payoff_date + payoff_amount + reason
  3. new_loan_terms    — institution/principal/APR/term for new loan
  4. accounts          — new loan's account paths
  5. funding           — funded date + offset (defaults to old
                         liability path so the new loan's funds
                         flow into the old loan's payoff)

Plan:
  - PlannedLoanWrite (OLD loan re-emit: is_active=False,
    payoff_date, payoff_amount) — the directive that supersedes
    the old loan's last directive on next reconstruct
  - PlannedSqliteRowUpdate (OLD loan SQLite row: is_active=0 +
    payoff_date + payoff_amount) — keeps the live cache in
    sync without waiting for reconstruct
  - PlannedLoanBalanceAnchor (OLD loan, payoff_date,
    payoff_amount, source='refi-payoff', notes='Refinanced
    as {NEW_SLUG}') — the cross-ref anchor that ties the two
    loans together
  - PlannedAccountsOpen (NEW loan's tracked paths)
  - PlannedLoanWrite (NEW loan)
  - PlannedLoanFunding (NEW loan's funding txn)

The cross-ref anchor is the load-bearing write: if close-out
succeeds and new-loan succeeds and the cross-ref-anchor fails,
the registry would have two loans that don't know about each
other — a worse state than no refi. WizardCommitTxn's atomic
rollback covers this; a named test asserts the rollback
discipline holds when the anchor write specifically fails.
"""
from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from lamella.features.loans.wizard._base import (
    FlowResult,
    PlannedAccountsOpen,
    PlannedLoanBalanceAnchor,
    PlannedLoanFunding,
    PlannedLoanWrite,
    PlannedSqliteRowUpdate,
    PlannedWrite,
    ValidationError,
    WizardCommitTxn,
    WizardStep,
    err,
)

log = logging.getLogger(__name__)


_LOAN_TYPES = ("mortgage", "auto", "student", "personal", "heloc", "other")
_PAYOFF_REASONS = (
    "rate_and_term", "cash_out", "switch_servicer", "consolidation", "other",
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
    if not s or len(s) < 3 or len(s) > 40:
        return False
    if not s[0].isalpha():
        return False
    return all(c.isalnum() or c == "_" for c in s)


def _load_old_loan(conn: Any, slug: str) -> dict | None:
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


class RefiFlow:
    name = "refi"
    title = "Refinance a loan"

    def steps(self) -> dict[str, WizardStep]:
        return {
            "select_old": WizardStep(
                name="select_old",
                template="loans_wizard_refi_step_select_old.html",
                title="Pick the loan to refinance",
            ),
            "payoff_terms": WizardStep(
                name="payoff_terms",
                template="loans_wizard_refi_step_payoff_terms.html",
                title="Payoff terms",
            ),
            "new_loan_terms": WizardStep(
                name="new_loan_terms",
                template="loans_wizard_refi_step_new_loan_terms.html",
                title="New loan terms",
            ),
            "accounts": WizardStep(
                name="accounts",
                template="loans_wizard_refi_step_accounts.html",
                title="New loan account paths",
            ),
            "funding": WizardStep(
                name="funding",
                template="loans_wizard_refi_step_funding.html",
                title="Funding",
            ),
        }

    def initial_step(self) -> str:
        return "select_old"

    # -------------------------------------------------------- validate

    def validate(
        self, step_name: str, params: dict, conn: Any,
    ) -> list[ValidationError]:
        errs: list[ValidationError] = []

        # Step 1: select_old required from this step forward.
        if step_name in (
            "select_old", "payoff_terms", "new_loan_terms",
            "accounts", "funding",
        ):
            old_slug = (params.get("old_loan_slug") or "").strip()
            if not old_slug:
                errs.append(err(
                    "Pick which loan you're refinancing.",
                    field="old_loan_slug",
                ))
            elif conn is not None:
                row = conn.execute(
                    "SELECT slug, is_active FROM loans WHERE slug = ?",
                    (old_slug,),
                ).fetchone()
                if row is None:
                    errs.append(err(
                        f"Loan '{old_slug}' is not in the registry.",
                        field="old_loan_slug",
                    ))
                elif not (row["is_active"] if hasattr(row, "keys") else row[1]):
                    errs.append(err(
                        f"Loan '{old_slug}' is already inactive — "
                        f"can't refinance a closed loan.",
                        field="old_loan_slug",
                    ))

        # Step 2: payoff_terms.
        if step_name in (
            "payoff_terms", "new_loan_terms", "accounts", "funding",
        ):
            payoff_date = _to_date(params.get("payoff_date"))
            if payoff_date is None:
                errs.append(err(
                    "Payoff date is required.", field="payoff_date",
                ))
            payoff_amount = _to_decimal(params.get("payoff_amount"))
            if payoff_amount is None or payoff_amount <= 0:
                errs.append(err(
                    "Payoff amount must be a positive number.",
                    field="payoff_amount",
                ))
            reason = (params.get("payoff_reason") or "").strip()
            if reason not in _PAYOFF_REASONS:
                errs.append(err(
                    f"Pick a payoff reason: {', '.join(_PAYOFF_REASONS)}.",
                    field="payoff_reason",
                ))

        # Step 3: new_loan_terms.
        if step_name in ("new_loan_terms", "accounts", "funding"):
            slug = (params.get("new_loan_slug") or "").strip()
            if not _is_valid_slug(slug):
                errs.append(err(
                    "New loan slug: 3–40 chars, starts with a letter, "
                    "alphanumeric + underscores only.",
                    field="new_loan_slug",
                ))
            elif conn is not None:
                row = conn.execute(
                    "SELECT slug FROM loans WHERE slug = ?", (slug,),
                ).fetchone()
                if row is not None:
                    errs.append(err(
                        f"Loan '{slug}' already exists.",
                        field="new_loan_slug",
                    ))
            old_slug = (params.get("old_loan_slug") or "").strip()
            if slug and slug == old_slug:
                errs.append(err(
                    "New loan slug can't match the old loan's slug.",
                    field="new_loan_slug",
                ))
            ltype = (params.get("new_loan_type") or "").strip()
            if ltype not in _LOAN_TYPES:
                errs.append(err(
                    f"Loan type must be one of: {', '.join(_LOAN_TYPES)}.",
                    field="new_loan_type",
                ))
            principal = _to_decimal(params.get("new_original_principal"))
            if principal is None or principal <= 0:
                errs.append(err(
                    "Original principal must be a positive number.",
                    field="new_original_principal",
                ))
            term = _to_int(params.get("new_term_months"))
            if term is None or term <= 0 or term > 600:
                errs.append(err(
                    "Term must be 1–600 months.",
                    field="new_term_months",
                ))
            apr = _to_decimal(params.get("new_interest_rate_apr"))
            if apr is None or apr < 0 or apr > 50:
                errs.append(err(
                    "APR must be 0–50%.",
                    field="new_interest_rate_apr",
                ))

        # Step 4: accounts.
        if step_name in ("accounts", "funding"):
            liability = (params.get("new_liability_account_path") or "").strip()
            if not liability or not liability.startswith("Liabilities:"):
                errs.append(err(
                    "Liability account path must start with 'Liabilities:'.",
                    field="new_liability_account_path",
                ))

        # Step 5: funding.
        if step_name == "funding":
            funded = _to_date(params.get("new_funded_date"))
            if funded is None:
                errs.append(err(
                    "Funded date is required.", field="new_funded_date",
                ))
            offset = (params.get("new_offset_account") or "").strip()
            if not offset:
                errs.append(err(
                    "Offset account is required (typically the old "
                    "loan's liability path).",
                    field="new_offset_account",
                ))

        return errs

    # -------------------------------------------------------- next_step

    def next_step(
        self, current_step: str, params: dict, conn: Any,
    ) -> str | None:
        if current_step == "select_old":
            return "payoff_terms"
        if current_step == "payoff_terms":
            return "new_loan_terms"
        if current_step == "new_loan_terms":
            return "accounts"
        if current_step == "accounts":
            return "funding"
        if current_step == "funding":
            return None  # → preview
        return None

    # ------------------------------------------------- template_context

    def template_context(
        self, step_name: str, params: dict, conn: Any,
    ) -> dict:
        ctx: dict = {}
        if step_name == "select_old" and conn is not None:
            try:
                rows = conn.execute(
                    "SELECT slug, display_name, loan_type, institution, "
                    "original_principal FROM loans "
                    "WHERE COALESCE(is_active, 1) = 1 ORDER BY slug"
                ).fetchall()
                ctx["active_loans"] = [
                    dict(r) if hasattr(r, "keys") else r for r in rows
                ]
            except Exception as exc:  # noqa: BLE001
                log.warning("refi wizard: active-loans lookup failed: %s", exc)
                ctx["active_loans"] = []
        if step_name in ("accounts", "funding"):
            # Surface the old loan's liability path so the funding
            # default can use it as the offset.
            old_loan = _load_old_loan(
                conn, (params.get("old_loan_slug") or "").strip(),
            )
            if old_loan:
                ctx["old_loan"] = old_loan
        return ctx

    # -------------------------------------------------------- write_plan

    def write_plan(
        self, params: dict, conn: Any,
    ) -> list[PlannedWrite]:
        plan: list[PlannedWrite] = []

        old_slug = (params.get("old_loan_slug") or "").strip()
        old_loan = _load_old_loan(conn, old_slug) or {}
        new_slug = (params.get("new_loan_slug") or "").strip()
        payoff_date = (params.get("payoff_date") or "").strip()
        payoff_amount = (
            _to_decimal(params.get("payoff_amount")) or Decimal("0")
        )
        payoff_reason = (params.get("payoff_reason") or "").strip()

        # 1. OLD loan close-out: re-emit its directive with
        # is_active=False, payoff_date, payoff_amount stamped.
        # All other fields preserved from the live SQLite row so
        # the directive supersedes correctly on next reconstruct.
        plan.append(PlannedLoanWrite(
            slug=old_slug,
            display_name=old_loan.get("display_name") or old_slug,
            loan_type=old_loan.get("loan_type") or "other",
            entity_slug=old_loan.get("entity_slug"),
            institution=old_loan.get("institution"),
            original_principal=str(old_loan.get("original_principal") or "0"),
            funded_date=old_loan.get("funded_date") or payoff_date,
            first_payment_date=old_loan.get("first_payment_date"),
            payment_due_day=(
                int(old_loan["payment_due_day"])
                if old_loan.get("payment_due_day") else None
            ),
            term_months=(
                int(old_loan["term_months"])
                if old_loan.get("term_months") else None
            ),
            interest_rate_apr=old_loan.get("interest_rate_apr"),
            monthly_payment_estimate=old_loan.get("monthly_payment_estimate"),
            escrow_monthly=old_loan.get("escrow_monthly"),
            property_tax_monthly=old_loan.get("property_tax_monthly"),
            insurance_monthly=old_loan.get("insurance_monthly"),
            liability_account_path=old_loan.get("liability_account_path"),
            interest_account_path=old_loan.get("interest_account_path"),
            escrow_account_path=old_loan.get("escrow_account_path"),
            property_slug=old_loan.get("property_slug"),
            notes=(
                f"Refinanced as {new_slug} on {payoff_date} "
                f"({payoff_reason}). "
                + (old_loan.get("notes") or "")
            ).strip(),
        ))

        # 2. SQLite UPDATE on the OLD loan row — keep cache in sync
        # without waiting for reconstruct. Rolled back by the
        # WizardCommitTxn SAVEPOINT if anything later fails.
        plan.append(PlannedSqliteRowUpdate(
            table="loans",
            set_columns=(
                ("is_active", 0),
                ("payoff_date", payoff_date),
                ("payoff_amount", str(payoff_amount)),
            ),
            where_clause="slug = ?",
            where_values=(old_slug,),
            summary_text=f"Mark old loan '{old_slug}' inactive (SQLite cache)",
        ))

        # 3. Cross-ref balance anchor on the OLD loan — the
        # load-bearing record that ties the two loans together.
        # If this fails, the rollback is what saves us.
        plan.append(PlannedLoanBalanceAnchor(
            loan_slug=old_slug,
            as_of_date=payoff_date,
            balance=str(payoff_amount),
            source="refi-payoff",
            notes=f"Refinanced as {new_slug} ({payoff_reason})",
        ))

        # 4. NEW loan's account opens.
        new_funded = (params.get("new_funded_date") or "").strip()
        liability = (params.get("new_liability_account_path") or "").strip()
        interest = (params.get("new_interest_account_path") or "").strip()
        escrow = (params.get("new_escrow_account_path") or "").strip()
        offset = (params.get("new_offset_account") or "").strip()
        paths: list[str] = []
        for p in (liability, interest, escrow, offset):
            if p and p not in paths:
                paths.append(p)
        if paths:
            plan.append(PlannedAccountsOpen(
                paths=tuple(paths),
                opened_on=new_funded,
                comment=f"Refi wizard scaffold for {new_slug}",
            ))

        # 5. NEW loan directive.
        new_principal = (
            _to_decimal(params.get("new_original_principal")) or Decimal("0")
        )
        plan.append(PlannedLoanWrite(
            slug=new_slug,
            display_name=(
                params.get("new_loan_display_name") or new_slug
            ).strip(),
            loan_type=(params.get("new_loan_type") or "").strip(),
            entity_slug=(params.get("new_loan_entity_slug") or "").strip()
                        or old_loan.get("entity_slug"),
            institution=(params.get("new_loan_institution") or "").strip()
                        or None,
            original_principal=str(new_principal),
            funded_date=new_funded,
            first_payment_date=(
                params.get("new_first_payment_date") or "").strip() or None,
            payment_due_day=_to_int(params.get("new_payment_due_day")),
            term_months=_to_int(params.get("new_term_months")),
            interest_rate_apr=str(
                _to_decimal(params.get("new_interest_rate_apr"))
                or Decimal("0")
            ),
            monthly_payment_estimate=(
                params.get("new_monthly_payment_estimate") or "").strip()
                or None,
            escrow_monthly=(params.get("new_escrow_monthly") or "").strip()
                           or None,
            property_tax_monthly=(
                params.get("new_property_tax_monthly") or "").strip() or None,
            insurance_monthly=(
                params.get("new_insurance_monthly") or "").strip() or None,
            liability_account_path=liability or None,
            interest_account_path=interest or None,
            escrow_account_path=escrow or None,
            property_slug=old_loan.get("property_slug"),
            notes=f"Refinanced from {old_slug} on {new_funded} ({payoff_reason})",
        ))

        # 6. NEW loan's funding transaction. Default offset is the
        # OLD loan's liability path so the new-loan funds flow
        # directly into the old-loan payoff in a single zero-sum txn.
        plan.append(PlannedLoanFunding(
            slug=new_slug,
            display_name=(
                params.get("new_loan_display_name") or new_slug
            ).strip(),
            funded_date=new_funded,
            principal=str(new_principal),
            offset_account=offset,
            liability_account_path=liability,
            narration=(
                params.get("new_funding_narration") or "").strip()
                or f"Refi funding — payoff {old_slug} as {new_slug}",
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

        new_slug = (params.get("new_loan_slug") or "").strip()
        return FlowResult(
            redirect_to=f"/settings/loans/{new_slug}",
            saved_message="refi-committed",
        )


def _register():
    from lamella.web.routes.loans_wizard import register_flow
    register_flow(RefiFlow())


_register()

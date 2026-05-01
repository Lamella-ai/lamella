# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""WP10 — Import-existing wizard flow.

"I have a mortgage already in progress and want to track it." User
provides loan terms (or skips term entry by entering a payoff
statement), an anchor balance to pin reality at a known date, and
optionally opts in to running the WP11 backfill flow afterwards.

Steps:
  1. terms_source        — pick "I have full original terms" vs
                           "I just have a recent statement"
  2. terms_full          — only when 'full'; principal/funded/term/APR
  3. terms_from_statement — only when 'statement'; recent balance +
                           remaining months → derives original_principal
                           via reverse-amortization
  4. accounts            — liability/interest/escrow paths
  5. anchor              — anchor date + balance (the loan's reality
                           pin going forward; coverage starts here)
  6. backfill_choice     — opt in to /settings/loans/{slug}/backfill
                           or skip and start clean from the anchor

Two branches:

* terms_source 'full' vs 'statement' — affects which fields the user
  sees but NOT what gets written. By the time write_plan() runs,
  params has the resolved terms, so the plan is identical regardless
  of which branch produced them.

* backfill_choice 'opt-in' vs 'skip' — affects ONLY the post-commit
  redirect target. The wizard's commit writes loan + anchor only;
  it never tries to run backfill inside the WizardCommitTxn (mixing
  job-runner with bean-check transactions ends badly). On 'opt-in'
  the redirect lands on the backfill flow with the loan slug
  pre-loaded; on 'skip' it lands on the loan detail page.

Plan produced:
  - PlannedAccountsOpen   (liability + optional interest/escrow)
  - PlannedLoanWrite      (the custom "loan" directive)
  - PlannedLoanBalanceAnchor  (the user-provided pin)
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
    PlannedLoanWrite,
    PlannedWrite,
    ValidationError,
    WizardCommitTxn,
    WizardStep,
    err,
)

log = logging.getLogger(__name__)


_LOAN_TYPES = ("mortgage", "auto", "student", "personal", "heloc", "other")
_TERMS_SOURCES = ("full", "statement")
_BACKFILL_CHOICES = ("opt-in", "skip")


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


# --------------------------------------------------------- term derivation


def _derive_original_principal_from_statement(
    statement_balance: Decimal,
    apr: Decimal,
    months_remaining: int,
    months_elapsed: int,
) -> Decimal | None:
    """Reverse-amortize a payoff-statement balance to original principal.

    Given a current balance, monthly rate, and how many payments have
    been made, the original principal P satisfies:

        balance = P · (1+r)^elapsed
                  - PMT · ((1+r)^elapsed - 1) / r

    where PMT is the level payment for the original (full) term.
    Solving for P given (balance, r, elapsed, total_months) requires
    knowing the original term — which the user provides as
    elapsed + remaining.

    This is best-effort: if the user's statement balance disagrees with
    a clean amortization schedule (extra-principal payments,
    skipped months, escrow shortages rolled into balance), the
    derived P will be off. The user can override on the terms_full
    step if they care; otherwise the loan's amortization model will
    just be slightly out and the anchor pins reality regardless.

    Returns None when inputs are degenerate (zero rate handled, but
    negative or zero terms / balance produce None).
    """
    if statement_balance <= 0 or months_remaining <= 0 or months_elapsed < 0:
        return None
    total_months = months_elapsed + months_remaining
    if total_months <= 0:
        return None

    monthly_rate = apr / Decimal("100") / Decimal("12")

    if monthly_rate == 0:
        # Linear payoff: P = balance + PMT * elapsed, PMT = P / total
        # → balance = P - P * elapsed/total = P * remaining/total
        # → P = balance * total/remaining
        return (
            statement_balance * Decimal(total_months) / Decimal(months_remaining)
        ).quantize(Decimal("0.01"))

    # Standard amortization: PMT = P · r · (1+r)^n / ((1+r)^n - 1)
    # Balance after k months: B = P · ((1+r)^n - (1+r)^k) / ((1+r)^n - 1)
    # Solve for P: P = B · ((1+r)^n - 1) / ((1+r)^n - (1+r)^k)
    one = Decimal("1")
    a = (one + monthly_rate) ** total_months
    b = (one + monthly_rate) ** months_elapsed
    denom = a - b
    if denom == 0:
        return None
    return (statement_balance * (a - one) / denom).quantize(Decimal("0.01"))


def _resolve_terms(params: dict) -> dict:
    """Normalize terms: when terms_source is 'statement', derive
    original_principal from the statement balance + remaining months.
    Sets ``_resolved_principal`` and ``_resolved_funded_date`` on
    a copy of params; original fields (statement_balance,
    months_remaining, months_elapsed) stay for re-render after a
    back-edit.

    Called by validate() (for the derived value's sanity-check) AND
    by write_plan() (for the resolved value used in the directive).
    """
    out = dict(params)
    source = (params.get("terms_source") or "").strip()
    if source == "full":
        out["_resolved_principal"] = (
            _to_decimal(params.get("original_principal")) or Decimal("0")
        )
        out["_resolved_funded_date"] = (params.get("funded_date") or "").strip()
        out["_resolved_term_months"] = _to_int(params.get("term_months")) or 0
        return out
    # source == 'statement'
    statement_balance = _to_decimal(params.get("statement_balance"))
    apr = _to_decimal(params.get("interest_rate_apr")) or Decimal("0")
    months_remaining = _to_int(params.get("months_remaining"))
    months_elapsed = _to_int(params.get("months_elapsed")) or 0
    if not statement_balance or not months_remaining:
        out["_resolved_principal"] = Decimal("0")
        out["_resolved_funded_date"] = ""
        out["_resolved_term_months"] = 0
        return out
    derived = _derive_original_principal_from_statement(
        statement_balance, apr, months_remaining, months_elapsed,
    )
    out["_resolved_principal"] = derived or Decimal("0")
    # If the user gave a statement_date and elapsed-month count, we
    # can compute the implied funded_date by subtracting elapsed
    # months from the statement date. Not perfect (real funded date
    # may differ by a few days due to first-payment-day mechanics)
    # but close enough for the amortization model.
    statement_date = _to_date(params.get("statement_date"))
    if statement_date and months_elapsed >= 0:
        # Roll back elapsed months.
        year_offset, month_offset = divmod(
            statement_date.month - 1 - months_elapsed, 12,
        )
        new_year = statement_date.year + year_offset
        new_month = month_offset + 1
        try:
            out["_resolved_funded_date"] = date(
                new_year, new_month, statement_date.day,
            ).isoformat()
        except ValueError:
            out["_resolved_funded_date"] = date(
                new_year, new_month, 1,
            ).isoformat()
    else:
        out["_resolved_funded_date"] = ""
    out["_resolved_term_months"] = (months_elapsed or 0) + months_remaining
    return out


# --------------------------------------------------------------------- flow


class ImportExistingFlow:
    name = "import_existing"
    title = "Import an existing loan"

    def steps(self) -> dict[str, WizardStep]:
        return {
            "terms_source": WizardStep(
                name="terms_source",
                template="loans_wizard_import_step_terms_source.html",
                title="How will you provide terms?",
            ),
            "terms_full": WizardStep(
                name="terms_full",
                template="loans_wizard_import_step_terms_full.html",
                title="Original loan terms",
            ),
            "terms_from_statement": WizardStep(
                name="terms_from_statement",
                template="loans_wizard_import_step_terms_statement.html",
                title="Terms from a recent statement",
            ),
            "accounts": WizardStep(
                name="accounts",
                template="loans_wizard_import_step_accounts.html",
                title="Account paths",
            ),
            "anchor": WizardStep(
                name="anchor",
                template="loans_wizard_import_step_anchor.html",
                title="Pin a balance",
            ),
            "backfill_choice": WizardStep(
                name="backfill_choice",
                template="loans_wizard_import_step_backfill_choice.html",
                title="Backfill historical payments?",
            ),
        }

    def initial_step(self) -> str:
        return "terms_source"

    # -------------------------------------------------------- validate

    def validate(
        self, step_name: str, params: dict, conn: Any,
    ) -> list[ValidationError]:
        errs: list[ValidationError] = []

        # Step 1: terms_source — required at every later step.
        terms_source = (params.get("terms_source") or "").strip()
        if step_name in (
            "terms_source", "terms_full", "terms_from_statement",
            "accounts", "anchor", "backfill_choice",
        ):
            if terms_source not in _TERMS_SOURCES:
                errs.append(err(
                    "Pick whether you have full original terms or just "
                    "a recent statement.",
                    field="terms_source",
                ))

        # Common fields required at every terms-bearing step.
        if step_name in (
            "terms_full", "terms_from_statement", "accounts",
            "anchor", "backfill_choice",
        ):
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
            apr = _to_decimal(params.get("interest_rate_apr"))
            if apr is None or apr < 0 or apr > 50:
                errs.append(err(
                    "APR must be 0–50%.",
                    field="interest_rate_apr",
                ))

        # Branch A: terms_full — full original terms.
        if terms_source == "full" and step_name in (
            "terms_full", "accounts", "anchor", "backfill_choice",
        ):
            principal = _to_decimal(params.get("original_principal"))
            if principal is None or principal <= 0:
                errs.append(err(
                    "Original principal must be a positive number.",
                    field="original_principal",
                ))
            term = _to_int(params.get("term_months"))
            if term is None or term <= 0 or term > 600:
                errs.append(err(
                    "Term must be 1–600 months.",
                    field="term_months",
                ))
            funded = _to_date(params.get("funded_date"))
            if funded is None:
                errs.append(err(
                    "Funded date is required.", field="funded_date",
                ))

        # Branch B: terms_from_statement — derive principal.
        if terms_source == "statement" and step_name in (
            "terms_from_statement", "accounts", "anchor", "backfill_choice",
        ):
            statement_balance = _to_decimal(params.get("statement_balance"))
            if statement_balance is None or statement_balance <= 0:
                errs.append(err(
                    "Statement balance must be a positive number.",
                    field="statement_balance",
                ))
            statement_date = _to_date(params.get("statement_date"))
            if statement_date is None:
                errs.append(err(
                    "Statement date is required.",
                    field="statement_date",
                ))
            months_remaining = _to_int(params.get("months_remaining"))
            if months_remaining is None or months_remaining <= 0:
                errs.append(err(
                    "Months remaining must be a positive integer.",
                    field="months_remaining",
                ))
            months_elapsed = _to_int(params.get("months_elapsed"))
            if months_elapsed is None or months_elapsed < 0:
                errs.append(err(
                    "Months elapsed must be 0 or more.",
                    field="months_elapsed",
                ))
            # Sanity-check the derivation produces a plausible number.
            if (
                not errs
                and statement_balance and apr is not None
                and months_remaining and months_elapsed is not None
            ):
                derived = _derive_original_principal_from_statement(
                    statement_balance, apr,
                    months_remaining, months_elapsed,
                )
                if derived is None or derived <= 0:
                    errs.append(err(
                        "Could not derive a sensible original principal "
                        "from the statement balance and term inputs. "
                        "Switch to 'full original terms' if you have them.",
                    ))

        # Step 4: accounts — same liability rule as purchase flow.
        if step_name in ("accounts", "anchor", "backfill_choice"):
            liability = (params.get("liability_account_path") or "").strip()
            if not liability or not liability.startswith("Liabilities:"):
                errs.append(err(
                    "Liability account path must start with 'Liabilities:'.",
                    field="liability_account_path",
                ))

        # Step 5: anchor — required.
        if step_name in ("anchor", "backfill_choice"):
            anchor_date = _to_date(params.get("anchor_date"))
            anchor_balance = _to_decimal(params.get("anchor_balance"))
            if anchor_date is None:
                errs.append(err(
                    "Anchor date is required.", field="anchor_date",
                ))
            if anchor_balance is None or anchor_balance < 0:
                errs.append(err(
                    "Anchor balance must be a non-negative number.",
                    field="anchor_balance",
                ))

        # Step 6: backfill_choice.
        if step_name == "backfill_choice":
            choice = (params.get("backfill_choice") or "").strip()
            if choice not in _BACKFILL_CHOICES:
                errs.append(err(
                    "Pick whether to opt in to backfill or skip it.",
                    field="backfill_choice",
                ))

        return errs

    # -------------------------------------------------------- next_step

    def next_step(
        self, current_step: str, params: dict, conn: Any,
    ) -> str | None:
        terms_source = (params.get("terms_source") or "").strip()
        if current_step == "terms_source":
            if terms_source == "full":
                return "terms_full"
            return "terms_from_statement"
        if current_step in ("terms_full", "terms_from_statement"):
            return "accounts"
        if current_step == "accounts":
            return "anchor"
        if current_step == "anchor":
            return "backfill_choice"
        if current_step == "backfill_choice":
            return None  # → preview
        return None

    # ------------------------------------------------- template_context

    def template_context(
        self, step_name: str, params: dict, conn: Any,
    ) -> dict:
        """Render-time context. On the terms_from_statement step, we
        compute and surface the derived principal so the user can
        see what their statement balance will be normalized to —
        important transparency for an heuristic that may need
        manual override."""
        ctx: dict = {}
        if step_name == "terms_from_statement":
            resolved = _resolve_terms(params)
            ctx["derived_principal"] = resolved.get("_resolved_principal")
            ctx["derived_funded_date"] = resolved.get("_resolved_funded_date")
        return ctx

    # -------------------------------------------------------- write_plan

    def write_plan(
        self, params: dict, conn: Any,
    ) -> list[PlannedWrite]:
        """Both branches produce identical plans — _resolve_terms()
        normalizes statement-derived terms into the same shape as
        full-terms input. The branch only affects what the user saw
        on the form."""
        resolved = _resolve_terms(params)
        plan: list[PlannedWrite] = []

        slug = (params.get("loan_slug") or "").strip()
        funded_iso = resolved.get("_resolved_funded_date") or ""
        principal = resolved.get("_resolved_principal") or Decimal("0")
        term_months = resolved.get("_resolved_term_months") or 0
        anchor_iso = (params.get("anchor_date") or "").strip()
        anchor_balance = (
            _to_decimal(params.get("anchor_balance")) or Decimal("0")
        )

        # Account opens — same shape as purchase flow.
        liability = (params.get("liability_account_path") or "").strip()
        interest = (params.get("interest_account_path") or "").strip()
        escrow = (params.get("escrow_account_path") or "").strip()
        paths: list[str] = []
        for p in (liability, interest, escrow):
            if p and p not in paths:
                paths.append(p)
        if paths:
            # Open dated to funded_date if known, else anchor_date —
            # bean-check needs the Open to predate the earliest
            # posting we'll write, and the anchor IS the earliest.
            opens_on = funded_iso or anchor_iso
            plan.append(PlannedAccountsOpen(
                paths=tuple(paths),
                opened_on=opens_on,
                comment=f"Import-existing wizard scaffold for {slug}",
            ))

        # Loan directive.
        plan.append(PlannedLoanWrite(
            slug=slug,
            display_name=(params.get("loan_display_name") or slug).strip(),
            loan_type=(params.get("loan_type") or "").strip(),
            entity_slug=(params.get("loan_entity_slug") or "").strip() or None,
            institution=(params.get("loan_institution") or "").strip() or None,
            original_principal=str(principal),
            funded_date=funded_iso,
            first_payment_date=(params.get("first_payment_date") or "").strip()
                               or None,
            payment_due_day=_to_int(params.get("payment_due_day")),
            term_months=term_months or None,
            interest_rate_apr=str(
                _to_decimal(params.get("interest_rate_apr")) or Decimal("0")
            ),
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
            property_slug=(params.get("property_slug") or "").strip() or None,
        ))

        # Anchor write — pins reality at the user-stated date.
        plan.append(PlannedLoanBalanceAnchor(
            loan_slug=slug,
            as_of_date=anchor_iso,
            balance=str(anchor_balance),
            source=(params.get("anchor_source") or "statement").strip(),
            notes=(params.get("anchor_notes") or "").strip() or None,
        ))

        return plan

    # -------------------------------------------------------- commit

    def commit(
        self, params: dict, settings: Any, conn: Any, reader: Any,
    ) -> FlowResult:
        """Commit loan + anchor under one WizardCommitTxn. Backfill
        is NEVER inside the txn — that's a job-runner-modal pattern,
        not a transactional ledger write. On 'opt-in' the redirect
        target points at the backfill flow with the loan slug
        pre-loaded; on 'skip' it lands on the loan detail page."""
        plan = self.write_plan(params, conn)
        with WizardCommitTxn(settings, conn=conn):
            for planned in plan:
                planned.execute(settings=settings, conn=conn, reader=reader)

        slug = (params.get("loan_slug") or "").strip()
        choice = (params.get("backfill_choice") or "").strip()
        if choice == "opt-in":
            return FlowResult(
                redirect_to=f"/settings/loans/{slug}/backfill",
                saved_message="loan-imported-go-backfill",
            )
        return FlowResult(
            redirect_to=f"/settings/loans/{slug}",
            saved_message="loan-imported",
        )


def _register():
    from lamella.web.routes.loans_wizard import register_flow
    register_flow(ImportExistingFlow())


_register()

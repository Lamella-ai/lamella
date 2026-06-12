# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Writers for loan state: `custom "loan"` and `custom "loan-balance-anchor"`.

Each function wraps append_custom_directive with the standard
snapshot / bean-check / rollback contract. The companion reader in
``loans/reader.py`` rebuilds SQLite rows from loaded Beancount entries.

Directive shape — ``loan``:
    2026-04-23 custom "loan" "MainResidenceMortgage"
      lamella-loan-display-name: "Main Residence Mortgage"
      lamella-loan-type: "mortgage"
      lamella-loan-entity-slug: "Personal"
      lamella-loan-institution: "Bank Two"
      lamella-loan-original-principal: "550000.00"
      lamella-loan-funded-date: 2025-10-27
      lamella-loan-interest-rate-apr: "6.625"
      lamella-loan-term-months: 360
      lamella-loan-monthly-payment: "3521.64"
      lamella-loan-escrow-monthly: "850.00"
      lamella-loan-liability-account: Liabilities:Personal:BankTwo:MainResidenceMortgage
      lamella-loan-property-slug: "MainResidence"
      lamella-loan-is-active: TRUE

A later ``custom "loan"`` with the same slug supersedes the earlier
one (reader keeps the last seen per slug). Used for simple edits.

Directive shape — ``loan-balance-anchor``:
    2026-02-01 custom "loan-balance-anchor" "MainResidenceMortgage" 548912.43
      lamella-anchor-source: "statement"
      lamella-anchor-notes: "Feb statement"
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from lamella.core.fs import validate_safe_path
from lamella.core.transform.custom_directive import (
    Account,
    append_custom_directive,
)

log = logging.getLogger(__name__)


CONNECTOR_CONFIG_HEADER = (
    "; connector_config.bean — configuration state written by Lamella.\n"
    "; Paperless field-role mappings and UI-persisted settings live here.\n"
    "; Do not hand-edit; use the /settings pages.\n"
)


def _today() -> date:
    return datetime.now(timezone.utc).date()


def append_loan(
    *,
    connector_config: Path,
    main_bean: Path,
    slug: str,
    display_name: str | None,
    loan_type: str,
    entity_slug: str | None,
    institution: str | None,
    original_principal: str,
    funded_date: str | date,
    first_payment_date: str | date | None = None,
    payment_due_day: int | None = None,
    term_months: int | None = None,
    interest_rate_apr: str | None = None,
    monthly_payment_estimate: str | None = None,
    escrow_monthly: str | None = None,
    property_tax_monthly: str | None = None,
    insurance_monthly: str | None = None,
    liability_account_path: str | None = None,
    interest_account_path: str | None = None,
    escrow_account_path: str | None = None,
    simplefin_account_id: str | None = None,
    property_slug: str | None = None,
    payoff_date: str | date | None = None,
    payoff_amount: str | None = None,
    is_active: bool = True,
    is_revolving: bool = False,
    credit_limit: str | None = None,
    notes: str | None = None,
    run_check: bool = True,
) -> str:
    meta: dict[str, Any] = {
        "lamella-loan-type": loan_type,
        "lamella-loan-original-principal": str(original_principal),
        "lamella-loan-funded-date": _as_date(funded_date),
        "lamella-loan-is-active": bool(is_active),
    }
    if display_name:
        meta["lamella-loan-display-name"] = display_name
    if entity_slug:
        meta["lamella-loan-entity-slug"] = entity_slug
    if institution:
        meta["lamella-loan-institution"] = institution
    if first_payment_date:
        meta["lamella-loan-first-payment-date"] = _as_date(first_payment_date)
    if payment_due_day is not None:
        meta["lamella-loan-payment-due-day"] = int(payment_due_day)
    if term_months is not None:
        meta["lamella-loan-term-months"] = int(term_months)
    if interest_rate_apr:
        meta["lamella-loan-interest-rate-apr"] = str(interest_rate_apr)
    if monthly_payment_estimate:
        meta["lamella-loan-monthly-payment"] = str(monthly_payment_estimate)
    if escrow_monthly:
        meta["lamella-loan-escrow-monthly"] = str(escrow_monthly)
    if property_tax_monthly:
        meta["lamella-loan-property-tax-monthly"] = str(property_tax_monthly)
    if insurance_monthly:
        meta["lamella-loan-insurance-monthly"] = str(insurance_monthly)
    if liability_account_path:
        meta["lamella-loan-liability-account"] = Account(liability_account_path)
    if interest_account_path:
        meta["lamella-loan-interest-account"] = Account(interest_account_path)
    if escrow_account_path:
        meta["lamella-loan-escrow-account"] = Account(escrow_account_path)
    if simplefin_account_id:
        meta["lamella-loan-simplefin-account-id"] = simplefin_account_id
    if property_slug:
        meta["lamella-loan-property-slug"] = property_slug
    if payoff_date:
        meta["lamella-loan-payoff-date"] = _as_date(payoff_date)
    if payoff_amount:
        meta["lamella-loan-payoff-amount"] = str(payoff_amount)
    if is_revolving:
        meta["lamella-loan-is-revolving"] = True
    if credit_limit:
        # DEFERRED-WP13-PHASE2: credit-limit history. Banks adjust HELOC
        # limits over time — recession-era cuts, post-recovery bumps,
        # cash-out refis on the underlying property. Today every edit
        # to credit_limit overwrites the prior value with no record of
        # what it used to be. The fix shape:
        #   - new state table ``loan_credit_limit_history (loan_slug,
        #     effective_date, credit_limit)`` reconstructable from a
        #     new ``custom "loan-credit-limit-change"`` directive
        #   - this writer emits the "change" directive instead of
        #     overwriting the meta on the parent ``loan`` directive when
        #     credit_limit changes vs. the prior cached value
        # Until that lands, the "available headroom" calc on the panel
        # is correct for *today* but historical headroom queries (e.g.
        # "how much was available before the 2020 cut?") aren't
        # answerable.
        meta["lamella-loan-credit-limit"] = str(credit_limit)
    if notes:
        meta["lamella-loan-notes"] = notes

    return append_custom_directive(
        target=connector_config, main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=_today(),
        directive_type="loan",
        args=[slug],
        meta=meta,
        run_check=run_check,
    )


def append_loan_balance_anchor(
    *,
    connector_config: Path,
    main_bean: Path,
    loan_slug: str,
    as_of_date: str | date,
    balance: str,
    source: str | None = None,
    notes: str | None = None,
    run_check: bool = True,
) -> str:
    meta: dict[str, Any] = {}
    if source:
        meta["lamella-anchor-source"] = source
    if notes:
        meta["lamella-anchor-notes"] = notes
    return append_custom_directive(
        target=connector_config, main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=_as_date(as_of_date),
        directive_type="loan-balance-anchor",
        args=[loan_slug, str(balance)],
        meta=meta,
        run_check=run_check,
    )


def write_loan_funding(
    *,
    loan: dict,
    settings: Any,
    funded_date: date,
    principal: Decimal,
    offset_account: str,
    narration: str | None = None,
    run_check: bool = True,
) -> None:
    """Append a ``#lamella-loan-funding`` opening-balance transaction.

    Used by both ``record_missing_payment``-style routes and the WP10
    purchase / refi wizards. Writes a tagged transaction that posts
    -principal on the liability and +principal on the offset account
    (cost-basis, opening-equity, checking, etc.).

    Block shape:

      {date} * "{narration}" #lamella-loan-funding
        lamella-loan-slug: "{slug}"
        {liability}    -{principal} USD     ; debt accrues
        {offset}       +{principal} USD     ; offset balances

    Caller is responsible for ensuring both the liability and the
    offset account are Open'd on or before ``funded_date``. On
    bean-check rejection this function rolls main.bean +
    connector_overrides.bean back byte-for-byte and re-raises
    BeanCheckError.

    Mirrors ``write_synthesized_payment``'s contract: ``run_check=False``
    skips the per-write check (the WizardCommitTxn runs a single check
    over a multi-write batch).
    """
    from lamella.core.ledger_writer import (
        BeanCheckError,
        capture_bean_check,
        ensure_include_in_main,
        run_bean_check_vs_baseline,
    )
    from lamella.features.rules.overrides import ensure_overrides_exists

    slug = loan.get("slug") or ""
    liability_path = loan.get("liability_account_path")
    if not liability_path:
        raise ValueError(f"loan {slug} has no liability_account_path")
    if principal <= 0:
        raise ValueError(f"principal must be > 0; got {principal}")
    if not offset_account:
        raise ValueError("offset_account is required")

    narration_str = narration or (
        f"Loan funding — {loan.get('display_name') or slug}"
    )
    narr_esc = narration_str.replace("\\", "\\\\").replace('"', '\\"')

    block = (
        f'\n{funded_date.isoformat()} * "{narr_esc}" #lamella-loan-funding\n'
        f'  lamella-loan-slug: "{slug}"\n'
        f"  {liability_path}  {(-principal):.2f} USD\n"
        f"  {offset_account}  {principal:.2f} USD\n"
    )

    overrides_path = settings.connector_overrides_path
    main_bean = settings.ledger_main
    # ADR-0030: validate both paths land inside the ledger directory
    # before any read/write touches disk.
    ledger_dir = main_bean.parent
    main_bean = validate_safe_path(main_bean, allowed_roots=[ledger_dir])
    overrides_path = validate_safe_path(
        overrides_path, allowed_roots=[ledger_dir]
    )
    backup_main = main_bean.read_bytes() if main_bean.exists() else b""
    backup_ov = (
        overrides_path.read_bytes() if overrides_path.exists() else None
    )
    if run_check:
        _, baseline = capture_bean_check(main_bean)
    else:
        baseline = ""

    ensure_overrides_exists(overrides_path)
    ensure_include_in_main(main_bean, overrides_path)
    with overrides_path.open("a", encoding="utf-8") as fh:
        fh.write(block)

    if run_check:
        try:
            run_bean_check_vs_baseline(main_bean, baseline)
        except BeanCheckError:
            main_bean.write_bytes(backup_main)
            if backup_ov is None:
                overrides_path.unlink(missing_ok=True)
            else:
                overrides_path.write_bytes(backup_ov)
            raise


def write_synthesized_payment(
    *,
    loan: dict,
    settings: Any,
    txn_date: date,
    expected_n: int | None,
    principal: Decimal,
    interest: Decimal,
    escrow: Decimal = Decimal("0"),
    tax: Decimal = Decimal("0"),
    insurance: Decimal = Decimal("0"),
    late_fee: Decimal = Decimal("0"),
    offset_account: str,
    narration: str | None = None,
    run_check: bool = True,
) -> None:
    """Append a synthesized historical-payment transaction.

    Used by both ``record_missing_payment`` (one-off) and the WP11
    backfill job worker (batch). Writes a tagged
    ``#lamella-loan-backfill`` transaction with lamella-loan-slug +
    lamella-loan-expected-n meta so reconstruct can rebuild it and WP8
    anomaly detection can tell user-synthesized vs observed
    payments apart.

    Block shape:

      {date} * "{narration}" #lamella-loan-backfill
        lamella-loan-slug: "{slug}"
        lamella-loan-expected-n: {n}
        {liability}      {principal} USD     ; positive — paydown
        {interest}       {interest} USD      ; if interest > 0
        {escrow}         {escrow} USD        ; if escrow > 0
        {tax}            {tax} USD           ; if tax > 0
        {insurance}      {insurance} USD     ; if insurance > 0
        {offset}         {-total} USD        ; negative — money out

    Caller is responsible for ensuring every referenced account is
    Open'd on or before ``txn_date`` (use scaffolding's
    ``ensure_open_on_or_before`` per leg). On bean-check rejection
    this function rolls main.bean + connector_overrides.bean back
    byte-for-byte and re-raises BeanCheckError.

    Note: this writes a fresh transaction, NOT an override block —
    historical backfill rows have no FIXME txn to point at, so
    lamella-override-of is intentionally absent.
    """
    from lamella.core.ledger_writer import (
        BeanCheckError,
        capture_bean_check,
        ensure_include_in_main,
        run_bean_check_vs_baseline,
    )
    from lamella.features.rules.overrides import ensure_overrides_exists

    slug = loan.get("slug") or ""
    liability_path = loan.get("liability_account_path")
    interest_path = loan.get("interest_account_path")
    escrow_path = loan.get("escrow_account_path")

    if not liability_path:
        raise ValueError(f"loan {slug} has no liability_account_path")
    if interest > 0 and not interest_path:
        raise ValueError(
            f"interest provided but loan {slug} has no interest_account_path"
        )
    if escrow > 0 and not escrow_path:
        raise ValueError(
            f"escrow provided but loan {slug} has no escrow_account_path"
        )

    # Tax + insurance auto-derive their paths from the loan's entity
    # + slug (matching scaffolding._default_tax_path / _insurance_path).
    entity = loan.get("entity_slug") or ""
    tax_path = (
        f"Expenses:{entity}:{slug}:PropertyTax"
        if (tax > 0 and entity and slug) else None
    )
    insurance_path = (
        f"Expenses:{entity}:{slug}:Insurance"
        if (insurance > 0 and entity and slug) else None
    )
    late_fee_path = (
        f"Expenses:{entity}:{slug}:LateFees"
        if (late_fee > 0 and entity and slug) else None
    )
    if tax > 0 and not tax_path:
        raise ValueError(
            f"tax provided but loan {slug} has no entity_slug for path derivation"
        )
    if insurance > 0 and not insurance_path:
        raise ValueError(
            f"insurance provided but loan {slug} has no entity_slug for path derivation"
        )
    if late_fee > 0 and not late_fee_path:
        raise ValueError(
            f"late_fee provided but loan {slug} has no entity_slug for path derivation"
        )

    total = principal + interest + escrow + tax + insurance + late_fee

    narration_str = (
        narration
        or f"Loan payment (recorded for expected #{expected_n})"
        if expected_n is not None
        else (narration or f"Loan payment {slug}")
    )
    # Escape narration for the beancount string literal.
    narr_esc = narration_str.replace("\\", "\\\\").replace('"', '\\"')

    lines: list[str] = []
    lines.append(
        f'\n{txn_date.isoformat()} * "{narr_esc}" #lamella-loan-backfill'
    )
    lines.append(f'  lamella-loan-slug: "{slug}"')
    if expected_n is not None:
        lines.append(f"  lamella-loan-expected-n: {int(expected_n)}")
    if principal > 0:
        lines.append(f"  {liability_path}  {principal:.2f} USD")
    if interest > 0 and interest_path:
        lines.append(f"  {interest_path}  {interest:.2f} USD")
    if escrow > 0 and escrow_path:
        lines.append(f"  {escrow_path}  {escrow:.2f} USD")
    if tax > 0 and tax_path:
        lines.append(f"  {tax_path}  {tax:.2f} USD")
    if insurance > 0 and insurance_path:
        lines.append(f"  {insurance_path}  {insurance:.2f} USD")
    if late_fee > 0 and late_fee_path:
        lines.append(f"  {late_fee_path}  {late_fee:.2f} USD")
    lines.append(f"  {offset_account}  {-total:.2f} USD")
    block = "\n".join(lines) + "\n"

    overrides_path = settings.connector_overrides_path
    main_bean = settings.ledger_main
    # ADR-0030: validate both paths land inside the ledger directory
    # before any read/write touches disk.
    ledger_dir = main_bean.parent
    main_bean = validate_safe_path(main_bean, allowed_roots=[ledger_dir])
    overrides_path = validate_safe_path(
        overrides_path, allowed_roots=[ledger_dir]
    )
    backup_main = main_bean.read_bytes() if main_bean.exists() else b""
    backup_ov = overrides_path.read_bytes() if overrides_path.exists() else None

    if run_check:
        _, baseline = capture_bean_check(main_bean)
    else:
        baseline = ""

    ensure_overrides_exists(overrides_path)
    ensure_include_in_main(main_bean, overrides_path)
    with overrides_path.open("a", encoding="utf-8") as fh:
        fh.write(block)

    if run_check:
        try:
            run_bean_check_vs_baseline(main_bean, baseline)
        except BeanCheckError:
            main_bean.write_bytes(backup_main)
            if backup_ov is None:
                overrides_path.unlink(missing_ok=True)
            else:
                overrides_path.write_bytes(backup_ov)
            raise


def append_loan_pause(
    *,
    connector_config: Path,
    main_bean: Path,
    loan_slug: str,
    start_date: str | date,
    end_date: str | date | None = None,
    reason: str | None = None,
    notes: str | None = None,
    accrued_interest: str | None = None,
    run_check: bool = True,
) -> str:
    """Write a ``custom "loan-pause" "<slug>"`` directive.

    Pauses suspend the WP3 coverage engine's expected-row generation
    for the (start_date, end_date or today] window so the system
    doesn't surface a forbearance period as missed payments. The
    ``custom "loan-pause"`` directive is the source of truth;
    ``loan_pauses`` (SQLite) is a cache reconstruct rebuilds via
    step23 + read_loan_pauses.

    Tombstoned by ``append_loan_pause_revoked`` carrying the same
    slug + lamella-pause-start so reconstruct knows the pause is gone.
    """
    meta: dict[str, Any] = {}
    if end_date:
        meta["lamella-pause-end"] = _as_date(end_date)
    if reason:
        meta["lamella-pause-reason"] = reason
    if notes:
        meta["lamella-pause-notes"] = notes
    if accrued_interest:
        meta["lamella-pause-accrued-interest"] = str(accrued_interest)
    return append_custom_directive(
        target=connector_config, main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=_as_date(start_date) or _today(),
        directive_type="loan-pause",
        args=[loan_slug],
        meta=meta,
        run_check=run_check,
    )


def append_loan_pause_revoked(
    *,
    connector_config: Path,
    main_bean: Path,
    loan_slug: str,
    pause_start: str | date,
    run_check: bool = True,
) -> str:
    """Tombstone a previously-written loan-pause.

    The ``lamella-pause-start`` arg disambiguates which pause is being
    revoked — without it, a loan with multiple historic pauses
    couldn't tell us which one to drop.
    """
    return append_custom_directive(
        target=connector_config, main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=_today(),
        directive_type="loan-pause-revoked",
        args=[loan_slug],
        meta={
            "lamella-pause-start": _as_date(pause_start),
        },
        run_check=run_check,
    )


def append_loan_deleted(
    *,
    connector_config: Path,
    main_bean: Path,
    slug: str,
    run_check: bool = True,
) -> str:
    """Tombstone directive so reconstruct skips a loan the user has
    hard-deleted. Without this, an earlier ``loan`` directive for the
    same slug would be re-materialized on the next reconstruct."""
    return append_custom_directive(
        target=connector_config, main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=_today(),
        directive_type="loan-deleted",
        args=[slug],
        meta=None,
        run_check=run_check,
    )


def _as_date(value: str | date | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    # String — ISO format expected.
    return date.fromisoformat(str(value)[:10])

# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""WP11 — historical payment backfill routes.

Lets the user import a year+ of loan-payment history from CSV. Four
endpoints:

  GET  /settings/loans/{slug}/backfill           — entry page
  POST /settings/loans/{slug}/backfill/preview   — parses + renders
  POST /settings/loans/{slug}/backfill/run       — submits job
  GET  /settings/loans/{slug}/backfill/sample.csv — example download

Pure logic lives in ``loans/backfill.py``; this module is just
parameter handling + JobRunner wiring + template rendering.
"""
from __future__ import annotations

import json
import logging
from dataclasses import asdict
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from lamella.core.beancount_io import LedgerReader
from lamella.core.config import Settings
from lamella.web.deps import get_db, get_ledger_reader, get_settings
from lamella.features.loans.amortization import (
    payment_number_on,
    split_for_payment_number,
)
from lamella.features.loans.backfill import (
    BackfillRow,
    SAMPLE_CSV,
    compute_splits,
    parse_csv,
    validate,
)
from lamella.features.loans.scaffolding import ensure_open_on_or_before
from lamella.features.loans.writer import write_synthesized_payment
from lamella.core.ledger_writer import BeanCheckError
from lamella.core.registry.accounts_writer import AccountsWriter

log = logging.getLogger(__name__)

router = APIRouter()


def _to_date_safe(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    try:
        return datetime.fromisoformat(str(value)[:10]).date()
    except (TypeError, ValueError):
        return None


def _row_to_dict(bf: BackfillRow) -> dict:
    """Serialize a BackfillRow for the preview template + job args."""
    return {
        "line_no": bf.line_no,
        "txn_date": bf.txn_date.isoformat() if bf.txn_date else None,
        "total_amount": str(bf.total_amount) if bf.total_amount is not None else None,
        "offset_account": bf.offset_account,
        "narration": bf.narration,
        "expected_n": bf.expected_n,
        "principal": str(bf.principal),
        "interest": str(bf.interest),
        "escrow": str(bf.escrow),
        "tax": str(bf.tax),
        "insurance": str(bf.insurance),
        "error": bf.error,
    }


@router.get("/settings/loans/{slug}/backfill", response_class=HTMLResponse)
def backfill_entry(
    slug: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    """Entry page: upload form + sample CSV link."""
    row = conn.execute(
        "SELECT * FROM loans WHERE slug = ?", (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="loan not found")
    loan = dict(row)
    return request.app.state.templates.TemplateResponse(
        request,
        "settings_loan_backfill.html",
        {"loan": loan, "preview_rows": None, "valid_count": 0,
         "invalid_count": 0, "csv_text": ""},
    )


@router.post("/settings/loans/{slug}/backfill/preview", response_class=HTMLResponse)
async def backfill_preview(
    slug: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    """Parse the submitted CSV, compute splits, render the preview."""
    row = conn.execute(
        "SELECT * FROM loans WHERE slug = ?", (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="loan not found")
    loan = dict(row)

    form = await request.form()
    csv_text = (form.get("csv_text") or "").strip()
    if not csv_text:
        raise HTTPException(
            status_code=400, detail="paste or upload a CSV first.",
        )

    rows = parse_csv(csv_text)
    rows = compute_splits(rows, loan)
    valid, invalid = validate(rows)
    preview_rows = [_row_to_dict(bf) for bf in rows]

    return request.app.state.templates.TemplateResponse(
        request,
        "settings_loan_backfill.html",
        {
            "loan": loan,
            "preview_rows": preview_rows,
            "valid_count": len(valid),
            "invalid_count": len(invalid),
            "csv_text": csv_text,
        },
    )


@router.post("/settings/loans/{slug}/backfill/run", response_class=HTMLResponse)
async def backfill_run(
    slug: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Submit a JobRunner job that walks the validated rows and writes
    a synthesized #lamella-loan-backfill transaction per row.

    Returns the _job_modal partial so the form can hx-swap it onto
    the page; the modal polls job state and shows progress.
    """
    row = conn.execute(
        "SELECT * FROM loans WHERE slug = ?", (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="loan not found")
    loan = dict(row)

    form = await request.form()
    csv_text = (form.get("csv_text") or "").strip()
    fallback_offset = (form.get("offset_account") or "").strip() or None
    if not csv_text:
        raise HTTPException(status_code=400, detail="csv_text is required.")

    rows = parse_csv(csv_text)
    rows = compute_splits(rows, loan)
    valid, _invalid = validate(rows)
    if not valid:
        raise HTTPException(
            status_code=400,
            detail="No valid rows to backfill — fix errors and re-preview.",
        )

    # Apply the fallback offset to any row that didn't carry its own.
    args_rows: list[dict] = []
    for bf in valid:
        d = _row_to_dict(bf)
        if not d["offset_account"]:
            if not fallback_offset:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"row {bf.line_no} has no offset_account and no "
                        f"fallback offset_account was provided."
                    ),
                )
            d["offset_account"] = fallback_offset
        args_rows.append(d)

    settings_payload = {
        "ledger_main": str(settings.ledger_main),
        "connector_overrides_path": str(settings.connector_overrides_path),
        "connector_accounts_path": str(settings.connector_accounts_path),
    }

    job_runner = request.app.state.job_runner
    job_id = job_runner.submit(
        kind="loan-backfill",
        title=f"Backfill {len(args_rows)} payments for {slug}",
        fn=lambda ctx: _backfill_worker(
            ctx, slug=slug, loan_dict=loan, rows=args_rows,
            settings_payload=settings_payload,
        ),
        total=len(args_rows),
        meta={"slug": slug, "rows_count": len(args_rows)},
        return_url=f"/settings/loans/{slug}",
    )
    return request.app.state.templates.TemplateResponse(
        request,
        "partials/_job_modal.html",
        {"job_id": job_id, "on_close_url": f"/settings/loans/{slug}"},
    )


@router.get("/settings/loans/{slug}/backfill/sample.csv")
def backfill_sample_csv(
    slug: str,
    conn = Depends(get_db),
):
    """Download a per-loan sample CSV pre-filled with the loan's
    expected payment amount and a few dates from the schedule.

    The user can edit it (especially the offset_account and dates) and
    paste the result back into the preview form. Falls back to the
    static SAMPLE_CSV when the loan's amortization model can't be
    derived (incomplete loan terms).
    """
    row = conn.execute(
        "SELECT * FROM loans WHERE slug = ?", (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="loan not found")
    loan = dict(row)

    text = _build_sample_csv(loan) or SAMPLE_CSV
    return Response(
        content=text, media_type="text/csv",
        headers={
            "Content-Disposition": (
                f'attachment; filename="{slug}-backfill-sample.csv"'
            ),
        },
    )


def _build_sample_csv(loan: dict) -> str | None:
    """Generate a 4-row sample CSV using the loan's actual schedule."""
    try:
        principal = Decimal(str(loan.get("original_principal") or "0"))
        apr = Decimal(str(loan.get("interest_rate_apr") or "0"))
        term = int(loan.get("term_months") or 0)
        first = _to_date_safe(loan.get("first_payment_date"))
        escrow = Decimal(str(loan.get("escrow_monthly") or "0"))
        tax = Decimal(str(loan.get("property_tax_monthly") or "0"))
        insurance = Decimal(str(loan.get("insurance_monthly") or "0"))
    except Exception:  # noqa: BLE001
        return None
    if not first or term <= 0 or principal <= 0:
        return None

    split_1 = split_for_payment_number(
        principal, apr, term, 1, escrow_monthly=escrow or None,
    )
    total = split_1["total"] + tax + insurance
    return (
        "date,amount,offset_account,narration\n"
        f"{first.isoformat()},{total:.2f},Assets:Personal:Checking:1234,"
        f"Payment 1\n"
        "# Replace dates and offset_account with your real history,\n"
        "# then paste the table back into the preview form.\n"
    )


# --------------------------------------------------------------- worker


def _backfill_worker(
    ctx,
    *,
    slug: str,
    loan_dict: dict,
    rows: list[dict],
    settings_payload: dict,
) -> dict:
    """JobRunner worker: writes one synthesized payment per row.

    Each row write is wrapped in try/except so a single bean-check
    failure (e.g. a row referencing an offset_account that's not
    Open'd) doesn't abort the whole batch — the worker emits an
    error event for that row and continues.

    Note: the worker re-opens the LedgerReader fresh on each row
    because writing a new transaction invalidates any cached load.

    TODO(wp11-dedup): the worker does NOT currently check whether a
    matching transaction already exists in the ledger before writing.
    This means a user who first imported their loan history via the
    generic CSV importer (creating FIXME-tagged Liabilities:* posts)
    and then runs backfill on the same dates will get TWO postings on
    the liability — bean-check passes (each block balances) but the
    principal-paid total doubles. Detection: pre-load every txn
    touching loan.liability_account_path keyed by (date, abs_amount),
    skip backfill rows where (txn_date, principal_or_total) already
    appears within ±$0.02 / ±2 days. Right place is here, not in the
    pure compute_splits pass — preview should still SHOW these rows so
    the user can decide whether to skip or supersede the prior import.
    Surface "already in ledger — skipped" via ctx.emit(outcome="info").
    """
    from types import SimpleNamespace

    settings = SimpleNamespace(
        ledger_main=Path(settings_payload["ledger_main"]),
        connector_overrides_path=Path(settings_payload["connector_overrides_path"]),
        connector_accounts_path=Path(settings_payload["connector_accounts_path"]),
    )

    written = 0
    failed = 0

    for row_dict in rows:
        ctx.raise_if_cancelled()
        line_no = row_dict.get("line_no")
        try:
            txn_date = _to_date_safe(row_dict["txn_date"])
            principal = Decimal(row_dict["principal"])
            interest = Decimal(row_dict["interest"])
            escrow = Decimal(row_dict["escrow"])
            tax = Decimal(row_dict["tax"])
            insurance = Decimal(row_dict["insurance"])
            offset_account = row_dict["offset_account"]
            expected_n = row_dict.get("expected_n")
            narration = row_dict.get("narration") or (
                f"Loan payment (recorded for expected #{expected_n})"
                if expected_n is not None
                else f"Loan payment {slug}"
            )

            # Per-row reader — picks up prior writes in this same job.
            reader = LedgerReader(settings.ledger_main)

            # Open every leg's account on or before the txn date.
            opener = AccountsWriter(
                main_bean=settings.ledger_main,
                connector_accounts=settings.connector_accounts_path,
            )
            entity = loan_dict.get("entity_slug") or ""
            leg_paths: list[str] = [
                loan_dict["liability_account_path"], offset_account,
            ]
            if interest > 0 and loan_dict.get("interest_account_path"):
                leg_paths.append(loan_dict["interest_account_path"])
            if escrow > 0 and loan_dict.get("escrow_account_path"):
                leg_paths.append(loan_dict["escrow_account_path"])
            if tax > 0 and entity:
                leg_paths.append(f"Expenses:{entity}:{slug}:PropertyTax")
            if insurance > 0 and entity:
                leg_paths.append(f"Expenses:{entity}:{slug}:Insurance")
            for path in leg_paths:
                ensure_open_on_or_before(
                    reader, opener, path, txn_date,
                    connector_accounts_path=settings.connector_accounts_path,
                    comment_tag=f"Backfill for {slug} on {txn_date}",
                )
            reader.invalidate()

            write_synthesized_payment(
                loan=loan_dict,
                settings=settings,
                txn_date=txn_date,
                expected_n=expected_n,
                principal=principal,
                interest=interest,
                escrow=escrow,
                tax=tax,
                insurance=insurance,
                offset_account=offset_account,
                narration=narration,
            )
            written += 1
            ctx.emit(
                f"Wrote {txn_date} ${row_dict['total_amount']} "
                f"(expected #{expected_n})",
                outcome="success",
            )
        except BeanCheckError as exc:
            failed += 1
            ctx.emit(
                f"Row {line_no} ({row_dict.get('txn_date')}): bean-check "
                f"rejected — {exc}",
                outcome="error",
            )
        except Exception as exc:  # noqa: BLE001
            failed += 1
            ctx.emit(
                f"Row {line_no} ({row_dict.get('txn_date')}): {exc}",
                outcome="error",
            )
        finally:
            ctx.advance(1)

    ctx.emit(
        f"Backfill complete · written={written} failed={failed}",
        outcome="info",
    )
    return {"written": written, "failed": failed}

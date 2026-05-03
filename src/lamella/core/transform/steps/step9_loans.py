# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Step 9: loan state reconstruction.

Rebuilds ``loans`` + ``loan_balance_anchors`` from ledger directives.
See loans/writer.py for the writing side, loans/reader.py for parsing.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any

from lamella.features.loans.reader import (
    read_loan_balance_anchors,
    read_loans,
)
from lamella.core.transform.reconstruct import ReconstructReport, register

log = logging.getLogger(__name__)


@register(
    "step9:loans",
    state_tables=["loans", "loan_balance_anchors"],
)
def reconstruct_loans(
    conn: sqlite3.Connection, entries: list[Any],
) -> ReconstructReport:
    written = 0
    notes: list[str] = []

    for row in read_loans(entries):
        is_active = 1 if row["is_active"] is None else (1 if row["is_active"] else 0)
        is_revolving = 1 if row.get("is_revolving") else 0
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
            ON CONFLICT (slug) DO UPDATE SET
                display_name             = excluded.display_name,
                loan_type                = excluded.loan_type,
                entity_slug              = excluded.entity_slug,
                institution              = excluded.institution,
                original_principal       = excluded.original_principal,
                funded_date              = excluded.funded_date,
                first_payment_date       = excluded.first_payment_date,
                payment_due_day          = excluded.payment_due_day,
                term_months              = excluded.term_months,
                interest_rate_apr        = excluded.interest_rate_apr,
                monthly_payment_estimate = excluded.monthly_payment_estimate,
                escrow_monthly           = excluded.escrow_monthly,
                property_tax_monthly     = excluded.property_tax_monthly,
                insurance_monthly        = excluded.insurance_monthly,
                liability_account_path   = excluded.liability_account_path,
                interest_account_path    = excluded.interest_account_path,
                escrow_account_path      = excluded.escrow_account_path,
                simplefin_account_id     = excluded.simplefin_account_id,
                property_slug            = excluded.property_slug,
                payoff_date              = excluded.payoff_date,
                payoff_amount            = excluded.payoff_amount,
                is_active                = excluded.is_active,
                is_revolving             = excluded.is_revolving,
                credit_limit             = excluded.credit_limit,
                notes                    = excluded.notes
            """,
            (
                row["slug"], row["display_name"], row["loan_type"],
                row["entity_slug"], row["institution"],
                row["original_principal"], row["funded_date"],
                row["first_payment_date"], row["payment_due_day"],
                row["term_months"], row["interest_rate_apr"],
                row["monthly_payment_estimate"], row["escrow_monthly"],
                row["property_tax_monthly"], row["insurance_monthly"],
                row["liability_account_path"], row["interest_account_path"],
                row["escrow_account_path"], row["simplefin_account_id"],
                row["property_slug"], row["payoff_date"], row["payoff_amount"],
                is_active, is_revolving, row.get("credit_limit"), row["notes"],
            ),
        )
        written += 1

    anchors = read_loan_balance_anchors(entries)
    for row in anchors:
        conn.execute(
            """
            INSERT INTO loan_balance_anchors
                (loan_slug, as_of_date, balance, source, notes)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (loan_slug, as_of_date) DO UPDATE SET
                balance = excluded.balance,
                source  = excluded.source,
                notes   = excluded.notes
            """,
            (row["loan_slug"], row["as_of_date"], row["balance"],
             row["source"], row["notes"]),
        )
        written += 1

    if written:
        notes.append(f"rebuilt {written} loan rows")
    return ReconstructReport(
        pass_name="step9:loans", rows_written=written, notes=notes,
    )

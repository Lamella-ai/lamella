# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0063: Scorer must be direction-invariant.

Scoring (doc, txn) returns the same total as scoring (txn, doc)
with the corresponding fields swapped. Any asymmetry would mean
the forward and reverse auto-link paths disagree on whether a
pair should auto-link — exactly the failure mode this ADR exists
to prevent.
"""
from __future__ import annotations

from datetime import date

import pytest

from lamella.features.receipts.scorer import Scorer


@pytest.fixture
def scorer() -> Scorer:
    return Scorer()


# (label, doc-side kwargs, txn-side kwargs)
PAIRS = [
    (
        "exact_total_same_day_receipt",
        dict(
            doc_date=date(2026, 4, 17),
            doc_total_cents=4217,
            doc_currency="USD",
            doc_vendor="Hardware Store",
            doc_doctype="receipt",
            doc_correspondent="Hardware Store",
            doc_content_excerpt="42.17 thanks",
            doc_last_four="1234",
        ),
        dict(
            txn_date=date(2026, 4, 17),
            txn_amount_cents=4217,
            txn_currency="USD",
            txn_payee="Hardware Store",
            txn_description="HARDWARE STORE 1234",
            txn_last_four="1234",
        ),
    ),
    (
        "exact_total_3d_off_receipt",
        dict(
            doc_date=date(2026, 4, 14),
            doc_total_cents=12000,
            doc_currency="USD",
            doc_vendor="Coffee Shop",
            doc_doctype="receipt",
            doc_correspondent="Coffee Shop",
            doc_content_excerpt=None,
            doc_last_four=None,
        ),
        dict(
            txn_date=date(2026, 4, 17),
            txn_amount_cents=12000,
            txn_currency="USD",
            txn_payee="Coffee Shop",
            txn_description="COFFEE SHOP",
            txn_last_four=None,
        ),
    ),
    (
        "subtotal_match_tight_date",
        dict(
            doc_date=date(2026, 4, 16),
            doc_total_cents=5500,
            doc_subtotal_cents=5000,
            doc_currency="USD",
            doc_vendor="Restaurant",
            doc_doctype="receipt",
            doc_correspondent="Restaurant",
            doc_content_excerpt=None,
        ),
        dict(
            txn_date=date(2026, 4, 17),
            txn_amount_cents=5000,
            txn_currency="USD",
            txn_payee="Restaurant",
            txn_description=None,
        ),
    ),
    (
        "currency_mismatch",
        dict(
            doc_date=date(2026, 4, 17),
            doc_total_cents=5000,
            doc_currency="EUR",
            doc_vendor="EuroVendor",
            doc_doctype="receipt",
            doc_correspondent=None,
            doc_content_excerpt=None,
        ),
        dict(
            txn_date=date(2026, 4, 17),
            txn_amount_cents=5000,
            txn_currency="USD",
            txn_payee="EuroVendor",
            txn_description=None,
        ),
    ),
    (
        "statement_excluded",
        dict(
            doc_date=date(2026, 4, 17),
            doc_total_cents=10000,
            doc_currency="USD",
            doc_vendor="Bank",
            doc_doctype="statement",
            doc_correspondent="Bank",
            doc_content_excerpt=None,
        ),
        dict(
            txn_date=date(2026, 4, 17),
            txn_amount_cents=10000,
            txn_currency="USD",
            txn_payee="Bank",
            txn_description=None,
        ),
    ),
    (
        "fuzzy_amount_within_tolerance",
        dict(
            doc_date=date(2026, 4, 17),
            doc_total_cents=12345,
            doc_currency="USD",
            doc_vendor="Vendor X",
            doc_doctype="receipt",
            doc_correspondent="Vendor X",
            doc_content_excerpt=None,
        ),
        dict(
            txn_date=date(2026, 4, 17),
            txn_amount_cents=12300,  # 45c off
            txn_currency="USD",
            txn_payee="Vendor X",
            txn_description=None,
        ),
    ),
]


@pytest.mark.parametrize("label,doc_kwargs,txn_kwargs", PAIRS)
def test_score_is_direction_invariant(scorer, label, doc_kwargs, txn_kwargs):
    """Forward (doc, txn) and reverse (txn, doc) must yield the same total."""
    forward = scorer.score(**doc_kwargs, **txn_kwargs)

    # Build the reverse call by swapping each pairwise field.
    reverse = scorer.score(
        doc_date=txn_kwargs["txn_date"],
        doc_total_cents=txn_kwargs["txn_amount_cents"],
        doc_currency=txn_kwargs["txn_currency"],
        doc_vendor=txn_kwargs["txn_payee"],
        # On reverse, the "doc" side now plays the txn's role; we keep
        # the original doc's doctype because the doctype always
        # belongs to the document, not the transaction. (The txn has
        # no doctype field — it's not a document.) This matches the
        # production call sites: find_ledger_candidates always carries
        # the real doc's doctype.
        doc_doctype=doc_kwargs["doc_doctype"],
        doc_correspondent=txn_kwargs["txn_payee"],
        doc_content_excerpt=txn_kwargs["txn_description"],
        doc_last_four=txn_kwargs.get("txn_last_four"),
        doc_subtotal_cents=None,  # txn has no subtotal concept
        txn_date=doc_kwargs["doc_date"],
        txn_amount_cents=doc_kwargs["doc_total_cents"],
        txn_currency=doc_kwargs["doc_currency"],
        txn_payee=doc_kwargs["doc_vendor"],
        txn_description=doc_kwargs.get("doc_content_excerpt"),
        txn_last_four=doc_kwargs.get("doc_last_four"),
    )

    # Direction invariance: same total, same verdict.
    assert forward.total == reverse.total, (
        f"{label}: forward={forward.total} reverse={reverse.total} "
        f"({forward.reasons} vs {reverse.reasons})"
    )
    assert forward.verdict == reverse.verdict


def test_score_is_invariant_under_repeated_calls(scorer):
    """Stateless: the same inputs always produce the same output."""
    kwargs = dict(
        doc_date=date(2026, 4, 17),
        doc_total_cents=4217,
        doc_currency="USD",
        doc_vendor="Hardware Store",
        doc_doctype="receipt",
        doc_correspondent="Hardware Store",
        doc_content_excerpt=None,
        doc_last_four="1234",
        txn_date=date(2026, 4, 17),
        txn_amount_cents=4217,
        txn_currency="USD",
        txn_payee="Hardware Store",
        txn_description="HARDWARE STORE 1234",
        txn_last_four="1234",
    )
    a = scorer.score(**kwargs)
    b = scorer.score(**kwargs)
    c = scorer.score(**kwargs)
    assert a.total == b.total == c.total
    assert a.reasons == b.reasons == c.reasons

# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from lamella.features.import_.classify import (
    classify_source,
    is_generic,
    KNOWN_SOURCE_CLASSES,
)


def test_classify_wf_annotated_by_columns():
    cols = [
        "Master Category", "Subcategory", "Date", "Location", "Payee",
        "Description", "Payment Method", "Amount",
        "Business Expense?", "Business", "Expense Category",
        "Amount.1", "Expense Memo",
    ]
    sc, stype, entity, _ = classify_source(
        "Bank One 2024.ods", "Sheet1", cols, row_count=500
    )
    assert sc == "wf_annotated"
    assert stype == "primary"


def test_classify_paypal_filename():
    sc, stype, entity, _ = classify_source(
        "Acme PayPal Transactions 2024.ods",
        "All Transactions",
        ["Date", "Name", "Type", "Status", "Gross", "Transaction ID"],
        row_count=1200,
    )
    assert sc == "paypal"
    assert entity == "Acme"


def test_classify_routes_unknown_csv_to_generic():
    sc, stype, entity, _ = classify_source(
        "weird_custom_bank_export.csv",
        "(csv)",
        ["Trans Date", "Amt", "Description"],
        row_count=50,
    )
    assert sc == "generic_csv"
    assert is_generic(sc)


def test_classify_routes_unknown_xlsx_to_generic():
    sc, _, _, _ = classify_source(
        "mystery.xlsx", "Sheet1", ["Date", "Value", "Note"], row_count=30
    )
    assert sc == "generic_xlsx"


def test_classify_eidl_is_counterparty():
    sc, stype, _, _ = classify_source(
        "SBA EIDL Schedule 2024.ods", "Sheet1", ["Date", "Amount"], row_count=36
    )
    assert sc == "eidl"
    assert stype == "counterparty"


def test_pivot_sheet_dropped():
    sc, stype, _, notes_str = classify_source(
        "Bank One 2024.ods",
        "Pivot Table 1",
        ["Business Expense?", "Business", "Sum"],
        row_count=10,
    )
    assert stype == "pivot"
    assert "pivot" in notes_str.lower()


def test_known_source_classes_exhaustive():
    # Sanity: migration schema and classifier agree on the main names.
    expected = {
        "wf_annotated", "paypal", "amazon_seller", "amazon_merch",
        "amazon_purchases", "costco_citibank", "amex", "chase",
        "eidl", "check_writing", "ebay", "generic_csv", "generic_xlsx",
    }
    assert expected.issubset(set(KNOWN_SOURCE_CLASSES))

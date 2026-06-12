# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Heuristic classifier for an uploaded sheet.

Adapted from importer_bundle/importers/classify_sheet.py. New source_class
values (`generic_csv`, `generic_xlsx`) route to the AI-column-mapping flow
when no known ingester signature matches. Entity detection now accepts the
upload's filename since there is no folder hierarchy on an ad-hoc upload.

source_class values:
    wf_annotated       13-col Bank One "Master Category / Business Expense?" format
    paypal             PayPal transaction export
    amazon_seller      Amazon Seller Central annual summary
    amazon_merch       Merch by Amazon royalties
    amazon_purchases   Amazon order history (own purchases)
    costco_citibank    Warehouse Club/Citibank credit card statement dump
    amex               American Express statement dump
    chase              Chase credit card
    eidl               SBA EIDL payment schedule
    check_writing      WF check register
    ebay               eBay Seller Hub report
    annual_form        1099/1098/W-2 form data
    inventory          Year-end inventory counts
    master_expense     Master Expenses pivot/roll-up
    profit_loss        P&L summary
    summary            Generic roll-up sheet
    generic_csv        Unknown CSV — routes to AI column mapping
    generic_xlsx       Unknown XLSX — routes to AI column mapping
    other              Unknown; leave for manual review
"""
from __future__ import annotations

import re
from typing import Tuple

_FILENAME_RULES: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\bmerch\b.*sales", re.I),        "amazon_merch"),
    (re.compile(r"amazon.*merch", re.I),           "amazon_merch"),
    (re.compile(r"amazon.*inventory", re.I),       "inventory"),
    (re.compile(r"amazon.*orders?.*purchase", re.I), "amazon_purchases"),
    (re.compile(r"amzon\s*orders", re.I),          "amazon_purchases"),
    (re.compile(r"amazon\s*orders\b", re.I),       "amazon_purchases"),
    (re.compile(r"amazon\s*sales", re.I),          "amazon_seller"),
    (re.compile(r"\bamazon\b.*(?:llc|transactions?)", re.I), "amazon_seller"),
    (re.compile(r"^amazon\b", re.I),               "amazon_seller"),
    (re.compile(r"\bcostco\b.*citi", re.I),        "costco_citibank"),
    (re.compile(r"\bcostco\b", re.I),              "costco_citibank"),
    (re.compile(r"\bciti\s*bank\b", re.I),         "costco_citibank"),
    (re.compile(r"american\s*express", re.I),      "amex"),
    (re.compile(r"\bamex\b", re.I),                "amex"),
    (re.compile(r"\bamiercan", re.I),              "amex"),
    (re.compile(r"\bchase\b", re.I),               "chase"),
    (re.compile(r"\bpaypal\b", re.I),              "paypal"),
    (re.compile(r"\be[-\s]?bay\b", re.I),          "ebay"),
    (re.compile(r"\bsba\s*eidl\b", re.I),          "eidl"),
    (re.compile(r"\beidl\b", re.I),                "eidl"),
    (re.compile(r"check.*writing", re.I),          "check_writing"),
    (re.compile(r"\b(?:wells\s*fargo|wellsfargo|wf)\b", re.I), "wf_annotated"),
    (re.compile(r"master\s*expenses?", re.I),      "master_expense"),
    (re.compile(r"profit.*loss", re.I),            "profit_loss"),
    (re.compile(r"\bp\s*&\s*l\b", re.I),           "profit_loss"),
    (re.compile(r"inventory", re.I),               "inventory"),
    (re.compile(r"1099[-\s]?nec", re.I),           "annual_form"),
    (re.compile(r"1099[-\s]?k", re.I),             "annual_form"),
    (re.compile(r"1099", re.I),                    "annual_form"),
    (re.compile(r"1098", re.I),                    "annual_form"),
    (re.compile(r"\bw[-\s]?2\b", re.I),            "annual_form"),
]

_ENTITY_RULES: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"acme\s*llc|acme", re.I),         "Acme"),
    (re.compile(r"cnc\s*xyz", re.I),                   "WidgetCo"),
    (re.compile(r"rent\s*quicks", re.I),               "Rentals"),
    (re.compile(r"farm\s*co", re.I),               "FarmCo"),
    (re.compile(r"quick\s*plan\s*review", re.I),       "Consulting"),
    (re.compile(r"right\s*generator", re.I),           "ThetaCo"),
    (re.compile(r"\bpersonal\b", re.I),                "Personal"),
]


KNOWN_SOURCE_CLASSES: tuple[str, ...] = (
    "wf_annotated", "paypal", "amazon_seller", "amazon_merch",
    "amazon_purchases", "costco_citibank", "amex", "chase", "eidl",
    "check_writing", "ebay", "annual_form", "inventory",
    "master_expense", "profit_loss", "summary",
    "generic_csv", "generic_xlsx", "other",
)


def classify_source(
    rel_path: str,
    sheet_name: str,
    columns: list[str] | None = None,
    row_count: int = 0,
) -> Tuple[str, str, str | None, str]:
    """Return (source_class, sheet_type, entity, notes).

    `rel_path` is either a relative path (folder hierarchy preserved) or the
    bare filename of the upload.
    """
    path_low = rel_path.lower()
    sheet_low = (sheet_name or "").lower()
    notes_parts: list[str] = []

    source_class = "other"
    for pat, cls in _FILENAME_RULES:
        if pat.search(path_low):
            source_class = cls
            break

    entity: str | None = None
    for pat, ent in _ENTITY_RULES:
        if pat.search(path_low):
            entity = ent
            break

    cols_lower = [str(c).lower() for c in (columns or [])]
    col_set = set(cols_lower)
    sheet_type = "primary"

    if "pivot" in sheet_low or sheet_low.startswith("pivot"):
        sheet_type = "pivot"
        notes_parts.append("pivot sheet")
    elif re.search(r"^(p&l|profit\s*loss|summary|roll[-\s]?up)\b", sheet_low):
        sheet_type = "summary"
    elif re.search(r"\bstatement\b|stmt", sheet_low):
        sheet_type = "statement"
    elif (
        re.search(r"\bexpens(?:es|e only)\b|\bincome\b|\brefund\b|\bfee\b", sheet_low)
        and "business expense?" not in col_set
    ):
        sheet_type = "filtered"
        notes_parts.append("filtered view of primary")

    # Annotated 13-col format marker
    if {"business expense?", "business", "expense category"}.issubset(col_set):
        if source_class in ("other", "master_expense", "generic_csv", "generic_xlsx"):
            source_class = "wf_annotated"
        if sheet_type == "primary" and re.search(
            r"filtered|subset|only|just", sheet_low
        ):
            sheet_type = "filtered"

    if source_class == "eidl":
        sheet_type = "counterparty"

    if source_class in ("master_expense", "profit_loss", "summary"):
        sheet_type = "summary"

    if source_class == "inventory":
        sheet_type = "form"

    if source_class == "annual_form":
        sheet_type = "form"

    # Route genuinely-unknown tabular content to the AI column mapper.
    if source_class == "other" and row_count >= 2 and len(cols_lower) >= 2:
        ext = ""
        if "." in path_low:
            ext = path_low.rsplit(".", 1)[-1]
        if ext == "csv":
            source_class = "generic_csv"
        elif ext in ("xlsx", "xls", "ods"):
            source_class = "generic_xlsx"

    return source_class, sheet_type, entity, "; ".join(notes_parts)


def is_generic(source_class: str) -> bool:
    return source_class in ("generic_csv", "generic_xlsx")

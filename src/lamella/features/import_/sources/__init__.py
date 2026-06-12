# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Per-source ingesters. Each module exposes:

    ingest_sheet(conn: sqlite3.Connection,
                 source_id: int,
                 path: Path,
                 sheet_name: str,
                 *,
                 column_map: dict | None = None) -> int

Returns the number of raw_rows inserted. `column_map` is None for known
source classes (wf, paypal, etc.) and carries the user-confirmed mapping
for `generic_csv` / `generic_xlsx`.
"""
from __future__ import annotations

from lamella.features.import_.sources import (
    amazon_merch,
    amazon_purchases,
    amazon_seller,
    cards,
    chase,
    ebay,
    eidl,
    generic,
    paypal,
    wf,
)

INGESTERS = {
    "wf_annotated": wf.ingest_sheet,
    "paypal": paypal.ingest_sheet,
    "amazon_seller": amazon_seller.ingest_sheet,
    "amazon_merch": amazon_merch.ingest_sheet,
    "amazon_purchases": amazon_purchases.ingest_sheet,
    "amex": cards.ingest_sheet_amex,
    "costco_citibank": cards.ingest_sheet_costco,
    "chase": chase.ingest_sheet,
    "ebay": ebay.ingest_sheet,
    "eidl": eidl.ingest_sheet,
    "generic_csv": generic.ingest_sheet,
    "generic_xlsx": generic.ingest_sheet,
}


def for_source_class(source_class: str):
    return INGESTERS.get(source_class)

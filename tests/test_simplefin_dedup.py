# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from pathlib import Path

from lamella.core.beancount_io import LedgerReader
from lamella.features.bank_sync.dedup import build_index


def test_build_index_finds_simplefin_ids_in_fixture_ledger(ledger_dir: Path):
    reader = LedgerReader(ledger_dir / "main.bean")
    ledger = reader.load()
    index = build_index(ledger.entries)

    # The fixture ledger contains sf-1001, sf-1002, sf-1003 on its seeded
    # Hardware Store / USPS / Grocery Store rows.
    assert {"sf-1001", "sf-1002", "sf-1003"} <= index


def test_build_index_is_empty_for_ledger_without_simplefin(tmp_path: Path):
    bean = tmp_path / "main.bean"
    bean.write_text(
        'option "title" "no-sf"\n'
        'option "operating_currency" "USD"\n\n'
        "2024-01-01 open Assets:Cash USD\n"
        "2024-01-01 open Equity:Opening USD\n\n"
        '2024-01-02 * "Opener"\n'
        "  Assets:Cash   100.00 USD\n"
        "  Equity:Opening -100.00 USD\n",
        encoding="utf-8",
    )
    reader = LedgerReader(bean)
    assert build_index(reader.load().entries) == set()

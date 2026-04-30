# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for ledger-history rule mining — NEXTGEN.md Phase E3."""
from __future__ import annotations

from pathlib import Path

import pytest

from lamella.core.beancount_io.reader import LedgerReader
from lamella.features.import_.staging import mine_rules


def _ledger(dir_: Path, body: str) -> Path:
    main = dir_ / "main.bean"
    main.write_text(
        'option "operating_currency" "USD"\n'
        "2020-01-01 open Assets:Bank USD\n"
        "2020-01-01 open Expenses:Groceries USD\n"
        "2020-01-01 open Expenses:FastFood USD\n"
        "2020-01-01 open Income:Work USD\n"
        + body,
        encoding="utf-8",
    )
    return main


def test_empty_ledger_produces_no_rules(tmp_path: Path):
    reader = LedgerReader(_ledger(tmp_path, ""))
    assert mine_rules(reader) == []


def test_dominant_pattern_produces_rule(tmp_path: Path):
    body = ""
    # Whole Foods → Groceries, 6 times.
    for i in range(6):
        body += (
            f'2026-0{(i % 9) + 1}-{((i * 3) % 28) + 1:02d} * "Whole Foods"\n'
            "  Assets:Bank         -42.00 USD\n"
            "  Expenses:Groceries   42.00 USD\n\n"
        )
    reader = LedgerReader(_ledger(tmp_path, body))
    rules = mine_rules(reader, min_support=5)
    assert len(rules) == 1
    assert rules[0].normalized_payee == "whole foods"
    assert rules[0].proposed_account == "Expenses:Groceries"
    assert rules[0].support == 6
    assert rules[0].confidence == 1.0


def test_below_support_threshold_is_dropped(tmp_path: Path):
    body = (
        '2026-04-20 * "Whole Foods"\n'
        "  Assets:Bank         -42.00 USD\n"
        "  Expenses:Groceries   42.00 USD\n"
    )
    reader = LedgerReader(_ledger(tmp_path, body))
    assert mine_rules(reader, min_support=5) == []


def test_low_confidence_is_dropped(tmp_path: Path):
    """Fast Food posted sometimes to FastFood, sometimes to Groceries
    (business card trip). Neither account dominates, so no rule."""
    body = ""
    for i in range(3):
        body += (
            f'2026-0{i + 1}-10 * "Fast Food"\n'
            "  Assets:Bank        -10.00 USD\n"
            "  Expenses:FastFood   10.00 USD\n\n"
        )
    for i in range(3):
        body += (
            f'2026-0{i + 4}-10 * "Fast Food"\n'
            "  Assets:Bank          -10.00 USD\n"
            "  Expenses:Groceries    10.00 USD\n\n"
        )
    reader = LedgerReader(_ledger(tmp_path, body))
    # With min_confidence=0.6 and a 3/3 split, no rule emerges.
    assert mine_rules(reader, min_support=5, min_confidence=0.6) == []


def test_alternatives_include_runner_ups(tmp_path: Path):
    """When the dominant account has 4/5 and a secondary has 1/5,
    the secondary shows up in alternatives so the user sees the
    directional-not-rigid aspect of the prior."""
    body = ""
    for _ in range(4):
        body += (
            '2026-04-10 * "Amazon"\n'
            "  Assets:Bank         -12.00 USD\n"
            "  Expenses:Groceries   12.00 USD\n\n"
        )
    body += (
        '2026-04-15 * "Amazon"\n'
        "  Assets:Bank         -12.00 USD\n"
        "  Expenses:FastFood    12.00 USD\n\n"
    )
    reader = LedgerReader(_ledger(tmp_path, body))
    rules = mine_rules(reader, min_support=5)
    assert len(rules) == 1
    assert rules[0].proposed_account == "Expenses:Groceries"
    assert rules[0].support == 4
    assert rules[0].confidence == 0.8
    assert rules[0].alternatives
    assert rules[0].alternatives[0] == ("Expenses:FastFood", 1)


def test_asset_liability_sides_not_proposed_as_rule_targets(
    tmp_path: Path,
):
    """Rule targets should be classification accounts (Expenses,
    Income, Equity), not the source side (Assets, Liabilities).
    The card/account itself is a deterministic binding, not a
    directional rule."""
    body = ""
    for _ in range(6):
        body += (
            '2026-04-20 * "Whole Foods"\n'
            "  Assets:Bank         -42.00 USD\n"
            "  Expenses:Groceries   42.00 USD\n\n"
        )
    reader = LedgerReader(_ledger(tmp_path, body))
    rules = mine_rules(reader, min_support=5)
    assert len(rules) == 1
    # The proposed account must be the Expenses side, not Assets:Bank.
    assert rules[0].proposed_account.startswith("Expenses:")

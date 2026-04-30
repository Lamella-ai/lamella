# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Regression test for the card_undo merchant_memory decrement.

The original code at ``routes/card.py`` decremented ``use_count`` on
``merchant_memory`` filtered only by ``target_account`` — so undoing
one categorize action stomped on every merchant that had ever pointed
at the same account, across every entity. The decrement must scope
to the specific (merchant_key, target_account) row that was bumped.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from lamella.core.db import connect, migrate
from lamella.core.registry.service import bump_merchant_memory, decrement_merchant_memory


@pytest.fixture()
def conn():
    c = connect(Path(":memory:"))
    migrate(c)
    return c


def test_decrement_only_touches_matching_merchant_key(conn):
    # Two entities both categorize purchases at the same shared
    # account (e.g. a generic bank-fee account). Different merchants
    # → two distinct merchant_memory rows under the schema PK
    # (merchant_key, target_account).
    bump_merchant_memory(
        conn, merchant_key="acme-vendor", target_account="Expenses:Bank:Fees",
        entity_slug="acme",
    )
    bump_merchant_memory(
        conn, merchant_key="acme-vendor", target_account="Expenses:Bank:Fees",
        entity_slug="acme",
    )
    bump_merchant_memory(
        conn, merchant_key="other-vendor", target_account="Expenses:Bank:Fees",
        entity_slug="personal",
    )

    # Decrementing one merchant's bump must NOT touch the other row.
    decrement_merchant_memory(
        conn, merchant_key="acme-vendor", target_account="Expenses:Bank:Fees",
    )

    rows = {
        (r["merchant_key"], r["target_account"]): r["use_count"]
        for r in conn.execute(
            "SELECT merchant_key, target_account, use_count FROM merchant_memory"
        )
    }
    assert rows[("acme-vendor", "Expenses:Bank:Fees")] == 1
    assert rows[("other-vendor", "Expenses:Bank:Fees")] == 1


def test_decrement_floor_zero(conn):
    bump_merchant_memory(
        conn, merchant_key="seen-once", target_account="Expenses:X",
        entity_slug="acme",
    )
    decrement_merchant_memory(
        conn, merchant_key="seen-once", target_account="Expenses:X",
    )
    decrement_merchant_memory(  # second decrement must not go negative
        conn, merchant_key="seen-once", target_account="Expenses:X",
    )
    row = conn.execute(
        "SELECT use_count FROM merchant_memory "
        "WHERE merchant_key = ? AND target_account = ?",
        ("seen-once", "Expenses:X"),
    ).fetchone()
    assert row["use_count"] == 0


def test_decrement_missing_row_is_noop(conn):
    # Pre-helper undo paths could be triggered against a merchant_key
    # that no longer has a row (e.g. a long-old undo). It must not
    # raise or affect unrelated rows.
    bump_merchant_memory(
        conn, merchant_key="present", target_account="Expenses:Y",
        entity_slug="acme",
    )
    decrement_merchant_memory(
        conn, merchant_key="missing", target_account="Expenses:Z",
    )
    row = conn.execute(
        "SELECT use_count FROM merchant_memory "
        "WHERE merchant_key = ? AND target_account = ?",
        ("present", "Expenses:Y"),
    ).fetchone()
    assert row["use_count"] == 1

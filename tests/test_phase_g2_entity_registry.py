# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for Phase G2 — card registry as entity source of truth."""
from __future__ import annotations

from pathlib import Path

import pytest

from lamella.features.ai_cascade.context import (
    entity_from_card,
    resolve_entity_for_account,
)
from lamella.core.db import connect, migrate


@pytest.fixture()
def conn():
    c = connect(Path(":memory:"))
    migrate(c)
    return c


def _seed_entity(conn, slug):
    conn.execute(
        "INSERT OR IGNORE INTO entities (slug, display_name) VALUES (?, ?)",
        (slug, slug),
    )


def _seed_card(conn, account_path, entity_slug):
    _seed_entity(conn, entity_slug)
    conn.execute(
        "INSERT OR REPLACE INTO accounts_meta "
        "(account_path, display_name, entity_slug) VALUES (?, ?, ?)",
        (account_path, account_path, entity_slug),
    )


class TestResolveEntity:
    def test_heuristic_only_when_no_conn(self):
        assert entity_from_card("Liabilities:Acme:Card:0123") == "Acme"
        assert resolve_entity_for_account(None, "Liabilities:Acme:Card:0123") == "Acme"

    def test_registry_match_returns_registry(self, conn):
        _seed_card(conn, "Liabilities:Acme:Card:0123", "Acme")
        assert resolve_entity_for_account(conn, "Liabilities:Acme:Card:0123") == "Acme"

    def test_registry_wins_on_disagreement(self, conn):
        """If the registry explicitly says a card belongs to another
        entity — e.g. the account path was renamed but the registry
        wasn't updated, or vice versa — the registry is authoritative
        and logs a warning. This is how the card → entity binding
        stops being load-bearing on the string split."""
        _seed_card(conn, "Liabilities:Acme:Card:0123", "WidgetCo")
        assert resolve_entity_for_account(conn, "Liabilities:Acme:Card:0123") == "WidgetCo"

    def test_no_registry_row_falls_back_to_heuristic(self, conn):
        # No row in accounts_meta for this path.
        assert resolve_entity_for_account(
            conn, "Liabilities:Personal:Card:9999"
        ) == "Personal"

    def test_null_registry_slug_falls_back(self, conn):
        conn.execute(
            "INSERT INTO accounts_meta "
            "(account_path, display_name, entity_slug) VALUES (?, ?, NULL)",
            ("Liabilities:Personal:Card:9999", "card"),
        )
        assert resolve_entity_for_account(
            conn, "Liabilities:Personal:Card:9999"
        ) == "Personal"

    def test_null_card_returns_none(self, conn):
        assert resolve_entity_for_account(conn, None) is None
        assert resolve_entity_for_account(None, None) is None

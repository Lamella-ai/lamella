# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0065 — binding_loader: list_active_bindings, list_known_actions."""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

import pytest

from lamella.features.paperless_bridge.binding_loader import (
    ActionMeta,
    BindingRow,
    list_active_bindings,
    list_all_bindings,
    list_known_actions,
)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS tag_workflow_bindings (
    tag_name      TEXT PRIMARY KEY,
    action_name   TEXT NOT NULL,
    enabled       INTEGER NOT NULL DEFAULT 1,
    config_json   TEXT NOT NULL DEFAULT '',
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);
"""


@pytest.fixture
def conn() -> sqlite3.Connection:
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.executescript(_SCHEMA)
    return c


def _insert(conn, tag_name, action_name, enabled=1, config_json=""):
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    conn.execute(
        "INSERT INTO tag_workflow_bindings "
        "(tag_name, action_name, enabled, config_json, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (tag_name, action_name, enabled, config_json, now, now),
    )
    conn.commit()


def test_list_active_bindings_empty(conn):
    """Empty table → empty list."""
    assert list_active_bindings(conn) == []


def test_list_active_bindings_returns_enabled_rows(conn):
    """Only enabled=1 rows are returned."""
    _insert(conn, "TagA", "extract_fields", enabled=1)
    _insert(conn, "TagB", "date_sanity_check", enabled=0)
    rows = list_active_bindings(conn)
    assert len(rows) == 1
    assert rows[0].tag_name == "TagA"


def test_list_active_bindings_row_shape(conn):
    """Returned BindingRow has all expected fields."""
    _insert(conn, "Lamella:Process", "extract_fields", enabled=1)
    rows = list_active_bindings(conn)
    assert len(rows) == 1
    row = rows[0]
    assert isinstance(row, BindingRow)
    assert row.tag_name == "Lamella:Process"
    assert row.action_name == "extract_fields"
    assert row.enabled is True
    assert isinstance(row.config_json, str)
    assert isinstance(row.created_at, str)


def test_list_all_bindings_includes_disabled(conn):
    """list_all_bindings returns both enabled and disabled."""
    _insert(conn, "TagA", "extract_fields", enabled=1)
    _insert(conn, "TagB", "date_sanity_check", enabled=0)
    rows = list_all_bindings(conn)
    assert len(rows) == 2
    enabled_states = {r.tag_name: r.enabled for r in rows}
    assert enabled_states["TagA"] is True
    assert enabled_states["TagB"] is False


def test_list_known_actions_returns_registered_actions():
    """list_known_actions returns the registered actions. v1 shipped
    three (extract_fields, date_sanity_check, link_to_ledger);
    verify_date_only was added later as the cheap-path date-only
    re-extract bound to Lamella:DateAnomaly."""
    actions = list_known_actions()
    names = {a.name for a in actions}
    assert names == {
        "extract_fields", "date_sanity_check", "link_to_ledger",
        "verify_date_only",
    }


def test_list_known_actions_shape():
    """Each ActionMeta has required fields."""
    actions = list_known_actions()
    for action in actions:
        assert isinstance(action, ActionMeta)
        assert action.name
        assert action.display_label
        assert action.description
        assert isinstance(action.default_config_json, str)
        # completion_tag is optional — verify_date_only sets it to
        # None because its only on-success op is removing the
        # trigger tag. Other actions keep it set so a state tag is
        # added on top.
        assert action.completion_tag is None or isinstance(
            action.completion_tag, str,
        )


def test_list_known_actions_extract_fields_completion_tag():
    """extract_fields action has Lamella:Extracted as completion tag."""
    from lamella.features.paperless_bridge.lamella_namespace import TAG_EXTRACTED
    actions = {a.name: a for a in list_known_actions()}
    assert actions["extract_fields"].completion_tag == TAG_EXTRACTED


def test_list_known_actions_date_sanity_check_completion_tag():
    """date_sanity_check action has Lamella:DateAnomaly as completion tag."""
    from lamella.features.paperless_bridge.lamella_namespace import TAG_DATE_ANOMALY
    actions = {a.name: a for a in list_known_actions()}
    assert actions["date_sanity_check"].completion_tag == TAG_DATE_ANOMALY


def test_list_known_actions_link_to_ledger_completion_tag():
    """link_to_ledger action has Lamella:Linked as completion tag."""
    from lamella.features.paperless_bridge.lamella_namespace import TAG_LINKED
    actions = {a.name: a for a in list_known_actions()}
    assert actions["link_to_ledger"].completion_tag == TAG_LINKED


def test_list_active_bindings_tolerates_missing_table():
    """list_active_bindings returns [] when the table doesn't exist."""
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    # No schema created — table does not exist.
    result = list_active_bindings(c)
    assert result == []

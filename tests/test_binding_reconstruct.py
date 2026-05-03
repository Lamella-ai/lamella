# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0065 / ADR-0015 — tag-binding reconstruct step.

Scenario: write 3 bindings + 1 revoke to a fresh ledger, delete DB,
run reconstruct step26, assert 2 active bindings restored.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from beancount import loader

from lamella.features.paperless_bridge.binding_writer import (
    append_binding,
    append_binding_revoke,
)
from lamella.core.transform.steps.step26_tag_bindings import (
    reconstruct_tag_bindings,
)


_MAIN_PRELUDE = (
    'option "title" "Test"\n'
    'option "operating_currency" "USD"\n'
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
def ledger(tmp_path: Path) -> Path:
    main = tmp_path / "main.bean"
    main.write_text(_MAIN_PRELUDE, encoding="utf-8")
    return tmp_path


@pytest.fixture
def conn() -> sqlite3.Connection:
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.executescript(_SCHEMA)
    return c


def _load(tmp_path: Path):
    entries, errors, _ = loader.load_file(str(tmp_path / "main.bean"))
    return entries


def test_reconstruct_restores_active_bindings(ledger: Path, conn):
    """Write 3 bindings + 1 revoke; reconstruct yields 2 active rows."""
    config = ledger / "connector_config.bean"
    main = ledger / "main.bean"

    append_binding(
        connector_config=config, main_bean=main,
        tag_name="TagA", action_name="extract_fields", run_check=False,
    )
    append_binding(
        connector_config=config, main_bean=main,
        tag_name="TagB", action_name="date_sanity_check", run_check=False,
    )
    append_binding(
        connector_config=config, main_bean=main,
        tag_name="TagC", action_name="link_to_ledger", run_check=False,
    )
    # Revoke TagC — should not appear after reconstruct
    append_binding_revoke(
        connector_config=config, main_bean=main,
        tag_name="TagC", run_check=False,
    )

    entries = _load(ledger)
    report = reconstruct_tag_bindings(conn, entries)

    assert report.rows_written == 2
    rows = conn.execute(
        "SELECT tag_name, action_name FROM tag_workflow_bindings ORDER BY tag_name"
    ).fetchall()
    assert len(rows) == 2
    assert rows[0]["tag_name"] == "TagA"
    assert rows[0]["action_name"] == "extract_fields"
    assert rows[1]["tag_name"] == "TagB"
    assert rows[1]["action_name"] == "date_sanity_check"


def test_reconstruct_empty_ledger(ledger: Path, conn):
    """Reconstruct on a ledger with no bindings yields empty table."""
    entries = _load(ledger)
    report = reconstruct_tag_bindings(conn, entries)

    assert report.rows_written == 0
    count = conn.execute(
        "SELECT COUNT(*) FROM tag_workflow_bindings"
    ).fetchone()[0]
    assert count == 0


def test_reconstruct_idempotent(ledger: Path, conn):
    """Running reconstruct twice on the same ledger yields the same state."""
    config = ledger / "connector_config.bean"
    main = ledger / "main.bean"
    append_binding(
        connector_config=config, main_bean=main,
        tag_name="TagA", action_name="extract_fields", run_check=False,
    )
    entries = _load(ledger)

    reconstruct_tag_bindings(conn, entries)
    report2 = reconstruct_tag_bindings(conn, entries)

    # Second run: the UPSERT replaces the row; rows_written still reflects upserts
    rows = conn.execute(
        "SELECT COUNT(*) FROM tag_workflow_bindings"
    ).fetchone()[0]
    assert rows == 1


def test_reconstruct_pass_name(ledger: Path, conn):
    """The report pass_name is 'step26:tag-bindings'."""
    entries = _load(ledger)
    report = reconstruct_tag_bindings(conn, entries)
    assert report.pass_name == "step26:tag-bindings"


def test_reconstruct_preserves_enabled_false(ledger: Path, conn):
    """A disabled binding is stored in the DB with enabled=0."""
    config = ledger / "connector_config.bean"
    main = ledger / "main.bean"
    append_binding(
        connector_config=config, main_bean=main,
        tag_name="TagA", action_name="extract_fields",
        enabled=False, run_check=False,
    )
    entries = _load(ledger)
    reconstruct_tag_bindings(conn, entries)

    row = conn.execute(
        "SELECT enabled FROM tag_workflow_bindings WHERE tag_name = 'TagA'"
    ).fetchone()
    assert row is not None
    assert row["enabled"] == 0

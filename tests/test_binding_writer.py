# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0065 — binding_writer: append, revoke, round-trip, last-write-wins,
revoke filtering, idempotent re-append.

All writes use ``run_check=False`` because tests run against a tmp dir
without a full beancount ledger to check against.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest
from beancount import loader

from lamella.features.paperless_bridge.binding_writer import (
    append_binding,
    append_binding_revoke,
    read_bindings_from_entries,
)


_MAIN_PRELUDE = (
    'option "title" "Test"\n'
    'option "operating_currency" "USD"\n'
)


@pytest.fixture
def ledger(tmp_path: Path):
    """A minimal ledger directory with a main.bean."""
    main = tmp_path / "main.bean"
    main.write_text(_MAIN_PRELUDE, encoding="utf-8")
    return tmp_path


def _load(tmp_path: Path):
    """Load main.bean and return entries."""
    main = tmp_path / "main.bean"
    entries, errors, _ = loader.load_file(str(main))
    return entries


def test_append_binding_creates_file(ledger: Path):
    """append_binding writes connector_config.bean if it does not exist."""
    config = ledger / "connector_config.bean"
    assert not config.exists()
    append_binding(
        connector_config=config,
        main_bean=ledger / "main.bean",
        tag_name="Lamella:Process",
        action_name="extract_fields",
        run_check=False,
    )
    assert config.exists()
    text = config.read_text(encoding="utf-8")
    assert "lamella-tag-binding" in text
    assert "Lamella:Process" in text
    assert "extract_fields" in text


def test_append_binding_round_trip(ledger: Path):
    """Write a binding, reload entries, read_bindings returns it."""
    config = ledger / "connector_config.bean"
    append_binding(
        connector_config=config,
        main_bean=ledger / "main.bean",
        tag_name="Todo",
        action_name="date_sanity_check",
        enabled=True,
        config_json="",
        run_check=False,
    )
    entries = _load(ledger)
    rows = read_bindings_from_entries(entries)
    assert len(rows) == 1
    row = rows[0]
    assert row["tag_name"] == "Todo"
    assert row["action_name"] == "date_sanity_check"
    assert row["enabled"] is True
    assert row["config_json"] == ""
    assert "created_at" in row


def test_append_revoke_removes_binding(ledger: Path):
    """After a binding is revoked, read_bindings returns empty."""
    config = ledger / "connector_config.bean"
    append_binding(
        connector_config=config,
        main_bean=ledger / "main.bean",
        tag_name="Lamella:Process",
        action_name="extract_fields",
        run_check=False,
    )
    append_binding_revoke(
        connector_config=config,
        main_bean=ledger / "main.bean",
        tag_name="Lamella:Process",
        run_check=False,
    )
    entries = _load(ledger)
    rows = read_bindings_from_entries(entries)
    assert rows == []


def test_last_write_wins_per_tag_name(ledger: Path):
    """A second append for the same tag_name supersedes the first."""
    config = ledger / "connector_config.bean"
    append_binding(
        connector_config=config,
        main_bean=ledger / "main.bean",
        tag_name="Lamella:Process",
        action_name="extract_fields",
        run_check=False,
    )
    # Change the action on the same trigger tag.
    append_binding(
        connector_config=config,
        main_bean=ledger / "main.bean",
        tag_name="Lamella:Process",
        action_name="link_to_ledger",
        run_check=False,
    )
    entries = _load(ledger)
    rows = read_bindings_from_entries(entries)
    # last-write-wins: only one row, new action
    assert len(rows) == 1
    assert rows[0]["action_name"] == "link_to_ledger"


def test_multiple_bindings_independent(ledger: Path):
    """Two bindings for different tag names coexist."""
    config = ledger / "connector_config.bean"
    append_binding(
        connector_config=config,
        main_bean=ledger / "main.bean",
        tag_name="TagA",
        action_name="extract_fields",
        run_check=False,
    )
    append_binding(
        connector_config=config,
        main_bean=ledger / "main.bean",
        tag_name="TagB",
        action_name="date_sanity_check",
        run_check=False,
    )
    entries = _load(ledger)
    rows = read_bindings_from_entries(entries)
    names = {r["tag_name"] for r in rows}
    assert names == {"TagA", "TagB"}


def test_revoke_one_of_two_bindings(ledger: Path):
    """Revoking one binding leaves the other active."""
    config = ledger / "connector_config.bean"
    append_binding(
        connector_config=config,
        main_bean=ledger / "main.bean",
        tag_name="TagA",
        action_name="extract_fields",
        run_check=False,
    )
    append_binding(
        connector_config=config,
        main_bean=ledger / "main.bean",
        tag_name="TagB",
        action_name="date_sanity_check",
        run_check=False,
    )
    append_binding_revoke(
        connector_config=config,
        main_bean=ledger / "main.bean",
        tag_name="TagA",
        run_check=False,
    )
    entries = _load(ledger)
    rows = read_bindings_from_entries(entries)
    assert len(rows) == 1
    assert rows[0]["tag_name"] == "TagB"


def test_re_bind_after_revoke(ledger: Path):
    """A re-bind after a revoke makes the binding active again."""
    config = ledger / "connector_config.bean"
    append_binding(
        connector_config=config,
        main_bean=ledger / "main.bean",
        tag_name="Lamella:Process",
        action_name="extract_fields",
        run_check=False,
    )
    append_binding_revoke(
        connector_config=config,
        main_bean=ledger / "main.bean",
        tag_name="Lamella:Process",
        run_check=False,
    )
    # Re-bind
    append_binding(
        connector_config=config,
        main_bean=ledger / "main.bean",
        tag_name="Lamella:Process",
        action_name="link_to_ledger",
        run_check=False,
    )
    entries = _load(ledger)
    rows = read_bindings_from_entries(entries)
    assert len(rows) == 1
    assert rows[0]["action_name"] == "link_to_ledger"


def test_disabled_binding_still_returned(ledger: Path):
    """An enabled=False binding is still returned by read_bindings_from_entries
    (the reader doesn't filter by enabled — that's the DB layer's job)."""
    config = ledger / "connector_config.bean"
    append_binding(
        connector_config=config,
        main_bean=ledger / "main.bean",
        tag_name="Todo",
        action_name="extract_fields",
        enabled=False,
        run_check=False,
    )
    entries = _load(ledger)
    rows = read_bindings_from_entries(entries)
    assert len(rows) == 1
    assert rows[0]["enabled"] is False


def test_append_binding_validates_empty_tag_name(ledger: Path):
    """append_binding raises ValueError on empty tag_name."""
    with pytest.raises(ValueError, match="tag_name"):
        append_binding(
            connector_config=ledger / "connector_config.bean",
            main_bean=ledger / "main.bean",
            tag_name="",
            action_name="extract_fields",
            run_check=False,
        )


def test_append_binding_validates_empty_action_name(ledger: Path):
    """append_binding raises ValueError on empty action_name."""
    with pytest.raises(ValueError, match="action_name"):
        append_binding(
            connector_config=ledger / "connector_config.bean",
            main_bean=ledger / "main.bean",
            tag_name="Todo",
            action_name="",
            run_check=False,
        )


def test_append_revoke_validates_empty_tag_name(ledger: Path):
    """append_binding_revoke raises ValueError on empty tag_name."""
    with pytest.raises(ValueError, match="tag_name"):
        append_binding_revoke(
            connector_config=ledger / "connector_config.bean",
            main_bean=ledger / "main.bean",
            tag_name="",
            run_check=False,
        )


def test_read_bindings_ignores_other_directives(ledger: Path):
    """read_bindings_from_entries ignores non-binding custom directives."""
    config = ledger / "connector_config.bean"
    # Write a non-binding directive manually
    config.write_text(
        '; Managed by Lamella. Do not hand-edit.\n'
        '2026-05-02 custom "some-other-directive" "foo"\n',
        encoding="utf-8",
    )
    main = ledger / "main.bean"
    existing = main.read_text(encoding="utf-8")
    main.write_text(
        existing + 'include "connector_config.bean"\n', encoding="utf-8"
    )
    entries = _load(ledger)
    rows = read_bindings_from_entries(entries)
    assert rows == []

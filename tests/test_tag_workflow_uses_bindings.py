# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0065 — tag_workflow.load_runtime_rules:

* Empty bindings → load_runtime_rules returns [] (scheduler is a no-op)
* Enabled binding → produces one WorkflowRule with correct selector
* Rule selector: must_have_tags=(tag_name,), must_not_have=(completion_tag,)
* Rule on_success: RemoveTag(trigger) + AddTag(completion_tag)
* Disabled binding → not included in runtime rules
* Unknown action name → skipped with warning (no crash)
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

import pytest

from lamella.features.paperless_bridge.tag_workflow import (
    ACTION_COMPLETION_TAGS,
    TagOp,
    load_runtime_rules,
)
from lamella.features.paperless_bridge.lamella_namespace import (
    TAG_EXTRACTED,
    TAG_DATE_ANOMALY,
    TAG_LINKED,
    TAG_NEEDS_REVIEW,
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


def _insert(conn, tag_name, action_name, enabled=1):
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    conn.execute(
        "INSERT INTO tag_workflow_bindings "
        "(tag_name, action_name, enabled, config_json, created_at, updated_at) "
        "VALUES (?, ?, ?, '', ?, ?)",
        (tag_name, action_name, enabled, now, now),
    )
    conn.commit()


def test_empty_bindings_returns_empty_rules(conn):
    """No bindings → load_runtime_rules returns empty list."""
    rules = load_runtime_rules(conn)
    assert rules == []


def test_single_binding_produces_one_rule(conn):
    """One enabled binding → one WorkflowRule."""
    _insert(conn, "Lamella:Process", "extract_fields")
    rules = load_runtime_rules(conn)
    assert len(rules) == 1


def test_rule_selector_must_have_trigger_tag(conn):
    """Rule selector requires the trigger tag."""
    _insert(conn, "Lamella:Process", "extract_fields")
    rules = load_runtime_rules(conn)
    rule = rules[0]
    assert "Lamella:Process" in rule.selector.must_have_tags


def test_rule_selector_must_not_have_completion_tag(conn):
    """Rule selector excludes docs that already carry the completion tag."""
    _insert(conn, "Lamella:Process", "extract_fields")
    rules = load_runtime_rules(conn)
    rule = rules[0]
    assert TAG_EXTRACTED in rule.selector.must_not_have_tags


def test_rule_on_success_removes_trigger_and_adds_completion(conn):
    """on_success has RemoveTag(trigger) and AddTag(completion_tag)."""
    _insert(conn, "Lamella:Process", "extract_fields")
    rules = load_runtime_rules(conn)
    rule = rules[0]
    ops = {(op.op, op.tag_name) for op in rule.on_success}
    assert ("remove", "Lamella:Process") in ops
    assert ("add", TAG_EXTRACTED) in ops


def test_rule_on_anomaly_adds_needs_review(conn):
    """on_anomaly applies Lamella:NeedsReview."""
    _insert(conn, "Lamella:Process", "extract_fields")
    rules = load_runtime_rules(conn)
    rule = rules[0]
    ops = {(op.op, op.tag_name) for op in rule.on_anomaly}
    assert ("add", TAG_NEEDS_REVIEW) in ops


def test_disabled_binding_not_in_rules(conn):
    """enabled=0 binding is not included in runtime rules."""
    _insert(conn, "Lamella:Process", "extract_fields", enabled=0)
    rules = load_runtime_rules(conn)
    assert rules == []


def test_multiple_bindings_produce_multiple_rules(conn):
    """Two bindings → two rules."""
    _insert(conn, "TagA", "extract_fields")
    _insert(conn, "TagB", "date_sanity_check")
    rules = load_runtime_rules(conn)
    assert len(rules) == 2
    rule_tags = {r.selector.must_have_tags[0] for r in rules}
    assert rule_tags == {"TagA", "TagB"}


def test_unknown_action_name_skipped(conn):
    """A binding with an unrecognized action name is skipped (no crash)."""
    _insert(conn, "TagA", "nonexistent_action")
    _insert(conn, "TagB", "extract_fields")
    rules = load_runtime_rules(conn)
    # Only TagB's rule should be created
    assert len(rules) == 1
    assert rules[0].selector.must_have_tags == ("TagB",)


def test_date_sanity_check_binding_completion_tag(conn):
    """date_sanity_check binding uses TAG_DATE_ANOMALY as completion tag."""
    _insert(conn, "MyTag", "date_sanity_check")
    rules = load_runtime_rules(conn)
    rule = rules[0]
    assert TAG_DATE_ANOMALY in rule.selector.must_not_have_tags
    ops = {(op.op, op.tag_name) for op in rule.on_success}
    assert ("add", TAG_DATE_ANOMALY) in ops


def test_link_to_ledger_binding_completion_tag(conn):
    """link_to_ledger binding uses TAG_LINKED as completion tag."""
    _insert(conn, "MyTag", "link_to_ledger")
    rules = load_runtime_rules(conn)
    rule = rules[0]
    assert TAG_LINKED in rule.selector.must_not_have_tags
    ops = {(op.op, op.tag_name) for op in rule.on_success}
    assert ("add", TAG_LINKED) in ops


def test_action_completion_tags_catalog():
    """ACTION_COMPLETION_TAGS maps all three v1 action names."""
    assert ACTION_COMPLETION_TAGS["extract_fields"] == TAG_EXTRACTED
    assert ACTION_COMPLETION_TAGS["date_sanity_check"] == TAG_DATE_ANOMALY
    assert ACTION_COMPLETION_TAGS["link_to_ledger"] == TAG_LINKED


def test_load_runtime_rules_tolerates_missing_table():
    """load_runtime_rules returns [] when the table doesn't exist."""
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    # No schema — table does not exist.
    rules = load_runtime_rules(c)
    assert rules == []

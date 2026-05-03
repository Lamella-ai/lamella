# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the staging-backed review surface (Phase B2 groundwork)."""
from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from lamella.core.db import connect, migrate
from lamella.features.import_.staging import (
    StagingService,
    count_pending_items,
    list_pending_items,
)


@pytest.fixture()
def svc() -> StagingService:
    conn = connect(Path(":memory:"))
    migrate(conn)
    return StagingService(conn)


def test_lists_staged_rows_awaiting_decision(svc: StagingService):
    """A staged row with no decision yet (status='new') counts as
    pending review."""
    r = svc.stage(
        source="simplefin",
        source_ref={"account_id": "ACT-1", "txn_id": "T1"},
        posting_date="2026-04-21",
        amount="-10.00",
        payee="Unknown Merchant",
    )
    items = list_pending_items(svc.conn)
    assert len(items) == 1
    assert items[0].staged_id == r.id
    assert items[0].proposed_account is None
    assert items[0].status == "new"


def test_needs_review_decisions_surface(svc: StagingService):
    """Decisions with needs_review=1 appear; confident ones don't."""
    pending = svc.stage(
        source="csv", source_ref={"r": 1},
        posting_date="2026-04-21", amount="-50.00",
    )
    svc.record_decision(
        staged_id=pending.id,
        account="Expenses:Acme:FIXME",
        confidence="low",
        decided_by="ai",
        needs_review=True,
    )
    confident = svc.stage(
        source="csv", source_ref={"r": 2},
        posting_date="2026-04-21", amount="-5.00",
    )
    svc.record_decision(
        staged_id=confident.id,
        account="Expenses:Acme:Supplies",
        confidence="high",
        decided_by="rule",
        needs_review=False,
    )
    items = list_pending_items(svc.conn)
    ids = {i.staged_id for i in items}
    assert pending.id in ids
    assert confident.id not in ids


def test_promoted_rows_excluded(svc: StagingService):
    """Rows already in the ledger (status='promoted') don't surface."""
    r = svc.stage(
        source="csv", source_ref={"r": 1},
        posting_date="2026-04-21", amount="-10.00",
    )
    svc.mark_promoted(r.id, promoted_to_file="x.bean")
    assert list_pending_items(svc.conn) == []
    assert count_pending_items(svc.conn) == 0


def test_dismissed_rows_excluded(svc: StagingService):
    r = svc.stage(
        source="paste", source_ref={"s": "abc"},
        posting_date="2026-04-21", amount="-1.00",
    )
    svc.dismiss(r.id, reason="not real")
    assert list_pending_items(svc.conn) == []


def test_pair_context_surfaced(svc: StagingService):
    """When a staged row is matched as part of a pair, the review
    item carries the pair context so UI can display 'paired with
    row X — confirm?'."""
    a = svc.stage(
        source="csv", source_ref={"r": 1},
        posting_date="2026-04-20", amount="-500.00",
    )
    svc.record_decision(
        staged_id=a.id, account="Assets:PayPal",
        confidence="low", decided_by="ai", needs_review=True,
    )
    b = svc.stage(
        source="simplefin", source_ref={"t": "x"},
        posting_date="2026-04-20", amount="500.00",
    )
    svc.record_decision(
        staged_id=b.id, account="Assets:WF",
        confidence="low", decided_by="ai", needs_review=True,
    )
    svc.record_pair(
        kind="transfer", confidence="medium",
        a_staged_id=a.id, b_staged_id=b.id,
        reason="opposite signs, same day",
    )

    items = list_pending_items(svc.conn)
    by_id = {i.staged_id: i for i in items}
    # Both sides show up because needs_review=1 is set on each.
    assert a.id in by_id and b.id in by_id
    a_item = by_id[a.id]
    assert a_item.pair_id is not None
    assert a_item.pair_kind == "transfer"
    assert a_item.pair_other_staged_id == b.id


def test_filter_by_source(svc: StagingService):
    svc.stage(source="csv", source_ref={"r": 1},
              posting_date="2026-04-20", amount="-10.00")
    svc.stage(source="simplefin", source_ref={"r": 2},
              posting_date="2026-04-20", amount="-20.00")
    only_csv = list_pending_items(svc.conn, source="csv")
    assert len(only_csv) == 1
    assert only_csv[0].source == "csv"


def test_count_matches_list_length(svc: StagingService):
    for i in range(3):
        svc.stage(
            source="csv", source_ref={"r": i},
            posting_date="2026-04-20", amount=f"-{i + 1}.00",
        )
    assert count_pending_items(svc.conn) == 3
    assert len(list_pending_items(svc.conn)) == 3

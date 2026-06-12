# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import pytest

from lamella.features.review_queue.service import ReviewService


def test_enqueue_and_list(db):
    svc = ReviewService(db)
    id1 = svc.enqueue(kind="ambiguous_match", source_ref="paperless:1", priority=2)
    id2 = svc.enqueue(kind="receipt_unmatched", source_ref="paperless:2", priority=0)
    open_items = svc.list_open()
    ids = [i.id for i in open_items]
    # Higher priority first
    assert ids == [id1, id2]
    assert svc.count_open() == 2


def test_resolve_marks_resolved(db):
    svc = ReviewService(db)
    iid = svc.enqueue(kind="fixme", source_ref="txn:abc", priority=0)
    assert svc.resolve(iid, user_decision="classified") is True
    assert svc.count_open() == 0
    # Re-resolving a resolved item returns False
    assert svc.resolve(iid, user_decision="noop") is False


def test_unknown_kind_rejected(db):
    svc = ReviewService(db)
    with pytest.raises(ValueError):
        svc.enqueue(kind="not_a_kind", source_ref="x")


def test_resolve_via_http(app_client, db):
    # Enqueue directly so we have an item
    svc = ReviewService(app_client.app.state.db)
    iid = svc.enqueue(kind="fixme", source_ref="txn:abc")
    resp = app_client.post(f"/review/{iid}/resolve", data={"user_decision": "ok"})
    assert resp.status_code == 204
    assert svc.count_open() == 0

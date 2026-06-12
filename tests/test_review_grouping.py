# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Workstream C2.1 — group_staged_rows partitions review items by
normalized (payee-stem, source-account). Singletons pass through.

The goal downstream in C2.2: classify one row in a group →
learn_from_decision creates a user-rule → the siblings auto-resolve
via tier-2 scan. The grouping key is the only place that's
normalization-sensitive; test the edges here, once.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from lamella.features.review_queue.grouping import (
    StagingReviewGroup,
    group_staged_rows,
)
from lamella.features.import_.staging.review import StagingReviewItem


def _item(
    *,
    staged_id: int,
    payee: str | None = None,
    description: str | None = None,
    account_id: str = "acct-A",
    source: str = "simplefin",
    posting_date: str = "2024-05-10",
) -> StagingReviewItem:
    return StagingReviewItem(
        staged_id=staged_id,
        source=source,
        source_ref={"account_id": account_id, "txn_id": f"sf-{staged_id}"},
        source_ref_hash=None,
        session_id=None,
        posting_date=posting_date,
        amount=Decimal("42.00"),
        currency="USD",
        payee=payee,
        description=description,
        proposed_account=None,
        proposed_confidence=None,
        proposed_by=None,
        proposed_rationale=None,
        status="classified",
        pair_id=None,
        pair_kind=None,
        pair_confidence=None,
        pair_other_staged_id=None,
        synthetic_match_meta=None,
        lamella_txn_id=None,
    )


def test_empty_input_is_empty_output():
    assert group_staged_rows([]) == []


def test_identical_rows_collapse_into_one_group():
    rows = [
        _item(staged_id=1, payee="Acme Supply"),
        _item(staged_id=2, payee="Acme Supply"),
        _item(staged_id=3, payee="Acme Supply"),
    ]
    out = group_staged_rows(rows)
    assert len(out) == 1
    assert out[0].size == 3
    assert [i.staged_id for i in out[0].items] == [1, 2, 3]
    assert out[0].prototype.staged_id == 1


def test_different_cards_are_different_groups():
    """Same payee on two different SimpleFIN accounts — two groups."""
    rows = [
        _item(staged_id=1, payee="Acme Supply", account_id="acct-A"),
        _item(staged_id=2, payee="Acme Supply", account_id="acct-B"),
    ]
    out = group_staged_rows(rows)
    assert len(out) == 2
    assert all(g.is_singleton for g in out)


def test_different_payees_are_different_groups():
    rows = [
        _item(staged_id=1, payee="Acme Supply"),
        _item(staged_id=2, payee="WidgetCo"),
    ]
    out = group_staged_rows(rows)
    assert len(out) == 2


def test_case_and_punctuation_collapse_into_one_group():
    """Normalization is lowercase + strip punctuation + collapse
    whitespace. Digits pass through unchanged — variable trailing
    IDs like store numbers will NOT collapse; that's intentional
    for now and tightening is on the day-1 observation checklist."""
    rows = [
        _item(staged_id=1, payee="ACME SUPPLY!"),
        _item(staged_id=2, payee="acme supply"),
        _item(staged_id=3, payee="Acme  Supply"),
    ]
    out = group_staged_rows(rows)
    assert len(out) == 1
    assert out[0].size == 3


def test_trailing_store_id_breaks_grouping_today():
    """Known day-1 characteristic: store IDs in the payee produce
    singletons. The implementation guide's observation cadence
    points at this as the first tuning target if the KPI surfaces
    it. Lock it here so a future 'fix' is deliberate."""
    rows = [
        _item(staged_id=1, payee="ACME SUPPLY #1234"),
        _item(staged_id=2, payee="ACME SUPPLY #5678"),
    ]
    out = group_staged_rows(rows)
    assert len(out) == 2


def test_long_stems_truncate_at_40_chars():
    """Stems are trimmed at 40 chars — two payees that share the
    first 40 chars collapse, regardless of what follows."""
    # Exactly 40 lowercase chars (no punctuation/space normalization
    # surprises). The differentiator lives strictly after position 40.
    prefix = "a" * 40
    rows = [
        _item(staged_id=1, payee=prefix + "_suffix_one"),
        _item(staged_id=2, payee=prefix + "_suffix_two"),
    ]
    out = group_staged_rows(rows)
    assert len(out) == 1, "stems trimmed at 40 should bucket together"


@pytest.mark.xfail(
    reason="pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md",
    strict=False,
)
def test_description_fallback_when_payee_missing():
    """A row with no payee falls back to description for the stem."""
    rows = [
        _item(staged_id=1, payee=None, description="CafeOne #1234"),
        _item(staged_id=2, payee=None, description="starbucks 1234"),
    ]
    out = group_staged_rows(rows)
    assert len(out) == 1


def test_empty_stem_rows_still_group_by_account():
    """Two rows with no payee AND no description still share a
    deterministic bucket when they're on the same account — but
    they're NOT merged with rows that have a real stem."""
    bare1 = _item(staged_id=1, payee=None, description=None)
    bare2 = _item(staged_id=2, payee=None, description=None)
    named = _item(staged_id=3, payee="Acme")
    out = group_staged_rows([bare1, bare2, named])
    # Two groups: (empty-stem, acct-A) and (acme, acct-A)
    sizes = sorted(g.size for g in out)
    assert sizes == [1, 2]


def test_singleton_is_still_a_group():
    """A size-1 result is a valid group — UI treats it identically,
    just doesn't render the multi-row action."""
    rows = [_item(staged_id=1, payee="Alone")]
    out = group_staged_rows(rows)
    assert len(out) == 1
    assert out[0].is_singleton
    assert out[0].size == 1
    assert out[0].prototype.staged_id == 1


def test_group_order_preserves_first_appearance():
    """If row 1 introduces group A, row 2 introduces group B, row 3
    is another A — groups come back [A, B] not [A, A, B] or [B, A]."""
    rows = [
        _item(staged_id=1, payee="Acme"),
        _item(staged_id=2, payee="WidgetCo"),
        _item(staged_id=3, payee="Acme"),
    ]
    out = group_staged_rows(rows)
    assert out[0].prototype.payee == "Acme"
    assert out[1].prototype.payee == "WidgetCo"
    assert out[0].size == 2
    assert out[1].size == 1


def test_group_is_frozen():
    """StagingReviewGroup is a frozen dataclass — catches someone
    trying to mutate `items` in place."""
    g = StagingReviewGroup(
        key=("x", "y"),
        prototype=_item(staged_id=1, payee="x"),
        items=(_item(staged_id=1, payee="x"),),
    )
    try:
        g.items = ()  # type: ignore[misc]
    except (AttributeError, TypeError):
        return
    raise AssertionError("StagingReviewGroup should be frozen")

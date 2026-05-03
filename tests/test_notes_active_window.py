# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the active-notes wire into classification — migration 022.

Covers the immediate-fix deliverables:
  * Proximity-based and explicit-range inclusion in notes_active_on.
  * Entity and card scope filters.
  * NoteAnnotations carries active_from/active_to + card_override.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from lamella.core.db import connect, migrate
from lamella.features.notes.service import NoteService


@pytest.fixture()
def svc() -> NoteService:
    c = connect(Path(":memory:"))
    migrate(c)
    return NoteService(c)


class TestProximity:
    def test_note_within_window_included(self, svc: NoteService):
        svc.create(
            "convention in Atlanta",
            captured_at=datetime(2026, 4, 15, 10, 0, tzinfo=timezone.utc),
        )
        # A txn three days later lands the note as context.
        results = svc.notes_active_on(date(2026, 4, 18))
        assert len(results) == 1
        assert "Atlanta" in results[0].body

    def test_note_outside_proximity_window_excluded(self, svc: NoteService):
        svc.create(
            "unrelated",
            captured_at=datetime(2026, 4, 1, 10, 0, tzinfo=timezone.utc),
        )
        results = svc.notes_active_on(date(2026, 4, 20))
        assert results == []

    def test_proximity_days_knob(self, svc: NoteService):
        svc.create(
            "note",
            captured_at=datetime(2026, 4, 10, 10, 0, tzinfo=timezone.utc),
        )
        # Default ±3 — does NOT cover April 15.
        assert svc.notes_active_on(date(2026, 4, 15)) == []
        # Expand to ±7 — now it does.
        results = svc.notes_active_on(date(2026, 4, 15), proximity_days=7)
        assert len(results) == 1

    def test_multiple_notes_within_window_ordered_recent_first(
        self, svc: NoteService,
    ):
        svc.create(
            "going to convention",
            captured_at=datetime(2026, 4, 14, 9, 0, tzinfo=timezone.utc),
        )
        svc.create(
            "leaving convention",
            captured_at=datetime(2026, 4, 16, 9, 0, tzinfo=timezone.utc),
        )
        results = svc.notes_active_on(date(2026, 4, 15))
        assert len(results) == 2
        # Most recent first.
        assert "leaving" in results[0].body


class TestExplicitRange:
    def test_note_with_active_range_covers_midpoint(self, svc: NoteService):
        svc.create(
            "atlanta trade show",
            captured_at=datetime(2026, 1, 10, tzinfo=timezone.utc),
            active_from=date(2026, 4, 14),
            active_to=date(2026, 4, 20),
        )
        # Captured in January but active mid-April — range match.
        assert len(svc.notes_active_on(date(2026, 4, 17))) == 1

    def test_active_range_match_wins_despite_old_captured_at(
        self, svc: NoteService,
    ):
        """Captured_at January, active April — a txn in April must
        pick it up via the range even though proximity to
        captured_at would exclude it."""
        svc.create(
            "trip",
            captured_at=datetime(2026, 1, 10, tzinfo=timezone.utc),
            active_from=date(2026, 4, 14),
            active_to=date(2026, 4, 20),
        )
        assert len(svc.notes_active_on(date(2026, 4, 15))) == 1


class TestScopeFilters:
    def test_entity_scope_filter_matches_scope(self, svc: NoteService):
        svc.create(
            "acme only",
            captured_at=datetime(2026, 4, 15, tzinfo=timezone.utc),
            entity_scope="Acme",
        )
        svc.create(
            "global",
            captured_at=datetime(2026, 4, 15, tzinfo=timezone.utc),
        )
        # Filtering by entity includes the scoped + global, not others.
        gb = svc.notes_active_on(date(2026, 4, 15), entity="Acme")
        assert len(gb) == 2

    def test_entity_scope_filter_excludes_wrong_scope(self, svc: NoteService):
        svc.create(
            "acme only",
            captured_at=datetime(2026, 4, 15, tzinfo=timezone.utc),
            entity_scope="Acme",
        )
        cnc = svc.notes_active_on(date(2026, 4, 15), entity="WidgetCo")
        # The Acme-scoped note is excluded.
        assert cnc == []


class TestClassifyWire:
    def test_build_classify_context_returns_five_tuple(self, svc: NoteService):
        """The classify context function must return active notes as
        the fifth element so render can pass them into the prompt."""
        from beancount.core import data as bdata
        from beancount.core.amount import Amount
        from beancount.core.number import D

        # Build a synthetic FIXME transaction.
        posting_card = bdata.Posting(
            account="Liabilities:Acme:Card:0123",
            units=Amount(D("-10"), "USD"),
            cost=None, price=None, flag=None, meta=None,
        )
        posting_fixme = bdata.Posting(
            account="Expenses:Acme:FIXME",
            units=Amount(D("10"), "USD"),
            cost=None, price=None, flag=None, meta=None,
        )
        txn = bdata.Transaction(
            meta={"filename": "x", "lineno": 1},
            date=date(2026, 4, 17),
            flag="*",
            payee="Some Vendor",
            narration="CARD PURCHASE",
            tags=frozenset(),
            links=frozenset(),
            postings=[posting_card, posting_fixme],
        )
        svc.create(
            "convention",
            captured_at=datetime(2026, 4, 17, tzinfo=timezone.utc),
        )

        from lamella.features.ai_cascade.classify import build_classify_context
        (view, similar, accounts, entity, notes,
         _suspicion, _abe, _receipt, _mileage,
         _density) = build_classify_context(
            entries=[], txn=txn, conn=svc.conn,
        )
        assert view is not None
        assert entity == "Acme"
        # The active-notes pull returned our convention note.
        assert len(notes) == 1
        assert "convention" in notes[0].body.lower()


class TestParseNoteAnnotations:
    def test_annotations_carry_active_range_and_override_flag(self):
        """parse_note's NoteAnnotations dataclass gains active_from,
        active_to, and card_override so the background task can
        persist them."""
        from lamella.features.ai_cascade.notes import NoteAnnotations
        a = NoteAnnotations(
            merchant_hint=None,
            entity_hint="Acme",
            amount_hint=None,
            date_hint=None,
            active_from=date(2026, 4, 14),
            active_to=date(2026, 4, 20),
            card_override=True,
            keywords=("atlanta", "convention"),
            decision_id=1,
        )
        assert a.active_from == date(2026, 4, 14)
        assert a.card_override is True


class TestUpdateHints:
    def test_update_hints_persists_active_range(self, svc: NoteService):
        note_id = svc.create(
            "atlanta trip",
            captured_at=datetime(2026, 4, 12, tzinfo=timezone.utc),
        )
        svc.update_hints(
            note_id,
            merchant_hint=None,
            entity_hint=None,
            active_from=date(2026, 4, 14),
            active_to=date(2026, 4, 20),
            keywords=["atlanta", "convention"],
            card_override=False,
        )
        note = svc.get(note_id)
        assert note is not None
        assert note.active_from == date(2026, 4, 14)
        assert note.active_to == date(2026, 4, 20)
        assert "atlanta" in note.keywords

    def test_update_hints_card_override_flag_survives(self, svc: NoteService):
        note_id = svc.create("body", captured_at=datetime(2026, 4, 12, tzinfo=timezone.utc))
        svc.update_hints(
            note_id,
            merchant_hint=None,
            entity_hint=None,
            card_override=True,
        )
        assert svc.get(note_id).card_override is True

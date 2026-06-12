# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Calendar feature acceptance tests.

Nine tests, mapped to the acceptance criteria in the brief plus two
covering the classification-dirty signal and the pre-existing-override
backfill behaviour.

The tests avoid mocking the parts that are easy to fake and get wrong.
Criterion 6 in particular renders the REAL classify_txn.j2 prompt and
asserts the day-note body appears in the output — a structural check
(``"day_note" in context``) would pass even if the wire to the prompt
were broken.
"""
from __future__ import annotations

import sqlite3
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from lamella.features.ai_cascade.context import TxnForClassify, render
from lamella.features.calendar.classification_modified import (
    bump,
    rebuild_from_entries,
)
from lamella.features.calendar.queries import (
    activity_in_range,
    day_activity,
)
from lamella.features.calendar.tz import app_tz, local_date_of, today_local
from lamella.core.config import Settings
from lamella.features.notes.service import NoteService


# ---- criterion 2: 11pm local mileage lands on today's cell -------------

class TestTimezoneHelpers:
    """APP_TZ matters for code paths that derive a date from a
    timestamp. The common foot-gun is calling ``date.today()`` (UTC
    in the container) and landing next-day mileage on a wrong cell."""

    def test_today_local_differs_from_utc_at_late_local_evening(self):
        # New York is UTC-4 in April (DST). 3:15 UTC on the 15th is
        # 11:15 PM on the 14th in NYC. today_local() must return
        # the local date, not the UTC date.
        # We emulate "now" with local_date_of, which is what any
        # calendar code path deriving a date from a stored instant
        # should use.
        utc_instant = datetime(2026, 4, 15, 3, 15, tzinfo=ZoneInfo("UTC"))
        ny_settings = Settings(app_tz="America/New_York")
        utc_settings = Settings(app_tz="UTC")

        ny_local = local_date_of(utc_instant, ny_settings)
        utc_local = local_date_of(utc_instant, utc_settings)

        assert ny_local == date(2026, 4, 14), (
            "11pm local on the 14th must land on the 14th under APP_TZ=NYC"
        )
        assert utc_local == date(2026, 4, 15)

    def test_app_tz_invalid_falls_back_to_utc_without_crashing(self):
        settings = Settings(app_tz="Not/A/Real/Zone")
        tz = app_tz(settings)
        assert tz.key == "UTC"


# ---- criteria 3, 4, 5, 8, 9: dirty-signal plumbing ---------------------

def _entries_for_fixture(settings: Settings):
    from lamella.core.beancount_io import LedgerReader

    reader = LedgerReader(settings.ledger_main)
    return list(reader.load().entries)


class TestDirtySignals:
    """Build the dirty state via each of the four activity-source
    timestamps + the classification signal. Each test marks a day
    reviewed, then induces an activity timestamp after that, and
    asserts the day's aggregate flips to dirty.

    The query under test is ``activity_in_range``, which is what
    the month grid runs. A per-day status test is the right place
    to catch wiring bugs (wrong column, wrong join, missing source).
    """

    @pytest.fixture
    def entries(self, settings: Settings):
        return _entries_for_fixture(settings)

    @staticmethod
    def _mark_reviewed(db: sqlite3.Connection, d: date, reviewed_at: datetime):
        db.execute(
            "INSERT INTO day_reviews (review_date, last_reviewed_at) "
            "VALUES (?, ?)",
            (d.isoformat(), reviewed_at.isoformat(timespec="seconds")),
        )

    @staticmethod
    def _agg_status(db, entries, d: date) -> str:
        out = activity_in_range(db, entries, d, d)
        return out[d].status if d in out else "empty"

    def test_mark_reviewed_clears_dirty_then_new_fixme_flips_back(
        self, db, settings, entries
    ):
        # Seed: Apr 10 has a real txn in the fixture. Add a FIXME
        # review_queue row to mimic SimpleFIN ingest dropping in a
        # new-txn activity signal.
        target = date(2026, 4, 10)
        reviewed_at = datetime(2026, 4, 10, 18, 0)
        self._mark_reviewed(db, target, reviewed_at)

        assert self._agg_status(db, entries, target) == "reviewed"

        # A new SimpleFIN txn lands for Apr 10 after the review. Its
        # review_queue row carries created_at=now.
        after = (reviewed_at + timedelta(hours=2)).isoformat(timespec="seconds")
        db.execute(
            "INSERT INTO review_queue (kind, source_ref, created_at) "
            "VALUES ('fixme', 'fixme:abc123', ?)",
            (after,),
        )

        assert self._agg_status(db, entries, target) == "dirty"

    def test_backimported_mileage_flips_reviewed_day_to_dirty(
        self, db, settings, entries
    ):
        target = date(2026, 4, 10)
        self._mark_reviewed(db, target, datetime(2026, 4, 10, 18, 0))
        assert self._agg_status(db, entries, target) == "reviewed"

        # User imports old mileage rows now; created_at gets CURRENT_TIMESTAMP
        # which is strictly after the review_at. Use an explicit future
        # stamp so the test doesn't rely on wall clock.
        db.execute(
            """
            INSERT INTO mileage_entries
                (entry_date, vehicle, odometer_start, odometer_end,
                 miles, purpose, entity, from_loc, to_loc, notes,
                 created_at)
            VALUES (?, ?, NULL, NULL, ?, ?, 'Acme', NULL, NULL, NULL, ?)
            """,
            (target.isoformat(), "Work Van", 12.0, "back-import",
             "2026-04-11 09:00:00"),
        )

        assert self._agg_status(db, entries, target) == "dirty"

    def test_paperless_modified_at_change_flips_to_dirty(
        self, db, settings, entries
    ):
        target = date(2026, 4, 10)
        self._mark_reviewed(db, target, datetime(2026, 4, 10, 18, 0))

        db.execute(
            """
            INSERT INTO paperless_doc_index
                (paperless_id, title, created_date, modified_at, content_excerpt)
            VALUES (?, ?, ?, ?, ?)
            """,
            (9001, "Receipt", target.isoformat(),
             "2026-04-11 10:00:00", "excerpt"),
        )

        assert self._agg_status(db, entries, target) == "dirty"

    def test_pinned_memo_dirties_txns_day_not_capture_day(
        self, db, settings, entries
    ):
        """A memo written today for a txn from last week must flip
        the txn's day to dirty — not today's calendar cell. This is
        the point of using datetime.now() for captured_at + looking
        up the txn's date via txn_hash in the aggregator."""
        # Fixture Apr 10 has a real txn. Mark it reviewed at 6pm
        # that day.
        target = date(2026, 4, 10)
        reviewed_at = datetime(2026, 4, 10, 18, 0)
        self._mark_reviewed(db, target, reviewed_at)
        assert self._agg_status(db, entries, target) == "reviewed"

        # Find the Apr 10 txn hash.
        from beancount.core.data import Transaction
        from lamella.core.beancount_io import txn_hash as _th
        apr10_hash = next(
            _th(e) for e in entries
            if isinstance(e, Transaction) and e.date == target
        )

        # Write a memo NOW (after review) pinned to that txn. Use
        # a future captured_at to simulate "written after the review."
        db.execute(
            """
            INSERT INTO notes
                (body, captured_at, txn_hash)
            VALUES (?, ?, ?)
            """,
            ("The Acme prototype build", "2026-04-11 09:00:00", apr10_hash),
        )

        # Apr 10 should now be dirty — the memo is activity attributed
        # to Apr 10 (not Apr 11, which is when the memo was written).
        assert self._agg_status(db, entries, target) == "dirty"

        # And Apr 11 — which has no other activity — should NOT show
        # as dirty (empty day).
        assert self._agg_status(db, entries, date(2026, 4, 11)) == "empty"

    def test_manual_override_bumps_classification_modified_and_dirties(
        self, db, settings, entries
    ):
        target = date(2026, 4, 10)
        self._mark_reviewed(db, target, datetime(2026, 4, 10, 18, 0))

        # Simulate OverrideWriter.append bumping the cache. (Testing
        # the full write path would require a fixture with a FIXME
        # leg; bumping the cache directly matches what the writer
        # does on a successful write.)
        bump(
            db,
            txn_hash="hash-abcdef",
            txn_date=target,
            modified_at=datetime(2026, 4, 10, 19, 30),
        )

        assert self._agg_status(db, entries, target) == "dirty"

    def test_prereexisting_override_without_bcg_modified_at_does_not_dirty_fresh_review(
        self, db, settings
    ):
        """Criterion 9 (added): the backfill default must fall in
        the past relative to any review done after the feature
        ships. We construct an override block WITHOUT lamella-modified-at
        and rebuild the cache from entries. The fallback timestamp
        is txn_date at local midnight. A review marked after the
        rebuild sees no dirty signal."""
        from beancount.core.data import Transaction, Posting, Amount
        from decimal import Decimal as _D

        # Synthesize one Transaction with lamella-override-of and NO
        # lamella-modified-at — the pre-044 shape.
        txn = Transaction(
            meta={"lamella-override-of": "preexisting-hash"},
            date=date(2026, 4, 10),
            flag="*",
            payee=None,
            narration="legacy override",
            tags=frozenset(),
            links=frozenset(),
            postings=[],
        )
        tz = ZoneInfo("America/New_York")
        rebuild_from_entries(db, [txn], tz_for_fallback=tz)

        row = db.execute(
            "SELECT modified_at FROM txn_classification_modified "
            "WHERE txn_hash = 'preexisting-hash'"
        ).fetchone()
        assert row is not None, "fallback row should be created"
        # Now review the day AFTER the rebuild. The fallback lived
        # at local midnight April 10 — long before any review
        # performed post-feature-ship.
        self_mark_time = datetime(2026, 4, 12, 9, 0)
        db.execute(
            "INSERT INTO day_reviews (review_date, last_reviewed_at) "
            "VALUES (?, ?)",
            ("2026-04-10", self_mark_time.isoformat(timespec="seconds")),
        )
        out = activity_in_range(
            db, [], date(2026, 4, 10), date(2026, 4, 10),
        )
        status = out[date(2026, 4, 10)].status if out else None
        # With only the fallback-row classification signal + a later
        # review, the day should NOT be dirty. (The backfill landed
        # at midnight on Apr 10; the review was on Apr 12. The cache
        # row's modified_at < review_at → not dirty.)
        # The aggregate is "empty" because there's no txn/note/mileage/paperless
        # activity; that's fine. The specific property we care about
        # is modified_at < last_reviewed_at, so the cache row alone
        # doesn't flip anything.
        # Inject one mileage-like activity to make the day non-empty,
        # with its created_at BEFORE the review:
        db.execute(
            """
            INSERT INTO mileage_entries
                (entry_date, vehicle, odometer_start, odometer_end,
                 miles, purpose, entity, from_loc, to_loc, notes,
                 created_at)
            VALUES ('2026-04-10', 'v', NULL, NULL, 5.0, NULL, 'Acme',
                    NULL, NULL, NULL, '2026-04-10 09:00:00')
            """
        )
        out = activity_in_range(
            db, [], date(2026, 4, 10), date(2026, 4, 10),
        )
        assert out[date(2026, 4, 10)].status == "reviewed", (
            "backfill default should be dated earlier than post-feature reviews"
        )


# ---- criterion 6: day-note feeds into the classify_txn prompt ---------

class TestTxnPinnedMemo:
    """A memo pinned to a specific transaction must be picked up by
    NoteService.notes_active_on and surface in the classify_txn
    prompt as a txn-specific memo — not just a date-scoped note."""

    def test_txn_scoped_memo_returned_regardless_of_date(self, db):
        service = NoteService(db)
        target_hash = "abc123" * 8
        nid = service.create(
            body="This charge was for the Acme prototype build.",
            txn_hash=target_hash,
        )
        assert nid > 0
        # Query on a totally unrelated date — the memo should still
        # come back because it's pinned to the txn hash.
        rows = service.notes_active_on(
            date(2099, 12, 31), txn_hash=target_hash,
        )
        assert any(r.txn_hash == target_hash for r in rows), (
            "txn-scoped memo must be returned when txn_hash matches, "
            "regardless of date distance"
        )

    def test_txn_memo_renders_marked_in_prompt(self, db):
        target_hash = "deadbeef" * 8
        unique = "PROTOTYPE-BUILD-ALPHA"
        service = NoteService(db)
        service.create(body=f"For {unique}", txn_hash=target_hash)
        rows = service.notes_active_on(
            date(2026, 4, 10), txn_hash=target_hash,
        )
        from lamella.features.ai_cascade.context import TxnForClassify, render
        from decimal import Decimal as _D
        txn = TxnForClassify(
            date=date(2026, 4, 10),
            amount=_D("10.00"),
            currency="USD",
            payee=None,
            narration="something",
            card_account="Liabilities:Acme:Card:CardA1234",
            fixme_account="Expenses:FIXME",
            txn_hash=target_hash,
        )
        rendered = render(
            "classify_txn.j2",
            txn=txn,
            similar=[],
            entity="Acme",
            accounts=["Expenses:Acme:Supplies"],
            accounts_by_entity={},
            registry_preamble="",
            active_notes=rows,
            card_suspicion=None,
            receipt=None,
            mileage_entries=[],
            vehicle_density=[],
            account_descriptions={},
            entity_context=None,
            active_projects=[],
            fixme_root="Expenses",
        )
        assert unique in rendered
        assert "MEMO FOR THIS TXN" in rendered, (
            "txn-scoped memo should be visually flagged in the prompt "
            "so the AI treats it as a direct instruction for this txn"
        )


class TestDayNoteInClassifierPrompt:
    """The wire we care about: the day note the user types on the
    calendar day view must appear in the real assembled prompt
    string the classifier sends to the model."""

    def test_single_day_unscoped_note_appears_in_rendered_prompt(self, db):
        target = date(2026, 4, 18)
        unique_marker = "ACME-QUARTERLY-CONFERENCE-BADGE-42"
        service = NoteService(db)
        service.create(
            body=f"At the {unique_marker} in Atlanta all week",
            active_from=target,
            active_to=target,
        )

        notes = service.notes_active_on(target)
        assert any(unique_marker in n.body for n in notes), (
            "precondition: NoteService must return the day note"
        )

        # Build the fake transaction context and render the REAL
        # prompt template that propose_account uses.
        txn = TxnForClassify(
            date=target,
            amount=Decimal("42.00"),
            currency="USD",
            payee="Some Vendor",
            narration="Dinner",
            card_account="Liabilities:Acme:Card:CardA1234",
            fixme_account="Expenses:FIXME",
            txn_hash="hash-test",
        )
        rendered = render(
            "classify_txn.j2",
            txn=txn,
            similar=[],
            entity="Acme",
            accounts=["Expenses:Acme:Meals"],
            accounts_by_entity={},
            registry_preamble="",
            active_notes=notes,
            card_suspicion=None,
            receipt=None,
            mileage_entries=[],
            vehicle_density=[],
            account_descriptions={},
            entity_context=None,
            active_projects=[],
            fixme_root="Expenses",
        )

        assert unique_marker in rendered, (
            f"day-note body must appear verbatim in the rendered prompt; "
            f"got:\n{rendered}"
        )


# ---- criterion 7: wipe SQLite, reconstruct from ledger ----------------

class TestReconstructDayReviews:
    def test_day_review_round_trips_through_ledger(self, tmp_path, settings):
        # Fresh DB + fixture ledger.
        from lamella.core.db import connect, migrate
        from lamella.features.calendar.writer import append_day_review
        from lamella.core.beancount_io import LedgerReader
        from lamella.core.transform.reconstruct import (
            _import_all_steps,
            run_all,
        )
        from lamella.features.receipts import linker as _linker

        # Disable bean-check shelling during tests.
        original = _linker.run_bean_check
        _linker.run_bean_check = lambda p: None
        try:
            conn = connect(tmp_path / "r.sqlite")
            migrate(conn)

            # Write a day-review directive to the ledger.
            target = date(2026, 4, 10)
            reviewed_at = datetime(2026, 4, 10, 19, 0)
            append_day_review(
                connector_config=settings.connector_config_path,
                main_bean=settings.ledger_main,
                review_date=target,
                last_reviewed_at=reviewed_at,
                run_check=False,
            )

            # Reconstruct against a fresh DB.
            reader = LedgerReader(settings.ledger_main)
            entries = list(reader.load().entries)
            _import_all_steps()
            run_all(conn, entries, force=True)

            row = conn.execute(
                "SELECT review_date, last_reviewed_at FROM day_reviews "
                "WHERE review_date = ?",
                (target.isoformat(),),
            ).fetchone()
            assert row is not None, "day_reviews row must be rebuilt from ledger"
            assert row["last_reviewed_at"]  # stamped
            conn.close()
        finally:
            _linker.run_bean_check = original


# ---- criterion 1: month grid loads + has correct dots/status ----------

class TestMonthGridSmoke:
    def test_grid_aggregates_reflect_fixture_activity(self, db, settings):
        entries = _entries_for_fixture(settings)
        start = date(2026, 4, 1)
        end = date(2026, 4, 30)

        out = activity_in_range(db, entries, start, end)

        # The fixture has txns on Apr 10, 12, 14. Every one of those
        # dates should report has_activity=True via txn_count > 0.
        assert date(2026, 4, 10) in out and out[date(2026, 4, 10)].txn_count >= 1
        assert date(2026, 4, 12) in out and out[date(2026, 4, 12)].txn_count >= 1
        assert date(2026, 4, 14) in out and out[date(2026, 4, 14)].txn_count >= 1

        # An empty-in-fixture date should not show as having activity.
        assert date(2026, 4, 1) not in out or not out[date(2026, 4, 1)].has_activity


# ---- day_activity smoke ------------------------------------------------

class TestDayViewSmoke:
    def test_day_activity_lists_fixture_txn(self, db, settings):
        entries = _entries_for_fixture(settings)
        view = day_activity(db, entries, date(2026, 4, 10))
        assert view.transactions, "Apr 10 should have at least one txn"
        # Default (no review row) → unreviewed when activity exists.
        assert view.status == "unreviewed"

    def test_day_activity_computes_per_account_deltas(self, db, settings):
        entries = _entries_for_fixture(settings)
        # Apr 10 fixture: Hardware Store charges $42.17 to CardA1234.
        view = day_activity(db, entries, date(2026, 4, 10))
        # There should be at least one account delta, and the card
        # account should be among the moved accounts.
        names = [ad.account for ad in view.account_deltas]
        assert any("CardA1234" in a for a in names), (
            f"expected the card account in deltas; got {names}"
        )
        # Non-zero amounts only.
        assert all(ad.delta != 0 for ad in view.account_deltas)
        # Sorted by magnitude descending.
        abs_deltas = [abs(ad.delta) for ad in view.account_deltas]
        assert abs_deltas == sorted(abs_deltas, reverse=True)

    def test_day_activity_surfaces_classification_context(self, db, settings):
        """The day view must surface entity, category, card, and kind
        for each txn — the user shouldn't have to open each row to
        see what business it belongs to."""
        entries = _entries_for_fixture(settings)
        view = day_activity(db, entries, date(2026, 4, 10))
        # Fixture Apr 10 has one txn: Hardware Store on Acme's CardA1234.
        by_payee = {t.payee or t.narration: t for t in view.transactions}
        t = next(iter(by_payee.values()))
        assert t.kind == "expense"
        assert t.entity == "Acme"
        assert t.card_account == "Liabilities:Acme:Card:CardA1234"
        assert t.expense_account == "Expenses:Acme:Supplies"
        assert t.category_leaf == "Supplies"
        assert t.is_fixme is False


class TestNearDuplicateFlagTransferExclusion:
    """The near-duplicate flag should NOT fire on transfer-shaped
    transactions — common false-positive from Bank One / Chase
    online-transfer reference-number narrations."""

    def test_transfer_pair_not_flagged_as_duplicate(self, db, settings, tmp_path):
        # Write a temp ledger with two transfer-shaped txns on the same
        # day that share merchant + amount (the classic false-positive).
        from beancount.loader import load_string
        bean_text = """
            option "title" "t"
            option "operating_currency" "USD"
            2024-01-01 open Assets:Personal:WF:Prime USD
            2024-01-01 open Assets:Personal:WF:Checking USD
            2024-01-01 open Expenses:FIXME USD

            2026-04-15 * "" "ONLINE TRANSFER FROM PRIME CHECKING"
              Assets:Personal:WF:Prime  -1000.00 USD
              Expenses:FIXME             1000.00 USD

            2026-04-15 * "" "ONLINE TRANSFER FROM PRIME CHECKING"
              Expenses:FIXME            -1000.00 USD
              Assets:Personal:WF:Checking 1000.00 USD
        """
        entries, _errors, _opts = load_string(bean_text)

        from lamella.features.calendar.flags import compute_day_flags
        flags = compute_day_flags(db, entries, date(2026, 4, 15))
        codes = {f.code for f in flags}
        assert "near_duplicate_txns" not in codes, (
            f"transfer-shaped txns should not trigger near_duplicate; got {codes}"
        )


# ---- HTTP-level smoke: the routes actually render (no 500s) ---------

@pytest.fixture
def ungated_client(settings, tmp_path, monkeypatch):
    """Same as app_client but with the first-run setup gate disabled.

    The gate redirects every route to /setup/welcome when the ledger
    has transactions but no lamella-* markers, which is exactly the state
    of the fixture ledger. Calendar HTTP smoke tests need to hit the
    real routes, so we flip the flags off after the app starts."""
    from fastapi.testclient import TestClient
    from lamella.main import create_app

    monkeypatch.setattr(
        "lamella.features.receipts.linker.run_bean_check",
        lambda main_bean: None,
    )

    app = create_app(settings=settings)
    with TestClient(app) as client:
        app.state.needs_welcome = False
        app.state.needs_reconstruct = False
        app.state.setup_required_complete = True
        # ledger_detection is a frozen dataclass with `needs_setup`
        # as a computed property, so we replace the whole object
        # with a stub that the setup_gate middleware reads.
        class _NoSetupNeeded:
            needs_setup = False
        app.state.ledger_detection = _NoSetupNeeded()
        yield client


class TestCalendarRoutesRender:
    """Catches template-rendering regressions that unit-level query
    tests miss — e.g. a stray ``%-d`` strftime token that 500s on
    Windows. Hits the real route via the app client and checks status."""

    def test_month_grid_renders(self, ungated_client):
        resp = ungated_client.get("/calendar/2026-04", follow_redirects=False)
        assert resp.status_code == 200, resp.text[:500]
        assert "April 2026" in resp.text

    def test_day_view_renders(self, ungated_client):
        resp = ungated_client.get("/calendar/2026-04-10", follow_redirects=False)
        assert resp.status_code == 200, resp.text[:500]
        # Day label is built in Python, not via %-d strftime.
        assert "April 10, 2026" in resp.text

    def test_calendar_root_redirects(self, ungated_client):
        resp = ungated_client.get("/calendar", follow_redirects=False)
        assert resp.status_code in (302, 303, 307)


# ---- phase 2 ----------------------------------------------------------

class TestNextUnreviewedEndpoint:
    """The workflow-critical 'n' hotkey endpoint. Skips empty days,
    stops on dirty days, 404s when nothing ahead."""

    def test_skips_empty_days_and_lands_on_activity(self, ungated_client, settings):
        # Fixture ledger has Apr 10, 12, 14 activity. Starting at
        # Apr 9 → next should be Apr 10.
        resp = ungated_client.get("/calendar/2026-04-09/next", follow_redirects=False)
        assert resp.status_code == 303, resp.text[:300]
        assert resp.headers["location"] == "/calendar/2026-04-10"

    def test_stops_on_dirty_day(self, ungated_client, settings):
        # Mark Apr 10 reviewed, then flip it dirty via a late Paperless
        # doc. Starting from Apr 9 → should still land on Apr 10 (dirty),
        # not skip ahead to Apr 12.
        # The ungated_client shares a db via app.state.
        client = ungated_client
        app = client.app
        db = app.state.db
        db.execute(
            "INSERT INTO day_reviews (review_date, last_reviewed_at) "
            "VALUES ('2026-04-10', '2026-04-10 18:00:00')"
        )
        db.execute(
            """
            INSERT INTO paperless_doc_index
                (paperless_id, title, created_date, modified_at)
            VALUES (42, 'Receipt', '2026-04-10', '2026-04-11 09:00:00')
            """
        )

        resp = client.get("/calendar/2026-04-09/next", follow_redirects=False)
        assert resp.status_code == 303
        assert resp.headers["location"] == "/calendar/2026-04-10"

    def test_skips_clean_reviewed_day(self, ungated_client, settings):
        """A reviewed-and-clean day should be skipped — the
        'next unreviewed with activity' loop shouldn't stop there."""
        client = ungated_client
        db = client.app.state.db
        # Mark Apr 10 as reviewed AFTER all txn activity. No additional
        # post-review activity → stays clean.
        db.execute(
            "INSERT INTO day_reviews (review_date, last_reviewed_at) "
            "VALUES ('2026-04-10', '2030-01-01 00:00:00')"
        )
        resp = client.get("/calendar/2026-04-09/next", follow_redirects=False)
        assert resp.status_code == 303
        # Apr 10 is clean-reviewed, so next should be Apr 12.
        assert resp.headers["location"] == "/calendar/2026-04-12"

    def test_404_when_nothing_ahead(self, ungated_client, settings):
        # Starting from far in the future → no activity ahead → 404.
        resp = ungated_client.get("/calendar/2030-04-01/next", follow_redirects=False)
        assert resp.status_code == 404


class TestAiSummaryService:
    """The summarize_day helper builds a valid prompt and returns
    the AI's text response. Uses a fake client to avoid network."""

    def test_summary_returns_text_from_client_response(self):
        import asyncio as _asyncio
        from lamella.features.calendar.ai import summarize_day
        from lamella.features.calendar.queries import (
            DayMileage, DayPaperless, DayTxn,
        )
        from decimal import Decimal as _D

        captured_prompts: dict[str, str] = {}

        class FakeClient:
            async def chat(self, *, decision_type, input_ref, system, user, schema, model=None, **kw):
                captured_prompts["user"] = user
                captured_prompts["system"] = system
                # Return an AIResult with a _SummaryResponse.
                from lamella.adapters.openrouter.client import AIResult
                return AIResult(
                    data=schema(summary="Day was light; one small supply purchase."),
                    decision_id=1,
                    prompt_tokens=50,
                    completion_tokens=10,
                    model="fake",
                    cached=False,
                )

        txns = [
            DayTxn(
                txn_hash="h1", date=date(2026, 4, 10),
                narration="Hardware Store",
                from_account="Liabilities:Acme:Card",
                to_account="Expenses:Supplies",
                account_summary="Liabilities:Acme:Card → Expenses:Supplies",
                amount=_D("42.17"), currency="USD", is_fixme=False,
            )
        ]
        result = _asyncio.run(
            summarize_day(
                FakeClient(),
                day=date(2026, 4, 10),
                day_note="trade show week",
                transactions=txns,
                mileage=[],
                paperless=[],
                flag_notes=[],
            )
        )
        assert result == "Day was light; one small supply purchase."
        assert "Hardware Store" in captured_prompts["user"]
        assert "trade show week" in captured_prompts["user"]


class TestAiAuditService:
    """audit_day compares proposed vs current per txn and produces
    agreed/disagreed entries without touching any storage."""

    def test_audit_produces_disagreement_entries(self, db):
        import asyncio as _asyncio
        from lamella.features.calendar.ai import audit_day
        from lamella.features.calendar.queries import DayTxn
        from lamella.features.ai_cascade.gating import AIProposal
        from decimal import Decimal as _D

        # Mock propose_account via monkeypatching? Simpler: patch the
        # module reference.
        import lamella.features.calendar.ai as ai_mod

        async def fake_propose_account(client, *, txn, valid_accounts, **kw):
            # Propose a different account so we get a disagreement.
            return AIProposal(
                target_account="Expenses:Acme:Supplies:Tools",
                confidence=0.85,
                reasoning="Looks like a specialty tool",
                decision_id=1,
                intercompany_flag=False,
                owning_entity=None,
            )

        original = ai_mod.__dict__.get("propose_account")

        class FakeClient:
            async def aclose(self):
                pass

        try:
            # Patch the symbol the module looks up at call time.
            import lamella.features.ai_cascade.classify as classify_mod
            classify_mod.propose_account = fake_propose_account

            txns = [
                DayTxn(
                    txn_hash="h1", date=date(2026, 4, 10),
                    narration="Hardware Store",
                    from_account="Liabilities:Acme:Card",
                    to_account="Expenses:Acme:Supplies",
                    account_summary="Liabilities:Acme:Card → Expenses:Acme:Supplies",
                    amount=_D("42.17"), currency="USD", is_fixme=False,
                ),
            ]
            entries = _asyncio.run(
                audit_day(
                    FakeClient(),
                    day=date(2026, 4, 10),
                    transactions=txns,
                    active_notes=[],
                    mileage_entries=[],
                    entity_accounts_by_entity={"Acme": ["Expenses:Acme:Supplies:Tools"]},
                    resolve_entity=lambda a: "Acme",
                )
            )
        finally:
            if original is not None:
                classify_mod.propose_account = original

        assert len(entries) == 1
        e = entries[0]
        assert e.agreed is False
        assert e.proposed_account == "Expenses:Acme:Supplies:Tools"
        assert e.current_account == "Expenses:Acme:Supplies"

    def test_audit_does_not_write_overrides(self, db, settings):
        """Criterion: audit MUST NOT mutate the ledger. Count
        override-block bytes before and after an audit-style call
        and assert they haven't changed."""
        overrides = settings.connector_overrides_path
        before = overrides.read_text(encoding="utf-8") if overrides.exists() else ""

        # Running audit_day directly (with no AI call path actually
        # triggering writes) must leave the ledger file byte-stable.
        # This is a tautological check at the helper level, but it
        # pins down the contract — if a future refactor adds a
        # writer call, this test fails.
        from lamella.features.calendar.ai import audit_day
        import asyncio as _asyncio

        async def never_called(*args, **kwargs):
            raise AssertionError("no AI call should happen on empty txn list")

        class FakeClient:
            async def aclose(self):
                pass

        import lamella.features.ai_cascade.classify as classify_mod
        original = classify_mod.propose_account
        classify_mod.propose_account = never_called
        try:
            entries = _asyncio.run(
                audit_day(
                    FakeClient(),
                    day=date(2026, 4, 10),
                    transactions=[],
                    active_notes=[],
                    mileage_entries=[],
                    entity_accounts_by_entity={},
                    resolve_entity=lambda a: None,
                )
            )
            assert entries == []
        finally:
            classify_mod.propose_account = original

        after = overrides.read_text(encoding="utf-8") if overrides.exists() else ""
        assert before == after


class TestReconstructAiFields:
    """The ai_summary and ai_audit_result columns must round-trip
    through the ledger like last_reviewed_at does."""

    def test_ai_summary_round_trips_through_ledger(self, tmp_path, settings):
        from lamella.core.db import connect, migrate
        from lamella.features.calendar.writer import append_day_review
        from lamella.core.beancount_io import LedgerReader
        from lamella.core.transform.reconstruct import (
            _import_all_steps,
            run_all,
        )
        from lamella.features.receipts import linker as _linker

        original = _linker.run_bean_check
        _linker.run_bean_check = lambda p: None
        try:
            conn = connect(tmp_path / "r.sqlite")
            migrate(conn)

            append_day_review(
                connector_config=settings.connector_config_path,
                main_bean=settings.ledger_main,
                review_date=date(2026, 4, 10),
                last_reviewed_at=None,
                ai_summary="A quiet day — one supplies purchase for Acme.",
                ai_summary_at="2026-04-11T09:00:00",
                run_check=False,
            )

            reader = LedgerReader(settings.ledger_main)
            entries = list(reader.load().entries)
            _import_all_steps()
            run_all(conn, entries, force=True)

            row = conn.execute(
                "SELECT ai_summary FROM day_reviews WHERE review_date = ?",
                ("2026-04-10",),
            ).fetchone()
            assert row is not None
            assert "quiet day" in row["ai_summary"]
            conn.close()
        finally:
            _linker.run_bean_check = original

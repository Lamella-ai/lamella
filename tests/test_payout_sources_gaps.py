# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Coverage for the gap-fill behaviors on payout-source detection:
  * Dismissed (pattern_id, entity) pairs are filtered out of both
    the global detector output and the row-context single-card path.
  * After scaffold, pending staged rows whose payee matches the
    new pattern + entity get their proposed account flipped to the
    scaffolded path so the user sees the new target on /review
    without typing it in.
"""
from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from lamella.core.db import connect, migrate
from lamella.features.bank_sync.payout_sources import (
    detect_payout_sources,
    read_payout_dismissals,
    reclassify_pending_rows_for_pattern,
)
from lamella.features.import_.staging import StagingService
from lamella.features.review_queue.suggestions import build_suggestion_cards


@pytest.fixture()
def conn() -> sqlite3.Connection:
    c = connect(Path(":memory:"))
    migrate(c)
    return c


@pytest.fixture()
def svc(conn: sqlite3.Connection) -> StagingService:
    return StagingService(conn)


def _seed_account(
    conn: sqlite3.Connection, *,
    account_path: str,
    entity_slug: str,
    simplefin_account_id: str | None = None,
    kind: str | None = "checking",
) -> None:
    if entity_slug:
        conn.execute(
            "INSERT OR IGNORE INTO entities (slug, display_name) VALUES (?, ?)",
            (entity_slug, entity_slug),
        )
    conn.execute(
        "INSERT INTO accounts_meta "
        "    (account_path, display_name, entity_slug, "
        "     simplefin_account_id, kind, is_active, seeded_from_ledger) "
        " VALUES (?, ?, ?, ?, ?, 1, 0)",
        (account_path, account_path.split(":")[-1],
         entity_slug or None, simplefin_account_id, kind),
    )
    conn.commit()


def _stage_inflow(
    svc: StagingService, *,
    sf_account_id: str, posting_date: str, amount: str,
    payee: str, description: str = "",
) -> int:
    return svc.stage(
        source="simplefin",
        source_ref={"account_id": sf_account_id, "txn_id": payee + posting_date},
        posting_date=posting_date,
        amount=amount,
        payee=payee,
        description=description,
    ).id


def _make_dismissal_entry(*, pattern_id: str, entity: str):
    """Build a Custom directive that mirrors what the dismiss
    endpoint writes — single positional ``"<pattern>:<entity>"`` arg
    plus structured metadata."""
    from beancount.core.data import Custom
    return Custom(
        meta={
            "lamella-pattern-id": pattern_id,
            "lamella-entity": entity,
            "filename": "x", "lineno": 1,
        },
        date=date(2026, 4, 25),
        type="payout-source-dismissed",
        values=[f"{pattern_id}:{entity}"],
    )


# --- dismissal reader ----------------------------------------------------


class TestReadPayoutDismissals:
    def test_extracts_pattern_entity_pair(self):
        entry = _make_dismissal_entry(pattern_id="ebay", entity="Acme")
        out = read_payout_dismissals([entry])
        assert out == {("ebay", "Acme")}

    def test_falls_back_to_positional_arg_when_meta_missing(self):
        # Older / hand-edited entries may have only the positional
        # arg. The reader should still pick it up.
        from beancount.core.data import Custom
        entry = Custom(
            meta={"filename": "x", "lineno": 1},
            date=date(2026, 4, 25),
            type="payout-source-dismissed",
            values=["paypal:Personal"],
        )
        assert read_payout_dismissals([entry]) == {("paypal", "Personal")}

    def test_ignores_other_directive_types(self):
        from beancount.core.data import Custom
        entry = Custom(
            meta={"filename": "x", "lineno": 1},
            date=date(2026, 4, 25),
            type="receipt-dismissed",
            values=["abc"],
        )
        assert read_payout_dismissals([entry]) == set()

    def test_empty_input(self):
        assert read_payout_dismissals([]) == set()


# --- detector filters dismissed candidates -------------------------------


class TestDismissalFiltersDetector:
    def test_dismissed_candidate_excluded_from_global(self, conn, svc):
        _seed_account(
            conn,
            account_path="Assets:Acme:Checking",
            entity_slug="Acme",
            simplefin_account_id="acme-chk",
        )
        for d in ("2026-03-24", "2026-04-06", "2026-04-20"):
            _stage_inflow(
                svc, sf_account_id="acme-chk",
                posting_date=d, amount="150.00",
                payee="eBay PAYMENTS",
            )
        # Without dismissal — candidate fires.
        assert len(detect_payout_sources(conn, [])) == 1
        # With dismissal for (ebay, Acme) — filtered out.
        entries = [_make_dismissal_entry(pattern_id="ebay", entity="Acme")]
        assert detect_payout_sources(conn, entries) == []

    def test_dismissal_is_pattern_entity_scoped(self, conn, svc):
        """Dismissing eBay for Acme should NOT suppress the same
        pattern firing for Personal — the user's intent is per-
        entity, not global."""
        _seed_account(
            conn,
            account_path="Assets:Acme:Checking",
            entity_slug="Acme",
            simplefin_account_id="acme-chk",
        )
        _seed_account(
            conn,
            account_path="Assets:Personal:Checking",
            entity_slug="Personal",
            simplefin_account_id="personal-chk",
        )
        for d in ("2026-03-24", "2026-04-06", "2026-04-20"):
            _stage_inflow(
                svc, sf_account_id="acme-chk",
                posting_date=d, amount="150.00", payee="eBay PAYMENTS",
            )
            _stage_inflow(
                svc, sf_account_id="personal-chk",
                posting_date=d, amount="40.00", payee="eBay PAYMENTS",
            )
        entries = [_make_dismissal_entry(pattern_id="ebay", entity="Acme")]
        cands = detect_payout_sources(conn, entries)
        assert len(cands) == 1
        assert cands[0].entity == "Personal"

    def test_dismissal_filters_row_context_card(self, conn):
        _seed_account(
            conn,
            account_path="Assets:Acme:Checking",
            entity_slug="Acme",
        )
        # No dismissal — card fires.
        cards = build_suggestion_cards(
            conn, [],
            context="row",
            row_payee_text="eBay PAYMENTS",
            row_account_path="Assets:Acme:Checking",
        )
        assert len(cards) == 1
        # With dismissal — card suppressed.
        entries = [_make_dismissal_entry(pattern_id="ebay", entity="Acme")]
        cards = build_suggestion_cards(
            conn, entries,
            context="row",
            row_payee_text="eBay PAYMENTS",
            row_account_path="Assets:Acme:Checking",
        )
        assert cards == []


# --- retroactive proposal flip on scaffold -------------------------------


class TestReclassifyPendingRows:
    def test_matching_pending_rows_get_proposal_updated(self, conn, svc):
        _seed_account(
            conn,
            account_path="Assets:Acme:Checking",
            entity_slug="Acme",
            simplefin_account_id="acme-chk",
        )
        ids = []
        for d in ("2026-03-24", "2026-04-06", "2026-04-20"):
            ids.append(_stage_inflow(
                svc, sf_account_id="acme-chk",
                posting_date=d, amount="150.00",
                payee="eBay PAYMENTS",
            ))
        # Seed a pre-existing FIXME proposal so we can prove it
        # gets overwritten, not just inserted.
        conn.execute(
            "INSERT INTO staged_decisions "
            "    (staged_id, account, confidence, decided_by, decided_at) "
            " VALUES (?, ?, 'low', 'rule', datetime('now'))",
            (ids[0], "Expenses:Acme:FIXME"),
        )
        conn.commit()

        touched = reclassify_pending_rows_for_pattern(
            conn,
            pattern_id="ebay",
            entity="Acme",
            target_account="Assets:Acme:eBay",
        )
        assert touched == 3
        rows = conn.execute(
            "SELECT staged_id, account, decided_by FROM staged_decisions "
            " ORDER BY staged_id"
        ).fetchall()
        assert len(rows) == 3
        for r in rows:
            assert r["account"] == "Assets:Acme:eBay"
            assert r["decided_by"] == "payout-detector"

    def test_non_matching_rows_left_alone(self, conn, svc):
        _seed_account(
            conn,
            account_path="Assets:Acme:Checking",
            entity_slug="Acme",
            simplefin_account_id="acme-chk",
        )
        sid_match = _stage_inflow(
            svc, sf_account_id="acme-chk",
            posting_date="2026-04-20", amount="150.00",
            payee="eBay PAYMENTS",
        )
        sid_other = _stage_inflow(
            svc, sf_account_id="acme-chk",
            posting_date="2026-04-21", amount="50.00",
            payee="Local Coffee Shop",
        )
        touched = reclassify_pending_rows_for_pattern(
            conn,
            pattern_id="ebay",
            entity="Acme",
            target_account="Assets:Acme:eBay",
        )
        assert touched == 1
        # The non-matching row got no decision row at all.
        coffee = conn.execute(
            "SELECT COUNT(*) AS n FROM staged_decisions WHERE staged_id = ?",
            (sid_other,),
        ).fetchone()
        assert coffee["n"] == 0
        # The match got the new account.
        ebay = conn.execute(
            "SELECT account FROM staged_decisions WHERE staged_id = ?",
            (sid_match,),
        ).fetchone()
        assert ebay["account"] == "Assets:Acme:eBay"

    def test_other_entity_rows_left_alone(self, conn, svc):
        """Scaffolding eBay for Acme must NOT retag eBay rows that
        landed in Personal's checking — the user only opted that
        one entity in."""
        _seed_account(
            conn,
            account_path="Assets:Acme:Checking",
            entity_slug="Acme",
            simplefin_account_id="acme-chk",
        )
        _seed_account(
            conn,
            account_path="Assets:Personal:Checking",
            entity_slug="Personal",
            simplefin_account_id="personal-chk",
        )
        acme_id = _stage_inflow(
            svc, sf_account_id="acme-chk",
            posting_date="2026-04-20", amount="150.00",
            payee="eBay PAYMENTS",
        )
        personal_id = _stage_inflow(
            svc, sf_account_id="personal-chk",
            posting_date="2026-04-20", amount="40.00",
            payee="eBay PAYMENTS",
        )
        touched = reclassify_pending_rows_for_pattern(
            conn,
            pattern_id="ebay",
            entity="Acme",
            target_account="Assets:Acme:eBay",
        )
        assert touched == 1
        personal = conn.execute(
            "SELECT COUNT(*) AS n FROM staged_decisions WHERE staged_id = ?",
            (personal_id,),
        ).fetchone()
        assert personal["n"] == 0

    def test_unknown_pattern_returns_zero(self, conn, svc):
        assert reclassify_pending_rows_for_pattern(
            conn,
            pattern_id="this_pattern_does_not_exist",
            entity="Acme",
            target_account="Assets:Acme:NoSuchThing",
        ) == 0

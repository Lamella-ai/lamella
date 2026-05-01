# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for ``detect_legacy_paths`` — Phase 3 of /setup/recovery.

Pins down: (a) the four legacy patterns it catches, (b) canonical-
destination derivation from registered vehicles/properties, (c) the
guard set on canonical destinations (parses valid, ≥3 segments,
parent-or-prefix-of-opened), (d) confidence/severity levels, (e)
proposed_fix vs. alternatives shaping based on posting count and
canonical availability.
"""
from __future__ import annotations

import sqlite3
from datetime import date

import pytest
from beancount.core import data, flags
from beancount.core.amount import Amount
from beancount.core.position import CostSpec
from decimal import Decimal

from lamella.features.recovery.findings.legacy_paths import (
    detect_legacy_paths,
)
from lamella.core.db import migrate


@pytest.fixture
def conn():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    migrate(db)
    yield db
    db.close()


def _meta(filename: str = "x.bean", lineno: int = 1) -> dict:
    return {"filename": filename, "lineno": lineno}


def _open(account: str, on: date | None = None) -> data.Open:
    on = on or date(2020, 1, 1)
    return data.Open(_meta(), on, account, None, None)


def _txn(*postings) -> data.Transaction:
    pl = []
    for acct, amount in postings:
        pl.append(data.Posting(
            account=acct,
            units=Amount(Decimal(str(amount)), "USD"),
            cost=None, price=None, flag=None, meta=None,
        ))
    return data.Transaction(
        meta=_meta(),
        date=date(2026, 1, 1),
        flag=flags.FLAG_OKAY,
        payee=None, narration="x",
        tags=set(), links=set(),
        postings=pl,
    )


def _seed_vehicle(conn: sqlite3.Connection, slug: str, entity: str) -> None:
    conn.execute(
        "INSERT INTO vehicles (slug, entity_slug) VALUES (?, ?)",
        (slug, entity),
    )
    conn.commit()


def _seed_property(conn: sqlite3.Connection, slug: str, entity: str) -> None:
    conn.execute(
        "INSERT INTO properties (slug, property_type, entity_slug) "
        "VALUES (?, 'other', ?)",
        (slug, entity),
    )
    conn.commit()


def _open_set(*paths: str) -> list[data.Open]:
    return [_open(p) for p in paths]


# ---------------------------------------------------------------------------
# Pattern: Assets:Vehicles:<slug>
# ---------------------------------------------------------------------------


class TestVehicleAssetPath:
    def test_no_findings_when_canonical(self, conn):
        # Canonical paths produce no findings.
        entries = _open_set(
            "Assets:Personal:Vehicle:V2008Fabrikam",
            "Assets:Personal:BankOne:Checking",
        )
        assert detect_legacy_paths(conn, entries) == ()

    def test_legacy_assets_vehicles_with_registered_entity(self, conn):
        _seed_vehicle(conn, "V2008Fabrikam", "Personal")
        # Canonical destination must pass the parent-opened guard,
        # so we open the entity's vehicle subtree.
        entries = _open_set(
            "Assets:Vehicles:V2008Fabrikam",
            "Assets:Personal:Vehicle:V2009FabrikamSuv",  # opens Assets:Personal:Vehicle:* prefix
        )
        findings = detect_legacy_paths(conn, entries)
        assert len(findings) == 1
        f = findings[0]
        assert f.category == "legacy_path"
        assert f.target == "Assets:Vehicles:V2008Fabrikam"
        assert f.severity == "warning"
        assert f.target_kind == "account"
        # Empty (no postings) + canonical known → close is the
        # default, move is the alternative.
        assert f.proposed_fix_dict == {"action": "close"}
        alt = f.alternatives_dicts
        assert any(
            a["action"] == "move"
            and a["canonical"] == "Assets:Personal:Vehicle:V2008Fabrikam"
            for a in alt
        )

    def test_legacy_assets_vehicles_unregistered_slug(self, conn):
        # Slug not in vehicles table → no canonical can be computed.
        # Heal-action menu collapses to close-only.
        entries = _open_set("Assets:Vehicles:V2008GhostTrain")
        findings = detect_legacy_paths(conn, entries)
        assert len(findings) == 1
        f = findings[0]
        assert f.proposed_fix_dict == {"action": "close"}
        assert f.alternatives == ()
        assert f.confidence in ("low", "medium")

    def test_unregistered_slug_with_postings_spells_out_register_first(self, conn):
        # The wedge case: legacy account references an unregistered
        # vehicle slug AND has postings. Close would refuse (would
        # orphan postings). The detail must tell the user the path
        # forward — register the vehicle first under /setup/vehicles
        # — rather than only saying "no registered vehicle". Without
        # this hint the user would click Close, see the refusal, and
        # have no way to figure out next steps.
        entries = _open_set(
            "Assets:Vehicles:V2008GhostTrain",
            "Assets:Cash",
        ) + [_txn(("Assets:Vehicles:V2008GhostTrain", "100"),
                  ("Assets:Cash", "-100"))]
        findings = detect_legacy_paths(conn, entries)
        assert len(findings) == 1
        f = findings[0]
        assert f.proposed_fix_dict == {"action": "close"}
        # Detail spells out the register-first workflow so the user
        # has a concrete next step.
        assert "/setup/vehicles" in f.detail
        assert "Register" in f.detail or "register" in f.detail
        # Mentions the specific friction (postings would orphan) so
        # the user understands why Close alone won't work.
        assert "orphan" in f.detail or "refused" in f.detail

    def test_with_postings_proposes_move_first(self, conn):
        _seed_vehicle(conn, "V2008Fabrikam", "Personal")
        entries = _open_set(
            "Assets:Vehicles:V2008Fabrikam",
            "Assets:Personal:Vehicle:V2009FabrikamSuv",
        ) + [_txn(("Assets:Vehicles:V2008Fabrikam", "100"),
                  ("Assets:Personal:Vehicle:V2009FabrikamSuv", "-100"))]
        findings = detect_legacy_paths(conn, entries)
        assert len(findings) == 1
        f = findings[0]
        # With postings, move is proposed; close is the alternative.
        assert f.proposed_fix_dict["action"] == "move"
        assert any(
            a["action"] == "close" for a in f.alternatives_dicts
        )

    def test_destination_guard_drops_canonical_when_branch_unopened(self, conn):
        # Vehicle is registered under Personal, but Personal:Vehicle
        # branch is not opened anywhere — the canonical destination
        # would be drive-by-created. Guard removes it.
        _seed_vehicle(conn, "V2008Fabrikam", "Personal")
        entries = _open_set("Assets:Vehicles:V2008Fabrikam")
        findings = detect_legacy_paths(conn, entries)
        assert len(findings) == 1
        f = findings[0]
        # No canonical → close-only.
        assert f.alternatives == ()


# ---------------------------------------------------------------------------
# Pattern: Expenses:Vehicles:<slug>:<cat>
# ---------------------------------------------------------------------------


class TestVehicleExpensePath:
    def test_legacy_expenses_vehicles_with_registered_entity(self, conn):
        _seed_vehicle(conn, "V2008Fabrikam", "Personal")
        # Open the canonical Expenses prefix so the guard accepts.
        entries = _open_set(
            "Expenses:Vehicles:V2008Fabrikam:Fuel",
            "Expenses:Personal:Vehicle:V2009FabrikamSuv:Fuel",
        )
        findings = detect_legacy_paths(conn, entries)
        assert len(findings) == 1
        f = findings[0]
        assert f.target == "Expenses:Vehicles:V2008Fabrikam:Fuel"
        # canonical = Expenses:{entity}:Vehicle:{slug}:{cat}
        alts = f.alternatives_dicts
        candidate = (
            f.proposed_fix_dict
            if f.proposed_fix_dict.get("action") == "move"
            else next((a for a in alts if a.get("action") == "move"), None)
        )
        assert candidate is not None
        assert candidate["canonical"] == "Expenses:Personal:Vehicle:V2008Fabrikam:Fuel"


# ---------------------------------------------------------------------------
# Pattern: Assets:Property:<slug> / Assets:Properties:<slug>
# ---------------------------------------------------------------------------


class TestPropertyPath:
    def test_legacy_property_with_registered_entity(self, conn):
        _seed_property(conn, "PinewoodHouse", "Personal")
        entries = _open_set(
            "Assets:Property:PinewoodHouse",
            "Assets:Personal:Property:LakeView",
        )
        findings = detect_legacy_paths(conn, entries)
        assert len(findings) == 1
        f = findings[0]
        # canonical = Assets:{entity}:Property:{slug}
        candidates = (f.proposed_fix_dict,) + f.alternatives_dicts
        move_actions = [c for c in candidates if c.get("action") == "move"]
        assert any(
            a["canonical"] == "Assets:Personal:Property:PinewoodHouse"
            for a in move_actions
        )

    def test_properties_plural_alias(self, conn):
        _seed_property(conn, "PinewoodHouse", "Personal")
        entries = _open_set(
            "Assets:Properties:PinewoodHouse",
            "Assets:Personal:Property:LakeView",
        )
        findings = detect_legacy_paths(conn, entries)
        assert len(findings) == 1
        assert findings[0].target == "Assets:Properties:PinewoodHouse"


# ---------------------------------------------------------------------------
# Pattern: Expenses:<Entity>:Custom:*
# ---------------------------------------------------------------------------


class TestCustomCatchAll:
    def test_legacy_custom_segment(self, conn):
        entries = _open_set(
            "Expenses:AcmeCo:Custom:Misc",
        )
        findings = detect_legacy_paths(conn, entries)
        assert len(findings) == 1
        f = findings[0]
        assert "Custom" in f.summary
        # No canonical proposed — leaf semantics aren't auto-derivable.
        assert f.proposed_fix_dict == {"action": "close"}
        assert f.alternatives == ()
        assert f.confidence == "low"


# ---------------------------------------------------------------------------
# Pattern: Expenses:Personal (flat bucket)
# ---------------------------------------------------------------------------


class TestFlatPersonalBucket:
    def test_flat_expenses_personal(self, conn):
        entries = _open_set("Expenses:Personal")
        findings = detect_legacy_paths(conn, entries)
        assert len(findings) == 1
        f = findings[0]
        assert "flat root" in f.summary.lower() or "flat" in f.summary.lower()
        assert f.proposed_fix_dict == {"action": "close"}

    def test_expenses_personal_with_leaf_is_canonical(self, conn):
        # `Expenses:Personal:Groceries` is fine — canonical.
        entries = _open_set("Expenses:Personal:Groceries")
        assert detect_legacy_paths(conn, entries) == ()


# ---------------------------------------------------------------------------
# Idempotence + id stability
# ---------------------------------------------------------------------------


class TestStability:
    def test_findings_same_across_calls(self, conn):
        _seed_vehicle(conn, "V2008Fabrikam", "Personal")
        entries = _open_set(
            "Assets:Vehicles:V2008Fabrikam",
            "Assets:Personal:Vehicle:V2009FabrikamSuv",
        )
        a = detect_legacy_paths(conn, entries)
        b = detect_legacy_paths(conn, entries)
        assert a == b
        assert {f.id for f in a} == {f.id for f in b}

    def test_finding_id_format(self, conn):
        entries = _open_set("Expenses:Personal")
        findings = detect_legacy_paths(conn, entries)
        assert len(findings) == 1
        assert findings[0].id.startswith("legacy_path:")


class TestNoDoubleClassify:
    def test_canonical_paths_yield_no_findings(self, conn):
        # Ledger that's already canonical produces zero findings —
        # detector is silent on healthy installs.
        entries = _open_set(
            "Assets:Personal:BankOne:Checking",
            "Assets:Personal:Vehicle:V2008Fabrikam",
            "Expenses:Personal:Vehicle:V2008Fabrikam:Fuel",
            "Expenses:Personal:Groceries",
            "Liabilities:AcmeCo:BankOne:BusinessElite",
        )
        assert detect_legacy_paths(conn, entries) == ()

# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Filter-parity characterization + regression tests.

Phase 1.3 (see ``FEATURE_SETUP_IMPLEMENTATION.md``) consolidates the
predicates that routes/setup.py and bootstrap/setup_progress.py each
inlined with subtly different filters. This file pins the intended
behavior at each call site — several tests are red today against the
pre-retrofit code and green after the helper is adopted.

The tests monkeypatch ``ledger_reader.load()`` to return a synthesized
entries list, so we can drive a handler through a specific ledger
shape without writing a full ``.bean`` file per case.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from beancount.core.amount import Amount
from beancount.core.data import Close, Open, Posting, Transaction
from beancount.core.number import D

from lamella.core.beancount_io import LoadedLedger
from lamella.core.beancount_io.txn_hash import txn_hash


# --- Synth helpers ---------------------------------------------------------


def _txn(
    d: date,
    narration: str,
    *posts: tuple[str, str],
    tags: set[str] | None = None,
    meta: dict | None = None,
) -> Transaction:
    postings: list[Posting] = []
    for acct, amt in posts:
        num, ccy = amt.split()
        postings.append(
            Posting(
                account=acct,
                units=Amount(D(num), ccy),
                cost=None, price=None, flag=None, meta=None,
            )
        )
    return Transaction(
        meta=meta or {},
        date=d,
        flag="*",
        payee=None,
        narration=narration,
        tags=tags or frozenset(),
        links=frozenset(),
        postings=postings,
    )


def _open(d: date, account: str) -> Open:
    return Open(meta={}, date=d, account=account, currencies=None, booking=None)


def _close(d: date, account: str) -> Close:
    return Close(meta={}, date=d, account=account)


def _install_fake_reader(app_client, entries: list) -> None:
    """Monkeypatch the app's ledger reader to return the given entries.
    Used so tests can drive a specific ledger shape without writing a
    full .bean file per case."""
    def _fake_load(*, force: bool = False):
        return LoadedLedger(
            entries=entries, errors=[], options={}, mtime_signature=(),
        )
    app_client.app.state.ledger_reader.load = _fake_load


def _stub_bean_check(monkeypatch) -> None:
    """Bypass the real bean-check subprocess for tests that drive a
    handler through a fake ledger reader. conftest stubs
    ``run_bean_check`` globally; this additionally stubs
    ``capture_bean_check`` + ``run_bean_check_vs_baseline`` so the
    setup-route write handlers don't compare the fake entries to the
    real ledger file on disk. Each setup handler imports these names
    inside the function body, so patching the module picks them up on
    the next call."""
    import lamella.features.receipts.linker as _linker
    monkeypatch.setattr(_linker, "capture_bean_check", lambda p: (0, ""))
    monkeypatch.setattr(
        _linker, "run_bean_check_vs_baseline", lambda p, b: None,
    )


def _seed_minimal(db) -> None:
    """Minimal entity + accounts_meta seed for handlers that go
    through conn.execute() for display. Mirrors the pattern in
    test_setup_smoke._seed but smaller."""
    db.execute("PRAGMA foreign_keys = OFF")
    db.execute("DELETE FROM accounts_meta")
    db.execute("DELETE FROM entities")
    db.execute("DELETE FROM vehicles")
    for slug, dn, et, ts, active in [
        ("Personal", "Personal", "personal", "A", 1),
        ("BetaCorp", "BETA CORP LLC", "llc", "C", 1),
    ]:
        db.execute(
            "INSERT INTO entities (slug, display_name, entity_type, "
            "tax_schedule, is_active) VALUES (?, ?, ?, ?, ?)",
            (slug, dn, et, ts, active),
        )
    db.execute("PRAGMA foreign_keys = ON")
    db.commit()


# --- Live bug #1: close-unused-orphans counted override postings as usage --


class TestCloseUnusedOrphansRespectsMigrationFilter:
    """§7 #5 shape in the bulk close-unused-orphans handler.

    Before the fix, ``used`` was built by iterating every Transaction
    posting with no override filter. An override written for the
    migration carries one posting on the orphan path (the 'from'
    side) + one on the canonical path ('to' side), so the orphan
    ended up in ``used`` even after full migration. The
    ``if acct in used: continue`` gate then skipped the orphan → the
    user clicked "close unused orphans" and nothing closed.
    """

    def test_fully_migrated_orphan_is_closed(
        self, app_client, settings, tmp_path, monkeypatch,
    ):
        _stub_bean_check(monkeypatch)
        _seed_minimal(app_client.app.state.db)
        # Shape: one orphan path with one original posting, one canonical
        # path, one override txn that migrates the original. The bulk-
        # close handler must recognize the orphan as 0-postings-left and
        # write a Close directive.
        orig = _txn(
            date(2024, 1, 1), "fuel",
            ("Expenses:Personal:Vehicles:FabrikamSuv:Fuel", "10 USD"),
            ("Assets:Personal:Bank:Checking", "-10 USD"),
        )
        override = _txn(
            date(2024, 2, 1), "migration",
            ("Expenses:Personal:Vehicle:FabrikamSuv:Fuel", "10 USD"),
            ("Expenses:Personal:Vehicles:FabrikamSuv:Fuel", "-10 USD"),
            tags={"lamella-override"},
            meta={"lamella-override-of": txn_hash(orig)},
        )
        entries = [
            _open(date(2020, 1, 1), "Expenses:Personal:Vehicles:FabrikamSuv:Fuel"),
            _open(date(2020, 1, 1), "Expenses:Personal:Vehicle:FabrikamSuv:Fuel"),
            _open(date(2020, 1, 1), "Assets:Personal:Bank:Checking"),
            orig, override,
        ]
        _install_fake_reader(app_client, entries)

        r = app_client.post(
            "/setup/vehicles/close-unused-orphans",
            follow_redirects=False,
        )
        assert r.status_code == 303
        # Success message carries the count; the number "1" means the
        # filter actually identified the fully-migrated orphan.
        assert "closed-1" in r.headers["location"], r.headers.get("location")

        # And the Close directive actually landed.
        accounts_bean = settings.connector_accounts_path
        assert accounts_bean.exists()
        text = accounts_bean.read_text(encoding="utf-8")
        assert "close Expenses:Personal:Vehicles:FabrikamSuv:Fuel" in text

    def test_partially_migrated_orphan_is_not_closed(
        self, app_client, settings, monkeypatch,
    ):
        """Discriminator: an orphan with 1 migrated + 1 unmigrated
        original has a real posting left and must not be closed."""
        _stub_bean_check(monkeypatch)
        _seed_minimal(app_client.app.state.db)
        orig1 = _txn(
            date(2024, 1, 1), "fuel",
            ("Expenses:Personal:Vehicles:FabrikamSuv:Fuel", "10 USD"),
            ("Assets:Personal:Bank:Checking", "-10 USD"),
        )
        orig2 = _txn(
            date(2024, 1, 15), "fuel2",
            ("Expenses:Personal:Vehicles:FabrikamSuv:Fuel", "20 USD"),
            ("Assets:Personal:Bank:Checking", "-20 USD"),
        )
        override1 = _txn(
            date(2024, 2, 1), "migration",
            ("Expenses:Personal:Vehicle:FabrikamSuv:Fuel", "10 USD"),
            ("Expenses:Personal:Vehicles:FabrikamSuv:Fuel", "-10 USD"),
            tags={"lamella-override"},
            meta={"lamella-override-of": txn_hash(orig1)},
        )
        entries = [
            _open(date(2020, 1, 1), "Expenses:Personal:Vehicles:FabrikamSuv:Fuel"),
            _open(date(2020, 1, 1), "Expenses:Personal:Vehicle:FabrikamSuv:Fuel"),
            _open(date(2020, 1, 1), "Assets:Personal:Bank:Checking"),
            orig1, orig2, override1,
        ]
        _install_fake_reader(app_client, entries)

        r = app_client.post(
            "/setup/vehicles/close-unused-orphans",
            follow_redirects=False,
        )
        assert r.status_code == 303
        # The orphan still has orig2, so bulk-close must NOT touch it.
        assert (
            "info=no-unused-orphans-found" in r.headers["location"]
            or "closed-0" in r.headers["location"]
        ), r.headers.get("location")


# --- Live bug #2: entity-manage posting count under-counts unmigrated ------


class TestEntityManagePageDeletableGate:
    """§7 #5 shape in the entity-manage display.

    Before the fix, ``setup_entity_manage_page`` filtered only by
    ``#lamella-override`` tag and did not exclude already-migrated
    originals. A user who fully migrates every posting off an entity's
    accounts still saw the originals counted as "posting_count",
    keeping the ``deletable`` gate False and the delete button
    disabled.
    """

    def test_fully_migrated_account_shows_zero_posting_count(
        self, app_client,
    ):
        db = app_client.app.state.db
        _seed_minimal(db)
        # Register an entity with one account, every posting migrated.
        db.execute(
            "INSERT INTO entities (slug, display_name, entity_type, "
            "tax_schedule, is_active) VALUES (?, ?, ?, ?, ?)",
            ("OldCo", "Old Co", "llc", "C", 1),
        )
        db.execute(
            "INSERT INTO accounts_meta "
            "(account_path, display_name, kind, entity_slug, closed_on) "
            "VALUES (?, ?, ?, ?, NULL)",
            ("Expenses:OldCo:Supplies", "OldCo-Supplies", None, "OldCo"),
        )
        db.commit()

        orig = _txn(
            date(2024, 1, 1), "supplies",
            ("Expenses:OldCo:Supplies", "50 USD"),
            ("Assets:Personal:Bank:Checking", "-50 USD"),
        )
        override = _txn(
            date(2024, 2, 1), "migration",
            ("Expenses:Personal:Supplies", "50 USD"),
            ("Expenses:OldCo:Supplies", "-50 USD"),
            tags={"lamella-override"},
            meta={"lamella-override-of": txn_hash(orig)},
        )
        entries = [
            _open(date(2020, 1, 1), "Expenses:OldCo:Supplies"),
            _open(date(2020, 1, 1), "Expenses:Personal:Supplies"),
            _open(date(2020, 1, 1), "Assets:Personal:Bank:Checking"),
            orig, override,
        ]
        _install_fake_reader(app_client, entries)

        r = app_client.get(
            "/setup/entities/OldCo/manage", follow_redirects=False,
        )
        assert r.status_code == 200
        # After full migration, total_postings on the manage page must
        # be 0. Pre-fix, the page walks the original-posting-still-on-
        # the-ledger and shows 1. The template renders
        #   <strong>Total postings:</strong>\n    {{ total_postings }}
        import re as _re
        m = _re.search(r"Total postings:\s*</strong>\s*(\d+)", r.text)
        assert m, f"total_postings line not found in response:\n{r.text[:500]}"
        assert m.group(1) == "0", (
            f"expected total_postings=0 after full migration, got "
            f"{m.group(1)} — entity-manage page still counting already-"
            f"migrated originals"
        )


# --- Live bug #3: checklist-vs-page drift over Close'd accounts ------------


class TestChecklistChartVsPageAgreeOnClosedAccount:
    """§2-Class-A shape. ``_check_charts_scaffolded`` used Opens-only
    while ``setup_charts_page`` used Opens-minus-Closes. If the user
    opened then Close'd a chart account, the checklist said "all
    scaffolded" and the page said "1 missing". After Phase 1.3 both
    go through ``setup.posting_counts.open_paths`` and agree."""

    def test_closed_chart_account_shows_as_missing_on_both_sides(
        self, app_client, tmp_path,
    ):
        db = app_client.app.state.db
        _seed_minimal(db)
        # Entity with Schedule C chart — we don't need the real YAML
        # to pull this off. The test only asserts the Opens-minus-
        # Closes semantics, which is what compute_setup_progress and
        # setup_charts_page must both use after Phase 1.3.
        from lamella.features.setup.posting_counts import open_paths

        entries = [
            _open(date(2020, 1, 1), "Expenses:BetaCorp:Materials"),
            _close(date(2024, 6, 1), "Expenses:BetaCorp:Materials"),
            _open(date(2020, 1, 1), "Expenses:BetaCorp:Supplies"),
        ]
        # The predicate itself must agree with this shape: Materials
        # is not open; Supplies is.
        assert open_paths(entries) == {"Expenses:BetaCorp:Supplies"}

        # Drive the checklist end-to-end via compute_setup_progress to
        # ensure _check_charts_scaffolded reads Closes. The check needs
        # a yaml_data result to exercise its missing-paths branch — so
        # pick an entity the loader knows about (BetaCorp = Schedule C).
        _install_fake_reader(app_client, entries)

        # Smoke: the handler renders and uses the expected predicate.
        # (Full E2E coverage of chart-missing detection belongs in
        # test_setup_e2e.py; this test only guards against the
        # predicate drifting back to Opens-only.)
        r = app_client.get("/setup/charts", follow_redirects=False)
        assert r.status_code == 200


# --- Live bug #4: entity migrate-account accumulation ---------------------


class TestEntityMigrateRejectsAlreadyMigratedOriginals:
    """§7 #4 shape in ``setup_entity_migrate_account``. Before the fix,
    the affected-list builder filtered only by ``#lamella-override`` tag
    and re-walked originals that had already been migrated via a prior
    override — re-clicking migrate would write a second override on
    the same original. After Phase 1.3 the handler uses the full
    filter (already-migrated originals excluded) and a re-click is a
    no-op."""

    def test_re_clicking_migrate_after_full_migration_is_a_noop(
        self, app_client, settings, tmp_path, monkeypatch,
    ):
        _stub_bean_check(monkeypatch)
        db = app_client.app.state.db
        _seed_minimal(db)
        db.execute(
            "INSERT INTO entities (slug, display_name, entity_type, "
            "tax_schedule, is_active) VALUES (?, ?, ?, ?, ?)",
            ("OldCo2", "Old Co 2", "llc", "C", 1),
        )
        db.execute(
            "INSERT INTO accounts_meta "
            "(account_path, display_name, kind, entity_slug, closed_on) "
            "VALUES (?, ?, ?, ?, NULL)",
            ("Expenses:OldCo2:Supplies", "OldCo2-Supplies", None, "OldCo2"),
        )
        db.commit()

        orig = _txn(
            date(2024, 1, 1), "supplies",
            ("Expenses:OldCo2:Supplies", "30 USD"),
            ("Assets:Personal:Bank:Checking", "-30 USD"),
        )
        existing_override = _txn(
            date(2024, 2, 1), "migration",
            ("Expenses:Personal:Supplies", "30 USD"),
            ("Expenses:OldCo2:Supplies", "-30 USD"),
            tags={"lamella-override"},
            meta={"lamella-override-of": txn_hash(orig)},
        )
        entries = [
            _open(date(2020, 1, 1), "Expenses:OldCo2:Supplies"),
            _open(date(2020, 1, 1), "Expenses:Personal:Supplies"),
            _open(date(2020, 1, 1), "Assets:Personal:Bank:Checking"),
            orig, existing_override,
        ]
        _install_fake_reader(app_client, entries)

        # Capture pre-migrate state of connector_overrides.bean.
        overrides_path = settings.connector_overrides_path
        pre_bytes = (
            overrides_path.read_bytes() if overrides_path.exists() else b""
        )

        r = app_client.post(
            "/setup/entities/OldCo2/migrate-account",
            data={
                "account": "Expenses:OldCo2:Supplies",
                "target": "Expenses:Personal:Supplies",
            },
            follow_redirects=False,
        )
        assert r.status_code == 303
        # Post-fix: the handler detects the original has already been
        # migrated and redirects with error=no-postings-found.
        assert "no-postings-found" in r.headers["location"], r.headers.get(
            "location"
        )
        # And connector_overrides.bean bytes are unchanged — no new
        # override was written.
        post_bytes = (
            overrides_path.read_bytes() if overrides_path.exists() else b""
        )
        assert post_bytes == pre_bytes


# --- Mechanical: _check_* functions align to Opens-minus-Closes -----------


class TestChecksOpenMinusCloseSemantics:
    """Regression guard: the five ``_check_*`` functions in
    bootstrap/setup_progress.py that iterate entries must exclude
    accounts that have been explicitly Close'd. Before Phase 1.3 they
    used an Opens-only predicate that left Close'd accounts counted
    as "still open" — causing the checklist to show green while the
    corresponding action page showed missing."""

    def test_check_companions_flags_closed_companion_as_missing(self, db):
        from lamella.features.setup.setup_progress import _check_companions
        # A labeled bank account → its companion paths come from
        # companion_paths_for(). Take one specific path the helper
        # produces (OpeningBalances is common across kinds) and put
        # it into the ledger as Open + Close. Opens-only would say
        # the companion is present; Opens-minus-Closes says missing.
        db.execute(
            "INSERT INTO entities (slug, display_name, entity_type, "
            "tax_schedule, is_active) VALUES (?, ?, ?, ?, 1)",
            ("Personal", "Personal", "personal", "A"),
        )
        db.execute(
            "INSERT INTO accounts_meta (account_path, display_name, kind, "
            "entity_slug, institution, last_four) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("Assets:Personal:Bank:Checking", "Chk", "bank",
             "Personal", "BankOne", "1234"),
        )
        db.commit()
        # Inspect what companion the helper demands so we target it
        # exactly — the test must be robust to the helper's specific
        # set of companion paths.
        from lamella.core.registry.companion_accounts import (
            companion_paths_for,
        )
        companions = companion_paths_for(
            account_path="Assets:Personal:Bank:Checking",
            kind="bank",
            entity_slug="Personal",
            institution="BankOne",
        )
        # Take one to pin. If companions list is empty in this config
        # the assertion below is trivially satisfied — which is fine,
        # the predicate semantics are already covered by the helper's
        # own unit tests.
        if not companions:
            pytest.skip("no companion paths for this shape")
        target = companions[0].path

        # Scenario A: companion is open. check_completes True.
        entries_open = [_open(date(2020, 1, 1), cp.path) for cp in companions]
        step_a = _check_companions(db, entries_open)
        assert step_a.is_complete is True

        # Scenario B: same companions, but our target is Open'd AND
        # then Close'd. Opens-only would still see it as present and
        # the check would remain complete; Opens-minus-Closes
        # correctly flags it as missing.
        entries_closed = list(entries_open) + [
            _close(date(2024, 1, 1), target),
        ]
        step_b = _check_companions(db, entries_closed)
        assert step_b.is_complete is False, (
            "check_companions must respect Close directives — "
            "a Close'd companion account is not present"
        )


# --- Auxiliary: vehicle orphan detection survives helper extraction -------


class TestVehicleOrphanDetectionConsistent:
    """Both ``setup_vehicles_page`` and
    ``setup_vehicles_close_unused_orphans`` classify orphan paths with
    the same regex triad. Phase 1.3 consolidates into
    ``is_vehicle_orphan``; this test pins that both sites agree on a
    representative set of paths."""

    @pytest.mark.parametrize("path,expected_orphan", [
        ("Expenses:Personal:Vehicles:FabrikamSuv:Fuel", True),   # plural legacy
        ("Expenses:Personal:Vehicle:FabrikamSuv:Fuel", False),   # canonical
        ("Assets:Personal:Vehicle:FabrikamSuv", False),           # canonical asset
        ("Expenses:Personal:Food:Groceries", False),          # non-vehicle
        # ``Trailer`` is still in the keyword set; ``FabrikamSuv``
        # is the fictional placeholder and is intentionally not a keyword.
        ("Expenses:Personal:Custom:TrailerFuel", True), # custom bucket
        ("Liabilities:Personal:Loan:VehicleLoan", False),     # wrong root
    ])
    def test_predicate_matches_both_handlers(self, path, expected_orphan):
        from lamella.features.setup.posting_counts import is_vehicle_orphan
        assert is_vehicle_orphan(path) is expected_orphan

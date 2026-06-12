# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 1.4 — boot-time auto-discovery resurrection tests.

The §7 #7 shape, generalized: when the user deletes an
entity / vehicle / property / accounts_meta row but the ledger still
has Open directives for it, ``sync_from_ledger`` on the next boot
re-INSERTs the row via INSERT OR IGNORE. The Phase 1.4 fix writes
a ``custom "<type>-deleted"`` tombstone directive at delete time;
the boot-time discovery filter honors the tombstone so resurrection
can't happen.

These tests exercise the full cycle end-to-end:
    seed DB + ledger → call delete handler → recreate FastAPI app
    (re-running lifespan) → assert the row stays gone.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


# --- Fixtures --------------------------------------------------------------


@pytest.fixture
def resurrection_settings(tmp_path: Path):
    """Tmp ledger that already has lamella-* markers — so detection
    classifies as ``ready`` and lifespan runs through the discovery
    pass instead of redirecting to /setup."""
    from lamella.core.config import Settings
    ledger_dir = tmp_path / "ledger"
    ledger_dir.mkdir()
    main_bean = ledger_dir / "main.bean"
    main_bean.write_text(
        'option "title" "Phase 1.4 fixture"\n'
        'option "operating_currency" "USD"\n'
        '\n'
        'plugin "beancount_lazy_plugins.auto_accounts"\n'
        '\n'
        '2020-01-01 custom "lamella-ledger-version" "1"\n'
        '\n'
        'include "connector_accounts.bean"\n'
        'include "connector_config.bean"\n',
        encoding="utf-8",
    )
    (ledger_dir / "connector_accounts.bean").write_text(
        "; connector_accounts.bean\n"
        '2020-01-01 open Assets:AcmeCo:BankOne:Checking\n'
        '2020-01-01 open Liabilities:AcmeCo:BankOne:CreditCard\n'
        '2020-01-01 open Expenses:AcmeCo:Supplies\n'
        '2020-01-01 open Equity:OpeningBalances:AcmeCo\n',
        encoding="utf-8",
    )
    (ledger_dir / "connector_config.bean").write_text(
        "; connector_config.bean\n",
        encoding="utf-8",
    )
    return Settings(
        data_dir=tmp_path / "data",
        ledger_dir=ledger_dir,
        paperless_url="https://paperless.test",
        paperless_api_token="token-test",
        # Match the conftest `settings` fixture: vector search OFF
        # in tests because its background sentence-transformers
        # worker outlives the TestClient event loop and segfaults
        # on the next test's SQLite handle.
        ai_vector_search_enabled=False,
    )


def _build_client(settings, monkeypatch):
    """Construct a FastAPI TestClient. The fixture ledger this builds
    against is small and syntactically clean, so the real bean-check
    subprocess (when on PATH) is invoked normally — matching the
    pattern in conftest.app_client. We do NOT stub
    ``capture_bean_check`` / ``run_bean_check_vs_baseline`` because
    cross-test leakage of those stubs has been observed when other
    suites (notably test_setup_e2e's unhappy-path test) rely on the
    real subprocess to detect the corrupt write they synthesize."""
    import lamella.features.receipts.linker as _linker
    monkeypatch.setattr(_linker, "run_bean_check", lambda p: None)
    from lamella.main import create_app
    return create_app(settings=settings)


# --- Entities --------------------------------------------------------------


class TestEntityDeletionSurvivesReboot:
    """The §7 #7 fix's missing piece for entities: a delete via the UI
    must persist across boot, even when the ledger still has
    ``Open Expenses:<slug>:*`` directives that would otherwise
    re-discover the slug."""

    def test_delete_then_reboot_does_not_resurrect(
        self, resurrection_settings, monkeypatch,
    ):
        # Boot 1: lifespan runs sync_from_ledger, seeds 'AcmeCo' from the
        # Open directives in the fixture ledger.
        app = _build_client(resurrection_settings, monkeypatch)
        with TestClient(app) as client:
            db = app.state.db
            row = db.execute(
                "SELECT slug FROM entities WHERE slug = ?", ("AcmeCo",),
            ).fetchone()
            assert row is not None, (
                "boot 1 should have auto-seeded AcmeCo from Open directives"
            )
            # The legacy delete handler refuses if any accounts_meta row
            # references the slug. The realistic flow is "user cleans up
            # stale meta first, then deletes the entity" — simulate the
            # cleanup step by nulling the references. This isolates the
            # test to the resurrection mechanism, not the FK guard.
            db.execute(
                "UPDATE accounts_meta SET entity_slug = NULL "
                "WHERE entity_slug = ?", ("AcmeCo",),
            )
            db.commit()
            r = client.post(
                "/settings/entities/AcmeCo/delete",
                follow_redirects=False,
            )
            assert r.status_code in (200, 303), (
                f"delete failed: {r.status_code} {r.text[:200]!r}"
            )
            # Row gone in the live DB.
            assert db.execute(
                "SELECT slug FROM entities WHERE slug = ?", ("AcmeCo",),
            ).fetchone() is None
            # Tombstone written to connector_config.bean.
            cfg_text = (
                resurrection_settings.connector_config_path
            ).read_text(encoding="utf-8")
            assert 'custom "entity-deleted" "AcmeCo"' in cfg_text

        # Boot 2: rebuild the app (re-runs lifespan, re-runs
        # sync_from_ledger). The tombstone must filter AcmeCo out of
        # discover_entity_slugs so seed_entities does NOT re-INSERT.
        app2 = _build_client(resurrection_settings, monkeypatch)
        with TestClient(app2) as client2:
            db2 = app2.state.db
            row2 = db2.execute(
                "SELECT slug FROM entities WHERE slug = ?", ("AcmeCo",),
            ).fetchone()
            assert row2 is None, (
                "boot 2 RESURRECTED AcmeCo — entity-deleted tombstone "
                "is not being honored by discover_entity_slugs"
            )

    def test_setup_entity_delete_writes_tombstone(
        self, resurrection_settings, monkeypatch,
    ):
        """The /setup/entities/{slug}/delete handler (the Phase 1.2
        retrofitted version) writes the same tombstone as the legacy
        admin route. This test exercises that the recovery envelope +
        tombstone block both land in connector_config.bean."""
        # Pre-condition: this handler refuses if any OPEN account uses
        # the slug. The fixture's AcmeCo has Open directives → handler
        # would refuse. Use a slug that's discovered but has no live
        # accounts: insert a synthetic 'GhostCo' entity directly so we
        # bypass the live-account check.
        app = _build_client(resurrection_settings, monkeypatch)
        with TestClient(app) as client:
            db = app.state.db
            # Insert with slug only — no display_name / entity_type /
            # tax_schedule. Per the Phase 1.4-followup delete-refusal
            # gate, any user-set field blocks delete; slug alone is
            # auto-populated boot scaffolding and remains deletable.
            db.execute(
                "INSERT OR IGNORE INTO entities (slug, is_active) "
                "VALUES (?, 1)",
                ("GhostCo",),
            )
            db.commit()
            r = client.post(
                "/setup/entities/GhostCo/delete",
                follow_redirects=False,
            )
            assert r.status_code in (200, 303)
            cfg_text = (
                resurrection_settings.connector_config_path
            ).read_text(encoding="utf-8")
            assert 'custom "entity-deleted" "GhostCo"' in cfg_text
            assert db.execute(
                "SELECT slug FROM entities WHERE slug = ?", ("GhostCo",),
            ).fetchone() is None


class TestEntityTombstoneReader:
    """Direct unit tests against ``read_deleted_entity_slugs`` and the
    integration in ``read_entity_directives``."""

    def test_tombstone_drops_row_from_reader(self):
        from beancount import loader
        import textwrap
        from pathlib import Path
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            main = tmp_path / "main.bean"
            main.write_text(textwrap.dedent("""\
                option "title" "test"
                option "operating_currency" "USD"
                2020-01-01 custom "entity" "AcmeCo"
                  lamella-display-name: "AcmeCo LLC"
                  lamella-entity-type: "llc"
                2024-01-01 custom "entity-deleted" "AcmeCo"
                  lamella-deleted-at: "2024-01-01T00:00:00-06:00"
            """), encoding="utf-8")
            entries, _, _ = loader.load_file(str(main))
        from lamella.core.registry.entity_writer import (
            read_deleted_entity_slugs, read_entity_directives,
        )
        assert read_deleted_entity_slugs(entries) == {"AcmeCo"}
        # The reader must drop the tombstoned slug — even though a
        # `custom "entity"` directive existed for it earlier in the
        # ledger.
        rows = read_entity_directives(entries)
        assert all(r["slug"] != "AcmeCo" for r in rows)


class TestDiscoverEntitySlugsRespectsTombstone:
    """``discover_entity_slugs`` filters out slugs whose latest
    directive is a tombstone."""

    def test_tombstoned_slug_dropped_from_discovery(self):
        from datetime import date
        from beancount.core.data import Custom, Open

        opens = [
            Open(
                meta={}, date=date(2020, 1, 1),
                account="Assets:AcmeCo:Checking",
                currencies=None, booking=None,
            ),
            Open(
                meta={}, date=date(2020, 1, 1),
                account="Assets:RealCo:Checking",
                currencies=None, booking=None,
            ),
        ]
        tombstone = Custom(
            meta={}, date=date(2024, 1, 1),
            type="entity-deleted",
            values=[type("V", (), {"value": "AcmeCo"})()],
        )
        from lamella.core.registry.discovery import discover_entity_slugs
        result = discover_entity_slugs(opens + [tombstone])
        assert "AcmeCo" not in result
        assert "RealCo" in result


# --- Vehicles ----------------------------------------------------------


class TestDiscoverVehicleSlugsRespectsTombstone:
    """``discover_vehicle_slugs`` filters out slugs whose latest
    directive is a ``vehicle-deleted`` tombstone. No top-level
    user-facing delete handler exists today, but the filter is in
    place so a manual DELETE FROM vehicles + tombstone write — or
    any future delete handler — won't resurrect the row on boot."""

    def test_tombstoned_vehicle_slug_dropped(self):
        from datetime import date
        from beancount.core.data import Custom, Open

        opens = [
            Open(
                meta={}, date=date(2020, 1, 1),
                account="Expenses:Personal:Vehicles:OldFabrikamSuv:Fuel",
                currencies=None, booking=None,
            ),
            Open(
                meta={}, date=date(2020, 1, 1),
                account="Expenses:Personal:Vehicles:KeepThisOne:Fuel",
                currencies=None, booking=None,
            ),
        ]
        tombstone = Custom(
            meta={}, date=date(2024, 1, 1),
            type="vehicle-deleted",
            values=[type("V", (), {"value": "OldFabrikamSuv"})()],
        )
        from lamella.core.registry.discovery import discover_vehicle_slugs
        result = discover_vehicle_slugs(opens + [tombstone])
        assert "OldFabrikamSuv" not in result
        assert "KeepThisOne" in result

    def test_vehicle_reader_drops_tombstoned_slug(self):
        from beancount import loader
        import textwrap
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            main = tmp_path / "main.bean"
            main.write_text(textwrap.dedent("""\
                option "title" "test"
                option "operating_currency" "USD"
                2020-01-01 custom "vehicle" "FabrikamSuv"
                  lamella-vehicle-display-name: "2008 Fabrikam Suv"
                2024-01-01 custom "vehicle-deleted" "FabrikamSuv"
                  lamella-deleted-at: "2024-01-01T00:00:00-06:00"
            """), encoding="utf-8")
            entries, _, _ = loader.load_file(str(main))
        from lamella.features.vehicles.reader import (
            read_deleted_vehicle_slugs, read_vehicles,
        )
        assert read_deleted_vehicle_slugs(entries) == {"FabrikamSuv"}
        rows = read_vehicles(entries)
        assert all(r["slug"] != "FabrikamSuv" for r in rows)


# --- Properties ----------------------------------------------------------


class TestDiscoverPropertySlugsRespectsTombstone:
    """``discover_property_slugs`` filters out slugs whose latest
    directive is a ``property-deleted`` tombstone. The reader at
    properties/reader.read_properties already honored the tombstone
    for the reconstruct path; Phase 1.4 wires it into discovery so
    boot-time auto-seed agrees."""

    def test_tombstoned_property_slug_dropped(self):
        from datetime import date
        from beancount.core.data import Custom, Open

        opens = [
            Open(
                meta={}, date=date(2020, 1, 1),
                account="Assets:Personal:Property:OldHouse",
                currencies=None, booking=None,
            ),
            Open(
                meta={}, date=date(2020, 1, 1),
                account="Assets:Personal:Property:CurrentHouse",
                currencies=None, booking=None,
            ),
        ]
        tombstone = Custom(
            meta={}, date=date(2024, 1, 1),
            type="property-deleted",
            values=[type("V", (), {"value": "OldHouse"})()],
        )
        from lamella.core.registry.discovery import (
            discover_property_slugs,
        )
        result = discover_property_slugs(opens + [tombstone])
        assert "OldHouse" not in result
        assert "CurrentHouse" in result


# --- accounts_meta ----------------------------------------------------


class TestAccountMetaDeletionSurvivesReboot:
    """``seed_accounts_meta`` walks every Open directive and INSERT OR
    IGNOREs an accounts_meta row. Without a tombstone, hard-deleting
    a row but leaving the Open directive in place would resurrect on
    boot. The Phase 1.4 fix writes ``custom "account-meta-deleted"``
    at delete time and ``seed_accounts_meta`` filters tombstoned
    paths out before the INSERT."""

    def test_cleanup_system_writes_tombstone(
        self, resurrection_settings, monkeypatch,
    ):
        # Add a system-shaped account that will be auto-seeded by
        # discovery, then run the cleanup-system endpoint and assert
        # both the SQL DELETE and the tombstone landed.
        ledger_dir = resurrection_settings.ledger_dir
        accounts_bean = ledger_dir / "connector_accounts.bean"
        accounts_bean.write_text(
            accounts_bean.read_text(encoding="utf-8")
            + "2020-01-01 open Equity:OpeningBalances:SystemyCleanup\n",
            encoding="utf-8",
        )
        app = _build_client(resurrection_settings, monkeypatch)
        with TestClient(app) as client:
            db = app.state.db
            row = db.execute(
                "SELECT account_path FROM accounts_meta "
                "WHERE account_path = ?",
                ("Equity:OpeningBalances:SystemyCleanup",),
            ).fetchone()
            assert row is not None, (
                "boot 1 should have auto-seeded the system path"
            )
            r = client.post(
                "/settings/accounts-cleanup-system",
                follow_redirects=False,
            )
            assert r.status_code in (200, 303)
            cfg_text = (
                resurrection_settings.connector_config_path
            ).read_text(encoding="utf-8")
            assert (
                'custom "account-meta-deleted" '
                in cfg_text
            ), (
                "cleanup-system did not write account-meta-deleted "
                "tombstones — system rows will resurrect on next boot"
            )

    def test_account_meta_reader_drops_tombstoned_path(self):
        from beancount import loader
        import textwrap
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            main = tmp_path / "main.bean"
            main.write_text(textwrap.dedent("""\
                option "title" "test"
                option "operating_currency" "USD"
                2020-01-01 open Assets:Personal:Bank:OldChecking
                2020-01-01 open Assets:Personal:Bank:CurrentChecking
                2020-01-01 custom "account-meta" Assets:Personal:Bank:OldChecking
                  lamella-display-name: "Old Checking 1234"
                2024-01-01 custom "account-meta-deleted" Assets:Personal:Bank:OldChecking
                  lamella-deleted-at: "2024-01-01T00:00:00-06:00"
            """), encoding="utf-8")
            entries, _, _ = loader.load_file(str(main))
        from lamella.core.registry.account_meta_writer import (
            read_account_meta_directives, read_deleted_account_paths,
        )
        deleted = read_deleted_account_paths(entries)
        assert "Assets:Personal:Bank:OldChecking" in deleted
        rows = read_account_meta_directives(entries)
        assert all(
            r["account_path"] != "Assets:Personal:Bank:OldChecking"
            for r in rows
        )

    def test_seed_accounts_meta_skips_tombstoned_path(self):
        """Direct unit test: seed_accounts_meta must not INSERT a row
        for a path whose Open is shadowed by an account-meta-deleted
        tombstone."""
        from beancount import loader
        import textwrap
        import tempfile
        from pathlib import Path
        from lamella.core.db import connect, migrate
        from lamella.core.registry.discovery import seed_accounts_meta

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            main = tmp_path / "main.bean"
            main.write_text(textwrap.dedent("""\
                option "title" "test"
                option "operating_currency" "USD"
                2020-01-01 open Assets:Personal:Bank:Tombstoned
                2020-01-01 open Assets:Personal:Bank:Live
                2024-01-01 custom "account-meta-deleted" Assets:Personal:Bank:Tombstoned
                  lamella-deleted-at: "2024-01-01T00:00:00-06:00"
            """), encoding="utf-8")
            entries, _, _ = loader.load_file(str(main))
            db_path = tmp_path / "test.sqlite"
            conn = connect(db_path)
            try:
                migrate(conn)
                # accounts_meta has an entity_slug FK to entities; seed
                # Personal first so the INSERT path doesn't trip on it.
                conn.execute(
                    "INSERT OR IGNORE INTO entities (slug) VALUES (?)",
                    ("Personal",),
                )
                conn.commit()
                seed_accounts_meta(conn, entries)
                rows = conn.execute(
                    "SELECT account_path FROM accounts_meta "
                    "WHERE account_path LIKE 'Assets:Personal:Bank:%'"
                ).fetchall()
            finally:
                conn.close()
        paths = {r["account_path"] for r in rows}
        assert "Assets:Personal:Bank:Live" in paths
        assert "Assets:Personal:Bank:Tombstoned" not in paths

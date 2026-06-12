# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""End-to-end smoke audit of every /setup page and action.

Seeds a DB with representative states (entities + accounts in every
shape the UI supports), then hits every route and asserts it returns
200/303 with parseable HTML. Catches template errors, 500s from
missing imports, broken SQL, and other regressions before they reach
the deployed server.

Run: ``uv run pytest tests/test_setup_smoke.py -v``
"""
from __future__ import annotations

import pytest


def _seed(db) -> None:
    # Disable FKs around the seed — we DELETE and re-populate entities +
    # accounts_meta wholesale, and the FK (accounts_meta.entity_slug →
    # entities.slug) would block the DELETE FROM entities otherwise.
    db.execute("PRAGMA foreign_keys = OFF")
    db.execute("DELETE FROM accounts_meta")
    db.execute("DELETE FROM entities")
    for slug, dn, et, ts, active in [
        ("Personal",  "Personal",            "personal", "A",  1),
        ("BetaCorp",    "BETA CORP LLC",         "llc",      "C",  1),
        ("AcmeCo",    "AcmeCo LLC",          "llc",      "C",  1),
        ("NewLLC",    "Newly Discovered",    None,       None, 1),
        ("Clearing",  "Clearing",            "skip",     None, 1),
        ("Retained",  None,                  None,       None, 1),
        ("Property",  "Property",            None,       None, 0),
    ]:
        db.execute(
            "INSERT INTO entities (slug, display_name, entity_type, tax_schedule, is_active) "
            "VALUES (?, ?, ?, ?, ?)", (slug, dn, et, ts, active),
        )
    for path, dn, kind, ent, inst, l4, sfid, closed in [
        ("Assets:Personal:Bank:Checking",         "Chk", "bank",        "Personal", "WF", "1234", "sf1", None),
        ("Liabilities:BetaCorp:BankOne:AffiliateD", "CS",  "credit_card", "BetaCorp",   "WF", "5678", "sf2", None),
        ("Liabilities:AcmeCo:BankOne:AffiliateD", "GS",  "credit_card", "AcmeCo",   "WF", "9012", None,  None),
        ("Liabilities:BetaCorp:Payable:ToAcmeCo",   "PCG", "virtual",     "BetaCorp",   None, None,   None,  None),
        ("Liabilities:AcmeCo:Payable:ToBetaCorp",   "PGC", "virtual",     "AcmeCo",   None, None,   None,  None),
        ("Assets:Personal:Bank:Savings",          "Sv",  None,          None,       None, None,   None,  None),
        ("Equity:Personal:OpeningBalances",       "OB",  None,          "OpeningBalances", None, None, None, None),
        ("Equity:Retained:Archived",              "Ar",  None,          "Retained", None, None,   None,  "2024-01-01"),
    ]:
        db.execute(
            "INSERT INTO accounts_meta (account_path, display_name, kind, entity_slug, "
            "institution, last_four, simplefin_account_id, closed_on) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (path, dn, kind, ent, inst, l4, sfid, closed),
        )
    db.execute("PRAGMA foreign_keys = ON")
    db.commit()


SETUP_GET_PAGES = [
    "/setup",
    # /setup/progress is a 302 alias as of Phase 7; the canonical
    # post-install drift surface is /setup/recovery, exercised
    # below by tests/test_setup_recovery_route.py.
    "/setup/entities",
    "/setup/accounts",
    "/setup/charts",
    "/setup/vehicles",
    "/setup/properties",
    "/setup/loans",
    "/setup/simplefin",
    "/setup/import-rewrite",
    "/setup/welcome",
]

MANAGE_SLUGS = ["Personal", "BetaCorp", "AcmeCo", "Clearing", "Retained"]


class TestSetupPagesRender:
    def test_all_setup_get_pages_render(self, app_client):
        _seed(app_client.app.state.db)
        for url in SETUP_GET_PAGES:
            r = app_client.get(url, follow_redirects=False)
            assert r.status_code in (200, 303), (
                f"{url}: status={r.status_code} body={r.text[:200]!r}"
            )

    def test_manage_pages_render_for_every_entity(self, app_client):
        _seed(app_client.app.state.db)
        for slug in MANAGE_SLUGS:
            r = app_client.get(f"/setup/entities/{slug}/manage", follow_redirects=False)
            assert r.status_code in (200, 303), (
                f"/setup/entities/{slug}/manage: {r.status_code} {r.text[:200]!r}"
            )

    def test_manage_page_for_missing_entity_redirects(self, app_client):
        _seed(app_client.app.state.db)
        r = app_client.get("/setup/entities/DoesNotExist/manage", follow_redirects=False)
        assert r.status_code == 303

    @pytest.mark.xfail(
        reason="pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md",
        strict=False,
    )
    def test_settings_and_dashboard_render(self, app_client):
        _seed(app_client.app.state.db)
        for url in ["/settings/entities", "/settings", "/", "/jobs/active/dock"]:
            r = app_client.get(url, follow_redirects=False)
            assert r.status_code in (200, 303), (
                f"{url}: {r.status_code} {r.text[:200]!r}"
            )


class TestSetupActions:
    def test_entity_save_returns_banner_oob(self, app_client):
        _seed(app_client.app.state.db)
        r = app_client.post(
            "/setup/entities/NewLLC/save",
            data={"entity_type": "llc", "tax_schedule": "C", "display_name": "New LLC"},
            headers={"HX-Request": "true"},
        )
        assert r.status_code == 200
        assert "hx-swap-oob" in r.text
        assert "setup-entities-banner" in r.text

    def test_entity_skip_flow(self, app_client):
        _seed(app_client.app.state.db)
        r = app_client.post(
            "/setup/entities/NewLLC/skip",
            headers={"HX-Request": "true"},
            follow_redirects=False,
        )
        assert r.status_code == 200
        # Row partial should come back
        assert 'id="setup-entity-NewLLC"' in r.text

    def test_entity_deactivate_then_reactivate(self, app_client):
        _seed(app_client.app.state.db)
        r1 = app_client.post(
            "/setup/entities/Clearing/deactivate",
            headers={"HX-Request": "true"},
        )
        assert r1.status_code == 200
        r2 = app_client.post(
            "/setup/entities/Clearing/reactivate",
            headers={"HX-Request": "true"},
        )
        assert r2.status_code == 200

    def test_entity_cleanup_stale_meta(self, app_client):
        _seed(app_client.app.state.db)
        # OpeningBalances has no entity row but accounts_meta.entity_slug
        # points there. cleanup-stale-meta clears it.
        r = app_client.post(
            "/setup/entities/OpeningBalances/cleanup-stale-meta",
            follow_redirects=False,
        )
        assert r.status_code == 303
        # The stale row should now have entity_slug = NULL
        row = app_client.app.state.db.execute(
            "SELECT entity_slug FROM accounts_meta WHERE account_path = ?",
            ("Equity:Personal:OpeningBalances",),
        ).fetchone()
        assert row["entity_slug"] is None

    def test_entity_close_unused_opens_on_empty_slug(self, app_client):
        _seed(app_client.app.state.db)
        r = app_client.post(
            "/setup/entities/Retained/close-unused-opens",
            follow_redirects=False,
        )
        assert r.status_code == 303
        # Even if zero real opens existed, the endpoint shouldn't 500.

    def test_entity_delete_refuses_with_references(self, app_client):
        _seed(app_client.app.state.db)
        # Personal has user-set fields (display_name, entity_type,
        # tax_schedule) AND live path-owned accounts. Either path
        # refuses delete; the post-Phase-1.4 contract is an actionable
        # ?error= message with "Cannot delete" + the slug.
        r = app_client.post(
            "/setup/entities/Personal/delete",
            follow_redirects=False,
        )
        assert r.status_code == 303
        from urllib.parse import unquote

        loc = unquote(r.headers["location"])
        assert "/setup/entities/Personal/manage" in loc
        assert "Cannot delete" in loc and "Personal" in loc

    def test_account_save(self, app_client):
        _seed(app_client.app.state.db)
        r = app_client.post(
            "/setup/accounts/save",
            data={
                "account_path": "Assets:Personal:Bank:Savings",
                "kind": "bank", "entity_slug": "Personal",
                "institution": "Bank One", "last_four": "0001",
            },
            headers={"HX-Request": "true"},
        )
        assert r.status_code == 200
        assert "hx-swap-oob" in r.text
        assert "setup-accounts-banner" in r.text

    def test_account_close_on_zero_posting_account(self, app_client):
        _seed(app_client.app.state.db)
        # Payable:To accounts have no postings in the fixture ledger
        r = app_client.post(
            "/setup/accounts/close",
            data={"account_path": "Liabilities:BetaCorp:Payable:ToAcmeCo"},
            headers={"HX-Request": "true"},
            follow_redirects=False,
        )
        # Either closes + returns empty 200 (has_open path) or drops
        # the stale cache row (no Open path). Both are valid and
        # non-500.
        assert r.status_code in (200, 303)

    def test_account_close_idempotent_no_duplicate_close_written(
        self, app_client, ledger_dir
    ):
        """Calling close twice on the same account must not produce
        a duplicate Close directive. Regression guard for the 4-hour
        lockout bug where the handler appended another line."""
        _seed(app_client.app.state.db)
        path = "Liabilities:BetaCorp:Payable:ToAcmeCo"
        # First close
        app_client.post(
            "/setup/accounts/close",
            data={"account_path": path},
            headers={"HX-Request": "true"},
            follow_redirects=False,
        )
        # Second close (idempotent)
        app_client.post(
            "/setup/accounts/close",
            data={"account_path": path},
            headers={"HX-Request": "true"},
            follow_redirects=False,
        )
        connector_accounts = ledger_dir / "connector_accounts.bean"
        if connector_accounts.exists():
            text = connector_accounts.read_text(encoding="utf-8")
            close_count = sum(
                1 for line in text.splitlines()
                if line.strip().endswith(path)
                and "close" in line.split()
            )
            assert close_count <= 1, (
                f"Duplicate Close detected: {close_count} lines for {path}\n"
                f"{text}"
            )

    def test_scaffold_refuses_when_main_bean_exists(self, app_client, ledger_dir):
        """Start-fresh must never overwrite existing data. The fixture
        ledger already has main.bean, so POST /setup/scaffold must
        return 4xx and leave the files untouched."""
        _seed(app_client.app.state.db)
        main_bean = ledger_dir / "main.bean"
        assert main_bean.exists(), "fixture sanity: main.bean should exist"
        before = main_bean.read_bytes()
        r = app_client.post("/setup/scaffold", follow_redirects=False)
        # Either 409 (our explicit guard), 400 (scaffolder's internal
        # refusal), or a 303 redirect to / (idempotency guard fired
        # because ledger parsed cleanly). Any of those is correct —
        # what we DON'T want is a 200 with the ledger rewritten.
        assert r.status_code in (303, 400, 409), (
            f"scaffold returned unexpected {r.status_code}; "
            f"user data could be at risk"
        )
        after = main_bean.read_bytes()
        assert before == after, "scaffold modified existing main.bean!"

    def test_setup_page_hides_scaffold_button_when_ledger_exists(self, app_client):
        """When main.bean exists AND setup isn't complete (user is in
        the fixup scenario), the scenario hero must name the state
        clearly. Forces setup_required_complete=False so /setup
        renders the fixup scenario rather than redirecting away."""
        app_client.app.state.setup_required_complete = False
        r = app_client.get("/setup", follow_redirects=False)
        assert r.status_code == 200
        indicators = [
            "Existing installation detected",
            "existing ledger detected",
            "Disabled (main.bean exists)",
        ]
        assert any(s in r.text for s in indicators), (
            f"/setup didn't render a recognizable 'ledger-exists' "
            f"indicator; none of {indicators} in response"
        )

    def test_fix_duplicate_closes_recovery(self, app_client, ledger_dir):
        """The emergency endpoint must dedupe a corrupt
        connector_accounts.bean without 500'ing."""
        _seed(app_client.app.state.db)
        connector_accounts = ledger_dir / "connector_accounts.bean"
        connector_accounts.write_text(
            "; managed by Lamella.\n"
            "\n2026-04-24 close Assets:Personal:Bank:Savings\n"
            "2026-04-24 close Assets:Personal:Bank:Savings\n"
            "2026-04-24 close Liabilities:BetaCorp:Payable:ToAcmeCo\n"
            "2026-04-24 close Liabilities:BetaCorp:Payable:ToAcmeCo\n"
            "2026-04-24 close Liabilities:BetaCorp:Payable:ToAcmeCo\n",
            encoding="utf-8",
        )
        r = app_client.post(
            "/setup/fix-duplicate-closes", follow_redirects=False,
        )
        assert r.status_code == 303
        text = connector_accounts.read_text(encoding="utf-8")
        lines = [l for l in text.splitlines() if "close " in l]
        # 5 Close lines → 2 unique (Savings + Payable)
        assert len(lines) == 2, f"Expected 2 unique Close lines, got {len(lines)}:\n{text}"

    def test_chart_scaffold_does_not_500(self, app_client):
        _seed(app_client.app.state.db)
        r = app_client.post(
            "/setup/charts/Personal/scaffold", follow_redirects=False,
        )
        assert r.status_code in (200, 303)


class TestSetupAddButtons:
    """Every setup section that manages records (vehicles, properties,
    loans) must expose an Add button so users can complete setup
    without hunting through /settings — a previous version of the
    flow hid these behind small 'Add via /settings' text links which
    users missed."""

    def test_setup_vehicles_has_add_button(self, app_client):
        r = app_client.get("/setup/vehicles")
        assert r.status_code == 200
        # The recovery-shell migration replaced the bare /vehicles/new
        # link with the in-page modal trigger ?add=vehicle. Modal-
        # trigger behavior itself is covered by
        # test_setup_vehicles_modal.py.
        assert "/setup/vehicles?add=vehicle" in r.text
        assert "Add vehicle" in r.text

    def test_setup_properties_has_add_button(self, app_client):
        r = app_client.get("/setup/properties")
        assert r.status_code == 200
        # Same recovery-shell migration: modal trigger replaces the
        # /settings/properties cross-shell link.
        assert "/setup/properties?add=property" in r.text
        assert "Add property" in r.text

    def test_bulk_close_unused_orphans_endpoint_exists(self, app_client):
        """Regression: when /setup/vehicles shows a pile of 0-posting
        orphan accounts (legacy non-canonical paths), the user needs a
        bulk close action. Without it they'd click Migrate→ on each
        row and see an empty dropdown because no vehicles have entities
        set yet."""
        _seed(app_client.app.state.db)
        r = app_client.post(
            "/setup/vehicles/close-unused-orphans", follow_redirects=False,
        )
        assert r.status_code == 303
        # Either "no-unused-orphans-found" or "closed-N-unused-orphans"
        loc = r.headers.get("location", "")
        assert "/setup/vehicles" in loc

    def test_setup_loans_has_add_button(self, app_client):
        r = app_client.get("/setup/loans")
        assert r.status_code == 200
        # Recovery-shell migration: ?add=loan modal trigger replaces
        # the bare /settings/loans link. The per-row Edit punt at
        # /settings/loans/{slug}/edit is still tracked in
        # SETUP_IMPLEMENTATION.md for Phase 8 — not under test here.
        assert "/setup/loans?add=loan" in r.text
        assert "Add loan" in r.text

    def test_vehicle_edit_placeholder_is_unambiguous(self, app_client):
        """The entity dropdown on vehicle_edit.html used to show
        '— personal —' as its placeholder label, which users read as
        'Personal is selected' even when entity_slug was NULL. Must
        now render 'not set' so the state is clear."""
        _seed(app_client.app.state.db)
        # Insert a vehicle without entity_slug
        app_client.app.state.db.execute(
            "INSERT INTO vehicles (slug, display_name, entity_slug, is_active) "
            "VALUES ('Vtest', 'Test', NULL, 1)"
        )
        app_client.app.state.db.commit()
        r = app_client.get("/vehicles/Vtest/edit", follow_redirects=False)
        # May 200 or redirect depending on gate, but if 200 the label
        # must not say "personal" as the placeholder.
        if r.status_code == 200:
            assert "— personal —" not in r.text.lower(), (
                "vehicle_edit placeholder still says 'personal' — "
                "users misread this as 'Personal is already selected'"
            )


class TestSetupGateOnHalfConfiguredOptions:
    """If the user has registered ≥1 vehicle/property/loan, a missing
    canonical account on any of them must block the setup gate.
    Zero-count cases stay optional."""

    def test_vehicle_check_zero_vehicles_is_optional(self, app_client):
        from lamella.features.setup.setup_progress import _check_vehicles
        # Ensure no vehicles
        app_client.app.state.db.execute("DELETE FROM vehicles")
        app_client.app.state.db.commit()
        step = _check_vehicles(app_client.app.state.db, [])
        assert step.required is False
        assert step.is_complete is True

    def test_vehicle_check_one_vehicle_missing_chart_is_required(self, app_client):
        from lamella.features.setup.setup_progress import _check_vehicles
        _seed(app_client.app.state.db)
        app_client.app.state.db.execute(
            "INSERT INTO vehicles (slug, display_name, entity_slug, is_active) "
            "VALUES ('Vincomplete','Incomplete','Personal',1)"
        )
        app_client.app.state.db.commit()
        step = _check_vehicles(app_client.app.state.db, [])  # no Opens
        assert step.required is True, (
            "half-scaffolded vehicle must be a required-to-fix blocker"
        )
        assert step.is_complete is False

    def test_property_check_zero_properties_is_optional(self, app_client):
        from lamella.features.setup.setup_progress import _check_properties
        app_client.app.state.db.execute("DELETE FROM properties")
        app_client.app.state.db.commit()
        step = _check_properties(app_client.app.state.db, [])
        assert step.required is False
        assert step.is_complete is True

    def test_loan_check_zero_loans_is_optional(self, app_client):
        from lamella.features.setup.setup_progress import _check_loans
        app_client.app.state.db.execute("DELETE FROM loans")
        app_client.app.state.db.commit()
        step = _check_loans(app_client.app.state.db)
        assert step.required is False
        assert step.is_complete is True


class TestOverrideWriterEscapesSpecialChars:
    """OverrideWriter must escape quote characters in narration. Real
    merchant data contains narrations like `3-1/2" Coaxial Loudspeakers`
    and `26"+23" Wiper Blades` — writing them raw produces an
    unterminated string and kills main.bean parseability on next
    load. Regression guard for the hour-long lockout this caused."""

    def test_override_block_escapes_quote_in_narration(self):
        from lamella.features.rules.overrides import _override_block
        from datetime import date
        from decimal import Decimal
        block = _override_block(
            txn_date=date(2025, 7, 30),
            txn_hash="abc123",
            amount=Decimal("73.20"),
            from_account="Expenses:Personal:Custom:Stuff",
            to_account="Expenses:Personal:Vehicle:V:Maintenance",
            narration='JBL GX328 3-1/2" Coaxial Car Audio Loudspeakers',
        )
        # Escaped quote should appear as \" not bare "
        assert '3-1/2\\"' in block, (
            f"narration quote was not escaped in override block: {block!r}"
        )
        # And the block must parse as beancount. Write to tmp + load.
        import tempfile
        from pathlib import Path
        from beancount import loader
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "test.bean"
            p.write_text(
                '2020-01-01 open Expenses:Personal:Custom:Stuff\n'
                '2020-01-01 open Expenses:Personal:Vehicle:V:Maintenance\n'
                + block,
                encoding="utf-8",
            )
            _, errors, _ = loader.load_file(str(p))
            real = [
                e for e in errors
                if "Auto-inserted" not in getattr(e, "message", "")
            ]
            assert not real, (
                f"override block with quoted narration failed beancount "
                f"parse: {[str(e) for e in real]}"
            )


class TestMigrateNetBalancesAndIdempotency:
    """The migrate flow was silently producing double-overrides when
    run twice on the same orphan, inflating the target's balance and
    leaving the orphan at a negative net instead of zero. Smoke tests
    only checked status codes; they missed the accounting bug.
    These tests guard the invariants that matter: net balance on
    orphan goes to 0, target absorbs the full amount, second run is
    a no-op."""

    def _prep_orphan_scenario(self, app_client, ledger_dir):
        """Seed a minimal ledger with 3 transactions on an orphan
        account and the canonical target already open (backdated)."""
        _seed(app_client.app.state.db)
        connector_accounts = ledger_dir / "connector_accounts.bean"
        connector_accounts.write_text(
            "; test scenario\n"
            "2020-01-01 open Expenses:Personal:Custom:LegacyGas\n"
            "2020-01-01 open Expenses:Personal:Vehicle:Vt:Fuel\n"
            "2020-01-01 open Assets:Personal:BankOne:Checking\n",
            encoding="utf-8",
        )
        manual = ledger_dir / "manual_transactions.bean"
        txns = "\n"
        for date, amt in [("2025-01-16", "51.90"), ("2025-02-18", "55.17"), ("2025-03-05", "47.22")]:
            txns += (
                f'{date} * "Sheridan" "fuel"\n'
                f"  Expenses:Personal:Custom:LegacyGas  {amt} USD\n"
                f"  Assets:Personal:BankOne:Checking  -{amt} USD\n\n"
            )
        manual.write_text(txns, encoding="utf-8")
        main = ledger_dir / "main.bean"
        main.write_text(
            'option "operating_currency" "USD"\n'
            'include "connector_accounts.bean"\n'
            'include "manual_transactions.bean"\n'
            'include "connector_overrides.bean"\n',
            encoding="utf-8",
        )
        # Also ensure overrides file exists
        (ledger_dir / "connector_overrides.bean").write_text(
            "; overrides\n", encoding="utf-8",
        )
        # Fresh reader
        reader = getattr(app_client.app.state, "ledger_reader", None)
        if reader is not None:
            reader.invalidate()
        # Register a Personal entity to satisfy schema
        db = app_client.app.state.db
        db.execute(
            "INSERT OR IGNORE INTO entities (slug, display_name, entity_type, is_active) "
            "VALUES ('Personal', 'Personal', 'personal', 1)"
        )
        db.commit()

    def _balances(self, ledger_dir):
        from beancount import loader
        from beancount.core.data import Transaction
        from decimal import Decimal
        entries, _, _ = loader.load_file(str(ledger_dir / "main.bean"))
        bal: dict[str, Decimal] = {}
        for t in entries:
            if not isinstance(t, Transaction):
                continue
            for p in t.postings or ():
                if p.units and p.units.number is not None:
                    bal[p.account] = bal.get(p.account, Decimal(0)) + Decimal(p.units.number)
        return bal

    def _txn_hashes_on_orphan(self, app_client, slug, orphan_path):
        """Scrape the migrate drilldown page to get the txn_hashes
        the server would accept."""
        import urllib.parse as up
        r = app_client.get(
            f"/setup/vehicles/{slug}/migrate?orphan={up.quote(orphan_path)}",
            follow_redirects=False,
        )
        import re
        return re.findall(r'name="txn_hash" value="([a-f0-9]+)"', r.text)

    def test_migrate_twice_does_not_double_override(
        self, app_client, ledger_dir
    ):
        """Regression: running migrate twice on the same orphan must
        not inflate the target balance. Run 1 writes N overrides;
        Run 2 must either write zero new overrides (affected list
        excludes #lamella-override), OR any rewrites must be
        replace-in-place (net-equivalent). Final state invariants:
        orphan net = 0, target net = sum of original postings."""
        self._prep_orphan_scenario(app_client, ledger_dir)
        # Register a test vehicle
        db = app_client.app.state.db
        db.execute(
            "INSERT INTO vehicles (slug, display_name, entity_slug, is_active) "
            "VALUES ('Vt', 'Test', 'Personal', 1)"
        )
        db.commit()
        # Re-check baseline
        bal0 = self._balances(ledger_dir)
        assert bal0.get("Expenses:Personal:Custom:LegacyGas") is not None
        total_gas = bal0["Expenses:Personal:Custom:LegacyGas"]

        # First migrate run
        hashes = self._txn_hashes_on_orphan(
            app_client, "Vt", "Expenses:Personal:Custom:LegacyGas"
        )
        assert len(hashes) == 3
        form = {
            "orphan": "Expenses:Personal:Custom:LegacyGas",
            "target": "Expenses:Personal:Vehicle:Vt:Fuel",
            "txn_hash": hashes,
        }
        r = app_client.post("/setup/vehicles/Vt/migrate", data=form, follow_redirects=False)
        assert r.status_code == 303
        loc = r.headers.get("location", "")
        # Migrate redirects back to the page with migrated=N&failed=N
        assert "error" not in loc.lower() or "migrated=" in loc, (
            f"migrate had an error: {loc}"
        )
        overrides_txt = (ledger_dir / "connector_overrides.bean").read_text(encoding="utf-8")
        assert "lamella-override-of" in overrides_txt, (
            f"no override blocks written. location={loc}, "
            f"overrides.bean size={len(overrides_txt)}"
        )

        bal1 = self._balances(ledger_dir)
        orphan_after_1 = bal1.get("Expenses:Personal:Custom:LegacyGas", 0)
        target_after_1 = bal1.get("Expenses:Personal:Vehicle:Vt:Fuel", 0)
        assert orphan_after_1 == 0, (
            f"orphan should net to 0 after migrate, got {orphan_after_1}"
        )
        assert target_after_1 == total_gas, (
            f"target should equal pre-migrate orphan total ({total_gas}), "
            f"got {target_after_1}"
        )

        # Second migrate run — should NOT double up
        hashes2 = self._txn_hashes_on_orphan(
            app_client, "Vt", "Expenses:Personal:Custom:LegacyGas"
        )
        # Second pass should see either 0 (affected list filters the
        # overrides we just wrote) or the same 3 (replace-in-place)
        if hashes2:
            form2 = {
                "orphan": "Expenses:Personal:Custom:LegacyGas",
                "target": "Expenses:Personal:Vehicle:Vt:Fuel",
                "txn_hash": hashes2,
            }
            app_client.post("/setup/vehicles/Vt/migrate", data=form2, follow_redirects=False)

        bal2 = self._balances(ledger_dir)
        orphan_after_2 = bal2.get("Expenses:Personal:Custom:LegacyGas", 0)
        target_after_2 = bal2.get("Expenses:Personal:Vehicle:Vt:Fuel", 0)
        assert orphan_after_2 == 0, (
            f"orphan should stay at 0 after second migrate, got {orphan_after_2}"
        )
        assert target_after_2 == total_gas, (
            f"target should stay at {total_gas} after second migrate, "
            f"got {target_after_2} — DUPLICATE OVERRIDE BUG"
        )

    def test_close_account_counts_only_unmigrated_postings(
        self, app_client, ledger_dir
    ):
        """Regression: /setup/accounts/close was counting EVERY
        posting touching the account, including #lamella-override txns
        the migrate flow wrote. Result: an account with 49 originals
        that had all been migrated would report "98 postings" and
        refuse to close — user sees
        ?error=account-still-has-98-postings even though the
        /setup/vehicles orphan list says 0. UI and close handler now
        use the same filter."""
        self._prep_orphan_scenario(app_client, ledger_dir)
        # Register a test vehicle so migrate targets resolve
        db = app_client.app.state.db
        db.execute(
            "INSERT INTO vehicles (slug, display_name, entity_slug, is_active) "
            "VALUES ('Vt', 'Test', 'Personal', 1)"
        )
        db.commit()
        # Migrate all 3 originals
        hashes = self._txn_hashes_on_orphan(
            app_client, "Vt", "Expenses:Personal:Custom:LegacyGas"
        )
        app_client.post(
            "/setup/vehicles/Vt/migrate",
            data={
                "orphan": "Expenses:Personal:Custom:LegacyGas",
                "target": "Expenses:Personal:Vehicle:Vt:Fuel",
                "txn_hash": hashes,
            },
            follow_redirects=False,
        )
        # Now try to close the orphan — it should succeed because
        # all 3 originals are migrated (no unmigrated postings
        # remain touching the account).
        r = app_client.post(
            "/setup/accounts/close",
            data={"account_path": "Expenses:Personal:Custom:LegacyGas"},
            follow_redirects=False,
        )
        loc = r.headers.get("location", "")
        assert "account-still-has" not in loc, (
            f"close-account refused a fully-migrated orphan: {loc}. "
            f"Counting includes override postings (UI and server disagree)."
        )

    def test_fix_orphan_overrides_removes_stale_blocks(
        self, app_client, ledger_dir
    ):
        """Recovery: /setup/fix-orphan-overrides deletes override
        blocks whose lamella-override-of hash doesn't match any
        non-override txn. Seeds a manually-crafted orphan override
        block and confirms it's removed."""
        self._prep_orphan_scenario(app_client, ledger_dir)
        # Write a fake orphan-override block (references a hash that
        # doesn't exist in the ledger)
        overrides = ledger_dir / "connector_overrides.bean"
        overrides.write_text(
            overrides.read_text(encoding="utf-8")
            + '\n2025-06-15 * "ghost" #lamella-override\n'
            + '  lamella-override-of: "deadbeef000000000000000000000000000000aa"\n'
            + '  lamella-modified-at: "2026-04-24T12:00:00-06:00"\n'
            + "  Expenses:Personal:Custom:LegacyGas  -10.00 USD\n"
            + "  Expenses:Personal:Vehicle:Vt:Fuel    10.00 USD\n",
            encoding="utf-8",
        )
        r = app_client.post("/setup/fix-orphan-overrides", follow_redirects=False)
        assert r.status_code == 303
        loc = r.headers.get("location", "")
        assert "removed-1-orphan-override-blocks" in loc or "removed-" in loc, (
            f"expected removal confirmation, got {loc}"
        )
        txt = overrides.read_text(encoding="utf-8")
        assert "deadbeef" not in txt, "stale orphan override was not removed"


class TestVehicleRenameMisattributionFix:
    """Phase 4.1: ``POST /vehicles/{slug}/change-ownership/rename``
    rewrites every posting on ``Expenses:<old>:Vehicle:<slug>:*`` and
    ``Assets:<old>:Vehicle:<slug>`` to point at the new entity. Two
    cases:

    * Case A — direct postings on old vehicle paths: a new
      override is appended.
    * Case B — existing migration overrides whose to_account points
      at old vehicle paths: textual rewrite of those override blocks.

    No disposal is recorded — this is the misattribution-fix flow,
    not the transfer flow."""

    def _seed_two_entities(self, db):
        _seed(db)
        # Add a second active entity to be the rename target.
        db.execute(
            "INSERT OR IGNORE INTO entities (slug, display_name, "
            "entity_type, is_active) "
            "VALUES ('Acme', 'Acme LLC', 'sole-prop', 1)"
        )
        db.commit()

    def _prep_vehicle_with_postings(self, app_client, ledger_dir, *, slug="Vt"):
        """Set up ledger with one vehicle + 2 fuel postings on its
        Expenses:Personal:Vehicle:Vt:Fuel path. Returns nothing."""
        connector_accounts = ledger_dir / "connector_accounts.bean"
        connector_accounts.write_text(
            "; test scenario\n"
            f"2020-01-01 open Expenses:Personal:Vehicle:{slug}:Fuel\n"
            "2020-01-01 open Assets:Personal:BankOne:Checking\n",
            encoding="utf-8",
        )
        manual = ledger_dir / "manual_transactions.bean"
        txns = "\n"
        for date_, amt in [("2025-01-16", "51.90"), ("2025-02-18", "55.17")]:
            txns += (
                f'{date_} * "Sheridan" "fuel"\n'
                f"  Expenses:Personal:Vehicle:{slug}:Fuel  {amt} USD\n"
                f"  Assets:Personal:BankOne:Checking  -{amt} USD\n\n"
            )
        manual.write_text(txns, encoding="utf-8")
        main = ledger_dir / "main.bean"
        main.write_text(
            'option "operating_currency" "USD"\n'
            'include "connector_accounts.bean"\n'
            'include "manual_transactions.bean"\n'
            'include "connector_overrides.bean"\n',
            encoding="utf-8",
        )
        (ledger_dir / "connector_overrides.bean").write_text(
            "; overrides\n", encoding="utf-8",
        )
        reader = getattr(app_client.app.state, "ledger_reader", None)
        if reader is not None:
            reader.invalidate()
        db = app_client.app.state.db
        self._seed_two_entities(db)
        db.execute(
            "INSERT OR REPLACE INTO vehicles "
            "(slug, display_name, entity_slug, is_active) "
            f"VALUES ('{slug}', 'Test', 'Personal', 1)"
        )
        db.commit()

    def test_rename_writes_case_a_overrides_and_closes_old_paths(
        self, app_client, ledger_dir,
    ):
        slug = "Vt"
        self._prep_vehicle_with_postings(app_client, ledger_dir, slug=slug)

        r = app_client.post(
            f"/vehicles/{slug}/change-ownership/rename",
            data={"new_entity_slug": "Acme"},
            follow_redirects=False,
        )
        assert r.status_code == 303, r.text
        loc = r.headers["location"]
        assert "renamed_to=Acme" in loc, loc
        assert "case_a=2" in loc, loc

        # Vehicle row reassigned
        db = app_client.app.state.db
        row = db.execute(
            "SELECT entity_slug FROM vehicles WHERE slug=?", (slug,),
        ).fetchone()
        assert row["entity_slug"] == "Acme"

        # Overrides written for both fuel postings
        overrides_text = (
            ledger_dir / "connector_overrides.bean"
        ).read_text(encoding="utf-8")
        assert overrides_text.count(
            f"Expenses:Personal:Vehicle:{slug}:Fuel"
        ) == 2
        assert overrides_text.count(
            f"Expenses:Acme:Vehicle:{slug}:Fuel"
        ) == 2

        # Old path Close written
        accounts_text = (
            ledger_dir / "connector_accounts.bean"
        ).read_text(encoding="utf-8")
        assert (
            f"close Expenses:Personal:Vehicle:{slug}:Fuel" in accounts_text
        )
        # New path Open written via ensure_vehicle_chart
        assert (
            f"open Expenses:Acme:Vehicle:{slug}:Fuel" in accounts_text
        )

    def test_rename_rejects_same_entity(self, app_client, ledger_dir):
        slug = "Vt"
        self._prep_vehicle_with_postings(app_client, ledger_dir, slug=slug)
        r = app_client.post(
            f"/vehicles/{slug}/change-ownership/rename",
            data={"new_entity_slug": "Personal"},
            follow_redirects=False,
        )
        assert r.status_code == 303
        assert "error=same-entity" in r.headers["location"]
        # Vehicle row unchanged
        db = app_client.app.state.db
        row = db.execute(
            "SELECT entity_slug FROM vehicles WHERE slug=?", (slug,),
        ).fetchone()
        assert row["entity_slug"] == "Personal"

    def test_rename_rejects_unknown_target_entity(self, app_client, ledger_dir):
        slug = "Vt"
        self._prep_vehicle_with_postings(app_client, ledger_dir, slug=slug)
        r = app_client.post(
            f"/vehicles/{slug}/change-ownership/rename",
            data={"new_entity_slug": "NoSuchEntity"},
            follow_redirects=False,
        )
        assert r.status_code == 303
        assert "error=unknown-new-entity" in r.headers["location"]
        db = app_client.app.state.db
        row = db.execute(
            "SELECT entity_slug FROM vehicles WHERE slug=?", (slug,),
        ).fetchone()
        assert row["entity_slug"] == "Personal"

    def test_rename_rewrites_rental_income_postings(
        self, app_client, ledger_dir,
    ):
        """Vehicle-rental-business case: a rental fleet posts to
        ``Income:<Entity>:Vehicle:<slug>:RentalIncome`` every time a
        customer rents the car. Misattribution rename has to move
        those legs along with the asset/expenses, otherwise income
        reports split across the old and new entities silently."""
        from decimal import Decimal
        from beancount import loader
        from beancount.core.data import Transaction
        slug = "Vrental"
        self._seed_two_entities(app_client.app.state.db)
        # Set up: vehicle on Personal with one fuel expense + one
        # rental income posting.
        connector_accounts = ledger_dir / "connector_accounts.bean"
        connector_accounts.write_text(
            "; test scenario\n"
            f"2020-01-01 open Assets:Personal:Vehicle:{slug}\n"
            f"2020-01-01 open Expenses:Personal:Vehicle:{slug}:Fuel\n"
            f"2020-01-01 open Income:Personal:Vehicle:{slug}:RentalIncome\n"
            "2020-01-01 open Assets:Personal:BankOne:Checking\n",
            encoding="utf-8",
        )
        manual = ledger_dir / "manual_transactions.bean"
        manual.write_text(
            f'\n2025-03-15 * "Sheridan" "fuel"\n'
            f"  Expenses:Personal:Vehicle:{slug}:Fuel  60.00 USD\n"
            f"  Assets:Personal:BankOne:Checking  -60.00 USD\n\n"
            f'2025-03-20 * "Customer" "rental"\n'
            f"  Assets:Personal:BankOne:Checking  500.00 USD\n"
            f"  Income:Personal:Vehicle:{slug}:RentalIncome  -500.00 USD\n",
            encoding="utf-8",
        )
        main = ledger_dir / "main.bean"
        main.write_text(
            'option "operating_currency" "USD"\n'
            'include "connector_accounts.bean"\n'
            'include "manual_transactions.bean"\n'
            'include "connector_overrides.bean"\n',
            encoding="utf-8",
        )
        (ledger_dir / "connector_overrides.bean").write_text(
            "; overrides\n", encoding="utf-8",
        )
        reader = getattr(app_client.app.state, "ledger_reader", None)
        if reader is not None:
            reader.invalidate()
        db = app_client.app.state.db
        db.execute(
            "INSERT OR REPLACE INTO vehicles "
            "(slug, display_name, entity_slug, is_active) "
            f"VALUES ('{slug}', 'Rental', 'Personal', 1)"
        )
        db.commit()

        r = app_client.post(
            f"/vehicles/{slug}/change-ownership/rename",
            data={"new_entity_slug": "Acme"},
            follow_redirects=False,
        )
        assert r.status_code == 303, r.text
        loc = r.headers["location"]
        assert "renamed_to=Acme" in loc, loc
        # Two case_a postings (one expense, one income)
        assert "case_a=2" in loc, loc

        entries, errors, _ = loader.load_file(
            str(ledger_dir / "main.bean")
        )
        assert errors == [], f"ledger has errors after rename: {errors}"
        bal: dict[str, Decimal] = {}
        for t in entries:
            if not isinstance(t, Transaction):
                continue
            for p in t.postings or ():
                if p.units and p.units.number is not None:
                    bal[p.account] = bal.get(
                        p.account, Decimal(0),
                    ) + Decimal(p.units.number)
        # Old paths net to 0
        assert bal.get(
            f"Expenses:Personal:Vehicle:{slug}:Fuel", Decimal(0),
        ) == Decimal(0)
        assert bal.get(
            f"Income:Personal:Vehicle:{slug}:RentalIncome", Decimal(0),
        ) == Decimal(0)
        # New entity absorbs both legs (Income negative per Beancount
        # convention for the credit-side rental income)
        assert bal.get(
            f"Expenses:Acme:Vehicle:{slug}:Fuel", Decimal(0),
        ) == Decimal("60")
        assert bal.get(
            f"Income:Acme:Vehicle:{slug}:RentalIncome", Decimal(0),
        ) == Decimal("-500")

    def test_rename_rewrites_existing_migration_overrides(
        self, app_client, ledger_dir,
    ):
        """Phase 5: Case B — the original posting is on a non-vehicle
        path (LegacyGas), an existing migration override redirects it
        to the old-entity vehicle path, then the user renames the
        vehicle to a new entity. The rename must rewrite the existing
        override's to_account from old → new, NOT append a separate
        Case-A override (which would double-count). Final balance:
        original LegacyGas posting cancelled by the (rewritten)
        override; new-entity vehicle path absorbs the value; old-
        entity vehicle path nets to zero."""
        from decimal import Decimal
        slug = "Vt"
        self._seed_two_entities(app_client.app.state.db)
        # Set up: original posting on a legacy path, NOT on the
        # vehicle path. We'll then write a manual migration override
        # redirecting to the OLD-entity vehicle path. Rename should
        # rewrite that override.
        connector_accounts = ledger_dir / "connector_accounts.bean"
        connector_accounts.write_text(
            "; test scenario\n"
            "2020-01-01 open Expenses:Personal:Custom:LegacyGas\n"
            f"2020-01-01 open Expenses:Personal:Vehicle:{slug}:Fuel\n"
            "2020-01-01 open Assets:Personal:BankOne:Checking\n",
            encoding="utf-8",
        )
        manual = ledger_dir / "manual_transactions.bean"
        manual.write_text(
            '\n2025-04-01 * "Sheridan" "fuel"\n'
            "  Expenses:Personal:Custom:LegacyGas  60.00 USD\n"
            "  Assets:Personal:BankOne:Checking  -60.00 USD\n",
            encoding="utf-8",
        )
        main = ledger_dir / "main.bean"
        main.write_text(
            'option "operating_currency" "USD"\n'
            'include "connector_accounts.bean"\n'
            'include "manual_transactions.bean"\n'
            'include "connector_overrides.bean"\n',
            encoding="utf-8",
        )
        (ledger_dir / "connector_overrides.bean").write_text(
            "; overrides\n", encoding="utf-8",
        )
        reader = getattr(app_client.app.state, "ledger_reader", None)
        if reader is not None:
            reader.invalidate()
        db = app_client.app.state.db
        db.execute(
            "INSERT OR REPLACE INTO vehicles "
            "(slug, display_name, entity_slug, is_active) "
            f"VALUES ('{slug}', 'Test', 'Personal', 1)"
        )
        db.commit()

        # Step 1: write a migration override redirecting LegacyGas →
        # Personal:Vehicle:Vt:Fuel. Use the actual /setup/vehicles/{slug}/migrate
        # handler so the override hash semantics match production.
        get_resp = app_client.get(
            f"/setup/vehicles/{slug}/migrate"
            f"?orphan=Expenses:Personal:Custom:LegacyGas",
            follow_redirects=False,
        )
        assert get_resp.status_code == 200
        import re as _re
        hashes = _re.findall(
            r'name="txn_hash" value="([a-f0-9]+)"', get_resp.text,
        )
        assert len(hashes) == 1
        app_client.post(
            f"/setup/vehicles/{slug}/migrate",
            data={
                "orphan": "Expenses:Personal:Custom:LegacyGas",
                "target": f"Expenses:Personal:Vehicle:{slug}:Fuel",
                "txn_hash": hashes,
            },
            follow_redirects=False,
        )
        # Sanity: the override now references the OLD-entity vehicle
        # path.
        ov_text = (
            ledger_dir / "connector_overrides.bean"
        ).read_text(encoding="utf-8")
        assert (
            f"Expenses:Personal:Vehicle:{slug}:Fuel" in ov_text
        )

        # Step 2: rename the vehicle Personal → Acme.
        r = app_client.post(
            f"/vehicles/{slug}/change-ownership/rename",
            data={"new_entity_slug": "Acme"},
            follow_redirects=False,
        )
        assert r.status_code == 303, r.text
        loc = r.headers["location"]
        assert "renamed_to=Acme" in loc
        # Case B should fire (textual rewrite). Case A should be
        # zero — the original posting was on LegacyGas, not on a
        # vehicle path.
        assert "case_b=" in loc
        # Could be 1 (just the to_account on one override block).

        # Final state: override now points at the NEW-entity path.
        ov_text = (
            ledger_dir / "connector_overrides.bean"
        ).read_text(encoding="utf-8")
        assert (
            f"Expenses:Acme:Vehicle:{slug}:Fuel" in ov_text
        ), ov_text
        assert (
            f"Expenses:Personal:Vehicle:{slug}:Fuel" not in ov_text
        ), ("rename failed to rewrite migration override:\n" + ov_text)

        # Net balances:
        from beancount import loader
        from beancount.core.data import Transaction
        entries, errors, _ = loader.load_file(
            str(ledger_dir / "main.bean")
        )
        assert errors == [], f"ledger has errors after rename: {errors}"
        bal: dict[str, Decimal] = {}
        for t in entries:
            if not isinstance(t, Transaction):
                continue
            for p in t.postings or ():
                if p.units and p.units.number is not None:
                    bal[p.account] = bal.get(
                        p.account, Decimal(0),
                    ) + Decimal(p.units.number)
        # Old vehicle path nets to 0 (was never posted to in original
        # txns; only the override pointed there pre-rename).
        assert bal.get(
            f"Expenses:Personal:Vehicle:{slug}:Fuel", Decimal(0)
        ) == Decimal(0)
        # New vehicle path absorbs the value.
        assert bal.get(
            f"Expenses:Acme:Vehicle:{slug}:Fuel", Decimal(0)
        ) == Decimal("60.00")
        # Original LegacyGas posting nets to 0 (cancelled by override).
        assert bal.get(
            "Expenses:Personal:Custom:LegacyGas", Decimal(0)
        ) == Decimal(0)


class TestPropertyOutrightDisposal:
    """Property disposal — vehicle has /dispose; this is the property
    analog. Records an outright sale (vehicle/property leaves the
    user's books entirely). Single 3-leg block: asset out at book,
    proceeds in to user-selected account, gain/loss plug."""

    def _seed(self, db):
        _seed(db)
        db.commit()

    def _prep(self, app_client, ledger_dir, *, slug="Phouse", book_value="200000"):
        connector_accounts = ledger_dir / "connector_accounts.bean"
        connector_accounts.write_text(
            "; test scenario\n"
            f"2020-01-01 open Assets:Personal:Property:{slug}\n"
            "2020-01-01 open Assets:Personal:Bank:Checking\n"
            "2020-01-01 open Equity:Personal:OpeningBalances\n",
            encoding="utf-8",
        )
        manual = ledger_dir / "manual_transactions.bean"
        manual.write_text(
            f'\n2020-01-01 * "Acquired" "house purchase"\n'
            f"  Assets:Personal:Property:{slug}  {book_value}.00 USD\n"
            f"  Equity:Personal:OpeningBalances  -{book_value}.00 USD\n",
            encoding="utf-8",
        )
        main = ledger_dir / "main.bean"
        main.write_text(
            'option "operating_currency" "USD"\n'
            'include "connector_accounts.bean"\n'
            'include "manual_transactions.bean"\n'
            'include "connector_overrides.bean"\n',
            encoding="utf-8",
        )
        (ledger_dir / "connector_overrides.bean").write_text(
            "; overrides\n", encoding="utf-8",
        )
        reader = getattr(app_client.app.state, "ledger_reader", None)
        if reader is not None:
            reader.invalidate()
        db = app_client.app.state.db
        self._seed(db)
        db.execute(
            "INSERT OR REPLACE INTO properties "
            "(slug, display_name, entity_slug, property_type, "
            "is_active, asset_account_path) "
            f"VALUES ('{slug}', 'Old House', 'Personal', 'house', "
            f"1, 'Assets:Personal:Property:{slug}')"
        )
        db.commit()

    @pytest.mark.xfail(
        reason="pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md",
        strict=False,
    )
    def test_dispose_form_renders(self, app_client, ledger_dir):
        slug = "Phouse"
        self._prep(app_client, ledger_dir, slug=slug)
        r = app_client.get(
            f"/settings/properties/{slug}/dispose", follow_redirects=False,
        )
        assert r.status_code == 200
        assert 'name="disposal_date"' in r.text
        assert 'name="proceeds_amount"' in r.text
        assert 'name="proceeds_account"' in r.text
        assert 'name="gain_loss_account"' in r.text
        # Book value pre-filled in display
        assert "200000.00" in r.text
        # Default plug account suggestion includes the slug
        assert (
            f"Income:Personal:Property:{slug}:DisposalGainLoss" in r.text
        )

    def test_dispose_with_gain(self, app_client, ledger_dir):
        from decimal import Decimal
        from beancount import loader
        from beancount.core.data import Transaction
        slug = "Phouse"
        self._prep(app_client, ledger_dir, slug=slug, book_value="200000")
        # Sell for $250k → 50k gain
        r = app_client.post(
            f"/settings/properties/{slug}/dispose",
            data={
                "disposal_date": "2026-04-24",
                "disposal_type": "sale",
                "proceeds_amount": "250000",
                "proceeds_account": "Assets:Personal:Bank:Checking",
                "gain_loss_account": (
                    f"Income:Personal:Property:{slug}:DisposalGainLoss"
                ),
            },
            follow_redirects=False,
        )
        assert r.status_code == 303, r.text
        loc = r.headers["location"]
        assert "error" not in loc, f"disposal failed: {loc}"
        assert "disposed=1" in loc

        # Property row updated
        db = app_client.app.state.db
        row = db.execute(
            "SELECT is_active, sale_date, sale_price FROM properties "
            "WHERE slug = ?", (slug,),
        ).fetchone()
        assert row["is_active"] == 0
        assert row["sale_date"] == "2026-04-24"
        assert str(row["sale_price"]) == "250000"

        # Bean-check clean
        entries, errors, _ = loader.load_file(
            str(ledger_dir / "main.bean"),
        )
        assert errors == [], f"ledger has errors: {errors}"

        # Net balances
        bal: dict[str, Decimal] = {}
        for t in entries:
            if not isinstance(t, Transaction):
                continue
            for p in t.postings or ():
                if p.units and p.units.number is not None:
                    bal[p.account] = bal.get(
                        p.account, Decimal(0),
                    ) + Decimal(p.units.number)
        # Asset went from 200k to 0 (200k acquisition minus 200k disposal)
        assert bal.get(
            f"Assets:Personal:Property:{slug}", Decimal(0),
        ) == Decimal(0)
        # Proceeds landed in checking
        assert bal.get(
            "Assets:Personal:Bank:Checking", Decimal(0),
        ) == Decimal("250000")
        # Income gain (Beancount convention: negative on Income)
        assert bal.get(
            f"Income:Personal:Property:{slug}:DisposalGainLoss", Decimal(0),
        ) == Decimal("-50000")

    def test_dispose_with_loss(self, app_client, ledger_dir):
        from decimal import Decimal
        from beancount import loader
        from beancount.core.data import Transaction
        slug = "Phouse"
        self._prep(app_client, ledger_dir, slug=slug, book_value="200000")
        # Sell for $150k → 50k loss
        r = app_client.post(
            f"/settings/properties/{slug}/dispose",
            data={
                "disposal_date": "2026-04-24",
                "disposal_type": "sale",
                "proceeds_amount": "150000",
                "proceeds_account": "Assets:Personal:Bank:Checking",
                "gain_loss_account": (
                    f"Income:Personal:Property:{slug}:DisposalGainLoss"
                ),
            },
            follow_redirects=False,
        )
        assert r.status_code == 303, r.text
        loc = r.headers["location"]
        assert "error" not in loc, f"disposal failed: {loc}"
        entries, errors, _ = loader.load_file(
            str(ledger_dir / "main.bean"),
        )
        assert errors == [], f"ledger has errors: {errors}"
        bal: dict[str, Decimal] = {}
        for t in entries:
            if not isinstance(t, Transaction):
                continue
            for p in t.postings or ():
                if p.units and p.units.number is not None:
                    bal[p.account] = bal.get(
                        p.account, Decimal(0),
                    ) + Decimal(p.units.number)
        # Loss: proceeds (150k) - book (200k) = -50k. On Income
        # account with sign-flip, that's +50000 (loss = positive
        # debit on Income line, reduces income).
        assert bal.get(
            f"Income:Personal:Property:{slug}:DisposalGainLoss", Decimal(0),
        ) == Decimal("50000")

    def test_dispose_refuses_already_disposed(self, app_client, ledger_dir):
        slug = "Phouse"
        self._prep(app_client, ledger_dir, slug=slug)
        # Mark already disposed
        db = app_client.app.state.db
        db.execute(
            "UPDATE properties SET is_active = 0, sale_date = '2025-01-01', "
            "sale_price = '100000' WHERE slug = ?", (slug,),
        )
        db.commit()
        r = app_client.post(
            f"/settings/properties/{slug}/dispose",
            data={
                "disposal_date": "2026-04-24",
                "disposal_type": "sale",
                "proceeds_amount": "100000",
                "proceeds_account": "Assets:Personal:Bank:Checking",
                "gain_loss_account": (
                    f"Income:Personal:Property:{slug}:DisposalGainLoss"
                ),
            },
            follow_redirects=False,
        )
        assert r.status_code == 303
        assert "error=already-disposed" in r.headers["location"]


class TestPropertyMisattributionRename:
    """Property rename = vehicle rename for real estate. Use case:
    user labeled a property under the wrong entity. Same Case A
    (direct postings) + Case B (textual rewrite of pre-existing
    overrides) shape, extended to cover Income paths for rentals
    and refuse on custom asset_account_path values."""

    def _seed_two_entities(self, db):
        _seed(db)
        db.execute(
            "INSERT OR IGNORE INTO entities (slug, display_name, "
            "entity_type, is_active) "
            "VALUES ('Acme', 'Acme LLC', 'sole-prop', 1)"
        )
        db.commit()

    def _prep(self, app_client, ledger_dir, *, slug="Pmain", is_rental=0):
        connector_accounts = ledger_dir / "connector_accounts.bean"
        accounts_text = (
            "; test scenario\n"
            f"2020-01-01 open Assets:Personal:Property:{slug}\n"
            f"2020-01-01 open Expenses:Personal:Property:{slug}:PropertyTax\n"
        )
        if is_rental:
            accounts_text += (
                f"2020-01-01 open Income:Personal:Property:{slug}:Rental\n"
            )
        accounts_text += "2020-01-01 open Assets:Personal:BankOne:Checking\n"
        connector_accounts.write_text(accounts_text, encoding="utf-8")

        manual = ledger_dir / "manual_transactions.bean"
        txns = (
            f'\n2025-01-15 * "County" "property tax"\n'
            f"  Expenses:Personal:Property:{slug}:PropertyTax  3000.00 USD\n"
            f"  Assets:Personal:BankOne:Checking  -3000.00 USD\n\n"
        )
        if is_rental:
            txns += (
                f'2025-02-15 * "Tenant" "rent"\n'
                f"  Assets:Personal:BankOne:Checking  1500.00 USD\n"
                f"  Income:Personal:Property:{slug}:Rental  -1500.00 USD\n\n"
            )
        manual.write_text(txns, encoding="utf-8")
        main = ledger_dir / "main.bean"
        main.write_text(
            'option "operating_currency" "USD"\n'
            'include "connector_accounts.bean"\n'
            'include "manual_transactions.bean"\n'
            'include "connector_overrides.bean"\n',
            encoding="utf-8",
        )
        (ledger_dir / "connector_overrides.bean").write_text(
            "; overrides\n", encoding="utf-8",
        )
        reader = getattr(app_client.app.state, "ledger_reader", None)
        if reader is not None:
            reader.invalidate()
        db = app_client.app.state.db
        self._seed_two_entities(db)
        db.execute(
            "INSERT OR REPLACE INTO properties "
            "(slug, display_name, entity_slug, property_type, "
            "is_rental, is_active, asset_account_path) "
            f"VALUES ('{slug}', 'Test Property', 'Personal', 'house', "
            f"{is_rental}, 1, 'Assets:Personal:Property:{slug}')"
        )
        db.commit()

    def test_rename_rewrites_expense_and_income_postings(
        self, app_client, ledger_dir,
    ):
        from beancount import loader
        from beancount.core.data import Transaction
        from decimal import Decimal
        slug = "Prental"
        self._prep(app_client, ledger_dir, slug=slug, is_rental=1)
        r = app_client.post(
            f"/settings/properties/{slug}/change-ownership/rename",
            data={"new_entity_slug": "Acme"},
            follow_redirects=False,
        )
        assert r.status_code == 303, r.text
        loc = r.headers["location"]
        assert "renamed_to=Acme" in loc, loc
        # Two case_a postings (one expense, one income)
        assert "case_a=2" in loc, loc

        db = app_client.app.state.db
        row = db.execute(
            "SELECT entity_slug, asset_account_path FROM properties WHERE slug=?",
            (slug,),
        ).fetchone()
        assert row["entity_slug"] == "Acme"
        assert row["asset_account_path"] == f"Assets:Acme:Property:{slug}"

        entries, errors, _ = loader.load_file(str(ledger_dir / "main.bean"))
        assert errors == [], f"ledger has errors after rename: {errors}"
        bal: dict[str, Decimal] = {}
        for t in entries:
            if not isinstance(t, Transaction):
                continue
            for p in t.postings or ():
                if p.units and p.units.number is not None:
                    bal[p.account] = bal.get(
                        p.account, Decimal(0),
                    ) + Decimal(p.units.number)
        # Old expense path nets to 0 (originals + override cancel)
        assert bal.get(
            f"Expenses:Personal:Property:{slug}:PropertyTax", Decimal(0),
        ) == Decimal(0)
        # New expense path absorbs full amount
        assert bal.get(
            f"Expenses:Acme:Property:{slug}:PropertyTax", Decimal(0),
        ) == Decimal("3000.00")
        # Old income path nets to 0
        assert bal.get(
            f"Income:Personal:Property:{slug}:Rental", Decimal(0),
        ) == Decimal(0)
        # New income path absorbs (Beancount income convention: negative)
        assert bal.get(
            f"Income:Acme:Property:{slug}:Rental", Decimal(0),
        ) == Decimal("-1500.00")

    def test_rename_refuses_custom_asset_path(self, app_client, ledger_dir):
        slug = "Pcustom"
        self._prep(app_client, ledger_dir, slug=slug)
        # Override the asset_account_path to something non-canonical.
        db = app_client.app.state.db
        db.execute(
            "UPDATE properties SET asset_account_path = ? WHERE slug = ?",
            ("Assets:Personal:RealEstate:CustomPath", slug),
        )
        db.commit()
        r = app_client.post(
            f"/settings/properties/{slug}/change-ownership/rename",
            data={"new_entity_slug": "Acme"},
            follow_redirects=False,
        )
        assert r.status_code == 303
        assert "error=custom-asset-path" in r.headers["location"]
        # Property entity_slug not changed
        row = db.execute(
            "SELECT entity_slug FROM properties WHERE slug = ?", (slug,),
        ).fetchone()
        assert row["entity_slug"] == "Personal"

    def test_rename_refuses_same_entity(self, app_client, ledger_dir):
        slug = "Pmain"
        self._prep(app_client, ledger_dir, slug=slug)
        r = app_client.post(
            f"/settings/properties/{slug}/change-ownership/rename",
            data={"new_entity_slug": "Personal"},
            follow_redirects=False,
        )
        assert r.status_code == 303
        assert "error=same-entity" in r.headers["location"]


class TestPropertyIntercompanyTransfer:
    """Phase 4.3: ``POST /settings/properties/{slug}/change-ownership/transfer``
    mirrors the vehicle transfer flow for real estate. Same shape:
    six legs across two entities, slug-embedded scaffolds,
    SaleRecapture for the CPA-touchpoint."""

    def _seed_two_entities(self, db):
        _seed(db)
        db.execute(
            "INSERT OR IGNORE INTO entities (slug, display_name, "
            "entity_type, is_active) "
            "VALUES ('Acme', 'Acme LLC', 'sole-prop', 1)"
        )
        db.commit()

    def _prep(
        self, app_client, ledger_dir, *, slug="Pmain", book_value="200000",
    ):
        connector_accounts = ledger_dir / "connector_accounts.bean"
        connector_accounts.write_text(
            "; test scenario\n"
            f"2020-01-01 open Assets:Personal:Property:{slug}\n"
            "2020-01-01 open Equity:Personal:OpeningBalances\n",
            encoding="utf-8",
        )
        manual = ledger_dir / "manual_transactions.bean"
        manual.write_text(
            f'\n2020-01-01 * "Acquired" "property purchase"\n'
            f"  Assets:Personal:Property:{slug}  {book_value}.00 USD\n"
            f"  Equity:Personal:OpeningBalances  -{book_value}.00 USD\n",
            encoding="utf-8",
        )
        main = ledger_dir / "main.bean"
        main.write_text(
            'option "operating_currency" "USD"\n'
            'include "connector_accounts.bean"\n'
            'include "manual_transactions.bean"\n'
            'include "connector_overrides.bean"\n',
            encoding="utf-8",
        )
        (ledger_dir / "connector_overrides.bean").write_text(
            "; overrides\n", encoding="utf-8",
        )
        reader = getattr(app_client.app.state, "ledger_reader", None)
        if reader is not None:
            reader.invalidate()
        db = app_client.app.state.db
        self._seed_two_entities(db)
        db.execute(
            "INSERT OR REPLACE INTO properties "
            "(slug, display_name, entity_slug, property_type, "
            "is_active, asset_account_path) "
            f"VALUES ('{slug}', 'Main residence', 'Personal', 'house', "
            f"1, 'Assets:Personal:Property:{slug}')"
        )
        db.commit()

    @pytest.mark.xfail(
        reason="pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md",
        strict=False,
    )
    def test_change_ownership_page_renders(self, app_client, ledger_dir):
        slug = "Pmain"
        self._prep(app_client, ledger_dir, slug=slug)
        r = app_client.get(
            f"/settings/properties/{slug}/change-ownership",
            follow_redirects=False,
        )
        assert r.status_code == 200
        # Form fields present
        assert 'name="new_entity_slug"' in r.text
        assert 'name="transfer_date"' in r.text
        assert 'name="cash_amount"' in r.text
        assert 'name="equity_amount"' in r.text
        assert 'name="basis_choice"' in r.text
        # Carryover NBV pre-filled with current book value
        assert "200000.00" in r.text

    def test_transfer_pure_equity_balanced(self, app_client, ledger_dir):
        from decimal import Decimal
        from beancount import loader
        from beancount.core.data import Transaction
        slug = "Pmain"
        self._prep(app_client, ledger_dir, slug=slug, book_value="200000")
        r = app_client.post(
            f"/settings/properties/{slug}/change-ownership/transfer",
            data={
                "new_entity_slug": "Acme",
                "transfer_date": "2026-04-24",
                "cash_amount": "0",
                "equity_amount": "200000",
                "basis_choice": "carryover",
            },
            follow_redirects=False,
        )
        assert r.status_code == 303, r.text
        loc = r.headers["location"]
        assert "error" not in loc, f"transfer failed: {loc}"
        assert "transferred_to=Acme" in loc

        db = app_client.app.state.db
        old = db.execute(
            "SELECT is_active, sale_date FROM properties WHERE slug = ?",
            (slug,),
        ).fetchone()
        assert old["is_active"] == 0
        assert old["sale_date"] == "2026-04-24"
        new_rows = db.execute(
            "SELECT slug, entity_slug FROM properties "
            "WHERE entity_slug = 'Acme' AND is_active = 1"
        ).fetchall()
        assert len(new_rows) == 1

        entries, errors, _ = loader.load_file(
            str(ledger_dir / "main.bean")
        )
        assert errors == [], f"ledger has errors: {errors}"
        bal: dict[str, Decimal] = {}
        for t in entries:
            if not isinstance(t, Transaction):
                continue
            for p in t.postings or ():
                if p.units and p.units.number is not None:
                    bal[p.account] = bal.get(
                        p.account, Decimal(0),
                    ) + Decimal(p.units.number)
        assert bal.get(
            f"Assets:Personal:Property:{slug}", Decimal(0),
        ) == Decimal(0)
        assert bal.get(
            f"Assets:Acme:Property:{slug}", Decimal(0),
        ) == Decimal("200000")

    def test_transfer_with_recapture(self, app_client, ledger_dir):
        from decimal import Decimal
        from beancount import loader
        from beancount.core.data import Transaction
        slug = "Pmain"
        self._prep(app_client, ledger_dir, slug=slug, book_value="200000")
        # Sell at $250k → 50k gain → SaleRecapture posts -50000
        r = app_client.post(
            f"/settings/properties/{slug}/change-ownership/transfer",
            data={
                "new_entity_slug": "Acme",
                "transfer_date": "2026-04-24",
                "cash_amount": "0",
                "equity_amount": "250000",
                "basis_choice": "sale_price",
            },
            follow_redirects=False,
        )
        assert r.status_code == 303, r.text
        loc = r.headers["location"]
        assert "error" not in loc, f"transfer failed: {loc}"
        entries, errors, _ = loader.load_file(
            str(ledger_dir / "main.bean")
        )
        assert errors == [], f"ledger has errors: {errors}"
        bal: dict[str, Decimal] = {}
        for t in entries:
            if not isinstance(t, Transaction):
                continue
            for p in t.postings or ():
                if p.units and p.units.number is not None:
                    bal[p.account] = bal.get(
                        p.account, Decimal(0),
                    ) + Decimal(p.units.number)
        assert bal.get(
            f"Equity:Personal:Property:{slug}:SaleRecapture", Decimal(0),
        ) == Decimal("-50000")

    def test_transfer_rejects_same_entity(self, app_client, ledger_dir):
        slug = "Pmain"
        self._prep(app_client, ledger_dir, slug=slug)
        r = app_client.post(
            f"/settings/properties/{slug}/change-ownership/transfer",
            data={
                "new_entity_slug": "Personal",
                "transfer_date": "2026-04-24",
                "cash_amount": "0",
                "equity_amount": "200000",
                "basis_choice": "carryover",
            },
            follow_redirects=False,
        )
        assert r.status_code == 303
        assert "error=same-entity" in r.headers["location"]


class TestVehicleIntercompanyTransfer:
    """Phase 4.2: ``POST /vehicles/{slug}/change-ownership/transfer``
    is no longer a 501 stub. Records a real-event intercompany
    transfer — disposal on the old entity + acquisition on the new,
    six legs total, slug-embedded scaffolds, asset-basis radio
    (carryover / sale price / explicit). Plugs any gap between book
    value and transaction value to ``...:SaleRecapture`` for the CPA."""

    def _seed_two_entities(self, db):
        _seed(db)
        db.execute(
            "INSERT OR IGNORE INTO entities (slug, display_name, "
            "entity_type, is_active) "
            "VALUES ('Acme', 'Acme LLC', 'sole-prop', 1)"
        )
        db.commit()

    def _prep(self, app_client, ledger_dir, *, slug="Vt", book_value="25000"):
        connector_accounts = ledger_dir / "connector_accounts.bean"
        connector_accounts.write_text(
            "; test scenario\n"
            f"2020-01-01 open Assets:Personal:Vehicle:{slug}\n"
            f"2020-01-01 open Expenses:Personal:Vehicle:{slug}:Fuel\n"
            "2020-01-01 open Equity:Personal:OpeningBalances\n",
            encoding="utf-8",
        )
        manual = ledger_dir / "manual_transactions.bean"
        manual.write_text(
            f'\n2020-01-01 * "Acquired" "vehicle purchase"\n'
            f"  Assets:Personal:Vehicle:{slug}  {book_value}.00 USD\n"
            f"  Equity:Personal:OpeningBalances  -{book_value}.00 USD\n",
            encoding="utf-8",
        )
        main = ledger_dir / "main.bean"
        main.write_text(
            'option "operating_currency" "USD"\n'
            'include "connector_accounts.bean"\n'
            'include "manual_transactions.bean"\n'
            'include "connector_overrides.bean"\n',
            encoding="utf-8",
        )
        (ledger_dir / "connector_overrides.bean").write_text(
            "; overrides\n", encoding="utf-8",
        )
        reader = getattr(app_client.app.state, "ledger_reader", None)
        if reader is not None:
            reader.invalidate()
        db = app_client.app.state.db
        self._seed_two_entities(db)
        db.execute(
            "INSERT OR REPLACE INTO vehicles "
            "(slug, display_name, entity_slug, is_active) "
            f"VALUES ('{slug}', 'Test Vehicle', 'Personal', 1)"
        )
        db.commit()

    def test_transfer_pure_equity_balanced(self, app_client, ledger_dir):
        """All-equity transfer at book value. Six legs, all balanced.
        SaleRecapture should NOT post (cash + equity = book value)."""
        from decimal import Decimal
        from beancount import loader
        from beancount.core.data import Transaction
        slug = "Vt"
        self._prep(app_client, ledger_dir, slug=slug, book_value="25000")
        r = app_client.post(
            f"/vehicles/{slug}/change-ownership/transfer",
            data={
                "new_entity_slug": "Acme",
                "transfer_date": "2026-04-24",
                "cash_amount": "0",
                "equity_amount": "25000",
                "basis_choice": "carryover",
            },
            follow_redirects=False,
        )
        assert r.status_code == 303, r.text
        loc = r.headers["location"]
        assert "transferred_to=Acme" in loc, loc
        # Old vehicle marked inactive; new vehicle row created on Acme
        db = app_client.app.state.db
        old = db.execute(
            "SELECT is_active, sale_date, sale_price FROM vehicles WHERE slug = ?",
            (slug,),
        ).fetchone()
        assert old["is_active"] == 0
        assert old["sale_date"] == "2026-04-24"
        assert str(old["sale_price"]) == "25000"
        new_rows = db.execute(
            "SELECT slug, entity_slug, is_active FROM vehicles "
            "WHERE entity_slug = 'Acme' AND is_active = 1"
        ).fetchall()
        assert len(new_rows) == 1
        # Bean-check should be clean
        entries, errors, _ = loader.load_file(
            str(ledger_dir / "main.bean")
        )
        assert errors == [], f"ledger has errors: {errors}"
        # Net balances
        bal: dict[str, Decimal] = {}
        for t in entries:
            if not isinstance(t, Transaction):
                continue
            for p in t.postings or ():
                if p.units and p.units.number is not None:
                    bal[p.account] = bal.get(
                        p.account, Decimal(0),
                    ) + Decimal(p.units.number)
        # Old asset is now zero (was 25000, disposal posted -25000)
        assert bal.get(
            f"Assets:Personal:Vehicle:{slug}", Decimal(0),
        ) == Decimal(0)
        # New asset has 25000 (carryover NBV)
        assert bal.get(
            f"Assets:Acme:Vehicle:{slug}", Decimal(0),
        ) == Decimal("25000")
        # Equity legs balance to zero across both entities
        assert bal.get(
            f"Equity:Personal:Vehicle:{slug}:SaleEquity", Decimal(0),
        ) == Decimal("25000")
        assert bal.get(
            f"Equity:Acme:Vehicle:{slug}:PurchaseEquity", Decimal(0),
        ) == Decimal("-25000")
        # No SaleRecapture (gap was zero)
        rec_path = f"Equity:Personal:Vehicle:{slug}:SaleRecapture"
        assert rec_path not in bal or bal[rec_path] == Decimal(0)

    def test_transfer_mixed_cash_and_equity(self, app_client, ledger_dir):
        from decimal import Decimal
        from beancount import loader
        from beancount.core.data import Transaction
        slug = "Vt"
        self._prep(app_client, ledger_dir, slug=slug, book_value="25000")
        r = app_client.post(
            f"/vehicles/{slug}/change-ownership/transfer",
            data={
                "new_entity_slug": "Acme",
                "transfer_date": "2026-04-24",
                "cash_amount": "5000",
                "equity_amount": "20000",
                "basis_choice": "sale_price",
            },
            follow_redirects=False,
        )
        assert r.status_code == 303, r.text
        entries, errors, _ = loader.load_file(
            str(ledger_dir / "main.bean")
        )
        assert errors == [], f"ledger has errors: {errors}"
        bal: dict[str, Decimal] = {}
        for t in entries:
            if not isinstance(t, Transaction):
                continue
            for p in t.postings or ():
                if p.units and p.units.number is not None:
                    bal[p.account] = bal.get(
                        p.account, Decimal(0),
                    ) + Decimal(p.units.number)
        # SaleClearing on old: +5000
        assert bal.get(
            f"Assets:Personal:Vehicle:{slug}:SaleClearing", Decimal(0),
        ) == Decimal("5000")
        # PurchaseClearing on new: -5000
        assert bal.get(
            f"Assets:Acme:Vehicle:{slug}:PurchaseClearing", Decimal(0),
        ) == Decimal("-5000")
        # SaleEquity old: +20000; PurchaseEquity new: -20000
        assert bal.get(
            f"Equity:Personal:Vehicle:{slug}:SaleEquity", Decimal(0),
        ) == Decimal("20000")
        assert bal.get(
            f"Equity:Acme:Vehicle:{slug}:PurchaseEquity", Decimal(0),
        ) == Decimal("-20000")

    def test_transfer_with_recapture_when_value_exceeds_book(
        self, app_client, ledger_dir,
    ):
        """Sale price > book value → SaleRecapture posts the gain."""
        from decimal import Decimal
        from beancount import loader
        from beancount.core.data import Transaction
        slug = "Vt"
        self._prep(app_client, ledger_dir, slug=slug, book_value="20000")
        r = app_client.post(
            f"/vehicles/{slug}/change-ownership/transfer",
            data={
                "new_entity_slug": "Acme",
                "transfer_date": "2026-04-24",
                "cash_amount": "0",
                "equity_amount": "25000",
                "basis_choice": "sale_price",
            },
            follow_redirects=False,
        )
        assert r.status_code == 303, r.text
        loc = r.headers["location"]
        assert "error" not in loc, f"transfer failed: {loc}"
        entries, errors, _ = loader.load_file(
            str(ledger_dir / "main.bean")
        )
        assert errors == [], f"ledger has errors: {errors}"
        bal: dict[str, Decimal] = {}
        for t in entries:
            if not isinstance(t, Transaction):
                continue
            for p in t.postings or ():
                if p.units and p.units.number is not None:
                    bal[p.account] = bal.get(
                        p.account, Decimal(0),
                    ) + Decimal(p.units.number)
        # Gap: book 20000, transaction value 25000 → 5000 gain.
        # In Beancount equity convention, gain credits equity →
        # SaleRecapture posts NEGATIVE (-5000). CPA reads sign to
        # tell gain (negative) from loss (positive).
        assert bal.get(
            f"Equity:Personal:Vehicle:{slug}:SaleRecapture", Decimal(0),
        ) == Decimal("-5000")

    def test_transfer_rejects_zero_transaction_value(
        self, app_client, ledger_dir,
    ):
        slug = "Vt"
        self._prep(app_client, ledger_dir, slug=slug, book_value="25000")
        r = app_client.post(
            f"/vehicles/{slug}/change-ownership/transfer",
            data={
                "new_entity_slug": "Acme",
                "transfer_date": "2026-04-24",
                "cash_amount": "0",
                "equity_amount": "0",
                "basis_choice": "carryover",
            },
            follow_redirects=False,
        )
        assert r.status_code == 303
        assert "error=zero-transaction-value" in r.headers["location"]

    def test_transfer_rejects_missing_basis_choice(
        self, app_client, ledger_dir,
    ):
        slug = "Vt"
        self._prep(app_client, ledger_dir, slug=slug, book_value="25000")
        r = app_client.post(
            f"/vehicles/{slug}/change-ownership/transfer",
            data={
                "new_entity_slug": "Acme",
                "transfer_date": "2026-04-24",
                "cash_amount": "5000",
                "equity_amount": "0",
                # basis_choice intentionally omitted
            },
            follow_redirects=False,
        )
        assert r.status_code == 303
        assert "error=missing-basis-choice" in r.headers["location"]


class TestEntityMigrateBalanceInvariant:
    """Phase 5.1: parity with vehicle migrate's balance-invariant
    suite. Entity-manage migrate-account writes overrides that move
    every posting from a source account to a target account. Net
    invariant: sum of original signed-amount postings == sum of
    target-account postings after migration. Source net should be
    zero (originals + offsetting overrides cancel)."""

    def _balances(self, ledger_dir):
        from beancount import loader
        from beancount.core.data import Transaction
        from decimal import Decimal
        entries, _, _ = loader.load_file(str(ledger_dir / "main.bean"))
        bal: dict[str, Decimal] = {}
        for t in entries:
            if not isinstance(t, Transaction):
                continue
            for p in t.postings or ():
                if p.units and p.units.number is not None:
                    bal[p.account] = bal.get(p.account, Decimal(0)) + Decimal(p.units.number)
        return bal

    def _prep(self, app_client, ledger_dir):
        _seed(app_client.app.state.db)
        # Assets:GhostCo:Bank:Checking → Assets:Personal:Bank:Checking
        # is the canonical "I labeled accounts under the wrong entity"
        # scenario. Entity-manage migrate-account is the surface the
        # user clicks to fix it.
        connector_accounts = ledger_dir / "connector_accounts.bean"
        connector_accounts.write_text(
            "; test scenario\n"
            "2020-01-01 open Assets:GhostCo:Bank:Checking\n"
            "2020-01-01 open Assets:Personal:Bank:Checking\n"
            "2020-01-01 open Income:Personal:Salary\n",
            encoding="utf-8",
        )
        manual = ledger_dir / "manual_transactions.bean"
        txns = "\n"
        for date_, amt in [
            ("2025-01-15", "1000.00"),
            ("2025-02-15", "1500.00"),
            ("2025-03-15", "2000.50"),
        ]:
            txns += (
                f'{date_} * "ACME Payroll" "monthly direct deposit"\n'
                f"  Assets:GhostCo:Bank:Checking  {amt} USD\n"
                f"  Income:Personal:Salary  -{amt} USD\n\n"
            )
        manual.write_text(txns, encoding="utf-8")
        main = ledger_dir / "main.bean"
        main.write_text(
            'option "operating_currency" "USD"\n'
            'include "connector_accounts.bean"\n'
            'include "manual_transactions.bean"\n'
            'include "connector_overrides.bean"\n',
            encoding="utf-8",
        )
        (ledger_dir / "connector_overrides.bean").write_text(
            "; overrides\n", encoding="utf-8",
        )
        reader = getattr(app_client.app.state, "ledger_reader", None)
        if reader is not None:
            reader.invalidate()
        db = app_client.app.state.db
        db.execute(
            "INSERT OR IGNORE INTO entities (slug, display_name, "
            "is_active) VALUES ('GhostCo', 'GhostCo', 1)"
        )
        db.execute(
            "INSERT OR REPLACE INTO accounts_meta "
            "(account_path, display_name, entity_slug, kind) "
            "VALUES ('Assets:GhostCo:Bank:Checking', 'GhostCo "
            "Checking', 'GhostCo', 'bank')"
        )
        db.commit()

    def test_full_migrate_preserves_total_dollar_amount(
        self, app_client, ledger_dir,
    ):
        from decimal import Decimal
        self._prep(app_client, ledger_dir)
        bal_before = self._balances(ledger_dir)
        source_total = bal_before.get(
            "Assets:GhostCo:Bank:Checking", Decimal(0)
        )
        # Three deposits totaling 4500.50
        assert source_total == Decimal("4500.50")

        r = app_client.post(
            "/setup/entities/GhostCo/migrate-account",
            data={
                "account": "Assets:GhostCo:Bank:Checking",
                "target": "Assets:Personal:Bank:Checking",
            },
            follow_redirects=False,
        )
        assert r.status_code == 303, r.text
        assert "migrated=3" in r.headers["location"], r.headers["location"]

        bal_after = self._balances(ledger_dir)
        # Source net is 0 — overrides exactly cancel the originals.
        assert bal_after.get(
            "Assets:GhostCo:Bank:Checking", Decimal(0)
        ) == Decimal(0)
        # Target absorbs the full original total.
        assert bal_after.get(
            "Assets:Personal:Bank:Checking", Decimal(0)
        ) == source_total
        # Income side is untouched (overrides only redirect the
        # source posting; the income leg stays where it was).
        assert (
            bal_after.get("Income:Personal:Salary", Decimal(0))
            == bal_before.get("Income:Personal:Salary", Decimal(0))
        )

    def test_re_clicking_migrate_is_a_noop(
        self, app_client, ledger_dir,
    ):
        """§7 #4 shape — re-running the migrate button after a clean
        run should re-walk and skip every already-migrated txn."""
        from decimal import Decimal
        self._prep(app_client, ledger_dir)
        # First run
        r1 = app_client.post(
            "/setup/entities/GhostCo/migrate-account",
            data={
                "account": "Assets:GhostCo:Bank:Checking",
                "target": "Assets:Personal:Bank:Checking",
            },
            follow_redirects=False,
        )
        assert r1.status_code == 303
        bal_after_1 = self._balances(ledger_dir)
        # Second run — should hit "no-postings-found" or migrated=0
        r2 = app_client.post(
            "/setup/entities/GhostCo/migrate-account",
            data={
                "account": "Assets:GhostCo:Bank:Checking",
                "target": "Assets:Personal:Bank:Checking",
            },
            follow_redirects=False,
        )
        assert r2.status_code == 303
        bal_after_2 = self._balances(ledger_dir)
        # Balances must not drift on the second click.
        assert bal_after_2.get(
            "Assets:GhostCo:Bank:Checking", Decimal(0)
        ) == bal_after_1.get(
            "Assets:GhostCo:Bank:Checking", Decimal(0)
        )
        assert bal_after_2.get(
            "Assets:Personal:Bank:Checking", Decimal(0)
        ) == bal_after_1.get(
            "Assets:Personal:Bank:Checking", Decimal(0)
        )


class TestImportAppliedCheckAndRedirect:
    """Phase 3.2: ``_check_import_applied`` reads
    ``connector_imports/*.bean`` and reports complete when any import
    file exists. Post Phase-7, ``/setup/import/apply`` success
    redirects to ``/setup/recovery`` (the renamed canonical entry
    point) and the underlying check is asserted against
    :func:`compute_setup_progress` directly — the page-level
    checklist UI was retired with the URL rename."""

    def test_check_import_applied_complete_when_imports_dir_populated(
        self, app_client, ledger_dir,
    ):
        _seed(app_client.app.state.db)
        imports_dir = ledger_dir / "connector_imports"
        imports_dir.mkdir(exist_ok=True)
        (imports_dir / "_2024.bean").write_text("; imported", encoding="utf-8")
        reader = getattr(app_client.app.state, "ledger_reader", None)
        if reader is not None:
            reader.invalidate()

        from lamella.features.setup.setup_progress import compute_setup_progress
        entries = list(reader.load().entries) if reader else []
        progress = compute_setup_progress(
            app_client.app.state.db, entries,
            imports_dir=app_client.app.state.settings.import_ledger_output_dir_resolved,
        )
        step = next(
            (s for s in progress.steps if s.label == "Prior ledger imported"),
            None,
        )
        assert step is not None, "Prior ledger imported step missing"
        assert step.is_complete, "import-detection failed to register populated dir"

    def test_check_import_applied_incomplete_when_dir_missing(
        self, app_client, ledger_dir,
    ):
        _seed(app_client.app.state.db)
        imports_dir = ledger_dir / "connector_imports"
        if imports_dir.exists():
            for p in imports_dir.iterdir():
                p.unlink()
            imports_dir.rmdir()

        from lamella.features.setup.setup_progress import compute_setup_progress
        reader = getattr(app_client.app.state, "ledger_reader", None)
        entries = list(reader.load().entries) if reader else []
        progress = compute_setup_progress(
            app_client.app.state.db, entries,
            imports_dir=app_client.app.state.settings.import_ledger_output_dir_resolved,
        )
        step = next(
            (s for s in progress.steps if s.label == "Prior ledger imported"),
            None,
        )
        assert step is not None
        assert not step.is_complete


class TestAutoCloseOrphanAfterMigrate:
    """Phase 3.1: when a migrate batch drains every unmigrated posting
    off an orphan path AND nothing failed, the handler auto-writes a
    Close directive so the dead account drops off /setup/vehicles and
    /setup/accounts without forcing a second click. Partial batches
    (any failed) MUST NOT auto-close so the user retains control."""

    def _prep(self, app_client, ledger_dir):
        # Reuse the orphan-scenario builder from the migrate-net class.
        prep = TestMigrateNetBalancesAndIdempotency()._prep_orphan_scenario
        prep(app_client, ledger_dir)
        db = app_client.app.state.db
        db.execute(
            "INSERT INTO vehicles (slug, display_name, entity_slug, is_active) "
            "VALUES ('Vt', 'Test', 'Personal', 1)"
        )
        db.commit()

    def _hashes(self, app_client):
        return TestMigrateNetBalancesAndIdempotency()._txn_hashes_on_orphan(
            app_client, "Vt", "Expenses:Personal:Custom:LegacyGas"
        )

    def test_full_migrate_writes_close_directive(self, app_client, ledger_dir):
        self._prep(app_client, ledger_dir)
        hashes = self._hashes(app_client)
        assert len(hashes) == 3
        r = app_client.post(
            "/setup/vehicles/Vt/migrate",
            data={
                "orphan": "Expenses:Personal:Custom:LegacyGas",
                "target": "Expenses:Personal:Vehicle:Vt:Fuel",
                "txn_hash": hashes,
            },
            follow_redirects=False,
        )
        assert r.status_code == 303
        loc = r.headers.get("location", "")
        from urllib.parse import unquote
        assert "auto_closed=" in loc
        assert "Expenses%3APersonal%3ACustom%3ALegacyGas" in loc or (
            "Expenses:Personal:Custom:LegacyGas" in unquote(loc)
        )
        accounts_text = (
            ledger_dir / "connector_accounts.bean"
        ).read_text(encoding="utf-8")
        assert "close Expenses:Personal:Custom:LegacyGas" in accounts_text

    def test_partial_migrate_does_not_auto_close(self, app_client, ledger_dir):
        """Migrate only 2 of 3 hashes — orphan still has 1 unmigrated
        posting, so auto-close MUST NOT fire."""
        self._prep(app_client, ledger_dir)
        hashes = self._hashes(app_client)
        partial = hashes[:2]  # leave one
        r = app_client.post(
            "/setup/vehicles/Vt/migrate",
            data={
                "orphan": "Expenses:Personal:Custom:LegacyGas",
                "target": "Expenses:Personal:Vehicle:Vt:Fuel",
                "txn_hash": partial,
            },
            follow_redirects=False,
        )
        assert r.status_code == 303
        loc = r.headers.get("location", "")
        assert "auto_closed=" not in loc
        accounts_text = (
            ledger_dir / "connector_accounts.bean"
        ).read_text(encoding="utf-8")
        assert "close Expenses:Personal:Custom:LegacyGas" not in accounts_text

    def test_idempotent_when_already_closed(self, app_client, ledger_dir):
        """If the orphan already has a Close directive, auto-close must
        be a no-op (no duplicate Close written)."""
        self._prep(app_client, ledger_dir)
        # Pre-close the orphan path manually
        accounts_file = ledger_dir / "connector_accounts.bean"
        accounts_file.write_text(
            accounts_file.read_text(encoding="utf-8")
            + "\n2099-01-01 close Expenses:Personal:Custom:LegacyGas\n",
            encoding="utf-8",
        )
        reader = getattr(app_client.app.state, "ledger_reader", None)
        if reader is not None:
            reader.invalidate()
        # Manually drain the postings so migrate has work to do without
        # tripping the pre-existing Close (push them into far past).
        # Actually, pre-existing close blocks postings after it; date
        # 2099 keeps 2025 postings legal.
        hashes = self._hashes(app_client)
        if not hashes:
            return  # filtered by close — that's fine, nothing to test
        app_client.post(
            "/setup/vehicles/Vt/migrate",
            data={
                "orphan": "Expenses:Personal:Custom:LegacyGas",
                "target": "Expenses:Personal:Vehicle:Vt:Fuel",
                "txn_hash": hashes,
            },
            follow_redirects=False,
        )
        accounts_text = accounts_file.read_text(encoding="utf-8")
        # Exactly one Close directive — no duplicate from auto-close.
        assert accounts_text.count(
            "close Expenses:Personal:Custom:LegacyGas"
        ) == 1


class TestClosedAccountsHidden:
    """Closed accounts must not appear as orphans on setup pages or
    in entity-manage lists. Regression guard: the orphan scan used to
    build from ALL Open directives without subtracting Closes — so
    clicking 'Close 37 unused' succeeded at the ledger layer but the
    UI still showed them, producing the complaint 'I clicked close
    and nothing happened.'"""

    def test_close_directive_hides_account_from_setup_vehicles(
        self, app_client, ledger_dir
    ):
        _seed(app_client.app.state.db)
        # Write both Open and Close for a legacy-shape orphan path
        accounts_file = ledger_dir / "connector_accounts.bean"
        initial = accounts_file.read_text(encoding="utf-8") if accounts_file.exists() else ""
        accounts_file.write_text(
            initial
            + "\n2024-01-01 open Expenses:Vehicles:Vlegacy:Fuel\n"
            + "2026-04-24 close Expenses:Vehicles:Vlegacy:Fuel\n",
            encoding="utf-8",
        )
        # Invalidate reader if live
        reader = getattr(app_client.app.state, "ledger_reader", None)
        if reader is not None:
            reader.invalidate()
        r = app_client.get("/setup/vehicles", follow_redirects=False)
        assert r.status_code == 200
        # The legacy-shape orphan should NOT appear since it was closed
        assert "Vlegacy" not in r.text, (
            "/setup/vehicles showed a closed legacy-shape orphan — "
            "it's been Closed in the ledger and should drop off the list"
        )


class TestEntityReassignmentInterlock:
    """Changing the owning-entity on a vehicle/property with history
    must go through the deliberate change-ownership flow, not the
    regular edit form. Server-side guards must silently preserve the
    original entity_slug even if a malicious/curl POST tries to
    change it. Regression guard for silent tax-accounting drift."""

    def test_vehicle_edit_cannot_change_entity_slug(self, app_client):
        _seed(app_client.app.state.db)
        db = app_client.app.state.db
        db.execute(
            "INSERT INTO vehicles (slug, display_name, entity_slug, is_active) "
            "VALUES ('Vowned','Owned','BetaCorp',1)"
        )
        db.commit()
        # Attempt to change entity through the regular save endpoint
        app_client.post(
            "/vehicles",
            data={
                "slug": "Vowned", "display_name": "Owned",
                "entity_slug": "Personal",  # trying to change from BetaCorp
                "is_active": "1",
            },
            follow_redirects=False,
        )
        row = db.execute(
            "SELECT entity_slug FROM vehicles WHERE slug='Vowned'"
        ).fetchone()
        assert row["entity_slug"] == "BetaCorp", (
            "save_vehicle let entity_slug change through the regular "
            "form — bypasses the change-ownership interlock"
        )

    def test_vehicle_change_ownership_page_renders(self, app_client):
        _seed(app_client.app.state.db)
        db = app_client.app.state.db
        db.execute(
            "INSERT INTO vehicles (slug, display_name, entity_slug, is_active) "
            "VALUES ('Vown2','Own2','BetaCorp',1)"
        )
        db.commit()
        r = app_client.get("/vehicles/Vown2/change-ownership")
        assert r.status_code == 200
        # Both option titles must render. Option B was renamed from
        # "Disposal + re-acquisition" → "Intercompany transfer" during
        # the change-ownership flow's tax-language overhaul; updating
        # the assertion to track. The semantics (rename-vs-transfer
        # split) are unchanged — only the label evolved.
        assert "Misattribution fix" in r.text
        assert "Intercompany transfer" in r.text

    def test_property_save_cannot_change_entity_slug(self, app_client):
        _seed(app_client.app.state.db)
        db = app_client.app.state.db
        db.execute(
            "INSERT INTO properties (slug, display_name, property_type, "
            "entity_slug, is_active) "
            "VALUES ('Powned','PropOwned','other','BetaCorp',1)"
        )
        db.commit()
        app_client.post(
            "/settings/properties",
            data={
                "slug": "Powned", "display_name": "PropOwned",
                "entity_slug": "Personal",
            },
            follow_redirects=False,
        )
        row = db.execute(
            "SELECT entity_slug FROM properties WHERE slug='Powned'"
        ).fetchone()
        assert row["entity_slug"] == "BetaCorp", (
            "save_property let entity_slug change — bypasses interlock"
        )


class TestSlugCollision:
    """POST /vehicles, /settings/properties, /settings/loans with a
    slug that already exists and intent=create must refuse with 409
    + a disambiguation suggestion. Without intent, the handler
    silently upserts (used by edit forms)."""

    def test_duplicate_slug_with_create_intent_is_refused(self, app_client):
        _seed(app_client.app.state.db)
        db = app_client.app.state.db
        db.execute(
            "INSERT INTO vehicles (slug, display_name, entity_slug, is_active) "
            "VALUES ('Vdup','First','Personal',1)"
        )
        db.commit()
        r = app_client.post(
            "/vehicles",
            data={
                "intent": "create",
                "slug": "Vdup", "display_name": "Second one",
                "entity_slug": "Personal",
                "is_active": "1",
            },
            follow_redirects=False,
        )
        assert r.status_code == 409, (
            f"expected 409 on duplicate create, got {r.status_code}"
        )
        body = r.text
        # Expect the suggestion in the error message
        assert "Vdup2" in body, (
            f"expected suggested disambiguated slug Vdup2 in body: {body[:300]!r}"
        )

    def test_duplicate_property_slug_with_create_intent_is_refused(
        self, app_client,
    ):
        _seed(app_client.app.state.db)
        db = app_client.app.state.db
        db.execute(
            "INSERT INTO properties (slug, display_name, entity_slug, "
            "is_active, asset_account_path, property_type) "
            "VALUES ('Pdup','First','Personal',1,"
            "'Assets:Personal:Property:Pdup','primary')"
        )
        db.commit()
        r = app_client.post(
            "/settings/properties",
            data={
                "intent": "create",
                "slug": "Pdup",
                "display_name": "Second one",
                "entity_slug": "Personal",
                "property_type": "primary",
                "is_active": "1",
            },
            follow_redirects=False,
        )
        assert r.status_code == 409, (
            f"expected 409, got {r.status_code}: {r.text[:300]!r}"
        )
        assert "Pdup2" in r.text

    def test_duplicate_loan_slug_with_create_intent_is_refused(
        self, app_client,
    ):
        _seed(app_client.app.state.db)
        db = app_client.app.state.db
        db.execute(
            "INSERT INTO loans (slug, display_name, entity_slug, "
            "loan_type, original_principal, funded_date, is_active) "
            "VALUES ('Ldup','First','Personal','mortgage',"
            "100000,'2020-01-01',1)"
        )
        db.commit()
        r = app_client.post(
            "/settings/loans",
            data={
                "intent": "create",
                "slug": "Ldup",
                "display_name": "Second one",
                "entity_slug": "Personal",
                "loan_type": "mortgage",
                "is_active": "1",
            },
            follow_redirects=False,
        )
        assert r.status_code == 409, (
            f"expected 409, got {r.status_code}: {r.text[:300]!r}"
        )
        assert "Ldup2" in r.text

    def test_property_silent_upsert_without_intent(self, app_client):
        """The edit form does NOT carry intent=create. Handler must
        UPDATE silently (not 409) so the edit flow works."""
        _seed(app_client.app.state.db)
        db = app_client.app.state.db
        db.execute(
            "INSERT INTO properties (slug, display_name, entity_slug, "
            "is_active, asset_account_path, property_type) "
            "VALUES ('Pkeep','Original','Personal',1,"
            "'Assets:Personal:Property:Pkeep','primary')"
        )
        db.commit()
        r = app_client.post(
            "/settings/properties",
            data={
                # NO intent=create — represents the edit form's POST.
                "slug": "Pkeep",
                "display_name": "Updated name",
                "entity_slug": "Personal",
                "property_type": "primary",
                "is_active": "1",
            },
            follow_redirects=False,
        )
        # 303 redirect or 200 update — both are upsert outcomes; the
        # critical assertion is "not 409".
        assert r.status_code in (200, 303), (
            f"edit-flow must not be refused, got {r.status_code}"
        )
        row = db.execute(
            "SELECT display_name FROM properties WHERE slug = 'Pkeep'",
        ).fetchone()
        assert row["display_name"] == "Updated name"


class TestVehicleCreation:
    def test_creating_vehicle_auto_scaffolds_canonical_chart(
        self, app_client, ledger_dir
    ):
        """Regression: POST /vehicles was calling the legacy helper
        _vehicle_expense_paths which writes non-canonical paths like
        `Expenses:Vehicles:<slug>:Fuel` (plural "Vehicles" as entity
        segment) — which resurrects a phantom "Vehicles" entity on
        boot. Must use the canonical shape
        `Expenses:<Entity>:Vehicle:<slug>:Fuel` (singular)."""
        _seed(app_client.app.state.db)
        r = app_client.post(
            "/vehicles",
            data={
                "slug": "Vtest", "display_name": "Test Car",
                "entity_slug": "Personal",
                "year": "2020", "make": "Fabrikam", "model": "Camry",
                "create_expense_tree": "1",
            },
            follow_redirects=False,
        )
        assert r.status_code in (200, 303), (
            f"POST /vehicles failed: {r.status_code} {r.text[:200]!r}"
        )
        connector_accounts = ledger_dir / "connector_accounts.bean"
        assert connector_accounts.exists()
        text = connector_accounts.read_text(encoding="utf-8")
        # Must write canonical singular `Vehicle` under Personal entity
        assert "Expenses:Personal:Vehicle:Vtest:Fuel" in text, (
            "canonical per-vehicle path not scaffolded; legacy shape?"
        )
        # Must NOT write legacy `Vehicles:Vtest:*` (entity-less / plural)
        assert "Expenses:Vehicles:Vtest:" not in text, (
            "legacy non-canonical path leaked back into the writer"
        )


class TestSetupGateSemantics:
    def test_vehicles_edit_is_reachable_when_gate_is_on(self, app_client):
        """Clicking 'Set owning entity →' on /setup/vehicles goes to
        /vehicles/<slug>/edit. Even with the setup-completeness gate
        on, that link must not bounce to the recovery surface — the
        whole point of the link is to let the user edit the entity
        binding DURING setup. Regression guard for the 2-hour-cycle
        issue."""
        app_client.app.state.setup_required_complete = False
        r = app_client.get("/vehicles", follow_redirects=False)
        # Must not redirect into the setup flow (would strand the user)
        loc = r.headers.get("location", "")
        assert r.status_code != 303 or (
            "setup/recovery" not in loc and "setup/progress" not in loc
        ), f"vehicles listing bounced to setup surface: {loc}"

    def test_mileage_projects_budgets_reachable_when_gate_is_on(self, app_client):
        app_client.app.state.setup_required_complete = False
        for url in ["/mileage", "/projects", "/budgets", "/note"]:
            r = app_client.get(url, follow_redirects=False)
            loc = r.headers.get("location", "")
            assert "/setup/recovery" not in loc and "/setup/progress" not in loc, (
                f"{url} bounced to a setup surface with gate on — user can't "
                f"finish setup if this path is blocked"
            )

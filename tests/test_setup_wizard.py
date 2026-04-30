# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the first-run onboarding wizard at ``/setup/wizard``.

Spec: ``FIRST-RUN.md``. Scope here is the wizard's own state machine
+ its handoff to the existing setup gate. We're verifying:

  1. A fresh install with the wizard not completed lands on the
     wizard, not on the maintenance checklist.
  2. Each intent option scaffolds the right entities.
  3. State persists across requests (resume on refresh).
  4. Account creation happens via AccountsWriter (single account
     enough to advance).
  5. Loan-kind metadata drives the property/vehicle smart prompts.
  6. The wizard becomes unreachable after completion.
  7. The "configure myself" exit routes to the existing checklist.

The fixture pattern mirrors ``test_setup_e2e.py`` — empty tmpdir
ledger + create_app + TestClient with bean-check stubbed.
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
def empty_ledger_dir(tmp_path: Path) -> Path:
    d = tmp_path / "ledger"
    d.mkdir()
    return d


@pytest.fixture
def wizard_settings(tmp_path: Path, empty_ledger_dir: Path):
    from lamella.core.config import Settings
    return Settings(
        data_dir=tmp_path / "data",
        ledger_dir=empty_ledger_dir,
        paperless_url="https://paperless.test",
        paperless_api_token="token-test",
    )


@pytest.fixture
def wizard_client(wizard_settings, monkeypatch):
    from lamella.main import create_app
    monkeypatch.setattr(
        "lamella.features.receipts.linker.run_bean_check",
        lambda main_bean: None,
    )
    app = create_app(settings=wizard_settings)
    with TestClient(app) as client:
        yield client


# --- Routing entry point ---------------------------------------------------


class TestFreshInstallRouting:
    """The setup gate funnels every redirect through /setup; /setup
    itself decides whether to forward to the wizard or render the
    maintenance checklist. So a fresh install GETs / → /setup →
    /setup/wizard/welcome (two hops). Tests follow the chain rather
    than assert one hop, because the middleware doesn't need to know
    about the wizard — only /setup does."""

    def test_root_lands_on_wizard_via_setup(self, wizard_client):
        # First hop: gate sends / to /setup (redirect for any
        # not-ready ledger).
        r = wizard_client.get("/", follow_redirects=False)
        assert r.status_code in (302, 303), r.text[:200]
        assert r.headers.get("location", "") == "/setup"
        # Second hop: /setup forwards to the wizard for fresh installs.
        r2 = wizard_client.get("/setup", follow_redirects=False)
        assert r2.status_code in (302, 303)
        assert r2.headers.get("location", "") == "/setup/wizard/welcome"

    def test_setup_redirects_to_wizard_on_fresh_install(self, wizard_client):
        r = wizard_client.get("/setup", follow_redirects=False)
        assert r.status_code in (302, 303)
        assert r.headers.get("location", "") == "/setup/wizard/welcome"

    def test_wizard_entry_redirects_to_current_step(self, wizard_client):
        r = wizard_client.get("/setup/wizard", follow_redirects=False)
        assert r.status_code in (302, 303)
        assert r.headers.get("location", "").endswith("/welcome")

    def test_jobs_dock_does_not_redirect_to_wizard(self, wizard_client):
        """Regression: base.html polls /jobs/active/dock every 5s.
        If the gate redirects that to /setup or /setup/wizard, htmx
        follows the redirect and swaps the full page into the dock,
        causing accumulating duplication on /setup, /setup/recovery,
        etc. The gate must let polling endpoints fall through."""
        r = wizard_client.get(
            "/jobs/active/dock", follow_redirects=False,
        )
        # Either the real handler responded (200) or it's not present
        # in this fixture (404). Either way, NOT a redirect to /setup.
        assert r.status_code not in (302, 303), (
            f"AJAX poll got hijacked: status={r.status_code} "
            f"location={r.headers.get('location')!r}"
        )

    def test_htmx_request_is_not_redirected(self, wizard_client):
        """Same protection for any explicit HTMX request: the gate
        must respect HX-Request and not redirect to a full page."""
        r = wizard_client.get(
            "/anything-the-gate-might-redirect",
            headers={"HX-Request": "true"},
            follow_redirects=False,
        )
        # A 404 from FastAPI's "no such route" handler is fine; what
        # we're guarding against is a 303 → /setup that would swap a
        # full page into the htmx target.
        assert r.status_code != 303


class TestWizardSkipsWhenLedgerHasFiles:
    """The wizard is for TRULY zero-state installs only. If the user
    has any beancount files in the ledger directory (even without a
    main.bean), they get the existing /setup checklist with the
    import path discoverable, not a guided onboarding that would
    talk past their data."""

    def test_other_bean_files_route_to_setup_not_wizard(
        self, tmp_path, monkeypatch,
    ):
        from lamella.core.config import Settings
        from lamella.main import create_app
        empty = tmp_path / "ledger"
        empty.mkdir()
        # User dropped a transactions.bean here from a prior tool.
        (empty / "transactions.bean").write_text(
            'option "operating_currency" "USD"\n'
            "2020-01-01 open Assets:Bank USD\n",
            encoding="utf-8",
        )
        monkeypatch.setattr(
            "lamella.features.receipts.linker.run_bean_check",
            lambda main_bean: None,
        )
        settings = Settings(
            data_dir=tmp_path / "data",
            ledger_dir=empty,
            paperless_url="https://paperless.test",
            paperless_api_token="token-test",
        )
        app = create_app(settings=settings)
        with TestClient(app) as c:
            r = c.get("/", follow_redirects=False)
            assert r.status_code in (302, 303)
            # Goes to /setup, NOT /setup/wizard.
            assert r.headers.get("location", "") == "/setup", (
                "other .bean files present should route to /setup, "
                f"got {r.headers.get('location')!r}"
            )

    def test_setup_renders_fresh_start_when_other_bean_files_present(
        self, tmp_path, monkeypatch,
    ):
        from lamella.core.config import Settings
        from lamella.main import create_app
        empty = tmp_path / "ledger"
        empty.mkdir()
        (empty / "transactions.bean").write_text(
            'option "operating_currency" "USD"\n', encoding="utf-8",
        )
        monkeypatch.setattr(
            "lamella.features.receipts.linker.run_bean_check",
            lambda main_bean: None,
        )
        settings = Settings(
            data_dir=tmp_path / "data",
            ledger_dir=empty,
            paperless_url="https://paperless.test",
            paperless_api_token="token-test",
        )
        app = create_app(settings=settings)
        with TestClient(app) as c:
            r = c.get("/setup", follow_redirects=False)
            # Renders setup.html (200) — does NOT bounce to wizard.
            assert r.status_code == 200, (
                "expected /setup to render, not redirect; "
                f"got status={r.status_code} location={r.headers.get('location')!r}"
            )


# --- Step 1: Welcome -------------------------------------------------------


class TestWelcomeStep:
    def test_welcome_renders(self, wizard_client):
        r = wizard_client.get("/setup/wizard/welcome")
        assert r.status_code == 200
        # Wordmark is split so the "ll" can pick up the brand accent
        # — same class every other wordmark in the app uses.
        assert 'Welcome to Lame<span class="brand-wordmark-accent">ll</span>a' in r.text
        # Every intent option visible.
        assert "Just my personal finances" in r.text
        assert "Both personal and business" in r.text
        assert "configure it myself" in r.text

    def test_welcome_offers_import_pivot(self, wizard_client):
        r = wizard_client.get("/setup/wizard/welcome")
        assert r.status_code == 200
        assert "Already have Beancount files" in r.text
        assert 'href="/setup/import"' in r.text

    def test_welcome_does_not_carry_main_app_chrome(self, wizard_client):
        r = wizard_client.get("/setup/wizard/welcome")
        assert r.status_code == 200
        # Standalone layout — none of the chrome that caused the
        # job-dock infinite-redirect duplication should appear.
        for marker in [
            'id="job-dock"',
            'id="paletteInput"',
            'id="toast-area"',
            'id="confirm-modal"',
            'class="sidebar"',
            'topbar-search',
        ]:
            assert marker not in r.text, (
                f"wizard layout leaked main-app chrome ({marker})"
            )

    def test_welcome_requires_name_and_intent(self, wizard_client):
        r = wizard_client.post("/setup/wizard/welcome", data={"name": "", "intent": ""})
        assert r.status_code == 200, "validation failure should re-render"
        assert "Tell us what to call you" in r.text or "Pick one option" in r.text

    def test_welcome_personal_advances_to_entities(self, wizard_client):
        r = wizard_client.post(
            "/setup/wizard/welcome",
            data={"name": "Jane", "intent": "personal"},
            follow_redirects=False,
        )
        assert r.status_code in (302, 303)
        assert r.headers.get("location", "").endswith("/entities")

    def test_welcome_manual_redirects_to_existing_checklist(self, wizard_client):
        r = wizard_client.post(
            "/setup/wizard/welcome",
            data={"name": "Jane", "intent": "manual"},
            follow_redirects=False,
        )
        assert r.status_code in (302, 303)
        assert r.headers.get("location", "") == "/setup", (
            "configure-myself must hand off to the maintenance page"
        )

    def test_state_persists_across_requests(self, wizard_client):
        wizard_client.post(
            "/setup/wizard/welcome",
            data={"name": "Jane", "intent": "personal"},
            follow_redirects=False,
        )
        # Refresh — the welcome page should still know our name + intent.
        r = wizard_client.get("/setup/wizard/welcome")
        assert r.status_code == 200
        # Name comes back as the input default value.
        assert 'value="Jane"' in r.text


# --- Step 2: Entities ------------------------------------------------------


class TestEntitiesStep:
    def test_personal_intent_renders_add_person_only(self, wizard_client):
        wizard_client.post(
            "/setup/wizard/welcome",
            data={"name": "Jane", "intent": "personal"},
        )
        r = wizard_client.get("/setup/wizard/entities")
        assert r.status_code == 200
        # New UX: page shows Add Person button; no business UI.
        assert "Add Person" in r.text
        assert "Add Business" not in r.text

    def test_business_intent_renders_add_business_only(self, wizard_client):
        wizard_client.post(
            "/setup/wizard/welcome",
            data={"name": "Jane", "intent": "business"},
        )
        r = wizard_client.get("/setup/wizard/entities")
        assert r.status_code == 200
        assert "Add Business" in r.text
        assert "Add Person" not in r.text

    def test_everything_intent_renders_both_buttons(self, wizard_client):
        wizard_client.post(
            "/setup/wizard/welcome",
            data={"name": "Jane", "intent": "everything"},
        )
        r = wizard_client.get("/setup/wizard/entities")
        assert "Add Person" in r.text
        assert "Add Business" in r.text

    def test_existing_entity_with_null_metadata_is_individual(self, wizard_client):
        """Regression: an entity seeded from ledger discovery has
        entity_type / tax_schedule NULL. The read path used to default
        such rows to business and slap a `business · read-only` chip
        on them — wrong, especially when the row is the personal
        scaffold. Default-to-individual is the safe call."""
        db = wizard_client.app.state.db
        # Seed a row the way registry.discovery.seed_entities does:
        # slug only, every other column NULL.
        db.execute(
            "INSERT OR REPLACE INTO entities (slug, is_active) VALUES ('Personal', 1)",
        )
        db.commit()
        # Drive the wizard to the entities step.
        wizard_client.post(
            "/setup/wizard/welcome",
            data={"name": "Jane", "intent": "everything"},
        )
        r = wizard_client.get("/setup/wizard/entities")
        assert r.status_code == 200
        # The business section's read-only chip would say
        # "business · read-only" — that string must NOT appear for
        # the bare-NULL row, since it should be in the People list.
        assert "business · read-only" not in r.text
        assert "business &middot; read-only" not in r.text

    def test_save_person_drafts_and_stays_on_step(self, wizard_client):
        """The wizard runs in DRAFT MODE: saving a person from the
        modal records it in state.draft_entities but does NOT write
        to the entities table. The Done step's commit is the only
        place that materializes drafts. The save also redirects back
        to the entities list (not advancing to /bank)."""
        wizard_client.post(
            "/setup/wizard/welcome",
            data={"name": "Jane", "intent": "personal"},
        )
        r = wizard_client.post(
            "/setup/wizard/entities/save-person",
            data={"display_name": "Jane", "slug": ""},
            follow_redirects=False,
        )
        assert r.status_code in (302, 303)
        assert r.headers.get("location", "") == "/setup/wizard/entities"
        # Entity must NOT exist in the entities table yet.
        row = wizard_client.app.state.db.execute(
            "SELECT slug FROM entities WHERE slug = 'Jane'"
        ).fetchone()
        assert row is None, "draft mode violated: save-person wrote to DB"
        # Draft must be in wizard state.
        from lamella.features.setup.wizard_state import load_state
        state = load_state(wizard_client.app.state.db)
        assert any(
            d["slug"] == "Jane" and d["kind"] == "individual"
            for d in state.draft_entities
        ), f"Jane draft missing: {state.draft_entities}"

    def test_continue_blocks_when_no_entities_at_all(self, wizard_client):
        """If user reaches the entities step with no existing entities
        AND no drafts, Continue must show an error rather than advance."""
        wizard_client.post(
            "/setup/wizard/welcome",
            data={"name": "Jane", "intent": "personal"},
        )
        r = wizard_client.post(
            "/setup/wizard/entities", data={}, follow_redirects=False,
        )
        assert r.status_code == 200, "expected error re-render, not redirect"
        assert "at least one" in r.text.lower()

    def test_continue_advances_with_at_least_one_draft(self, wizard_client):
        wizard_client.post(
            "/setup/wizard/welcome",
            data={"name": "Jane", "intent": "personal"},
        )
        wizard_client.post(
            "/setup/wizard/entities/save-person",
            data={"display_name": "Jane", "slug": "Jane"},
        )
        r = wizard_client.post(
            "/setup/wizard/entities", data={}, follow_redirects=False,
        )
        assert r.status_code in (302, 303)
        assert r.headers.get("location", "").endswith("/bank")

    def test_remove_drops_only_drafts(self, wizard_client):
        wizard_client.post(
            "/setup/wizard/welcome",
            data={"name": "Jane", "intent": "personal"},
        )
        wizard_client.post(
            "/setup/wizard/entities/save-person",
            data={"display_name": "Jane", "slug": "Jane"},
        )
        wizard_client.post(
            "/setup/wizard/entities/remove",
            data={"slug": "Jane"},
        )
        from lamella.features.setup.wizard_state import load_state
        state = load_state(wizard_client.app.state.db)
        assert all(d["slug"] != "Jane" for d in state.draft_entities)


# --- Step 3: Bank ----------------------------------------------------------


class TestBankStep:
    def test_skip_advances_to_accounts(self, wizard_client):
        wizard_client.post(
            "/setup/wizard/welcome",
            data={"name": "Jane", "intent": "personal"},
        )
        wizard_client.post(
            "/setup/wizard/entities",
            data={"indiv_display_name": "Jane"},
        )
        r = wizard_client.post(
            "/setup/wizard/bank/skip", follow_redirects=False,
        )
        assert r.status_code in (302, 303)
        assert r.headers.get("location", "").endswith("/accounts")


# --- Step 4: Accounts ------------------------------------------------------


class TestAccountsStep:
    def _setup_personal(self, client):
        client.post(
            "/setup/wizard/welcome",
            data={"name": "Jane", "intent": "personal"},
        )
        client.post(
            "/setup/wizard/entities/save-person",
            data={"display_name": "Jane", "slug": "Jane"},
        )
        client.post("/setup/wizard/bank/skip")

    def test_renders_account_list_view(self, wizard_client):
        self._setup_personal(wizard_client)
        r = wizard_client.get("/setup/wizard/accounts")
        assert r.status_code == 200
        # New UX: list view + Add Account button (not a pre-rendered form).
        assert "Add Account" in r.text

    def test_no_accounts_blocks_continue(self, wizard_client):
        self._setup_personal(wizard_client)
        r = wizard_client.post(
            "/setup/wizard/accounts", data={}, follow_redirects=False,
        )
        # 200 with error banner, not a redirect onward.
        assert r.status_code == 200
        assert "at least one account" in r.text.lower()

    def test_save_account_drafts_and_stays_on_step(self, wizard_client):
        """Draft mode: saving an account from the modal records the
        values in state.draft_accounts but does NOT write to
        accounts_meta. Redirects back to the accounts list (not
        advancing to /property-vehicle)."""
        self._setup_personal(wizard_client)
        r = wizard_client.post(
            "/setup/wizard/accounts/save",
            data={
                "display_name": "Wells Checking",
                "institution": "Bank One",
                "last_four": "1234",
                "entity_slug": "Jane",
                "kind": "checking",
                "loan_kind": "",
                "opening_balance": "",
                "simplefin_account_id": "",
            },
            follow_redirects=False,
        )
        assert r.status_code in (302, 303)
        assert r.headers.get("location", "") == "/setup/wizard/accounts"
        # accounts_meta MUST NOT have a row yet.
        row = wizard_client.app.state.db.execute(
            "SELECT account_path FROM accounts_meta WHERE entity_slug = 'Jane'"
        ).fetchone()
        assert row is None, "draft mode violated: save wrote to accounts_meta"
        # Draft recorded.
        from lamella.features.setup.wizard_state import load_state
        state = load_state(wizard_client.app.state.db)
        assert state.draft_accounts, "no draft account recorded"
        d = state.draft_accounts[0]
        assert d["kind"] == "checking"
        assert d["entity_slug"] == "Jane"
        assert d["account_path"].startswith("Assets:Jane:")

    def test_continue_advances_with_complete_draft(self, wizard_client):
        self._setup_personal(wizard_client)
        wizard_client.post(
            "/setup/wizard/accounts/save",
            data={
                "display_name": "Wells Checking",
                "entity_slug": "Jane",
                "kind": "checking",
                "loan_kind": "",
                "institution": "",
                "last_four": "",
                "opening_balance": "",
                "simplefin_account_id": "",
            },
        )
        r = wizard_client.post(
            "/setup/wizard/accounts", data={}, follow_redirects=False,
        )
        assert r.status_code in (302, 303)
        assert r.headers.get("location", "").endswith("/property-vehicle")

    def test_continue_blocks_when_draft_missing_entity(self, wizard_client):
        """Manually inject an incomplete draft (mimics the SimpleFIN
        seed on a multi-entity install) and verify Continue blocks."""
        self._setup_personal(wizard_client)
        from lamella.features.setup.wizard_state import (
            load_state, save_state,
        )
        db = wizard_client.app.state.db
        state = load_state(db)
        state.draft_accounts.append({
            "account_path": "",
            "kind": "checking",
            "entity_slug": "",  # missing — should block
            "institution": "Acme Bank",
            "last_four": "",
            "display_name": "Acme Checking",
            "simplefin_account_id": "",
            "loan_kind": "",
            "opening_balance": "",
            "from_simplefin": "1",
        })
        save_state(db, state)
        db.commit()
        r = wizard_client.post(
            "/setup/wizard/accounts", data={}, follow_redirects=False,
        )
        assert r.status_code == 200, "expected error re-render"
        assert "still need" in r.text.lower() or "still needs" in r.text.lower()

    def test_continue_blocks_when_loan_missing_loan_kind(self, wizard_client):
        """A loan account without a loan_kind (Mortgage / Auto / etc.)
        is still incomplete — the property/vehicle smart-link logic
        depends on loan_kind, and the Done commit can't classify a
        loan without it."""
        self._setup_personal(wizard_client)
        from lamella.features.setup.wizard_state import (
            load_state, save_state,
        )
        db = wizard_client.app.state.db
        state = load_state(db)
        state.draft_accounts.append({
            "account_path": "",
            "kind": "loan",
            "entity_slug": "Jane",
            "institution": "Acme Bank",
            "last_four": "",
            "display_name": "Some Loan",
            "simplefin_account_id": "",
            "loan_kind": "",  # missing — should block since kind=loan
            "opening_balance": "",
            "from_simplefin": "",
        })
        save_state(db, state)
        db.commit()
        r = wizard_client.post(
            "/setup/wizard/accounts", data={}, follow_redirects=False,
        )
        assert r.status_code == 200
        assert "loan" in r.text.lower()

    def test_loan_kind_recorded_for_step_5(self, wizard_client):
        """Even in draft mode, the loan-kind metadata is recorded
        eagerly so step 5's smart prompts know what to suggest."""
        self._setup_personal(wizard_client)
        wizard_client.post(
            "/setup/wizard/accounts/save",
            data={
                "display_name": "Mortgage",
                "institution": "Lender",
                "last_four": "",
                "entity_slug": "Jane",
                "kind": "loan",
                "loan_kind": "Mortgage",
                "opening_balance": "",
                "simplefin_account_id": "",
            },
        )
        from lamella.features.setup.wizard_state import load_state
        state = load_state(wizard_client.app.state.db)
        assert state.created_loan_paths, "loan path not recorded"
        assert "Mortgage" in state.loan_kinds.values()
        # And it's a draft, not yet committed to accounts_meta.
        row = wizard_client.app.state.db.execute(
            "SELECT 1 FROM accounts_meta WHERE entity_slug = 'Jane'"
        ).fetchone()
        assert row is None, "draft mode violated"


# --- Step 5 + finalize -----------------------------------------------------


class TestPropertyVehicleAndDone:
    def _walk_to_propvehicle(self, client):
        client.post(
            "/setup/wizard/welcome",
            data={"name": "Jane", "intent": "personal"},
        )
        client.post(
            "/setup/wizard/entities/save-person",
            data={"display_name": "Jane", "slug": "Jane"},
        )
        client.post("/setup/wizard/bank/skip")
        # Add a mortgage draft via the modal save endpoint.
        client.post(
            "/setup/wizard/accounts/save",
            data={
                "display_name": "Mortgage",
                "institution": "Lender",
                "last_four": "",
                "entity_slug": "Jane",
                "kind": "loan",
                "loan_kind": "Mortgage",
                "opening_balance": "",
                "simplefin_account_id": "",
            },
        )
        # Continue to the property/vehicle step.
        client.post("/setup/wizard/accounts")

    def test_property_modal_lists_mortgage_loan(self, wizard_client):
        """The new property modal's "Linked mortgage" dropdown should
        include the mortgage loan account we created in step 4."""
        self._walk_to_propvehicle(wizard_client)
        r = wizard_client.get("/setup/wizard/property-vehicle?add=property")
        assert r.status_code == 200
        # The mortgage loan path must appear inside the modal as a
        # linked-loan option.
        assert "Liabilities:Jane:Lender:Mortgage" in r.text
        # And the property modal itself must be open.
        assert "Add a property" in r.text

    def test_skip_advances_to_done(self, wizard_client):
        self._walk_to_propvehicle(wizard_client)
        r = wizard_client.post(
            "/setup/wizard/property-vehicle/continue",
            follow_redirects=False,
        )
        assert r.status_code in (302, 303)
        assert r.headers.get("location", "").endswith("/done")

    def test_finalize_marks_complete_and_redirects_to_finalizing(self, wizard_client):
        """Done POST runs the commits + flips wizard-complete + sends
        the user to /setup/wizard/finalizing for the celebratory
        animation. The animation page itself JS-redirects to /."""
        self._walk_to_propvehicle(wizard_client)
        wizard_client.post("/setup/wizard/property-vehicle/continue")
        r = wizard_client.post("/setup/wizard/done", follow_redirects=False)
        assert r.status_code in (302, 303)
        # The redirect now carries a ?job=… query so the finalizing
        # page can subscribe to the SSE stream for live progress.
        location = r.headers.get("location", "")
        assert location.startswith("/setup/wizard/finalizing"), location
        assert "?job=" in location, location
        # Wizard locked immediately after POST returns — even though
        # the job is still running in the background, the user must
        # not be able to re-enter the wizard from /welcome.
        from lamella.features.setup.wizard_state import is_wizard_complete
        assert is_wizard_complete(wizard_client.app.state.db) is True
        # And the finalizing page is reachable.
        r2 = wizard_client.get("/setup/wizard/finalizing")
        assert r2.status_code == 200
        assert "Setting things up" in r2.text or "Setting up" in r2.text

    def test_completed_wizard_blocks_re_entry(self, wizard_client):
        self._walk_to_propvehicle(wizard_client)
        wizard_client.post("/setup/wizard/property-vehicle/continue")
        wizard_client.post("/setup/wizard/done")
        r = wizard_client.get("/setup/wizard/welcome", follow_redirects=False)
        # Wizard re-entry → bounce home.
        assert r.status_code in (302, 303)
        assert r.headers.get("location", "") == "/"


# --- State module unit tests ----------------------------------------------


class TestSimpleFinNameParsing:
    """The bridge often appends "(####)" to account names — sometimes
    with a duplicate of the same digits inline too. The parser pulls
    them apart so the wizard's draft has a clean name + a populated
    last_four."""

    def test_trailing_parens_only(self):
        from lamella.web.routes.setup_wizard import _parse_simplefin_name
        assert _parse_simplefin_name("Bank One Checking (1234)") == (
            "Bank One Checking", "1234",
        )

    def test_inline_duplicate_is_stripped(self):
        from lamella.web.routes.setup_wizard import _parse_simplefin_name
        # The bridge sometimes inlines the same digits before the
        # parens — we want a single clean name with no leftover dash.
        assert _parse_simplefin_name(
            "Anywhere Card by EntityA-4959 (4959)",
        ) == ("Anywhere Card by EntityA", "4959")

    def test_no_parens_returns_unchanged(self):
        from lamella.web.routes.setup_wizard import _parse_simplefin_name
        # No (####) → leave the name alone, last_four empty.
        assert _parse_simplefin_name("Some Account 5678") == (
            "Some Account 5678", "",
        )

    def test_empty(self):
        from lamella.web.routes.setup_wizard import _parse_simplefin_name
        assert _parse_simplefin_name("") == ("", "")
        assert _parse_simplefin_name("   ") == ("", "")

    def test_format_account_display_appends_when_missing(self):
        from lamella.web.routes.setup_wizard import _format_account_display
        assert _format_account_display("Checking", "1234") == "Checking (1234)"

    def test_format_account_display_no_double_append(self):
        from lamella.web.routes.setup_wizard import _format_account_display
        # Idempotent: if (####) is already there, don't add it again.
        assert _format_account_display("Checking (1234)", "1234") == "Checking (1234)"

    def test_format_account_display_no_lastfour(self):
        from lamella.web.routes.setup_wizard import _format_account_display
        assert _format_account_display("Checking", "") == "Checking"

    def test_strips_trademark_glyphs(self):
        from lamella.web.routes.setup_wizard import _parse_simplefin_name
        # The ® should disappear; the inline 4959 duplicate should
        # be stripped along with the trailing dash.
        name, lf = _parse_simplefin_name(
            "Costco Anywhere Visa® Card by EntityA-4959 (4959)",
        )
        assert lf == "4959"
        assert "®" not in name
        assert name == "Costco Anywhere Visa Card by EntityA"

    def test_strips_replacement_chars(self):
        from lamella.web.routes.setup_wizard import _parse_simplefin_name
        # The "VISA SIGNATURE�� CARD ...5500 (5500)" case —
        # the replacement chars (and the leading "..." prefix on the
        # inline dup) both go away.
        name, lf = _parse_simplefin_name(
            "VISA SIGNATURE�� CARD ...5500 (5500)",
        )
        assert lf == "5500"
        assert "�" not in name
        assert name == "VISA SIGNATURE CARD"

    def test_strips_zero_width_chars(self):
        from lamella.web.routes.setup_wizard import _parse_simplefin_name
        # Zero-width space embedded mid-name should disappear.
        name, lf = _parse_simplefin_name("Some​Account (1234)")
        assert lf == "1234"
        assert name == "SomeAccount"


class TestWizardStateModule:
    def test_round_trip(self, tmp_path):
        import sqlite3
        from lamella.features.setup.wizard_state import (
            WizardState, load_state, save_state,
        )
        db = sqlite3.connect(":memory:")
        db.row_factory = sqlite3.Row
        db.execute("""
            CREATE TABLE setup_wizard_state (
                slug TEXT PRIMARY KEY, payload_json TEXT NOT NULL DEFAULT '{}',
                started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP)
        """)

        s = WizardState(name="Jane", intent="both")
        s.individuals_planned = ["Jane"]
        s.businesses_planned = ["Acme"]
        s.loan_kinds = {"Liabilities:Jane:Bank:Loan": "Mortgage"}
        save_state(db, s)
        db.commit()

        out = load_state(db)
        assert out.name == "Jane"
        assert out.intent == "both"
        assert out.individuals_planned == ["Jane"]
        assert out.businesses_planned == ["Acme"]
        assert out.loan_kinds == {"Liabilities:Jane:Bank:Loan": "Mortgage"}

    def test_default_when_table_missing(self, tmp_path):
        import sqlite3
        from lamella.features.setup.wizard_state import load_state
        db = sqlite3.connect(":memory:")
        db.row_factory = sqlite3.Row
        # No table — should not raise.
        s = load_state(db)
        assert s.step == "welcome"
        assert s.name == ""

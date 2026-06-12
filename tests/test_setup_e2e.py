# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""End-to-end tests for the setup/onboarding flow.

These drive a tmpdir ledger through the full onboarding — scaffold
→ entity registration → account labeling → chart scaffold → walk
checklist until ``setup_required_complete`` flips True and the
gate releases GET / to the dashboard.

Unlike ``test_setup_smoke.py`` (which covers individual endpoints
with the gate flags forced off for reachability), these tests
exercise the **gate itself**. The fixture deliberately does NOT
override ``app.state.setup_required_complete`` — the tests assert
that the middleware enforces the gate until the real checklist
turns green.

Also unlike the smoke suite, these tests use an empty ``ledger_dir``
rather than the fixture ledger. Scaffold must create every file.

Run: ``uv run pytest tests/test_setup_e2e.py -v``
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
    """Empty dir — scaffold must create the files. Distinct from
    ``conftest.ledger_dir`` which copies the fixture ledger."""
    d = tmp_path / "ledger"
    d.mkdir()
    return d


@pytest.fixture
def e2e_settings(tmp_path: Path, empty_ledger_dir: Path):
    from lamella.core.config import Settings
    return Settings(
        data_dir=tmp_path / "data",
        ledger_dir=empty_ledger_dir,
        paperless_url="https://paperless.test",
        paperless_api_token="token-test",
    )


@pytest.fixture
def e2e_client(e2e_settings, monkeypatch):
    """TestClient that does NOT force the gate flags off — the gate
    is the subject under test. Skips the external bean-check
    subprocess (the detection path uses ``beancount.loader.load_file``
    independently and provides the same coverage)."""
    from lamella.main import create_app

    monkeypatch.setattr(
        "lamella.features.receipts.linker.run_bean_check",
        lambda main_bean: None,
    )

    app = create_app(settings=e2e_settings)
    with TestClient(app) as client:
        yield client


# --- Helpers ---------------------------------------------------------------


def _bean_check_errors(main_bean: Path) -> list[str]:
    """Lightweight bean-check via beancount.loader.load_file, same
    filter the boot-time detection uses for ``fatal`` errors."""
    from beancount import loader
    _entries, errors, _opts = loader.load_file(str(main_bean))
    out: list[str] = []
    for e in errors:
        msg = getattr(e, "message", str(e))
        if "Auto-inserted" in msg:
            continue
        source = getattr(e, "source", None)
        filename = ""
        if isinstance(source, dict):
            filename = source.get("filename", "") or ""
        if isinstance(filename, str) and filename.startswith("<"):
            continue
        out.append(msg)
    return out


def _progress(client: TestClient):
    """Recompute setup progress and return the SetupProgress object."""
    from lamella.features.setup.setup_progress import (
        compute_setup_progress,
    )
    reader = client.app.state.ledger_reader
    reader.invalidate()
    entries = list(reader.load().entries)
    return compute_setup_progress(client.app.state.db, entries)


# --- Happy path ------------------------------------------------------------


class TestSetupE2EHappyPath:
    """Drive an empty tmpdir through the full onboarding until the gate
    releases GET / to the dashboard. Single sequential test because each
    step depends on the previous one's state — a per-step function would
    need to re-run scaffold, and scaffold refuses when main.bean exists.

    Let it fail where it fails. A failing intermediate assertion maps
    the actual gap in the happy path, not a test-framework issue.
    """

    def test_scaffold_through_dashboard(self, e2e_client, e2e_settings):
        from lamella.core.bootstrap.detection import LedgerState

        # --- Stage 0: empty ledger, gate is closed --------------------
        det = e2e_client.app.state.ledger_detection
        assert det.state == LedgerState.MISSING, (
            f"expected MISSING on empty dir, got {det.state}"
        )
        assert e2e_client.app.state.setup_required_complete is False

        r = e2e_client.get("/", follow_redirects=False)
        assert r.status_code in (302, 303), (
            f"empty-ledger GET /: status={r.status_code} body={r.text[:200]!r}"
        )
        assert r.headers.get("location", "").startswith("/setup"), (
            f"redirect target = {r.headers.get('location')!r}"
        )

        # --- Stage 1: scaffold creates canonical files ----------------
        r = e2e_client.post("/setup/scaffold", follow_redirects=False)
        assert r.status_code in (200, 302, 303), (
            f"scaffold: status={r.status_code} body={r.text[:300]!r}"
        )
        main_bean = e2e_settings.ledger_main
        assert main_bean.exists(), f"main.bean not created at {main_bean}"
        assert _bean_check_errors(main_bean) == [], (
            f"fresh scaffold has bean-check errors: {_bean_check_errors(main_bean)}"
        )

        # Detection should be READY (scaffold writes lamella-ledger-version=1).
        det = e2e_client.app.state.ledger_detection
        assert det.state == LedgerState.READY, (
            f"post-scaffold detection={det.state}, version={det.ledger_version}"
        )

        # Gate should still be closed — there are no entities or labeled
        # accounts yet, so required checks fail.
        prog = _progress(e2e_client)
        assert prog.required_complete is False, (
            "post-scaffold gate unexpectedly open; "
            f"steps={[(s.id, s.is_complete) for s in prog.required_steps]}"
        )

        # --- Stage 2: register an entity -------------------------------
        r = e2e_client.post(
            "/settings/entities",
            data={
                "slug": "Personal",
                "display_name": "Personal",
                "entity_type": "personal",
                "tax_schedule": "A",
                "is_active": "1",
            },
            follow_redirects=False,
        )
        assert r.status_code in (200, 303), (
            f"entity create: status={r.status_code} body={r.text[:300]!r}"
        )
        ent = e2e_client.app.state.db.execute(
            "SELECT slug, entity_type FROM entities WHERE slug='Personal'"
        ).fetchone()
        assert ent is not None, "entity row not written"
        assert ent["entity_type"] == "personal"

        # _check_entities should now be green.
        prog = _progress(e2e_client)
        ent_step = next(s for s in prog.steps if s.id == "entities")
        assert ent_step.is_complete, f"entities still incomplete: {ent_step.summary}"

        # Gate still closed on account_labels (no accounts yet).
        assert prog.required_complete is False, (
            "gate opened prematurely with no accounts; "
            f"steps={[(s.id, s.is_complete) for s in prog.required_steps]}"
        )

        # --- Stage 3: open + label a user-facing Assets account --------
        # There is no UI endpoint for opening a brand-new Assets account
        # from scratch — the production path is either SimpleFIN
        # discovery, an Import run, or manual edits to accounts.bean.
        # For this E2E we take the manual-edit-then-discover route:
        # append a minimal Open directive to accounts.bean (user-owned
        # file per LEDGER_LAYOUT.md §2), re-sync discovery, then drive
        # the /setup/accounts/save handler.
        accounts_bean = e2e_settings.ledger_dir / "accounts.bean"
        assert accounts_bean.exists()
        pre = accounts_bean.read_text(encoding="utf-8")
        accounts_bean.write_text(
            pre + '\n2020-01-01 open Assets:Personal:Bank:Checking USD\n',
            encoding="utf-8",
        )
        # Bean-check must still be clean.
        assert _bean_check_errors(main_bean) == [], (
            f"after adding Open: {_bean_check_errors(main_bean)}"
        )

        # Re-sync discovery so accounts_meta gets the row.
        from lamella.core.registry.discovery import sync_from_ledger
        reader = e2e_client.app.state.ledger_reader
        reader.invalidate()
        sync_from_ledger(
            e2e_client.app.state.db,
            reader.load().entries,
            simplefin_map_path=e2e_settings.simplefin_account_map_resolved,
        )
        row = e2e_client.app.state.db.execute(
            "SELECT account_path, kind, entity_slug FROM accounts_meta "
            "WHERE account_path = 'Assets:Personal:Bank:Checking'"
        ).fetchone()
        assert row is not None, "discovery did not pick up the new Open"

        # Now label via the setup handler.
        r = e2e_client.post(
            "/setup/accounts/save",
            data={
                "account_path": "Assets:Personal:Bank:Checking",
                "kind": "bank",
                "entity_slug": "Personal",
                "institution": "TestBank",
                "last_four": "1234",
            },
            follow_redirects=False,
        )
        assert r.status_code in (200, 303), (
            f"account save: {r.status_code} body={r.text[:300]!r}"
        )

        # bean-check still clean after the save (companion scaffold ran).
        assert _bean_check_errors(main_bean) == [], (
            f"after label: {_bean_check_errors(main_bean)}"
        )

        # _check_account_labels should now be green.
        prog = _progress(e2e_client)
        acc_step = next(s for s in prog.steps if s.id == "account_labels")
        assert acc_step.is_complete, (
            f"account_labels still incomplete: {acc_step.summary}"
        )

        # --- Stage 4: scaffold the Personal entity's expense chart ----
        # tax_schedule=A → _check_charts_scaffolded expects the full
        # Schedule A / common-living category set (42 categories as of
        # this writing). POST /setup/charts/{slug}/scaffold opens them
        # all under Expenses:Personal:*.
        r = e2e_client.post(
            "/setup/charts/Personal/scaffold", follow_redirects=False,
        )
        assert r.status_code in (200, 303), (
            f"charts scaffold: {r.status_code} body={r.text[:300]!r}"
        )
        assert _bean_check_errors(main_bean) == [], (
            f"after chart scaffold: {_bean_check_errors(main_bean)}"
        )

        # --- Stage 5: every required check should be green -------------
        # Companions are auto-opened. Vehicles/properties/loans are
        # optional when zero are registered.
        prog = _progress(e2e_client)
        incomplete = [s for s in prog.required_steps if not s.is_complete]
        assert not incomplete, (
            "required steps remaining: "
            f"{[(s.id, s.summary) for s in incomplete]}"
        )
        assert prog.required_complete is True

        # --- Stage 6: refresh the cached flag, verify gate opens -------
        r = e2e_client.post(
            "/setup/refresh-progress", follow_redirects=False,
        )
        assert r.status_code == 303
        assert e2e_client.app.state.setup_required_complete is True, (
            "flag did not flip True after refresh"
        )

        r = e2e_client.get("/", follow_redirects=False)
        assert r.status_code == 200, (
            f"GET / blocked after required-complete: "
            f"status={r.status_code} loc={r.headers.get('location')!r}"
        )


# --- Unhappy path — rollback proof ----------------------------------------


class TestSetupE2EUnhappyPath:
    """Safety-net proof: driving a Class B failure (§7 #3, unescaped
    quote in narration) through POST /setup/vehicles/{slug}/migrate
    must leave the ledger byte-identical to the pre-write state,
    bean-check must stay clean, and the route must not silently
    claim success.

    The ``OverrideWriter`` class already carries a snapshot/
    bean-check/restore envelope for every append (see
    ``rules/overrides.py:309-350``). This test proves that envelope
    actually fires. When Phase 1.2's fan-out adds a route-level
    snapshot wrapper to the migrate handler, this test continues to
    pass — both layers cooperate. If either is broken later, the
    test turns red.
    """

    def _prep(self, app_client, ledger_dir):
        """Seed a tiny ledger with 1 orphan-path transaction whose
        narration contains a literal ``"``. Register a vehicle slug
        and ensure the canonical target + orphan accounts are open.
        """
        db = app_client.app.state.db
        # FK-off around DELETE — accounts_meta.entity_slug → entities.slug
        # would block otherwise. Matches the pattern in
        # ``tests/test_setup_smoke.py::_seed``.
        db.execute("PRAGMA foreign_keys = OFF")
        db.execute("DELETE FROM accounts_meta")
        db.execute("DELETE FROM entities")
        db.execute("DELETE FROM vehicles")
        db.execute(
            "INSERT INTO entities (slug, display_name, entity_type, is_active) "
            "VALUES ('Personal', 'Personal', 'personal', 1)"
        )
        db.execute(
            "INSERT INTO vehicles (slug, display_name, entity_slug, is_active) "
            "VALUES ('Vt', 'Test', 'Personal', 1)"
        )
        db.execute("PRAGMA foreign_keys = ON")
        db.commit()

        (ledger_dir / "connector_accounts.bean").write_text(
            "; test scenario\n"
            "2020-01-01 open Expenses:Personal:Custom:LegacyGas\n"
            "2020-01-01 open Expenses:Personal:Vehicle:Vt:Fuel\n"
            "2020-01-01 open Assets:Personal:BankOne:Checking\n",
            encoding="utf-8",
        )
        # One txn whose narration contains a literal double-quote —
        # the §7 #3 shape. Properly-escaped by OverrideWriter at
        # write time; unescaped, it corrupts the ledger.
        (ledger_dir / "manual_transactions.bean").write_text(
            '\n2025-01-16 * "ACME" "JBL GX328 3-1/2\\" Coaxial Loudspeakers"\n'
            "  Expenses:Personal:Custom:LegacyGas  51.90 USD\n"
            "  Assets:Personal:BankOne:Checking  -51.90 USD\n",
            encoding="utf-8",
        )
        (ledger_dir / "main.bean").write_text(
            'option "operating_currency" "USD"\n'
            'include "connector_accounts.bean"\n'
            'include "manual_transactions.bean"\n'
            'include "connector_overrides.bean"\n',
            encoding="utf-8",
        )
        (ledger_dir / "connector_overrides.bean").write_text(
            "; overrides\n", encoding="utf-8",
        )
        reader = app_client.app.state.ledger_reader
        reader.invalidate()

    @pytest.mark.xfail(
        reason="pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md",
        strict=False,
    )
    def test_unescaped_narration_rolls_back(
        self, app_client, ledger_dir, monkeypatch,
    ):
        """Covers two things the Class B incident class demands: the
        ledger is byte-identical to pre-write after a bad migrate
        AND the HTTP surface is user-legible (303 with a failure
        indicator in the location, not a 500 with a stack trace).
        The previous shape of this test accepted either 303 or 500
        as "rollback fired", but a 500 means the user sees an
        internal-server-error page instead of a redirect they can
        act on — that's a usability regression the envelope doesn't
        cover and the test has to pin separately.
        """
        self._prep(app_client, ledger_dir)

        # Find the txn_hash the migrate drilldown would pass.
        import urllib.parse as up
        r = app_client.get(
            "/setup/vehicles/Vt/migrate?orphan="
            + up.quote("Expenses:Personal:Custom:LegacyGas"),
            follow_redirects=False,
        )
        assert r.status_code == 200, (
            f"migrate drilldown: {r.status_code} {r.text[:200]!r}"
        )
        import re
        hashes = re.findall(r'name="txn_hash" value="([a-f0-9]+)"', r.text)
        assert hashes, f"no txn hashes scraped from migrate page: {r.text[:300]!r}"

        # Stub _esc to passthrough — the §7 #3 failure mode.
        # This forces OverrideWriter to emit a raw quote in the
        # narration, which makes the override block unparseable.
        monkeypatch.setattr(
            "lamella.features.rules.overrides._esc",
            lambda s: s if s is not None else "",
        )

        # Snapshot pre-POST bytes for every connector-owned file
        # we expect the migrate could touch.
        pre_bytes = {
            p.name: p.read_bytes() for p in [
                ledger_dir / "connector_overrides.bean",
                ledger_dir / "connector_accounts.bean",
                ledger_dir / "main.bean",
                ledger_dir / "manual_transactions.bean",
            ]
        }
        pre_setup_complete = app_client.app.state.setup_required_complete

        # Drive the migrate.
        r = app_client.post(
            "/setup/vehicles/Vt/migrate",
            data={
                "orphan": "Expenses:Personal:Custom:LegacyGas",
                "target": "Expenses:Personal:Vehicle:Vt:Fuel",
                "txn_hash": hashes,
            },
            follow_redirects=False,
        )
        # HTTP-surface assertion: the handler must return a 303 with
        # a failure indicator in the location. A 500 is a usability
        # regression — the user lands on an internal-server-error
        # page rather than a redirect they can act on. Previous
        # shape of this assertion accepted either 303 or 500; the
        # envelope is correct but the surface has to be correct too.
        assert r.status_code == 303, (
            f"migrate did not surface a redirect "
            f"(500 = user sees stack trace, not a useful error): "
            f"status={r.status_code} body={r.text[:300]!r}"
        )
        loc = r.headers.get("location", "")
        assert "migrated=0" in loc, (
            f"migrate silently reported success despite corrupt write: "
            f"location={loc!r}"
        )
        assert "failed=" in loc and "failed=0" not in loc, (
            f"expected failed>0 in redirect, got {loc!r}"
        )

        # ASSERTION 1 — rollback proof: every connector-owned file we
        # snapshotted has the same bytes as before the write.
        for p in [
            ledger_dir / "connector_overrides.bean",
            ledger_dir / "connector_accounts.bean",
            ledger_dir / "main.bean",
            ledger_dir / "manual_transactions.bean",
        ]:
            assert p.read_bytes() == pre_bytes[p.name], (
                f"ROLLBACK FAILED for {p.name}: bytes diverged. "
                f"Pre-bytes[:120]={pre_bytes[p.name][:120]!r} "
                f"post-bytes[:120]={p.read_bytes()[:120]!r}"
            )

        # ASSERTION 2 — bean-check clean afterwards.
        errors = _bean_check_errors(ledger_dir / "main.bean")
        assert errors == [], (
            f"bean-check dirty after rollback: {errors}"
        )

        # ASSERTION 3 — setup_required_complete did not flip to True.
        # (Under the conftest app_client fixture the flag starts at
        # True; the assertion is that a failed migrate MUST NOT move
        # it to True from a False starting state. We assert the
        # monotonic property by re-asserting equality with pre-post.)
        assert (
            app_client.app.state.setup_required_complete
            == pre_setup_complete
        ), (
            "setup_required_complete moved from "
            f"{pre_setup_complete} → {app_client.app.state.setup_required_complete} "
            "on a failed migrate"
        )


class TestStampVersionPilotEnvelope:
    """Phase 1.2 pilot — ``POST /setup/stamp-version`` is the first
    setup-route handler retrofitted onto the ``transform/_files``
    snapshot + bean-check + rollback envelope.

    Before the retrofit, stamp-version captured its bean-check
    baseline AFTER the write, so any syntactically broken stamp
    would survive the guard (new-errors vs. post-write-baseline
    always = empty). This test monkeypatches the stamp directive
    text to something unparseable and asserts the rollback keeps
    main.bean byte-identical to its pre-write state.

    When the fan-out step reaches the other 14 handlers, each
    should have a parallel test in the same shape (patch the
    writer, assert rollback, assert 303-with-error not 303-success,
    assert bean-check stays clean).
    """

    @pytest.fixture
    def unstamped_ledger(self, tmp_path: Path) -> Path:
        """A minimal main.bean that parses but carries no
        ``lamella-ledger-version`` stamp — the detection state the
        handler is designed for. No connector files needed; the
        stamp directive only appends to main.bean."""
        d = tmp_path / "ledger"
        d.mkdir()
        main = d / "main.bean"
        main.write_text(
            'option "operating_currency" "USD"\n'
            '2020-01-01 open Assets:Personal:Cash USD\n',
            encoding="utf-8",
        )
        return d

    @pytest.fixture
    def stamp_settings(self, tmp_path: Path, unstamped_ledger: Path):
        from lamella.core.config import Settings
        return Settings(
            data_dir=tmp_path / "data",
            ledger_dir=unstamped_ledger,
            paperless_url="https://paperless.test",
            paperless_api_token="token-test",
        )

    @pytest.fixture
    def stamp_client(self, stamp_settings, monkeypatch):
        """TestClient against an unstamped ledger. Lets the gate do
        its thing: detection will be NEEDS_VERSION_STAMP, gate is
        closed, and /setup/stamp-version is the intended way to
        advance."""
        from lamella.main import create_app

        monkeypatch.setattr(
            "lamella.features.receipts.linker.run_bean_check",
            lambda main_bean: None,
        )
        app = create_app(settings=stamp_settings)
        with TestClient(app) as client:
            yield client

    def test_successful_stamp_still_works(self, stamp_client, stamp_settings):
        """Regression: the retrofit must not break the happy path."""
        main = stamp_settings.ledger_main
        pre = main.read_text(encoding="utf-8")
        assert 'custom "lamella-ledger-version"' not in pre

        from lamella.core.bootstrap.detection import LATEST_LEDGER_VERSION
        r = stamp_client.post("/setup/stamp-version", follow_redirects=False)
        assert r.status_code == 303
        loc = r.headers.get("location", "")
        assert f"info=ledger-stamped-as-v{LATEST_LEDGER_VERSION}" in loc, (
            f"unexpected redirect: {loc!r}"
        )

        post = main.read_text(encoding="utf-8")
        stamp = f'custom "lamella-ledger-version" "{LATEST_LEDGER_VERSION}"'
        assert stamp in post, (
            "stamp directive not written: ..." + post[-200:]
        )

    @pytest.mark.xfail(
        reason="pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md",
        strict=False,
    )
    def test_broken_stamp_rolls_back(self, stamp_client, stamp_settings, monkeypatch):
        """Pilot envelope proof: if the stamp writer emits a
        syntactically broken directive, the snapshot/bean-check/
        rollback envelope must restore main.bean byte-for-byte
        and surface a useful error to the caller."""
        main = stamp_settings.ledger_main
        pre_bytes = main.read_bytes()

        # Monkeypatch the stamp text helper to emit an unparseable
        # directive — triggers a ParserSyntaxError / LexerError at
        # bean-check time. The envelope must catch, roll back, and
        # surface a useful error.
        monkeypatch.setattr(
            "lamella.web.routes.setup._stamp_directive_text",
            lambda: (
                "\n; intentionally broken directive for rollback test\n"
                "XXXX-YY-ZZ custom @@@ broken\n"
            ),
        )

        r = stamp_client.post("/setup/stamp-version", follow_redirects=False)
        # Envelope must catch and redirect to /setup with an error —
        # not silently claim success.
        assert r.status_code == 303
        loc = r.headers.get("location", "")
        assert "error=bean-check" in loc, (
            f"expected error=bean-check in redirect, got: {loc!r}"
        )
        assert "info=" not in loc, (
            f"unexpected info-level redirect on a failed write: {loc!r}"
        )

        # Bytes must match pre-write snapshot.
        assert main.read_bytes() == pre_bytes, (
            "ROLLBACK FAILED: main.bean bytes diverged. "
            f"pre[-200:]={pre_bytes[-200:]!r} "
            f"post[-200:]={main.read_bytes()[-200:]!r}"
        )

        # Bean-check clean afterwards.
        errors = _bean_check_errors(main)
        assert errors == [], (
            f"bean-check dirty after rollback: {errors}"
        )


class TestRecoveryWriteEnvelope:
    """Phase 1.2 follow-up: the recovery handlers (``/setup/fix-*``)
    edit connector-owned files on an already-unparseable ledger and
    frequently delete lines. ``_recovery_write_envelope`` uses
    ``beancount.loader.load_file`` + ``_fatal_error_messages`` instead
    of ``run_bean_check_vs_baseline`` because the latter's
    ``_error_lines`` set-diff is keyed on ``"<file>:<line>: <msg>"``
    — a line-deleting edit shifts every downstream line number, so an
    identity-preserving dedupe reads as "new errors introduced" and
    the guard would fire a false rollback. These tests pin that
    reasoning in executable form.

    Uses ``/setup/fix-duplicate-closes`` as the test vehicle; the
    same envelope powers ``/setup/fix-orphan-overrides``.
    """

    def test_recovery_envelope_allows_line_deleting_dedupe(
        self, app_client, ledger_dir,
    ):
        """A valid dedupe that SHIFTS line numbers but produces the
        same (or fewer) fatal errors must pass the envelope and
        commit the write. Demonstrates the primary reason the
        recovery envelope exists: line-deleting edits don't read as
        regressions."""
        connector_accounts = ledger_dir / "connector_accounts.bean"
        # 5 Close lines, 2 unique — and 3 Payable closes with no
        # matching Open, which produces "Unopened account ... being
        # closed" errors on pre-existing lines. Every remaining Close
        # after dedupe still has no matching Open, so post-write will
        # *still* error on the same account — but with shifted line
        # numbers. The envelope must tolerate that.
        connector_accounts.write_text(
            "; managed by Lamella.\n"
            "\n2026-04-24 close Assets:Personal:Bank:Savings\n"
            "2026-04-24 close Assets:Personal:Bank:Savings\n"
            "2026-04-24 close Liabilities:BetaCorp:Payable:ToAcmeCo\n"
            "2026-04-24 close Liabilities:BetaCorp:Payable:ToAcmeCo\n"
            "2026-04-24 close Liabilities:BetaCorp:Payable:ToAcmeCo\n",
            encoding="utf-8",
        )
        # Also include connector_accounts from main.bean so the
        # loader actually parses it (the fixture main.bean doesn't
        # include it by default).
        main = ledger_dir / "main.bean"
        main_text = main.read_text(encoding="utf-8")
        if 'include "connector_accounts.bean"' not in main_text:
            main.write_text(
                main_text + '\ninclude "connector_accounts.bean"\n',
                encoding="utf-8",
            )

        r = app_client.post(
            "/setup/fix-duplicate-closes", follow_redirects=False,
        )
        assert r.status_code == 303
        loc = r.headers.get("location", "")
        assert "info=removed-" in loc, (
            f"envelope rolled back a valid dedupe: location={loc!r}"
        )
        # Dedupe committed: 5 → 2 unique Close lines.
        text = connector_accounts.read_text(encoding="utf-8")
        close_lines = [l for l in text.splitlines() if "close " in l]
        assert len(close_lines) == 2, (
            f"dedupe rolled back unexpectedly: {close_lines}"
        )

    def test_recovery_envelope_rolls_back_on_new_fatal_error(
        self, app_client, ledger_dir, monkeypatch,
    ):
        """If the recovery write produces a NEW fatal error
        (semantically different from any pre-write error), the
        envelope must restore every snapshotted file byte-for-byte
        and the route must redirect with an error code, not silently
        claim success."""
        connector_accounts = ledger_dir / "connector_accounts.bean"
        # Seed pre-write duplicates — same as the happy-path test,
        # so the handler has something legitimate to dedupe.
        pre_text = (
            "; managed by Lamella.\n"
            "\n2026-04-24 close Assets:Personal:Bank:Savings\n"
            "2026-04-24 close Assets:Personal:Bank:Savings\n"
            "2026-04-24 close Liabilities:BetaCorp:Payable:ToAcmeCo\n"
            "2026-04-24 close Liabilities:BetaCorp:Payable:ToAcmeCo\n"
            "2026-04-24 close Liabilities:BetaCorp:Payable:ToAcmeCo\n"
        )
        connector_accounts.write_text(pre_text, encoding="utf-8")
        main = ledger_dir / "main.bean"
        main_pre = main.read_text(encoding="utf-8")
        if 'include "connector_accounts.bean"' not in main_pre:
            main.write_text(
                main_pre + '\ninclude "connector_accounts.bean"\n',
                encoding="utf-8",
            )
        main_pre_bytes = main.read_bytes()
        accounts_pre_bytes = connector_accounts.read_bytes()

        # Monkeypatch connector_accounts.write_text INSIDE the
        # envelope's write_fn to emit a syntactically unparseable
        # file. This simulates the case we care about: the dedupe
        # somehow corrupts the file and bean parse-check must
        # reject it even though a naive content diff wouldn't flag
        # it.
        from pathlib import Path as _P
        real_write_text = _P.write_text

        def _broken_write(self, data, *a, **kw):
            if self == connector_accounts:
                data = "XXXX-YY-ZZ close @@@@ broken\n"
            return real_write_text(self, data, *a, **kw)

        monkeypatch.setattr(_P, "write_text", _broken_write)

        r = app_client.post(
            "/setup/fix-duplicate-closes", follow_redirects=False,
        )
        assert r.status_code == 303
        loc = r.headers.get("location", "")
        assert "error=" in loc, (
            f"envelope did not surface a rollback error: location={loc!r}"
        )
        assert "info=removed-" not in loc, (
            f"envelope silently claimed success on a broken write: {loc!r}"
        )

        # Rollback proof — bytes match pre-write for every file the
        # envelope snapshotted.
        assert connector_accounts.read_bytes() == accounts_pre_bytes, (
            "connector_accounts.bean not restored"
        )
        assert main.read_bytes() == main_pre_bytes, (
            "main.bean not restored"
        )

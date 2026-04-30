# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Add-modal error recovery + auto-scaffold for entities.

Two pieces:

1. When the new-modal POST fails validation (collision, format, unknown
   enum), the server returns the modal RE-RENDERED with an inline
   error banner + the user's submitted values pre-filled. HX-Retarget
   redirects the swap so the modal updates in place instead of leaking
   into the destination grid. The user can read the error, fix the
   field, and resubmit without losing their work.

2. When the new-modal POST succeeds with auto_scaffold=1, the matching
   Schedule C/F/Personal expense tree is scaffolded as part of the
   create transaction. No separate visit to /settings/entities/{slug}/
   scaffold required.
"""
from __future__ import annotations


class TestNewModalErrorRecovery:
    def test_collision_returns_modal_with_error_banner(self, app_client):
        conn = app_client.app.state.db
        conn.execute(
            "INSERT OR IGNORE INTO entities (slug, display_name, entity_type, is_active) "
            "VALUES (?, ?, ?, ?)",
            ("Acme", "Acme Co", "sole_proprietorship", 1),
        )
        conn.commit()
        r = app_client.post(
            "/settings/entities",
            data={
                "slug": "Acme",
                "display_name": "Acme",
                "entity_type": "sole_proprietorship",
                "tax_schedule": "C",
                "is_active": "1",
            },
            headers={
                "HX-Request": "true",
                "X-Modal-Kind": "businesses",
            },
            follow_redirects=False,
        )
        assert r.status_code == 200, r.text
        body = r.text
        # Inline error banner appears in the re-rendered modal.
        assert "already taken" in body
        # Suggested next-free slug surfaced.
        assert "Acme2" in body
        # Modal form is present (the same modal element re-rendered).
        assert 'id="entity-new-modal"' in body
        # User's submitted values are preserved in the form.
        assert 'value="Acme"' in body
        # HX-Retarget so HTMX swaps the modal in place.
        assert r.headers.get("HX-Retarget") == "#entity-new-modal"
        assert r.headers.get("HX-Reswap") == "outerHTML"

    def test_invalid_slug_format_returns_modal_with_error(self, app_client):
        r = app_client.post(
            "/settings/entities",
            data={
                "slug": "lowercase-bad",
                "display_name": "Bad slug demo",
                "entity_type": "sole_proprietorship",
                "tax_schedule": "C",
                "is_active": "1",
            },
            headers={
                "HX-Request": "true",
                "X-Modal-Kind": "businesses",
            },
            follow_redirects=False,
        )
        assert r.status_code == 200
        assert "not valid" in r.text or "must start with" in r.text
        assert r.headers.get("HX-Retarget") == "#entity-new-modal"

    def test_unknown_entity_type_returns_modal_with_error(self, app_client):
        r = app_client.post(
            "/settings/entities",
            data={
                "slug": "Acme",
                "display_name": "Acme",
                "entity_type": "made_up_value",
                "tax_schedule": "C",
                "is_active": "1",
            },
            headers={
                "HX-Request": "true",
                "X-Modal-Kind": "businesses",
            },
            follow_redirects=False,
        )
        assert r.status_code == 200
        assert "unknown entity_type" in r.text
        assert r.headers.get("HX-Retarget") == "#entity-new-modal"

    def test_validation_error_from_legacy_caller_still_400(self, app_client):
        # Non-modal callers (curl, the focused entity-edit page) get
        # the legacy 400 HTTPException rather than a re-rendered modal,
        # since they have no modal to redraw.
        r = app_client.post(
            "/settings/entities",
            data={
                "slug": "lowercase-bad",
                "display_name": "Bad slug",
                "entity_type": "sole_proprietorship",
                "tax_schedule": "C",
                "is_active": "1",
            },
            follow_redirects=False,
        )
        assert r.status_code == 400


class TestAutoScaffoldOnCreate:
    def test_modal_create_with_auto_scaffold_writes_opens(
        self, app_client, tmp_path, monkeypatch,
    ):
        # Point CONFIG_DIR at the repo's config so schedule_c_lines.yml
        # resolves regardless of test-fixture quirks.
        from pathlib import Path
        repo_root = Path(__file__).resolve().parents[1]
        monkeypatch.setenv("LAMELLA_CONFIG_DIR", str(repo_root / "config"))
        # The fixture's bean writer writes to a temp ledger; fetch it
        # before/after to confirm an Open directive landed.
        settings = app_client.app.state.settings
        ledger_before = ""
        if settings.ledger_main.exists():
            ledger_before = settings.ledger_main.read_text(encoding="utf-8")

        r = app_client.post(
            "/settings/entities",
            data={
                "slug": "AcmeFoo",
                "display_name": "Acme Foo",
                "entity_type": "sole_proprietorship",
                "tax_schedule": "C",
                "is_active": "1",
                "auto_scaffold": "1",
            },
            headers={
                "HX-Request": "true",
                "X-Modal-Kind": "businesses",
            },
            follow_redirects=False,
        )
        # Successful save returns the entity card partial.
        assert r.status_code == 200, r.text
        assert "AcmeFoo" in r.text
        # Auto-scaffold ran — at least one Expenses:AcmeFoo:* Open
        # directive should now appear in the ledger.
        ledger_after = ""
        if settings.ledger_main.exists():
            ledger_after = settings.ledger_main.read_text(encoding="utf-8")
        # Either main.bean or one of the connector accounts files got
        # the new opens. Search the whole ledger dir to be safe.
        any_scaffold = False
        for f in settings.ledger_dir.rglob("*.bean"):
            if "Expenses:AcmeFoo:" in f.read_text(encoding="utf-8"):
                any_scaffold = True
                break
        assert any_scaffold, (
            "expected at least one Expenses:AcmeFoo:* open after "
            "auto_scaffold=1; ledger before: " + ledger_before[:200]
        )

    def test_modal_create_without_auto_scaffold_does_not_open(
        self, app_client, monkeypatch,
    ):
        from pathlib import Path
        repo_root = Path(__file__).resolve().parents[1]
        monkeypatch.setenv("LAMELLA_CONFIG_DIR", str(repo_root / "config"))
        settings = app_client.app.state.settings

        r = app_client.post(
            "/settings/entities",
            data={
                "slug": "NoScaffold",
                "display_name": "No Scaffold",
                "entity_type": "sole_proprietorship",
                "tax_schedule": "C",
                "is_active": "1",
                # auto_scaffold deliberately omitted
            },
            headers={
                "HX-Request": "true",
                "X-Modal-Kind": "businesses",
            },
            follow_redirects=False,
        )
        assert r.status_code == 200, r.text
        # No Expenses:NoScaffold:* opens should appear.
        any_scaffold = False
        for f in settings.ledger_dir.rglob("*.bean"):
            if "Expenses:NoScaffold:" in f.read_text(encoding="utf-8"):
                any_scaffold = True
                break
        assert not any_scaffold

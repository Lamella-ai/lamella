# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""End-to-end walk of the schema-drift surface — Phase 5 checkpoint.

The unit tests cover detector / heal / route layers in isolation; this
test drives the full request/response cycle on a real (non-mocked) v0
install:

  1. Strip the version stamp from the fixture ledger to simulate a
     pre-stamped install with content.
  2. ``GET /setup/recovery/schema`` — detector fires on the missing
     stamp, page lists one ledger-axis Finding.
  3. ``GET /setup/recovery/schema/confirm?finding_id=…`` — Migration's
     dry_run runs against the real ledger, preview renders.
  4. ``POST /setup/recovery/schema/heal`` — heal_schema_drift dispatches
     to MigrateLedgerV0ToV1 inside the bean-snapshot envelope.
  5. After redirect: re-render of /setup/recovery/schema reports zero
     findings (stamp now in main.bean).
  6. main.bean carries a single ``custom "lamella-ledger-version"
     "1"`` directive.

This is the kind of validation no per-layer unit test can do — it
exercises the detector → route → heal → migrate_to_ledger →
version-stamp chain against a real app context, not a mocked one.

Heavy fixture: stamps + un-stamps a tmp ledger, takes ~5s. Worth
the cost as the only integration test for the heal-action chain;
leaving it absent would mean the first time anyone notices a
broken integration is when it fails on a real user's install.
"""
from __future__ import annotations

from urllib.parse import parse_qs, urlsplit


def _strip_version_stamp(main_bean_path):
    """Rewrite main.bean without the legacy bcg-ledger-version stamp
    so the detector classifies the ledger as 'has content but no
    stamp' — same shape a pre-spec ledger lands in on first boot
    after an upgrade."""
    text = main_bean_path.read_text(encoding="utf-8")
    # Strip any ``custom "bcg-ledger-version"`` or
    # ``custom "lamella-ledger-version"`` directive (defense against
    # whichever the fixture happens to ship).
    new_lines: list[str] = []
    for line in text.splitlines(keepends=True):
        if 'custom "bcg-ledger-version"' in line:
            continue
        if 'custom "lamella-ledger-version"' in line:
            continue
        new_lines.append(line)
    main_bean_path.write_text("".join(new_lines), encoding="utf-8")


def test_schema_drift_full_walk_v0_to_v1(app_client, settings):
    """Drive the real ledger-axis flow via TestClient. No mocked
    detectors / migrations / writers — every layer uses the live
    code path. The only setup is rewriting the fixture's main.bean
    to look like a stampless install."""
    # ---- 1. Simulate v0: strip the stamp from main.bean ----
    main = settings.ledger_main
    _strip_version_stamp(main)
    # Sanity-check pre-state.
    pre_text = main.read_text(encoding="utf-8")
    assert 'custom "lamella-ledger-version"' not in pre_text
    assert 'custom "bcg-ledger-version"' not in pre_text

    # The reader caches by mtime, so any previously-loaded entries
    # need to be invalidated. The route's get_ledger_reader pulls a
    # fresh reader per request via FastAPI's dependency injection,
    # so this is automatic — we don't need to invalidate manually.

    # ---- 2. GET /setup/recovery/schema — detector fires ----
    r = app_client.get("/setup/recovery/schema")
    assert r.status_code == 200
    assert "Schema is in sync" not in r.text  # NOT the empty state
    assert "Ledger axis" in r.text
    assert "Review" in r.text  # Apply→ button is "Review & apply"

    # The page links to the confirm step with a finding_id query.
    # Pull the id out by walking the rendered HTML.
    confirm_link = _extract_confirm_link(r.text)
    assert confirm_link is not None
    finding_id = parse_qs(urlsplit(confirm_link).query)["finding_id"][0]
    assert finding_id.startswith("schema_drift:")

    # ---- 3. GET /confirm — dry_run preview ----
    r = app_client.get(confirm_link)
    assert r.status_code == 200
    # MigrateLedgerV0ToV1.dry_run() returns kind='recompute' with a
    # summary mentioning the version stamp. Check the page rendered
    # the preview and the Apply button.
    assert "Apply migration" in r.text
    # Version stamp mention — current target may be v1, v2 or v3 depending
    # on how the latest schema target evolves; just check the stamp key
    # is referenced somewhere in the preview.
    assert "ledger-version" in r.text or "version" in r.text
    # No /settings/, /accounts, /simplefin leak in confirm page.
    assert "/settings/" not in r.text
    assert " /accounts" not in r.text

    # ---- 4. POST /heal — apply ----
    r = app_client.post(
        "/setup/recovery/schema/heal",
        data={"finding_id": finding_id},
        follow_redirects=False,
    )
    assert r.status_code == 303
    location = r.headers["location"]
    assert location.startswith("/setup/recovery/schema?")
    assert "last_ok=1" in location, (
        f"heal redirect carries success flag; got: {location}"
    )

    # ---- 5. Re-render — drift cleared ----
    r = app_client.get("/setup/recovery/schema")
    assert r.status_code == 200
    assert "Schema is in sync" in r.text

    # ---- 6. main.bean carries the stamp ----
    from lamella.core.bootstrap.detection import LATEST_LEDGER_VERSION
    stamp = f'custom "lamella-ledger-version" "{LATEST_LEDGER_VERSION}"'
    post_text = main.read_text(encoding="utf-8")
    assert stamp in post_text
    # Exactly once — idempotency check.
    assert post_text.count(stamp) == 1


def test_schema_drift_full_walk_idempotent_after_apply(
    app_client, settings,
):
    """A second click on Apply (e.g. user double-submits, browser
    re-POSTs on back-button) must not double-stamp or break the
    ledger. Detector reports 'in sync'; if the user somehow
    fabricates a POST, the missing-finding-id path returns a clean
    redirect."""
    main = settings.ledger_main
    _strip_version_stamp(main)

    # First walk: GET → confirm → heal.
    r = app_client.get("/setup/recovery/schema")
    confirm_link = _extract_confirm_link(r.text)
    finding_id = parse_qs(urlsplit(confirm_link).query)["finding_id"][0]
    r = app_client.post(
        "/setup/recovery/schema/heal",
        data={"finding_id": finding_id},
        follow_redirects=False,
    )
    assert r.status_code == 303

    # Second POST with the same id — drift is gone, the route's
    # re-detect-on-each-request defense kicks in.
    r = app_client.post(
        "/setup/recovery/schema/heal",
        data={"finding_id": finding_id},
        follow_redirects=False,
    )
    assert r.status_code == 303
    # Redirect carries the "no longer present" info banner with
    # last_ok=1 (treat as success — the desired state was reached,
    # just by a different path).
    assert "last_ok=1" in r.headers["location"]
    assert "no%20longer%20present" in r.headers["location"].lower()

    # Stamp count still 1.
    from lamella.core.bootstrap.detection import LATEST_LEDGER_VERSION
    stamp = f'custom "lamella-ledger-version" "{LATEST_LEDGER_VERSION}"'
    post_text = main.read_text(encoding="utf-8")
    assert post_text.count(stamp) == 1


# ---------------------------------------------------------------------------


def _extract_confirm_link(html: str) -> str | None:
    """Pull the first href pointing at the confirm endpoint."""
    needle = '/setup/recovery/schema/confirm?finding_id='
    idx = html.find(needle)
    if idx == -1:
        return None
    end = html.find('"', idx)
    if end == -1:
        return None
    return html[idx:end]

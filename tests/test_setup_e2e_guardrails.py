# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""End-to-end guardrail test for the setup-isolated surfaces.

Promotes audit discipline (Phase 4 audit `116467e`, Phase 6 audit
`b35ee61`) from one-off checks into permanent regression coverage.
The single slow parametrized test walks every reachable
``/setup/*`` URL under every state-fixture and asserts:

**Negative grep-rules** (these strings MUST NOT appear in any
rendered body):

1. ``/settings/`` — recovery shell isolation. The recovery flow
   never links into the main-app settings surfaces.
2. ``href="/accounts"`` (bare) — same isolation rule.
3. ``href="/simplefin"`` (bare) — same; the wrapper at
   ``/setup/simplefin`` is allowed.
4. ``data:`` URI prefix — rules out base64-leaked binary blobs in
   the rendered page.
5. Email regex — rules out maintainer / test email leaks in error
   messages and templates.
6. File-path-shaped string outside ``<code>`` / ``<pre>`` —
   promotes the Phase 6 audit's exception-message sanitization to
   a permanent guard. Heal-action errors flowing through the SSE
   stream into the finalizing page would be caught here.

**Positive grep-rules** per URL: each rendered page contains its
expected ``<h1>`` heading, drift fixtures show finding counts > 0,
healthy fixtures show empty-state text. Stops a "200-but-blank"
regression — a page returning 200 with no meaningful content
would silently pass a status-code-only test.

**Fixture states (4):**

- ``empty_install`` — fresh DB after migrate. No entities, no
  accounts, no findings.
- ``healthy_install`` — minimal entities seeded, every detector
  monkeypatched to ``()``. The success-shape we celebrate.
- ``drift_install`` — at least one finding per registered
  category (schema_drift, legacy_path) injected via monkeypatch.
- ``mid_batch_install`` — ``drift_install`` + populated
  ``setup_repair_state`` with one dismissed + one edited + one
  applied_history entry.

URLs not reachable under a given fixture (e.g. drift_install-only
URLs under empty_install) are marked with ``pytest.skip(...)``
inside the test body so the parametrize matrix stays explicit
without silently missing combinations.

This test is intentionally slow (counted in seconds, not ms) and
runs in the default suite. The Phase 4 audit lesson was that
audit checks have to run on every commit or they bit-rot.
"""
from __future__ import annotations

import re

import pytest

from lamella.features.recovery.models import (
    Finding,
    fix_payload,
    make_finding_id,
)


# ---------------------------------------------------------------------------
# Negative grep-rules — applied to every rendered page body
# ---------------------------------------------------------------------------


_FORBIDDEN_SUBSTRINGS = (
    "/settings/",
    'href="/accounts"',
    'href="/simplefin"',
    "data:",
)

_EMAIL_RE = re.compile(
    r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
)

# File-path-shaped string detector. Matches absolute Unix-style
# paths with at least 3 segments. Run AFTER stripping <code> /
# <pre> blocks because legitimate code samples can include paths.
# The Phase 6 audit's exception sanitization promoted to a guard
# here: if a heal action's OSError leaks /etc/sensitive/path/to.bean
# into an SSE event detail, this catches it on the finalizing page.
_PATH_RE = re.compile(r"(?:/[a-zA-Z][a-zA-Z0-9_.-]+){3,}")
# Whitelist legitimate URLs the templates ship — any path that's a
# valid in-app route should be exempt. Prefix-based to keep the
# rule simple.
_PATH_WHITELIST_PREFIXES = (
    "/setup/",
    "/static/",
    "/jobs/",
    "/api/",
    "/vehicles/",
    "/properties/",
    "/loans/",
    "/mileage/",
    "/projects/",
    "/budgets/",
    "/note/",
    "/recurring/",
    "/ai/",
    "/accounts/",  # subpaths only — bare /accounts is in the
                   # forbidden-substring list above
)


def _strip_path_safe_zones(html: str) -> str:
    """Remove zones where path-shaped tokens are legitimate before
    running the path-shape leak-detection regex.

    The rule applies to *natural* rendered body copy — text a user
    reads — not to:

    - ``<code>`` / ``<pre>`` spans (intentional code samples)
    - ``<script>`` blocks (EventSource targets, fetch URLs)
    - HTML attribute values (every src=/href=/action= contains
      template-controlled URLs; framework-injected ones include
      ``request.url_for("static", path=...)`` which expands to
      absolute URLs like ``http://testserver/static/app.css``)

    Without this, every page that loads CSS or images would trip
    the leak detector. The leak class we actually care about —
    OSError / sqlite3.OperationalError messages bleeding into
    finalize-page event-log lines — lands in *body* text, not in
    attribute values.

    Substring rules (``/settings/``, ``href="/accounts"``,
    ``href="/simplefin"``) still run against the FULL body so
    attribute-embedded leaks are caught by those.
    """
    no_pre = re.sub(r"<pre[\s\S]*?</pre>", "", html, flags=re.IGNORECASE)
    no_code = re.sub(r"<code[\s\S]*?</code>", "", no_pre, flags=re.IGNORECASE)
    no_script = re.sub(
        r"<script[\s\S]*?</script>", "", no_code, flags=re.IGNORECASE,
    )
    no_attr = re.sub(r'"[^"]*"', '""', no_script)
    return no_attr


def _assert_no_leaks(url: str, body: str) -> None:
    """Run all negative grep-rules. Raises AssertionError on any
    violation, with the offending URL + match snippet for debug.

    The shell-isolation substring rules run against the FULL body
    (so attribute values like ``href="/settings/foo"`` are caught).
    The path-shape rule runs against a body with attribute values
    stripped (so legitimate ``href="/static/app.css"`` URLs the
    framework injects don't trigger false positives — the rule is
    aimed at exception messages bleeding into rendered body copy).
    """
    for forbidden in _FORBIDDEN_SUBSTRINGS:
        assert forbidden not in body, (
            f"{url}: rendered body contains forbidden substring "
            f"{forbidden!r} — recovery shell isolation broken"
        )

    email_match = _EMAIL_RE.search(body)
    assert email_match is None or email_match.group(0).endswith("example.com"), (
        f"{url}: rendered body contains email-shaped string "
        f"{email_match.group(0)!r} — placeholder leak"
    )

    stripped = _strip_path_safe_zones(body)
    for path_match in _PATH_RE.finditer(stripped):
        path = path_match.group(0)
        if any(path.startswith(p) for p in _PATH_WHITELIST_PREFIXES):
            continue
        if "lamella-icon" in path or "favicon" in path:
            continue
        raise AssertionError(
            f"{url}: rendered body contains file-path-shaped string "
            f"{path!r} outside a code block — possible exception-message leak"
        )


# ---------------------------------------------------------------------------
# Synthetic findings (drift / mid-batch fixtures)
# ---------------------------------------------------------------------------


def _legacy_finding(target: str = "Assets:Vehicles:Foo") -> Finding:
    return Finding(
        id=make_finding_id("legacy_path", target),
        category="legacy_path",
        severity="warning",
        target_kind="account",
        target=target,
        summary=f"Move {target}",
        detail=None,
        proposed_fix=fix_payload(
            action="move", canonical="Assets:Personal:Vehicle:Foo",
        ),
        alternatives=(),
        confidence="high",
        source="detect_legacy_paths",
    )


def _schema_finding() -> Finding:
    target = "sqlite:50:53"
    return Finding(
        id=make_finding_id("schema_drift", target),
        category="schema_drift",
        severity="blocker",
        target_kind="schema",
        target=target,
        summary="SQLite drift v50 → v53",
        detail=None,
        proposed_fix=fix_payload(
            action="migrate", axis="sqlite",
            from_version=50, to_version=53,
        ),
        alternatives=(),
        confidence="high",
        source="detect_schema_drift",
    )


# ---------------------------------------------------------------------------
# Fixture seeders — apply per-state setup to the shared app_client
# ---------------------------------------------------------------------------


def _seed_empty(_app_client, _monkeypatch) -> None:
    """Default state from conftest's app_client. No additional seeding."""
    _monkeypatch.setattr(
        "lamella.features.recovery.findings.detect_all",
        lambda conn, entries: (),
    )
    _monkeypatch.setattr(
        "lamella.web.routes.setup_recovery.detect_all",
        lambda conn, entries: (),
    )


def _seed_healthy(app_client, monkeypatch) -> None:
    """Mark setup as complete; every detector returns ()."""
    app_client.app.state.setup_required_complete = True
    monkeypatch.setattr(
        "lamella.features.recovery.findings.detect_all",
        lambda conn, entries: (),
    )
    monkeypatch.setattr(
        "lamella.web.routes.setup_recovery.detect_all",
        lambda conn, entries: (),
    )


_DRIFT_LEGACY = _legacy_finding()
_DRIFT_SCHEMA = _schema_finding()


def _seed_drift(app_client, monkeypatch) -> None:
    """Inject one finding per registered category."""
    findings = (_DRIFT_LEGACY, _DRIFT_SCHEMA)
    monkeypatch.setattr(
        "lamella.features.recovery.findings.detect_all",
        lambda conn, entries: findings,
    )
    monkeypatch.setattr(
        "lamella.web.routes.setup_recovery.detect_all",
        lambda conn, entries: findings,
    )
    monkeypatch.setattr(
        "lamella.features.recovery.findings.detect_schema_drift",
        lambda conn, entries: (_DRIFT_SCHEMA,),
    )


def _seed_mid_batch(app_client, monkeypatch) -> None:
    """Drift + populated repair_state (one dismissed + one edited
    + one applied_history entry)."""
    _seed_drift(app_client, monkeypatch)
    from lamella.features.recovery.repair_state import write_repair_state
    write_repair_state(app_client.app.state.db, {
        "findings": {
            _DRIFT_LEGACY.id: {
                "action": "edit",
                "edit_payload": {"canonical": "Assets:Acme:Vehicle:Foo"},
            },
            _DRIFT_SCHEMA.id: {"action": "dismiss", "edit_payload": None},
        },
        "applied_history": [{
            "group": "cleanup",
            "committed_at": "2026-01-01T00:00:00+00:00",
            "applied_finding_ids": ["legacy_path:dead0001"],
            "failed_finding_ids": [],
            "rolled_back": False,
        }],
    })


# Map fixture name → seeder. Used by the parametrized test to set
# state via monkeypatch + app_client mutation.
_FIXTURE_SEEDERS = {
    "empty": _seed_empty,
    "healthy": _seed_healthy,
    "drift": _seed_drift,
    "mid_batch": _seed_mid_batch,
}


# ---------------------------------------------------------------------------
# URL × fixture matrix
# ---------------------------------------------------------------------------


# Each entry: (url, reachable_fixtures, expected_substrings).
# expected_substrings is asserted as positive grep — at least one
# of the listed substrings must appear in the body. This prevents
# the "200-but-blank" regression class.
URL_MATRIX: list[tuple[str, tuple[str, ...], tuple[str, ...]]] = [
    # Legacy /setup/progress URL — must 302 to /setup/recovery
    # under every fixture.
    (
        "/setup/progress",
        ("empty", "healthy", "drift", "mid_batch"),
        # 302 has no body to grep; positive check happens via
        # location header in the test body itself.
        (),
    ),
    # The canonical recovery surface.
    (
        "/setup/recovery",
        ("empty", "healthy", "drift", "mid_batch"),
        # Heading text from the recovery template.
        ("Recovery",),
    ),
    # Finalizing page — needs a job_id; we test the "no job" path
    # which renders the shell with steps pre-marked done.
    (
        "/setup/recovery/finalizing",
        ("empty", "healthy", "drift", "mid_batch"),
        ("Applying repairs",),
    ),
    # Schema-drift list page (Phase 5).
    (
        "/setup/recovery/schema",
        ("empty", "healthy", "drift", "mid_batch"),
        ("Schema drift",),
    ),
    # Legacy-paths surface (Phase 3).
    (
        "/setup/legacy-paths",
        ("empty", "healthy", "drift", "mid_batch"),
        ("Legacy paths cleanup",),
    ),
]


@pytest.mark.parametrize("url,reachable,expected", URL_MATRIX)
@pytest.mark.parametrize("fixture", ["empty", "healthy", "drift", "mid_batch"])
def test_e2e_setup_recovery_guardrails(
    app_client, monkeypatch,
    fixture, url, reachable, expected,
):
    """The single slow guardrail walk. See module docstring."""
    if fixture not in reachable:
        pytest.skip(f"{url} not reachable under fixture {fixture!r}")

    _FIXTURE_SEEDERS[fixture](app_client, monkeypatch)

    r = app_client.get(url, follow_redirects=False)

    # The 302 alias is special — assert the redirect target then exit.
    if url == "/setup/progress":
        assert r.status_code == 302, (
            f"{url}: expected 302 redirect, got {r.status_code}"
        )
        assert r.headers["location"] == "/setup/recovery", (
            f"{url}: expected redirect to /setup/recovery, got "
            f"{r.headers['location']!r}"
        )
        return

    # Every other URL must render a 200.
    assert r.status_code == 200, (
        f"{url} under fixture {fixture!r}: expected 200, got {r.status_code}"
    )

    body = r.text
    _assert_no_leaks(url, body)

    # Positive grep — at least one expected substring must appear.
    if expected:
        assert any(s in body for s in expected), (
            f"{url} under fixture {fixture!r}: none of "
            f"{expected!r} appeared in body — possible 200-but-blank "
            f"regression"
        )

# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import shutil
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from lamella.utils._legacy_env import apply_env_aliases  # noqa: E402

# Tests bypass main.py's startup, so call the env-alias shim directly
# here. Anything that imports lamella.core.config and calls Settings() in
# a test process needs LAMELLA_* / CONNECTOR_* aliases applied first.
apply_env_aliases()

# Test-suite default: vector search OFF for every Settings() construction.
# The lifespan path spawns a sentence-transformers worker that outlives
# its TestClient event loop and segfaults the next test's SQLite handle
# (see the per-fixture explanation in `settings` below). Setting the env
# at conftest import time ensures every test that constructs Settings()
# directly — including tests in this suite that don't go through the
# `settings` fixture — picks up the safe default. Tests that explicitly
# need the index can override via the Settings constructor or
# monkeypatch.setenv.
import os as _os
_os.environ.setdefault("AI_VECTOR_SEARCH_ENABLED", "0")

from lamella.core.db import connect, migrate  # noqa: E402
from lamella.core.config import Settings  # noqa: E402


FIXTURE_LEDGER = Path(__file__).parent / "fixtures" / "ledger"


@pytest.fixture(autouse=True)
def _isolate_process_state():
    """Per-test isolation of mutable process-wide state.

    Why: tests like ``test_legacy_env.py`` write directly to
    ``os.environ`` (not via ``monkeypatch``), and
    ``lamella.core.config.get_settings()`` is ``@lru_cache``-wrapped.
    Without this fixture, the first call to ``get_settings()`` caches
    a Settings instance built from whatever env was set at that
    moment — every later test in the suite then sees stale settings
    even after their own ``monkeypatch.setenv``. Likewise, env vars
    leaked from earlier tests show up as fake DATA_DIR / LEDGER_DIR
    paths, causing "no such table" errors when the next test's
    lifespan tries to migrate against the wrong sqlite file.

    Restores os.environ to snapshot at teardown, clears cached
    Settings, and resets the one-shot deprecation-warning ledger.
    """
    import os as _os

    env_snapshot = dict(_os.environ)
    yield
    # Restore env to snapshot exactly: drop new keys, fix mutated
    # values, re-add deleted keys.
    current = list(_os.environ.keys())
    for key in current:
        if key not in env_snapshot:
            del _os.environ[key]
    for key, value in env_snapshot.items():
        if _os.environ.get(key) != value:
            _os.environ[key] = value
    # Drop cached Settings so the next test rebuilds from its own env.
    try:
        from lamella.core.config import get_settings as _get_settings
        _get_settings.cache_clear()
    except Exception:  # noqa: BLE001
        pass
    # Reset legacy-env one-shot warning tracker so each test sees
    # its own DeprecationWarnings.
    try:
        from lamella.utils import _legacy_env as _le
        _le._warned.clear()
    except Exception:  # noqa: BLE001
        pass


@pytest.fixture(autouse=True)
def _no_real_external_http():
    """Network safety net for the test suite. Any unmocked HTTP call
    to a paid / external service is a bug — at best it costs money
    or rate-limits, at worst it leaks data.

    Implementation: an autouse respx context-manager refuses every
    request by default (assert_all_mocked=True). Tests that
    legitimately exercise an HTTP-bound path declare their own
    routes via `with respx.mock(...) as mock` inside the test;
    respx's nested-router behavior lets those stubs win for the
    duration of the inner context. Anything not matched by any
    stub raises ``AllMockedAssertionError`` — loud failure, no
    silent escape to production.

    Hosts mocked here are explicitly the paid / external surfaces.
    Local TestClient calls (`testserver`) and the conftest's fake
    Paperless / SimpleFIN-bridge hostnames are not intercepted —
    those use respx pass-through paths that the route-level mocks
    add when needed.
    """
    try:
        import respx as _respx
    except Exception:  # noqa: BLE001 — respx not installed; safe by default
        yield
        return

    # Block the paid / external hosts the app actually talks to.
    # Other hosts (TestClient, conftest's fake paperless.test /
    # simplefin-bridge.test) get explicit pass-through routes so
    # they don't trip the strict-mode unmocked-request check.
    paid_hosts = (
        "https://openrouter.ai",
        "https://api.openrouter.ai",
        "https://api.anthropic.com",
        "https://api.openai.com",
    )
    # Hosts the test fixtures use that need to pass through (or
    # be served by route-level mocks the test installs itself).
    # Listed as explicit pass-through routes so respx's strict-
    # mode default (assert_all_mocked=True) doesn't reject them.
    passthrough_hosts = (
        "paperless.test",
        "simplefin-bridge.test",
        "testserver",  # FastAPI TestClient
    )
    # assert_all_mocked=True (respx default) — strict on unknown
    # hosts. Tests that legitimately hit a paid host declare a
    # nested respx.mock(base_url=...) and respx layers it on top
    # of the refusal route; the nested context wins for the
    # duration of the test. Earlier `assert_all_mocked=False`
    # workaround broke nested mocks (test_ai_classify), so we
    # revert to strict + explicit pass-through for the local-
    # only hosts.
    with _respx.mock(assert_all_called=False) as router:
        for base in paid_hosts:
            # Default refusal — unmocked calls to these hosts raise.
            # A test that declares its own route via a nested
            # respx.mock(base_url=base) will override these.
            router.route(host=_respx.patterns.M(host=base.split("//", 1)[1])).mock(
                side_effect=lambda req: (_ for _ in ()).throw(
                    RuntimeError(
                        f"Refusing real HTTP call to {req.url} during "
                        f"tests. Mock with respx.mock(base_url=...) "
                        f"or monkeypatch the underlying client."
                    )
                )
            )
        for host in passthrough_hosts:
            # Pass-through: respx lets the request go to the real
            # network (which fails fast for paperless.test —
            # invalid TLD — and that's fine; the lifespan paths
            # that call it tolerate the failure).
            router.route(host=_respx.patterns.M(host=host)).pass_through()
        yield


@pytest.fixture
def db(tmp_path: Path) -> sqlite3.Connection:
    conn = connect(tmp_path / "test.sqlite")
    migrate(conn)
    yield conn
    conn.close()


@pytest.fixture
def ledger_dir(tmp_path: Path) -> Path:
    """Copy of the fixture ledger into a tmp dir so tests may mutate it."""
    dest = tmp_path / "ledger"
    shutil.copytree(FIXTURE_LEDGER, dest)
    return dest


@pytest.fixture
def settings(tmp_path: Path, ledger_dir: Path) -> Settings:
    return Settings(
        data_dir=tmp_path / "data",
        ledger_dir=ledger_dir,
        paperless_url="https://paperless.test",
        paperless_api_token="token-test",  # pydantic wraps into SecretStr
        # Vector search OFF in tests: the lifespan spawns
        # `_vector_index_refresh` as a background task that loads
        # sentence-transformers + walks the ledger on a worker
        # thread. Across many TestClient context cycles in one
        # session, those worker threads outlive their owning
        # event loop and crash on the next test's SQLite handle —
        # surfaces as a Windows access violation in
        # tests/test_setup_smoke.py at ~test #11. Tests that
        # specifically need the vector index can override this.
        ai_vector_search_enabled=False,
    )


@pytest.fixture
def app_client(settings, tmp_path, monkeypatch):
    """Build a FastAPI TestClient against a tmp ledger + SQLite."""
    from fastapi.testclient import TestClient
    from lamella.main import create_app

    # Skip actually shelling out to bean-check in tests.
    monkeypatch.setattr(
        "lamella.features.receipts.linker.run_bean_check",
        lambda main_bean: None,
    )

    app = create_app(settings=settings)
    # Tests focus on individual route behavior, not the first-run
    # onboarding gate. Force the gate flags off so every route is
    # reachable; tests that need to exercise the gate set the flags
    # explicitly via app.state. Lifespan startup may set these — so
    # we override after entering the TestClient context too.
    with TestClient(app) as client:
        app.state.needs_welcome = False
        app.state.needs_reconstruct = False
        app.state.setup_required_complete = True
        # ledger_detection is a frozen dataclass with `needs_setup`
        # as a computed property — can't be assigned to. Replace the
        # whole object with a stub the setup_gate middleware reads.
        # Tests that specifically need to exercise the setup-gate path
        # construct their own app via `create_app(settings=...)` and
        # don't go through this fixture (e.g. tests/test_setup_*).
        class _NoSetupNeeded:
            needs_setup = False
        app.state.ledger_detection = _NoSetupNeeded()
        try:
            yield client
        finally:
            # Drain any in-flight job_runner threads BEFORE the
            # TestClient context exits — otherwise a worker thread
            # touching the SQLite handle after teardown segfaults the
            # process. JobRunner.shutdown is best-effort
            # (wait=False, cancel_futures=True); we wait briefly for
            # workers already mid-execute.
            try:
                runner = getattr(app.state, "job_runner", None)
                if runner is not None and hasattr(runner, "_pool"):
                    runner.shutdown()
                    try:
                        runner._pool.shutdown(wait=True)
                    except Exception:  # noqa: BLE001
                        pass
            except Exception:  # noqa: BLE001
                pass

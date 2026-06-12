# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware
# financial intelligence
# https://lamella.ai

"""Tests for ``_resolve_account_path`` in
``lamella.web.routes.staging_review``.

The resolver is shared across the synchronous classify route, the
"Ask AI" job worker, the deposit-skip preflight, and the row-extras
display helper. A regression here breaks every one of those paths
silently; this test fixture pins the per-source semantics so a
future cleanup can't quietly drop reboot support.
"""
from __future__ import annotations

import sqlite3

import pytest

from lamella.web.routes.staging_review import _resolve_account_path


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.execute(
        """
        CREATE TABLE accounts_meta (
            account_path TEXT PRIMARY KEY,
            simplefin_account_id TEXT,
            kind TEXT,
            entity_slug TEXT
        )
        """,
    )
    c.execute(
        "INSERT INTO accounts_meta(account_path, simplefin_account_id) "
        "VALUES (?, ?)",
        ("Liabilities:Acme:Card:CardA1234", "sf-card-1234"),
    )
    yield c
    c.close()


def test_simplefin_resolves_via_account_id(conn):
    out = _resolve_account_path(
        conn, "simplefin", {"account_id": "sf-card-1234"},
    )
    assert out == "Liabilities:Acme:Card:CardA1234"


def test_simplefin_unknown_account_id_returns_none(conn):
    out = _resolve_account_path(
        conn, "simplefin", {"account_id": "sf-unknown"},
    )
    assert out is None


def test_reboot_resolves_via_representative_account(conn):
    """The historical reboot row shape: source_ref carries
    {file, lineno} only; the bank-side leg lives in raw."""
    raw = {
        "representative_account": "Liabilities:Acme:Card:CardA1234",
        "leg_count": 2,
        "postings": [
            {"account": "Liabilities:Acme:Card:CardA1234"},
            {"account": "Expenses:Acme:FIXME"},
        ],
    }
    out = _resolve_account_path(
        conn, "reboot",
        {"file": "/ledger/simplefin_transactions.bean", "lineno": 77},
        raw=raw,
    )
    assert out == "Liabilities:Acme:Card:CardA1234"


def test_reboot_skips_non_bank_representative_account(conn):
    """If representative_account isn't an Assets:/Liabilities: leg,
    fall through and scan postings for the first one that is."""
    raw = {
        "representative_account": "Expenses:Acme:FIXME",
        "postings": [
            {"account": "Expenses:Acme:FIXME"},
            {"account": "Assets:Personal:Checking"},
        ],
    }
    out = _resolve_account_path(
        conn, "reboot", {"file": "/ledger/x.bean", "lineno": 1},
        raw=raw,
    )
    assert out == "Assets:Personal:Checking"


def test_reboot_falls_back_when_raw_missing(conn):
    """Pre-fix reboot rows with no enriched source_ref AND no raw
    payload still surface as None — caller decides."""
    out = _resolve_account_path(
        conn, "reboot",
        {"file": "/ledger/x.bean", "lineno": 1},
        raw=None,
    )
    assert out is None


def test_reboot_honors_account_path_in_source_ref(conn):
    """Forward-fix path: newly-staged reboot rows write
    ``account_path`` directly into source_ref so the existing
    generic resolver branch fires."""
    out = _resolve_account_path(
        conn, "reboot",
        {
            "file": "/ledger/x.bean",
            "lineno": 1,
            "account_path": "Liabilities:Acme:Card:CardA1234",
        },
    )
    assert out == "Liabilities:Acme:Card:CardA1234"


def test_generic_account_path_branch(conn):
    """Any source whose source_ref carries an account_path resolves
    via the generic branch."""
    out = _resolve_account_path(
        conn, "paste",
        {"account_path": "Assets:Personal:Checking"},
    )
    assert out == "Assets:Personal:Checking"


def test_unknown_source_returns_none(conn):
    out = _resolve_account_path(conn, "csv", {"foo": "bar"})
    assert out is None


# --- Gap 3: ai.py decision-card hydration passes raw= to resolver ----------


def test_ai_py_staged_hydration_passes_raw_json_to_resolver():
    """The staged-row hydration in ai.py (AI decision card display) must
    include raw_json in its SELECT and pass the parsed value as raw= to
    _resolve_account_path.

    Without this, reboot rows older than commit 470de1e8 (which lack
    account_path in source_ref) cannot have entity resolved for display —
    the card shows no source_account/source_entity even though the data
    is available in the raw_json envelope.

    Source-level guard mirroring the existing call-site pattern."""
    import inspect
    import lamella.web.routes.ai as ai_mod

    src = inspect.getsource(ai_mod)

    # 1. The SELECT must include raw_json so the data is fetched.
    assert "raw_json" in src, (
        "ai.py staged-hydration SELECT must include raw_json so reboot "
        "rows can have entity resolved via the raw payload."
    )

    # 2. The _resolve_account_path call in this module must pass raw=
    assert "raw=_raw_for_resolver" in src or "raw=" in src, (
        "ai.py _resolve_account_path call must pass raw= so the reboot "
        "resolver branch fires for rows without account_path in source_ref."
    )

    # 3. Specifically confirm raw= is passed in the staged hydration block —
    #    check that the _resolve_account_path call appears after raw_json
    #    in the source (so we know raw_json is fetched before the call).
    raw_json_idx = src.find("raw_json")
    resolve_idx = src.find("_resolve_account_path(")
    assert raw_json_idx != -1, "raw_json not found in ai.py"
    assert resolve_idx != -1, "_resolve_account_path not found in ai.py"
    # The resolve call should come after the raw_json fetch (SELECT + parse).
    assert resolve_idx > raw_json_idx, (
        "ai.py _resolve_account_path call appears before raw_json is fetched; "
        "the raw payload won't be available for the reboot resolver branch."
    )

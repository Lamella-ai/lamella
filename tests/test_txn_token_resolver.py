# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 3 of the immutable /txn/{token} URL invariant: the resolver
accepts both UUIDv7 and legacy txn_hash. UUIDv7 tokens that match a
staged row (status != 'promoted') render the staged detail; tokens
that match an on-disk ``lamella-txn-id`` (or alias) render the ledger
detail; hex tokens fall through to the legacy resolver."""
from __future__ import annotations

from decimal import Decimal

import pytest

from lamella.features.import_.staging import StagingService
from lamella.web.routes.search import (
    _find_txn_by_lamella_id,
    _is_uuidv7_token,
    _UUIDV7_RE,
)


_VALID = "01900000-0000-7000-8000-000000000abc"


class TestUuidv7Detection:
    @pytest.mark.parametrize("token", [
        _VALID,
        "01900000-0000-7000-A000-000000000abc",  # uppercase A allowed in variant
        "01900000-0000-7fff-bfff-ffffffffffff",
    ])
    def test_valid_uuidv7(self, token):
        assert _is_uuidv7_token(token)

    @pytest.mark.parametrize("token", [
        "",
        # Wrong version (v4)
        "01900000-0000-4000-8000-000000000abc",
        # Wrong variant nibble (no 8/9/a/b)
        "01900000-0000-7000-7000-000000000abc",
        # Too short
        "01900000-0000-7000-8000-",
        # Plain hex hash
        "deadbeefcafebabe1234567890abcdef",
    ])
    def test_rejects_non_uuidv7(self, token):
        assert not _is_uuidv7_token(token)


class TestStagedRouting:
    def test_pre_promotion_renders_staged_detail(self, app_client):
        # Stage a row, then GET /txn/{lamella_txn_id}.
        conn = app_client.app.state.db
        svc = StagingService(conn)
        row = svc.stage(
            source="csv",
            source_ref={"id": "csv-1"},
            posting_date="2026-04-20",
            amount=Decimal("-12.34"),
            payee="Acme Co.",
            description="testing",
        )
        assert row.lamella_txn_id is not None

        resp = app_client.get(f"/txn/{row.lamella_txn_id}")
        assert resp.status_code == 200
        # The staged template carries this banner copy and the
        # immutable token in the kicker.
        assert "Not yet in the ledger" in resp.text
        assert row.lamella_txn_id[:16] in resp.text
        # And the action component renders for the staged ref.
        assert f'data-ref="staged:{row.id}"' in resp.text

    def test_unknown_uuidv7_returns_404(self, app_client):
        resp = app_client.get(f"/txn/{_VALID}")
        assert resp.status_code == 404

    def test_legacy_hex_token_rejected(self, app_client):
        # Post-v3 the hex form is gone — every entry has a
        # lamella-txn-id and the resolver only accepts UUIDv7.
        resp = app_client.get("/txn/deadbeefcafebabe1234567890abcdef")
        assert resp.status_code == 404
        assert "retired in v3" in resp.text


class TestLedgerWalkAlias:
    def test_alias_meta_resolves(self, monkeypatch):
        """``lamella-txn-id-alias-N`` is the transfer-pair preservation
        path. The walker has to match either the primary or the alias.
        """
        from beancount.core.data import Transaction
        # Build a synthetic Transaction-shape with primary id A and
        # alias id B; assert the walker finds the entry under both.
        primary = "01900000-0000-7000-8000-aaaaaaaaaaaa"
        alias = "01900000-0000-7000-8000-bbbbbbbbbbbb"
        # Lightweight stand-in: namedtuple-shaped object with .meta.
        e = Transaction(
            meta={
                "lamella-txn-id": primary,
                "lamella-txn-id-alias-0": alias,
                "filename": "x.bean",
                "lineno": 1,
            },
            date=__import__("datetime").date(2026, 4, 20),
            flag="*",
            payee="Acme",
            narration="x",
            tags=set(),
            links=set(),
            postings=[],
        )

        class FakeReader:
            def load(self):
                class Loaded:
                    entries = [e]
                return Loaded()

        reader = FakeReader()
        assert _find_txn_by_lamella_id(reader, primary) is e
        assert _find_txn_by_lamella_id(reader, alias) is e
        assert _find_txn_by_lamella_id(
            reader, "01900000-0000-7000-8000-cccccccccccc",
        ) is None

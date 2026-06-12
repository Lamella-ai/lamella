# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the v3 → v4 ledger migration: stamp-only bump.

Per ADR-0061 the v3 → v4 cutover renames the receipt-* directive
vocabulary to document-*. The reader accepts both vocabularies
indefinitely; the writer emits document-* only. The migration
itself is therefore stamp-only — it bumps the
``lamella-ledger-version`` stamp from ``"3"`` to ``"4"`` and does
NOT rewrite existing receipt-* directives in connector_links.bean.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from lamella.core.db import connect, migrate
from lamella.features.recovery.migrations.migrate_ledger_v3_to_v4 import (
    MigrateLedgerV3ToV4,
)


class _Settings:
    """Minimal Settings stand-in. The migration reads ledger_main only."""

    def __init__(self, ledger_dir: Path):
        self.ledger_dir = ledger_dir
        self.ledger_main = ledger_dir / "main.bean"


def _make_v3_ledger(tmp_path: Path, *, with_receipt_directives: bool = True) -> _Settings:
    """Stand up a v3 ledger that may carry legacy receipt-* directives.

    The migration is supposed to leave those directives alone — the
    point of this test suite is to assert that the receipt-* lines
    survive verbatim across the version bump.
    """
    (tmp_path / "main.bean").write_text(
        'option "operating_currency" "USD"\n'
        '\n'
        '2020-01-01 custom "lamella-ledger-version" "3"\n'
        '\n'
        '2020-01-01 open Assets:Bank USD\n',
        encoding="utf-8",
    )
    if with_receipt_directives:
        (tmp_path / "connector_links.bean").write_text(
            '; Managed by Lamella. Do not hand-edit.\n'
            '\n'
            '2026-01-15 custom "receipt-link" "01900000-0000-7000-8000-aaaaaaaaaaaa"\n'
            '  lamella-paperless-id: 42\n'
            '\n'
            '2026-01-16 custom "receipt-dismissed" "01900000-0000-7000-8000-bbbbbbbbbbbb"\n'
            '  lamella-dismissed-by: "user"\n',
            encoding="utf-8",
        )
    return _Settings(tmp_path)


@pytest.fixture
def conn() -> sqlite3.Connection:
    c = connect(Path(":memory:"))
    migrate(c)
    return c


def test_declared_paths_only_main_bean(tmp_path: Path):
    settings = _make_v3_ledger(tmp_path)
    paths = MigrateLedgerV3ToV4().declared_paths(settings)
    # Stamp-only migration: the only file that gets touched is main.
    # Existing receipt-* directives elsewhere are untouched.
    assert paths == (settings.ledger_main,)


def test_dry_run_reports_stamp_bump(tmp_path: Path, conn):
    settings = _make_v3_ledger(tmp_path)
    pre = settings.ledger_main.read_text(encoding="utf-8")
    pre_links = (tmp_path / "connector_links.bean").read_text(encoding="utf-8")
    result = MigrateLedgerV3ToV4().dry_run(conn=conn, settings=settings)
    assert '"3"' in result.summary and '"4"' in result.summary
    # Disk unchanged: dry-run is plan-only.
    assert settings.ledger_main.read_text(encoding="utf-8") == pre
    assert (tmp_path / "connector_links.bean").read_text(encoding="utf-8") == pre_links


def test_dry_run_idempotent_on_v4_ledger(tmp_path: Path, conn):
    settings = _make_v3_ledger(tmp_path)
    settings.ledger_main.write_text(
        settings.ledger_main.read_text().replace(
            'custom "lamella-ledger-version" "3"',
            'custom "lamella-ledger-version" "4"',
        ),
        encoding="utf-8",
    )
    result = MigrateLedgerV3ToV4().dry_run(conn=conn, settings=settings)
    assert "already at v4" in result.summary


def test_apply_bumps_stamp_only(tmp_path: Path, conn):
    settings = _make_v3_ledger(tmp_path)
    pre_links = (tmp_path / "connector_links.bean").read_text(encoding="utf-8")
    MigrateLedgerV3ToV4().apply(conn=conn, settings=settings)
    text = settings.ledger_main.read_text(encoding="utf-8")
    # Stamp bumped.
    assert 'custom "lamella-ledger-version" "4"' in text
    assert 'custom "lamella-ledger-version" "3"' not in text
    # Critical: the receipt-* directives in connector_links.bean were
    # NOT rewritten. ADR-0061 specifies opportunistic rewrite, not
    # eager — the next time a writer touches connector_links it will
    # naturally emit document-* lines, but this migration leaves
    # legacy directives alone.
    post_links = (tmp_path / "connector_links.bean").read_text(encoding="utf-8")
    assert post_links == pre_links
    assert 'custom "receipt-link"' in post_links
    assert 'custom "receipt-dismissed"' in post_links


def test_apply_idempotent_on_v4_ledger(tmp_path: Path, conn):
    settings = _make_v3_ledger(tmp_path)
    # Pre-stamp as v4.
    settings.ledger_main.write_text(
        settings.ledger_main.read_text().replace(
            'custom "lamella-ledger-version" "3"',
            'custom "lamella-ledger-version" "4"',
        ),
        encoding="utf-8",
    )
    pre = settings.ledger_main.read_text(encoding="utf-8")
    MigrateLedgerV3ToV4().apply(conn=conn, settings=settings)
    # Disk unchanged — re-applying a v4 migration on a v4 ledger is
    # a no-op.
    assert settings.ledger_main.read_text(encoding="utf-8") == pre


def test_apply_appends_stamp_when_missing(tmp_path: Path, conn):
    """Defensive case: a ledger with no version stamp at all gets
    a fresh v4 stamp. ``detect_ledger_state`` should route un-stamped
    ledgers to NEEDS_VERSION_STAMP before calling this migration, but
    if it ever does land here, we want to stamp rather than crash.
    """
    settings = _make_v3_ledger(tmp_path)
    # Strip the stamp.
    txt = settings.ledger_main.read_text(encoding="utf-8")
    settings.ledger_main.write_text(
        "\n".join(
            ln for ln in txt.splitlines()
            if 'lamella-ledger-version' not in ln
        ) + "\n",
        encoding="utf-8",
    )
    MigrateLedgerV3ToV4().apply(conn=conn, settings=settings)
    text = settings.ledger_main.read_text(encoding="utf-8")
    assert 'custom "lamella-ledger-version" "4"' in text


def test_apply_raises_when_main_missing(tmp_path: Path, conn):
    settings = _Settings(tmp_path)
    # main.bean does not exist
    from lamella.features.recovery.migrations.base import MigrationError
    with pytest.raises(MigrationError, match="main.bean is missing"):
        MigrateLedgerV3ToV4().apply(conn=conn, settings=settings)


def test_migration_is_registered_for_3_to_4_key():
    """Auto-discovery must route the (3, 4) finding to this migration."""
    from lamella.features.recovery.migrations import _LEDGER_REGISTRY

    instance = _LEDGER_REGISTRY.get(("3", 4))
    assert isinstance(instance, MigrateLedgerV3ToV4)

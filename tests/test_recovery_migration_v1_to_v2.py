# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the v1 → v2 ledger migration: bcg-* on-disk rewrite.

Phase: post-rebrand cleanup. The v1 → v2 migration walks every
``.bean`` under the ledger root, applies the three legacy-prefix
regexes from ``transform.bcg_to_lamella``, and bumps the version
stamp from ``"1"`` to ``"2"``. These tests pin the contract:

1. ``declared_paths`` covers every .bean (not just connector-owned).
2. ``dry_run`` returns counts without modifying disk.
3. ``apply`` rewrites the three legacy forms (meta keys, tags, custom
   directive types) and bumps the version stamp.
4. Idempotent — a second apply on a clean v2 ledger is a no-op.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from lamella.features.recovery.migrations.migrate_ledger_v1_to_v2 import (
    MigrateLedgerV1ToV2,
)


class _Settings:
    """Minimal Settings stand-in. The migration only reads
    ``ledger_dir`` and ``ledger_main``."""

    def __init__(self, ledger_dir: Path):
        self.ledger_dir = ledger_dir
        self.ledger_main = ledger_dir / "main.bean"


def _make_v1_ledger(tmp_path: Path) -> _Settings:
    """Stand up a v1 ledger with bcg-* references in three forms:
    metadata key, transaction tag, custom directive type."""
    main = tmp_path / "main.bean"
    main.write_text(
        'option "operating_currency" "USD"\n'
        '\n'
        '; Schema marker — v1 (bcg-era stamp pre-rewrite)\n'
        '2020-01-01 custom "bcg-ledger-version" "1"\n'
        '\n'
        '2020-01-01 open Assets:Bank USD\n'
        '2020-01-01 open Income:Salary USD\n'
        '\n'
        '2026-01-15 * "Paycheck" #bcg-override\n'
        '  bcg-simplefin-id: "abc123"\n'
        '  Assets:Bank      1000 USD\n'
        '  Income:Salary   -1000 USD\n',
        encoding="utf-8",
    )
    # An additional connector file with bcg- key
    (tmp_path / "connector_config.bean").write_text(
        '2026-01-01 custom "setting" "key" "value"\n'
        '  bcg-set-at: "2026-04-21T22:09:47"\n',
        encoding="utf-8",
    )
    return _Settings(tmp_path)


def test_declared_paths_returns_all_bean_files(tmp_path: Path):
    """The migration must declare every .bean under the ledger root,
    not just connector-owned ones — bcg-* keys can land anywhere."""
    settings = _make_v1_ledger(tmp_path)
    # Add a hand-authored file that lives outside the connector set.
    (tmp_path / "manual_transactions.bean").write_text(
        '; user-authored\n', encoding="utf-8",
    )

    paths = MigrateLedgerV1ToV2().declared_paths(settings)
    names = {p.name for p in paths}
    assert "main.bean" in names
    assert "connector_config.bean" in names
    assert "manual_transactions.bean" in names


def test_dry_run_counts_bcg_references_without_writing(tmp_path: Path):
    """Dry-run reports per-file substitution counts and totals; no
    disk content changes."""
    settings = _make_v1_ledger(tmp_path)
    pre_main = settings.ledger_main.read_text(encoding="utf-8")
    pre_config = (tmp_path / "connector_config.bean").read_text(encoding="utf-8")

    result = MigrateLedgerV1ToV2().dry_run(conn=None, settings=settings)
    assert result.kind == "rename"
    # main has 3 bcg-* references: custom "bcg-ledger-version", a
    # #bcg-override tag, and a bcg-simplefin-id meta key.
    assert result.counts.get("main.bean", 0) == 3
    # connector_config has 1: bcg-set-at.
    assert result.counts.get("connector_config.bean", 0) == 1

    # Disk unchanged.
    assert settings.ledger_main.read_text(encoding="utf-8") == pre_main
    assert (
        (tmp_path / "connector_config.bean").read_text(encoding="utf-8")
        == pre_config
    )


def test_apply_rewrites_all_three_forms_and_bumps_stamp(tmp_path: Path):
    """Apply rewrites meta keys, tags, and custom directive types,
    then bumps the version stamp from "1" to "2"."""
    settings = _make_v1_ledger(tmp_path)

    MigrateLedgerV1ToV2().apply(conn=None, settings=settings)

    main_text = settings.ledger_main.read_text(encoding="utf-8")
    # Stamp bumped to v2 and rewritten to lamella- prefix.
    assert 'custom "lamella-ledger-version" "2"' in main_text
    assert 'custom "bcg-ledger-version"' not in main_text
    # Tag rewritten.
    assert "#lamella-override" in main_text
    assert "#bcg-override" not in main_text
    # Meta key rewritten.
    assert "lamella-simplefin-id:" in main_text
    assert "bcg-simplefin-id:" not in main_text

    config_text = (tmp_path / "connector_config.bean").read_text(encoding="utf-8")
    assert "lamella-set-at:" in config_text
    assert "bcg-set-at:" not in config_text


def test_apply_idempotent_on_clean_v2_ledger(tmp_path: Path):
    """A second apply on a v2 ledger is a no-op: no writes happen and
    the stamp count stays at one."""
    settings = _make_v1_ledger(tmp_path)
    MigrateLedgerV1ToV2().apply(conn=None, settings=settings)

    pre_main = settings.ledger_main.read_text(encoding="utf-8")

    MigrateLedgerV1ToV2().apply(conn=None, settings=settings)

    post_main = settings.ledger_main.read_text(encoding="utf-8")
    assert post_main == pre_main
    # Exactly one v2 stamp.
    assert post_main.count('custom "lamella-ledger-version" "2"') == 1


def test_apply_appends_v2_stamp_when_no_existing_version(tmp_path: Path):
    """Edge: a ledger with bcg-* keys but no version stamp at all
    (shouldn't happen via real detection but defensive). Apply appends
    a fresh v2 stamp rather than failing."""
    main = tmp_path / "main.bean"
    main.write_text(
        'option "operating_currency" "USD"\n'
        '2020-01-01 open Assets:Bank USD\n'
        '2026-01-15 * "Test"\n'
        '  bcg-simplefin-id: "x"\n'
        '  Assets:Bank   10 USD\n'
        '  Assets:Bank  -10 USD\n',
        encoding="utf-8",
    )
    settings = _Settings(tmp_path)

    MigrateLedgerV1ToV2().apply(conn=None, settings=settings)

    text = main.read_text(encoding="utf-8")
    assert 'custom "lamella-ledger-version" "2"' in text
    assert "lamella-simplefin-id:" in text


def test_dry_run_zero_count_on_clean_ledger(tmp_path: Path):
    """Dry-run on a clean v1 ledger (no bcg-* references) returns
    a zero count and a 'stamp only' summary."""
    main = tmp_path / "main.bean"
    main.write_text(
        'option "operating_currency" "USD"\n'
        '2020-01-01 custom "lamella-ledger-version" "1"\n'
        '2020-01-01 open Assets:Bank USD\n',
        encoding="utf-8",
    )
    settings = _Settings(tmp_path)

    result = MigrateLedgerV1ToV2().dry_run(conn=None, settings=settings)
    assert result.kind == "rename"
    assert sum(result.counts.values()) == 0


def test_apply_raises_when_main_bean_missing(tmp_path: Path):
    """Apply against a ledger dir without main.bean raises
    MigrationError so the heal-action's snapshot envelope rolls back."""
    from lamella.features.recovery.migrations.base import MigrationError
    # Create an .bean somewhere but no main.bean.
    (tmp_path / "other.bean").write_text("; placeholder\n", encoding="utf-8")
    settings = _Settings(tmp_path)

    with pytest.raises(MigrationError):
        MigrateLedgerV1ToV2().apply(conn=None, settings=settings)

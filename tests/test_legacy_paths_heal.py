# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for ``heal_legacy_path`` — Phase 3 of /setup/recovery.

The detector tests pin down what gets surfaced; these tests pin
down what happens when the user clicks Close or Move-and-close.
"""
from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path

import pytest
from beancount.loader import load_file

from lamella.features.recovery.findings.legacy_paths import (
    detect_legacy_paths,
)
from lamella.features.recovery.heal.legacy_paths import (
    HealRefused,
    heal_legacy_path,
)
from lamella.core.db import migrate


# ---------------------------------------------------------------------------
# Fixtures (mirrors test_loans_scaffolding._make_ledger pattern)
# ---------------------------------------------------------------------------


def _make_ledger(tmp_path: Path, body: str = "") -> dict:
    """Build a minimal real ledger that bean-check will accept."""
    main = tmp_path / "main.bean"
    connector_accounts = tmp_path / "connector_accounts.bean"
    connector_config = tmp_path / "connector_config.bean"
    connector_overrides = tmp_path / "connector_overrides.bean"

    main.write_text(
        'option "title" "Recovery test"\n'
        'plugin "beancount.plugins.auto_accounts"\n'
        f'include "{connector_accounts.name}"\n'
        f'include "{connector_overrides.name}"\n'
        f'include "{connector_config.name}"\n'
        "\n"
        + body,
        encoding="utf-8",
    )
    connector_accounts.write_text(
        "; connector_accounts.bean\n", encoding="utf-8",
    )
    connector_config.write_text(
        "; connector_config.bean\n", encoding="utf-8",
    )
    connector_overrides.write_text(
        "; connector_overrides.bean\n", encoding="utf-8",
    )
    return {
        "main": main,
        "connector_accounts": connector_accounts,
        "connector_config": connector_config,
        "connector_overrides": connector_overrides,
    }


class _Settings:
    def __init__(self, paths: dict):
        self.ledger_main = paths["main"]
        self.connector_accounts_path = paths["connector_accounts"]
        self.connector_config_path = paths["connector_config"]
        self.connector_overrides_path = paths["connector_overrides"]


class _Reader:
    def __init__(self, main: Path):
        self.main = main
        self._loaded = None

    def load(self):
        if self._loaded is None:
            entries, _errs, _opts = load_file(str(self.main))

            class _L:
                def __init__(self, ents):
                    self.entries = ents
            self._loaded = _L(entries)
        return self._loaded

    def invalidate(self):
        self._loaded = None


@pytest.fixture
def conn():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    migrate(db)
    yield db
    db.close()


# ---------------------------------------------------------------------------
# Close action
# ---------------------------------------------------------------------------


class TestHealClose:
    def test_closes_empty_account_writes_close_directive(
        self, tmp_path: Path, conn,
    ):
        # Set up a ledger with a legacy Assets:Vehicles:V2008Fabrikam
        # path that has no postings.
        paths = _make_ledger(
            tmp_path,
            body=(
                "2020-01-01 open Assets:Vehicles:V2008Fabrikam\n"
                "2020-01-01 open Assets:Personal:Vehicle:V2009FabrikamSuv\n"
            ),
        )
        reader = _Reader(paths["main"])
        settings = _Settings(paths)

        findings = detect_legacy_paths(conn, reader.load().entries)
        # Pick the close-proposal finding for V2008Fabrikam (no
        # postings → close is the proposed action).
        target = next(
            f for f in findings
            if f.target == "Assets:Vehicles:V2008Fabrikam"
        )
        assert target.proposed_fix_dict["action"] == "close"

        result = heal_legacy_path(
            target, conn=conn, settings=settings, reader=reader,
        )

        assert result.success
        assert "Closed" in result.message

        # Verify a Close directive was appended.
        text = paths["connector_accounts"].read_text(encoding="utf-8")
        assert "close Assets:Vehicles:V2008Fabrikam" in text

    def test_refuses_close_when_account_has_postings(
        self, tmp_path: Path, conn,
    ):
        paths = _make_ledger(
            tmp_path,
            body=(
                "2020-01-01 open Assets:Vehicles:V2008Fabrikam\n"
                "2020-01-01 open Assets:Personal:Vehicle:V2009FabrikamSuv\n"
                '2024-06-01 * "purchase"\n'
                "  Assets:Vehicles:V2008Fabrikam   500 USD\n"
                "  Assets:Personal:Vehicle:V2009FabrikamSuv  -500 USD\n"
            ),
        )
        reader = _Reader(paths["main"])
        settings = _Settings(paths)

        findings = detect_legacy_paths(conn, reader.load().entries)
        target = next(
            f for f in findings
            if f.target == "Assets:Vehicles:V2008Fabrikam"
        )

        # The detector proposes 'move' for accounts with postings,
        # but we can synthesize a 'close' fix to test the refusal.
        from dataclasses import replace
        from lamella.features.recovery.models import fix_payload
        close_finding = replace(
            target,
            proposed_fix=fix_payload(action="close"),
            alternatives=(),
        )
        with pytest.raises(HealRefused, match="posting"):
            heal_legacy_path(
                close_finding, conn=conn, settings=settings, reader=reader,
            )


# ---------------------------------------------------------------------------
# Move-and-close action
# ---------------------------------------------------------------------------


class TestHealMoveAndClose:
    def test_moves_postings_opens_canonical_and_closes_legacy(
        self, tmp_path: Path, conn,
    ):
        # Vehicle is registered → canonical is computable.
        conn.execute(
            "INSERT INTO vehicles (slug, entity_slug) VALUES (?, ?)",
            ("V2008Fabrikam", "Personal"),
        )
        conn.commit()

        paths = _make_ledger(
            tmp_path,
            body=(
                "2020-01-01 open Assets:Vehicles:V2008Fabrikam\n"
                "2020-01-01 open Assets:Personal:Vehicle:V2009FabrikamSuv\n"
                "2020-01-01 open Assets:Cash USD\n"
                '2024-06-01 * "fueled up"\n'
                "  Assets:Vehicles:V2008Fabrikam   50 USD\n"
                "  Assets:Cash                  -50 USD\n"
            ),
        )
        reader = _Reader(paths["main"])
        settings = _Settings(paths)

        findings = detect_legacy_paths(conn, reader.load().entries)
        target = next(
            f for f in findings
            if f.target == "Assets:Vehicles:V2008Fabrikam"
        )
        # With a posting present, detector proposes move.
        assert target.proposed_fix_dict["action"] == "move"
        assert target.proposed_fix_dict["canonical"] == (
            "Assets:Personal:Vehicle:V2008Fabrikam"
        )

        result = heal_legacy_path(
            target, conn=conn, settings=settings, reader=reader,
        )

        assert result.success, result.message
        assert "1 posting" in result.message
        assert "Assets:Personal:Vehicle:V2008Fabrikam" in result.message
        assert "closed Assets:Vehicles:V2008Fabrikam" in result.message

        # Verify the posting got rewritten in main.bean.
        main_text = paths["main"].read_text(encoding="utf-8")
        # The legacy path no longer appears as a posting account in
        # main.bean (only as the now-closed Open directive).
        # Filter to lines that are postings (indented).
        posting_lines = [
            line for line in main_text.splitlines()
            if line.startswith("  ")
        ]
        legacy_postings = [
            line for line in posting_lines
            if "Assets:Vehicles:V2008Fabrikam" in line
        ]
        assert legacy_postings == [], (
            f"legacy postings still present: {legacy_postings}"
        )
        canonical_postings = [
            line for line in posting_lines
            if "Assets:Personal:Vehicle:V2008Fabrikam" in line
        ]
        assert canonical_postings, (
            "canonical posting did not appear after rewrite"
        )

        # Connector_accounts.bean now has the new Open + the Close.
        ca_text = paths["connector_accounts"].read_text(encoding="utf-8")
        assert "open Assets:Personal:Vehicle:V2008Fabrikam" in ca_text
        assert "close Assets:Vehicles:V2008Fabrikam" in ca_text

    def test_refuses_when_canonical_no_longer_passes_guards(
        self, tmp_path: Path, conn,
    ):
        # Detector emitted a 'move' finding earlier, but between
        # that and the heal click someone closed every account
        # under the destination's branch — the canonical's
        # ancestor is no longer part of any opened branch. Heal
        # must refuse rather than scaffold a phantom branch.
        paths = _make_ledger(
            tmp_path,
            body=(
                "2020-01-01 open Assets:Vehicles:V2008Fabrikam\n"
                "2020-01-01 open Assets:Cash USD\n"
                '2024-06-01 * "x"\n'
                "  Assets:Vehicles:V2008Fabrikam   50 USD\n"
                "  Assets:Cash                  -50 USD\n"
            ),
        )
        reader = _Reader(paths["main"])
        settings = _Settings(paths)

        # Hand-craft a finding that proposes a canonical the guards
        # would reject (no Personal:Vehicle ancestor opened anywhere).
        from lamella.features.recovery.models import (
            Finding, fix_payload, make_finding_id,
        )
        rogue = Finding(
            id=make_finding_id("legacy_path", "Assets:Vehicles:V2008Fabrikam"),
            category="legacy_path",
            severity="warning",
            target_kind="account",
            target="Assets:Vehicles:V2008Fabrikam",
            summary="x",
            detail=None,
            proposed_fix=fix_payload(
                action="move",
                canonical="Assets:Personal:Vehicle:V2008Fabrikam",
            ),
            alternatives=(),
            confidence="medium",
            source="test",
        )
        with pytest.raises(HealRefused, match="guards"):
            heal_legacy_path(
                rogue, conn=conn, settings=settings, reader=reader,
            )


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


class TestDispatch:
    def test_rejects_non_legacy_finding(self, tmp_path: Path, conn):
        from lamella.features.recovery.models import (
            Finding, fix_payload, make_finding_id,
        )
        wrong = Finding(
            id=make_finding_id("schema_drift", "main.bean"),
            category="schema_drift",  # not legacy_path
            severity="blocker",
            target_kind="schema",
            target="main.bean",
            summary="x",
            detail=None,
            proposed_fix=fix_payload(action="migrate"),
            alternatives=(),
            confidence="high",
            source="test",
        )
        paths = _make_ledger(tmp_path)
        reader = _Reader(paths["main"])
        settings = _Settings(paths)
        with pytest.raises(HealRefused, match="not a legacy_path"):
            heal_legacy_path(
                wrong, conn=conn, settings=settings, reader=reader,
            )

    def test_rejects_unknown_action(self, tmp_path: Path, conn):
        from lamella.features.recovery.models import (
            Finding, fix_payload, make_finding_id,
        )
        weird = Finding(
            id=make_finding_id("legacy_path", "x"),
            category="legacy_path",
            severity="warning",
            target_kind="account",
            target="x",
            summary="x",
            detail=None,
            proposed_fix=fix_payload(action="vaporize"),
            alternatives=(),
            confidence="low",
            source="test",
        )
        paths = _make_ledger(tmp_path)
        reader = _Reader(paths["main"])
        settings = _Settings(paths)
        with pytest.raises(HealRefused, match="unknown legacy_path action"):
            heal_legacy_path(
                weird, conn=conn, settings=settings, reader=reader,
            )

# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the Finding dataclass + make_finding_id.

Phase 3 of /setup/recovery — the dataclass shape is the contract
Phases 4–6 inherit, so the tests here pin down id stability,
hashability, validation, and the proposed_fix tuple-of-pairs
format.
"""
from __future__ import annotations

import pytest

from lamella.features.recovery.models import (
    CATEGORIES,
    CONFIDENCES,
    SEVERITIES,
    TARGET_KINDS,
    Finding,
    HealResult,
    fix_payload,
    make_finding_id,
)


class TestMakeFindingId:
    def test_format_is_category_colon_hash(self):
        fid = make_finding_id("legacy_path", "Assets:Vehicles:V2008Fabrikam")
        category, _, digest = fid.partition(":")
        assert category == "legacy_path"
        # 12-char truncated sha1 hex
        assert len(digest) == 12
        assert all(c in "0123456789abcdef" for c in digest)

    def test_stable_across_calls(self):
        a = make_finding_id("legacy_path", "Assets:Vehicles:V2008Fabrikam")
        b = make_finding_id("legacy_path", "Assets:Vehicles:V2008Fabrikam")
        assert a == b

    def test_different_targets_yield_different_ids(self):
        a = make_finding_id("legacy_path", "Assets:Vehicles:V2008Fabrikam")
        b = make_finding_id("legacy_path", "Assets:Vehicles:V2009Fabrikam")
        assert a != b

    def test_different_categories_yield_different_ids(self):
        a = make_finding_id("legacy_path", "main.bean")
        b = make_finding_id("schema_drift", "main.bean")
        assert a != b

    def test_empty_inputs_raise(self):
        with pytest.raises(ValueError):
            make_finding_id("", "target")
        with pytest.raises(ValueError):
            make_finding_id("legacy_path", "")


class TestFinding:
    def _valid_kwargs(self, **overrides):
        base = dict(
            id="legacy_path:abc123",
            category="legacy_path",
            severity="warning",
            target_kind="account",
            target="Assets:Vehicles:V2008",
            summary="Non-canonical vehicle path",
            detail=None,
            proposed_fix=fix_payload(action="close"),
            alternatives=(),
            confidence="high",
            source="detect_legacy_paths",
        )
        base.update(overrides)
        return base

    def test_constructs_with_valid_inputs(self):
        f = Finding(**self._valid_kwargs())
        assert f.category == "legacy_path"
        assert f.proposed_fix_dict == {"action": "close"}

    def test_rejects_invalid_severity(self):
        with pytest.raises(ValueError, match="severity"):
            Finding(**self._valid_kwargs(severity="critical"))

    def test_rejects_invalid_target_kind(self):
        with pytest.raises(ValueError, match="target_kind"):
            Finding(**self._valid_kwargs(target_kind="filesystem"))

    def test_rejects_invalid_confidence(self):
        with pytest.raises(ValueError, match="confidence"):
            Finding(**self._valid_kwargs(confidence="probably"))

    def test_is_hashable(self):
        f1 = Finding(**self._valid_kwargs())
        f2 = Finding(**self._valid_kwargs())
        # Two equal Findings should hash identically.
        assert hash(f1) == hash(f2)
        # Usable as dict key.
        d = {f1: "ok"}
        assert d[f2] == "ok"

    def test_alternatives_stored_as_tuple_of_tuples(self):
        f = Finding(**self._valid_kwargs(
            alternatives=(
                fix_payload(action="move", canonical="Assets:Personal:Vehicle:V2008"),
                fix_payload(action="close"),
            ),
        ))
        # Stored shape is preserved.
        assert isinstance(f.alternatives, tuple)
        assert all(isinstance(a, tuple) for a in f.alternatives)
        # Helper converts to dicts for ergonomics.
        as_dicts = f.alternatives_dicts
        assert as_dicts[0]["action"] == "move"
        assert as_dicts[1]["action"] == "close"

    def test_frozen_cant_mutate(self):
        f = Finding(**self._valid_kwargs())
        with pytest.raises(Exception):  # FrozenInstanceError
            f.summary = "mutated"  # type: ignore[misc]


class TestFixPayload:
    def test_sorts_keys_for_stable_hash(self):
        a = fix_payload(action="move", canonical="X")
        b = fix_payload(canonical="X", action="move")
        # Order-independent — same payload regardless of kwarg order.
        assert a == b
        assert hash(a) == hash(b)

    def test_round_trips_via_dict(self):
        p = fix_payload(action="move", canonical="X", note="reason")
        assert dict(p) == {"action": "move", "canonical": "X", "note": "reason"}


class TestEnumStability:
    """Pin the enum tuples so a refactor that drops a value gets
    caught — the strings are the contract Phase 6's repair_state
    will key off of."""

    def test_severities(self):
        assert SEVERITIES == ("blocker", "warning", "suggestion")

    def test_confidences(self):
        assert CONFIDENCES == ("high", "medium", "low")

    def test_target_kinds_includes_phase_3_4_5_targets(self):
        # Phase 3 needs 'account'. Phase 4: 'entity', 'vehicle',
        # 'property'. Phase 5: 'schema'. Phase 1's importer-asset
        # detection (deferred) would use 'file' / 'config'.
        for k in ("account", "entity", "vehicle", "property",
                  "schema", "file", "config"):
            assert k in TARGET_KINDS

    def test_categories_includes_phase_3(self):
        assert "legacy_path" in CATEGORIES


class TestHealResult:
    def test_construct_success(self):
        from pathlib import Path
        r = HealResult(
            success=True,
            message="closed Assets:Vehicles:V2008",
            files_touched=(Path("/x/connector_accounts.bean"),),
            finding_id="legacy_path:abc123",
        )
        assert r.success
        assert "closed" in r.message

    def test_is_hashable(self):
        from pathlib import Path
        r1 = HealResult(
            success=True, message="ok",
            files_touched=(Path("/x/y.bean"),),
            finding_id="x:1",
        )
        r2 = HealResult(
            success=True, message="ok",
            files_touched=(Path("/x/y.bean"),),
            finding_id="x:1",
        )
        assert hash(r1) == hash(r2)

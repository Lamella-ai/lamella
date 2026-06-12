# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for ``run_bulk_apply`` — Phase 6.1.3.

Pins down the locked-spec behavior:

- BatchEvent dataclasses serialize to the right (message, outcome,
  detail) shape with the discriminator field.
- ``categorize`` partitions findings into the three locked groups
  with cleanup as the fallback for unknown categories.
- Orchestrator emits events in the locked order: batch_started,
  per-group {group_started, finding_*, group_committed},
  batch_done.
- Empty findings → batch_done(success) with applied=0/failed=0.
- Single-group batch → other groups skipped silently (no
  group_started for empty groups).
- Mid-group failure → group_committed with failed>0, subsequent
  groups don't start, batch_done(partial).
- Re-detection between groups: new findings ignored for current
  batch, dropped findings silently removed.
- Unknown category dispatch → FindingFailed with HealRefused
  message.
- Edit-payload override → effective_finding's proposed_fix
  reflects user's edit (legacy_path canonical override).
"""
from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from beancount.loader import load_file

from lamella.features.recovery.bulk_apply import (
    BatchDone,
    BatchEvent,
    BatchStarted,
    BulkContext,
    CATEGORY_GROUP,
    FindingApplied,
    FindingFailed,
    GROUPS,
    GroupCommitted,
    GroupRolledBack,
    GroupStarted,
    categorize,
    run_bulk_apply,
)
from lamella.features.recovery.heal.legacy_paths import HealRefused
from lamella.features.recovery.models import (
    Finding,
    HealResult,
    fix_payload,
    make_finding_id,
)
from lamella.core.db import migrate


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def conn():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    migrate(db)
    yield db
    db.close()


def _make_ledger(tmp_path: Path) -> dict:
    """Minimal real ledger that bean-check accepts."""
    main = tmp_path / "main.bean"
    connector_accounts = tmp_path / "connector_accounts.bean"
    connector_config = tmp_path / "connector_config.bean"
    main.write_text(
        'option "title" "bulk-apply test"\n'
        'plugin "beancount.plugins.auto_accounts"\n'
        f'include "{connector_accounts.name}"\n'
        f'include "{connector_config.name}"\n'
        '2020-01-01 custom "lamella-ledger-version" "1"\n'
        # A small content sentinel so the ledger-axis stamp
        # detector treats this as "has content".
        '\n2024-06-01 * "sentinel"\n'
        '  Assets:Personal:Cash    1.00 USD\n'
        '  Equity:OpeningBalances\n',
        encoding="utf-8",
    )
    connector_accounts.write_text("; connector_accounts.bean\n", encoding="utf-8")
    connector_config.write_text("; connector_config.bean\n", encoding="utf-8")
    return {
        "ledger_dir": tmp_path,
        "main": main,
        "connector_accounts": connector_accounts,
        "connector_config": connector_config,
    }


class _Settings:
    def __init__(self, paths: dict):
        self.ledger_dir = paths["ledger_dir"]
        self.ledger_main = paths["main"]
        self.connector_accounts_path = paths["connector_accounts"]
        self.connector_config_path = paths["connector_config"]


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


def _finding(category: str, target: str, *, id_suffix: str = "") -> Finding:
    """Synthetic Finding builder. Tests don't need real
    detector output; they need predictable Finding objects to
    drive the orchestrator."""
    return Finding(
        id=make_finding_id(category, target + id_suffix),
        category=category,
        severity="warning" if category == "legacy_path" else "blocker",
        target_kind="account" if category == "legacy_path" else "schema",
        target=target + id_suffix,
        summary=f"{category}: {target}{id_suffix}",
        detail=None,
        proposed_fix=fix_payload(action="apply"),
        alternatives=(),
        confidence="high",
        source="synthetic",
    )


def _drafts_for(*findings: Finding, action: str = "apply") -> dict:
    """Build a closed-world ``repair_state`` blob for the given
    findings — every finding gets an explicit draft entry so the
    orchestrator runs them. Mirrors the production page contract:
    submit POST writes a draft per rendered finding."""
    return {
        "findings": {
            f.id: {"action": action, "edit_payload": None}
            for f in findings
        },
        "applied_history": [],
    }


# ---------------------------------------------------------------------------
# BatchEvent dataclasses
# ---------------------------------------------------------------------------


class TestBatchEventVocabulary:
    def test_to_emit_carries_event_discriminator(self):
        e = BatchStarted(groups=("schema", "labels", "cleanup"), total_findings=3)
        msg, outcome, detail = e.to_emit()
        assert detail["event"] == "batch_started"
        assert detail["groups"] == ["schema", "labels", "cleanup"]
        assert detail["total_findings"] == 3

    def test_finding_applied_payload_includes_summary_and_category(self):
        """Per the sub-freeze: payload extension beyond the
        original spec so the UI can render narratives without
        state lookup."""
        e = FindingApplied(
            finding_id="schema_drift:abc",
            group="schema",
            summary="BankTwo Mortgage labeled as loan",
            category="schema_drift",
        )
        _msg, outcome, detail = e.to_emit()
        assert detail["summary"] == "BankTwo Mortgage labeled as loan"
        assert detail["category"] == "schema_drift"
        assert outcome == "success"

    def test_finding_failed_payload_includes_summary_and_category(self):
        e = FindingFailed(
            finding_id="legacy_path:xyz",
            group="cleanup",
            summary="Move Assets:Vehicles:Foo → canonical",
            category="legacy_path",
            message="bean-check failed",
        )
        msg, outcome, detail = e.to_emit()
        assert detail["summary"] == "Move Assets:Vehicles:Foo → canonical"
        assert detail["category"] == "legacy_path"
        assert detail["message"] == "bean-check failed"
        assert outcome == "failure"

    def test_group_rolled_back_outcome_is_failure(self):
        """Failure events bump the modal's failure counter."""
        e = GroupRolledBack(
            group="labels",
            reason="x",
            preserved_groups=("schema",),
        )
        _msg, outcome, _detail = e.to_emit()
        assert outcome == "failure"

    def test_tuple_fields_serialize_as_lists(self):
        """SSE detail is JSON-serialized; tuples become lists.
        The test asserts the conversion happens at the dataclass
        boundary, not in the SSE adapter — keeps the contract
        tight."""
        e = GroupRolledBack(
            group="cleanup",
            reason="x",
            preserved_groups=("schema", "labels"),
        )
        _msg, _outcome, detail = e.to_emit()
        assert detail["preserved_groups"] == ["schema", "labels"]
        assert isinstance(detail["preserved_groups"], list)


# ---------------------------------------------------------------------------
# Categorization
# ---------------------------------------------------------------------------


class TestCategorize:
    def test_known_categories_route_to_locked_groups(self):
        findings = (
            _finding("schema_drift", "sqlite:48:53"),
            _finding("legacy_path", "Assets:Vehicles:Foo"),
            _finding("unlabeled_account", "Liabilities:Acme:Loan:Bar"),
        )
        out = categorize(findings)
        assert len(out["schema"]) == 1
        assert len(out["labels"]) == 1
        assert len(out["cleanup"]) == 1

    def test_unknown_category_falls_back_to_cleanup(self):
        """Defensive — better than silently dropping. A new
        Finding category that's added without updating
        CATEGORY_GROUP shows up in cleanup until the mapping
        catches up."""
        f = _finding("invented_category", "x")
        out = categorize((f,))
        assert out["cleanup"] == [f]

    def test_partition_preserves_per_group_order(self):
        """Findings within a group surface in input order — UI
        rendering assumes deterministic ordering."""
        findings = (
            _finding("legacy_path", "Assets:Vehicles:A"),
            _finding("legacy_path", "Assets:Vehicles:B"),
            _finding("legacy_path", "Assets:Vehicles:C"),
        )
        out = categorize(findings)
        assert [f.target for f in out["cleanup"]] == [
            "Assets:Vehicles:A",
            "Assets:Vehicles:B",
            "Assets:Vehicles:C",
        ]

    def test_groups_constant_matches_locked_order(self):
        """The locked group order is part of the public contract;
        a refactor that reorders this tuple would silently change
        bulk-apply behavior."""
        assert GROUPS == ("schema", "labels", "cleanup")


# ---------------------------------------------------------------------------
# Orchestrator behavior
# ---------------------------------------------------------------------------


class TestRunBulkApply:
    def test_empty_findings_emits_batch_done_success(self, tmp_path, conn):
        """No findings → batch_started, batch_done(success) with
        applied=0/failed=0. No group_started events between."""
        paths = _make_ledger(tmp_path)
        reader = _Reader(paths["main"])
        settings = _Settings(paths)

        events = list(run_bulk_apply(
            conn=conn, settings=settings, reader=reader,
            repair_state={"findings": {}, "applied_history": []},
            detect_fn=lambda c, e: (),
        ))

        types = [e.EVENT for e in events]
        assert types == ["batch_started", "batch_done"]
        done = events[-1]
        assert isinstance(done, BatchDone)
        assert done.outcome == "success"
        assert done.summary == {
            "applied": 0, "failed": 0, "groups": [],
        }

    def test_dismissed_findings_excluded_from_batch(self, tmp_path, conn):
        """Findings with action='dismiss' in repair_state aren't
        applied — they don't count toward the batch."""
        paths = _make_ledger(tmp_path)
        reader = _Reader(paths["main"])
        settings = _Settings(paths)

        f = _finding("legacy_path", "Assets:Vehicles:Foo")
        repair_state = {
            "findings": {f.id: {"action": "dismiss", "edit_payload": None}},
            "applied_history": [],
        }
        events = list(run_bulk_apply(
            conn=conn, settings=settings, reader=reader,
            repair_state=repair_state,
            detect_fn=lambda c, e: (f,),
        ))

        # Effectively empty batch.
        types = [e.EVENT for e in events]
        assert types == ["batch_started", "batch_done"]
        assert events[-1].outcome == "success"
        assert events[-1].summary["applied"] == 0

    def test_single_group_batch_skips_other_groups_silently(
        self, tmp_path, conn, monkeypatch,
    ):
        """Only legacy_path findings → cleanup group runs, schema
        and labels skipped (no group_started events for empty
        groups)."""
        paths = _make_ledger(tmp_path)
        reader = _Reader(paths["main"])
        settings = _Settings(paths)

        f = _finding("legacy_path", "Assets:Vehicles:Foo")
        # Stub the heal so we don't actually need a real legacy
        # path in the ledger.
        from lamella.features.recovery import bulk_apply as ba_mod

        def _stub_heal(finding, **_kwargs):
            return HealResult(
                success=True,
                message="closed",
                files_touched=(),
                finding_id=finding.id,
            )
        monkeypatch.setattr(ba_mod, "_heal_one", _stub_heal)

        events = list(run_bulk_apply(
            conn=conn, settings=settings, reader=reader,
            repair_state=_drafts_for(f),
            detect_fn=lambda c, e: (f,),
        ))

        types = [e.EVENT for e in events]
        # batch_started, then ONLY cleanup group, then batch_done.
        # No group_started for schema or labels.
        group_started_groups = [
            e.group for e in events if e.EVENT == "group_started"
        ]
        assert group_started_groups == ["cleanup"]
        assert types[0] == "batch_started"
        assert types[-1] == "batch_done"
        assert events[-1].outcome == "success"

    def test_mid_group_failure_aborts_remaining_groups(
        self, tmp_path, conn, monkeypatch,
    ):
        """Failure mid-Group-1 → group_committed with failed>0,
        groups N+1 never start. With at least one succeeded
        finding earlier in the same group, batch_done(partial)
        — without one, batch_done(failed)."""
        paths = _make_ledger(tmp_path)
        reader = _Reader(paths["main"])
        settings = _Settings(paths)

        # Two schema findings: first succeeds, second fails.
        # One cleanup finding that should never run.
        schema_ok = _finding("schema_drift", "sqlite:50:51", id_suffix="-ok")
        schema_fail = _finding("schema_drift", "sqlite:51:53", id_suffix="-fail")
        cleanup_f = _finding("legacy_path", "Assets:Vehicles:Foo")

        from lamella.features.recovery import bulk_apply as ba_mod

        def _stub_heal(finding, **_kwargs):
            if finding.id == schema_fail.id:
                return HealResult(
                    success=False,
                    message="migration broke",
                    files_touched=(),
                    finding_id=finding.id,
                )
            return HealResult(
                success=True,
                message="ok",
                files_touched=(),
                finding_id=finding.id,
            )
        monkeypatch.setattr(ba_mod, "_heal_one", _stub_heal)

        events = list(run_bulk_apply(
            conn=conn, settings=settings, reader=reader,
            repair_state=_drafts_for(schema_ok, schema_fail, cleanup_f),
            detect_fn=lambda c, e: (schema_ok, schema_fail, cleanup_f),
        ))

        types = [e.EVENT for e in events]
        assert "finding_failed" in types
        assert "finding_applied" in types
        # Schema group_started + group_committed fired.
        schema_starts = [e for e in events if e.EVENT == "group_started" and e.group == "schema"]
        assert len(schema_starts) == 1
        # Cleanup group_started never fired.
        cleanup_starts = [e for e in events if e.EVENT == "group_started" and e.group == "cleanup"]
        assert cleanup_starts == []
        # batch_done says partial (≥1 applied, ≥1 failed).
        assert events[-1].EVENT == "batch_done"
        assert events[-1].outcome == "partial"
        # Group committed event for schema with applied=1, failed=1.
        gc = [e for e in events if e.EVENT == "group_committed" and e.group == "schema"][0]
        assert gc.applied == 1
        assert gc.failed == 1

    def test_full_group_failure_with_no_successes_is_failed_not_partial(
        self, tmp_path, conn, monkeypatch,
    ):
        """Single failing finding with no preceding successes →
        batch_done(failed). 'partial' requires that something at
        least worked."""
        paths = _make_ledger(tmp_path)
        reader = _Reader(paths["main"])
        settings = _Settings(paths)

        only_fail = _finding("schema_drift", "sqlite:50:53")

        from lamella.features.recovery import bulk_apply as ba_mod

        def _stub_heal(finding, **_kwargs):
            return HealResult(
                success=False,
                message="boom",
                files_touched=(),
                finding_id=finding.id,
            )
        monkeypatch.setattr(ba_mod, "_heal_one", _stub_heal)

        events = list(run_bulk_apply(
            conn=conn, settings=settings, reader=reader,
            repair_state=_drafts_for(only_fail),
            detect_fn=lambda c, e: (only_fail,),
        ))

        assert events[-1].EVENT == "batch_done"
        assert events[-1].outcome == "failed"

    def test_re_detection_between_groups_drops_resolved_findings(
        self, tmp_path, conn, monkeypatch,
    ):
        """A finding the user marked apply that's no longer present
        in the post-prior-group detector output silently drops from
        the batch. Per locked policy (a)."""
        paths = _make_ledger(tmp_path)
        reader = _Reader(paths["main"])
        settings = _Settings(paths)

        schema_f = _finding("schema_drift", "sqlite:50:53")
        cleanup_f1 = _finding("legacy_path", "Assets:Vehicles:Foo")
        cleanup_f2 = _finding("legacy_path", "Assets:Vehicles:Bar")

        # First detect_all call: returns all three. Second call
        # (between groups): returns only the schema (already
        # applied) and cleanup_f2 (cleanup_f1 silently dropped).
        call_count = {"n": 0}

        def _detect_fn(c, e):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return (schema_f, cleanup_f1, cleanup_f2)
            return (schema_f, cleanup_f2)

        from lamella.features.recovery import bulk_apply as ba_mod

        def _stub_heal(finding, **_kwargs):
            return HealResult(
                success=True, message="ok",
                files_touched=(), finding_id=finding.id,
            )
        monkeypatch.setattr(ba_mod, "_heal_one", _stub_heal)

        events = list(run_bulk_apply(
            conn=conn, settings=settings, reader=reader,
            repair_state=_drafts_for(schema_f, cleanup_f1, cleanup_f2),
            detect_fn=_detect_fn,
        ))

        # Re-detection happened — call_count >= 2.
        assert call_count["n"] >= 2
        # Cleanup group ran with one finding (cleanup_f2 only).
        cleanup_started = [
            e for e in events
            if e.EVENT == "group_started" and e.group == "cleanup"
        ]
        assert len(cleanup_started) == 1
        assert cleanup_started[0].findings == 1

    def test_re_detection_ignores_newly_surfaced_findings(
        self, tmp_path, conn, monkeypatch,
    ):
        """Locked policy (a): findings that newly appear between
        groups don't auto-join the batch — they render on the next
        page load."""
        paths = _make_ledger(tmp_path)
        reader = _Reader(paths["main"])
        settings = _Settings(paths)

        schema_f = _finding("schema_drift", "sqlite:50:53")
        new_cleanup = _finding("legacy_path", "Assets:Vehicles:NewlyAppeared")

        call_count = {"n": 0}

        def _detect_fn(c, e):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return (schema_f,)  # initial: only schema
            return (new_cleanup,)  # post-schema: a new finding shows up

        from lamella.features.recovery import bulk_apply as ba_mod

        def _stub_heal(finding, **_kwargs):
            return HealResult(
                success=True, message="ok",
                files_touched=(), finding_id=finding.id,
            )
        monkeypatch.setattr(ba_mod, "_heal_one", _stub_heal)

        events = list(run_bulk_apply(
            conn=conn, settings=settings, reader=reader,
            # schema_f is in drafts; new_cleanup is NOT (the user
            # never saw it at page-render time).
            repair_state=_drafts_for(schema_f),
            detect_fn=_detect_fn,
        ))

        # Cleanup group did not start — the new finding was not
        # composed into the batch.
        cleanup_starts = [
            e for e in events
            if e.EVENT == "group_started" and e.group == "cleanup"
        ]
        assert cleanup_starts == []

    def test_initial_detect_finding_not_in_drafts_is_ignored(
        self, tmp_path, conn, monkeypatch,
    ):
        """Closed-world batch composition: a finding present at
        initial detect but absent from ``repair_state['findings']``
        is ignored. Mirrors the between-groups locked policy (a)
        and is what makes Resume durable — a Group-2 failure that
        forces a re-visit replays the same composition without
        sweeping in everything that has surfaced since."""
        paths = _make_ledger(tmp_path)
        reader = _Reader(paths["main"])
        settings = _Settings(paths)

        in_drafts = _finding("schema_drift", "sqlite:50:51", id_suffix="-in")
        not_in_drafts = _finding("legacy_path", "Assets:Vehicles:Surprise")

        from lamella.features.recovery import bulk_apply as ba_mod

        def _stub_heal(finding, **_kwargs):
            return HealResult(
                success=True, message="ok",
                files_touched=(), finding_id=finding.id,
            )
        monkeypatch.setattr(ba_mod, "_heal_one", _stub_heal)

        events = list(run_bulk_apply(
            conn=conn, settings=settings, reader=reader,
            # Only in_drafts has an explicit decision.
            repair_state=_drafts_for(in_drafts),
            detect_fn=lambda c, e: (in_drafts, not_in_drafts),
        ))

        applied = [e for e in events if e.EVENT == "finding_applied"]
        # Only the explicitly-drafted finding ran.
        assert {e.finding_id for e in applied} == {in_drafts.id}
        # No cleanup group_started — the surprise finding was
        # filtered out at composition.
        cleanup_starts = [
            e for e in events
            if e.EVENT == "group_started" and e.group == "cleanup"
        ]
        assert cleanup_starts == []

    def test_unknown_category_routes_to_finding_failed(
        self, tmp_path, conn,
    ):
        """A Finding category with no registered heal action
        surfaces as finding_failed rather than crashing the
        orchestrator."""
        paths = _make_ledger(tmp_path)
        reader = _Reader(paths["main"])
        settings = _Settings(paths)

        bogus = _finding("unset_config", "some.key")  # mapped to labels
        events = list(run_bulk_apply(
            conn=conn, settings=settings, reader=reader,
            repair_state=_drafts_for(bogus),
            detect_fn=lambda c, e: (bogus,),
        ))

        failed = [e for e in events if e.EVENT == "finding_failed"]
        assert len(failed) == 1
        assert "no heal action registered" in failed[0].message

    def test_edit_payload_overrides_canonical_for_legacy_path(
        self, tmp_path, conn, monkeypatch,
    ):
        """User edits a legacy_path canonical destination; the
        heal action receives a Finding with the edited
        proposed_fix.

        Phase 8 step 8: pre-flight validation runs before any group;
        stub the destination guard so this test exercises the
        orchestrator-passes-edits-through path rather than the
        pre-flight reject path."""
        paths = _make_ledger(tmp_path)
        reader = _Reader(paths["main"])
        settings = _Settings(paths)

        f = _finding("legacy_path", "Assets:Vehicles:Foo")
        captured: list[Finding] = []

        from lamella.features.recovery import bulk_apply as ba_mod

        def _stub_heal(finding, **_kwargs):
            captured.append(finding)
            return HealResult(
                success=True, message="ok",
                files_touched=(), finding_id=finding.id,
            )
        monkeypatch.setattr(ba_mod, "_heal_one", _stub_heal)

        # Bypass the Phase 8 step 8 pre-flight guard — this test is
        # about the orchestrator's edit-payload pass-through, not
        # destination validation.
        monkeypatch.setattr(
            "lamella.features.recovery.findings.legacy_paths."
            "_passes_destination_guards",
            lambda canonical, opened: True,
        )

        events = list(run_bulk_apply(
            conn=conn, settings=settings, reader=reader,
            repair_state={
                "findings": {
                    f.id: {
                        "action": "edit",
                        "edit_payload": {"canonical": "Assets:Acme:Vehicle:Foo"},
                    },
                },
                "applied_history": [],
            },
            detect_fn=lambda c, e: (f,),
        ))
        # The stub captured the finding the orchestrator dispatched.
        # The edit swap happens in real _heal_one which we
        # monkeypatched out — so this test only verifies the
        # orchestrator passes the repair_state entry through.
        # Coverage for the actual swap logic is below.
        assert len(captured) == 1


class TestEditPayloadSwap:
    """Unit tests for ``_heal_one``'s edit_payload swap logic.
    Tested separately so the orchestrator-level tests can stub
    _heal_one without losing coverage on the swap."""

    def test_swap_replaces_canonical_for_legacy_path(self, tmp_path, conn, monkeypatch):
        from lamella.features.recovery import bulk_apply as ba_mod

        captured: list[Finding] = []

        def _stub_heal_legacy_path(finding, **_kwargs):
            captured.append(finding)
            return HealResult(
                success=True, message="ok",
                files_touched=(), finding_id=finding.id,
            )

        # Patch the heal action that _heal_one dispatches to.
        monkeypatch.setattr(
            "lamella.features.recovery.heal.heal_legacy_path",
            _stub_heal_legacy_path,
        )

        f = Finding(
            id="legacy_path:xyz123456789",
            category="legacy_path",
            severity="warning",
            target_kind="account",
            target="Assets:Vehicles:Foo",
            summary="x", detail=None,
            proposed_fix=fix_payload(
                action="move", canonical="Assets:Personal:Vehicle:Foo",
            ),
            alternatives=(),
            confidence="high", source="x",
        )

        ba_mod._heal_one(
            f,
            conn=conn, settings=None, reader=None, bean_check=None,
            repair_state={
                "action": "edit",
                "edit_payload": {"canonical": "Assets:Acme:Vehicle:Foo"},
            },
        )

        assert len(captured) == 1
        passed = captured[0]
        # Swap landed: proposed_fix carries the new canonical.
        assert passed.proposed_fix_dict["canonical"] == "Assets:Acme:Vehicle:Foo"

    def test_no_swap_when_action_is_apply(self, tmp_path, conn, monkeypatch):
        """action='apply' (the default) doesn't trigger the swap
        even if edit_payload is present — that combination would
        be a draft-state inconsistency we don't act on."""
        from lamella.features.recovery import bulk_apply as ba_mod

        captured: list[Finding] = []

        def _stub_heal_legacy_path(finding, **_kwargs):
            captured.append(finding)
            return HealResult(
                success=True, message="ok",
                files_touched=(), finding_id=finding.id,
            )

        monkeypatch.setattr(
            "lamella.features.recovery.heal.heal_legacy_path",
            _stub_heal_legacy_path,
        )

        original_canonical = "Assets:Personal:Vehicle:Foo"
        f = Finding(
            id="legacy_path:xyz123456789",
            category="legacy_path",
            severity="warning",
            target_kind="account",
            target="Assets:Vehicles:Foo",
            summary="x", detail=None,
            proposed_fix=fix_payload(
                action="move", canonical=original_canonical,
            ),
            alternatives=(),
            confidence="high", source="x",
        )

        ba_mod._heal_one(
            f,
            conn=conn, settings=None, reader=None, bean_check=None,
            repair_state={
                "action": "apply",
                "edit_payload": {"canonical": "Assets:Acme:Vehicle:Foo"},
            },
        )

        passed = captured[0]
        assert passed.proposed_fix_dict["canonical"] == original_canonical


# ---------------------------------------------------------------------------
# Phase 6.1.3.5 — atomic-group rollback semantics for Groups 2+3
# ---------------------------------------------------------------------------


class TestAtomicGroupSemantics:
    """The cleanup group runs through ``_run_group_atomic``: any
    per-finding failure rolls the whole group back via the outer
    snapshot envelope. Buffered FindingApplied events for
    preceding successes are dropped so the SSE stream's invariant
    holds — every emitted FindingApplied corresponds to a write
    that committed."""

    def test_atomic_cleanup_group_buffers_then_flushes_on_clean_commit(
        self, tmp_path, conn, monkeypatch,
    ):
        """All-success path: the buffered FindingApplied events get
        flushed on clean commit, in input order, before
        GroupCommitted."""
        paths = _make_ledger(tmp_path)
        reader = _Reader(paths["main"])
        settings = _Settings(paths)

        f1 = _finding("legacy_path", "Assets:Vehicles:A")
        f2 = _finding("legacy_path", "Assets:Vehicles:B")

        from lamella.features.recovery import bulk_apply as ba_mod

        def _stub_heal(finding, **_kwargs):
            return HealResult(
                success=True, message="ok",
                files_touched=(), finding_id=finding.id,
            )
        monkeypatch.setattr(ba_mod, "_heal_one", _stub_heal)

        events = list(run_bulk_apply(
            conn=conn, settings=settings, reader=reader,
            repair_state=_drafts_for(f1, f2),
            detect_fn=lambda c, e: (f1, f2),
        ))

        # Clean cleanup commit: group_started, finding_applied(f1),
        # finding_applied(f2), group_committed, batch_done.
        cleanup_evs = [
            e for e in events
            if getattr(e, "group", None) == "cleanup"
            or e.EVENT == "batch_done"
        ]
        types = [e.EVENT for e in cleanup_evs]
        assert "group_rolled_back" not in types
        assert types == [
            "group_started",
            "finding_applied",
            "finding_applied",
            "group_committed",
            "batch_done",
        ]
        gc = next(e for e in events if e.EVENT == "group_committed" and e.group == "cleanup")
        assert gc.applied == 2
        assert gc.failed == 0

    def test_atomic_cleanup_group_rolls_back_on_first_failure(
        self, tmp_path, conn, monkeypatch,
    ):
        """f1 succeeds, f2 fails → buffered FindingApplied(f1) is
        dropped (its writes were rolled back), FindingFailed(f2) is
        emitted, GroupRolledBack closes the group."""
        paths = _make_ledger(tmp_path)
        reader = _Reader(paths["main"])
        settings = _Settings(paths)

        f1 = _finding("legacy_path", "Assets:Vehicles:A")
        f2 = _finding("legacy_path", "Assets:Vehicles:B")

        from lamella.features.recovery import bulk_apply as ba_mod

        def _stub_heal(finding, **_kwargs):
            if finding.id == f2.id:
                return HealResult(
                    success=False, message="contrived failure",
                    files_touched=(), finding_id=finding.id,
                )
            return HealResult(
                success=True, message="ok",
                files_touched=(), finding_id=finding.id,
            )
        monkeypatch.setattr(ba_mod, "_heal_one", _stub_heal)

        events = list(run_bulk_apply(
            conn=conn, settings=settings, reader=reader,
            repair_state=_drafts_for(f1, f2),
            detect_fn=lambda c, e: (f1, f2),
        ))

        # No FindingApplied for f1 — the atomic-group runner
        # buffered + dropped it on rollback.
        applied = [e for e in events if e.EVENT == "finding_applied"]
        assert applied == [], (
            "atomic-group rollback must NOT leak buffered "
            "FindingApplied events to the stream"
        )
        # FindingFailed for the trigger finding is emitted.
        failed = [e for e in events if e.EVENT == "finding_failed"]
        assert len(failed) == 1
        assert failed[0].finding_id == f2.id
        assert "contrived failure" in failed[0].message
        # GroupRolledBack closes the group.
        rb = [e for e in events if e.EVENT == "group_rolled_back"]
        assert len(rb) == 1
        assert rb[0].group == "cleanup"
        assert "contrived failure" in rb[0].reason
        # batch_done outcome is "failed" (no group ran without
        # failure, no preceding group committed since cleanup is
        # the first non-empty group in this batch).
        bd = events[-1]
        assert bd.EVENT == "batch_done"
        assert bd.outcome == "failed"

    def test_atomic_cleanup_rolls_back_on_heal_raised(
        self, tmp_path, conn, monkeypatch,
    ):
        """A heal that raises (not just returns success=False) also
        triggers the atomic rollback path. The exception type goes
        into the GroupRolledBack reason."""
        paths = _make_ledger(tmp_path)
        reader = _Reader(paths["main"])
        settings = _Settings(paths)

        f1 = _finding("legacy_path", "Assets:Vehicles:A")
        f2 = _finding("legacy_path", "Assets:Vehicles:B")

        from lamella.features.recovery import bulk_apply as ba_mod

        def _stub_heal(finding, **_kwargs):
            if finding.id == f2.id:
                raise RuntimeError("deliberate boom")
            return HealResult(
                success=True, message="ok",
                files_touched=(), finding_id=finding.id,
            )
        monkeypatch.setattr(ba_mod, "_heal_one", _stub_heal)

        events = list(run_bulk_apply(
            conn=conn, settings=settings, reader=reader,
            repair_state=_drafts_for(f1, f2),
            detect_fn=lambda c, e: (f1, f2),
        ))

        # Same shape as the success=False trigger path — buffered
        # FindingApplied(f1) dropped, FindingFailed(f2) emitted,
        # GroupRolledBack closes.
        assert [e for e in events if e.EVENT == "finding_applied"] == []
        rb = [e for e in events if e.EVENT == "group_rolled_back"]
        assert len(rb) == 1
        # Exception sanitization (Phase 6 audit) fires here too —
        # message names the type, not str(exc) which would leak
        # OSError paths.
        assert "RuntimeError" in rb[0].reason

    def test_schema_group_stays_best_effort_when_finding_fails(
        self, tmp_path, conn, monkeypatch,
    ):
        """Schema (Group 1) is NOT in _ATOMIC_GROUPS — its failures
        are partial-success-OK by physics (DDL can't roll back).
        Verify the schema group still emits FindingApplied for the
        succeeding migrations even if a later one fails, and emits
        GroupCommitted (not GroupRolledBack) with failed > 0."""
        paths = _make_ledger(tmp_path)
        reader = _Reader(paths["main"])
        settings = _Settings(paths)

        s1 = _finding("schema_drift", "sqlite:50:51")
        s2 = _finding("schema_drift", "sqlite:51:52", id_suffix="-fail")

        from lamella.features.recovery import bulk_apply as ba_mod

        def _stub_heal(finding, **_kwargs):
            if finding.id == s2.id:
                return HealResult(
                    success=False, message="migration broke",
                    files_touched=(), finding_id=finding.id,
                )
            return HealResult(
                success=True, message="ok",
                files_touched=(), finding_id=finding.id,
            )
        monkeypatch.setattr(ba_mod, "_heal_one", _stub_heal)

        events = list(run_bulk_apply(
            conn=conn, settings=settings, reader=reader,
            repair_state=_drafts_for(s1, s2),
            detect_fn=lambda c, e: (s1, s2),
        ))

        # FindingApplied for s1 is emitted live (no buffering for
        # best-effort groups).
        applied = [e for e in events if e.EVENT == "finding_applied"]
        assert [e.finding_id for e in applied] == [s1.id]
        # GroupCommitted with applied=1, failed=1 — the best-effort
        # signal for partial success. No GroupRolledBack.
        gc = next(e for e in events if e.EVENT == "group_committed" and e.group == "schema")
        assert gc.applied == 1
        assert gc.failed == 1
        assert [e for e in events if e.EVENT == "group_rolled_back"] == []


# ---------------------------------------------------------------------------
# Phase 8 step 8 — pre-flight edit_payload validation
# ---------------------------------------------------------------------------


class TestPreflightEditPayloadValidation:
    """Pre-flight validation runs against the user's composed
    ``repair_state["findings"]`` AFTER BatchStarted but BEFORE any
    GroupStarted. Stale edits surface as FindingFailed events
    emitted outside any group, followed by BatchDone(failed)
    without starting any group. Fails the whole batch in one pass
    so the user gets a complete list of stale edits to fix —
    vs the prior fail-mid-batch behavior where only the first
    stale edit surfaced before the orchestrator stopped."""

    def test_valid_edit_passes_preflight_and_runs_normally(
        self, tmp_path, conn, monkeypatch,
    ):
        """An edit with a canonical that passes the destination
        guard runs through to the heal action — pre-flight is
        transparent for valid edits."""
        paths = _make_ledger(tmp_path)
        reader = _Reader(paths["main"])
        settings = _Settings(paths)
        f = _finding("legacy_path", "Assets:Vehicles:Foo")

        monkeypatch.setattr(
            "lamella.features.recovery.findings.legacy_paths."
            "_passes_destination_guards",
            lambda canonical, opened: True,
        )
        from lamella.features.recovery import bulk_apply as ba_mod
        monkeypatch.setattr(ba_mod, "_heal_one", lambda finding, **_: HealResult(
            success=True, message="ok",
            files_touched=(), finding_id=finding.id,
        ))

        events = list(run_bulk_apply(
            conn=conn, settings=settings, reader=reader,
            repair_state={
                "findings": {f.id: {
                    "action": "edit",
                    "edit_payload": {"canonical": "Assets:Acme:Vehicle:Foo"},
                }},
                "applied_history": [],
            },
            detect_fn=lambda c, e: (f,),
        ))
        # Group flow runs normally — GroupStarted fires.
        assert any(e.EVENT == "group_started" for e in events)
        assert any(e.EVENT == "finding_applied" for e in events)
        assert events[-1].EVENT == "batch_done"
        assert events[-1].outcome == "success"

    def test_stale_edit_fails_preflight_no_group_starts(
        self, tmp_path, conn, monkeypatch,
    ):
        """A stale edit (canonical no longer passes the guard)
        emits FindingFailed BEFORE any GroupStarted, then
        BatchDone(failed). No group runs — preceding writes
        zero."""
        paths = _make_ledger(tmp_path)
        reader = _Reader(paths["main"])
        settings = _Settings(paths)
        f = _finding("legacy_path", "Assets:Vehicles:Stale")

        monkeypatch.setattr(
            "lamella.features.recovery.findings.legacy_paths."
            "_passes_destination_guards",
            lambda canonical, opened: False,
        )
        # Heal stub — should NEVER be called because pre-flight
        # rejects before group iteration.
        called: list[Finding] = []
        from lamella.features.recovery import bulk_apply as ba_mod
        def _trap(finding, **_):
            called.append(finding)
            return HealResult(
                success=True, message="x",
                files_touched=(), finding_id=finding.id,
            )
        monkeypatch.setattr(ba_mod, "_heal_one", _trap)

        events = list(run_bulk_apply(
            conn=conn, settings=settings, reader=reader,
            repair_state={
                "findings": {f.id: {
                    "action": "edit",
                    "edit_payload": {"canonical": "Assets:Bogus:Path"},
                }},
                "applied_history": [],
            },
            detect_fn=lambda c, e: (f,),
        ))

        # Heal action never invoked.
        assert called == [], (
            "pre-flight should have rejected the stale edit before "
            "any heal action ran"
        )
        # Event order: BatchStarted → FindingFailed → BatchDone.
        # No GroupStarted.
        types = [e.EVENT for e in events]
        assert "group_started" not in types
        assert types == ["batch_started", "finding_failed", "batch_done"]
        # batch_done outcome is failed.
        assert events[-1].outcome == "failed"
        # The pre-flight failure message describes the move-target
        # guard rejection (sanitized — no raw path-leak shape).
        ff = next(e for e in events if e.EVENT == "finding_failed")
        assert "move-target guards" in ff.message
        # BatchDone summary carries the per-finding preflight list.
        assert "preflight_failures" in events[-1].summary
        assert len(events[-1].summary["preflight_failures"]) == 1

    def test_multiple_stale_edits_all_surface_in_one_pass(
        self, tmp_path, conn, monkeypatch,
    ):
        """Pre-flight collects EVERY stale edit, not just the
        first. The user gets a complete list to fix in one
        round-trip — this is the contract that motivates pre-
        flight existing in the first place."""
        paths = _make_ledger(tmp_path)
        reader = _Reader(paths["main"])
        settings = _Settings(paths)
        f1 = _finding("legacy_path", "Assets:Vehicles:A")
        f2 = _finding("legacy_path", "Assets:Vehicles:B")
        f3 = _finding("legacy_path", "Assets:Vehicles:C")

        monkeypatch.setattr(
            "lamella.features.recovery.findings.legacy_paths."
            "_passes_destination_guards",
            lambda canonical, opened: False,
        )

        events = list(run_bulk_apply(
            conn=conn, settings=settings, reader=reader,
            repair_state={
                "findings": {
                    f1.id: {"action": "edit",
                            "edit_payload": {"canonical": "Assets:X:1"}},
                    f2.id: {"action": "edit",
                            "edit_payload": {"canonical": "Assets:Y:2"}},
                    f3.id: {"action": "edit",
                            "edit_payload": {"canonical": "Assets:Z:3"}},
                },
                "applied_history": [],
            },
            detect_fn=lambda c, e: (f1, f2, f3),
        ))

        failed = [e for e in events if e.EVENT == "finding_failed"]
        assert len(failed) == 3, (
            "pre-flight should surface ALL stale edits in one pass, "
            f"got {len(failed)}: {[e.finding_id for e in failed]}"
        )
        failed_ids = {e.finding_id for e in failed}
        assert failed_ids == {f1.id, f2.id, f3.id}
        assert events[-1].outcome == "failed"

    def test_empty_canonical_edit_fails_preflight(
        self, tmp_path, conn, monkeypatch,
    ):
        """A draft with action='edit' and empty canonical (e.g. a
        partial edit the user never finished) is rejected at
        pre-flight with a specific message. Doesn't even reach the
        guard — the empty check fires first."""
        paths = _make_ledger(tmp_path)
        reader = _Reader(paths["main"])
        settings = _Settings(paths)
        f = _finding("legacy_path", "Assets:Vehicles:Empty")

        events = list(run_bulk_apply(
            conn=conn, settings=settings, reader=reader,
            repair_state={
                "findings": {f.id: {
                    "action": "edit",
                    "edit_payload": {"canonical": "   "},  # blank
                }},
                "applied_history": [],
            },
            detect_fn=lambda c, e: (f,),
        ))
        ff = next(e for e in events if e.EVENT == "finding_failed")
        assert "empty" in ff.message.lower()
        assert events[-1].outcome == "failed"

    def test_action_apply_skips_preflight_validation(
        self, tmp_path, conn, monkeypatch,
    ):
        """Pre-flight only validates findings with action='edit'.
        action='apply' uses the detector's proposed_fix as-is —
        no edit_payload to validate."""
        paths = _make_ledger(tmp_path)
        reader = _Reader(paths["main"])
        settings = _Settings(paths)
        f = _finding("legacy_path", "Assets:Vehicles:Foo")

        # Force the guard to fail — but since this finding's action
        # is 'apply' (not 'edit'), pre-flight should skip it.
        monkeypatch.setattr(
            "lamella.features.recovery.findings.legacy_paths."
            "_passes_destination_guards",
            lambda canonical, opened: False,
        )
        from lamella.features.recovery import bulk_apply as ba_mod
        monkeypatch.setattr(ba_mod, "_heal_one", lambda finding, **_: HealResult(
            success=True, message="ok",
            files_touched=(), finding_id=finding.id,
        ))

        events = list(run_bulk_apply(
            conn=conn, settings=settings, reader=reader,
            repair_state=_drafts_for(f, action="apply"),
            detect_fn=lambda c, e: (f,),
        ))
        # Group ran normally — pre-flight didn't reject.
        assert any(e.EVENT == "group_started" for e in events)
        assert events[-1].outcome == "success"

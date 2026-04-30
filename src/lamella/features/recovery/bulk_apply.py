# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Bulk-apply orchestrator — Phase 6.1.3 of /setup/recovery.

Takes a user-composed batch (Findings + per-finding actions from
``setup_repair_state``) and applies it sequentially across three
groups: schema → labels → cleanup. Per the Phase 6.1.3 sub-freeze,
groups commit independently, with per-finding atomicity inside
each group provided by the existing heal-action envelopes.

**Group ordering rationale (locked, not commutable).**

The sequence schema → labels → cleanup matters because each
group's preconditions depend on the prior group's postconditions:

1. **Schema first.** Every later heal action assumes the current
   schema. A label-write directive that doesn't exist pre-
   migration would fail; a cleanup move that depends on a
   column added in a pending migration would reference a missing
   field.
2. **Labels before cleanup.** Cleanup heal actions like
   move-and-close consult ``_passes_destination_guards``, whose
   answer depends on the kind/entity labels of the canonical
   destination's parent path being settled. Cleanup before
   labels would mean the guards see NULL-kind parents and refuse
   moves the user definitely wants.

A future refactor that sees three groups and assumes they
commute will reintroduce a class of bugs the spec freeze
deliberately closed. Don't reorder.

**Per-axis atomicity (locked).**

- Group 1 (schema): best-effort, partial-success-OK. SQLite DDL
  committed by ``db.migrate``'s inner ``BEGIN/COMMIT`` cannot be
  rolled back by an outer transaction. If migration #3 of 5
  fails, migrations #1+#2 stay committed; emit ``group_committed``
  with ``failed > 0`` rather than ``group_rolled_back``.
- Groups 2+3 (labels, cleanup): atomic via outer envelope as of
  Phase 6.1.3.5. The orchestrator opens one ``with_bean_snapshot``
  per group covering the worst-case declared path set
  (connector-owned files + main.bean + every loaded ledger
  source file). Per-finding heals run with ``bulk_context``
  threaded through; they SKIP their own per-call snapshot wrap
  and write directly. On any per-finding failure (raise or
  HealRefused), the orchestrator's outer envelope restores every
  declared file byte-identically and the orchestrator emits
  ``group_rolled_back`` with the offending finding's message as
  ``reason``. Per-finding ``finding_applied`` events are buffered
  during the run and only flushed on a clean group commit — so
  the SSE stream's invariant holds: every emitted
  ``finding_applied`` corresponds to a write that committed.

**Event stream (locked vocabulary).**

The orchestrator yields ``BatchEvent`` dataclass instances. The
JobRunner adapter serializes them via ``event.to_emit()`` into
``ctx.emit(message, outcome, detail)`` calls; the existing
``/jobs/{id}/stream`` SSE route surfaces them to the frontend.

Seven event variants per the locked spec, plus ``summary`` and
``category`` payload extensions on ``finding_applied`` /
``finding_failed`` so the UI can render narratives without
re-deriving state.

``GroupCommitted`` and ``GroupRolledBack`` have non-overlapping
semantics — never collapse them into one event:

- ``GroupCommitted(applied=A, failed=F)`` — the group ran and
  some/all heals committed. ``failed > 0`` is the canonical
  signal for *best-effort partial success*. This is the only
  shape Group 1 (schema, best-effort by physics) emits on
  failure; a future reader should not interpret the absence of
  ``GroupRolledBack`` for Group 1 as a missing event.
- ``GroupRolledBack(group, reason, preserved_groups)`` — the
  group did NOT commit any work. Two firing paths today:
  re-detection between groups raised (so the next group never
  starts), and (Phase 6.1.3.5) outer-envelope rollback for
  Groups 2+3 atomic failure. ``preserved_groups`` lists the
  prior groups that committed; those stay committed.

**Closed-world batch composition (resume durability).**

``repair_state["findings"]`` is the authoritative batch
composition. The orchestrator only runs findings explicitly
listed there — newly surfaced findings (post-page-render) are
ignored at initial detect, mirroring locked policy (a)'s
between-groups behavior. This makes Resume deterministic: a
Group-2 failure forces the user back to the page, and clicking
Apply Repairs again replays the same composition (intersected
with current detected set, so resolved findings drop) rather
than sweeping in everything that has surfaced since.

**edit_payload schema (locked per category).**

When ``repair_state["findings"][id]["action"] == "edit"``, the
``edit_payload`` blob shape is fixed per the finding's category.
Heal-action dispatch validates the payload before invoking the
heal. Today only ``legacy_path`` supports edits:

- ``legacy_path``: ``{"canonical": str}`` — overrides the
  detector's canonical destination. Validation runs in two
  places, both consulting ``_passes_destination_guards``:
  pre-flight (Phase 8 step 8) collects every stale-edit error
  for the whole batch BEFORE any group runs; the heal action's
  own re-validation is the second-line defense if the world
  shifts between pre-flight and apply (still surfaces as
  ``HealRefused`` mid-batch if so, though atomic-group rollback
  reverts the affected group cleanly per Phase 6.1.3.5).

Other categories ignore ``edit_payload`` and treat
``action='edit'`` as equivalent to ``action='apply'``. Future
categories adding edit support extend this table.
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, ClassVar, Iterator

from lamella.features.recovery.findings import detect_all
from lamella.features.recovery.heal.legacy_paths import HealRefused
from lamella.features.recovery.models import Finding, HealResult
from lamella.features.recovery.snapshot import (
    BeanSnapshotCheckError,
    with_bean_snapshot,
)


# Groups whose per-finding heals participate in an outer-envelope
# snapshot. Schema (Group 1) stays out: SQLite DDL is committed by
# db.migrate's inner BEGIN/COMMIT and can't be rolled back by an
# outer transaction. See module docstring "Per-axis atomicity".
_ATOMIC_GROUPS: frozenset[str] = frozenset({"labels", "cleanup"})


class _GroupRollbackTrigger(Exception):
    """Internal signal raised inside an atomic group's outer
    envelope to tear it down on the first per-finding failure.
    Carries the offending finding's id + message for the
    GroupRolledBack event the orchestrator emits."""

    def __init__(self, finding_id: str, reason: str):
        self.finding_id = finding_id
        self.reason = reason
        super().__init__(reason)


__all__ = [
    "BatchEvent",
    "BatchStarted",
    "GroupStarted",
    "FindingApplied",
    "FindingFailed",
    "GroupCommitted",
    "GroupRolledBack",
    "BatchDone",
    "BulkContext",
    "GroupOutcome",
    "BatchOutcome",
    "GROUPS",
    "categorize",
    "run_bulk_apply",
]


_LOG = logging.getLogger(__name__)


# --- group taxonomy --------------------------------------------------------


# Locked group order — see module docstring for the rationale.
# Categorization is by Finding.category; each category maps to
# exactly one group. New categories add an entry to ``CATEGORY_GROUP``
# below.
GROUPS: tuple[str, ...] = ("schema", "labels", "cleanup")

CATEGORY_GROUP: dict[str, str] = {
    # Group 1 — schema migrations.
    "schema_drift":      "schema",
    # Group 2 — label/kind/entity findings. Phase 6 doesn't ship
    # detectors for these yet; the mapping reserves the slot.
    "unlabeled_account": "labels",
    "unset_config":      "labels",
    # Group 3 — cleanup actions.
    "legacy_path":       "cleanup",
    "orphan_ref":        "cleanup",
    "missing_scaffold":  "cleanup",
    "missing_data_file": "cleanup",
}


def categorize(
    findings: tuple[Finding, ...],
) -> dict[str, list[Finding]]:
    """Partition findings into the three locked groups.

    Findings with an unknown category are placed in the cleanup
    group (defensive — better than silently dropping them).
    """
    out: dict[str, list[Finding]] = {g: [] for g in GROUPS}
    for f in findings:
        group = CATEGORY_GROUP.get(f.category, "cleanup")
        out[group].append(f)
    return out


# --- event vocabulary ------------------------------------------------------


@dataclass(frozen=True)
class BatchEvent:
    """Base for all bulk-apply events. Subclasses set ``EVENT`` to
    the locked SSE event-type literal. ``to_emit()`` returns the
    triple consumed by ``JobContext.emit``."""

    EVENT: ClassVar[str] = "<base>"

    def to_emit(self) -> tuple[str, str | None, dict[str, Any]]:
        """Translate to ``(message, outcome, detail)``. Detail
        carries the full event payload including the discriminator
        ``event`` field so the frontend can dispatch by type
        without inferring from message text."""
        detail: dict[str, Any] = {"event": self.EVENT}
        # Pull every dataclass field except those starting with _
        # into the detail. Subclasses are frozen + small enough
        # that this introspection is fine.
        from dataclasses import fields, asdict
        # asdict recursively converts nested dataclasses; we want
        # tuples to round-trip as lists for JSON-friendliness.
        payload = asdict(self)
        for k, v in payload.items():
            detail[k] = list(v) if isinstance(v, tuple) else v
        return (self._message(), self._outcome(), detail)

    def _message(self) -> str:
        return self.EVENT

    def _outcome(self) -> str | None:
        """JobContext outcome bucket — drives the modal's counter
        cells. Most events are ``info``; failures bump ``failure``."""
        return "info"


@dataclass(frozen=True)
class BatchStarted(BatchEvent):
    EVENT: ClassVar[str] = "batch_started"
    groups: tuple[str, ...]
    total_findings: int

    def _message(self) -> str:
        return f"Batch started: {self.total_findings} findings across {len(self.groups)} groups"


@dataclass(frozen=True)
class GroupStarted(BatchEvent):
    EVENT: ClassVar[str] = "group_started"
    group: str
    findings: int

    def _message(self) -> str:
        return f"Group '{self.group}' started ({self.findings} findings)"


@dataclass(frozen=True)
class FindingApplied(BatchEvent):
    EVENT: ClassVar[str] = "finding_applied"
    finding_id: str
    group: str
    summary: str
    category: str

    def _message(self) -> str:
        return f"Applied: {self.summary}"

    def _outcome(self) -> str:
        return "success"


@dataclass(frozen=True)
class FindingFailed(BatchEvent):
    EVENT: ClassVar[str] = "finding_failed"
    finding_id: str
    group: str
    summary: str
    category: str
    message: str

    def _message(self) -> str:
        return f"Failed: {self.summary} — {self.message}"

    def _outcome(self) -> str:
        return "failure"


@dataclass(frozen=True)
class GroupCommitted(BatchEvent):
    EVENT: ClassVar[str] = "group_committed"
    group: str
    applied: int
    failed: int

    def _message(self) -> str:
        return (
            f"Group '{self.group}' committed: "
            f"{self.applied} applied, {self.failed} failed"
        )


@dataclass(frozen=True)
class GroupRolledBack(BatchEvent):
    EVENT: ClassVar[str] = "group_rolled_back"
    group: str
    reason: str
    preserved_groups: tuple[str, ...]

    def _message(self) -> str:
        return f"Group '{self.group}' rolled back: {self.reason}"

    def _outcome(self) -> str:
        return "failure"


@dataclass(frozen=True)
class BatchDone(BatchEvent):
    EVENT: ClassVar[str] = "batch_done"
    outcome: str  # "success" | "partial" | "failed"
    summary: dict[str, Any]

    def _message(self) -> str:
        return f"Batch done: {self.outcome}"


# --- bulk context ----------------------------------------------------------


@dataclass
class BulkContext:
    """Passed to per-finding heal actions when they participate in
    an orchestrator-scoped envelope (Groups 2+3 atomicity).

    Phase 6.1.3 ships this dataclass shape but doesn't yet thread
    it into the heal-action call sites — that's the Phase 6.1.3.5
    extension. When ``None`` is passed (the only path Phase 6.1.3
    exercises), heal actions retain Phase 3/5 self-managed
    envelope behavior.
    """

    conn: sqlite3.Connection
    """Active SQLite connection. The orchestrator holds the outer
    transaction; per-finding heals write through this conn without
    starting their own ``BEGIN/COMMIT``."""

    declared_paths: list[Path] = field(default_factory=list)
    """Union of every per-finding heal's declared paths. The
    orchestrator's outer ``with_bean_snapshot`` envelope is opened
    against this set. Per-finding heals call ``add_paths()`` to
    extend; the orchestrator validates the set is fixed at envelope
    open and refuses extensions afterward."""

    group: str = ""
    """Current group label — heal actions can use this for logging
    or to short-circuit cross-group writes that shouldn't happen."""

    def add_paths(self, paths: tuple[Path, ...]) -> None:
        for p in paths:
            if p not in self.declared_paths:
                self.declared_paths.append(p)


# --- group / batch outcomes ------------------------------------------------


@dataclass(frozen=True)
class GroupOutcome:
    """What a group's runner returns. Captured per group so the
    orchestrator can decide whether to start the next group and
    so ``setup_repair_state.applied_history`` can be written."""

    group: str
    applied_finding_ids: tuple[str, ...]
    failed_finding_ids: tuple[str, ...]
    rolled_back: bool
    """True if the group rolled back atomically (Group 2+3 outer-
    envelope failure). False for Group 1 best-effort partial
    success even when ``failed`` > 0."""


@dataclass(frozen=True)
class BatchOutcome:
    """End-of-batch summary surfaced via ``BatchDone``."""

    outcome: str  # "success" | "partial" | "failed"
    groups: tuple[GroupOutcome, ...]
    total_applied: int
    total_failed: int


# --- the orchestrator ------------------------------------------------------


def run_bulk_apply(
    *,
    conn: sqlite3.Connection,
    settings: Any,
    reader: Any,
    repair_state: dict[str, Any],
    bean_check: Any | None = None,
    detect_fn: Any | None = None,
) -> Iterator[BatchEvent]:
    """Orchestrate a bulk-apply pass. Yields ``BatchEvent`` instances
    in the locked event order. Generator-shaped so the JobRunner
    worker can iterate and emit each via ``ctx.emit`` without the
    orchestrator ever touching the SSE plumbing.

    Args:
        conn: live SQLite connection.
        settings: ``lamella.core.config.Settings``.
        reader: a LedgerReader. Invalidated between groups so
            re-detection sees post-prior-group state.
        repair_state: parsed blob from ``read_repair_state(conn)``.
            The orchestrator reads ``findings`` (the user's draft
            decisions). It does NOT write back to the storage —
            ``applied_history`` updates are the route layer's job
            via ``write_repair_state`` after each group commits.
        bean_check: optional callable matching
            :data:`lamella.features.recovery.snapshot.BeanCheck`.
            Passed through to per-finding heals.
        detect_fn: optional override for the detector aggregator.
            Defaults to :func:`detect_all`. Tests pass a stub to
            inject specific findings between groups.

    Yields:
        BatchEvent instances per the locked sub-freeze schema.

    Failure semantics (per the locked sub-freeze):
        - Mid-Group-1 finding fails → preceding findings committed,
          ``GroupCommitted(failed > 0)``, subsequent groups don't
          start, ``BatchDone(outcome='partial')``.
        - Mid-Group-2 or Group-3 finding fails (best-effort in
          v1) → same as Group 1.
        - Detect_all between groups raises → ``GroupRolledBack``
          for the next group with reason='detection failed',
          ``BatchDone(outcome='failed')``.
    """
    if detect_fn is None:
        detect_fn = detect_all

    drafts = repair_state.get("findings", {})
    # Initial finding set composed at page render time — re-loaded
    # here in case the user backed-out of the page mid-edit.
    initial_entries = list(reader.load().entries)
    initial_findings = detect_fn(conn, initial_entries)

    # Filter to "user wants to apply" — the user may have set
    # action='dismiss' on some findings; those don't enter the
    # batch. action='edit' is treated as 'apply' (the heal action
    # consults the edit_payload).
    #
    # Closed-world batch composition: a finding without an explicit
    # draft entry is a finding the user never saw at page-render
    # time (newly surfaced between page-render and submit). Locked
    # policy (a) extends to initial detect — ignore it; the user
    # sees it on the next /setup/recovery page load. This makes
    # ``drafts`` the authoritative batch composition and Resume
    # durable: if a Group-2 failure later forces a re-visit, the
    # same drafts replay the same composition without sweeping in
    # findings that have surfaced since.
    selected: list[Finding] = []
    for f in initial_findings:
        decision = drafts.get(f.id)
        if decision is None:
            continue
        action = decision.get("action", "apply")
        if action in ("apply", "edit"):
            selected.append(f)

    grouped = categorize(tuple(selected))
    total_findings = sum(len(grouped[g]) for g in GROUPS)
    yield BatchStarted(groups=GROUPS, total_findings=total_findings)

    if total_findings == 0:
        yield BatchDone(
            outcome="success",
            summary={"applied": 0, "failed": 0, "groups": []},
        )
        return

    # Pre-flight edit_payload validation (Phase 8 step 8). For every
    # selected finding the user composed an edit on, validate the
    # edit_payload against current world state before any group
    # runs. Stale edits (e.g. a legacy_path canonical whose parent
    # path got closed between draft-save and apply-time) surface as
    # FindingFailed events emitted outside any group, followed by
    # BatchDone(failed) without starting any group. The user gets a
    # complete list of stale edits in one pass, fixes them, and
    # re-submits — vs the previous fail-mid-batch where only the
    # first stale edit surfaced before the orchestrator stopped.
    preflight_errors = _preflight_edit_payloads(
        selected=selected, drafts=drafts, reader=reader,
    )
    if preflight_errors:
        for finding, message in preflight_errors:
            yield FindingFailed(
                finding_id=finding.id,
                group=CATEGORY_GROUP.get(finding.category, "cleanup"),
                summary=finding.summary,
                category=finding.category,
                message=message,
            )
        yield BatchDone(
            outcome="failed",
            summary={
                "applied": 0,
                "failed": len(preflight_errors),
                "groups": [],
                "preflight_failures": [
                    {"finding_id": f.id, "message": m}
                    for f, m in preflight_errors
                ],
            },
        )
        return

    group_outcomes: list[GroupOutcome] = []
    abort_remaining = False

    for group_name in GROUPS:
        group_findings = grouped[group_name]

        # Re-detect between groups so each group sees the post-
        # prior-group state. Per locked-spec policy (a), new
        # findings are silently ignored for this batch; findings
        # the user marked apply that no longer exist drop silently.
        if group_outcomes:  # Skip re-detect on the first group.
            try:
                reader.invalidate()
                fresh_entries = list(reader.load().entries)
                fresh_findings = detect_fn(conn, fresh_entries)
            except Exception as exc:  # noqa: BLE001
                _LOG.exception("re-detection between groups failed")
                yield GroupRolledBack(
                    group=group_name,
                    reason=f"detection failed: {type(exc).__name__}",
                    preserved_groups=tuple(
                        o.group for o in group_outcomes
                    ),
                )
                group_outcomes.append(GroupOutcome(
                    group=group_name,
                    applied_finding_ids=(),
                    failed_finding_ids=(),
                    rolled_back=True,
                ))
                abort_remaining = True
                break

            fresh_ids = {f.id for f in fresh_findings}
            # Drop findings that no longer exist in the world.
            # New findings (in fresh_findings but not in selected)
            # are not added to the batch — locked policy (a).
            group_findings = [
                f for f in group_findings if f.id in fresh_ids
            ]
            # Refresh the Finding objects for the in-batch ones —
            # the proposed_fix may have changed if the world
            # shifted (e.g. a vehicle slug got registered between
            # composition and apply, so canonical destination is
            # now derivable). Use the fresh detector output.
            fresh_by_id = {f.id: f for f in fresh_findings}
            group_findings = [fresh_by_id[f.id] for f in group_findings]

        # Empty group → skip silently. Saves SSE traffic on the
        # small-batch case (locked-spec acceptance).
        if not group_findings:
            continue

        yield GroupStarted(group=group_name, findings=len(group_findings))

        if group_name in _ATOMIC_GROUPS:
            outcome, events = _run_group_atomic(
                group_name, group_findings,
                conn=conn, settings=settings, reader=reader,
                bean_check=bean_check, drafts=drafts,
                preserved_groups=tuple(o.group for o in group_outcomes),
            )
        else:
            outcome, events = _run_group_best_effort(
                group_name, group_findings,
                conn=conn, settings=settings, reader=reader,
                bean_check=bean_check, drafts=drafts,
            )
        for event in events:
            yield event
        group_outcomes.append(outcome)

        # Stop semantics: if the group rolled back OR (best-effort)
        # any finding failed, halt the batch. Preceding committed
        # groups stay committed; subsequent groups don't start.
        if outcome.rolled_back or outcome.failed_finding_ids:
            abort_remaining = True
            break

    # Compute final outcome.
    total_applied = sum(len(o.applied_finding_ids) for o in group_outcomes)
    total_failed = sum(len(o.failed_finding_ids) for o in group_outcomes)
    if total_failed == 0 and not abort_remaining:
        outcome = "success"
    elif total_applied > 0:
        outcome = "partial"
    else:
        outcome = "failed"

    yield BatchDone(
        outcome=outcome,
        summary={
            "applied": total_applied,
            "failed": total_failed,
            "groups": [
                {
                    "group": o.group,
                    "applied": list(o.applied_finding_ids),
                    "failed": list(o.failed_finding_ids),
                    "rolled_back": o.rolled_back,
                }
                for o in group_outcomes
            ],
        },
    )


# --- per-group runners -----------------------------------------------------


def _run_group_best_effort(
    group_name: str,
    group_findings: list[Finding],
    *,
    conn: sqlite3.Connection,
    settings: Any,
    reader: Any,
    bean_check: Any | None,
    drafts: dict[str, Any],
) -> tuple[GroupOutcome, list[BatchEvent]]:
    """Phase 6.1.3 best-effort group runner. Each finding heals
    independently inside its own per-call snapshot envelope; partial
    success across the group is OK. Used for the ``schema`` group
    where SQLite DDL atomicity can't be wrapped in an outer
    transaction (see module docstring)."""
    events: list[BatchEvent] = []
    applied_ids: list[str] = []
    failed_ids: list[str] = []

    for finding in group_findings:
        try:
            result = _heal_one(
                finding,
                conn=conn, settings=settings, reader=reader,
                bean_check=bean_check,
                repair_state=drafts.get(finding.id),
            )
        except HealRefused as exc:
            events.append(FindingFailed(
                finding_id=finding.id,
                group=group_name,
                summary=finding.summary,
                category=finding.category,
                message=str(exc),
            ))
            failed_ids.append(finding.id)
            continue
        except Exception as exc:  # noqa: BLE001
            _LOG.exception(
                "heal_one raised for finding %s", finding.id,
            )
            events.append(FindingFailed(
                finding_id=finding.id,
                group=group_name,
                summary=finding.summary,
                category=finding.category,
                message=f"unexpected {type(exc).__name__} — see server log",
            ))
            failed_ids.append(finding.id)
            continue

        if result.success:
            events.append(FindingApplied(
                finding_id=finding.id,
                group=group_name,
                summary=finding.summary,
                category=finding.category,
            ))
            applied_ids.append(finding.id)
        else:
            events.append(FindingFailed(
                finding_id=finding.id,
                group=group_name,
                summary=finding.summary,
                category=finding.category,
                message=result.message,
            ))
            failed_ids.append(finding.id)

    events.append(GroupCommitted(
        group=group_name,
        applied=len(applied_ids),
        failed=len(failed_ids),
    ))
    outcome = GroupOutcome(
        group=group_name,
        applied_finding_ids=tuple(applied_ids),
        failed_finding_ids=tuple(failed_ids),
        rolled_back=False,
    )
    return outcome, events


def _collect_atomic_declared_paths(
    settings: Any, reader: Any,
) -> list[Path]:
    """Worst-case declared path set for an atomic group's outer
    snapshot. Snapshots get restored byte-identically on rollback,
    so over-declaring is safe (just wastes a few file reads at
    snapshot time); under-declaring leaks unrolled-back writes.
    Strategy: every connector-owned file we know about + main.bean
    + every loaded ledger source file."""
    declared: list[Path] = []

    def _add(p):
        if p is None:
            return
        path = Path(p)
        if path not in declared:
            declared.append(path)

    _add(getattr(settings, "ledger_main", None))
    for attr in (
        "connector_accounts_path",
        "connector_overrides_path",
        "connector_links_path",
        "connector_config_path",
        "connector_rules_path",
        "connector_budgets_path",
    ):
        _add(getattr(settings, attr, None))

    try:
        for entry in reader.load().entries:
            meta = getattr(entry, "meta", None)
            if not isinstance(meta, dict):
                continue
            filename = meta.get("filename")
            if isinstance(filename, str) and not filename.startswith("<"):
                _add(filename)
    except Exception:  # noqa: BLE001
        # Reader failures shouldn't block the orchestrator from
        # opening the snapshot; the connector-owned set above is
        # the most-write-heavy slice and covers the common case.
        _LOG.warning(
            "atomic-group declared-path collection: reader.load failed; "
            "snapshot will cover connector-owned files only",
            exc_info=True,
        )

    return [p for p in declared if p.exists()]


def _run_group_atomic(
    group_name: str,
    group_findings: list[Finding],
    *,
    conn: sqlite3.Connection,
    settings: Any,
    reader: Any,
    bean_check: Any | None,
    drafts: dict[str, Any],
    preserved_groups: tuple[str, ...],
) -> tuple[GroupOutcome, list[BatchEvent]]:
    """Phase 6.1.3.5 atomic group runner. Wraps every per-finding
    heal in one outer ``with_bean_snapshot`` envelope; first failure
    raises to roll back every preceding heal in the group.

    Event-stream contract: per-finding events are buffered locally
    during the run and only released to the caller on a clean group
    commit. On rollback, the buffer is discarded and a single
    ``GroupRolledBack`` event is emitted instead — preserving the
    SSE invariant that every visible ``finding_applied`` corresponds
    to a write that committed.
    """
    declared = _collect_atomic_declared_paths(settings, reader)
    bctx = BulkContext(conn=conn, group=group_name)

    buffered: list[BatchEvent] = []
    applied_ids: list[str] = []
    failed_ids: list[str] = []
    rollback_reason: str | None = None
    main_bean = Path(getattr(settings, "ledger_main", "."))

    try:
        with with_bean_snapshot(
            declared,
            bean_check=bean_check,
            bean_check_path=main_bean,
        ) as snap:
            for finding in group_findings:
                try:
                    result = _heal_one(
                        finding,
                        conn=conn, settings=settings, reader=reader,
                        bean_check=bean_check,
                        repair_state=drafts.get(finding.id),
                        bulk_context=bctx,
                    )
                except HealRefused as exc:
                    raise _GroupRollbackTrigger(finding.id, str(exc))
                except Exception as exc:  # noqa: BLE001
                    _LOG.exception(
                        "heal_one raised for finding %s in atomic group %s",
                        finding.id, group_name,
                    )
                    raise _GroupRollbackTrigger(
                        finding.id,
                        f"unexpected {type(exc).__name__} — see server log",
                    )

                if not result.success:
                    raise _GroupRollbackTrigger(finding.id, result.message)

                buffered.append(FindingApplied(
                    finding_id=finding.id,
                    group=group_name,
                    summary=finding.summary,
                    category=finding.category,
                ))
                applied_ids.append(finding.id)
                for p in bctx.declared_paths:
                    if p in declared:
                        snap.add_touched(p)
    except _GroupRollbackTrigger as trig:
        rollback_reason = trig.reason
        failed_ids = [trig.finding_id]
    except BeanSnapshotCheckError as exc:
        rollback_reason = f"bean-check rejected the group: {exc}"
    except Exception as exc:  # noqa: BLE001
        _LOG.exception(
            "atomic group %s envelope raised unexpectedly", group_name,
        )
        rollback_reason = (
            f"unexpected {type(exc).__name__} — see server log"
        )

    if rollback_reason is not None:
        # Outer envelope rolled back every snapshotted file. Drop
        # the buffered FindingApplied events (those writes were
        # reverted) but DO emit FindingFailed for the trigger
        # finding (it objectively failed; user / log gets the
        # category + message context), followed by GroupRolledBack
        # with the trigger reason. Preserved groups list = every
        # group that committed before this one.
        events: list[BatchEvent] = []
        trigger_id = failed_ids[0] if failed_ids else None
        if trigger_id is not None:
            trigger_finding = next(
                (f for f in group_findings if f.id == trigger_id), None,
            )
            if trigger_finding is not None:
                events.append(FindingFailed(
                    finding_id=trigger_id,
                    group=group_name,
                    summary=trigger_finding.summary,
                    category=trigger_finding.category,
                    message=rollback_reason,
                ))
        events.append(GroupRolledBack(
            group=group_name,
            reason=rollback_reason,
            preserved_groups=preserved_groups,
        ))
        outcome = GroupOutcome(
            group=group_name,
            applied_finding_ids=(),
            failed_finding_ids=tuple(failed_ids),
            rolled_back=True,
        )
        return outcome, events

    # Clean exit. Flush buffered per-finding events then group_committed.
    events = list(buffered)
    events.append(GroupCommitted(
        group=group_name,
        applied=len(applied_ids),
        failed=0,
    ))
    outcome = GroupOutcome(
        group=group_name,
        applied_finding_ids=tuple(applied_ids),
        failed_finding_ids=(),
        rolled_back=False,
    )
    return outcome, events


# --- helpers ---------------------------------------------------------------


def _preflight_edit_payloads(
    *,
    selected: list[Finding],
    drafts: dict[str, Any],
    reader: Any,
) -> list[tuple[Finding, str]]:
    """Walk the selected findings; for each with ``action='edit'`` and
    a category that supports edits, validate the ``edit_payload``
    against the current world state. Return ``[(finding, message),
    ...]`` for every failure — the orchestrator emits them as
    FindingFailed events before any group runs.

    Today only ``legacy_path`` supports edits (the user can override
    the canonical destination via the per-row Edit form on the bulk-
    review page). Other categories ignore ``edit_payload``; pre-
    flight skips them silently. When future categories add edit
    support, extend the dispatch here.

    Validation re-runs the same ``_passes_destination_guards`` check
    the heal action would do — the difference is timing. Pre-flight
    fails the whole batch BEFORE any commit, surfacing every stale
    edit in one pass; without it, the first stale edit fails mid-
    batch (HealRefused) and subsequent stale edits never surface
    until the user resumes.
    """
    errors: list[tuple[Finding, str]] = []
    opened: set[str] | None = None  # lazy-loaded once across findings

    for finding in selected:
        decision = drafts.get(finding.id)
        if not decision or decision.get("action") != "edit":
            continue

        edit_payload = decision.get("edit_payload") or {}

        if finding.category == "legacy_path":
            canonical = (edit_payload.get("canonical") or "").strip()
            if not canonical:
                errors.append((
                    finding,
                    "edited canonical destination is empty — re-open the "
                    "row's Edit form and provide one, or dismiss the row.",
                ))
                continue
            if opened is None:
                from beancount.core.data import Open as _Open
                entries = list(reader.load().entries)
                opened = {
                    e.account for e in entries if isinstance(e, _Open)
                }
            from lamella.features.recovery.findings.legacy_paths import (
                _passes_destination_guards,
            )
            if not _passes_destination_guards(canonical, opened):
                errors.append((
                    finding,
                    f"edited canonical {canonical!r} no longer passes the "
                    "move-target guards (parent path not opened or not "
                    "part of an existing branch). Re-open the row's Edit "
                    "form and pick a different destination, or open the "
                    "parent path first.",
                ))
        # Other categories: no edit support today; ignore edit_payload.

    return errors


def _heal_one(
    finding: Finding,
    *,
    conn: sqlite3.Connection,
    settings: Any,
    reader: Any,
    bean_check: Any | None,
    repair_state: dict[str, Any] | None,
    bulk_context: BulkContext | None = None,
) -> HealResult:
    """Dispatch one finding to its category-specific heal action.

    ``bulk_context`` (Phase 6.1.3.5) threads the orchestrator's
    outer-envelope handle through to atomic-group heals. When
    None (Group 1 schema, single-finding callers), heals run with
    self-managed Phase 3/5 envelope behavior. When provided
    (Groups 2+3), heals skip their own snapshot wrap and write
    directly — the orchestrator's outer envelope catches per-
    finding failures and rolls the whole group back.

    repair_state, when provided, carries the user's per-finding
    edit decisions. Heal actions that support edits consult the
    ``edit_payload`` field to override the detector's
    proposed_fix; heal actions that don't support edits ignore it.
    """
    from lamella.features.recovery.heal import (
        heal_legacy_path,
        heal_schema_drift,
    )

    category = finding.category

    # If the user edited the proposed_fix, swap in the edited
    # payload. Phase 6 supports edits for legacy_path (canonical
    # destination override). Other categories ignore edit_payload
    # for now; the heal action's category-level constant in the
    # editable-fields table determines whether edits apply.
    effective_finding = finding
    if (
        repair_state
        and repair_state.get("action") == "edit"
        and repair_state.get("edit_payload")
        and category == "legacy_path"
    ):
        from dataclasses import replace

        from lamella.features.recovery.models import fix_payload

        edit = repair_state["edit_payload"]
        canonical = edit.get("canonical")
        if canonical:
            effective_finding = replace(
                finding,
                proposed_fix=fix_payload(action="move", canonical=canonical),
                alternatives=(),
            )

    if category == "schema_drift":
        return heal_schema_drift(
            effective_finding,
            conn=conn, settings=settings, reader=reader,
            bean_check=bean_check, bulk_context=bulk_context,
        )
    if category == "legacy_path":
        return heal_legacy_path(
            effective_finding,
            conn=conn, settings=settings, reader=reader,
            bean_check=bean_check, bulk_context=bulk_context,
        )

    # Unknown category — refuse rather than silently no-op so the
    # orchestrator's failure path captures it.
    raise HealRefused(
        f"no heal action registered for category {category!r}"
    )

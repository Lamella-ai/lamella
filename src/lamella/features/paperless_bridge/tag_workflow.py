# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tag-driven workflow engine for Paperless documents (ADR-0062).

A ``WorkflowRule`` is a ``(selector, action, on_success/on_anomaly/
on_error)`` quadruple. The engine iterates documents matched by the
selector, runs the action on each, and applies the outcome's tag ops
back to Paperless. Rules are code-defined (not DB-rows) for v1 — the
user is one person and source-controlled rules are diffable, testable,
and reviewable.

The engine writes one ``paperless_writeback_log`` row per (document,
action) showing the input tag set, the output tag set, and the
decision the action made — so /paperless/writebacks and the new
/paperless/anomalies queue can render an audit trail without a fresh
schema.

Idempotency is layered:
  * ``DocumentSelector.must_not_have_tags`` filters out docs already
    carrying the rule's success tag, so a re-run on already-tagged
    docs is a no-op.
  * ``PaperlessClient.add_tag`` / ``remove_tag`` are themselves
    idempotent (they do read-merge-write, not blind PATCH), so even if
    a workflow ran but failed mid-tag-op, the next run picks up where
    it left off.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import date as _date, datetime, timezone
from typing import Any, Literal

from lamella.adapters.paperless.client import PaperlessClient, PaperlessError
from lamella.adapters.paperless.schemas import Document
from lamella.features.paperless_bridge.lamella_namespace import (
    TAG_AWAITING_EXTRACTION,
    TAG_DATE_ANOMALY,
    TAG_EXTRACTED,
    TAG_LINKED,
    TAG_NEEDS_REVIEW,
    name_variants,
)

log = logging.getLogger(__name__)


# ── Canonical tag namespace (ADR-0062 §1, separator per ADR-0064) ────
#
# Five state markers under the `Lamella:` namespace reserved by
# ADR-0044 / ADR-0064. The workflows own these — users should not edit
# them by hand. ``ensure_tag(name)`` is called at engine startup to
# create any missing ones idempotently. Colors are deterministic so a
# user scanning Paperless tags can recognize them at a glance:
#   - blue  = neutral state (Awaiting/Extracted/Linked)
#   - amber = needs attention (NeedsReview)
#   - red   = problem (DateAnomaly)
#
# The ``TAG_*`` constants live in ``lamella_namespace`` so the
# migration module and the bootstrap can reference them without
# importing this whole engine.

CANONICAL_TAGS: tuple[tuple[str, str], ...] = (
    (TAG_AWAITING_EXTRACTION, "#6c757d"),  # gray  — pending pickup
    (TAG_EXTRACTED,           "#0d6efd"),  # blue  — neutral, processed
    (TAG_NEEDS_REVIEW,        "#f59e0b"),  # amber — wants a human
    (TAG_DATE_ANOMALY,        "#dc2626"),  # red   — flagged
    (TAG_LINKED,              "#16a34a"),  # green — done, linked
)


# ── Audit-log kind values (ADR-0062 §7) ──────────────────────────────
#
# New `kind` values written into the existing `paperless_writeback_log`
# table. The /paperless/writebacks audit page filters on `kind` query
# string; the /paperless/anomalies queue filters on `kind =
# 'workflow_anomaly'` plus an unresolved-tag check.
KIND_WORKFLOW_ACTION = "workflow_action"
KIND_WORKFLOW_ANOMALY = "workflow_anomaly"
KIND_WORKFLOW_ERROR = "workflow_error"


# ── Action result vocabulary ─────────────────────────────────────────
ActionStatus = Literal["success", "anomaly", "error", "skipped"]


# ── Selector dataclass (ADR-0062 §2) ─────────────────────────────────
@dataclass(frozen=True)
class DocumentSelector:
    """Describes which documents a rule applies to.

    Three fields:

    * ``must_have_tags`` — docs MUST carry every tag in this tuple
    * ``must_not_have_tags`` — docs MUST NOT carry any tag in this tuple
    * ``must_have_doctype`` — None means "any type"; otherwise an
      allowlist of canonical document_type values

    All three combine with AND semantics. The selector translates to
    a Paperless ``iter_documents`` query that pre-filters by
    ``tags__id__all`` (must_have) and then a Python-side filter for
    must_not_have (Paperless's tag NOT-IN query is awkward and
    versions vary on support).
    """

    must_have_tags: tuple[str, ...] = ()
    must_not_have_tags: tuple[str, ...] = ()
    must_have_doctype: tuple[str, ...] | None = None


# ── Action sum-type (ADR-0062 §2) ────────────────────────────────────
@dataclass(frozen=True)
class WorkflowAction:
    """Base sentinel for action subclasses. Concrete actions inherit
    and override ``run``."""

    name: str = "base"

    async def run(
        self,
        doc: Document,
        *,
        conn: sqlite3.Connection,
        client: PaperlessClient,
    ) -> "ActionResult":
        raise NotImplementedError


@dataclass(frozen=True)
class RunExtraction(WorkflowAction):
    """Run AI field extraction against a document and report success
    when every requested field came back at or above the confidence
    threshold. ``target_fields`` is informational for the audit log
    — the underlying VerifyService extracts the full set per
    ADR-0061's type-aware cascade.
    """

    target_fields: tuple[str, ...] = ("vendor", "document_date", "total_amount")
    confidence_threshold: float = 0.6
    name: str = "extract_fields"

    async def run(
        self,
        doc: Document,
        *,
        conn: sqlite3.Connection,
        client: PaperlessClient,
    ) -> "ActionResult":
        # Lazy-import the VerifyService so this module stays
        # importable in tests that don't wire up the full AI stack.
        try:
            from lamella.features.ai_cascade.service import AIService
            from lamella.core.config import get_settings
            from lamella.features.paperless_bridge.verify import (
                VerifyService,
            )
        except Exception as exc:  # noqa: BLE001
            return ActionResult(
                status="error",
                summary=f"verify-service import failed: {exc}",
            )
        settings = get_settings()
        ai = AIService(settings=settings, conn=conn)
        service = VerifyService(ai=ai, paperless=client, conn=conn)
        try:
            outcome = await service.verify_and_correct(
                doc.id, dry_run=False, ocr_first=True,
            )
        except Exception as exc:  # noqa: BLE001
            return ActionResult(
                status="error",
                summary=f"verify_and_correct raised: {exc}",
                details={"exception_type": type(exc).__name__},
            )
        # VerifyOutcome carries the per-field confidences. Treat
        # "any target field below threshold" as anomaly so the doc
        # gets the NeedsReview tag.
        below: list[str] = []
        confidences: dict[str, float] = {}
        if outcome.extracted is not None:
            ext = outcome.extracted
            try:
                ext_dict = ext.model_dump() if hasattr(ext, "model_dump") else {}
            except Exception:  # noqa: BLE001
                ext_dict = {}
            conf = ext_dict.get("confidences") or {}
            if isinstance(conf, dict):
                for k in self.target_fields:
                    raw = conf.get(k)
                    if isinstance(raw, (int, float)):
                        confidences[k] = float(raw)
                        if float(raw) < self.confidence_threshold:
                            below.append(k)
        if below:
            return ActionResult(
                status="anomaly",
                summary=(
                    f"low confidence on fields: {', '.join(below)} "
                    f"(threshold {self.confidence_threshold})"
                ),
                details={
                    "confidences": confidences,
                    "below_threshold": below,
                    "extraction_source": outcome.extraction_source,
                },
            )
        return ActionResult(
            status="success",
            summary=(
                f"extracted via {outcome.extraction_source}; "
                f"{len(self.target_fields)} fields OK"
            ),
            details={
                "confidences": confidences,
                "extraction_source": outcome.extraction_source,
            },
        )


@dataclass(frozen=True)
class RunDateOnlyVerify(WorkflowAction):
    """Cheap-path verify that only re-extracts and writes back the
    receipt_date. Wired to ``VerifyService.verify_and_correct`` with
    ``fields_of_interest=("receipt_date",)`` so:

    - The Tier 1 → Tier 2 escalation gate only considers the date's
      confidence (other fields' blurry confidences don't trigger an
      expensive vision call).
    - The vendor-mismatch escalation reason is suppressed (vendor
      isn't in scope).
    - The PATCH back to Paperless only writes ``receipt_date`` even
      if Tier 1 happened to extract other fields confidently. No
      correspondent or title side-effects.

    Cost: typically Tier 1 only (~$0.001) instead of Tier 1 + Tier 2
    (~$0.05+) — ~50× cheaper for the date-anomaly cleanup loop the
    user already runs by manually applying Lamella:DateAnomaly.

    Vision can still be reached when the date specifically is
    ambiguous (Tier 1 returns receipt_date below threshold, or the
    model self-flags NEEDS_VISION). That escalation is *correct*
    work, not waste — it's the cases where vision was firing because
    of OTHER blurry fields that this action eliminates.
    """

    confidence_threshold: float = 0.6
    name: str = "verify_date_only"

    async def run(
        self,
        doc: Document,
        *,
        conn: sqlite3.Connection,
        client: PaperlessClient,
    ) -> "ActionResult":
        try:
            from lamella.features.ai_cascade.service import AIService
            from lamella.core.config import get_settings
            from lamella.features.paperless_bridge.verify import (
                VerifyService,
            )
        except Exception as exc:  # noqa: BLE001
            return ActionResult(
                status="error",
                summary=f"verify-service import failed: {exc}",
            )
        settings = get_settings()
        ai = AIService(settings=settings, conn=conn)
        service = VerifyService(ai=ai, paperless=client, conn=conn)
        try:
            outcome = await service.verify_and_correct(
                doc.id,
                dry_run=False,
                ocr_first=True,
                fields_of_interest=("receipt_date",),
            )
        except Exception as exc:  # noqa: BLE001
            return ActionResult(
                status="error",
                summary=f"verify_and_correct raised: {exc}",
                details={"exception_type": type(exc).__name__},
            )
        date_conf: float = 0.0
        if outcome.extracted is not None:
            try:
                date_conf = float(outcome.extracted.confidence.receipt_date)
            except Exception:  # noqa: BLE001
                date_conf = 0.0
        if date_conf < self.confidence_threshold:
            return ActionResult(
                status="anomaly",
                summary=(
                    f"receipt_date confidence {date_conf:.2f} below "
                    f"threshold {self.confidence_threshold} — left for "
                    f"manual review"
                ),
                details={
                    "receipt_date_confidence": date_conf,
                    "extraction_source": outcome.extraction_source,
                },
            )
        return ActionResult(
            status="success",
            summary=(
                f"date confirmed via {outcome.extraction_source} "
                f"(conf {date_conf:.2f})"
            ),
            details={
                "receipt_date_confidence": date_conf,
                "extraction_source": outcome.extraction_source,
            },
        )


@dataclass(frozen=True)
class RunDateSanityCheck(WorkflowAction):
    """Flag a document whose document_date is outside the configured
    sanity bounds. ``min_year`` is an inclusive lower bound on the
    extracted year. ``max_offset_days`` is an inclusive upper bound
    on (extracted_date - today) — set to 0 to forbid future dates.
    """

    min_year: int = 2000
    max_offset_days: int = 0
    name: str = "date_sanity_check"

    async def run(
        self,
        doc: Document,
        *,
        conn: sqlite3.Connection,
        client: PaperlessClient,
    ) -> "ActionResult":
        # Resolve the document's date. Prefer the locally-cached
        # document_date (which the field_map_writer keeps in sync
        # with the canonical role); fall back to the Paperless
        # `created` field if the local row hasn't been populated.
        doc_date = _resolve_doc_date(conn, doc)
        if doc_date is None:
            return ActionResult(
                status="skipped",
                summary="no extractable date — nothing to sanity-check",
            )
        today = _date.today()
        offset = (doc_date - today).days
        if doc_date.year < self.min_year:
            return ActionResult(
                status="anomaly",
                summary=(
                    f"date {doc_date.isoformat()} is before "
                    f"min_year={self.min_year}"
                ),
                details={
                    "document_date": doc_date.isoformat(),
                    "min_year": self.min_year,
                    "reason": "before_min_year",
                },
            )
        if offset > self.max_offset_days:
            return ActionResult(
                status="anomaly",
                summary=(
                    f"date {doc_date.isoformat()} is "
                    f"{offset} day(s) in the future "
                    f"(max_offset_days={self.max_offset_days})"
                ),
                details={
                    "document_date": doc_date.isoformat(),
                    "max_offset_days": self.max_offset_days,
                    "reason": "future_date",
                },
            )
        return ActionResult(
            status="success",
            summary=(
                f"date {doc_date.isoformat()} within bounds "
                f"[year>={self.min_year}, offset<={self.max_offset_days}]"
            ),
            details={"document_date": doc_date.isoformat()},
        )


@dataclass(frozen=True)
class LinkToLedger(WorkflowAction):
    """Compose with the ADR-0063 reverse-matcher to find a candidate
    transaction for this document and (when found, with high
    confidence) write the link. Wired to
    ``lamella.features.receipts.auto_match.auto_link_unlinked_documents``.
    """

    name: str = "link_to_ledger"

    async def run(
        self,
        doc: Document,
        *,
        conn: sqlite3.Connection,
        client: PaperlessClient,
    ) -> "ActionResult":
        # ADR-0063 reverse matcher lives at receipts.auto_match (the
        # receipts/ directory rename is deferred per ADR-0061 §7).
        # The function takes no doc_id filter — it sweeps all unlinked
        # extracted docs; the selector at the rule level prevents
        # already-linked docs from coming back through. We trigger the
        # sweep once per LinkToLedger.run(doc) call and inspect the
        # report's linked_pairs to determine this doc's outcome.
        # Wasteful at O(N) per doc per tick; acceptable for v1 since
        # the function is fast and idempotent. Follow-up: add a
        # doc_id filter to auto_link_unlinked_documents.
        from lamella.features.receipts.auto_match import (
            auto_link_unlinked_documents,
        )
        try:
            report = auto_link_unlinked_documents(
                conn=conn, paperless_client=client,
            )
        except Exception as exc:  # noqa: BLE001
            return ActionResult(
                status="error",
                summary=f"auto_link_unlinked_documents raised: {exc}",
            )
        linked_for_doc = next(
            (pair for pair in report.linked_pairs if pair[0] == doc.id),
            None,
        )
        if linked_for_doc is not None:
            _pid, txn_hash, score = linked_for_doc
            return ActionResult(
                status="success",
                summary=f"linked to txn {txn_hash[:12]} (score {score:.2f})",
                details={
                    "txn_hash": txn_hash,
                    "score": score,
                    "scanned": report.scanned,
                    "linked": report.linked,
                },
            )
        return ActionResult(
            status="skipped",
            summary=(
                "no high-confidence candidate found"
                if report.scanned > 0 else "doc not eligible (excluded or no fields)"
            ),
            details={
                "scanned": report.scanned,
                "linked": report.linked,
                "queued_for_review": report.queued_for_review,
                "skipped_excluded": report.skipped_excluded,
                "skipped_ambiguous": report.skipped_ambiguous,
            },
        )


# ── Tag-op sum-type (ADR-0062 §2) ────────────────────────────────────
@dataclass(frozen=True)
class TagOp:
    """Tag operation to apply to a document. ``op`` is 'add' or
    'remove'; ``tag_name`` is the canonical tag name (the engine
    resolves it to a Paperless tag id at apply time)."""

    op: Literal["add", "remove"]
    tag_name: str


# ── Action result + workflow rule + run report ───────────────────────
@dataclass
class ActionResult:
    """Returned by ``WorkflowAction.run``. The engine maps
    ``status`` to which set of tag ops to apply
    (on_success / on_anomaly / on_error)."""

    status: ActionStatus
    summary: str = ""
    details: dict[str, Any] | None = None


@dataclass(frozen=True)
class WorkflowRule:
    """A single rule binding a selector to an action and an outcome
    map. Rules are immutable so the registry stays diffable."""

    name: str
    description: str
    selector: DocumentSelector
    action: WorkflowAction
    on_success: tuple[TagOp, ...] = ()
    on_anomaly: tuple[TagOp, ...] = ()
    on_error: tuple[TagOp, ...] = ()
    trigger: Literal["scheduled", "on_demand"] = "scheduled"
    batch_size: int = 50


@dataclass
class DocumentRunResult:
    """Per-document outcome from a single rule run."""

    paperless_id: int
    status: ActionStatus
    summary: str
    tag_ops_applied: list[TagOp] = field(default_factory=list)
    before_tag_ids: list[int] = field(default_factory=list)
    after_tag_ids: list[int] = field(default_factory=list)
    error: str | None = None


@dataclass
class RunReport:
    """Aggregated outcome across every doc matched by a single rule
    invocation. Returned to scheduler + on-demand callers; also
    serialized into the audit log payload."""

    rule_name: str
    started_at: datetime
    finished_at: datetime
    docs_matched: int = 0
    docs_processed: int = 0
    successes: int = 0
    anomalies: int = 0
    errors: int = 0
    skipped: int = 0
    per_doc: list[DocumentRunResult] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_name": self.rule_name,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "docs_matched": self.docs_matched,
            "docs_processed": self.docs_processed,
            "successes": self.successes,
            "anomalies": self.anomalies,
            "errors": self.errors,
            "skipped": self.skipped,
            "per_doc": [
                {
                    "paperless_id": r.paperless_id,
                    "status": r.status,
                    "summary": r.summary,
                    "tag_ops_applied": [
                        {"op": t.op, "tag_name": t.tag_name}
                        for t in r.tag_ops_applied
                    ],
                    "before_tag_ids": r.before_tag_ids,
                    "after_tag_ids": r.after_tag_ids,
                    "error": r.error,
                }
                for r in self.per_doc
            ],
        }


# ── Default rule set (ADR-0062 §3) ───────────────────────────────────
extract_missing_fields_rule = WorkflowRule(
    name="extract_missing_fields",
    description=(
        "Run AI field extraction against documents that have neither "
        "the Extracted nor NeedsReview tag yet. Tag with Extracted on "
        "high-confidence success or NeedsReview when any target field "
        "falls below the confidence threshold."
    ),
    selector=DocumentSelector(
        must_have_tags=(),
        must_not_have_tags=(TAG_EXTRACTED, TAG_NEEDS_REVIEW),
    ),
    action=RunExtraction(),
    on_success=(TagOp("add", TAG_EXTRACTED),),
    on_anomaly=(TagOp("add", TAG_NEEDS_REVIEW),),
    on_error=(TagOp("add", TAG_NEEDS_REVIEW),),
    trigger="scheduled",
)

date_sanity_check_rule = WorkflowRule(
    name="date_sanity_check",
    description=(
        "Flag documents whose extracted date is impossibly old "
        "(before min_year) or in the future. Runs on docs already "
        "tagged Extracted that don't yet carry the DateAnomaly or "
        "NeedsReview tag."
    ),
    selector=DocumentSelector(
        must_have_tags=(TAG_EXTRACTED,),
        must_not_have_tags=(TAG_DATE_ANOMALY, TAG_NEEDS_REVIEW),
    ),
    action=RunDateSanityCheck(),
    on_success=(),  # nothing to do — clean date doesn't need a marker
    on_anomaly=(TagOp("add", TAG_DATE_ANOMALY),),
    on_error=(),
    trigger="scheduled",
)

auto_link_rule = WorkflowRule(
    name="auto_link",
    description=(
        "Hand documents that are extracted but not yet linked to the "
        "ADR-0063 reverse-matcher. Tag with Linked on high-confidence "
        "match. No-op when ADR-0063 hasn't merged yet."
    ),
    selector=DocumentSelector(
        must_have_tags=(TAG_EXTRACTED,),
        must_not_have_tags=(TAG_LINKED, TAG_DATE_ANOMALY, TAG_NEEDS_REVIEW),
    ),
    action=LinkToLedger(),
    on_success=(TagOp("add", TAG_LINKED),),
    on_anomaly=(),
    on_error=(),
    trigger="scheduled",
)

DEFAULT_RULES: tuple[WorkflowRule, ...] = (
    extract_missing_fields_rule,
    date_sanity_check_rule,
    auto_link_rule,
)


# ── Helpers ──────────────────────────────────────────────────────────
def _resolve_doc_date(conn: sqlite3.Connection, doc: Document) -> _date | None:
    """Best-effort date resolution. Prefer the locally-cached
    ``paperless_doc_index.document_date`` (canonical role mapping
    keeps it in sync); fall back to Paperless's `created` date."""
    try:
        row = conn.execute(
            "SELECT document_date FROM paperless_doc_index "
            "WHERE paperless_id = ?",
            (doc.id,),
        ).fetchone()
    except sqlite3.Error:
        row = None
    if row is not None:
        raw = row["document_date"] if isinstance(row, sqlite3.Row) else row[0]
        if raw:
            try:
                return _date.fromisoformat(str(raw)[:10])
            except ValueError:
                pass
    if doc.created is not None:
        if isinstance(doc.created, datetime):
            return doc.created.date()
        if isinstance(doc.created, _date):
            return doc.created
        try:
            return _date.fromisoformat(str(doc.created)[:10])
        except (ValueError, TypeError):
            return None
    return None


async def _resolve_tag_ids(
    client: PaperlessClient,
    *,
    names: tuple[str, ...],
    create_missing: bool = False,
) -> dict[str, int]:
    """Resolve tag names to Paperless tag ids using the memoized
    ``list_tags`` cache. When ``create_missing`` is True, missing
    names are ``ensure_tag``-d (idempotent create).

    ADR-0064 backwards-compat: if ``name`` starts with ``Lamella:`` and
    no canonical tag exists yet, fall back to the legacy ``Lamella_X``
    name. This lets a partially-migrated Paperless instance (canonical
    tag not yet renamed in place) keep working without intervention.
    """
    out: dict[str, int] = {}
    if not names:
        return out
    available = await client.list_tags()
    for name in names:
        if name in available:
            out[name] = available[name]
            continue
        # ADR-0064 backwards-compat fallback. ``name_variants``
        # only flips the prefix when the input is a Lamella name;
        # for non-Lamella names the tuple is (name, name) and the
        # legacy probe is harmless.
        canonical, legacy = (name, name)
        try:
            from lamella.features.paperless_bridge.lamella_namespace import (
                LAMELLA_NAMESPACE_PREFIX_NEW, name_variants as _nv,
            )
            if name.startswith(LAMELLA_NAMESPACE_PREFIX_NEW):
                # Suffix is everything past 'Lamella:'
                suffix = name[len(LAMELLA_NAMESPACE_PREFIX_NEW):]
                canonical, legacy = _nv(suffix)
        except Exception:  # noqa: BLE001
            pass
        if legacy != name and legacy in available:
            log.debug(
                "ADR-0064: tag %r not found; falling back to legacy %r",
                name, legacy,
            )
            out[name] = available[legacy]
            continue
        if create_missing:
            tag_id = await client.ensure_tag(name)
            out[name] = tag_id
    return out


def _doc_matches_must_not(
    doc_tag_ids: list[int], must_not: dict[str, int],
) -> bool:
    """Return False (filtered out) when the doc carries any of the
    must_not tag ids."""
    if not must_not:
        return True
    forbidden = set(must_not.values())
    return not any(t in forbidden for t in doc_tag_ids)


def _doc_matches_must_have(
    doc_tag_ids: list[int], must_have: dict[str, int],
) -> bool:
    if not must_have:
        return True
    required = set(must_have.values())
    return required.issubset(set(doc_tag_ids))


async def _select_documents(
    rule: WorkflowRule,
    *,
    client: PaperlessClient,
) -> list[Document]:
    """Resolve the selector to a concrete list of documents.

    Strategy:
      * Resolve every tag name in must_have / must_not to a tag id.
        Names that don't exist in Paperless yet are simply not in the
        resolved id dict — for must_have that means "no doc could
        possibly satisfy this rule" so we return early; for must_not
        that means "no doc could possibly carry the forbidden tag" so
        the python-side filter is a no-op.
      * Build Paperless query params: ``tags__id__all`` for must_have
        (Paperless supports the AND semantic). Pull the page, then
        filter must_not in Python (Paperless's NOT-IN tag query is
        version-dependent and awkward).
      * Apply the batch_size cap so a single tick doesn't swarm the
        API on a fresh-install with thousands of un-extracted docs.
    """
    # Don't auto-create tags here — the bootstrap path is the only
    # caller that should create. If a must_have tag is missing, the
    # rule legitimately can't match anything yet.
    must_have_ids = await _resolve_tag_ids(
        client, names=rule.selector.must_have_tags, create_missing=False,
    )
    if rule.selector.must_have_tags and len(must_have_ids) != len(
        rule.selector.must_have_tags
    ):
        # Required tag isn't in Paperless yet → nothing matches.
        log.debug(
            "rule %s: must_have tags not all present in Paperless yet, "
            "skipping selection",
            rule.name,
        )
        return []
    must_not_ids = await _resolve_tag_ids(
        client, names=rule.selector.must_not_have_tags, create_missing=False,
    )

    params: dict[str, Any] = {"ordering": "-created"}
    if must_have_ids:
        # Paperless ngx accepts comma-joined ids with __all for AND.
        params["tags__id__all"] = ",".join(str(i) for i in must_have_ids.values())

    matched: list[Document] = []
    async for doc in client.iter_documents(params):
        doc_tag_ids = list(doc.tags or [])
        if not _doc_matches_must_not(doc_tag_ids, must_not_ids):
            continue
        if not _doc_matches_must_have(doc_tag_ids, must_have_ids):
            # Defensive: the server-side filter should already have
            # done this, but the python-side check protects against
            # Paperless versions that ignore __all.
            continue
        if rule.selector.must_have_doctype is not None:
            # We don't have document_type on the Document model; rely
            # on the local index to look it up. Skip docs without an
            # index row to keep the workflow surface small.
            allowed = set(rule.selector.must_have_doctype)
            # No connection in this scope — the doctype check moves
            # to the caller's run loop where conn is available.
            doc.tags = doc_tag_ids  # noop, just keep the model consistent
            matched.append(doc)
            continue
        matched.append(doc)
        if len(matched) >= rule.batch_size:
            break
    return matched


def _filter_by_doctype(
    docs: list[Document],
    *,
    conn: sqlite3.Connection,
    allowed: tuple[str, ...] | None,
) -> list[Document]:
    """Apply the document_type allowlist filter using the local
    paperless_doc_index. Docs without an index row (or with NULL
    document_type) are excluded when an allowlist is set."""
    if allowed is None:
        return docs
    out: list[Document] = []
    for doc in docs:
        try:
            row = conn.execute(
                "SELECT document_type FROM paperless_doc_index "
                "WHERE paperless_id = ?",
                (doc.id,),
            ).fetchone()
        except sqlite3.Error:
            row = None
        if row is None:
            continue
        dt = row["document_type"] if isinstance(row, sqlite3.Row) else row[0]
        if dt and dt in allowed:
            out.append(doc)
    return out


async def _apply_tag_ops(
    *,
    client: PaperlessClient,
    doc: Document,
    ops: tuple[TagOp, ...],
) -> tuple[list[int], list[int], list[TagOp]]:
    """Apply tag ops to a Paperless doc. Returns (before_ids,
    after_ids, ops_actually_applied). add_tag/remove_tag are
    idempotent so calling op twice has the same end state.
    """
    before = list(doc.tags or [])
    after = list(before)
    applied: list[TagOp] = []
    if not ops:
        return before, after, applied
    # Resolve and create-if-missing so workflows can stamp tags
    # even when bootstrap missed one (e.g. a fresh tag added later).
    names = tuple({op.tag_name for op in ops})
    name_to_id = await _resolve_tag_ids(
        client, names=names, create_missing=True,
    )
    for op in ops:
        tag_id = name_to_id.get(op.tag_name)
        if tag_id is None:
            log.warning(
                "tag op skipped — tag %r could not be resolved/created",
                op.tag_name,
            )
            continue
        if op.op == "add":
            if tag_id in after:
                continue
            after.append(tag_id)
            applied.append(op)
        elif op.op == "remove":
            if tag_id not in after:
                continue
            after = [t for t in after if t != tag_id]
            applied.append(op)
    if applied:
        # One PATCH for the whole final set — fewer round-trips than
        # calling add_tag/remove_tag per op.
        await client.patch_document(doc.id, tags=after)
    return before, after, applied


def _write_audit_row(
    conn: sqlite3.Connection,
    *,
    paperless_id: int,
    rule_name: str,
    kind: str,
    result: DocumentRunResult,
    action_summary: str,
    extra_payload: dict[str, Any] | None = None,
) -> None:
    """Append one row to ``paperless_writeback_log`` with the
    workflow audit shape (ADR-0062 §7). Dedup key is
    `<rule>:<paperless_id>:<isoformat-now>` so multiple runs against
    the same doc accrete rather than collide."""
    payload: dict[str, Any] = {
        "rule": rule_name,
        "status": result.status,
        "action_summary": action_summary,
        "tag_ops_applied": [
            {"op": op.op, "tag_name": op.tag_name}
            for op in result.tag_ops_applied
        ],
        "before_tag_ids": result.before_tag_ids,
        "after_tag_ids": result.after_tag_ids,
    }
    if result.error:
        payload["error"] = result.error
    if extra_payload:
        payload.update(extra_payload)
    dedup_key = (
        f"{rule_name}:{paperless_id}:"
        f"{datetime.now(timezone.utc).isoformat()}"
    )
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO paperless_writeback_log
                (paperless_id, kind, dedup_key, payload_json, ai_decision_id)
            VALUES (?, ?, ?, ?, ?)
            """,
            (paperless_id, kind, dedup_key, json.dumps(payload), None),
        )
        conn.commit()
    except sqlite3.Error as exc:
        log.warning("workflow audit insert failed: %s", exc)


async def run_rule(
    rule: WorkflowRule,
    *,
    conn: sqlite3.Connection,
    paperless_client: PaperlessClient,
) -> RunReport:
    """Run a single rule end-to-end. Selects documents, runs the
    action on each, applies tag ops, writes audit rows, and returns
    a RunReport with per-doc detail.

    Idempotency: the selector's ``must_not_have_tags`` filter
    excludes documents that already carry the success tag, so a
    re-run on already-processed docs is a no-op.
    """
    started = datetime.now(timezone.utc)
    report = RunReport(
        rule_name=rule.name,
        started_at=started,
        finished_at=started,
    )
    log.info("workflow rule %r: starting", rule.name)
    try:
        candidates = await _select_documents(rule, client=paperless_client)
    except PaperlessError as exc:
        log.warning("rule %r: selector failed: %s", rule.name, exc)
        report.finished_at = datetime.now(timezone.utc)
        return report
    candidates = _filter_by_doctype(
        candidates,
        conn=conn,
        allowed=rule.selector.must_have_doctype,
    )
    report.docs_matched = len(candidates)
    log.info(
        "workflow rule %r: %d document(s) matched the selector",
        rule.name, len(candidates),
    )
    for doc in candidates:
        result = DocumentRunResult(
            paperless_id=doc.id,
            status="skipped",
            summary="",
            before_tag_ids=list(doc.tags or []),
            after_tag_ids=list(doc.tags or []),
        )
        action_summary = ""
        try:
            outcome = await rule.action.run(
                doc, conn=conn, client=paperless_client,
            )
        except Exception as exc:  # noqa: BLE001
            outcome = ActionResult(
                status="error",
                summary=f"action raised: {exc}",
            )
        action_summary = outcome.summary
        result.status = outcome.status
        result.summary = outcome.summary
        if outcome.status == "error":
            result.error = outcome.summary
            ops = rule.on_error
            audit_kind = KIND_WORKFLOW_ERROR
        elif outcome.status == "anomaly":
            ops = rule.on_anomaly
            audit_kind = KIND_WORKFLOW_ANOMALY
        elif outcome.status == "success":
            ops = rule.on_success
            audit_kind = KIND_WORKFLOW_ACTION
        else:
            # 'skipped' — write the audit row but don't apply ops.
            ops = ()
            audit_kind = KIND_WORKFLOW_ACTION
        try:
            before, after, applied = await _apply_tag_ops(
                client=paperless_client, doc=doc, ops=ops,
            )
            result.before_tag_ids = before
            result.after_tag_ids = after
            result.tag_ops_applied = applied
        except PaperlessError as exc:
            log.warning(
                "rule %r: tag-op PATCH failed for doc %d: %s",
                rule.name, doc.id, exc,
            )
            result.error = (
                (result.error + " | " if result.error else "")
                + f"tag-op PATCH failed: {exc}"
            )
        report.per_doc.append(result)
        report.docs_processed += 1
        if outcome.status == "success":
            report.successes += 1
        elif outcome.status == "anomaly":
            report.anomalies += 1
        elif outcome.status == "error":
            report.errors += 1
        else:
            report.skipped += 1
        # Best-effort audit. Logging schema lives in
        # paperless_writeback_log so /paperless/writebacks +
        # /paperless/anomalies see it without a new table.
        extra: dict[str, Any] = {}
        if outcome.details:
            extra["details"] = outcome.details
        _write_audit_row(
            conn,
            paperless_id=doc.id,
            rule_name=rule.name,
            kind=audit_kind,
            result=result,
            action_summary=action_summary,
            extra_payload=extra,
        )
    report.finished_at = datetime.now(timezone.utc)
    log.info(
        "workflow rule %r: done — matched=%d processed=%d ok=%d "
        "anomaly=%d err=%d skipped=%d",
        rule.name, report.docs_matched, report.docs_processed,
        report.successes, report.anomalies, report.errors,
        report.skipped,
    )
    return report


async def bootstrap_canonical_tags(client: PaperlessClient) -> dict[str, int]:
    """Idempotently ensure the five canonical Lamella_* tags exist
    in Paperless. Called from main.py lifespan; safe to re-run on
    every boot."""
    out: dict[str, int] = {}
    for name, color in CANONICAL_TAGS:
        try:
            tag_id = await client.ensure_tag(name, color=color)
            out[name] = tag_id
        except PaperlessError as exc:
            log.warning(
                "bootstrap: failed to ensure tag %r: %s — workflows "
                "depending on it will skip until next boot",
                name, exc,
            )
    return out


def get_rule_by_name(name: str) -> WorkflowRule | None:
    """Look up a rule by name. Checks DEFAULT_RULES (the legacy
    code-defined rules kept for backward-compat tests) AND the
    per-action factory names so callers can still resolve a rule
    by action slug for on-demand triggers."""
    for rule in DEFAULT_RULES:
        if rule.name == name:
            return rule
    return None


# ── Action → completion-tag mapping (ADR-0065) ───────────────────────
#
# These are the state tags stamped on success for each action. The
# scheduler's on_success TagOps for user-defined bindings include:
#   RemoveTag(trigger_tag) + ApplyTag(ACTION_COMPLETION_TAGS[action_name])
# so idempotency is preserved: a doc that already carries the
# completion tag can't be re-triggered (the selector's must_not_have
# won't include it, but caller-side the Paperless query still works).

ACTION_COMPLETION_TAGS: dict[str, str] = {
    "extract_fields": TAG_EXTRACTED,
    "date_sanity_check": TAG_DATE_ANOMALY,
    "link_to_ledger": TAG_LINKED,
    # verify_date_only intentionally absent — its only on-success op
    # is RemoveTag(trigger). Stamping a completion tag would
    # overwrite the doc's existing workflow state (the doc was
    # already extracted before being flagged as a date anomaly), and
    # Lamella Fixed is still applied separately by _apply_corrections
    # when a real date diff is patched. The .get() lookup returns
    # None for missing keys, which the loader handles correctly.
}

# ── Action factory (ADR-0065) ─────────────────────────────────────────

_ACTION_FACTORIES: dict[str, type[WorkflowAction]] = {
    "extract_fields": RunExtraction,
    "date_sanity_check": RunDateSanityCheck,
    "link_to_ledger": LinkToLedger,
    "verify_date_only": RunDateOnlyVerify,
}


def _build_action(action_name: str) -> WorkflowAction | None:
    """Instantiate the named action with default parameters.
    Returns None when the action_name is not recognized so the
    caller can skip unknown actions without crashing.
    """
    factory = _ACTION_FACTORIES.get(action_name)
    if factory is None:
        log.warning(
            "load_runtime_rules: unknown action name %r — binding skipped",
            action_name,
        )
        return None
    return factory()


def load_runtime_rules(conn: sqlite3.Connection) -> list[WorkflowRule]:
    """Build rules dynamically from the user's bindings table.

    Each enabled binding in ``tag_workflow_bindings`` produces one
    WorkflowRule whose selector requires the trigger tag and whose
    action is a factory-built instance of the named action.

    On success the rule's on_success TagOps are:
      * RemoveTag(binding.tag_name) — removes the trigger so re-runs
        are idempotent (the doc no longer matches the selector).
      * ApplyTag(completion_tag) — the per-action canonical state tag.

    An empty bindings table → empty list → scheduler tick is a no-op.
    This is the correct default behavior per ADR-0065 (no opt-in by
    the user = no automation).
    """
    try:
        rows = conn.execute(
            """
            SELECT tag_name, action_name, enabled, config_json
            FROM tag_workflow_bindings
            WHERE enabled = 1
            ORDER BY created_at ASC
            """
        ).fetchall()
    except Exception as exc:  # noqa: BLE001
        log.warning("load_runtime_rules: DB query failed: %s", exc)
        return []

    rules: list[WorkflowRule] = []
    for row in rows:
        if isinstance(row, sqlite3.Row):
            tag_name = row["tag_name"]
            action_name = row["action_name"]
        else:
            tag_name, action_name = row[0], row[1]

        action = _build_action(action_name)
        if action is None:
            continue

        completion_tag = ACTION_COMPLETION_TAGS.get(action_name)
        on_success: tuple[TagOp, ...] = (
            TagOp("remove", tag_name),
        )
        if completion_tag:
            on_success = on_success + (TagOp("add", completion_tag),)

        rule = WorkflowRule(
            name=f"user:{tag_name}:{action_name}",
            description=(
                f"User-defined binding: tag '{tag_name}' → "
                f"action '{action_name}'. "
                f"On success: removes '{tag_name}' and adds "
                f"'{completion_tag or '(none)'}' tag."
            ),
            selector=DocumentSelector(
                must_have_tags=(tag_name,),
                # Don't re-process docs that already carry the
                # completion tag — they've been processed already.
                must_not_have_tags=(completion_tag,) if completion_tag else (),
            ),
            action=action,
            on_success=on_success,
            on_anomaly=(TagOp("add", TAG_NEEDS_REVIEW),),
            on_error=(TagOp("add", TAG_NEEDS_REVIEW),),
            trigger="scheduled",
        )
        rules.append(rule)

    return rules


__all__ = [
    "ACTION_COMPLETION_TAGS",
    "ActionResult",
    "ActionStatus",
    "CANONICAL_TAGS",
    "DEFAULT_RULES",
    "DocumentRunResult",
    "DocumentSelector",
    "KIND_WORKFLOW_ACTION",
    "KIND_WORKFLOW_ANOMALY",
    "KIND_WORKFLOW_ERROR",
    "LinkToLedger",
    "RunExtraction",
    "RunDateSanityCheck",
    "RunReport",
    "TAG_AWAITING_EXTRACTION",
    "TAG_DATE_ANOMALY",
    "TAG_EXTRACTED",
    "TAG_LINKED",
    "TAG_NEEDS_REVIEW",
    "TagOp",
    "WorkflowAction",
    "WorkflowRule",
    "auto_link_rule",
    "bootstrap_canonical_tags",
    "date_sanity_check_rule",
    "extract_missing_fields_rule",
    "get_rule_by_name",
    "load_runtime_rules",
    "run_rule",
]

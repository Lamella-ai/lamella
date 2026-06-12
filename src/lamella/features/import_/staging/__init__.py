# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Unified staging pipeline — NEXTGEN.md Phase A.

Every data source (SimpleFIN, CSV, ODS, XLSX, paste, reboot)
writes to ``staged_transactions`` before anything lands in
Beancount. This package owns the schema helpers, the staging
service, and the promotion/rollback primitives that later phases
(B–F) build on.
"""
from lamella.features.import_.staging.matcher import (
    PairProposal,
    apply_pairs,
    find_pairs,
    sweep,
)
from lamella.features.import_.staging.service import (
    StagedDecision,
    StagedPair,
    StagedRow,
    StagingError,
    StagingService,
)
from lamella.features.import_.staging.intake import (
    DuplicateReport,
    IntakeError,
    IntakeResult,
    IntakeService,
    ParsedPaste,
    RowMatch,
    SessionOverlap,
    content_fingerprint,
    detect_columns_by_content,
    detect_paste_duplicates,
    heuristic_column_map,
    parse_pasted_text,
)
from lamella.features.import_.staging.reboot import (
    DuplicateGroup,
    RebootResult,
    RebootService,
    scan_ledger,
)
from lamella.features.import_.staging.integrity_check import (
    IntegrityReport,
    ensure_integrity_table,
    latest_integrity_report,
    run_integrity_check,
)
from lamella.features.import_.staging.preflight import (
    FixmePayee,
    PreflightReport,
    fixme_heavy_payees,
    report_hash as preflight_report_hash,
)
from lamella.features.import_.staging.rule_mining import (
    MinedRule,
    mine_rules,
)
from lamella.features.import_.staging.reboot_writer import (
    FileDiff,
    RebootApplyError,
    RebootApplyResult,
    RebootPlan,
    RebootWriter,
    noop_cleaner,
)
from lamella.features.import_.staging.retrofit import (
    RetrofitError,
    RetrofitResult,
    retrofit_fingerprint,
)
from lamella.features.import_.staging.review import (
    StagingReviewItem,
    count_pending_items,
    list_pending_items,
)
from lamella.features.import_.staging.transfer_writer import (
    TransferWriter,
    emit_pending_pairs,
)

__all__ = [
    "DuplicateGroup",
    "DuplicateReport",
    "FileDiff",
    "FixmePayee",
    "IntakeError",
    "IntegrityReport",
    "MinedRule",
    "PreflightReport",
    "IntakeResult",
    "IntakeService",
    "PairProposal",
    "ParsedPaste",
    "RebootApplyError",
    "RebootApplyResult",
    "RebootPlan",
    "RebootResult",
    "RebootService",
    "RebootWriter",
    "RetrofitError",
    "RetrofitResult",
    "RowMatch",
    "SessionOverlap",
    "StagedDecision",
    "StagedPair",
    "StagedRow",
    "StagingError",
    "StagingReviewItem",
    "StagingService",
    "TransferWriter",
    "apply_pairs",
    "content_fingerprint",
    "count_pending_items",
    "detect_columns_by_content",
    "detect_paste_duplicates",
    "emit_pending_pairs",
    "ensure_integrity_table",
    "fixme_heavy_payees",
    "preflight_report_hash",
    "find_pairs",
    "heuristic_column_map",
    "latest_integrity_report",
    "list_pending_items",
    "mine_rules",
    "noop_cleaner",
    "parse_pasted_text",
    "retrofit_fingerprint",
    "run_integrity_check",
    "scan_ledger",
    "sweep",
]

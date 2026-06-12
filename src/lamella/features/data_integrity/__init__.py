# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Detection + cleanup of duplicate SimpleFIN transactions in the
ledger.

Root cause we're solving for: the SimpleFIN feed sometimes delivers
the same underlying bank event multiple times with *different*
SimpleFIN ids (bank re-posts it, the feed provider retries with a
fresh id, a reconnect re-exports the history with new ids, etc.).
``simplefin.dedup.build_index`` keys on the SimpleFIN id, so when the
ids differ each copy gets appended to ``simplefin_transactions.bean``
as a fresh transaction. The user ends up with 2x, 3x, sometimes more
of the same event.

This module finds those duplicates by content fingerprint — date,
primary-account, amount, narration — and exposes a removal path that
physically rewrites ``simplefin_transactions.bean`` to drop the
extras, keeping the ledger the byte-for-byte source of truth.
"""
from lamella.features.data_integrity.scanner import (
    DuplicateGroup,
    DuplicateTxn,
    scan_duplicates,
)
from lamella.features.data_integrity.cleaner import (
    CleanupResult,
    remove_duplicate_sfids,
)
from lamella.features.data_integrity.safety import (
    ArchiveRecord,
    WouldEmptyGroupError,
    archive_before_change,
    assert_would_keep_one_per_group,
)

__all__ = [
    "DuplicateGroup",
    "DuplicateTxn",
    "scan_duplicates",
    "CleanupResult",
    "remove_duplicate_sfids",
    "ArchiveRecord",
    "WouldEmptyGroupError",
    "archive_before_change",
    "assert_would_keep_one_per_group",
]

# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""One-shot migrations over Connector-owned ledger files and the SQLite
cache. Each pass is idempotent, dry-run by default, snapshots files
before mutating, runs bean-check on completion, and rolls back on any
regression relative to a pre-run baseline.

Run inside the container:
    python -m lamella.core.transform.key_rename --dry-run
    python -m lamella.core.transform.key_rename --apply
    python -m lamella.core.transform.backfill_hash --dry-run
    python -m lamella.core.transform.backfill_hash --apply
"""

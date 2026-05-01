# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Step 7: note-coverage audit.

Audit finding: in the shipped code, notes are **capture-only**. The
``notes`` table holds a user-captured body + AI-parsed hints
(merchant, entity). Nothing in the codebase transitions a note to
``status='resolved'`` or populates ``resolved_txn`` / ``resolved_receipt``.

Implication for reconstruct:
  * Captured-but-unattached notes are ephemeral. No state → ledger
    migration required.
  * When a note-resolution UI ships later, IT must write a trace to
    the ledger (e.g., add the note body as metadata on the override
    transaction the note resolved into). Document that rule then; no
    directive shape needed here.

This step therefore registers:
  * No reconstruct pass (there is no state to reconstruct).
  * A ``notes`` verify policy classifying the table as **ephemeral**
    so ``verify`` doesn't flag captured-but-unattached notes as drift.

When the note-resolution UX arrives and the table splits into
"captured draft" (ephemeral) and "resolved attachment" (state), this
step adds the actual writer + reconstruct pass.
"""
from __future__ import annotations

import logging

from lamella.core.transform.verify import TablePolicy
from lamella.core.transform.verify import register as register_policy

log = logging.getLogger(__name__)


register_policy(
    TablePolicy(
        table="notes",
        kind="ephemeral",
        primary_key=("id",),
    )
)

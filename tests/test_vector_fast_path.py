# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Fast-path re-embed test: when only the corrections part of
the ledger signature changed, VectorIndex.build should skip
iterating and re-embedding every ledger row."""
from __future__ import annotations

from pathlib import Path

from lamella.features.ai_cascade.vector_index import (
    VectorIndex,
    _corrections_part,
    _ledger_part,
)
from lamella.core.db import connect, migrate


def test_ledger_part_parses_signature():
    sig = "500:2026-04-17:c12:l42"
    assert _ledger_part(sig) == "500:2026-04-17"
    assert _corrections_part(sig) == "c12:l42"


def test_ledger_part_handles_legacy_signature():
    """Pre-corrections-in-signature builds are 'N:YYYY-MM-DD'
    with no trailing parts."""
    sig = "300:2025-01-01"
    assert _ledger_part(sig) == "300:2025-01-01"
    assert _corrections_part(sig) == ""


def test_fast_path_skips_ledger_iteration(tmp_path: Path):
    """When stored signature has the same ledger part but a
    different corrections part, build() must NOT iterate the
    entries list — we simulate this by passing an iterator
    that would raise if consumed."""
    db = connect(tmp_path / "vx.sqlite")
    migrate(db)
    db.execute(
        "INSERT INTO txn_embeddings_build "
        "(source, model_name, ledger_signature, row_count) "
        "VALUES ('ledger', 'test-model', '500:2026-04-17:c5:l10', 500)"
    )

    # Captures a deterministic vector-return so no model load.
    def _fake_embed(texts):
        return [[0.1, 0.2] for _ in texts]

    idx = VectorIndex(db, model_name="test-model", embed_fn=_fake_embed)

    class _ExplodingIterable:
        def __iter__(self):
            raise AssertionError(
                "build() iterated entries even though the ledger "
                "part of the signature was unchanged — fast path "
                "failed"
            )

    # Signature differs only in corrections part (c5→c6).
    stats = idx.build(
        entries=_ExplodingIterable(),
        ai_decisions=None,  # no corrections table to query
        ledger_signature="500:2026-04-17:c6:l20",
    )
    # Didn't iterate → didn't embed ledger → no ledger rows added.
    assert stats["ledger_added"] == 0
    db.close()


def test_full_rebuild_when_ledger_changed(tmp_path: Path):
    """When the ledger part DOES change, the iterator must be
    consumed (the old fast-path guard doesn't apply)."""
    db = connect(tmp_path / "vx.sqlite")
    migrate(db)
    db.execute(
        "INSERT INTO txn_embeddings_build "
        "(source, model_name, ledger_signature, row_count) "
        "VALUES ('ledger', 'test-model', '500:2026-04-17:c5:l10', 500)"
    )

    def _fake_embed(texts):
        return [[0.1, 0.2] for _ in texts]

    idx = VectorIndex(db, model_name="test-model", embed_fn=_fake_embed)
    consumed = {"count": 0}

    class _CountingIterable:
        def __iter__(self):
            consumed["count"] += 1
            return iter([])  # 0 entries but WAS consumed

    # Ledger part changed (500→501).
    idx.build(
        entries=_CountingIterable(),
        ai_decisions=None,
        ledger_signature="501:2026-04-18:c5:l10",
    )
    assert consumed["count"] == 1
    db.close()

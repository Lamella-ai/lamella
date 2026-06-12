# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Regression — routes/simplefin.py reads several attributes off
IngestResult to compose the 'Ingest complete …' UI emit. If any of
those names get renamed without updating the route, the modal
crashes AFTER the ingest already wrote results (the original bug:
'IngestResult' object has no attribute 'accounts').

This test pins the contract: the names the route uses must exist
on IngestResult. If you rename a field, update both sides in the
same commit."""
from __future__ import annotations

from lamella.features.bank_sync.ingest import IngestResult


def test_ingest_result_exposes_route_emit_attributes():
    r = IngestResult()
    # The five attributes routes/simplefin.py currently reads in the
    # success-emit and the JSON return body.
    assert hasattr(r, "per_account")
    assert hasattr(r, "new_txns")
    assert hasattr(r, "classified_by_rule")
    assert hasattr(r, "classified_by_ai")
    assert hasattr(r, "fixme_txns")
    # Sanity: defaults are usable in arithmetic / len(), so the
    # route's `len(...)` and `+` calls don't blow up on a default-
    # constructed result.
    assert isinstance(r.per_account, list)
    assert isinstance(r.new_txns, int)
    assert isinstance(r.classified_by_rule + r.classified_by_ai, int)
    assert isinstance(r.fixme_txns, int)

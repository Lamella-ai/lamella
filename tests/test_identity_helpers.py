# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Unit tests for ``lamella.core.identity`` — the txn-identity helpers
that back the source-provenance + lineage-id schema.

Spec: ``docs/specs/NORMALIZE_TXN_IDENTITY.md``.
"""
from __future__ import annotations

import re
import uuid

from lamella.core.identity import (
    REF_KEY,
    SOURCE_KEY,
    TXN_ID_KEY,
    iter_sources,
    mint_txn_id,
    normalize_bare_to_indexed,
    stamp_source,
)


# ─── mint_txn_id ──────────────────────────────────────────────────


def test_mint_txn_id_returns_valid_uuid_string():
    s = mint_txn_id()
    assert re.fullmatch(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
        s,
    ), s
    parsed = uuid.UUID(s)
    # UUIDv7 has version 7 in the high nibble of byte 6.
    assert parsed.version == 7


def test_mint_txn_id_is_unique_across_calls():
    seen = {mint_txn_id() for _ in range(50)}
    assert len(seen) == 50


def test_mint_txn_id_is_time_ordered():
    """UUIDv7 sorts lexicographically in time order. A second call
    should produce a string >= the first when the timestamps span at
    least one millisecond."""
    import time
    a = mint_txn_id()
    time.sleep(0.002)
    b = mint_txn_id()
    assert a < b


# ─── iter_sources ─────────────────────────────────────────────────


def test_iter_sources_empty_meta_yields_nothing():
    assert list(iter_sources(None)) == []
    assert list(iter_sources({})) == []
    assert list(iter_sources({"unrelated": "value"})) == []


def test_iter_sources_yields_indexed_pairs_in_order():
    meta = {
        f"{SOURCE_KEY}-0": "simplefin",
        f"{REF_KEY}-0": "TRN-A",
        f"{SOURCE_KEY}-1": "csv",
        f"{REF_KEY}-1": "ROW-7",
    }
    assert list(iter_sources(meta)) == [
        ("simplefin", "TRN-A"),
        ("csv", "ROW-7"),
    ]


def test_iter_sources_skips_orphans():
    meta = {
        f"{SOURCE_KEY}-0": "simplefin",
        # ref-id 0 is missing — orphan
        f"{SOURCE_KEY}-1": "csv",
        f"{REF_KEY}-1": "ROW-7",
    }
    pairs = list(iter_sources(meta))
    assert pairs == [("csv", "ROW-7")]


def test_iter_sources_tolerates_one_index_gap():
    """A single empty slot in the middle (e.g., mid-rewrite) is still
    walked past; two consecutive empty slots stop iteration."""
    meta = {
        f"{SOURCE_KEY}-0": "simplefin",
        f"{REF_KEY}-0": "TRN-A",
        # gap at index 1
        f"{SOURCE_KEY}-2": "csv",
        f"{REF_KEY}-2": "ROW-7",
    }
    assert list(iter_sources(meta)) == [
        ("simplefin", "TRN-A"),
        ("csv", "ROW-7"),
    ]


def test_iter_sources_yields_bare_pair_last():
    meta = {
        f"{SOURCE_KEY}-0": "simplefin",
        f"{REF_KEY}-0": "TRN-A",
        SOURCE_KEY: "csv",
        REF_KEY: "ROW-7",
    }
    assert list(iter_sources(meta)) == [
        ("simplefin", "TRN-A"),
        ("csv", "ROW-7"),
    ]


def test_iter_sources_dedupes_bare_against_indexed():
    """If the bare pair duplicates an existing indexed pair (writer
    bug or hand-edit), don't yield it twice."""
    meta = {
        f"{SOURCE_KEY}-0": "simplefin",
        f"{REF_KEY}-0": "TRN-A",
        SOURCE_KEY: "simplefin",
        REF_KEY: "TRN-A",
    }
    assert list(iter_sources(meta)) == [("simplefin", "TRN-A")]


def test_iter_sources_drops_orphan_bare():
    meta = {
        SOURCE_KEY: "csv",
        # missing ref
    }
    assert list(iter_sources(meta)) == []


# ─── stamp_source ─────────────────────────────────────────────────


def test_stamp_source_into_empty_meta_uses_index_zero():
    meta: dict = {}
    idx = stamp_source(meta, "simplefin", "TRN-A")
    assert idx == 0
    assert meta == {
        f"{SOURCE_KEY}-0": "simplefin",
        f"{REF_KEY}-0": "TRN-A",
    }


def test_stamp_source_appends_at_next_free_index():
    meta = {
        f"{SOURCE_KEY}-0": "simplefin",
        f"{REF_KEY}-0": "TRN-A",
    }
    idx = stamp_source(meta, "csv", "ROW-7")
    assert idx == 1
    assert meta[f"{SOURCE_KEY}-1"] == "csv"
    assert meta[f"{REF_KEY}-1"] == "ROW-7"


def test_stamp_source_is_idempotent_for_existing_pair():
    meta = {
        f"{SOURCE_KEY}-0": "simplefin",
        f"{REF_KEY}-0": "TRN-A",
    }
    before = dict(meta)
    idx = stamp_source(meta, "simplefin", "TRN-A")
    assert idx == 0
    assert meta == before


def test_stamp_source_fills_a_gap():
    """If index 0 is empty but index 1 is present (renumbering bug or
    mid-edit), the next stamp fills index 0, not index 2."""
    meta = {
        f"{SOURCE_KEY}-1": "csv",
        f"{REF_KEY}-1": "ROW-7",
    }
    idx = stamp_source(meta, "simplefin", "TRN-A")
    assert idx == 0


def test_stamp_source_rejects_empty_inputs():
    import pytest
    with pytest.raises(ValueError):
        stamp_source({}, "", "ref")
    with pytest.raises(ValueError):
        stamp_source({}, "simplefin", "")


# ─── normalize_bare_to_indexed ────────────────────────────────────


def test_normalize_bare_promotes_to_index_zero_when_empty():
    meta = {SOURCE_KEY: "simplefin", REF_KEY: "TRN-A"}
    changed = normalize_bare_to_indexed(meta)
    assert changed
    assert meta == {
        f"{SOURCE_KEY}-0": "simplefin",
        f"{REF_KEY}-0": "TRN-A",
    }


def test_normalize_bare_no_op_when_no_bare_keys():
    meta = {f"{SOURCE_KEY}-0": "simplefin", f"{REF_KEY}-0": "TRN-A"}
    before = dict(meta)
    changed = normalize_bare_to_indexed(meta)
    assert not changed
    assert meta == before


def test_normalize_bare_drops_silently_when_duplicates_indexed_zero():
    meta = {
        f"{SOURCE_KEY}-0": "simplefin",
        f"{REF_KEY}-0": "TRN-A",
        SOURCE_KEY: "simplefin",
        REF_KEY: "TRN-A",
    }
    changed = normalize_bare_to_indexed(meta)
    assert changed
    assert SOURCE_KEY not in meta
    assert REF_KEY not in meta
    # Indexed -0 untouched.
    assert meta[f"{SOURCE_KEY}-0"] == "simplefin"


def test_normalize_bare_appends_to_next_slot_on_conflict():
    """Bare and -0 disagree — indexed wins, bare gets relocated to
    the next free index instead of being dropped."""
    meta = {
        f"{SOURCE_KEY}-0": "simplefin",
        f"{REF_KEY}-0": "TRN-A",
        SOURCE_KEY: "csv",
        REF_KEY: "ROW-7",
    }
    changed = normalize_bare_to_indexed(meta)
    assert changed
    assert SOURCE_KEY not in meta
    assert REF_KEY not in meta
    assert meta[f"{SOURCE_KEY}-1"] == "csv"
    assert meta[f"{REF_KEY}-1"] == "ROW-7"


def test_normalize_bare_drops_orphan():
    meta = {SOURCE_KEY: "simplefin"}  # no ref
    changed = normalize_bare_to_indexed(meta)
    assert changed
    assert SOURCE_KEY not in meta


# ─── lineage key constant sanity ──────────────────────────────────


def test_txn_id_key_is_lamella_namespaced():
    assert TXN_ID_KEY.startswith("lamella-")

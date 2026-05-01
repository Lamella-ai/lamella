# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from lamella.features.import_.mapping import (
    ColumnMapResponse,
    build_prompt,
    deserialize_mapping,
    heuristic_map,
    serialize_mapping,
    MappingResult,
)
from lamella.features.import_.preview import SheetPreview


def test_heuristic_map_handles_common_column_names():
    result = heuristic_map(["Trans Date", "Amt", "Description", "Ref No"])
    assert result["Trans Date"] == "date"
    assert result["Amt"] == "amount"
    assert result["Description"] == "description"
    assert result["Ref No"] == "transaction_id"


def test_heuristic_map_drops_unknown_columns():
    result = heuristic_map(["ZZZ Made-Up Column", "Time"])
    assert result["ZZZ Made-Up Column"] is None
    # Time gets explicitly dropped.
    assert result["Time"] is None


def test_build_prompt_includes_columns_and_rows():
    preview = SheetPreview(
        sheet_name="Sheet1",
        columns=["Trans Date", "Amt"],
        rows=[["2024-01-01", "100"], ["2024-01-02", "-5"]],
        row_count=2,
    )
    system, user = build_prompt(preview)
    assert "date" in system
    assert "Trans Date" in user
    assert "row 1" in user


def test_response_schema_accepts_partial_mapping():
    payload = {
        "column_map": {"Trans Date": "date", "Amt": "amount", "Extra": None},
        "header_row_index": 0,
        "confidence": 0.88,
        "notes": "",
    }
    parsed = ColumnMapResponse.model_validate(payload)
    assert parsed.column_map["Extra"] is None
    assert parsed.confidence == 0.88


def test_mapping_round_trip_through_sources_notes():
    original = MappingResult(
        column_map={"A": "date", "B": "amount", "C": None},
        header_row_index=1,
        confidence=0.75,
        notes="note",
        source="ai",
        decision_id=42,
    )
    blob = serialize_mapping(original)
    back = deserialize_mapping(blob)
    assert back is not None
    assert back.column_map == original.column_map
    assert back.confidence == 0.75
    assert back.decision_id == 42

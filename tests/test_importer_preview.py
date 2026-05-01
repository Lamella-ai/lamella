# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from pathlib import Path

from lamella.features.import_.preview import preview_sheet


FIXTURES = Path(__file__).parent / "fixtures" / "imports"


def test_preview_generic_csv_picks_first_row_as_header():
    p = preview_sheet(FIXTURES / "generic_custom_bank.csv", None, n_rows=4)
    assert p.columns == ["Trans Date", "Description", "Amt", "Ref No"]
    assert p.header_row_index == 0
    assert p.row_count == 4
    assert p.rows[0][0].startswith("2024-02-01")


def test_preview_wf_csv_preserves_all_columns():
    p = preview_sheet(FIXTURES / "wf_2024_sample.csv", None, n_rows=3)
    # 13-col annotated format
    assert "Business Expense?" in p.columns
    assert "Expense Category" in p.columns
    assert p.row_count == 3

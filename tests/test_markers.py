# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for bootstrap/markers.py — removal marker writer + parser."""
from __future__ import annotations

from datetime import date

import pytest

from lamella.core.bootstrap.markers import (
    format_removal_marker,
    parse_removal_marker,
)


class TestFormat:
    def test_basic(self):
        out = format_removal_marker(
            '2010-01-01 custom "fava-extension" "fava_dashboards"',
            reason="foreign-fava-extension",
            tool="ws1-manual",
            on=date(2026, 4, 21),
        )
        assert out == (
            "; [lamella-removed 2026-04-21 reason=foreign-fava-extension tool=ws1-manual]\n"
            '; 2010-01-01 custom "fava-extension" "fava_dashboards"\n'
        )

    def test_strips_trailing_newline_in_original(self):
        out = format_removal_marker(
            "line\n",
            reason="foo",
            tool="bar",
            on=date(2026, 1, 1),
        )
        assert out.endswith("; line\n")
        assert "; line\n\n" not in out

    def test_default_date_is_today(self):
        out = format_removal_marker("x", reason="r", tool="t")
        assert date.today().isoformat() in out.splitlines()[0]

    @pytest.mark.parametrize(
        "reason",
        ["BadCase", "space reason", "has_underscore", "", "1starts-digit", "-leading-dash"],
    )
    def test_rejects_bad_reason(self, reason: str):
        with pytest.raises(ValueError, match="reason"):
            format_removal_marker("x", reason=reason, tool="t")

    @pytest.mark.parametrize(
        "tool",
        ["BadCase", "space tool", "has_underscore", ""],
    )
    def test_rejects_bad_tool(self, tool: str):
        with pytest.raises(ValueError, match="tool"):
            format_removal_marker("x", reason="r", tool=tool)


class TestParse:
    def test_single_marker(self):
        text = (
            "; [lamella-removed 2026-04-21 reason=foreign-fava-extension tool=ws1-manual]\n"
            '; 2010-01-01 custom "fava-extension" "fava_dashboards"\n'
        )
        markers = parse_removal_marker(text)
        assert len(markers) == 1
        m = markers[0]
        assert m.date == date(2026, 4, 21)
        assert m.reason == "foreign-fava-extension"
        assert m.tool == "ws1-manual"
        assert m.original_line == '2010-01-01 custom "fava-extension" "fava_dashboards"'

    def test_multiple_markers_interleaved_with_other_content(self):
        text = (
            "; unrelated comment\n"
            "real directive line\n"
            "; [lamella-removed 2026-04-21 reason=a tool=b]\n"
            "; first original\n"
            "something between\n"
            "; [lamella-removed 2026-04-22 reason=c tool=d]\n"
            "; second original\n"
        )
        markers = parse_removal_marker(text)
        assert len(markers) == 2
        assert markers[0].reason == "a"
        assert markers[0].original_line == "first original"
        assert markers[1].reason == "c"
        assert markers[1].original_line == "second original"

    def test_malformed_marker_without_data_line_is_skipped(self):
        text = (
            "; [lamella-removed 2026-04-21 reason=a tool=b]\n"
            "not a comment line\n"
        )
        assert parse_removal_marker(text) == []

    def test_marker_at_eof_without_data_line_is_skipped(self):
        text = "; [lamella-removed 2026-04-21 reason=a tool=b]\n"
        assert parse_removal_marker(text) == []

    def test_empty_text(self):
        assert parse_removal_marker("") == []

    def test_text_with_no_markers(self):
        text = (
            "2026-01-01 * \"Payee\" \"Narration\"\n"
            "  Expenses:Foo  10 USD\n"
            "  Assets:Bar   -10 USD\n"
        )
        assert parse_removal_marker(text) == []


class TestRoundTrip:
    def test_format_then_parse(self):
        original = '2010-01-01 custom "fava-extension" "fava_dashboards"'
        block = format_removal_marker(
            original,
            reason="foreign-fava-extension",
            tool="ws1-manual",
            on=date(2026, 4, 21),
        )
        markers = parse_removal_marker(block)
        assert len(markers) == 1
        assert markers[0].original_line == original
        assert markers[0].reason == "foreign-fava-extension"
        assert markers[0].tool == "ws1-manual"
        assert markers[0].date == date(2026, 4, 21)

    def test_many_round_trips(self):
        cases = [
            ('include "totals/*.bean"', "empty-glob", "ws1-sed"),
            (
                '1970-01-01 custom "fava-option" "default-page" "extension/FavaDashboards/?dashboard=overview"',
                "foreign-fava-option",
                "ws1-sed",
            ),
            ("2020-01-01 event \"birthday\" \"eve\"", "keep-for-audit", "editor-auto"),
        ]
        assembled = "".join(
            format_removal_marker(line, reason=r, tool=t, on=date(2026, 4, 21))
            for line, r, t in cases
        )
        markers = parse_removal_marker(assembled)
        assert len(markers) == len(cases)
        for parsed, (line, r, t) in zip(markers, cases):
            assert parsed.original_line == line
            assert parsed.reason == r
            assert parsed.tool == t

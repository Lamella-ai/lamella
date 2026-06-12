# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for bootstrap/transforms.py — textual comment-out transform."""
from __future__ import annotations

from datetime import date

import pytest

from lamella.core.bootstrap.markers import parse_removal_marker
from lamella.core.bootstrap.transforms import (
    CommentOutTransform,
    apply_transforms,
)


FIXED = date(2026, 4, 21)


class TestNoTransforms:
    def test_empty_transforms_returns_source_unchanged(self):
        src = "line one\nline two\n"
        assert apply_transforms(src, []) == src


class TestSingleLineCommentOut:
    def test_single_line(self):
        src = (
            'option "operating_currency" "USD"\n'
            '2010-01-01 custom "fava-extension" "fava_dashboards"\n'
            "2020-01-01 open Assets:Bank USD\n"
        )
        out = apply_transforms(
            src,
            [CommentOutTransform(line=2, reason="foreign-fava", tool="ws3-import")],
            on=FIXED,
        )
        assert 'option "operating_currency" "USD"' in out
        assert "2020-01-01 open Assets:Bank USD" in out
        # The original line is present but commented.
        assert '; 2010-01-01 custom "fava-extension" "fava_dashboards"' in out
        # Marker header is present.
        assert "[lamella-removed 2026-04-21 reason=foreign-fava tool=ws3-import]" in out

    def test_marker_round_trips_via_parser(self):
        src = '2010-01-01 custom "fava-extension" "fava_dashboards"\n'
        out = apply_transforms(
            src,
            [CommentOutTransform(line=1, reason="foreign-fava", tool="ws3-import")],
            on=FIXED,
        )
        markers = parse_removal_marker(out)
        assert len(markers) == 1
        assert markers[0].reason == "foreign-fava"
        assert markers[0].tool == "ws3-import"
        assert markers[0].date == FIXED
        assert markers[0].original_line == '2010-01-01 custom "fava-extension" "fava_dashboards"'


class TestMultipleSingleLine:
    def test_multiple_non_adjacent(self):
        src = (
            "line1\n"
            'line2 custom "fava-option"\n'
            "line3\n"
            'line4 custom "fava-extension"\n'
            "line5\n"
        )
        out = apply_transforms(
            src,
            [
                CommentOutTransform(line=2, reason="foreign-fava", tool="x"),
                CommentOutTransform(line=4, reason="foreign-fava", tool="x"),
            ],
            on=FIXED,
        )
        # Both originals commented in place.
        assert '; line2 custom "fava-option"' in out
        assert '; line4 custom "fava-extension"' in out
        # Untouched lines intact.
        assert "line1\n" in out
        assert "line3\n" in out
        assert "line5\n" in out

    def test_out_of_order_transforms_get_sorted(self):
        src = "a\nb\nc\n"
        out = apply_transforms(
            src,
            [
                CommentOutTransform(line=3, reason="r", tool="t"),
                CommentOutTransform(line=1, reason="r", tool="t"),
            ],
            on=FIXED,
        )
        # Both commented, b stays intact.
        assert "; a" in out
        assert "\nb\n" in out
        assert "; c" in out


class TestMultiLineBlock:
    """lazy-beancount's multi-line custom directive syntax:

        2024-01-01 custom "fava-extension" "fava_portfolio_returns" "{
          key: value
          list: [a, b, c]
        }"

    The whole block must be commented out, not just the first line,
    or beancount's parser will choke on the orphaned `}"`.
    """

    def test_multiline_block_commented_entirely(self):
        src = (
            "line before\n"
            '2024-01-01 custom "fava-extension" "fava_portfolio_returns" "{\n'
            "  key: value\n"
            "  list: [a, b]\n"
            '}"\n'
            "line after\n"
        )
        out = apply_transforms(
            src,
            [CommentOutTransform(line=2, reason="foreign-fava", tool="x")],
            on=FIXED,
        )
        # Untouched lines intact.
        assert "line before\n" in out
        assert "line after\n" in out
        # The closing `}"` must NOT appear bare, or parsing breaks.
        assert '\n}"\n' not in out
        # Each line inside the block is now a comment line.
        expected_lines = [
            '; 2024-01-01 custom "fava-extension" "fava_portfolio_returns" "{',
            ";   key: value",
            ";   list: [a, b]",
            '; }"',
        ]
        for expected in expected_lines:
            assert expected in out, f"missing commented line: {expected!r}"

    def test_multiline_block_without_closing_raises(self):
        src = (
            '2024-01-01 custom "fava-extension" "x" "{\n'
            "  never closed\n"
        )
        with pytest.raises(ValueError, match="no closing"):
            apply_transforms(
                src,
                [CommentOutTransform(line=1, reason="r", tool="t")],
                on=FIXED,
            )


class TestValidation:
    def test_line_past_eof_raises(self):
        src = "only one line\n"
        with pytest.raises(ValueError, match="out-of-range|past EOF"):
            apply_transforms(
                src,
                [CommentOutTransform(line=10, reason="r", tool="t")],
                on=FIXED,
            )

    def test_line_zero_raises(self):
        src = "a\n"
        with pytest.raises(ValueError, match="out-of-range"):
            apply_transforms(
                src,
                [CommentOutTransform(line=0, reason="r", tool="t")],
                on=FIXED,
            )

    def test_overlapping_transforms_raise(self):
        src = (
            '2024-01-01 custom "fava-extension" "x" "{\n'
            "  inside\n"
            '}"\n'
        )
        with pytest.raises(ValueError, match="overlap"):
            apply_transforms(
                src,
                [
                    CommentOutTransform(line=1, reason="r", tool="t"),
                    CommentOutTransform(line=2, reason="r", tool="t"),
                ],
                on=FIXED,
            )


class TestIdempotency:
    """Apply twice shouldn't re-comment already-commented lines —
    but the caller wouldn't pass transforms for lines that are
    already comments. This test locks in that the transform utility
    itself is deterministic: same input → same output."""

    def test_same_input_same_output(self):
        src = '2010-01-01 custom "fava-extension" "x"\n'
        t = [CommentOutTransform(line=1, reason="r", tool="tl")]
        a = apply_transforms(src, t, on=FIXED)
        b = apply_transforms(src, t, on=FIXED)
        assert a == b

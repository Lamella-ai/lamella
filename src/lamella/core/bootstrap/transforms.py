# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Textual transforms for Import flow.

Given a ``.bean`` file's text and a set of transform instructions
keyed by source line number, produce the transformed text.
Supported transforms today:

- ``CommentOutTransform`` — prefix the targeted directive with the
  §7.4 marker comment. Handles both single-line directives and
  multi-line ``"{ ... }"`` custom blocks.

Everything not targeted by a transform passes through verbatim.
Comments, blank lines, and whitespace are preserved exactly.

The Apply step (Part 5c) composes these into per-file rewrites.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date as _date

from .markers import format_removal_marker

__all__ = [
    "CommentOutTransform",
    "apply_transforms",
]


@dataclass(frozen=True)
class CommentOutTransform:
    """Instruction to comment out the directive at ``line`` (1-based).

    If the directive's first line ends with ``"{`` (multi-line
    custom argument), the transform extends through the next line
    matching ``}"``. The §7.4 marker is emitted for the first line,
    and each subsequent line inside the block is prefixed with
    ``; `` so beancount's parser ignores the whole block.
    """
    line: int
    reason: str
    tool: str


def apply_transforms(
    source_text: str,
    transforms: list[CommentOutTransform],
    *,
    on: _date | None = None,
) -> str:
    """Apply ``transforms`` to ``source_text`` and return new text.

    Transforms are processed in ascending line-number order. Lines
    not targeted by any transform pass through verbatim.

    Raises:
        ValueError: a transform points past EOF, a multi-line block
        doesn't close, or two transforms overlap on the same lines.
    """
    if not transforms:
        return source_text

    by_line = sorted(transforms, key=lambda t: t.line)
    _validate_no_overlap(by_line, source_text)

    lines = source_text.splitlines(keepends=True)
    out_parts: list[str] = []
    t_idx = 0
    i = 0
    while i < len(lines):
        line_no = i + 1  # 1-based
        if t_idx < len(by_line) and by_line[t_idx].line == line_no:
            t = by_line[t_idx]
            t_idx += 1
            end = _find_block_end(lines, i)  # exclusive

            # First line: emit the §7.4 marker block (header + commented original).
            first_line_text = lines[i].rstrip("\n").rstrip("\r")
            out_parts.append(
                format_removal_marker(
                    first_line_text,
                    reason=t.reason,
                    tool=t.tool,
                    on=on,
                )
            )
            # Remaining lines of a multi-line block: prefix with "; ".
            for j in range(i + 1, end):
                out_parts.append("; " + lines[j])
            i = end
        else:
            out_parts.append(lines[i])
            i += 1

    if t_idx < len(by_line):
        missed = [t.line for t in by_line[t_idx:]]
        raise ValueError(f"transforms target line(s) past EOF: {missed}")

    return "".join(out_parts)


def _find_block_end(lines: list[str], start: int) -> int:
    """Return the exclusive end index for the directive starting at ``start``.

    Single-line directives: ``start + 1``. Multi-line directives
    (first line ends with ``"{``): scan forward to the line equal to
    ``}"``, return its index + 1.
    """
    first = lines[start].rstrip("\n").rstrip("\r")
    if not first.endswith('"{'):
        return start + 1
    for j in range(start + 1, len(lines)):
        stripped = lines[j].rstrip("\n").rstrip("\r").strip()
        if stripped == '}"':
            return j + 1
    raise ValueError(
        f'multi-line custom block at line {start + 1} has no closing `}}"`'
    )


def _validate_no_overlap(
    sorted_transforms: list[CommentOutTransform],
    source_text: str,
) -> None:
    """Raise if two transforms hit the same directive block."""
    lines = source_text.splitlines(keepends=True)
    prev_end: int | None = None
    for t in sorted_transforms:
        if t.line < 1 or t.line > len(lines):
            raise ValueError(
                f"transform targets out-of-range line {t.line} "
                f"(source has {len(lines)} lines)"
            )
        end = _find_block_end(lines, t.line - 1)
        if prev_end is not None and t.line - 1 < prev_end:
            raise ValueError(
                f"transforms overlap: one ends at line {prev_end} "
                f"but another starts at line {t.line}"
            )
        prev_end = end

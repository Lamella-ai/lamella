# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Add SPDX Apache-2.0 copyright headers to source files.

Idempotent: skips any file already containing 'SPDX-License-Identifier:'
in its first 20 lines. Preserves shebangs (header inserted after the
shebang). Skips vendored, generated, ledger, and dependency directories.

Usage:
    python scripts/add_license_headers.py --dry-run
    python scripts/add_license_headers.py --apply
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

HOLDER = "Lamella LLC"
YEAR = "2026"
DESCRIPTION = "AI-powered bookkeeping software that provides context-aware financial intelligence"
URL = "https://lamella.ai"

HEADER_LINES = [
    f"Copyright {YEAR} {HOLDER}",
    "SPDX-License-Identifier: Apache-2.0",
    "",
    f"Lamella - {DESCRIPTION}",
    URL,
]

SKIP_DIR_NAMES = {
    ".venv", "venv", ".git", "node_modules", "vendor", "dist", "build",
    "__pycache__", ".pytest_cache", ".ruff_cache", ".mypy_cache",
    "data", ".design-fetch", ".claude", ".github_old",
    "egg-info",
}

# Path-prefix skips (relative to repo root), e.g. ledger directories.
SKIP_PREFIXES = ("ledger/", "ledger.", "ledger-", "data/", "data.")

# Specific files that must never get a header (vendored / has own).
SKIP_FILES = {
    "src/lamella/static/htmx.min.js",  # vendored derivative of htmx.org
    "LICENSE",
    "NOTICE",
}


def comment_block(style: str) -> str:
    """Render HEADER_LINES as a comment block in the given style."""
    if style == "hash":
        body = "\n".join(f"# {line}" if line else "#" for line in HEADER_LINES)
        return body + "\n"
    if style == "slash":
        body = "\n".join(f"// {line}" if line else "//" for line in HEADER_LINES)
        return body + "\n"
    if style == "dash":
        body = "\n".join(f"-- {line}" if line else "--" for line in HEADER_LINES)
        return body + "\n"
    if style == "html":
        inner = "\n".join(f"  {line}" if line else "" for line in HEADER_LINES)
        return f"<!--\n{inner}\n-->\n"
    if style == "css":
        inner = "\n".join(f" * {line}" if line else " *" for line in HEADER_LINES)
        return f"/*\n{inner}\n */\n"
    raise ValueError(f"unknown style {style!r}")


# Map extension/filename to comment style.
EXTENSION_STYLE: dict[str, str] = {
    ".py": "hash",
    ".sh": "hash",
    ".bash": "hash",
    ".yml": "hash",
    ".yaml": "hash",
    ".toml": "hash",
    ".js": "slash",
    ".mjs": "slash",
    ".cjs": "slash",
    ".ts": "slash",
    ".tsx": "slash",
    ".sql": "dash",
    ".html": "html",
    ".htm": "html",
    ".css": "css",
}

FILENAME_STYLE: dict[str, str] = {
    "Dockerfile": "hash",
    "Dockerfile.dev": "hash",
    "Dockerfile.prod": "hash",
}


def style_for(path: Path) -> str | None:
    name = path.name
    if name in FILENAME_STYLE:
        return FILENAME_STYLE[name]
    return EXTENSION_STYLE.get(path.suffix.lower())


def should_skip_path(rel_parts: tuple[str, ...]) -> bool:
    """Skip if any path component is in SKIP_DIR_NAMES."""
    return any(part in SKIP_DIR_NAMES for part in rel_parts)


def should_skip_prefix(rel_posix: str) -> bool:
    return any(rel_posix == p.rstrip("/") or rel_posix.startswith(p) for p in SKIP_PREFIXES)


_COMMENT_PREFIXES = ("#", "//", "--", "*", "<!--")


def has_header(text: str) -> bool:
    """True if a comment line in the first 20 lines declares SPDX-License-Identifier."""
    for line in text.splitlines()[:20]:
        stripped = line.lstrip()
        if "SPDX-License-Identifier" not in stripped:
            continue
        if stripped.startswith(_COMMENT_PREFIXES):
            return True
    return False


def insert_header(text: str, header: str) -> str:
    """Insert header at top, preserving shebang line if present."""
    if text.startswith("#!"):
        first_nl = text.find("\n")
        if first_nl == -1:
            return text + "\n" + header
        shebang = text[: first_nl + 1]
        rest = text[first_nl + 1 :]
        # Add a blank line between shebang and header for readability.
        return shebang + header + ("\n" if not rest.startswith("\n") else "") + rest
    return header + ("\n" if not text.startswith("\n") else "") + text


def iter_candidates() -> list[Path]:
    """Walk the repo, return paths that need headers based on extension+location filters."""
    out: list[Path] = []
    for path in REPO_ROOT.rglob("*"):
        if not path.is_file():
            continue
        try:
            rel = path.relative_to(REPO_ROOT)
        except ValueError:
            continue
        rel_posix = rel.as_posix()
        if should_skip_path(rel.parts):
            continue
        if should_skip_prefix(rel_posix):
            continue
        if rel_posix in SKIP_FILES:
            continue
        if style_for(path) is None:
            continue
        out.append(path)
    return sorted(out)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true", help="List files without modifying.")
    group.add_argument("--apply", action="store_true", help="Write headers to files in place.")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    candidates = iter_candidates()

    counts_added: dict[str, int] = defaultdict(int)
    counts_skipped_existing: dict[str, int] = defaultdict(int)
    counts_total: dict[str, int] = defaultdict(int)
    to_modify: list[Path] = []

    for path in candidates:
        ext = path.suffix.lower() or path.name
        counts_total[ext] += 1
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            if args.verbose:
                print(f"SKIP (binary): {path.relative_to(REPO_ROOT)}", file=sys.stderr)
            continue
        if has_header(text):
            counts_skipped_existing[ext] += 1
            continue
        to_modify.append(path)
        counts_added[ext] += 1

    print(f"Repo root: {REPO_ROOT}")
    print(f"Candidates scanned: {len(candidates)}")
    print(f"Already headered:  {sum(counts_skipped_existing.values())}")
    print(f"Need header added: {len(to_modify)}")
    print()
    print(f"{'Extension':<12} {'Total':>7} {'Skipped':>9} {'ToAdd':>7}")
    print("-" * 40)
    for ext in sorted(counts_total):
        print(
            f"{ext:<12} {counts_total[ext]:>7} {counts_skipped_existing[ext]:>9} {counts_added[ext]:>7}"
        )

    if args.verbose or args.dry_run:
        print()
        print("Files to be modified:")
        for path in to_modify:
            print(f"  {path.relative_to(REPO_ROOT).as_posix()}  [{style_for(path)}]")

    if args.dry_run:
        print()
        print("Dry-run only — no files written. Re-run with --apply to write headers.")
        return 0

    written = 0
    for path in to_modify:
        text = path.read_text(encoding="utf-8")
        header = comment_block(style_for(path))
        new_text = insert_header(text, header)
        path.write_text(new_text, encoding="utf-8", newline="\n" if "\r\n" not in text else None)
        written += 1
        if args.verbose:
            print(f"  wrote {path.relative_to(REPO_ROOT).as_posix()}")

    print()
    print(f"Wrote headers to {written} file(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())

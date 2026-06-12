# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""One-shot rename of legacy metadata keys and tags in Connector-owned
ledger files to the `lamella-*` namespace.

Scope — ONLY Connector-owned files are touched (see `_files.py`). The
user's hand-authored files are never rewritten. Pre-glue entries in
those user files that happen to use `memo:` / `txn-id:` / `simplefin-id:`
stay unchanged — the rename is to disambiguate our writes from theirs,
not to rewrite theirs.

Idempotent: running this twice is a no-op. A line that already uses the
lamella- key does not match the legacy regex and is left alone.

Dry-run by default (prints unified diffs + counts). Pass `--apply` to
actually write. Every writable pass snapshots each target file, runs
bean-check vs. a pre-pass baseline, and rolls back every file if a new
error appears.
"""
from __future__ import annotations

import argparse
import difflib
import logging
import re
import sys
from dataclasses import dataclass
from pathlib import Path

from lamella.core.config import Settings
from lamella.core.transform._files import (
    FileSnapshot,
    baseline,
    connector_owned_files,
    run_check_with_rollback,
    snapshot,
)

log = logging.getLogger(__name__)


# Order matters only because we rewrite key-by-key via a single-pass regex.
# Keys that are identical targets or substrings of each other would be
# unsafe if we used naive substring replacement, but the regex anchors at
# line start + whitespace + key + colon so substring-collision isn't a
# risk with this set.
META_KEY_RENAMES: dict[str, str] = {
    "paperless-id": "lamella-paperless-id",
    "match-method": "lamella-match-method",
    "match-confidence": "lamella-match-confidence",
    "txn-date": "lamella-txn-date",
    "txn-amount": "lamella-txn-amount",
    "simplefin-id": "lamella-simplefin-id",
    "ai-classified": "lamella-ai-classified",
    "ai-decision-id": "lamella-ai-decision-id",
    "rule-id": "lamella-rule-id",
    "override-of": "lamella-override-of",
    "loan-slug": "lamella-loan-slug",
    "import-source": "lamella-import-source",
    "import-id": "lamella-import-id",
    "txn-id": "lamella-import-txn-id",
    "memo": "lamella-import-memo",
    "vehicle": "lamella-mileage-vehicle",
    "entity": "lamella-mileage-entity",
    "miles": "lamella-mileage-miles",
    "rate-per-mile": "lamella-mileage-rate",
}

TAG_RENAMES: dict[str, str] = {
    "#connector-override": "#lamella-override",
    "#loan-initial-funding": "#lamella-loan-funding",
}

# Matches a metadata continuation line: one-or-more spaces, then a key,
# then a colon, then whitespace. We capture (indent, key) and rewrite
# only the key.
_META_KEY_RE = re.compile(
    r"^(?P<indent>[ \t]+)(?P<key>[A-Za-z][A-Za-z0-9_-]*):(?=[ \t])",
    re.MULTILINE,
)


def rewrite_text(text: str) -> tuple[str, int]:
    """Return (new_text, edits_count). Edits count covers both metadata
    key renames and tag renames. Idempotent."""
    edits = 0

    def _meta(match: re.Match[str]) -> str:
        nonlocal edits
        key = match.group("key")
        target = META_KEY_RENAMES.get(key)
        if target is None:
            return match.group(0)
        edits += 1
        return f"{match.group('indent')}{target}:"

    new_text = _META_KEY_RE.sub(_meta, text)

    for old_tag, new_tag in TAG_RENAMES.items():
        # Tag matches only with a word boundary after — avoid rewriting
        # `#connector-override-v2` if one ever existed.
        pattern = re.compile(re.escape(old_tag) + r"(?![A-Za-z0-9_/-])")
        new_text, n = pattern.subn(new_tag, new_text)
        edits += n

    return new_text, edits


@dataclass
class FileResult:
    path: Path
    edits: int
    diff: str


def plan(files: list[Path]) -> list[FileResult]:
    results: list[FileResult] = []
    for path in files:
        before = path.read_text(encoding="utf-8")
        after, edits = rewrite_text(before)
        diff = ""
        if edits:
            diff = "".join(
                difflib.unified_diff(
                    before.splitlines(keepends=True),
                    after.splitlines(keepends=True),
                    fromfile=str(path),
                    tofile=str(path),
                    n=1,
                )
            )
        results.append(FileResult(path=path, edits=edits, diff=diff))
    return results


def apply(
    results: list[FileResult],
    *,
    main_bean: Path,
    run_check: bool = True,
) -> None:
    snapshots: list[FileSnapshot] = []
    try:
        base_output = baseline(main_bean, run_check=run_check)
        for r in results:
            if r.edits == 0:
                continue
            snapshots.append(snapshot(r.path))
            before = r.path.read_text(encoding="utf-8")
            after, _ = rewrite_text(before)
            r.path.write_text(after, encoding="utf-8")
        run_check_with_rollback(
            main_bean, base_output, snapshots, run_check=run_check
        )
    except Exception:
        for s in snapshots:
            s.restore()
        raise


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description="Rename legacy Connector metadata keys/tags to lamella-* in Connector-owned files."
    )
    parser.add_argument("--apply", action="store_true", help="Write changes; default is dry-run.")
    parser.add_argument(
        "--no-check",
        action="store_true",
        help="Skip bean-check (faster, riskier). Default: run bean-check and rollback on regression.",
    )
    parser.add_argument(
        "--show-diff",
        action="store_true",
        help="Print unified diffs per file (dry-run only).",
    )
    args = parser.parse_args(argv)

    settings = Settings()
    files = connector_owned_files(settings)
    if not files:
        print("No Connector-owned ledger files found.")
        return 0

    results = plan(files)
    total_edits = sum(r.edits for r in results)

    for r in results:
        mark = "*" if r.edits else " "
        print(f"  [{mark}] {r.path} — {r.edits} edits")
        if args.show_diff and r.diff:
            print(r.diff)

    print(f"\nTotal edits across {len(results)} file(s): {total_edits}")

    if total_edits == 0:
        print("Nothing to rename. (idempotent — already on lamella-* schema.)")
        return 0

    if not args.apply:
        print("\nDry-run complete. Re-run with --apply to write.")
        return 0

    print("\nApplying …")
    apply(results, main_bean=settings.ledger_main, run_check=not args.no_check)
    print("Done. bean-check clean (or tolerated pre-existing errors).")
    return 0


if __name__ == "__main__":
    sys.exit(main())

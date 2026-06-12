# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for scripts/regenerate_blueprint.py.

These exercise the regenerator without depending on the live codebase
state -- every test stages its own fixture tree under tmp_path and
either invokes the script via subprocess (for the unknown-slug exit
code path) or imports the module and calls collect_routes directly
(for the route-discovery test).
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "regenerate_blueprint.py"


def _run(*args: str, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    """Invoke the regenerator script with the given args."""
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        capture_output=True,
        text=True,
        cwd=cwd or REPO,
        check=False,
    )


def _make_fake_repo(root: Path) -> None:
    """Create a minimal fake repo layout under root.

    Layout:
      root/
        src/lamella/
          routes/
            sample.py    # has 2 routes under /sample/*
          templates/
          __init__.py
        tests/
        docs/features/
    """
    src = root / "src" / "lamella"
    (src / "routes").mkdir(parents=True)
    (src / "templates").mkdir(parents=True)
    (src / "__init__.py").write_text("", encoding="utf-8")
    (src / "routes" / "__init__.py").write_text("", encoding="utf-8")
    (root / "tests").mkdir(parents=True)
    (root / "docs" / "features").mkdir(parents=True)


# --------------------------------------------------------------------
# Test 1: unknown slug → non-zero exit
# --------------------------------------------------------------------

def test_unknown_slug_exits_nonzero(tmp_path: Path) -> None:
    """A slug not in OWNERSHIP and not in docs/features/ exits 1."""
    _make_fake_repo(tmp_path)
    result = _run("zzz-not-a-slug", "--repo", str(tmp_path))
    assert result.returncode == 1, (
        f"expected exit 1, got {result.returncode}\n"
        f"stdout: {result.stdout!r}\nstderr: {result.stderr!r}"
    )
    assert "unknown slug" in result.stderr.lower()


def test_known_slug_with_no_blueprint_warns(tmp_path: Path) -> None:
    """A slug in OWNERSHIP but no existing blueprint is a warning, not an error.

    The script should still exit 0 and emit a TODO-bearing skeleton.
    """
    _make_fake_repo(tmp_path)
    # bank-sync is in OWNERSHIP but has no blueprint under tmp_path.
    result = _run("bank-sync", "--repo", str(tmp_path))
    assert result.returncode == 0, (
        f"expected exit 0, got {result.returncode}\n"
        f"stderr: {result.stderr!r}"
    )
    assert "no existing blueprint" in result.stderr.lower()
    # The TODO marker must appear since sections 6-9 had no source.
    assert "TODO: human review" in result.stdout


# --------------------------------------------------------------------
# Test 2: preserves sections 6-9 from existing blueprint
# --------------------------------------------------------------------

PRESERVED_BLUEPRINT = """---
audience: agents
last-derived-from-code: 2026-01-15
---

# Bank Sync

Stale auto-generated summary that should be replaced.

## Summary

Stale summary line.

## Owned routes

_No routes own this feature._

## Owned templates

_No templates own this feature._

## Owned source files

_No source files own this feature._

## Owned tests

_No tests own this feature._

## ADR compliance

- ADR-0019: COMPLIANT (find_source_reference is the canonical helper)
- ADR-0020: VIOLATION (no port/adapter abstraction yet)

## Current state

The ingest pipeline runs nightly. Three known issues:
1. Foo is broken on Tuesdays.
2. Bar leaks file handles under load.
3. Baz needs a rewrite.

## Known gaps

- Reconciliation never closes the loop on partial failures.

## Remaining tasks

- [ ] Wire Foo to the new event bus.
- [ ] Add a leak-test for Bar.
"""


def test_preserves_human_judgment_sections(tmp_path: Path) -> None:
    """Sections 6-9 of an existing blueprint must round-trip verbatim."""
    _make_fake_repo(tmp_path)
    blueprint = tmp_path / "docs" / "features" / "bank-sync.md"
    blueprint.write_text(PRESERVED_BLUEPRINT, encoding="utf-8")

    result = _run("bank-sync", "--repo", str(tmp_path))
    assert result.returncode == 0, (
        f"expected exit 0, got {result.returncode}\n"
        f"stderr: {result.stderr!r}"
    )
    out = result.stdout

    # Each preserved section's distinctive content must appear verbatim
    # in the regenerated output.
    assert "ADR-0019: COMPLIANT" in out
    assert "ADR-0020: VIOLATION" in out
    assert "Foo is broken on Tuesdays." in out
    assert "Reconciliation never closes the loop" in out
    assert "Wire Foo to the new event bus." in out

    # The summary should be replaced with the OWNERSHIP-driven one,
    # not the stale one.
    assert "Stale summary line." not in out
    # Frontmatter is preserved verbatim (including the stale stamp --
    # by design, the regenerator does not auto-bump it).
    assert "last-derived-from-code: 2026-01-15" in out


# --------------------------------------------------------------------
# Test 3: identifies routes from a fixture file
# --------------------------------------------------------------------

ROUTE_FIXTURE = '''
"""Fake route module for testing."""
from fastapi import APIRouter

router = APIRouter()


@router.get("/sample/list")
async def list_items():
    return []


@router.post("/sample/{item_id}/edit")
def edit_item(item_id: int):
    return {"ok": True}


@router.get("/unrelated")
def unrelated():
    """Not under /sample, should not match."""
    return {}
'''


def test_identifies_routes_from_fixture(tmp_path: Path, monkeypatch) -> None:
    """The route scanner must pick up @router.<method>(...) decorators
    and filter by route_prefixes."""
    _make_fake_repo(tmp_path)
    routes_dir = tmp_path / "src" / "lamella" / "routes"
    (routes_dir / "sample.py").write_text(ROUTE_FIXTURE, encoding="utf-8")

    # Import the module fresh so we can drive its collect_routes()
    # against our tmp_path fixture without spawning a subprocess.
    monkeypatch.syspath_prepend(str(REPO / "scripts"))
    # Reset any cached module so we can monkeypatch its module-level
    # path constants.
    sys.modules.pop("regenerate_blueprint", None)
    import regenerate_blueprint as rb  # type: ignore[import-not-found]

    monkeypatch.setattr(rb, "REPO", tmp_path)
    monkeypatch.setattr(rb, "SRC", tmp_path / "src" / "lamella")
    monkeypatch.setattr(rb, "ROUTES_DIR_LEGACY", tmp_path / "src" / "lamella" / "routes")
    monkeypatch.setattr(rb, "ROUTES_DIR_TARGET", tmp_path / "src" / "lamella" / "web" / "routes")

    ownership = {
        "route_prefixes": ["/sample"],
        "route_files": [],
    }
    routes = rb.collect_routes("sample", ownership)

    # /unrelated should be filtered out; the two /sample/* routes kept.
    assert len(routes) == 2, f"expected 2 routes, got {[(r.method, r.path) for r in routes]}"
    paths = {(r.method, r.path) for r in routes}
    assert ("GET", "/sample/list") in paths
    assert ("POST", "/sample/{item_id}/edit") in paths
    # Handlers came through.
    handlers = {r.handler for r in routes}
    assert handlers == {"list_items", "edit_item"}
    # File path is repo-relative.
    for r in routes:
        assert r.file == "src/lamella/routes/sample.py"
        assert r.line > 0


def test_route_files_explicit_match(tmp_path: Path, monkeypatch) -> None:
    """A route file listed in route_files matches even with no prefix overlap."""
    _make_fake_repo(tmp_path)
    routes_dir = tmp_path / "src" / "lamella" / "routes"
    (routes_dir / "sample.py").write_text(ROUTE_FIXTURE, encoding="utf-8")

    monkeypatch.syspath_prepend(str(REPO / "scripts"))
    sys.modules.pop("regenerate_blueprint", None)
    import regenerate_blueprint as rb  # type: ignore[import-not-found]

    monkeypatch.setattr(rb, "REPO", tmp_path)
    monkeypatch.setattr(rb, "SRC", tmp_path / "src" / "lamella")
    monkeypatch.setattr(rb, "ROUTES_DIR_LEGACY", tmp_path / "src" / "lamella" / "routes")
    monkeypatch.setattr(rb, "ROUTES_DIR_TARGET", tmp_path / "src" / "lamella" / "web" / "routes")

    ownership = {
        "route_prefixes": [],          # nothing matches by prefix
        "route_files": ["sample.py"],  # but the whole file is owned
    }
    routes = rb.collect_routes("sample", ownership)
    # All 3 routes from sample.py come through.
    assert len(routes) == 3
    paths = {r.path for r in routes}
    assert paths == {"/sample/list", "/sample/{item_id}/edit", "/unrelated"}


# --------------------------------------------------------------------
# Bonus: --list mode does not require a slug
# --------------------------------------------------------------------

def test_list_mode_does_not_require_slug() -> None:
    """`--list` should print at least one slug and exit 0."""
    result = _run("--list")
    assert result.returncode == 0, (
        f"expected exit 0, got {result.returncode}\n"
        f"stderr: {result.stderr!r}"
    )
    # Real repo has 23 feature blueprints; just assert non-empty.
    slugs = [line for line in result.stdout.splitlines() if line.strip()]
    assert len(slugs) >= 1
    assert "bank-sync" in slugs

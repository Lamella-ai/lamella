# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""``Settings.config_dir`` resolution.

The Docker image sets ``LAMELLA_CONFIG_DIR=/app/config`` (the canonical,
documented name). Earlier code only read ``CONNECTOR_CONFIG_DIR`` (the
pre-rebrand legacy name), so the live install fell through to the
repo-relative parents[3] walk — which lands on the venv site-packages
path inside the image, NOT on /app/config. Result: ``schedule_c_lines.yml``
couldn't be found and the entity scaffold endpoint returned
"no category chart resolves for this entity" for every entity, even
ones that had ``tax_schedule = 'C'`` set.

Pin both names so the canonical wins, the legacy still works (with a
deprecation warning), and dev fallback (no env var set) keeps working.
"""
from __future__ import annotations

from pathlib import Path

from lamella.core.config import Settings


def test_config_dir_reads_lamella_config_dir(monkeypatch, tmp_path: Path):
    target = tmp_path / "app_config"
    target.mkdir()
    monkeypatch.setenv("LAMELLA_CONFIG_DIR", str(target))
    monkeypatch.delenv("CONFIG_DIR", raising=False)
    monkeypatch.delenv("CONNECTOR_CONFIG_DIR", raising=False)
    s = Settings(data_dir=tmp_path / "data", ledger_dir=tmp_path / "ledger")
    assert s.config_dir == target


def test_config_dir_reads_legacy_connector_config_dir(monkeypatch, tmp_path: Path):
    target = tmp_path / "legacy_config"
    target.mkdir()
    monkeypatch.delenv("LAMELLA_CONFIG_DIR", raising=False)
    monkeypatch.delenv("CONFIG_DIR", raising=False)
    monkeypatch.setenv("CONNECTOR_CONFIG_DIR", str(target))
    s = Settings(data_dir=tmp_path / "data", ledger_dir=tmp_path / "ledger")
    assert s.config_dir == target


def test_config_dir_reads_pydantic_source_config_dir(monkeypatch, tmp_path: Path):
    """The legacy_env shim copies LAMELLA_CONFIG_DIR into CONFIG_DIR at
    process startup. Settings.config_dir should also accept that value
    directly so the shim's output stays usable (defense in depth)."""
    target = tmp_path / "src_config"
    target.mkdir()
    monkeypatch.delenv("LAMELLA_CONFIG_DIR", raising=False)
    monkeypatch.delenv("CONNECTOR_CONFIG_DIR", raising=False)
    monkeypatch.setenv("CONFIG_DIR", str(target))
    s = Settings(data_dir=tmp_path / "data", ledger_dir=tmp_path / "ledger")
    assert s.config_dir == target


def test_canonical_wins_over_legacy(monkeypatch, tmp_path: Path):
    canonical = tmp_path / "canonical"
    legacy = tmp_path / "legacy"
    canonical.mkdir()
    legacy.mkdir()
    monkeypatch.setenv("LAMELLA_CONFIG_DIR", str(canonical))
    monkeypatch.setenv("CONNECTOR_CONFIG_DIR", str(legacy))
    monkeypatch.delenv("CONFIG_DIR", raising=False)
    s = Settings(data_dir=tmp_path / "data", ledger_dir=tmp_path / "ledger")
    assert s.config_dir == canonical


def test_dev_fallback_when_no_env(monkeypatch, tmp_path: Path):
    """Without any env var set, config_dir resolves to the repo-relative
    parents[3] / 'config' path so a checkout-and-run dev session works
    out of the box."""
    monkeypatch.delenv("LAMELLA_CONFIG_DIR", raising=False)
    monkeypatch.delenv("CONNECTOR_CONFIG_DIR", raising=False)
    monkeypatch.delenv("CONFIG_DIR", raising=False)
    s = Settings(data_dir=tmp_path / "data", ledger_dir=tmp_path / "ledger")
    # Fallback path lives next to the source tree.
    expected = Path(__file__).resolve().parents[1] / "config"
    assert s.config_dir == expected


def test_config_dir_resolves_schedule_c_lines_yaml(monkeypatch, tmp_path: Path):
    """Regression: live-install scaffold endpoint failed because
    schedule_c_lines.yml couldn't be located. Verify the path resolver
    finds it when LAMELLA_CONFIG_DIR points at a directory containing
    the file."""
    target = tmp_path / "config"
    target.mkdir()
    (target / "schedule_c_lines.yml").write_text("[]\n", encoding="utf-8")
    monkeypatch.setenv("LAMELLA_CONFIG_DIR", str(target))
    monkeypatch.delenv("CONFIG_DIR", raising=False)
    monkeypatch.delenv("CONNECTOR_CONFIG_DIR", raising=False)
    s = Settings(data_dir=tmp_path / "data", ledger_dir=tmp_path / "ledger")
    assert s.schedule_c_lines_path.exists()

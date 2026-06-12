# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0

"""Pin the rename ``Settings.connector_data_dir`` → ``Settings.data_dir``.

Confirms:

- ``Settings(data_dir=...)`` is the canonical constructor kwarg.
- ``settings.connector_data_dir`` still resolves (read-only deprecated
  property) so the one external reader at
  ``transform/normalize_txn_identity.py:865`` keeps working without
  forcing a touch of that do-not-modify module.
- Every derived path (``db_path``, ``backups_dir``,
  ``reports_output_resolved``, ``import_upload_dir_resolved``,
  ``legacy_db_path``) sits under the new ``data_dir`` value.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from lamella.core.config import Settings


@pytest.fixture
def s(tmp_path: Path) -> Settings:
    return Settings(data_dir=tmp_path / "data", ledger_dir=tmp_path / "ledger")


def test_data_dir_kwarg_sets_field(tmp_path: Path, s: Settings):
    assert s.data_dir == tmp_path / "data"


def test_connector_data_dir_property_aliases_data_dir(s: Settings):
    """``connector_data_dir`` must still read as the same value as
    ``data_dir`` so ``transform/normalize_txn_identity.py`` (which
    uses the old name and is on the do-not-touch list) keeps working."""
    assert s.connector_data_dir == s.data_dir


def test_connector_data_dir_kwarg_silently_ignored(tmp_path: Path):
    """The old kwarg is no longer a real field. Because the Settings
    model_config carries ``extra="ignore"``, pydantic silently drops
    it instead of raising — so the field falls back to its default
    (``Path('/data')``), NOT the value the caller tried to pass.

    This test pins that behavior so it's an obvious gotcha during
    review: any forgotten ``Settings(connector_data_dir=...)``
    callsite will produce a Settings object where ``data_dir`` is
    the wrong value (the default) — easy to spot in an integration
    test that checks an actual file write, but easy to miss in a
    pure unit test. Maintainers should grep for the old kwarg name
    after every Settings change until the property is removed."""
    s = Settings(connector_data_dir=tmp_path / "intent")  # type: ignore[call-arg]
    # The old kwarg name was dropped; data_dir falls back to its default.
    assert s.data_dir != tmp_path / "intent"
    assert s.data_dir == Path("/data")
    # The property still aliases data_dir, so reads of the old name
    # show the default — the same value, just confirming aliasing holds.
    assert s.connector_data_dir == Path("/data")


def test_derived_paths_use_data_dir(s: Settings, tmp_path: Path):
    base = tmp_path / "data"
    assert s.db_path == base / "lamella.sqlite"
    assert s.legacy_db_path == base / "beancounter-glue.sqlite"
    assert s.backups_dir == base / "backups"
    assert s.reports_output_resolved == base / "reports"
    assert s.import_upload_dir_resolved == base / "imports"

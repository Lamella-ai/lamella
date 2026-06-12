# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0

"""Tests for ``lamella._legacy_env`` — the deprecation shim that
copies pre-rebrand env-var names into the slots pydantic-settings
actually reads.

The shim's contract:

- The pydantic ``Settings.data_dir`` field reads from env var ``DATA_DIR``
  (case-insensitive, no env_prefix on this Settings class).
- The documented user-facing name is ``LAMELLA_DATA_DIR``. The shim copies
  it into ``DATA_DIR`` at process startup.
- Two pre-rebrand legacy aliases are also accepted:
  * ``CONNECTOR_DATA_DIR`` — actual pre-rebrand name.
  * ``LAMELLA_CONNECTOR_DATA_DIR`` — accepted defensively for operators
    who saw the 2026-04-26 doc-drift audit memo (which incorrectly
    claimed pydantic was reading the doubled-prefix form) and adjusted
    their compose file. Both legacy names emit ``DeprecationWarning``.

These tests exercise the shim directly; they do not import ``Settings``
because the goal is to pin the env-var copy behavior in isolation.
"""
from __future__ import annotations

import os
import warnings

import pytest

from lamella.utils import _legacy_env


@pytest.fixture(autouse=True)
def _reset_shim_state(monkeypatch):
    """Each test starts with a clean warning ledger and no relevant env
    vars set. ``monkeypatch.delenv(..., raising=False)`` is fine because
    the suite never relies on these being set at import time."""
    for name in (
        "DATA_DIR",
        "LAMELLA_DATA_DIR",
        "CONNECTOR_DATA_DIR",
        "LAMELLA_CONNECTOR_DATA_DIR",
        "MIGRATIONS_DIR",
        "LAMELLA_MIGRATIONS_DIR",
        "CONNECTOR_MIGRATIONS_DIR",
        "CONFIG_DIR",
        "LAMELLA_CONFIG_DIR",
        "CONNECTOR_CONFIG_DIR",
        "SKIP_DISCOVERY_GUARD",
        "LAMELLA_SKIP_DISCOVERY_GUARD",
        "BCG_SKIP_DISCOVERY_GUARD",
    ):
        monkeypatch.delenv(name, raising=False)
    # Reset the one-shot warning ledger so each test sees its own warnings.
    _legacy_env._warned.clear()
    yield


def test_canonical_lamella_data_dir_copied_to_pydantic_source():
    """Setting the documented ``LAMELLA_DATA_DIR`` should copy into the
    ``DATA_DIR`` slot pydantic reads. No warning on the canonical name."""
    os.environ["LAMELLA_DATA_DIR"] = "/canonical/dir"
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _legacy_env.apply_env_aliases()
    assert os.environ["DATA_DIR"] == "/canonical/dir"
    deprecation_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
    assert deprecation_warnings == [], (
        "Canonical LAMELLA_DATA_DIR must not emit DeprecationWarning"
    )


def test_legacy_connector_data_dir_warns_and_copies():
    """The pre-rebrand ``CONNECTOR_DATA_DIR`` still works; emits a
    one-shot DeprecationWarning pointing at the canonical name."""
    os.environ["CONNECTOR_DATA_DIR"] = "/legacy/connector"
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _legacy_env.apply_env_aliases()
    assert os.environ["DATA_DIR"] == "/legacy/connector"
    msgs = [str(w.message) for w in caught if issubclass(w.category, DeprecationWarning)]
    assert any("CONNECTOR_DATA_DIR" in m and "LAMELLA_DATA_DIR" in m for m in msgs), (
        f"Expected DeprecationWarning naming both legacy and canonical; got {msgs}"
    )


def test_audit_doubled_prefix_alias_works():
    """``LAMELLA_CONNECTOR_DATA_DIR`` was named in the 2026-04-26 audit
    memo as the (incorrectly attributed) actual env var. Pydantic never
    actually read that name — the field has no env_prefix — but operators
    who applied the memo's recommendation may have set it. Accept it as
    a tertiary alias so they aren't broken; warn them to switch."""
    os.environ["LAMELLA_CONNECTOR_DATA_DIR"] = "/audit/recommended"
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _legacy_env.apply_env_aliases()
    assert os.environ["DATA_DIR"] == "/audit/recommended"
    msgs = [str(w.message) for w in caught if issubclass(w.category, DeprecationWarning)]
    assert any(
        "LAMELLA_CONNECTOR_DATA_DIR" in m and "LAMELLA_DATA_DIR" in m for m in msgs
    ), f"Expected DeprecationWarning naming both names; got {msgs}"


def test_canonical_wins_over_legacy_when_both_set():
    """If the operator sets the canonical AND a legacy name with
    different values, the canonical value wins. No warning fires
    because the operator clearly intended the canonical."""
    os.environ["LAMELLA_DATA_DIR"] = "/canonical/wins"
    os.environ["CONNECTOR_DATA_DIR"] = "/legacy/loses"
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _legacy_env.apply_env_aliases()
    assert os.environ["DATA_DIR"] == "/canonical/wins"
    deprecation = [w for w in caught if issubclass(w.category, DeprecationWarning)]
    assert deprecation == [], "Canonical-wins path should be silent"


def test_legacy_aliases_resolved_in_declaration_order():
    """When two legacy aliases are set with no canonical, the one
    declared first in ``LEGACY_ENV['DATA_DIR']`` after the canonical
    wins. CONNECTOR_DATA_DIR is declared before LAMELLA_CONNECTOR_DATA_DIR."""
    os.environ["CONNECTOR_DATA_DIR"] = "/legacy/connector-wins"
    os.environ["LAMELLA_CONNECTOR_DATA_DIR"] = "/legacy/doubled-loses"
    _legacy_env.apply_env_aliases()
    assert os.environ["DATA_DIR"] == "/legacy/connector-wins"


def test_pydantic_source_name_set_directly_skips_aliasing():
    """If the operator already set the bare pydantic-source name (e.g.
    ``DATA_DIR`` directly), the shim must not overwrite it from any
    alias — the operator clearly knows what they're doing."""
    os.environ["DATA_DIR"] = "/operator/explicit"
    os.environ["LAMELLA_DATA_DIR"] = "/should-not-overwrite"
    _legacy_env.apply_env_aliases()
    assert os.environ["DATA_DIR"] == "/operator/explicit"


def test_warn_fires_only_once_per_legacy_name():
    """``DeprecationWarning`` should be one-shot — repeat startups in
    the same process must not flood the logs."""
    os.environ["CONNECTOR_DATA_DIR"] = "/legacy"
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _legacy_env.apply_env_aliases()
        _legacy_env.apply_env_aliases()
        _legacy_env.apply_env_aliases()
    deprecation = [w for w in caught if issubclass(w.category, DeprecationWarning)]
    assert len(deprecation) == 1, f"Expected one warning, got {len(deprecation)}: {deprecation}"


def test_read_env_returns_canonical_value_when_set():
    os.environ["LAMELLA_DATA_DIR"] = "/canonical"
    assert _legacy_env.read_env("LAMELLA_DATA_DIR") == "/canonical"


def test_read_env_falls_back_to_legacy_with_warning():
    os.environ["CONNECTOR_DATA_DIR"] = "/legacy"
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        value = _legacy_env.read_env("LAMELLA_DATA_DIR")
    assert value == "/legacy"
    msgs = [str(w.message) for w in caught if issubclass(w.category, DeprecationWarning)]
    assert any("CONNECTOR_DATA_DIR" in m for m in msgs), msgs


def test_read_env_falls_back_to_doubled_prefix_with_warning():
    os.environ["LAMELLA_CONNECTOR_DATA_DIR"] = "/audit"
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        value = _legacy_env.read_env("LAMELLA_DATA_DIR")
    assert value == "/audit"
    msgs = [str(w.message) for w in caught if issubclass(w.category, DeprecationWarning)]
    assert any("LAMELLA_CONNECTOR_DATA_DIR" in m for m in msgs), msgs


def test_read_env_returns_default_when_nothing_set():
    assert _legacy_env.read_env("LAMELLA_DATA_DIR", default="/fallback") == "/fallback"
    assert _legacy_env.read_env("LAMELLA_DATA_DIR") is None


def test_other_groups_still_work_after_data_dir_refactor():
    """Sanity check: the dict-of-list refactor must not have broken
    the other three alias groups (MIGRATIONS_DIR, CONFIG_DIR,
    SKIP_DISCOVERY_GUARD)."""
    os.environ["BCG_SKIP_DISCOVERY_GUARD"] = "1"
    os.environ["CONNECTOR_MIGRATIONS_DIR"] = "/legacy/migrations"
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _legacy_env.apply_env_aliases()
    assert os.environ["SKIP_DISCOVERY_GUARD"] == "1"
    assert os.environ["MIGRATIONS_DIR"] == "/legacy/migrations"
    msgs = [str(w.message) for w in caught if issubclass(w.category, DeprecationWarning)]
    assert any("BCG_SKIP_DISCOVERY_GUARD" in m for m in msgs), msgs
    assert any("CONNECTOR_MIGRATIONS_DIR" in m for m in msgs), msgs

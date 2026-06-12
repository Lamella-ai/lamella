# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from pathlib import Path

from beancount import loader

from lamella.core.settings.writer import (
    append_setting,
    append_setting_unset,
    is_secret_key,
    read_settings_from_entries,
)


def _load(main_bean: Path) -> list:
    entries, _errors, _ = loader.load_file(str(main_bean))
    return list(entries)


def test_secret_detection_by_suffix():
    # Convention-matching keys are secret.
    assert is_secret_key("paperless_api_token")
    assert is_secret_key("ntfy_token")
    assert is_secret_key("pushover_user_key")
    assert is_secret_key("something_password")
    assert is_secret_key("db.credentials")
    # Non-matching keys are not.
    assert not is_secret_key("mileage_rate")
    assert not is_secret_key("ai_max_monthly_spend_usd")
    assert not is_secret_key("notify_digest_day")


def test_simplefin_access_url_is_secret_by_explicit_list():
    # Name doesn't match the suffix convention but the value contains
    # embedded credentials; explicit-list override catches it.
    assert is_secret_key("simplefin_access_url")


def test_append_setting_writes_valid_directive(ledger_dir: Path):
    main_bean = ledger_dir / "main.bean"
    config_path = ledger_dir / "connector_config.bean"
    block = append_setting(
        connector_config=config_path,
        main_bean=main_bean,
        key="mileage_rate",
        value="0.67",
        run_check=False,
    )
    assert block is not None
    assert 'custom "setting" "mileage_rate" "0.67"' in block
    assert "lamella-set-at:" in block
    entries, errors, _ = loader.load_file(str(main_bean))
    assert errors == []


def test_append_setting_refuses_secret(ledger_dir: Path):
    main_bean = ledger_dir / "main.bean"
    config_path = ledger_dir / "connector_config.bean"
    block = append_setting(
        connector_config=config_path, main_bean=main_bean,
        key="paperless_api_token", value="abc123", run_check=False,
    )
    assert block is None
    # File wasn't created (nothing written).
    assert not config_path.exists() or "paperless_api_token" not in config_path.read_text()


def test_unset_removes_setting(ledger_dir: Path):
    main_bean = ledger_dir / "main.bean"
    config_path = ledger_dir / "connector_config.bean"
    append_setting(
        connector_config=config_path, main_bean=main_bean,
        key="mileage_rate", value="0.67", run_check=False,
    )
    append_setting_unset(
        connector_config=config_path, main_bean=main_bean,
        key="mileage_rate", run_check=False,
    )
    settings = read_settings_from_entries(_load(main_bean))
    assert "mileage_rate" not in settings


def test_reconstruct_rebuilds_settings(ledger_dir: Path, tmp_path):
    from lamella.core.db import connect, migrate

    main_bean = ledger_dir / "main.bean"
    config_path = ledger_dir / "connector_config.bean"
    append_setting(
        connector_config=config_path, main_bean=main_bean,
        key="mileage_rate", value="0.67", run_check=False,
    )
    append_setting(
        connector_config=config_path, main_bean=main_bean,
        key="notify_digest_day", value="monday", run_check=False,
    )
    # This one must NOT round-trip even if accidentally passed in.
    append_setting(
        connector_config=config_path, main_bean=main_bean,
        key="paperless_api_token", value="should-be-blocked", run_check=False,
    )

    db = connect(tmp_path / "rc.sqlite")
    migrate(db)

    import lamella.core.transform.steps.step6_settings_overrides  # noqa: F401
    from lamella.core.transform.reconstruct import run_all

    reports = run_all(db, _load(main_bean))
    assert any(r.pass_name == "step6:settings-overrides" for r in reports)

    rows = db.execute(
        "SELECT key, value FROM app_settings ORDER BY key"
    ).fetchall()
    by_key = {r["key"]: r["value"] for r in rows}
    assert by_key.get("mileage_rate") == "0.67"
    assert by_key.get("notify_digest_day") == "monday"
    assert "paperless_api_token" not in by_key

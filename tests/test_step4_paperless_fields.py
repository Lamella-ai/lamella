# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from pathlib import Path

from beancount import loader

from lamella.features.paperless_bridge.field_map_writer import (
    append_field_mapping,
    read_field_mappings_from_entries,
)


def _load(main_bean: Path) -> list:
    entries, _errors, _ = loader.load_file(str(main_bean))
    return list(entries)


def test_append_field_mapping_writes_valid_directive(ledger_dir: Path):
    main_bean = ledger_dir / "main.bean"
    config_path = ledger_dir / "connector_config.bean"
    block = append_field_mapping(
        connector_config=config_path,
        main_bean=main_bean,
        paperless_field_id=42,
        paperless_field_name="Total Amount",
        canonical_role="total",
        run_check=False,
    )
    assert 'custom "paperless-field" 42 "total"' in block
    assert 'lamella-field-name: "Total Amount"' in block
    # Bare FALSE, not quoted.
    assert "lamella-auto-assigned: FALSE" in block
    assert '"FALSE"' not in block
    entries, errors, _ = loader.load_file(str(main_bean))
    assert errors == []


def test_last_write_wins_per_field_id(ledger_dir: Path):
    main_bean = ledger_dir / "main.bean"
    config_path = ledger_dir / "connector_config.bean"
    append_field_mapping(
        connector_config=config_path, main_bean=main_bean,
        paperless_field_id=42, paperless_field_name="Total",
        canonical_role="total", run_check=False,
    )
    append_field_mapping(
        connector_config=config_path, main_bean=main_bean,
        paperless_field_id=42, paperless_field_name="Grand Total",
        canonical_role="total", run_check=False,
    )
    rows = read_field_mappings_from_entries(_load(main_bean))
    by_id = {r["paperless_field_id"]: r for r in rows}
    assert by_id[42]["paperless_field_name"] == "Grand Total"


def test_reconstruct_rebuilds_user_mappings(ledger_dir: Path, tmp_path):
    from lamella.core.db import connect, migrate

    main_bean = ledger_dir / "main.bean"
    config_path = ledger_dir / "connector_config.bean"
    append_field_mapping(
        connector_config=config_path, main_bean=main_bean,
        paperless_field_id=42, paperless_field_name="Total",
        canonical_role="total", run_check=False,
    )
    append_field_mapping(
        connector_config=config_path, main_bean=main_bean,
        paperless_field_id=43, paperless_field_name="Vendor",
        canonical_role="vendor", run_check=False,
    )

    db = connect(tmp_path / "rc.sqlite")
    migrate(db)

    import lamella.core.transform.steps.step4_paperless_fields  # noqa: F401
    from lamella.core.transform.reconstruct import run_all

    reports = run_all(db, _load(main_bean))
    assert any(r.pass_name == "step4:paperless-fields" for r in reports)

    rows = db.execute(
        "SELECT paperless_field_id, canonical_role, auto_assigned "
        "FROM paperless_field_map ORDER BY paperless_field_id"
    ).fetchall()
    assert [(r["paperless_field_id"], r["canonical_role"], r["auto_assigned"]) for r in rows] == [
        (42, "total", 0),
        (43, "vendor", 0),
    ]

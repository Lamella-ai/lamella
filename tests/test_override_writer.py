# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import shutil
from datetime import date
from decimal import Decimal
from pathlib import Path

from beancount import loader

from lamella.features.rules.overrides import OverrideWriter


def _prepare(ledger_dir: Path) -> tuple[Path, Path]:
    main_bean = ledger_dir / "main.bean"
    # Append an Expenses:FIXME open + a FIXME txn the override will correct.
    (ledger_dir / "accounts.bean").write_text(
        (ledger_dir / "accounts.bean").read_text(encoding="utf-8")
        + "2024-01-01 open Expenses:FIXME USD\n"
        + "2024-01-01 open Expenses:Acme:Supplies:ToolRental USD\n",
        encoding="utf-8",
    )
    (ledger_dir / "simplefin_transactions.bean").write_text(
        (ledger_dir / "simplefin_transactions.bean").read_text(encoding="utf-8")
        + "\n2026-04-15 * \"UNCAT Lowe\" \"Tool rental\"\n"
        "  simplefin-id: \"sf-9001\"\n"
        "  Liabilities:Acme:Card:CardA1234  -50.00 USD\n"
        "  Expenses:FIXME                      50.00 USD\n",
        encoding="utf-8",
    )
    return main_bean, ledger_dir / "connector_overrides.bean"


def test_override_block_is_valid_beancount(ledger_dir: Path):
    main_bean, overrides_path = _prepare(ledger_dir)

    writer = OverrideWriter(
        main_bean=main_bean,
        overrides=overrides_path,
        run_check=False,  # don't require bean-check on PATH in CI
    )
    writer.append(
        txn_date=date(2026, 4, 15),
        txn_hash="deadbeef",
        amount=Decimal("50.00"),
        from_account="Expenses:FIXME",
        to_account="Expenses:Acme:Supplies:ToolRental",
        narration="Tool rental override",
    )

    # The override file exists and was included from main.bean.
    assert overrides_path.exists()
    assert 'include "connector_overrides.bean"' in main_bean.read_text(encoding="utf-8")

    # The full ledger parses without Beancount errors.
    entries, errors, _ = loader.load_file(str(main_bean))
    assert errors == []

    # The override block has the lamella-override-of metadata tag.
    override_text = overrides_path.read_text(encoding="utf-8")
    assert 'lamella-override-of: "deadbeef"' in override_text
    assert "#lamella-override" in override_text


def test_override_idempotent_include(ledger_dir: Path):
    main_bean, overrides_path = _prepare(ledger_dir)
    writer = OverrideWriter(
        main_bean=main_bean, overrides=overrides_path, run_check=False
    )
    for _ in range(3):
        writer.append(
            txn_date=date(2026, 4, 15),
            txn_hash="deadbeef",
            amount=Decimal("10"),
            from_account="Expenses:FIXME",
            to_account="Expenses:Acme:Supplies:ToolRental",
        )
    main_text = main_bean.read_text(encoding="utf-8")
    # Include should appear exactly once despite repeated writes.
    assert main_text.count('include "connector_overrides.bean"') == 1

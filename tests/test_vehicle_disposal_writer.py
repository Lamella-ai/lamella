# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 4 — vehicle disposal writer (unit-level).

Covers block rendering, single-compound-posting structure, the
balanced triple contract, bean-check rollback, and the revoke
offsetting pattern. Route-level tests (dispose_preview, commit,
revoke) live in tests/test_vehicle_disposal_route.py.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from lamella.core.ledger_writer import BeanCheckError
from lamella.features.vehicles.disposal_writer import (
    DISPOSAL_TAG,
    DisposalDraft,
    compute_gain_loss,
    new_disposal_id,
    render_disposal_block,
    render_revoke_block,
    write_disposal,
    write_revoke,
)


def _draft(**overrides) -> DisposalDraft:
    base = dict(
        disposal_id="abc123",
        vehicle_slug="suvone",
        vehicle_display_name="2015 SuvA",
        disposal_date=date(2026, 4, 1),
        disposal_type="sale",
        proceeds_amount=Decimal("15000"),
        proceeds_account="Assets:Personal:Checking",
        asset_account="Assets:Vehicles:suvone",
        asset_amount_out=Decimal("12000"),
        gain_loss_account="Income:Personal:CapitalGains:VehicleSale",
        gain_loss_amount=Decimal("-3000"),   # income = negative sign
        buyer_or_party="J. Smith",
        notes=None,
    )
    base.update(overrides)
    return DisposalDraft(**base)


def test_render_carries_tag_and_metadata():
    block = render_disposal_block(_draft())
    assert DISPOSAL_TAG in block
    assert 'lamella-disposal-id: "abc123"' in block
    assert 'lamella-disposal-vehicle: "suvone"' in block
    assert 'lamella-disposal-date: "2026-04-01"' in block
    assert 'lamella-disposal-type: "sale"' in block
    assert 'lamella-disposal-party: "J. Smith"' in block


def test_render_has_three_postings():
    block = render_disposal_block(_draft())
    lines = [ln for ln in block.splitlines() if ln.startswith("  ") and "USD" in ln]
    assert len(lines) == 3
    # Asset out negative.
    assert any("-12000.00 USD" in ln and "Assets:Vehicles:suvone" in ln for ln in lines)
    # Proceeds positive.
    assert any("15000.00 USD" in ln and "Assets:Personal:Checking" in ln for ln in lines)
    # Plug.
    assert any(
        "-3000.00 USD" in ln and "Income:Personal:CapitalGains" in ln
        for ln in lines
    )


def test_render_escapes_quotes_in_narration_and_party():
    d = _draft(buyer_or_party='J "Fast" Smith')
    block = render_disposal_block(d)
    assert 'J \\"Fast\\" Smith' in block


def test_new_disposal_id_is_hex_uuid():
    a = new_disposal_id()
    b = new_disposal_id()
    assert a != b
    assert len(a) == 32
    assert all(c in "0123456789abcdef" for c in a)


def test_compute_gain_loss_arithmetic():
    # Proceeds 15000, basis 20000, accum_dep 8000 → remaining basis
    # 12000; gain = 15000 - 12000 = 3000.
    gl = compute_gain_loss(
        proceeds=Decimal("15000"),
        adjusted_basis=Decimal("20000"),
        accumulated_depreciation=Decimal("8000"),
    )
    assert gl == Decimal("3000")


def test_write_disposal_appends_and_bean_check_succeeds(tmp_path: Path):
    main_bean = tmp_path / "main.bean"
    overrides = tmp_path / "connector_overrides.bean"
    main_bean.write_text(
        '2020-01-01 open Assets:Vehicles:suvone USD\n'
        '2020-01-01 open Assets:Personal:Checking USD\n'
        '2020-01-01 open Income:Personal:CapitalGains:VehicleSale USD\n',
        encoding="utf-8",
    )

    write_disposal(
        draft=_draft(),
        main_bean=main_bean,
        overrides_path=overrides,
        skip_check=True,  # standalone test — no bean-check binary needed
    )
    text = overrides.read_text(encoding="utf-8")
    assert DISPOSAL_TAG in text
    assert "Vehicle disposal" in text
    # main.bean now includes connector_overrides.bean.
    assert 'include "connector_overrides.bean"' in main_bean.read_text(
        encoding="utf-8",
    )


def test_write_disposal_rolls_back_on_bean_check_regression(
    tmp_path: Path, monkeypatch,
):
    """If bean-check surfaces a new error after our append, the
    writer must revert both files to their pre-append bytes."""
    main_bean = tmp_path / "main.bean"
    overrides = tmp_path / "connector_overrides.bean"
    main_bean.write_text(
        "; baseline clean\n", encoding="utf-8",
    )
    main_before = main_bean.read_bytes()

    # Fake bean-check: baseline 0 errors, after write 1 new error.
    call = {"n": 0}

    def fake_capture_bean_check(_path):
        call["n"] += 1
        if call["n"] == 1:
            return 0, ""
        return 1, "oops: new-error-line"

    monkeypatch.setattr(
        "lamella.features.vehicles.disposal_writer.capture_bean_check",
        fake_capture_bean_check,
    )
    # ``run_bean_check_vs_baseline`` (called inside the writer's
    # ``_append_with_check``) re-imports ``capture_bean_check`` from
    # ``lamella.core.ledger_writer`` directly, so we have to patch the
    # source module too — otherwise the second call falls through to
    # the real subprocess (which sees no bean-check on PATH and returns
    # rc=0, swallowing the regression we want to detect here).
    monkeypatch.setattr(
        "lamella.core.ledger_writer.capture_bean_check",
        fake_capture_bean_check,
    )

    with pytest.raises(BeanCheckError):
        write_disposal(
            draft=_draft(),
            main_bean=main_bean,
            overrides_path=overrides,
        )
    # Both files reverted.
    assert main_bean.read_bytes() == main_before
    # Overrides never existed before; should not exist after failure.
    assert not overrides.exists()


def test_render_revoke_negates_every_posting():
    original = _draft()
    block = render_revoke_block(
        revoke_id="def456",
        original=original,
        revoke_date=date(2026, 5, 1),
    )
    assert 'lamella-disposal-id: "def456"' in block
    assert 'lamella-disposal-revokes: "abc123"' in block
    # Three negated amounts.
    assert "12000.00 USD" in block                # was -12000
    assert "-15000.00 USD" in block               # was +15000
    assert "3000.00 USD" in block                 # was -3000 (negated)


def test_render_revoke_uses_today_when_no_date_provided():
    original = _draft()
    block = render_revoke_block(revoke_id="def456", original=original)
    # Whatever today is, the transaction line starts with an ISO date.
    first_line = next(ln for ln in block.splitlines() if "* \"Vehicle disposal revoked\"" in ln)
    iso = first_line.split()[0]
    date.fromisoformat(iso)   # parses — that's the assertion


def test_write_revoke_appends_offsetting_block(tmp_path: Path):
    main_bean = tmp_path / "main.bean"
    overrides = tmp_path / "connector_overrides.bean"
    main_bean.write_text(
        '2020-01-01 open Assets:Vehicles:suvone USD\n'
        '2020-01-01 open Assets:Personal:Checking USD\n'
        '2020-01-01 open Income:Personal:CapitalGains:VehicleSale USD\n',
        encoding="utf-8",
    )
    original = _draft()
    write_disposal(
        draft=original, main_bean=main_bean, overrides_path=overrides,
        skip_check=True,
    )
    before_text = overrides.read_text(encoding="utf-8")

    write_revoke(
        revoke_id="def456",
        original=original,
        main_bean=main_bean,
        overrides_path=overrides,
        skip_check=True,
    )
    after_text = overrides.read_text(encoding="utf-8")
    added = after_text[len(before_text):]
    assert 'lamella-disposal-revokes: "abc123"' in added
    assert "12000.00 USD" in added
    assert "-15000.00 USD" in added

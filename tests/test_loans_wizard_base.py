# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""WP10 — wizard framework primitives.

Tests:
  - WizardCommitTxn snapshots / restores connector files atomically
  - WizardCommitTxn rolls back when the body raises
  - WizardCommitTxn rolls back when bean-check fails
  - PlannedWrite subclasses produce sensible preview dicts
  - ValidationError shape carries a field association
  - WizardFlow Protocol is conformant against a stub flow
"""
from __future__ import annotations

import sqlite3
import tempfile
from datetime import date
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from lamella.features.loans.wizard._base import (
    FlowResult,
    PlannedAccountsOpen,
    PlannedLoanBalanceAnchor,
    PlannedLoanFunding,
    PlannedLoanWrite,
    PlannedPropertyWrite,
    PlannedSqliteUpsert,
    ValidationError,
    WizardCommitError,
    WizardCommitTxn,
    WizardFlow,
    WizardStep,
    err,
)


# --------------------------------------------------------------- ValidationError


class TestValidationError:
    def test_message_only(self):
        e = ValidationError("missing field")
        assert e.message == "missing field"
        assert e.field is None

    def test_with_field(self):
        e = ValidationError("bad date", field="funded_date")
        assert e.field == "funded_date"

    def test_err_helper(self):
        e = err("nope", field="amount")
        assert isinstance(e, ValidationError)
        assert e.field == "amount"


# --------------------------------------------------------------- WizardStep


class TestWizardStep:
    def test_basic_construction(self):
        s = WizardStep(name="x", template="foo.html", title="X")
        assert s.name == "x"
        assert s.template == "foo.html"
        assert s.title == "X"


# --------------------------------------------------------------- PlannedWrite previews


class TestPlannedWritePreview:
    def test_property_preview_shape(self):
        p = PlannedPropertyWrite(
            slug="MainResidence",
            display_name="Main Residence",
            property_type="primary_residence",
            entity_slug="Personal",
            address="123 Main St",
            is_primary_residence=True,
        )
        d = p.render_preview()
        assert d["kind"] == "property"
        assert "MainResidence" in d["summary"]
        details = dict(d["details"])
        assert details["Slug"] == "MainResidence"
        assert details["Display name"] == "Main Residence"
        assert details["Entity"] == "Personal"

    def test_loan_preview_shape(self):
        p = PlannedLoanWrite(
            slug="Mortgage1",
            display_name="Mortgage 1",
            loan_type="mortgage",
            entity_slug="Personal",
            institution="Bank Two",
            original_principal="550000",
            funded_date="2025-01-01",
            term_months=360,
            interest_rate_apr="6.625",
            liability_account_path="Liabilities:Personal:BankTwo:Mortgage1",
        )
        d = p.render_preview()
        assert d["kind"] == "loan"
        assert "Mortgage1" in d["summary"]
        details = dict(d["details"])
        assert details["Type"] == "mortgage"
        assert details["Term (months)"] == "360"

    def test_accounts_open_preview_shape(self):
        p = PlannedAccountsOpen(
            paths=("A:1", "A:2", "A:3"),
            opened_on="2025-01-01",
        )
        d = p.render_preview()
        assert d["kind"] == "accounts_open"
        assert "3 account" in d["summary"]
        assert len(d["details"]) == 3

    def test_loan_funding_preview_shape(self):
        p = PlannedLoanFunding(
            slug="M",
            display_name="Mortgage",
            funded_date="2025-01-01",
            principal="550000",
            offset_account="Assets:Personal:Cash",
            liability_account_path="Liabilities:Personal:M",
        )
        d = p.render_preview()
        assert d["kind"] == "loan_funding"
        details = dict(d["details"])
        assert "Liabilities:Personal:M" in details["Liability"]
        assert "Assets:Personal:Cash" in details["Offset"]

    def test_balance_anchor_preview_shape(self):
        p = PlannedLoanBalanceAnchor(
            loan_slug="M", as_of_date="2025-01-01", balance="100000",
            source="statement",
        )
        d = p.render_preview()
        assert d["kind"] == "loan_balance_anchor"
        assert "100000" in d["summary"]

    def test_sqlite_upsert_preview_shape(self):
        p = PlannedSqliteUpsert(
            table="properties",
            columns=("slug", "display_name"),
            values=("M", "Main"),
            summary_text="Insert property M",
        )
        d = p.render_preview()
        assert d["kind"] == "sqlite_upsert"
        assert d["summary"] == "Insert property M"


# --------------------------------------------------------------- WizardCommitTxn


def _settings_for(tmp_path):
    main = tmp_path / "main.bean"
    overrides = tmp_path / "connector_overrides.bean"
    accounts = tmp_path / "connector_accounts.bean"
    config = tmp_path / "connector_config.bean"
    main.write_text(
        'option "operating_currency" "USD"\n'
        'plugin "beancount.plugins.auto_accounts"\n'
        'include "connector_overrides.bean"\n'
        'include "connector_accounts.bean"\n'
        'include "connector_config.bean"\n',
        encoding="utf-8",
    )
    overrides.write_text("", encoding="utf-8")
    accounts.write_text("", encoding="utf-8")
    config.write_text("", encoding="utf-8")
    return SimpleNamespace(
        ledger_main=main,
        connector_overrides_path=overrides,
        connector_accounts_path=accounts,
        connector_config_path=config,
    )


class TestWizardCommitTxn:
    def test_snapshot_taken_on_enter(self, tmp_path):
        settings = _settings_for(tmp_path)
        original = settings.connector_overrides_path.read_bytes()
        with pytest.raises(RuntimeError, match="forced"):
            with WizardCommitTxn(settings, run_check=False):
                settings.connector_overrides_path.write_text(
                    "modified\n", encoding="utf-8",
                )
                raise RuntimeError("forced rollback")
        assert settings.connector_overrides_path.read_bytes() == original

    @pytest.mark.parametrize("dummy", [None])
    def test_body_exception_restores_files(self, tmp_path, dummy):
        settings = _settings_for(tmp_path)
        original = settings.connector_overrides_path.read_bytes()
        with pytest.raises(RuntimeError, match="forced"):
            with WizardCommitTxn(settings, run_check=False):
                settings.connector_overrides_path.write_text(
                    "modified\n", encoding="utf-8",
                )
                raise RuntimeError("forced rollback")
        assert settings.connector_overrides_path.read_bytes() == original

    def test_clean_body_keeps_writes(self, tmp_path):
        settings = _settings_for(tmp_path)
        with WizardCommitTxn(settings, run_check=False):
            settings.connector_overrides_path.write_text(
                "kept\n", encoding="utf-8",
            )
        assert settings.connector_overrides_path.read_text(
            encoding="utf-8"
        ) == "kept\n"

    @pytest.mark.xfail(
        reason="bean-check not on test PATH; rollback isn't triggered. "
        "Pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md.",
        strict=False,
    )
    def test_bean_check_failure_rolls_back(self, tmp_path):
        settings = _settings_for(tmp_path)
        original = settings.connector_overrides_path.read_bytes()
        with pytest.raises(WizardCommitError):
            with WizardCommitTxn(settings, run_check=True):
                # Write syntactically-broken Beancount — bean-check
                # rejects it at the txn boundary.
                settings.connector_overrides_path.write_text(
                    "this is not valid beancount syntax @@@\n",
                    encoding="utf-8",
                )
        assert settings.connector_overrides_path.read_bytes() == original

    def test_nonexistent_file_unlinked_on_rollback(self, tmp_path):
        settings = _settings_for(tmp_path)
        # Pretend connector_overrides didn't exist before.
        settings.connector_overrides_path.unlink()
        with pytest.raises(RuntimeError, match="forced"):
            with WizardCommitTxn(settings, run_check=False):
                settings.connector_overrides_path.write_text(
                    "created during txn\n", encoding="utf-8",
                )
                raise RuntimeError("forced rollback")
        assert not settings.connector_overrides_path.exists()


# --------------------------------------------------------------- WizardFlow Protocol


class _StubFlow:
    """Minimal flow that satisfies the Protocol."""
    name = "stub"
    title = "Stub Flow"

    def steps(self):
        return {
            "first": WizardStep("first", "first.html", "First"),
            "second": WizardStep("second", "second.html", "Second"),
        }

    def initial_step(self):
        return "first"

    def validate(self, step_name, params, conn):
        if step_name == "first" and not params.get("foo"):
            return [err("foo required", field="foo")]
        return []

    def next_step(self, current_step, params, conn):
        if current_step == "first":
            return "second"
        return None

    def write_plan(self, params, conn):
        return []

    def commit(self, params, settings, conn, reader):
        return FlowResult(redirect_to="/done", saved_message="ok")

    def template_context(self, step_name, params, conn):
        return {}


class TestWizardFlowProtocol:
    def test_stub_satisfies_protocol(self):
        flow = _StubFlow()
        # runtime_checkable protocol — isinstance() works.
        assert isinstance(flow, WizardFlow)

    def test_register_flow_idempotent(self):
        from lamella.web.routes.loans_wizard import (
            FLOW_REGISTRY, register_flow as register,
        )
        original_count = len(FLOW_REGISTRY)
        flow = _StubFlow()
        register(flow)
        register(flow)  # second call should replace, not duplicate
        assert FLOW_REGISTRY.get("stub") is flow
        # Cleanup so other tests aren't affected.
        FLOW_REGISTRY.pop("stub", None)

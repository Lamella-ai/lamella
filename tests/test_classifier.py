# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for bootstrap/classifier.py — three-bucket Import analysis."""
from __future__ import annotations

from pathlib import Path

import pytest

from lamella.core.bootstrap.classifier import (
    FOREIGN_FAVA_CUSTOM_TYPES,
    OWNED_CUSTOM_TYPES,
    ImportDecision,
    ImportAnalysis,
    analyze_import,
)


def _write_main(dir_: Path, content: str) -> Path:
    main = dir_ / "main.bean"
    main.write_text(content, encoding="utf-8")
    return main


# --- missing / parse errors ------------------------------------------------


class TestMissingOrBroken:
    def test_missing_main_bean_blocks(self, tmp_path: Path):
        result = analyze_import(tmp_path)
        assert result.is_blocked
        assert "main.bean not found" in result.parse_errors[0]
        assert result.decisions == ()

    def test_unparseable_blocks(self, tmp_path: Path):
        _write_main(tmp_path, '2026-01-01 * "unclosed\n')
        result = analyze_import(tmp_path)
        assert result.is_blocked
        assert len(result.parse_errors) > 0


# --- plugin allowlist ------------------------------------------------------


class TestPluginAllowlist:
    def test_allowed_plugin_passes(self, tmp_path: Path):
        _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            'plugin "beancount_lazy_plugins.auto_accounts"\n',
        )
        result = analyze_import(tmp_path)
        assert not result.is_blocked
        assert result.plugin_block_reason is None

    def test_core_beancount_plugins_allowed(self, tmp_path: Path):
        # "beancount.*" is allowed as a prefix so users who explicitly
        # declared the redundant core plugins (common in lazy-beancount
        # ledgers) aren't blocked.
        _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            'plugin "beancount.plugins.implicit_prices"\n'
            'plugin "beancount.ops.balance"\n',
        )
        result = analyze_import(tmp_path)
        assert not result.is_blocked

    def test_disallowed_plugin_blocks(self, tmp_path: Path):
        _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            'plugin "beancount_share.share"\n',
        )
        result = analyze_import(tmp_path)
        assert result.is_blocked
        assert result.plugin_block_reason is not None
        assert "beancount_share.share" in result.plugin_block_reason
        assert "beancount_share.share" in result.disallowed_plugins

    def test_multiple_disallowed_plugins(self, tmp_path: Path):
        _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            'plugin "beancount_interpolate.recur"\n'
            'plugin "beancount_reds_plugins.effective_date.effective_date"\n',
        )
        result = analyze_import(tmp_path)
        assert result.is_blocked
        assert len(result.disallowed_plugins) == 2


# --- keep / transform / foreign classification -----------------------------


class TestClassification:
    def test_transaction_is_keep(self, tmp_path: Path):
        _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            "2020-01-01 open Assets:Bank USD\n"
            "2020-01-01 open Income:Work USD\n"
            '2026-01-15 * "Paycheck"\n'
            "  Assets:Bank    1000 USD\n"
            "  Income:Work   -1000 USD\n",
        )
        result = analyze_import(tmp_path)
        txn_decisions = [d for d in result.decisions if d.directive_label == "Transaction"]
        assert len(txn_decisions) == 1
        assert txn_decisions[0].bucket == "keep"
        assert txn_decisions[0].target_file == "manual_transactions.bean"

    def test_bcg_stamped_transaction_routes_to_simplefin(self, tmp_path: Path):
        _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            "2020-01-01 open Assets:Bank USD\n"
            "2020-01-01 open Income:Work USD\n"
            '2026-01-15 * "Paycheck"\n'
            '  lamella-simplefin-id: "abc"\n'
            "  Assets:Bank    1000 USD\n"
            "  Income:Work   -1000 USD\n",
        )
        result = analyze_import(tmp_path)
        txn = [d for d in result.decisions if d.directive_label == "Transaction"][0]
        assert txn.target_file == "simplefin_transactions.bean"

    def test_open_routes_to_accounts_bean(self, tmp_path: Path):
        _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            "2020-01-01 open Assets:Bank USD\n",
        )
        result = analyze_import(tmp_path)
        opens = [d for d in result.decisions if d.directive_label == "Open"]
        assert len(opens) == 1
        assert opens[0].bucket == "keep"
        assert opens[0].target_file == "accounts.bean"

    def test_foreign_fava_extension_is_transform(self, tmp_path: Path):
        _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            '2010-01-01 custom "fava-extension" "fava_dashboards"\n'
            '1970-01-01 custom "fava-option" "default-page" "/x"\n',
        )
        result = analyze_import(tmp_path)
        fava = [
            d for d in result.decisions
            if d.directive_label.startswith("Custom:fava-")
        ]
        assert len(fava) == 2
        for d in fava:
            assert d.bucket == "transform"
            assert d.action == "comment-out"
            assert d.reversibility == "reversible"

    def test_owned_custom_types_are_keep(self, tmp_path: Path):
        _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            '2026-01-01 custom "lamella-ledger-version" "1"\n'
            '2026-02-01 custom "classification-rule" "some-pattern"\n'
            '2026-03-01 custom "budget" "groceries" 500 USD\n',
        )
        result = analyze_import(tmp_path)
        owned = [d for d in result.decisions if d.directive_label.startswith("Custom:")]
        assert len(owned) >= 3
        for d in owned:
            assert d.bucket == "keep"

        # Target file routing: version → main.bean, rule → rules, budget → budgets.
        by_type = {d.directive_label: d for d in owned}
        assert by_type["Custom:lamella-ledger-version"].target_file == "main.bean"
        assert by_type["Custom:classification-rule"].target_file == "connector_rules.bean"
        assert by_type["Custom:budget"].target_file == "connector_budgets.bean"

    def test_registry_and_state_custom_types_are_keep(self, tmp_path: Path):
        # Regression: every directive type Lamella actually writes
        # today must be Keep, not Foreign. Re-importing a Lamella
        # ledger into a fresh install otherwise treats our own state
        # (entity registry, account meta, vehicles, properties, loans,
        # notes, balance anchors, projects, day reviews, mileage) as
        # foreign and shoves it past with a warning.
        _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            '2020-01-01 custom "entity" "EntityA"\n'
            '2020-01-01 custom "entity-context" "EntityA" "ctx"\n'
            '2020-01-01 custom "account-meta" Assets:Bank\n'
            '2020-01-01 custom "account-description" Assets:Bank "desc"\n'
            '2020-01-01 custom "account-kind" Assets:Bank "depository"\n'
            '2020-01-01 custom "vehicle" "veh-a"\n'
            '2020-01-01 custom "vehicle-fuel-entry" "veh-a"\n'
            '2020-01-01 custom "mileage-trip-meta" "veh-a"\n'
            '2020-01-01 custom "mileage-attribution" "veh-a"\n'
            '2020-01-01 custom "property" "prop-a"\n'
            '2020-01-01 custom "loan" "loan-a"\n'
            '2020-01-01 custom "loan-balance-anchor" "loan-a" 100 USD\n'
            '2020-01-01 custom "note" 1\n'
            '2020-01-01 custom "balance-anchor" Assets:Bank 100 USD\n'
            '2020-01-01 custom "day-review" 2026-01-01\n'
            '2020-01-01 custom "project" "proj-a"\n'
            '2020-01-01 custom "audit-dismissed" "x"\n'
            '2020-01-01 custom "paperless-field" 1 "total"\n'
            '2020-01-01 custom "setting" "k" "v"\n',
        )
        result = analyze_import(tmp_path)
        customs = [d for d in result.decisions if d.directive_label.startswith("Custom:")]
        # All of the above must be Keep, routed to a Connector file.
        for d in customs:
            assert d.bucket == "keep", f"{d.directive_label} bucketed {d.bucket}"
            assert d.target_file is not None, f"{d.directive_label} has no target_file"
        by_type = {d.directive_label: d for d in customs}
        assert by_type["Custom:entity"].target_file == "connector_config.bean"
        assert by_type["Custom:account-meta"].target_file == "connector_config.bean"
        assert by_type["Custom:vehicle"].target_file == "connector_config.bean"
        assert by_type["Custom:property"].target_file == "connector_config.bean"
        assert by_type["Custom:loan"].target_file == "connector_config.bean"
        assert by_type["Custom:balance-anchor"].target_file == "connector_config.bean"
        assert by_type["Custom:project"].target_file == "connector_config.bean"
        assert by_type["Custom:note"].target_file == "connector_config.bean"

    def test_unknown_custom_type_is_foreign(self, tmp_path: Path):
        _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            '2020-01-01 custom "my-personal-marker" "something"\n',
        )
        result = analyze_import(tmp_path)
        unknown = [d for d in result.decisions if d.directive_label == "Custom:my-personal-marker"]
        assert len(unknown) == 1
        assert unknown[0].bucket == "foreign"
        assert unknown[0].action == "pass-through"
        assert unknown[0].reversibility == "reversible"


# --- synthetic-entry filter ------------------------------------------------


class TestSyntheticFilter:
    def test_auto_inserted_opens_are_not_in_decisions(self, tmp_path: Path):
        # auto_accounts inserts Open directives from "<auto_insert_open>"
        # — those shouldn't count as source-directive decisions.
        _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            'plugin "beancount_lazy_plugins.auto_accounts"\n'
            '2026-01-15 * "Sale"\n'
            "  Assets:NeverOpened    10 USD\n"
            "  Income:AlsoNeverOpened  -10 USD\n",
        )
        result = analyze_import(tmp_path)
        # Exactly one Transaction; no explicit Open directives in source.
        opens = [d for d in result.decisions if d.directive_label == "Open"]
        assert opens == []
        txns = [d for d in result.decisions if d.directive_label == "Transaction"]
        assert len(txns) == 1


# --- summary counts --------------------------------------------------------


class TestCountByBucket:
    def test_mixed_ledger_counts(self, tmp_path: Path):
        _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            "2020-01-01 open Assets:A USD\n"  # keep
            "2020-01-01 open Income:B USD\n"  # keep
            '2010-01-01 custom "fava-extension" "x"\n'  # transform
            '2010-01-01 custom "fava-option" "y" "z"\n'  # transform
            '2020-01-01 custom "unknown" "foo"\n'  # foreign
            '2026-01-01 * "T"\n'
            "  Assets:A    10 USD\n"
            "  Income:B   -10 USD\n",  # keep
        )
        result = analyze_import(tmp_path)
        counts = result.count_by_bucket
        assert counts["keep"] >= 3   # two Opens + one Transaction (plus option? no, options don't show)
        assert counts["transform"] == 2
        assert counts["foreign"] == 1


# --- invariant checks ------------------------------------------------------


def test_owned_and_foreign_fava_sets_disjoint():
    """Sanity: the two custom-type whitelists don't overlap."""
    assert OWNED_CUSTOM_TYPES.isdisjoint(FOREIGN_FAVA_CUSTOM_TYPES)


# --- decision grouping ----------------------------------------------------


class TestDecisionGroups:
    """``ImportAnalysis.decision_groups`` collapses identical rows. The
    Import preview UI uses it so a 1,200-row ledger renders as ~10
    grouped table rows instead of 1,200 individual ones."""

    def _decision(self, file: str, line: int, label: str, **kw) -> ImportDecision:
        return ImportDecision(
            source_file=file,
            source_line=line,
            directive_label=label,
            bucket=kw.get("bucket", "keep"),
            action=kw.get("action", "pass-through"),
            reversibility=kw.get("reversibility", "reversible"),
            reason=kw.get("reason", "ok"),
            target_file=kw.get("target_file"),
        )

    def test_identical_rows_collapse(self):
        decisions = tuple(
            self._decision("/x/connector_config.bean", 1199 + 14 * i, "Custom:entity")
            for i in range(20)
        )
        analysis = ImportAnalysis(
            source_dir=Path("/x"), source_main_bean=Path("/x/main.bean"),
            decisions=decisions,
        )
        groups = analysis.decision_groups
        assert len(groups) == 1
        assert groups[0].count == 20
        assert groups[0].directive_label == "Custom:entity"
        # Lines are preserved in source order for the expand-on-click view.
        assert groups[0].lines[0] == 1199
        assert groups[0].lines[-1] == 1199 + 14 * 19

    def test_different_directive_labels_split(self):
        decisions = (
            self._decision("/x/main.bean", 10, "Custom:entity"),
            self._decision("/x/main.bean", 20, "Custom:account-meta"),
            self._decision("/x/main.bean", 30, "Custom:entity"),
        )
        analysis = ImportAnalysis(
            source_dir=Path("/x"), source_main_bean=Path("/x/main.bean"),
            decisions=decisions,
        )
        groups = analysis.decision_groups
        assert len(groups) == 2
        # Insertion order preserved: entity first.
        assert groups[0].directive_label == "Custom:entity"
        assert groups[0].count == 2
        assert groups[1].directive_label == "Custom:account-meta"
        assert groups[1].count == 1

    def test_different_buckets_split(self):
        # Same directive label but different bucket / reason → separate groups.
        decisions = (
            self._decision("/x/main.bean", 10, "Custom:foo", bucket="keep", reason="ok"),
            self._decision("/x/main.bean", 20, "Custom:foo", bucket="foreign", reason="unknown"),
        )
        analysis = ImportAnalysis(
            source_dir=Path("/x"), source_main_bean=Path("/x/main.bean"),
            decisions=decisions,
        )
        assert len(analysis.decision_groups) == 2

    def test_line_summary_formats(self):
        # 1 line, 2 lines, ≥3 lines render distinct compact summaries.
        a = ImportAnalysis(
            source_dir=Path("/x"), source_main_bean=Path("/x/main.bean"),
            decisions=(
                self._decision("/x/main.bean", 5, "Custom:a"),
                self._decision("/x/main.bean", 10, "Custom:b"),
                self._decision("/x/main.bean", 15, "Custom:b"),
                self._decision("/x/main.bean", 20, "Custom:c"),
                self._decision("/x/main.bean", 25, "Custom:c"),
                self._decision("/x/main.bean", 99, "Custom:c"),
            ),
        )
        by_label = {g.directive_label: g for g in a.decision_groups}
        assert by_label["Custom:a"].line_summary == "5"
        assert by_label["Custom:b"].line_summary == "10, 15"
        assert by_label["Custom:c"].line_summary == "20–99 (3)"

    def test_grouping_through_analyze_import(self, tmp_path: Path):
        # End-to-end: many identical entity directives in a real ledger
        # collapse to one group.
        body = ['option "operating_currency" "USD"\n']
        for i in range(15):
            body.append(f'2020-01-01 custom "entity" "EntityA{i}"\n')
        _write_main(tmp_path, "".join(body))
        result = analyze_import(tmp_path)
        entity_groups = [
            g for g in result.decision_groups if g.directive_label == "Custom:entity"
        ]
        assert len(entity_groups) == 1
        assert entity_groups[0].count == 15

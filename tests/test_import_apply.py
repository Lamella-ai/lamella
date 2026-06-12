# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for bootstrap/import_apply.py — the Import Apply step."""
from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path

import pytest

from lamella.core.bootstrap.classifier import analyze_import
from lamella.core.bootstrap.import_apply import (
    DryRunReport,
    ImportApplyError,
    InstallCopyResult,
    apply_import,
    copy_bean_tree,
    copy_install_tree,
    plan_import,
)
from lamella.core.bootstrap.scaffold import scaffold_fresh
from lamella.core.bootstrap.templates import CANONICAL_FILES
from lamella.core.db import connect, migrate

FIXED = date(2026, 4, 21)


def _write_main(dir_: Path, content: str) -> None:
    (dir_ / "main.bean").write_text(content, encoding="utf-8")


# --- refusal ---------------------------------------------------------------


class TestRefusal:
    def test_blocked_analysis_refuses(self, tmp_path: Path):
        _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            'plugin "beancount_share.share"\n',
        )
        analysis = analyze_import(tmp_path)
        assert analysis.is_blocked
        with pytest.raises(ImportApplyError, match="blocked"):
            apply_import(tmp_path, analysis, on=FIXED)

    def test_missing_ledger_dir_raises(self, tmp_path: Path):
        _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n',
        )
        analysis = analyze_import(tmp_path)
        missing = tmp_path / "does-not-exist"
        with pytest.raises(ImportApplyError, match="does not exist"):
            apply_import(missing, analysis, on=FIXED)


# --- baseline happy path ---------------------------------------------------


class TestHappyPath:
    def test_scaffold_files_created(self, tmp_path: Path):
        _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            "2020-01-01 open Assets:Bank USD\n"
            "2020-01-01 open Income:Work USD\n"
            '2026-01-15 * "Paycheck"\n'
            "  Assets:Bank    1000 USD\n"
            "  Income:Work   -1000 USD\n",
        )
        analysis = analyze_import(tmp_path)
        result = apply_import(tmp_path, analysis, on=FIXED)

        for cfile in CANONICAL_FILES:
            assert (tmp_path / cfile.name).exists(), cfile.name
        assert set(p.name for p in result.files_created) == {
            cfile.name for cfile in CANONICAL_FILES
        }

    def test_version_stamp_added_to_main(self, tmp_path: Path):
        _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            "2020-01-01 open Assets:Bank USD\n"
            '2026-01-15 * "x"\n'
            "  Assets:Bank    10 USD\n"
            "  Assets:Bank   -10 USD\n",
        )
        analysis = analyze_import(tmp_path)
        result = apply_import(tmp_path, analysis, on=FIXED)
        from lamella.core.bootstrap.detection import LATEST_LEDGER_VERSION
        assert result.version_stamped
        main_text = (tmp_path / "main.bean").read_text(encoding="utf-8")
        assert (
            f'custom "lamella-ledger-version" "{LATEST_LEDGER_VERSION}"'
            in main_text
        )

    def test_existing_version_stamp_not_duplicated(self, tmp_path: Path):
        _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            '2026-01-01 custom "lamella-ledger-version" "1"\n'
            "2020-01-01 open Assets:Bank USD\n"
            '2026-01-15 * "x"\n'
            "  Assets:Bank    10 USD\n"
            "  Assets:Bank   -10 USD\n",
        )
        analysis = analyze_import(tmp_path)
        result = apply_import(tmp_path, analysis, on=FIXED)
        assert not result.version_stamped
        main_text = (tmp_path / "main.bean").read_text(encoding="utf-8")
        # Should appear exactly once.
        assert main_text.count('custom "lamella-ledger-version"') == 1

    def test_non_bean_files_ignored(self, tmp_path: Path):
        _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            "2020-01-01 open Assets:Bank USD\n"
            '2026-01-15 * "x"\n'
            "  Assets:Bank    10 USD\n"
            "  Assets:Bank   -10 USD\n",
        )
        (tmp_path / "notes.txt").write_text("preserved", encoding="utf-8")
        analysis = analyze_import(tmp_path)
        apply_import(tmp_path, analysis, on=FIXED)
        assert (tmp_path / "notes.txt").read_text(encoding="utf-8") == "preserved"


# --- transform application -------------------------------------------------


class TestTransformApplication:
    def test_fava_extension_commented_out(self, tmp_path: Path):
        _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            '2010-01-01 custom "fava-extension" "fava_dashboards"\n'
            "2020-01-01 open Assets:Bank USD\n"
            '2026-01-15 * "x"\n'
            "  Assets:Bank    10 USD\n"
            "  Assets:Bank   -10 USD\n",
        )
        analysis = analyze_import(tmp_path)
        apply_import(tmp_path, analysis, on=FIXED)

        main_text = (tmp_path / "main.bean").read_text(encoding="utf-8")
        assert "; [lamella-removed 2026-04-21" in main_text
        assert '; 2010-01-01 custom "fava-extension" "fava_dashboards"' in main_text
        # Original uncommented line must not remain active.
        lines = main_text.splitlines()
        active_fava = [l for l in lines if l.startswith('2010-01-01 custom "fava-extension"')]
        assert active_fava == []

    def test_multiline_fava_block_commented_out(self, tmp_path: Path):
        _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            '2024-01-01 custom "fava-extension" "fava_portfolio_returns" "{\n'
            "  some: config\n"
            '}"\n'
            "2020-01-01 open Assets:Bank USD\n"
            '2026-01-15 * "x"\n'
            "  Assets:Bank    10 USD\n"
            "  Assets:Bank   -10 USD\n",
        )
        analysis = analyze_import(tmp_path)
        apply_import(tmp_path, analysis, on=FIXED)

        main_text = (tmp_path / "main.bean").read_text(encoding="utf-8")
        # Closing }" must be commented, not bare.
        assert '\n}"\n' not in main_text
        assert '; }"' in main_text


# --- rollback --------------------------------------------------------------


class TestRollback:
    def test_bean_check_failure_restores_files(self, tmp_path: Path):
        _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            '2010-01-01 custom "fava-extension" "fava_dashboards"\n'
            "2020-01-01 open Assets:Bank USD\n"
            '2026-01-15 * "x"\n'
            "  Assets:Bank    10 USD\n"
            "  Assets:Bank   -10 USD\n",
        )
        original_main = (tmp_path / "main.bean").read_text(encoding="utf-8")
        analysis = analyze_import(tmp_path)

        def failing(_path: Path) -> list[str]:
            return ["synthetic error from test"]

        with pytest.raises(ImportApplyError, match="synthetic error"):
            apply_import(tmp_path, analysis, on=FIXED, bean_check=failing)

        # main.bean is restored exactly.
        assert (tmp_path / "main.bean").read_text(encoding="utf-8") == original_main
        # None of the canonical scaffold files should persist.
        for cfile in CANONICAL_FILES:
            path = tmp_path / cfile.name
            if cfile.name == "main.bean":
                continue
            # They may or may not have existed before; assert they don't
            # exist now since we know the original ledger didn't have them.
            assert not path.exists(), (
                f"rollback left {cfile.name} on disk"
            )

    def test_bean_check_success_produces_clean_result(self, tmp_path: Path):
        _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            '2010-01-01 custom "fava-extension" "fava_dashboards"\n'
            "2020-01-01 open Assets:Bank USD\n"
            '2026-01-15 * "x"\n'
            "  Assets:Bank    10 USD\n"
            "  Assets:Bank   -10 USD\n",
        )
        analysis = analyze_import(tmp_path)

        def passing(_path: Path) -> list[str]:
            return []

        result = apply_import(tmp_path, analysis, on=FIXED, bean_check=passing)
        assert result.ledger_dir == tmp_path
        assert result.version_stamped


# --- integration with real beancount --------------------------------------


class TestRealBeanCheck:
    def test_post_apply_parses_clean_without_fava_extensions(self, tmp_path: Path):
        _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            'plugin "beancount_lazy_plugins.auto_accounts"\n'
            '2010-01-01 custom "fava-extension" "fava_dashboards"\n'
            '1970-01-01 custom "fava-option" "default-page" "/x"\n'
            "2020-01-01 open Assets:Bank USD\n"
            '2026-01-15 * "x"\n'
            "  Assets:Bank    10 USD\n"
            "  Assets:Bank   -10 USD\n",
        )
        analysis = analyze_import(tmp_path)

        def real_check(path: Path) -> list[str]:
            from beancount import loader
            _entries, errors, _opts = loader.load_file(str(path))
            out: list[str] = []
            for e in errors:
                msg = getattr(e, "message", str(e))
                if "Auto-inserted" in msg:
                    continue
                src = getattr(e, "source", None)
                fn = (src or {}).get("filename", "") if isinstance(src, dict) else ""
                if isinstance(fn, str) and fn.startswith("<"):
                    continue
                out.append(msg)
            return out

        result = apply_import(tmp_path, analysis, on=FIXED, bean_check=real_check)
        assert result.version_stamped

        # Re-analyze: zero Transform decisions should remain (fava-
        # extensions were commented out), confirming §11 convergence.
        post = analyze_import(tmp_path)
        assert post.count_by_bucket["transform"] == 0


# --- dry-run mode (§9 "dry-run mode for this same flow") ------------------


class TestDryRun:
    def test_plan_import_returns_report_without_writing(self, tmp_path: Path):
        _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            '2010-01-01 custom "fava-extension" "fava_dashboards"\n'
            "2020-01-01 open Assets:Bank USD\n"
            '2026-01-15 * "x"\n'
            "  Assets:Bank    10 USD\n"
            "  Assets:Bank   -10 USD\n",
        )
        pre_main = (tmp_path / "main.bean").read_bytes()
        analysis = analyze_import(tmp_path)

        plan = plan_import(tmp_path, analysis, on=FIXED)

        assert isinstance(plan, DryRunReport)
        # Nothing written to disk.
        assert (tmp_path / "main.bean").read_bytes() == pre_main
        # No canonical files materialized.
        for cfile in CANONICAL_FILES:
            if cfile.name == "main.bean":
                continue
            assert not (tmp_path / cfile.name).exists()
        # Plan reports what *would* happen.
        assert plan.version_stamp_planned
        assert len(plan.transforms_planned) == 1
        assert plan.transforms_planned[0].directives_commented == 1
        assert 'custom "fava-extension"' in plan.transforms_planned[0].unified_diff
        to_create_names = {p.name for p in plan.files_to_create}
        assert "connector_links.bean" in to_create_names

    def test_apply_import_dry_run_flag_equivalent_to_plan_import(
        self, tmp_path: Path
    ):
        _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            "2020-01-01 open Assets:Bank USD\n"
            '2026-01-15 * "x"\n'
            "  Assets:Bank    10 USD\n"
            "  Assets:Bank   -10 USD\n",
        )
        analysis = analyze_import(tmp_path)
        via_flag = apply_import(tmp_path, analysis, on=FIXED, dry_run=True)
        via_fn = plan_import(tmp_path, analysis, on=FIXED)
        assert isinstance(via_flag, DryRunReport)
        assert via_flag.version_stamp_planned == via_fn.version_stamp_planned
        assert len(via_flag.files_to_create) == len(via_fn.files_to_create)

    def test_plan_import_blocked_analysis_refuses(self, tmp_path: Path):
        _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            'plugin "beancount_share.share"\n',
        )
        analysis = analyze_import(tmp_path)
        assert analysis.is_blocked
        with pytest.raises(ImportApplyError, match="blocked"):
            plan_import(tmp_path, analysis, on=FIXED)


# --- seed (§9 step 8) and reconstruct-failure rollback (§9 step 8.5) -----


def _fresh_db() -> sqlite3.Connection:
    conn = connect(Path(":memory:"))
    migrate(conn)
    return conn


class TestSeed:
    def test_seed_runs_when_conn_provided(self, tmp_path: Path):
        _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            'plugin "beancount_lazy_plugins.auto_accounts"\n'
            "2020-01-01 open Assets:Bank USD\n"
            '2026-01-15 * "Paycheck"\n'
            "  Assets:Bank    1000 USD\n"
            "  Equity:Opening-Balances\n",
        )
        analysis = analyze_import(tmp_path)
        conn = _fresh_db()

        def passing(_path: Path) -> list[str]:
            return []

        result = apply_import(
            tmp_path, analysis, on=FIXED, bean_check=passing, seed_conn=conn,
        )
        assert result.seed_ran is True
        # At least one reconstruct pass ran (document_dismissals etc).
        assert len(result.seed_reports) >= 1

    def test_seed_failure_rolls_back_files_and_wipes_state(
        self, tmp_path: Path
    ):
        _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            '2010-01-01 custom "fava-extension" "fava_dashboards"\n'
            "2020-01-01 open Assets:Bank USD\n"
            '2026-01-15 * "x"\n'
            "  Assets:Bank    10 USD\n"
            "  Assets:Bank   -10 USD\n",
        )
        original_main = (tmp_path / "main.bean").read_text(encoding="utf-8")
        analysis = analyze_import(tmp_path)
        conn = _fresh_db()

        # Pre-seed a state row so we can assert the rollback wipes it.
        conn.execute(
            "INSERT OR REPLACE INTO document_dismissals "
            "(txn_hash, reason, dismissed_by, dismissed_at) "
            "VALUES ('deadbeef', 'pre-existing', 'test', '2026-04-01T00:00:00Z')"
        )
        conn.commit()

        def passing(_path: Path) -> list[str]:
            return []

        def blow_up(_path: Path) -> list:
            raise RuntimeError("synthetic reconstruct failure")

        with pytest.raises(ImportApplyError, match="cannot interpret"):
            apply_import(
                tmp_path, analysis, on=FIXED,
                bean_check=passing, seed_conn=conn, seed_reader=blow_up,
            )

        # Ledger bytes restored exactly.
        assert (tmp_path / "main.bean").read_text(encoding="utf-8") == original_main
        # Canonical files rolled back (none existed before).
        for cfile in CANONICAL_FILES:
            if cfile.name == "main.bean":
                continue
            assert not (tmp_path / cfile.name).exists(), (
                f"rollback left {cfile.name} on disk"
            )
        # State tables wiped — the pre-existing row is gone too. That's
        # the documented trade-off of seed-failure rollback: DB matches
        # the rolled-back ledger (which had no state), so existing state
        # is sacrificed. Acceptable for first-run import.
        count = conn.execute(
            "SELECT COUNT(*) FROM document_dismissals"
        ).fetchone()[0]
        assert count == 0


# --- convergence (§11: Keep-bucket-only on re-import of canonical) -------


class TestConvergence:
    def test_scaffolded_ledger_imports_as_all_keep(self, tmp_path: Path):
        """A freshly scaffolded canonical ledger analyzed against
        analyze_import must produce zero Transform and zero Foreign
        decisions — the §11 convergence test."""
        ledger_dir = tmp_path / "fresh"
        ledger_dir.mkdir()

        def real_check(path: Path) -> list[str]:
            from beancount import loader

            _entries, errors, _opts = loader.load_file(str(path))
            out: list[str] = []
            for e in errors:
                msg = getattr(e, "message", str(e))
                if "Auto-inserted" in msg:
                    continue
                src = getattr(e, "source", None)
                fn = (src or {}).get("filename", "") if isinstance(src, dict) else ""
                if isinstance(fn, str) and fn.startswith("<"):
                    continue
                out.append(msg)
            return out

        scaffold_fresh(ledger_dir, bean_check=real_check)

        analysis = analyze_import(ledger_dir)
        assert not analysis.is_blocked
        counts = analysis.count_by_bucket
        assert counts["transform"] == 0, (
            f"canonical scaffold should need zero transforms; got {counts}"
        )
        assert counts["foreign"] == 0, (
            f"canonical scaffold should have zero foreign directives; got {counts}"
        )

    def test_imported_ledger_reimports_as_all_keep(self, tmp_path: Path):
        """Apply a real import, then re-run analyze_import — the
        result must land entirely in Keep. This is the full round-trip
        version of the convergence assertion."""
        _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            'plugin "beancount_lazy_plugins.auto_accounts"\n'
            '2010-01-01 custom "fava-extension" "fava_dashboards"\n'
            '1970-01-01 custom "fava-option" "default-page" "/x"\n'
            "2020-01-01 open Assets:Bank USD\n"
            '2026-01-15 * "x"\n'
            "  Assets:Bank    10 USD\n"
            "  Assets:Bank   -10 USD\n",
        )

        def real_check(path: Path) -> list[str]:
            from beancount import loader

            _entries, errors, _opts = loader.load_file(str(path))
            out: list[str] = []
            for e in errors:
                msg = getattr(e, "message", str(e))
                if "Auto-inserted" in msg:
                    continue
                src = getattr(e, "source", None)
                fn = (src or {}).get("filename", "") if isinstance(src, dict) else ""
                if isinstance(fn, str) and fn.startswith("<"):
                    continue
                out.append(msg)
            return out

        analysis = analyze_import(tmp_path)
        apply_import(tmp_path, analysis, on=FIXED, bean_check=real_check)

        post = analyze_import(tmp_path)
        assert not post.is_blocked
        counts = post.count_by_bucket
        assert counts["transform"] == 0, (
            f"post-import should have zero transforms left; got {counts}"
        )
        # Foreign is expected to drop to zero for this fixture.
        assert counts["foreign"] == 0, (
            f"post-import should have zero foreign directives; got {counts}"
        )


# --- copy_bean_tree -------------------------------------------------------


class TestCopyBeanTree:
    """``copy_bean_tree`` is the helper /setup/import uses when the
    user types a source directory other than ``settings.ledger_dir`` —
    we copy every .bean file (including subdirs like
    ``connector_imports/``) into the active ledger dir, then run the
    apply flow against the copy. The originals are never mutated."""

    def test_copies_top_level_bean_files(self, tmp_path: Path):
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        src.mkdir()
        (src / "main.bean").write_text("a", encoding="utf-8")
        (src / "accounts.bean").write_text("b", encoding="utf-8")

        created = copy_bean_tree(src, dst)

        assert sorted(p.name for p in created) == ["accounts.bean", "main.bean"]
        assert (dst / "main.bean").read_text(encoding="utf-8") == "a"
        assert (dst / "accounts.bean").read_text(encoding="utf-8") == "b"

    def test_preserves_subdirectory_structure(self, tmp_path: Path):
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        (src / "connector_imports").mkdir(parents=True)
        (src / "main.bean").write_text("m", encoding="utf-8")
        (src / "connector_imports" / "2024.bean").write_text("y24", encoding="utf-8")

        copy_bean_tree(src, dst)

        assert (dst / "main.bean").read_text(encoding="utf-8") == "m"
        assert (dst / "connector_imports" / "2024.bean").read_text(encoding="utf-8") == "y24"

    def test_skips_non_bean_files(self, tmp_path: Path):
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        src.mkdir()
        (src / "main.bean").write_text("m", encoding="utf-8")
        (src / "README.md").write_text("readme", encoding="utf-8")
        (src / "secret.txt").write_text("secret", encoding="utf-8")

        copy_bean_tree(src, dst)

        assert (dst / "main.bean").exists()
        assert not (dst / "README.md").exists()
        assert not (dst / "secret.txt").exists()

    def test_refuses_when_destination_has_bean_files(self, tmp_path: Path):
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        src.mkdir()
        dst.mkdir()
        (src / "main.bean").write_text("m", encoding="utf-8")
        (dst / "main.bean").write_text("existing", encoding="utf-8")

        with pytest.raises(FileExistsError, match="refusing to overwrite"):
            copy_bean_tree(src, dst)
        # Existing destination file is untouched.
        assert (dst / "main.bean").read_text(encoding="utf-8") == "existing"

    def test_source_nested_in_empty_destination_is_allowed(self, tmp_path: Path):
        # Regression: user typed source=/ledger/temp with active
        # ledger=/ledger. Top-level /ledger is empty so the import
        # must proceed even though rglob would have found bean files
        # under /ledger/temp.
        dst = tmp_path / "ledger"
        src = dst / "temp"
        src.mkdir(parents=True)
        (src / "main.bean").write_text("m", encoding="utf-8")
        (src / "accounts.bean").write_text("a", encoding="utf-8")

        created = copy_bean_tree(src, dst)

        assert sorted(p.name for p in created) == ["accounts.bean", "main.bean"]
        # Files land at the top of dst.
        assert (dst / "main.bean").read_text(encoding="utf-8") == "m"
        # Original source files in the nested subdir untouched.
        assert (src / "main.bean").read_text(encoding="utf-8") == "m"

    def test_destination_inside_source_refused(self, tmp_path: Path):
        # Inverse of the regression above — dst inside src would have
        # the copy write into the source tree while we walk it.
        src = tmp_path / "ledger"
        dst = src / "active"
        src.mkdir()
        (src / "main.bean").write_text("m", encoding="utf-8")

        with pytest.raises(ValueError, match="inside source"):
            copy_bean_tree(src, dst)

    def test_creates_destination_directory_if_absent(self, tmp_path: Path):
        src = tmp_path / "src"
        dst = tmp_path / "deeply" / "nested" / "new"
        src.mkdir()
        (src / "main.bean").write_text("m", encoding="utf-8")

        copy_bean_tree(src, dst)

        assert dst.is_dir()
        assert (dst / "main.bean").exists()

    def test_missing_source_raises(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError, match="does not exist"):
            copy_bean_tree(tmp_path / "nope", tmp_path / "dst")

    def test_source_is_not_mutated(self, tmp_path: Path):
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        src.mkdir()
        original = "option \"operating_currency\" \"USD\"\n"
        (src / "main.bean").write_text(original, encoding="utf-8")

        copy_bean_tree(src, dst)

        # Source content unchanged.
        assert (src / "main.bean").read_text(encoding="utf-8") == original


# --- copy_install_tree ----------------------------------------------------


class TestCopyInstallTree:
    """``copy_install_tree`` extends ``copy_bean_tree`` with an
    allowlist for non-.bean install assets — mileage CSVs, SimpleFIN
    account map, importer configs, prices configs, custom importer
    scripts. Phase 1 of the /setup/recovery work."""

    def test_copies_mileage_csv(self, tmp_path: Path):
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        (src / "mileage").mkdir(parents=True)
        (src / "main.bean").write_text("m", encoding="utf-8")
        (src / "mileage" / "vehicles.csv").write_text(
            "date,vehicle\n2024-01-01,Fabrikam\n", encoding="utf-8"
        )

        result = copy_install_tree(src, dst)

        assert isinstance(result, InstallCopyResult)
        assert (dst / "mileage" / "vehicles.csv").exists()
        assert any(p.name == "vehicles.csv" for p in result.extra_files)
        assert any(p.name == "main.bean" for p in result.bean_files)

    def test_copies_simplefin_map_yaml(self, tmp_path: Path):
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        src.mkdir()
        (src / "main.bean").write_text("m", encoding="utf-8")
        (src / "simplefin_account_map.yml").write_text(
            "ACC_123: Assets:Personal:Checking\n", encoding="utf-8"
        )

        result = copy_install_tree(src, dst)

        assert (dst / "simplefin_account_map.yml").exists()
        assert any(
            p.name == "simplefin_account_map.yml" for p in result.extra_files
        )

    def test_copies_importer_configs_and_scripts(self, tmp_path: Path):
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        (src / "importers").mkdir(parents=True)
        (src / "scripts").mkdir(parents=True)
        (src / "main.bean").write_text("m", encoding="utf-8")
        (src / "importers_config.yml").write_text("foo: bar\n", encoding="utf-8")
        (src / "prices_config.yml").write_text("baz: qux\n", encoding="utf-8")
        (src / "importers" / "wells_fargo.py").write_text("# importer\n", encoding="utf-8")
        (src / "scripts" / "backup.sh").write_text("#!/bin/sh\n", encoding="utf-8")

        result = copy_install_tree(src, dst)

        assert (dst / "importers_config.yml").exists()
        assert (dst / "prices_config.yml").exists()
        assert (dst / "importers" / "wells_fargo.py").exists()
        assert (dst / "scripts" / "backup.sh").exists()
        # All four extras land in extra_files (plus any others).
        names = {p.name for p in result.extra_files}
        assert {"importers_config.yml", "prices_config.yml",
                "wells_fargo.py", "backup.sh"} <= names

    def test_skips_secret_named_files(self, tmp_path: Path):
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        src.mkdir()
        (src / "main.bean").write_text("m", encoding="utf-8")
        # Each of these matches an allowlist glob shape but should
        # be skipped because the filename is secret-shaped. We use
        # importers_config-prefixed names so they match the glob.
        (src / "importers_config_token.yml").write_text("x", encoding="utf-8")
        (src / "importers_config.key").write_text("x", encoding="utf-8")
        # And a clearly-named secret in the importers/ tree.
        (src / "importers").mkdir()
        (src / "importers" / "credentials.py").write_text("x", encoding="utf-8")

        result = copy_install_tree(src, dst)

        assert not (dst / "importers_config_token.yml").exists()
        assert not (dst / "importers_config.key").exists()
        assert not (dst / "importers" / "credentials.py").exists()
        # Skipped secrets are surfaced so the post-import UI can
        # tell the user to re-enter them.
        assert len(result.skipped_secrets) >= 1
        skipped_names = {p.name for p in result.skipped_secrets}
        assert "credentials.py" in skipped_names

    def test_skips_unallowlisted_files(self, tmp_path: Path):
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        src.mkdir()
        (src / "main.bean").write_text("m", encoding="utf-8")
        (src / "README.md").write_text("readme", encoding="utf-8")
        (src / "scratch.txt").write_text("notes", encoding="utf-8")
        (src / "data.json").write_text("{}", encoding="utf-8")

        result = copy_install_tree(src, dst)

        # None of these are in INSTALL_NON_BEAN_GLOBS — silently dropped.
        assert not (dst / "README.md").exists()
        assert not (dst / "scratch.txt").exists()
        assert not (dst / "data.json").exists()
        assert result.extra_files == ()

    def test_dedups_when_globs_overlap(self, tmp_path: Path):
        # importers/**/*.py would match a file even if a future glob
        # also matches it. The dedup pass keeps each file at one copy.
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        (src / "importers" / "sub").mkdir(parents=True)
        (src / "main.bean").write_text("m", encoding="utf-8")
        (src / "importers" / "sub" / "x.py").write_text("# x\n", encoding="utf-8")

        result = copy_install_tree(src, dst)

        names = [p.name for p in result.extra_files]
        assert names.count("x.py") == 1

    def test_compat_shim_returns_only_bean_files(self, tmp_path: Path):
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        (src / "mileage").mkdir(parents=True)
        (src / "main.bean").write_text("m", encoding="utf-8")
        (src / "mileage" / "vehicles.csv").write_text("x,y\n", encoding="utf-8")

        # Old API: returns just .bean files. The CSV is still copied
        # to disk, but doesn't appear in the return value.
        out = copy_bean_tree(src, dst)

        assert all(p.suffix == ".bean" for p in out)
        assert (dst / "mileage" / "vehicles.csv").exists()

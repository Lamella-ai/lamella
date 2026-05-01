# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from beancount import loader

from lamella.features.receipts.dismissals_writer import (
    append_dismissal,
    append_dismissal_revoke,
    read_dismissals_from_entries,
)


def _setup(ledger_dir: Path) -> tuple[Path, Path]:
    main_bean = ledger_dir / "main.bean"
    connector_links = ledger_dir / "connector_links.bean"
    return main_bean, connector_links


def _load(main_bean: Path) -> list:
    entries, _errors, _options = loader.load_file(str(main_bean))
    return list(entries)


def test_append_dismissal_writes_valid_directive(ledger_dir: Path):
    main_bean, connector_links = _setup(ledger_dir)
    block = append_dismissal(
        connector_links=connector_links,
        main_bean=main_bean,
        txn_hash="deadbeef",
        reason="cash tip",
        run_check=False,
    )
    assert 'custom "receipt-dismissed" "deadbeef"' in block
    assert 'lamella-dismissed-at:' in block
    assert 'lamella-reason: "cash tip"' in block
    # Ledger still parses.
    entries, errors, _ = loader.load_file(str(main_bean))
    assert errors == []


def test_dismissal_is_idempotent_by_read_path(ledger_dir: Path):
    main_bean, connector_links = _setup(ledger_dir)
    append_dismissal(
        connector_links=connector_links, main_bean=main_bean,
        txn_hash="hash-a", run_check=False,
    )
    append_dismissal(
        connector_links=connector_links, main_bean=main_bean,
        txn_hash="hash-a", reason="updated", run_check=False,
    )
    rows = read_dismissals_from_entries(_load(main_bean))
    # Last write wins — one active dismissal for hash-a, with the newer reason.
    by_hash = {r["txn_hash"]: r for r in rows}
    assert "hash-a" in by_hash
    assert by_hash["hash-a"]["reason"] == "updated"


def test_revoke_removes_dismissal(ledger_dir: Path):
    main_bean, connector_links = _setup(ledger_dir)
    append_dismissal(
        connector_links=connector_links, main_bean=main_bean,
        txn_hash="hash-b", reason="tip", run_check=False,
    )
    append_dismissal_revoke(
        connector_links=connector_links, main_bean=main_bean,
        txn_hash="hash-b", run_check=False,
    )
    rows = read_dismissals_from_entries(_load(main_bean))
    assert not any(r["txn_hash"] == "hash-b" for r in rows)


def test_dismiss_after_revoke_restores(ledger_dir: Path):
    main_bean, connector_links = _setup(ledger_dir)
    append_dismissal(
        connector_links=connector_links, main_bean=main_bean,
        txn_hash="hash-c", run_check=False,
    )
    append_dismissal_revoke(
        connector_links=connector_links, main_bean=main_bean,
        txn_hash="hash-c", run_check=False,
    )
    append_dismissal(
        connector_links=connector_links, main_bean=main_bean,
        txn_hash="hash-c", reason="re-dismissed", run_check=False,
    )
    rows = read_dismissals_from_entries(_load(main_bean))
    by_hash = {r["txn_hash"]: r for r in rows}
    assert by_hash["hash-c"]["reason"] == "re-dismissed"


def test_reconstruct_rebuilds_dismissals(ledger_dir: Path, tmp_path):
    from lamella.core.db import connect, migrate

    main_bean, connector_links = _setup(ledger_dir)
    append_dismissal(
        connector_links=connector_links, main_bean=main_bean,
        txn_hash="rc-1", reason="parking", run_check=False,
    )
    append_dismissal(
        connector_links=connector_links, main_bean=main_bean,
        txn_hash="rc-2", run_check=False,
    )
    append_dismissal_revoke(
        connector_links=connector_links, main_bean=main_bean,
        txn_hash="rc-2", run_check=False,
    )

    db = connect(tmp_path / "reconstruct.sqlite")
    migrate(db)

    # Import triggers registration.
    import lamella.core.transform.steps.step1_receipt_dismissals  # noqa: F401
    from lamella.core.transform.reconstruct import run_all

    entries = _load(main_bean)
    reports = run_all(db, entries)
    assert any(r.pass_name == "step1:receipt-dismissals" for r in reports)

    rows = db.execute(
        "SELECT txn_hash, reason FROM receipt_dismissals ORDER BY txn_hash"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["txn_hash"] == "rc-1"
    assert rows[0]["reason"] == "parking"


def test_orphan_dismissal_detection(ledger_dir: Path, tmp_path):
    """Dismissals whose txn_hash no longer matches any current txn
    should be surfaced as orphans so the UI can explain why they
    re-appeared."""
    from lamella.core.db import connect, migrate
    from lamella.features.receipts.needs_queue import find_orphan_dismissals

    main_bean, _connector_links = _setup(ledger_dir)
    db = connect(tmp_path / "orphan.sqlite")
    migrate(db)
    db.execute(
        "INSERT INTO receipt_dismissals (txn_hash, reason) VALUES (?, ?)",
        ("nonexistent-hash", "previously dismissed"),
    )
    orphans = find_orphan_dismissals(_load(main_bean), db)
    assert len(orphans) == 1
    assert orphans[0]["txn_hash"] == "nonexistent-hash"
    assert orphans[0]["reason"] == "previously dismissed"

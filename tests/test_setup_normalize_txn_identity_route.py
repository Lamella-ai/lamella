# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""POST /setup/normalize-txn-identity — the recovery surface for the
bulk identity normalization. Self-healing on-touch flow handles the
common case; this action exists for users who want disk content
cleaned up all at once instead of letting it converge over time."""
from __future__ import annotations


def _seed_legacy_simplefin_entry(ledger_dir):
    """Stamp a legacy ``lamella-simplefin-id`` on the fixture's
    Hardware Store entry so we can verify the recovery action
    converts it to the new schema on disk."""
    sf_path = ledger_dir / "simplefin_transactions.bean"
    text = sf_path.read_text(encoding="utf-8")
    new_text = text.replace(
        '  simplefin-id: "sf-1001"\n',
        '  simplefin-id: "sf-1001"\n'
        '  lamella-simplefin-id: "sf-1001"\n',
    )
    assert new_text != text, "fixture must contain the sf-1001 line"
    sf_path.write_text(new_text, encoding="utf-8")


def test_recovery_route_normalizes_legacy_identity_on_disk(
    app_client, ledger_dir,
):
    """The route runs the bulk normalization end-to-end: mints
    lineage on legacy entries, drops the txn-level legacy keys,
    and stamps paired source meta on the source-side posting."""
    _seed_legacy_simplefin_entry(ledger_dir)
    sf_path = ledger_dir / "simplefin_transactions.bean"
    pre = sf_path.read_text(encoding="utf-8")
    # The fixture is now pre-stamped with lineage as part of the v2
    # ledger migration, so the legacy artefact under test is the txn-
    # level lamella-simplefin-id key (not the absence of lamella-txn-id).
    assert "lamella-simplefin-id" in pre

    r = app_client.post("/setup/normalize-txn-identity", follow_redirects=False)
    assert r.status_code == 303, r.text
    assert "/setup/recovery" in r.headers["location"]
    assert "info=normalized" in r.headers["location"]

    post = sf_path.read_text(encoding="utf-8")
    # Lineage minted on the previously-legacy entry.
    assert "lamella-txn-id:" in post
    # Paired indexed source meta now lives on the source-side posting.
    assert 'lamella-source-0: "simplefin"' in post
    assert 'lamella-source-reference-id-0: "sf-1001"' in post


def test_recovery_route_idempotent_on_already_normalized_ledger(
    app_client, ledger_dir,
):
    """A second invocation reports already-normalized and writes
    nothing further — the contract for any recovery action that
    converges on a clean state."""
    _seed_legacy_simplefin_entry(ledger_dir)
    # First run does the work.
    r1 = app_client.post("/setup/normalize-txn-identity", follow_redirects=False)
    assert r1.status_code == 303

    # Second run is a no-op.
    sf_path = ledger_dir / "simplefin_transactions.bean"
    after_first = sf_path.read_text(encoding="utf-8")
    r2 = app_client.post("/setup/normalize-txn-identity", follow_redirects=False)
    assert r2.status_code == 303
    assert "identity-already-normalized" in r2.headers["location"]
    assert sf_path.read_text(encoding="utf-8") == after_first

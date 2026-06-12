# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Workstream C2.2 / C2.3 — /review/staged/classify-group + post-scan.

Invariants:
  * N staged rows + one target → N clean Beancount writes in a
    single bean-check pass.
  * learn_from_decision is called exactly **once** per group with
    the prototype row's payee/card, producing one user-rule at
    hit_count = 1.
  * After the write, FixmeScanner runs once and auto-applies the
    new rule to any pre-existing ledger FIXMEs matching the
    group's pattern (the "confirm one, next 99 free" property
    from docs/specs/AI-CLASSIFICATION.md).
"""
from __future__ import annotations

from decimal import Decimal
from pathlib import Path


def _stage_three_amazon_rows(app_client) -> list[int]:
    """Stage three AMAZON rows via /intake/stage and return the
    staged_ids in order."""
    app_client.post(
        "/intake/stage",
        data={
            "text": (
                "Date,Amount,Description\n"
                "2026-04-20,-12.34,AMAZON\n"
                "2026-04-21,-22.50,amazon\n"
                "2026-04-22,-7.10,Amazon\n"
            ),
            "has_header": "1",
        },
    )
    import re
    r = app_client.get("/review/staged")
    ids = [int(m) for m in re.findall(
        r'name="staged_id" value="(\d+)"', r.text,
    )]
    # The group has 3 items; each row's forms also list the id. We
    # care about distinct ids.
    return sorted(set(ids))


def test_classify_group_requires_at_least_one_id(app_client):
    r = app_client.post(
        "/review/staged/classify-group",
        data={"target_account": "Expenses:X"},
        follow_redirects=False,
    )
    # FastAPI Form(...) rejects missing staged_ids with 422.
    assert r.status_code in (400, 422)


def test_classify_group_writes_once_per_row_and_one_rule(
    app_client, settings, monkeypatch
):
    """Three AMAZON rows classified as Expenses:Personal:Amazon →
    three SimpleFIN-format entries appear in the file, and exactly
    ONE user rule gets created for the group (not three)."""
    # The conftest monkeypatch catches receipts.linker.run_bean_check
    # but simplefin.writer imported the name by reference, so we
    # patch the writer-local binding too.
    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check",
        lambda main_bean: None,
    )
    from lamella.features.import_.staging import StagingService
    db = app_client.app.state.db
    svc = StagingService(db)
    # Register a SimpleFIN account binding so _resolve_account_path
    # can find the ledger account for the staged rows.
    db.execute(
        "INSERT INTO accounts_meta (account_path, display_name, "
        "simplefin_account_id) VALUES (?, ?, ?)",
        ("Liabilities:Personal:Card:CardA1234", "CardA 1234", "sf-acct-a"),
    )
    db.commit()
    # The classify-group route enforces target-account is opened on
    # the txn date. Append the Open directive so the test mirrors
    # the real precondition.
    accounts_path = settings.ledger_dir / "accounts.bean"
    accounts_path.write_text(
        accounts_path.read_text(encoding="utf-8")
        + "\n2023-01-01 open Expenses:Personal:Amazon USD\n",
        encoding="utf-8",
    )
    app_client.app.state.ledger_reader.invalidate()

    staged_ids: list[int] = []
    for i in range(3):
        row = svc.stage(
            source="simplefin",
            source_ref={"account_id": "sf-acct-a", "txn_id": f"sf-{i}"},
            posting_date="2026-04-20",
            amount="-12.34",
            currency="USD",
            payee="AMAZON",
            description=None,
        )
        staged_ids.append(row.id)
    db.commit()

    r = app_client.post(
        "/review/staged/classify-group",
        data={
            "staged_ids": [str(sid) for sid in staged_ids],
            "target_account": "Expenses:Personal:Amazon",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303, r.text

    # Every staged row should now be promoted.
    for sid in staged_ids:
        row = svc.get(sid)
        assert row is not None
        assert row.status == "promoted"

    # Exactly one user-rule created for the group.
    rule_rows = db.execute(
        "SELECT id, pattern_value, card_account, target_account, "
        "       created_by, hit_count "
        "  FROM classification_rules "
        " WHERE target_account = ? AND created_by = 'user'",
        ("Expenses:Personal:Amazon",),
    ).fetchall()
    assert len(rule_rows) == 1, (
        f"expected ONE user-rule, got {len(rule_rows)}: "
        f"{[dict(r) for r in rule_rows]}"
    )
    assert rule_rows[0]["pattern_value"] == "AMAZON"
    assert rule_rows[0]["card_account"] == "Liabilities:Personal:Card:CardA1234"
    # Exactly ONE hit (not three). If a future edit loops
    # learn_from_decision inside the row loop and bumps per-row,
    # this will flip to 3 and flag the regression.
    assert rule_rows[0]["hit_count"] == 1


def test_classify_group_post_scan_resolves_preexisting_fixmes(
    app_client, settings, monkeypatch
):
    """C2.3 — 'confirm one, next 99 free'. Pre-existing FIXMEs in
    the ledger matching the group's pattern auto-resolve via the
    user-rule the group decision creates, without another LLM call.
    """
    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check",
        lambda main_bean: None,
    )
    # The OverrideWriter under FixmeScanner also runs bean-check via
    # receipts.linker; silence that path for the test too.
    monkeypatch.setattr(
        "lamella.features.rules.overrides.run_bean_check",
        lambda main_bean: None,
    )

    from lamella.features.import_.staging import StagingService

    db = app_client.app.state.db
    svc = StagingService(db)

    # Seed an accounts_meta binding so _resolve_account_path can
    # find the ledger account backing the SimpleFIN row. The boot
    # scan may have already created the row from the fixture; use
    # an UPDATE-by-path so we own the simplefin_account_id.
    db.execute(
        """
        INSERT INTO accounts_meta (account_path, display_name,
                                   simplefin_account_id)
        VALUES (?, ?, ?)
        ON CONFLICT(account_path) DO UPDATE SET
            simplefin_account_id = excluded.simplefin_account_id
        """,
        ("Liabilities:Acme:Card:CardA1234", "CardA Acme", "sf-card-a-acme"),
    )
    db.commit()

    # Drop three pre-existing AMAZON FIXMEs into the ledger fixture
    # — these are what the post-classify scan should auto-resolve.
    ledger_dir = settings.ledger_dir
    accounts_path = ledger_dir / "accounts.bean"
    accounts_path.write_text(
        accounts_path.read_text(encoding="utf-8")
        + "\n2023-01-01 open Expenses:FIXME USD\n",
        encoding="utf-8",
    )
    transactions_path = ledger_dir / "simplefin_transactions.bean"
    transactions_path.write_text(
        transactions_path.read_text(encoding="utf-8")
        + (
            "\n"
            '2026-04-01 * "AMAZON" "pre-existing FIXME A"\n'
            '  simplefin-id: "sf-amzn-a"\n'
            "  Liabilities:Acme:Card:CardA1234  -11.11 USD\n"
            "  Expenses:FIXME                    11.11 USD\n"
            "\n"
            '2026-04-02 * "AMAZON" "pre-existing FIXME B"\n'
            '  simplefin-id: "sf-amzn-b"\n'
            "  Liabilities:Acme:Card:CardA1234  -22.22 USD\n"
            "  Expenses:FIXME                    22.22 USD\n"
            "\n"
            '2026-04-03 * "AMAZON" "pre-existing FIXME C"\n'
            '  simplefin-id: "sf-amzn-c"\n'
            "  Liabilities:Acme:Card:CardA1234  -33.33 USD\n"
            "  Expenses:FIXME                    33.33 USD\n"
        ),
        encoding="utf-8",
    )
    # Invalidate the server's reader so it picks up the appended
    # FIXMEs when classify-group runs the post-scan.
    app_client.app.state.ledger_reader.invalidate()

    # Stage one new SimpleFIN AMAZON row (not yet in the bean).
    staged_row = svc.stage(
        source="simplefin",
        source_ref={"account_id": "sf-card-a-acme", "txn_id": "sf-amzn-new"},
        posting_date="2026-04-20",
        amount="-44.44",
        currency="USD",
        payee="AMAZON",
        description=None,
    )
    db.commit()

    # Classify the staged row as Expenses:Acme:Supplies. The
    # post-write scan should then find the three pre-existing
    # FIXMEs and auto-apply the new user-rule to each of them.
    r = app_client.post(
        "/review/staged/classify-group",
        data={
            "staged_ids": [str(staged_row.id)],
            "target_account": "Expenses:Acme:Supplies",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303

    # The rule was created exactly once.
    rule_rows = db.execute(
        "SELECT id, hit_count FROM classification_rules "
        " WHERE created_by = 'user' AND target_account = ?",
        ("Expenses:Acme:Supplies",),
    ).fetchall()
    assert len(rule_rows) == 1
    # hit_count: +1 from learn_from_decision, +3 from the scan's
    # auto-apply (one bump per pre-existing FIXME it resolved).
    assert rule_rows[0]["hit_count"] == 4

    # The overrides file now carries three auto-applied rows for
    # the pre-existing FIXMEs. All went through the rule, not the
    # LLM.
    overrides_text = settings.connector_overrides_path.read_text(
        encoding="utf-8"
    )
    assert "sf-amzn-a" in overrides_text or "11.11" in overrides_text
    assert "sf-amzn-b" in overrides_text or "22.22" in overrides_text
    assert "sf-amzn-c" in overrides_text or "33.33" in overrides_text


def test_classify_group_skips_already_promoted_rows(
    app_client, settings, monkeypatch
):
    """Idempotent on retry: re-submitting a group whose members were
    already promoted returns 303 but doesn't double-write or create
    a second rule."""
    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check",
        lambda main_bean: None,
    )
    from lamella.features.import_.staging import StagingService
    db = app_client.app.state.db
    svc = StagingService(db)
    db.execute(
        "INSERT INTO accounts_meta (account_path, display_name, "
        "simplefin_account_id) VALUES (?, ?, ?)",
        ("Liabilities:Personal:Card:CardA1234", "CardA 1234", "sf-acct-a"),
    )
    db.commit()
    accounts_path = settings.ledger_dir / "accounts.bean"
    accounts_path.write_text(
        accounts_path.read_text(encoding="utf-8")
        + "\n2023-01-01 open Expenses:Personal:Other USD\n",
        encoding="utf-8",
    )
    app_client.app.state.ledger_reader.invalidate()

    staged_ids: list[int] = []
    for i in range(2):
        row = svc.stage(
            source="simplefin",
            source_ref={"account_id": "sf-acct-a", "txn_id": f"sf-{i}"},
            posting_date="2026-04-20",
            amount="-10.00",
            currency="USD",
            payee="SOMESTORE",
            description=None,
        )
        staged_ids.append(row.id)
    db.commit()

    # First classify.
    r1 = app_client.post(
        "/review/staged/classify-group",
        data={
            "staged_ids": [str(sid) for sid in staged_ids],
            "target_account": "Expenses:Personal:Other",
        },
        follow_redirects=False,
    )
    assert r1.status_code == 303

    # Second classify — all rows already promoted.
    r2 = app_client.post(
        "/review/staged/classify-group",
        data={
            "staged_ids": [str(sid) for sid in staged_ids],
            "target_account": "Expenses:Personal:Other",
        },
        follow_redirects=False,
    )
    assert r2.status_code == 303
    assert "nothing_to_do" in r2.headers.get("location", "")

    # Still exactly one rule — the retry didn't create a second.
    rule_rows = db.execute(
        "SELECT id FROM classification_rules WHERE created_by = 'user' "
        "  AND target_account = ?",
        ("Expenses:Personal:Other",),
    ).fetchall()
    assert len(rule_rows) == 1

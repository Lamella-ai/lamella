# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Auto-scaffold target account on classify when it extends an
existing opened branch.

Live failure this guards: the staged-review account picker is a
free-text input backed by a `<datalist>` of opened accounts, so
the user can type any string. When they refine an existing bucket
(e.g. typing `Expenses:Acme:Supplies:Tape` while only
`Expenses:Acme:Supplies` is opened) the classify endpoint should
auto-open the new leaf in `connector_accounts.bean` rather than
reject with "account doesn't exist." Brand-new top-level entities
or typo'd parallel branches are still rejected.
"""
from __future__ import annotations

from lamella.features.import_.staging import StagingService


def _seed_card(db):
    db.execute(
        """
        INSERT INTO accounts_meta (account_path, display_name,
                                   simplefin_account_id)
        VALUES (?, ?, ?)
        ON CONFLICT(account_path) DO UPDATE SET
            simplefin_account_id = excluded.simplefin_account_id
        """,
        ("Liabilities:Acme:Card:CardA1234", "CardA Acme", "sf-acct-x"),
    )
    db.commit()


def _stage_simplefin(db, txn_id: str, posting_date: str, payee="Acme"):
    svc = StagingService(db)
    return svc.stage(
        source="simplefin",
        source_ref={"account_id": "sf-acct-x", "txn_id": txn_id},
        posting_date=posting_date,
        amount="-25.00",
        currency="USD",
        payee=payee,
        description=None,
    ).id


def test_classify_auto_scaffolds_child_of_opened_account(
    app_client, settings, monkeypatch,
):
    """User typed `Expenses:Acme:Supplies:Tape` — parent
    `Expenses:Acme:Supplies` is already opened in the fixture, so
    classify should open the new leaf and proceed."""
    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check",
        lambda main_bean: None,
    )
    monkeypatch.setattr(
        "lamella.core.registry.accounts_writer.AccountsWriter._check",
        lambda self, baseline=None: None,
    )
    db = app_client.app.state.db
    _seed_card(db)

    staged_id = _stage_simplefin(
        db, txn_id="sf-extend-1", posting_date="2024-03-15",
    )
    r = app_client.post(
        "/review/staged/classify",
        data={
            "staged_id": staged_id,
            "target_account": "Expenses:Acme:Supplies:Tape",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303, r.text

    # Verify the Open landed in connector_accounts.bean dated on or
    # before the txn date.
    text = settings.connector_accounts_path.read_text(encoding="utf-8")
    assert "open Expenses:Acme:Supplies:Tape" in text


def test_classify_auto_scaffolds_sibling_under_known_entity(
    app_client, settings, monkeypatch,
):
    """User typed `Expenses:Acme:NewBucket` — parent `Expenses:Acme`
    is not itself opened but is a prefix of opened accounts
    (`Expenses:Acme:Supplies`, `Expenses:Acme:Shipping`), so the
    branch is recognized as legitimate and the new leaf opens."""
    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check",
        lambda main_bean: None,
    )
    monkeypatch.setattr(
        "lamella.core.registry.accounts_writer.AccountsWriter._check",
        lambda self, baseline=None: None,
    )
    db = app_client.app.state.db
    _seed_card(db)

    staged_id = _stage_simplefin(
        db, txn_id="sf-extend-2", posting_date="2024-04-15",
    )
    r = app_client.post(
        "/review/staged/classify",
        data={
            "staged_id": staged_id,
            "target_account": "Expenses:Acme:NewBucket",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303, r.text
    text = settings.connector_accounts_path.read_text(encoding="utf-8")
    assert "open Expenses:Acme:NewBucket" in text


def test_classify_auto_scaffolds_deeper_branch_under_known_entity(
    app_client, settings, monkeypatch,
):
    """User typed `Expenses:Acme:COGS:Materials` for a brand-new
    sub-branch (`Expenses:Acme:COGS:*` doesn't exist yet) under an
    entity that's already attested elsewhere in the ledger. Should
    auto-scaffold rather than 400 — entity-attestation is the
    legitimacy signal, not parent-prefix existence."""
    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check",
        lambda main_bean: None,
    )
    monkeypatch.setattr(
        "lamella.core.registry.accounts_writer.AccountsWriter._check",
        lambda self, baseline=None: None,
    )
    db = app_client.app.state.db
    _seed_card(db)

    staged_id = _stage_simplefin(
        db, txn_id="sf-deep-1", posting_date="2024-04-15",
    )
    # Acme already has `Liabilities:Acme:Card:CardA1234` and
    # `Expenses:Acme:Supplies` opened from the fixture. The user is
    # now adding their FIRST `Expenses:Acme:COGS:*` account — a
    # legitimate new sub-branch under a known entity.
    r = app_client.post(
        "/review/staged/classify",
        data={
            "staged_id": staged_id,
            "target_account": "Expenses:Acme:COGS:Materials",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303, r.text
    text = settings.connector_accounts_path.read_text(encoding="utf-8")
    assert "open Expenses:Acme:COGS:Materials" in text


def test_classify_rejects_orphan_top_level_entity(
    app_client, settings, monkeypatch,
):
    """User typed `Expenses:UnknownEntity:Whatever` — no opened
    account shares the `Expenses:UnknownEntity` branch, so refuse
    rather than create a new top-level entity by drive-by."""
    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check",
        lambda main_bean: None,
    )
    db = app_client.app.state.db
    _seed_card(db)

    staged_id = _stage_simplefin(
        db, txn_id="sf-orphan-1", posting_date="2024-03-15",
    )
    r = app_client.post(
        "/review/staged/classify",
        data={
            "staged_id": staged_id,
            "target_account": "Expenses:UnknownEntity:Whatever",
        },
        follow_redirects=False,
    )
    assert r.status_code == 400, r.text
    detail = r.json()["detail"].lower()
    assert "expenses:unknownentity:whatever" in detail


def test_classify_rejects_invalid_account_syntax(
    app_client, settings, monkeypatch,
):
    """A syntactically invalid account name (`Expe$nses:...`) is
    rejected before any scaffolding attempt."""
    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check",
        lambda main_bean: None,
    )
    db = app_client.app.state.db
    _seed_card(db)

    staged_id = _stage_simplefin(
        db, txn_id="sf-bad-syntax", posting_date="2024-03-15",
    )
    r = app_client.post(
        "/review/staged/classify",
        data={
            "staged_id": staged_id,
            "target_account": "Expe$nses:Acme:Other:Stamp2",
        },
        follow_redirects=False,
    )
    assert r.status_code == 400, r.text
    detail = r.json()["detail"].lower()
    # Three valid error sources for a syntactically broken path:
    #   1. ADR-0045 validate_beancount_account (segment regex check)
    #      → "invalid account path"
    #   2. _ensure_target_account_open (no open found) → "account name"
    #   3. Older "valid beancount" wording from prior implementations
    assert (
        "valid beancount" in detail
        or "account name" in detail
        or "invalid account" in detail
    )


def test_classify_rejects_too_shallow_account(
    app_client, settings, monkeypatch,
):
    """A two-segment account (Root:Leaf) cannot be auto-scaffolded —
    that's a top-level branch, not an extension. Without this guard
    a typo like `Expenses:Tape` would silently create
    a brand-new entity."""
    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check",
        lambda main_bean: None,
    )
    db = app_client.app.state.db
    _seed_card(db)

    staged_id = _stage_simplefin(
        db, txn_id="sf-shallow", posting_date="2024-03-15",
    )
    r = app_client.post(
        "/review/staged/classify",
        data={
            "staged_id": staged_id,
            "target_account": "Expenses:NoSuchLeaf",
        },
        follow_redirects=False,
    )
    assert r.status_code == 400, r.text


def test_classify_auto_backdates_open_in_connector_accounts(
    app_client, settings, monkeypatch,
):
    """Companion accounts (e.g. ``Expenses:<entity>:Bank:<inst>:Fees``)
    are scaffolded by the wizard with today's date because
    ``ensure_companions`` historically didn't pass ``opened_on``. When
    the user then classifies a backdated transaction into one, the
    classify endpoint should rewrite the obstructing Open in
    ``connector_accounts.bean`` to cover the txn date instead of
    refusing — this is the auto-heal that makes the bug self-resolve
    on first classify rather than requiring manual edits."""
    from datetime import date as _date
    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check",
        lambda main_bean: None,
    )
    monkeypatch.setattr(
        "lamella.core.registry.accounts_writer.AccountsWriter._check",
        lambda self, baseline=None: None,
    )
    db = app_client.app.state.db
    _seed_card(db)

    # Pre-write a today-dated Open into connector_accounts.bean to
    # mirror the wizard-scaffolded companion-account state, and
    # include it from main.bean so the parser sees it (the real
    # wizard writes the include line on first scaffold).
    target = "Expenses:Acme:Supplies:NewLeaf"
    today = _date.today().isoformat()
    settings.connector_accounts_path.write_text(
        f"; managed by lamella\n{today} open {target}\n",
        encoding="utf-8",
    )
    main_text = settings.ledger_main.read_text(encoding="utf-8")
    include_line = (
        f'include "{settings.connector_accounts_path.name}"\n'
    )
    if include_line.strip() not in main_text:
        settings.ledger_main.write_text(
            main_text + "\n" + include_line, encoding="utf-8",
        )

    # Classify a txn dated well before today's Open.
    staged_id = _stage_simplefin(
        db, txn_id="sf-backdate-1", posting_date="2024-03-15",
    )
    r = app_client.post(
        "/review/staged/classify",
        data={
            "staged_id": staged_id,
            "target_account": target,
        },
        follow_redirects=False,
    )
    assert r.status_code == 303, r.text

    text = settings.connector_accounts_path.read_text(encoding="utf-8")
    # The Open for our target was rewritten — date is now <= the
    # configured default (1900-01-01) so any older txn also classifies.
    for line in text.splitlines():
        if line.endswith(f"open {target}") or line.endswith(
            f"open {target} "
        ):
            date_str = line.split()[0]
            assert date_str <= "2024-03-15", (
                f"Open dated {date_str}, expected backdate to <= "
                f"txn date 2024-03-15"
            )
            assert date_str != today, (
                f"Open still dated {today} — auto-backdate didn't fire"
            )
            break
    else:
        raise AssertionError(
            f"Open line for {target} not found in "
            f"connector_accounts.bean:\n{text}"
        )


def test_classify_does_not_backdate_open_in_user_owned_file(
    app_client, settings, monkeypatch,
):
    """Auto-backdate is only allowed for Opens in
    ``connector_accounts.bean``. If the obstructing Open lives in a
    file we don't own (e.g. the user's accounts.bean), the classify
    endpoint must surface the error instead of silently mutating
    user-authored state."""
    from datetime import date as _date
    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check",
        lambda main_bean: None,
    )
    db = app_client.app.state.db
    _seed_card(db)

    # accounts.bean (user-owned, included from the fixture's
    # main.bean) already opens Expenses:Acme:Supplies on 2023-01-01
    # per the fixture. Add a narrower account opened today to
    # force the conflict.
    target = "Expenses:Acme:Supplies:UserOwned"
    accounts_path = settings.ledger_dir / "accounts.bean"
    today = _date.today().isoformat()
    text = accounts_path.read_text(encoding="utf-8")
    accounts_path.write_text(
        text + f"\n{today} open {target}\n", encoding="utf-8",
    )

    staged_id = _stage_simplefin(
        db, txn_id="sf-user-owned", posting_date="2024-03-15",
    )
    r = app_client.post(
        "/review/staged/classify",
        data={
            "staged_id": staged_id,
            "target_account": target,
        },
        follow_redirects=False,
    )
    assert r.status_code == 400, r.text
    detail = r.json()["detail"].lower()
    assert "opened on" in detail and "transaction is dated" in detail
    # User's file untouched.
    assert (
        f"{today} open {target}"
        in accounts_path.read_text(encoding="utf-8")
    )


def test_classify_group_auto_scaffolds_with_earliest_date(
    app_client, settings, monkeypatch,
):
    """When a group classify auto-scaffolds, the new Open is dated
    on or before the earliest txn in the group so every member
    classifies cleanly without a per-row bean-check rollback."""
    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check",
        lambda main_bean: None,
    )
    monkeypatch.setattr(
        "lamella.core.registry.accounts_writer.AccountsWriter._check",
        lambda self, baseline=None: None,
    )
    db = app_client.app.state.db
    _seed_card(db)

    sid_old = _stage_simplefin(
        db, txn_id="sf-grp-old", posting_date="2020-06-01",
    )
    sid_new = _stage_simplefin(
        db, txn_id="sf-grp-new", posting_date="2024-06-01",
    )

    r = app_client.post(
        "/review/staged/classify-group",
        data={
            "staged_ids": [str(sid_old), str(sid_new)],
            "target_account": "Expenses:Acme:Supplies:GroupLeaf",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303, r.text
    text = settings.connector_accounts_path.read_text(encoding="utf-8")
    # The Open must be dated <= the earliest row (2020-06-01).
    assert "open Expenses:Acme:Supplies:GroupLeaf" in text
    # Find the Open line and confirm its date is on or before 2020-06-01.
    for line in text.splitlines():
        if "open Expenses:Acme:Supplies:GroupLeaf" in line:
            date_str = line.split()[0]
            assert date_str <= "2020-06-01", (
                f"Open dated {date_str}, must be <= 2020-06-01"
            )
            break
    else:
        raise AssertionError("Open line not found")

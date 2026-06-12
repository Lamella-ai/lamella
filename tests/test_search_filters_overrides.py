# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Regression — /search must NOT render override blocks as
standalone hits. They're a correction layer on top of the
underlying txn (linked by lamella-override-of), not separate events.
A bulk-apply that wrote N overrides was making every corrected
txn appear twice in search ('every single one got duplicated').
"""
from __future__ import annotations


def _seed_card(db):
    db.execute(
        "INSERT INTO accounts_meta (account_path, display_name) "
        "VALUES (?, ?)",
        ("Liabilities:Acme:Card:CardA1234", "Acme Card"),
    )
    db.commit()


def test_search_skips_override_tagged_transactions(app_client, settings):
    """Append an override block to the fixture ledger that
    references an existing txn's hash, then search for the payee.
    The original should appear once; the override block should
    NOT appear as a second hit."""
    # The fixture has a "Hardware Store" entry on 2026-04-10
    # for 42.17 USD posting to Expenses:Acme:Supplies. Use that
    # as our existing txn — search for "Hardware" should find it.
    overrides_path = settings.connector_overrides_path
    overrides_path.parent.mkdir(parents=True, exist_ok=True)
    overrides_path.write_text(
        '2026-04-10 * "Hardware Store" "duplicate-via-override" #lamella-override\n'
        '  lamella-override-of: "fakehash000000000000000000000000000000000"\n'
        '  lamella-modified-at: "2026-04-24T12:00:00-06:00"\n'
        '  Expenses:Acme:Supplies        -42.17 USD\n'
        '  Expenses:Acme:Shipping         42.17 USD\n',
        encoding="utf-8",
    )
    # Make sure the override file is actually included from main.bean.
    # The fixture's main.bean does NOT include connector_overrides.bean
    # by default — but for the search to pick it up, the bean parser
    # must load it. We'll inject an include directive in main.bean.
    main_bean = settings.ledger_main
    main_text = main_bean.read_text(encoding="utf-8")
    if 'include "connector_overrides.bean"' not in main_text:
        main_bean.write_text(
            main_text + '\ninclude "connector_overrides.bean"\n',
            encoding="utf-8",
        )

    app_client.app.state.ledger_reader.invalidate()

    r = app_client.get("/search?q=Hardware&lookback_days=365")
    assert r.status_code == 200, r.text

    # The override has narration "duplicate-via-override" — that
    # should NOT appear in the search results, because the search
    # filters #lamella-override-tagged entries.
    assert "duplicate-via-override" not in r.text, (
        "search returned the override block as a standalone hit — "
        "the #lamella-override filter is missing or broken"
    )
    # The original Hardware Store txn should still appear.
    assert "Hardware Store" in r.text or "HARDWARE" in r.text.upper()

# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Smoke-tests the /card partial path the same way the shim hits it.
Verifies skip/focus actually return the next-row partial under HX-Request."""
from __future__ import annotations


def _stage_three_rows(client):
    client.post(
        "/intake/stage",
        data={
            "text": (
                "Date,Amount,Description\n"
                "2026-04-20,-12.34,AMAZON.COM\n"
                "2026-04-21,-22.50,amazon.com\n"
                "2026-04-23,-5.00,WIDGETCO\n"
            ),
            "has_header": "1",
        },
    )


def test_card_full_page_renders(app_client):
    _stage_three_rows(app_client)
    r = app_client.get("/card")
    assert r.status_code == 200
    assert 'id="card-pane"' in r.text
    assert "<aside" in r.text  # full page → shell present


def test_card_hx_returns_partial_only(app_client):
    _stage_three_rows(app_client)
    r = app_client.get("/card", headers={"HX-Request": "true"})
    assert r.status_code == 200
    assert 'id="card-pane"' in r.text
    assert "<aside" not in r.text
    assert "<!doctype" not in r.text.lower()


def test_card_skip_returns_partial(app_client):
    _stage_three_rows(app_client)
    # First load to discover the prototype id.
    r0 = app_client.get("/card")
    import re
    m = re.search(r'id="card-staged-(\d+)"', r0.text)
    assert m is not None, "first /card render must include a card-staged-* id"
    first_id = int(m.group(1))

    r = app_client.get(
        f"/card?skip={first_id}", headers={"HX-Request": "true"},
    )
    assert r.status_code == 200
    assert 'id="card-pane"' in r.text
    # The skipped id must NOT be the one rendered next.
    assert f'id="card-staged-{first_id}"' not in r.text


def test_card_focus_jumps_to_specific_group(app_client):
    _stage_three_rows(app_client)
    # Default top pick is AMAZON's prototype (group of size 2). Focus
    # the WIDGETCO singleton instead — it's its own prototype, so the
    # focused id is what gets rendered.
    import re
    r_list = app_client.get("/review")
    # The WIDGETCO row carries that label; pluck its staged_id.
    m = re.search(
        r'id="rsg-row-(\d+)"[\s\S]*?WIDGETCO',
        r_list.text,
    )
    assert m is not None, "expected a WIDGETCO row in /review"
    widgetco_id = int(m.group(1))

    r = app_client.get(
        f"/card?focus={widgetco_id}", headers={"HX-Request": "true"},
    )
    assert r.status_code == 200
    assert f'id="card-staged-{widgetco_id}"' in r.text
    assert "WIDGETCO" in r.text

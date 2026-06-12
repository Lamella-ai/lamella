# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the lamella.web.routes._htmx convention helpers."""
from __future__ import annotations

from urllib.parse import unquote

from lamella.web.routes import _htmx


class _StubRequest:
    """Bare-minimum Request stand-in — _htmx only ever reads .headers."""
    def __init__(self, headers: dict[str, str] | None = None):
        self.headers = headers or {}


def test_is_htmx_reads_header_case_insensitive():
    assert _htmx.is_htmx(_StubRequest({"hx-request": "true"})) is True
    assert _htmx.is_htmx(_StubRequest({"hx-request": "TRUE"})) is True
    assert _htmx.is_htmx(_StubRequest({"hx-request": "false"})) is False
    assert _htmx.is_htmx(_StubRequest({})) is False
    assert _htmx.is_htmx(_StubRequest({"hx-request": ""})) is False


def test_redirect_vanilla_returns_303():
    r = _htmx.redirect(_StubRequest({}), "/review")
    assert r.status_code == 303
    assert r.headers["location"] == "/review"


def test_redirect_htmx_returns_204_with_hx_redirect_header():
    r = _htmx.redirect(_StubRequest({"hx-request": "true"}), "/review")
    assert r.status_code == 204
    assert r.headers["HX-Redirect"] == "/review"
    # Body must be empty (HTMX reads the header, not the body).
    assert r.body == b""


def test_redirect_appends_error_query_string():
    r = _htmx.redirect(
        _StubRequest({}), "/setup/accounts", error="bean-check-failed",
    )
    assert r.status_code == 303
    assert r.headers["location"] == "/setup/accounts?error=bean-check-failed"


def test_redirect_appends_error_to_existing_query_string():
    r = _htmx.redirect(
        _StubRequest({}), "/review?source=simplefin", error="oh-no",
    )
    assert "/review?source=simplefin&error=oh-no" == r.headers["location"]


def test_redirect_url_encodes_error_and_message():
    r = _htmx.redirect(
        _StubRequest({}), "/x",
        error="needs escape",
        message="and a space",
    )
    target = r.headers["location"]
    assert "?" in target
    # Whitespace must be encoded (would break the query string otherwise).
    assert "error=needs%20escape" in target or "error=needs+escape" in target
    assert "message=and%20a%20space" in target or "message=and+a+space" in target


def test_empty_response_is_200_no_body():
    r = _htmx.empty()
    assert r.status_code == 200
    assert r.body == b""


def test_error_fragment_returns_400_with_html():
    r = _htmx.error_fragment("<tr><td>broken</td></tr>")
    assert r.status_code == 400
    assert b"broken" in r.body


def test_error_fragment_status_override():
    r = _htmx.error_fragment("<p>conflict</p>", status_code=409)
    assert r.status_code == 409

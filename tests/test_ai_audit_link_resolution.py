# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Audit-page link resolution. Live bug: clicking the input_ref
on /ai/audit produced /txn/TRN-<simplefin-uuid> which 404s
because the route expects a Beancount txn_hash. The fix
distinguishes a real hex-digest hash from any other identifier
shape and resolves SimpleFIN ids via ledger metadata when
possible."""
from __future__ import annotations

from lamella.web.routes.ai import (
    _looks_like_txn_hash,
    _source_href,
)


def test_real_sha1_hash_routes_to_txn():
    h = "48d561591cfc2217a4c385045b2632c934996b31"
    assert _looks_like_txn_hash(h)
    assert _source_href("classify_txn", h) == f"/txn/{h}"


def test_real_sha256_hash_routes_to_txn():
    h = "e" * 64
    assert _looks_like_txn_hash(h)
    assert _source_href("classify_txn", h) == f"/txn/{h}"


def test_simplefin_id_does_not_route_to_txn():
    """The original bug: TRN-<uuid> got routed to /txn/ where it
    404s. Without a resolution map, the link should go somewhere
    safe — /inbox is the canonical staged-review URL."""
    sf_id = "TRN-8f22601c-0a1e-4950-85d7-b729408f4647"
    assert not _looks_like_txn_hash(sf_id)
    href = _source_href("classify_txn", sf_id)
    assert href is not None
    assert "/txn/" not in href
    assert "/inbox" in href


def test_simplefin_id_resolves_to_hash_when_map_provides_it():
    """When the audit page builds a {simplefin_id: txn_hash} map
    from ledger metadata, _source_href uses it to give the user a
    real link."""
    sf_id = "TRN-8f22601c-0a1e-4950-85d7-b729408f4647"
    real_hash = "abcdef0123456789" * 2 + "deadbeef" * 1  # 40 hex
    real_hash = real_hash[:40]
    href = _source_href(
        "classify_txn", sf_id, sf_id_to_hash={sf_id: real_hash},
    )
    assert href == f"/txn/{real_hash}"


def test_short_or_non_hex_strings_arent_treated_as_hashes():
    for s in ["", "abc", "not-a-hash", "TRN-short", "x" * 40]:
        assert not _looks_like_txn_hash(s), (
            f"{s!r} should not be considered a txn_hash"
        )


def test_paperless_input_ref_still_routes_to_paperless():
    """Existing paperless: prefix routing must still work."""
    href = _source_href("receipt_verify", "paperless:1234")
    assert href == "/paperless/preview/1234"


def test_lineage_uuid_resolves_via_alias_map():
    """Post-Phase-3 AI decisions log input_ref=lamella-txn-id (a
    UUID). The alias map _build_simplefin_id_to_hash_map produces
    must include lineage UUIDs so /ai/audit can route them to /txn.
    Without this the audit row dead-ends at /review."""
    lineage = "0190fe22-7c10-7000-8000-aaaaaaaaaaaa"
    real_hash = "a" * 40
    # UUIDs are 36 chars w/ dashes — never look like a 40/64 hex hash.
    assert not _looks_like_txn_hash(lineage)
    href = _source_href(
        "classify_txn", lineage, sf_id_to_hash={lineage: real_hash},
    )
    assert href == f"/txn/{real_hash}"


def test_build_simplefin_id_to_hash_map_includes_lineage():
    """The alias map's contract: every key shape an AI decision can
    log under (lineage UUID, SimpleFIN id) → entry's txn_hash. Builds
    once, walks the ledger once, used by audit row resolution."""
    from datetime import date as _date
    from decimal import Decimal as _D
    from types import SimpleNamespace
    from beancount.core import data as bdata
    from beancount.core.amount import Amount
    from beancount.core.number import D
    from lamella.core.beancount_io.txn_hash import txn_hash
    from lamella.core.identity import REF_KEY, SOURCE_KEY, TXN_ID_KEY
    from lamella.web.routes.ai import _build_simplefin_id_to_hash_map

    lineage = "0190fe22-7c10-7000-8000-aaaaaaaaaaaa"
    sf_id = "TRN-test-1"
    posting_card = bdata.Posting(
        account="Liabilities:Acme:Card",
        units=Amount(D("-10.00"), "USD"),
        cost=None, price=None, flag=None,
        meta={f"{SOURCE_KEY}-0": "simplefin", f"{REF_KEY}-0": sf_id},
    )
    posting_exp = bdata.Posting(
        account="Expenses:Acme:Misc",
        units=Amount(D("10.00"), "USD"),
        cost=None, price=None, flag=None, meta={},
    )
    txn = bdata.Transaction(
        meta={"filename": "<test>", "lineno": 1, TXN_ID_KEY: lineage},
        date=_date(2026, 4, 15), flag="*", payee="Test", narration="x",
        tags=frozenset(), links=frozenset(),
        postings=[posting_card, posting_exp],
    )

    fake_state = SimpleNamespace(
        ledger_reader=SimpleNamespace(
            load=lambda: SimpleNamespace(entries=[txn]),
        ),
    )
    out = _build_simplefin_id_to_hash_map(fake_state)
    expected_hash = txn_hash(txn)
    # Map contains BOTH the SimpleFIN id and the lineage UUID, both
    # pointing at the same txn_hash.
    assert out.get(sf_id) == expected_hash
    assert out.get(lineage) == expected_hash

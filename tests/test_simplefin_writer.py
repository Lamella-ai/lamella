# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from lamella.features.bank_sync.writer import (
    PendingEntry,
    SimpleFINWriter,
    render_entry,
)


def _entry(
    *,
    sid="sf-x",
    date_=date(2026, 4, 15),
    amount=Decimal("-10.00"),
    source="Liabilities:Acme:Card:CardA1234",
    target="Expenses:Acme:Supplies",
    payee="ACME",
    narration="thing",
    ai_classified=False,
    ai_decision_id=None,
    rule_id=None,
    synthetic_kind=None,
    synthetic_confidence=None,
    synthetic_replaceable=True,
) -> PendingEntry:
    return PendingEntry(
        date=date_,
        simplefin_id=sid,
        payee=payee,
        narration=narration,
        amount=amount,
        currency="USD",
        source_account=source,
        target_account=target,
        ai_classified=ai_classified,
        ai_decision_id=ai_decision_id,
        rule_id=rule_id,
        synthetic_kind=synthetic_kind,
        synthetic_confidence=synthetic_confidence,
        synthetic_replaceable=synthetic_replaceable,
    )


def test_render_entry_shape():
    rendered = render_entry(_entry())
    assert '2026-04-15 *' in rendered
    # Phase 7b: SimpleFIN provenance lives at posting-meta level via
    # paired indexed source keys (see test_render_entry_emits_paired_…
    # below). The legacy txn-level `lamella-simplefin-id` is no longer
    # emitted; the read path resolves it via find_source_reference.
    assert 'lamella-simplefin-id' not in rendered
    assert 'Liabilities:Acme:Card:CardA1234  -10.00 USD' in rendered
    assert 'Expenses:Acme:Supplies  10.00 USD' in rendered
    assert 'lamella-ai-classified' not in rendered


def test_render_entry_emits_lineage_id():
    """Phase 2 of NORMALIZE_TXN_IDENTITY.md: every emitted txn carries
    a fresh ``lamella-txn-id`` (UUIDv7) at the txn-meta level."""
    rendered = render_entry(_entry())
    assert "lamella-txn-id:" in rendered
    # Two emits produce two different lineage ids.
    a = render_entry(_entry())
    b = render_entry(_entry())
    import re
    ids = re.findall(r'lamella-txn-id: "([^"]+)"', a + b)
    assert len(ids) == 2
    assert ids[0] != ids[1]


def test_render_entry_emits_paired_indexed_source_on_bank_posting():
    """Phase 2: SimpleFIN provenance is encoded as paired indexed
    source meta (``lamella-source-0`` + ``lamella-source-reference-id-0``)
    on the source-side (first) posting, the canonical post-norm
    location for provenance."""
    rendered = render_entry(_entry(sid="TRN-abc"))
    assert 'lamella-source-0: "simplefin"' in rendered
    assert 'lamella-source-reference-id-0: "TRN-abc"' in rendered


def test_render_entry_emits_source_description_when_present():
    """ADR-0059: when a source carries a description text, it
    persists verbatim as ``lamella-source-description-0`` alongside
    the source / source-reference-id pair on the bank-side posting.
    This is the on-disk store of "what did this source actually
    say about this leg" — preserved even when the canonical
    txn-level narration gets re-synthesized from multiple sources."""
    entry = PendingEntry(
        date=date(2026, 4, 15),
        simplefin_id="TRN-abc",
        payee="ACME",
        narration="thing",
        amount=Decimal("-10.00"),
        currency="USD",
        source_account="Liabilities:Acme:Card:CardA1234",
        target_account="Expenses:Acme:Supplies",
        source_description="POS DEBIT — ACME #1234 ANYTOWN",
    )
    rendered = render_entry(entry)
    assert 'lamella-source-0: "simplefin"' in rendered
    assert 'lamella-source-reference-id-0: "TRN-abc"' in rendered
    assert (
        'lamella-source-description-0: '
        '"POS DEBIT — ACME #1234 ANYTOWN"'
    ) in rendered


def test_render_entry_skips_source_description_when_absent():
    """ADR-0059: source_description is optional. Sources that don't
    carry useful description text (or older PendingEntry callers
    that haven't been threaded yet) MUST NOT produce an empty
    ``lamella-source-description-0: ""`` line — the line is just
    omitted. Avoids polluting the ledger with empty values."""
    rendered = render_entry(_entry(sid="TRN-abc"))
    assert 'lamella-source-description-0' not in rendered


def test_render_entry_escapes_quotes_and_backslashes_in_source_description():
    """ADR-0059: source descriptions can contain awkward characters
    (a merchant name with a quote, a backslash in a wire reference).
    The writer's existing _q escape function must apply here too,
    same as for narration / payee — otherwise a stray quote
    invalidates the .bean file."""
    entry = PendingEntry(
        date=date(2026, 4, 15),
        simplefin_id="TRN-abc",
        payee="ACME",
        narration="thing",
        amount=Decimal("-10.00"),
        currency="USD",
        source_account="Liabilities:Acme:Card:CardA1234",
        target_account="Expenses:Acme:Supplies",
        source_description='Joe\'s "Coffee" \\Co.',
    )
    rendered = render_entry(entry)
    # The single quote and backslash are fine raw in a Beancount
    # double-quoted string; the double quotes inside the value need
    # to be escaped. Confirm the escape ran.
    assert (
        'lamella-source-description-0: '
        '"Joe\'s \\"Coffee\\" \\\\Co."'
    ) in rendered


def test_render_entry_ai_classified_has_metadata():
    rendered = render_entry(_entry(ai_classified=True, ai_decision_id=42))
    assert 'lamella-ai-classified: TRUE' in rendered
    assert 'lamella-ai-decision-id: "42"' in rendered


def test_append_entries_writes_and_passes_bean_check(ledger_dir: Path, monkeypatch):
    # Stub bean-check so tests don't depend on the CLI being installed.
    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check",
        lambda main_bean: None,
    )
    target = ledger_dir / "simplefin_transactions.bean"
    pre_size = target.stat().st_size  # fixture already has content

    writer = SimpleFINWriter(
        main_bean=ledger_dir / "main.bean",
        simplefin_path=target,
    )
    n = writer.append_entries([_entry()])
    assert n == 1
    assert target.stat().st_size > pre_size
    contents = target.read_text(encoding="utf-8")
    # Phase 7b: provenance is on the source-side posting, not txn meta.
    assert 'lamella-source-0: "simplefin"' in contents
    assert 'lamella-source-reference-id-0: "sf-x"' in contents


def test_append_entries_reverts_on_bean_check_failure(ledger_dir: Path, monkeypatch):
    from lamella.core.ledger_writer import BeanCheckError

    def _fail(_main):
        raise BeanCheckError("deliberate")

    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check", _fail
    )

    target = ledger_dir / "simplefin_transactions.bean"
    pre_size = target.stat().st_size
    pre_bytes = target.read_bytes()

    writer = SimpleFINWriter(
        main_bean=ledger_dir / "main.bean",
        simplefin_path=target,
    )
    with pytest.raises(BeanCheckError):
        writer.append_entries([_entry()])

    # Size and content must match pre-write exactly.
    assert target.stat().st_size == pre_size
    assert target.read_bytes() == pre_bytes


def test_append_to_preview_path_skips_main_bean_include(tmp_path: Path, ledger_dir: Path, monkeypatch):
    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check", lambda main_bean: None
    )
    preview = ledger_dir / "simplefin_transactions.connector_preview.bean"
    assert not preview.exists()

    main_before = (ledger_dir / "main.bean").read_text(encoding="utf-8")

    writer = SimpleFINWriter(
        main_bean=ledger_dir / "main.bean",
        simplefin_path=ledger_dir / "simplefin_transactions.bean",
    )
    n = writer.append_entries([_entry()], target_path=preview)
    assert n == 1
    assert preview.exists()

    # main.bean must NOT have been touched when writing to the preview file.
    assert (ledger_dir / "main.bean").read_text(encoding="utf-8") == main_before


# ─── ADR-0046 Phase 1: synthetic-counterpart meta on user-classified
# transfer legs ────────────────────────────────────────────────────


def test_render_entry_emits_synthetic_meta_on_assets_target():
    """ADR-0046 Phase 1: when the caller flags ``synthetic_kind`` on a
    PendingEntry whose target is ``Assets:…``, the destination posting
    carries the four ``lamella-synthetic-*`` meta keys."""
    rendered = render_entry(_entry(
        amount=Decimal("-840.82"),
        source="Assets:Personal:BankOne:Checking",
        target="Assets:Personal:PayPal",
        payee="PAYPAL TRANSFER",
        narration="PAYPAL TRANSFER 1049791515428",
        synthetic_kind="user-classified-counterpart",
        synthetic_confidence="guessed",
        synthetic_replaceable=True,
    ))

    # Source leg unchanged — paired indexed source meta + amount.
    assert (
        "Assets:Personal:BankOne:Checking  -840.82 USD" in rendered
    )
    assert 'lamella-source-0: "simplefin"' in rendered
    # Destination leg carries the synthetic provenance keys.
    assert "Assets:Personal:PayPal  840.82 USD" in rendered
    assert (
        'lamella-synthetic: "user-classified-counterpart"' in rendered
    )
    assert 'lamella-synthetic-confidence: "guessed"' in rendered
    assert "lamella-synthetic-replaceable: TRUE" in rendered
    # Timestamp is TZ-aware UTC ISO-8601 per ADR-0023.
    import re
    m = re.search(
        r'lamella-synthetic-decided-at: "([^"]+)"', rendered,
    )
    assert m is not None, "missing lamella-synthetic-decided-at"
    stamp = m.group(1)
    # datetime.now(UTC).isoformat(timespec="seconds") emits +00:00.
    assert stamp.endswith("+00:00"), f"unexpected timestamp: {stamp!r}"


def test_render_entry_no_synthetic_meta_for_expenses_target():
    """ADR-0046 Phase 1 negative: an Expenses: target NEVER gets
    synthetic-* meta — those keys are reserved for balance-sheet
    counterparts. The route's heuristic must agree (it never sets
    ``synthetic_kind`` on an Expenses target); the writer is the
    last line of defense."""
    rendered = render_entry(_entry(
        amount=Decimal("-12.50"),
        source="Liabilities:Acme:Card:CardA1234",
        target="Expenses:Acme:Supplies",
        narration="ACME OFFICE SUPPLIES",
    ))
    # synthetic_kind not set → no synthetic meta keys.
    assert "lamella-synthetic" not in rendered


def test_render_entry_no_synthetic_meta_when_kind_is_none():
    """ADR-0046 Phase 1: even on an Assets:/Liab: target, if the
    route's heuristic decides this isn't a transfer (e.g. the user
    is reclassifying a one-off transaction to a Suspense-style
    account on a non-transfer narration), the route passes
    ``synthetic_kind=None`` and the writer must NOT stamp
    synthetic meta."""
    rendered = render_entry(_entry(
        amount=Decimal("-100.00"),
        source="Liabilities:Acme:Card:CardA1234",
        target="Assets:Personal:PayPal",
        narration="ACME OFFICE SUPPLIES",  # no "transfer" word
        synthetic_kind=None,
    ))
    # synthetic_kind not set → no synthetic meta keys, even though
    # the target is Assets:.
    assert "lamella-synthetic" not in rendered


def test_append_entries_writes_synthetic_meta_end_to_end(
    ledger_dir: Path, monkeypatch,
):
    """ADR-0046 Phase 1 end-to-end: a transfer-suspect entry with
    ``synthetic_kind`` set lands in the .bean file with the four
    ``lamella-synthetic-*`` keys on the destination posting and
    bean-check accepts the result."""
    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check",
        lambda main_bean: None,
    )
    target = ledger_dir / "simplefin_transactions.bean"
    writer = SimpleFINWriter(
        main_bean=ledger_dir / "main.bean",
        simplefin_path=target,
    )
    entry = _entry(
        sid="TRN-e25e28f2",
        amount=Decimal("-840.82"),
        source="Assets:Acme:Checking",
        target="Assets:Personal:Checking",
        payee="PAYPAL TRANSFER",
        narration="PAYPAL TRANSFER 1049791515428",
        synthetic_kind="user-classified-counterpart",
        synthetic_confidence="guessed",
        synthetic_replaceable=True,
    )
    n = writer.append_entries([entry])
    assert n == 1
    contents = target.read_text(encoding="utf-8")
    assert 'lamella-synthetic: "user-classified-counterpart"' in contents
    assert 'lamella-synthetic-confidence: "guessed"' in contents
    assert "lamella-synthetic-replaceable: TRUE" in contents
    assert "lamella-synthetic-decided-at:" in contents

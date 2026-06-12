# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware
# financial intelligence
# https://lamella.ai

"""Regression: classifier MUST use the *universal* sign convention
that matches actual ledger writes.

Verified against real ``simplefin_transactions.bean`` entries:
  Liabilities:CC  -9.49 USD   ← credit-card CHARGE (money OUT)
  Liabilities:CC +20.95 USD   ← credit-card REFUND (money IN)
  Assets:Checking -50.00 USD  ← withdrawal/expense
  Assets:Checking +100.00 USD ← deposit/income

So *positive* on the bank-side leg = money IN = Income whitelist;
*negative* = money OUT = Expenses whitelist. Same rule for asset and
liability sources. Earlier code carried a "liability-aware inversion"
that assumed SimpleFIN delivered card charges as POSITIVE — actual
ledger entries showed the opposite, and the inversion silently routed
every CC charge to the Income whitelist + every refund to the Expenses
whitelist. These tests pin the universal convention at three layers:

  * ``build_classify_context`` flips Expenses-FIXME → Income-FIXME on
    *any* negative FIXME amount (asset OR liability).
  * The Jinja prompt renders the Liability sign-convention block so
    the AI sees explicit guidance for liability source rows.
  * ``_maybe_ai_classify`` picks Income whitelist for positive amounts
    + Expenses whitelist for negative amounts regardless of source.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

from lamella.features.ai_cascade.classify import TxnForClassify
from lamella.features.ai_cascade.context import render


def _render(**overrides) -> str:
    txn = overrides.pop(
        "txn",
        TxnForClassify(
            date=date(2026, 4, 18),
            amount=Decimal("182.00"),
            currency="USD",
            payee="County Sheriff",
            narration="charge",
            card_account="Liabilities:Acme:Card:CardA1234",
            fixme_account="Expenses:Acme:FIXME",
            txn_hash="hash-test",
        ),
    )
    base = dict(
        txn=txn,
        similar=[],
        entity="Acme",
        accounts=["Expenses:Acme:Supplies"],
        accounts_by_entity={},
        registry_preamble="",
        active_notes=[],
        card_suspicion=None,
        receipt=None,
        mileage_entries=[],
        vehicle_density=[],
        account_descriptions={},
        entity_context=None,
        active_projects=[],
        fixme_root="Expenses",
    )
    base.update(overrides)
    return render("classify_txn.j2", **base)


# ---------- build_classify_context flips on any negative FIXME ----------


def test_liability_refund_flips_fixme_root_to_income():
    """Positive bank-side on a liability = refund/paydown (money IN
    to user). FIXME leg is negative (opposite sign of bank). Universal
    convention: negative FIXME → flip Expenses → Income whitelist."""
    from beancount.core.data import (
        Posting, Transaction as BCTxn, Open,
    )
    from beancount.core.amount import Amount
    from lamella.features.ai_cascade.classify import build_classify_context

    income_open = Open(
        meta={"filename": "x.bean", "lineno": 1},
        date=date(2025, 1, 1),
        account="Income:Acme:Refunds",
        currencies=None, booking=None,
    )
    bank_leg = Posting(
        account="Liabilities:Acme:Card:CardA1234",
        units=Amount(Decimal("100"), "USD"),
        cost=None, price=None, flag=None, meta=None,
    )
    fixme_leg = Posting(
        account="Expenses:Acme:FIXME",
        units=Amount(Decimal("-100"), "USD"),
        cost=None, price=None, flag=None, meta=None,
    )
    txn = BCTxn(
        meta={"filename": "x.bean", "lineno": 5},
        date=date(2026, 4, 18),
        flag="*",
        payee="Test",
        narration="refund",
        tags=frozenset(),
        links=frozenset(),
        postings=[bank_leg, fixme_leg],
    )
    out = build_classify_context(
        entries=[income_open, txn], txn=txn, conn=None,
    )
    view = out[0]
    valid_accounts = out[2]
    assert view is not None
    assert any(
        a.startswith("Income:") for a in valid_accounts
    ), (
        f"build_classify_context failed to flip to Income for a "
        f"liability-source refund (positive bank-side). Got "
        f"{valid_accounts!r}"
    )


def test_asset_deposit_still_flips_to_income():
    """Negative FIXME on an asset = positive bank-side = deposit.
    Whitelist must be Income."""
    from beancount.core.data import (
        Posting, Transaction as BCTxn, Open,
    )
    from beancount.core.amount import Amount
    from lamella.features.ai_cascade.classify import build_classify_context

    income_open = Open(
        meta={"filename": "x.bean", "lineno": 1},
        date=date(2025, 1, 1),
        account="Income:Personal:Refunds",
        currencies=None, booking=None,
    )
    bank_leg = Posting(
        account="Assets:Personal:Checking",
        units=Amount(Decimal("100"), "USD"),
        cost=None, price=None, flag=None, meta=None,
    )
    fixme_leg = Posting(
        account="Expenses:Personal:FIXME",
        units=Amount(Decimal("-100"), "USD"),
        cost=None, price=None, flag=None, meta=None,
    )
    txn = BCTxn(
        meta={"filename": "x.bean", "lineno": 5},
        date=date(2026, 4, 18),
        flag="*",
        payee="Test",
        narration="deposit",
        tags=frozenset(),
        links=frozenset(),
        postings=[bank_leg, fixme_leg],
    )
    out = build_classify_context(
        entries=[income_open, txn], txn=txn, conn=None,
    )
    view = out[0]
    valid_accounts = out[2]
    assert view is not None
    assert any(
        a.startswith("Income:") for a in valid_accounts
    ), (
        f"build_classify_context failed to flip to Income for an "
        f"asset deposit — got {valid_accounts!r}"
    )


# ---------- Jinja prompt still renders the liability convention block ----------


def test_liability_card_renders_sign_convention_block():
    out = _render()
    flat = " ".join(out.split()).lower()
    assert "sign convention" in flat, (
        "Liability card account must surface the sign-convention "
        "block in the prompt — without it the AI re-derives the "
        "convention and may misroute liability-source rows."
    )
    assert "negative bank-side amount" in flat
    assert "the card was charged" in flat


def test_asset_card_does_not_render_sign_convention_block():
    """The block must NOT fire for asset cards — the AI's default
    asset-side reasoning is correct without it, and adding it would
    bloat the prompt."""
    txn = TxnForClassify(
        date=date(2026, 4, 18),
        amount=Decimal("100.00"),
        currency="USD",
        payee="Test",
        narration="something",
        card_account="Assets:Personal:Checking",
        fixme_account="Expenses:FIXME",
        txn_hash="hash-test",
    )
    out = _render(txn=txn)
    flat = " ".join(out.split()).lower()
    assert "sign convention" not in flat


def test_no_card_account_does_not_render_sign_convention_block():
    """No card hint means no liability-specific block either (we don't
    know the convention)."""
    txn = TxnForClassify(
        date=date(2026, 4, 18),
        amount=Decimal("100.00"),
        currency="USD",
        payee="Test",
        narration="something",
        card_account=None,
        fixme_account="Expenses:FIXME",
        txn_hash="hash-test",
    )
    out = _render(txn=txn)
    flat = " ".join(out.split()).lower()
    assert "sign convention" not in flat


# ---------- bulk_classify renders prompt with fixme_root kwarg ----------


def test_bulk_classify_passes_fixme_root_to_render():
    """``_classify_one`` must derive ``fixme_root`` from the FIXME
    leg's account and pass it to render(...)."""
    import inspect
    from lamella.features.ai_cascade import bulk_classify

    src = inspect.getsource(bulk_classify._classify_one)
    assert "fixme_root=" in src, (
        "bulk_classify._classify_one must pass fixme_root= to "
        "render(); without it the prompt defaults to the EXPENSES "
        "FIXME branch on every call regardless of root, so deposit "
        "rows whose FIXME is Income:Acme:FIXME hit the wrong "
        "preamble."
    )


# ---------- ingest._maybe_ai_classify renders prompt with fixme_root ----------


def test_ingest_maybe_ai_classify_passes_fixme_root_to_render():
    """``_maybe_ai_classify`` must derive ``fixme_root`` from the
    computed FIXME account and pass it to render(...)."""
    import inspect
    from lamella.features.bank_sync import ingest

    src = inspect.getsource(ingest.SimpleFINIngest._maybe_ai_classify)
    assert "fixme_root=" in src, (
        "ingest.SimpleFINIngest._maybe_ai_classify must pass fixme_root= to "
        "render(); without it the prompt always uses the EXPENSES FIXME "
        "preamble regardless of the transaction root, so deposit rows "
        "and liability payments get the wrong AI framing."
    )
    assert "_fixme_root_for_prompt" in src, (
        "ingest._maybe_ai_classify must compute _fixme_root_for_prompt "
        "from the FIXME account before passing it to render()."
    )


# ---------- _maybe_ai_classify whitelist-root selection (universal) ----------


class _RootCaptured(Exception):
    """Raised by the test stub for valid_accounts_by_root after
    capturing the root the production code chose. Lets us short-circuit
    the rest of _maybe_ai_classify without standing up an AI client."""

    def __init__(self, root: str):
        super().__init__(root)
        self.root = root


def _run_maybe_ai_classify_capture_root(
    monkeypatch, *, source_account, amount,
) -> str | None:
    import asyncio
    from datetime import datetime, timezone

    from lamella.adapters.simplefin.schemas import SimpleFINTransaction
    from lamella.features.bank_sync import ingest as ingest_mod

    def _fake_valid(entries, *, root, entity, **_kw):
        raise _RootCaptured(root)

    monkeypatch.setattr(
        "lamella.features.ai_cascade.context.valid_accounts_by_root",
        _fake_valid,
    )

    class _StubAI:
        enabled = True

        def new_client(self):
            class _C:
                async def aclose(self_inner):
                    return None
            return _C()

        def model_for(self, _):
            return "stub-model"

    class _StubReader:
        def load(self):
            class _R:
                entries: list = []
            return _R()

    class _StubConn:
        def execute(self, *a, **kw):
            class _Cur:
                def fetchall(self_inner):
                    return []
                def fetchone(self_inner):
                    return None
            return _Cur()

    monkeypatch.setattr(
        "lamella.features.bank_sync.ingest._entity_from_source",
        lambda src, conn=None: "Acme" if src else None,
    )

    sf_txn = SimpleFINTransaction(
        id="sf-test-root",
        posted=int(
            datetime(2026, 4, 18, tzinfo=timezone.utc).timestamp()
        ),
        amount=amount,
        description="test",
    )

    obj = ingest_mod.SimpleFINIngest.__new__(ingest_mod.SimpleFINIngest)
    obj.ai = _StubAI()
    obj.reader = _StubReader()
    obj.conn = _StubConn()

    loop = asyncio.new_event_loop()
    captured_root: list[str] = []
    try:
        try:
            loop.run_until_complete(
                obj._maybe_ai_classify(
                    txn=sf_txn, source_account=source_account,
                )
            )
        except _RootCaptured as exc:
            captured_root.append(exc.root)
    finally:
        loop.close()
    return captured_root[0] if captured_root else None


def test_ingest_maybe_ai_classify_liability_charge_picks_expenses_whitelist(
    monkeypatch,
):
    """Negative amount on a Liabilities:* source = real CC charge
    (verified against actual ledger writes). Must select Expenses
    whitelist."""
    from decimal import Decimal as _D

    root = _run_maybe_ai_classify_capture_root(
        monkeypatch,
        source_account="Liabilities:Acme:Card:CardA1234",
        amount=_D("-182.00"),  # NEGATIVE = credit-card charge per ledger
    )
    assert root == "Expenses", (
        "Negative-amount charge on a Liabilities:* source must select "
        f"the Expenses whitelist root. Got: {root!r}"
    )


def test_ingest_maybe_ai_classify_liability_refund_picks_income_whitelist(
    monkeypatch,
):
    """Positive amount on a Liabilities:* source = refund / paydown
    (money IN to user). Must select Income whitelist."""
    from decimal import Decimal as _D

    root = _run_maybe_ai_classify_capture_root(
        monkeypatch,
        source_account="Liabilities:Acme:Card:CardA1234",
        amount=_D("20.95"),  # POSITIVE = refund or paydown per ledger
    )
    assert root == "Income", (
        "Positive-amount refund on a Liabilities:* source must select "
        f"the Income whitelist root. Got: {root!r}"
    )


def test_ingest_maybe_ai_classify_asset_deposit_picks_income_whitelist(
    monkeypatch,
):
    from decimal import Decimal as _D

    root = _run_maybe_ai_classify_capture_root(
        monkeypatch,
        source_account="Assets:Personal:Checking",
        amount=_D("15.00"),
    )
    assert root == "Income", (
        "Positive-amount deposit on an Assets:* source must select "
        f"Income whitelist root. Got: {root!r}"
    )


def test_ingest_maybe_ai_classify_asset_withdrawal_picks_expenses_whitelist(
    monkeypatch,
):
    from decimal import Decimal as _D

    root = _run_maybe_ai_classify_capture_root(
        monkeypatch,
        source_account="Assets:Personal:Checking",
        amount=_D("-42.17"),
    )
    assert root == "Expenses", (
        "Negative-amount withdrawal on an Assets:* source must select "
        f"Expenses whitelist. Got: {root!r}"
    )


def test_ingest_maybe_ai_classify_unresolvable_source_uses_universal_convention(
    monkeypatch,
):
    """When source_account is None, the universal positive→Income /
    negative→Expenses rule still applies."""
    from decimal import Decimal as _D

    root = _run_maybe_ai_classify_capture_root(
        monkeypatch,
        source_account=None,
        amount=_D("50.00"),
    )
    assert root == "Income", (
        f"Unresolvable source_account with positive amount must select "
        f"Income whitelist root. Got: {root!r}"
    )


# ---------- api_txn deposit-skip uses universal sign convention ----------


def test_api_txn_deposit_detection_uses_universal_convention():
    """The deposit-skip preflight in api_txn must NOT branch on
    account kind / source path root. Universal rule: positive
    bank-side amount = deposit-shaped (skip AI), negative = let AI
    classify."""
    import inspect
    import lamella.web.routes.api_txn as api_txn_mod

    src = inspect.getsource(api_txn_mod)
    # The fix removed the account-kind branching from deposit
    # detection. The previous code carried two
    # ``startswith("Liabilities:")`` calls inside the deposit-detect
    # block plus references to ``credit_card``/``line_of_credit``
    # account kinds. Those should be gone from the deposit-skip
    # preflight (they may still appear elsewhere in the file).
    deposit_block_start = src.index("_is_deposit = False")
    deposit_block = src[
        deposit_block_start
        : src.index("if _is_deposit:", deposit_block_start)
    ]
    assert "credit_card" not in deposit_block, (
        "Deposit-skip preflight must not branch on accounts_meta.kind"
    )
    assert "line_of_credit" not in deposit_block, (
        "Deposit-skip preflight must not branch on accounts_meta.kind"
    )
    assert "_is_liability_kind" not in deposit_block, (
        "Deposit-skip preflight must not carry the inverted "
        "liability-kind sign branch — universal convention applies"
    )

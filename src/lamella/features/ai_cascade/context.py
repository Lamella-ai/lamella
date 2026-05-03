# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Iterable

from jinja2 import Environment, FileSystemLoader, select_autoescape

from beancount.core.data import Transaction

from lamella.core.beancount_io.txn_hash import txn_hash

PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"

_env = Environment(
    loader=FileSystemLoader(str(PROMPTS_DIR)),
    autoescape=select_autoescape(enabled_extensions=("j2",), default_for_string=False),
    trim_blocks=True,
    lstrip_blocks=True,
    keep_trailing_newline=True,
)


@dataclass(frozen=True)
class TxnForClassify:
    date: date
    amount: Decimal
    currency: str
    payee: str | None
    narration: str | None
    card_account: str | None
    fixme_account: str
    txn_hash: str
    # Phase 3 of NORMALIZE_TXN_IDENTITY.md: when the entry carries a
    # ``lamella-txn-id`` (lineage), AI calls key on it instead of the
    # content hash so AI history survives ledger edits + the
    # ingest→promotion handoff. None for legacy pre-Phase-4 entries —
    # callers fall back to txn_hash.
    lamella_txn_id: str | None = None


@dataclass(frozen=True)
class SimilarTxn:
    date: date
    amount: Decimal
    narration: str
    target_account: str


@dataclass(frozen=True)
class ReceiptFacts:
    vendor: str | None
    total: Decimal
    currency: str
    date: date
    last4: str | None


@dataclass(frozen=True)
class CandidateFacts:
    txn_hash: str
    date: date
    amount: Decimal
    payee: str | None
    narration: str | None
    card_account: str | None
    day_delta: int


def render(template: str, **kwargs) -> str:
    return _env.get_template(template).render(**kwargs)


# ---------- helpers for classify context ----------

CARD_ROOTS = ("Liabilities", "Assets")


def _posting_target(posting) -> str | None:
    acct = posting.account or ""
    return acct if acct.split(":", 1)[0] == "Expenses" else None


def _resolved_target(txn: Transaction) -> str | None:
    """For similarity lookup: return the Expenses account on a resolved txn
    (i.e. any Expenses posting not ending in FIXME)."""
    for posting in txn.postings:
        acct = _posting_target(posting)
        if acct and acct.split(":")[-1].upper() != "FIXME":
            return acct
    return None


def _merchant_text(txn: Transaction) -> str:
    return " ".join(filter(None, [txn.payee or "", txn.narration or ""])).lower()


def similar_transactions(
    entries: Iterable,
    *,
    needle: str,
    reference_date: date,
    window_days: int = 180,
    limit: int = 5,
) -> list[SimilarTxn]:
    if not needle:
        return []
    needle_low = needle.lower()
    lo = reference_date - timedelta(days=window_days)
    out: list[SimilarTxn] = []
    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        if entry.date < lo or entry.date > reference_date:
            continue
        if needle_low not in _merchant_text(entry):
            continue
        target = _resolved_target(entry)
        if target is None:
            continue
        # Use first Expenses-posting amount magnitude.
        amount: Decimal | None = None
        for posting in entry.postings:
            if _posting_target(posting) == target and posting.units and posting.units.number is not None:
                amount = Decimal(posting.units.number)
                break
        out.append(
            SimilarTxn(
                date=entry.date,
                amount=abs(amount) if amount is not None else Decimal("0"),
                narration=entry.narration or "",
                target_account=target,
            )
        )
    out.sort(key=lambda r: r.date, reverse=True)
    return out[:limit]


def entity_from_card(card_account: str | None) -> str | None:
    """`Liabilities:Acme:Card:0123` → `Acme`.

    Heuristic-only string split. Callers with a ``conn`` available
    should prefer ``resolve_entity_for_account`` so the
    ``accounts_meta.entity_slug`` column (the registry's
    authoritative answer) wins when set.
    """
    if not card_account:
        return None
    parts = card_account.split(":")
    if len(parts) >= 2:
        return parts[1]
    return None


def resolve_entity_for_account(
    conn,
    card_account: str | None,
) -> str | None:
    """Phase G2 — canonical card → entity resolver.

    Prefers ``accounts_meta.entity_slug`` (the explicit registry
    binding set during WS3 onboarding or edited in settings).
    Falls back to the second-path-segment heuristic and logs a
    warning when the registry and the heuristic disagree — this is
    exactly the signal that points at a mislabeled card or a
    wrong-card situation worth human review.
    """
    if not card_account:
        return None
    heuristic = entity_from_card(card_account)
    if conn is None:
        return heuristic
    try:
        row = conn.execute(
            "SELECT entity_slug FROM accounts_meta WHERE account_path = ?",
            (card_account,),
        ).fetchone()
    except Exception:  # noqa: BLE001
        return heuristic
    if row is None or row["entity_slug"] is None:
        return heuristic
    registered = str(row["entity_slug"]).strip() or None
    if registered and heuristic and registered != heuristic:
        import logging as _log
        _log.getLogger(__name__).warning(
            "entity mismatch for %s: heuristic=%r, registry=%r — registry wins",
            card_account, heuristic, registered,
        )
    return registered or heuristic


def valid_expense_accounts(
    entries: Iterable,
    *,
    entity: str | None,
) -> list[str]:
    """Return expense account paths open in the ledger.

    When ``entity`` is given, filters to that entity only (Phase B1
    behavior). When ``entity`` is None, returns the full cross-entity
    set — used by Phase G4 classifier calls that want to let the AI
    pick across entities and flag intercompany situations.
    """
    return valid_accounts_by_root(entries, root="Expenses", entity=entity)


def valid_accounts_by_root(
    entries: Iterable,
    *,
    root: str,
    entity: str | None,
) -> list[str]:
    """Generalization of ``valid_expense_accounts`` — AI-AGENT.md Phase 2.

    Return Open'd account paths for any top-level root (Expenses,
    Income, Liabilities, Equity, Assets). Skips FIXME leaves and
    optionally filters to the given entity (path segment 1).

    Used by classify when the FIXME being resolved isn't on the
    Expenses side — Income:FIXME attribution, Liabilities:FIXME
    destination-picking for CC payments, etc.
    """
    prefix = f"{root}:"
    accounts: set[str] = set()
    for entry in entries:
        val = getattr(entry, "account", None)
        if not isinstance(val, str) or not val.startswith(prefix):
            continue
        if val.split(":")[-1].upper() == "FIXME":
            continue
        if entity is None or (
            len(val.split(":")) >= 2 and val.split(":")[1] == entity
        ):
            accounts.add(val)
    return sorted(accounts)


_CARD_ROOTS_FOR_HISTOGRAM = ("Liabilities", "Assets")


def merchant_entity_counts(
    entries: Iterable,
    *,
    merchant: str,
) -> dict[str, int]:
    """Phase G3 — for a given merchant, count how many historical
    txns placed a charge on each entity's card.

    Match is substring-insensitive on the txn's payee OR narration.
    The entity for each matching txn is derived from the account
    path of its Liabilities/Assets posting (second path segment).

    This is the data backbone of ``suspicious_card_binding`` — when
    a merchant has only ever appeared on Entity B's cards and
    suddenly shows up on Entity A, the classifier should force
    human review instead of silently misattributing.
    """
    if not merchant or not merchant.strip():
        return {}
    needle = merchant.lower().strip()
    counts: dict[str, int] = {}
    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        hay = " ".join(filter(None, [entry.payee or "", entry.narration or ""])).lower()
        if needle not in hay:
            continue
        # Any posting on a Liabilities/Assets account counts as "this
        # card was charged." Take the first one with an entity-looking
        # second segment.
        for posting in (entry.postings or []):
            acct = posting.account or ""
            parts = acct.split(":")
            if len(parts) < 2:
                continue
            if parts[0] not in _CARD_ROOTS_FOR_HISTOGRAM:
                continue
            entity = parts[1]
            counts[entity] = counts.get(entity, 0) + 1
            break
    return counts


@dataclass(frozen=True)
class CardBindingSuspicion:
    """Result of ``suspicious_card_binding``. ``reason`` is a short
    human-readable summary safe to include in a prompt or a review-
    queue item. ``dominant_entity`` is the entity the merchant's
    history points at; ``card_entity`` is what the card binding says.
    """
    card_entity: str | None
    dominant_entity: str
    card_entity_count: int
    dominant_count: int
    total: int
    reason: str


def suspicious_card_binding(
    entries: Iterable,
    *,
    merchant: str,
    card_entity: str | None,
    dominance_ratio: float = 0.8,
    min_total_history: int = 5,
) -> CardBindingSuspicion | None:
    """Return a suspicion record when this merchant's historical
    charges skew decisively away from the card's entity.

    Defaults:
      * ``dominance_ratio=0.8`` — another entity must hold ≥ 80%
        of the merchant's history for a flag.
      * ``min_total_history=5`` — at least five prior charges total;
        below that we don't have enough data to call it suspicious.

    Returns ``None`` when the history supports the card binding
    (the dominant entity equals the card entity), the data is too
    thin, or the merchant is empty.
    """
    counts = merchant_entity_counts(entries, merchant=merchant)
    total = sum(counts.values())
    if total < min_total_history:
        return None
    # Dominant entity = the one with the most charges.
    dominant, dominant_count = max(counts.items(), key=lambda kv: kv[1])
    if dominant == card_entity:
        return None
    # If the dominant entity holds the required share and the card
    # entity is NOT dominant, call it suspicious.
    share = dominant_count / total
    if share < dominance_ratio:
        return None
    card_count = counts.get(card_entity or "", 0) if card_entity else 0
    reason = (
        f"merchant '{merchant}' has appeared {dominant_count} time(s) on "
        f"'{dominant}' cards and {card_count} time(s) on "
        f"'{card_entity or 'this'}' cards "
        f"(share {share:.0%} of {total} prior charges) — "
        "likely wrong-card situation"
    )
    return CardBindingSuspicion(
        card_entity=card_entity,
        dominant_entity=dominant,
        card_entity_count=card_count,
        dominant_count=dominant_count,
        total=total,
        reason=reason,
    )


def all_expense_accounts_by_entity(
    entries: Iterable,
) -> dict[str, list[str]]:
    """Phase G4 — cross-entity grouping of expense accounts.

    Returns ``{entity_slug: [account, …]}``. Accounts whose second
    path segment doesn't look like an entity (e.g.,
    ``Expenses:Uncategorized``) land under the empty-string key
    so the classifier can still see them.
    """
    grouped: dict[str, list[str]] = {}
    for entry in entries:
        val = getattr(entry, "account", None)
        if not isinstance(val, str) or not val.startswith("Expenses:"):
            continue
        parts = val.split(":")
        if parts[-1].upper() == "FIXME":
            continue
        entity = parts[1] if len(parts) >= 2 else ""
        grouped.setdefault(entity, []).append(val)
    for k in grouped:
        grouped[k] = sorted(set(grouped[k]))
    return grouped


def extract_fixme_txn(txn: Transaction) -> TxnForClassify | None:
    fixme_posting = None
    for posting in txn.postings:
        acct = posting.account or ""
        if acct.split(":")[-1].upper() == "FIXME":
            fixme_posting = posting
            break
    if fixme_posting is None or fixme_posting.units is None or fixme_posting.units.number is None:
        return None
    card = None
    for posting in txn.postings:
        root = (posting.account or "").split(":", 1)[0]
        if root in CARD_ROOTS:
            card = posting.account
            break
    from lamella.core.identity import get_txn_id
    return TxnForClassify(
        date=txn.date,
        amount=Decimal(fixme_posting.units.number),
        currency=fixme_posting.units.currency or "USD",
        payee=txn.payee,
        narration=txn.narration,
        card_account=card,
        fixme_account=fixme_posting.account,
        txn_hash=txn_hash(txn),
        lamella_txn_id=get_txn_id(txn),
    )


def candidate_from_txn(txn: Transaction, *, receipt_date: date) -> CandidateFacts:
    amount = Decimal("0")
    card = None
    for posting in txn.postings:
        root = (posting.account or "").split(":", 1)[0]
        if root in CARD_ROOTS and card is None:
            card = posting.account
        if posting.units and posting.units.number is not None:
            if posting.account and posting.account.startswith("Expenses:"):
                amount = abs(Decimal(posting.units.number))
    return CandidateFacts(
        txn_hash=txn_hash(txn),
        date=txn.date,
        amount=amount,
        payee=txn.payee,
        narration=txn.narration,
        card_account=card,
        day_delta=abs((txn.date - receipt_date).days),
    )

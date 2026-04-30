# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Pre-flight "needs-to-be-addressed" report for Phase E reboot.

Scans the ledger for payees whose historical transactions land
predominantly in an unresolved-leaf account. "Unresolved" here
means one of the set ``{FIXME, UNKNOWN, UNCATEGORIZED,
UNCLASSIFIED}`` at the end of the account path — all four are
the codebase's accepted ways of saying "we haven't figured this
out yet." Phase E will faithfully reproduce these as still-
unresolved unless the user attends to them first (PayPal
transfers that never got a source account, Fast Food charges
in ``Uncategorized``, etc.).

The report surfaces on the data-integrity page and gates the
reboot-apply step via a hash-based acknowledgment: when the
user clicks "I've reviewed these," we store the hash of the
current unresolved-heavy set in app_settings. If new unresolved
patterns accumulate afterwards, the hash changes and the gate
re-asserts.

Note: classification accounts like ``Expenses:Personal:Uncategorized``
might be intentional catch-alls for you. If so, after acknowledging
the report once, the gate stays unlocked until NEW unresolved
patterns appear — you don't have to re-ack the same set every
reboot. Names that you treat as real categories can be excluded
from this check via the ``extra_ok_leaves`` knob.
"""
from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Iterable

from beancount.core.data import Transaction

__all__ = [
    "FixmePayee",
    "PreflightReport",
    "UNRESOLVED_LEAVES",
    "fixme_heavy_payees",
    "report_hash",
]


_WS = re.compile(r"\s+")


# The set of account-leaf tokens treated as "needs-to-be-addressed."
# All-uppercase comparison: ``Expenses:Personal:FIXME``,
# ``Expenses:Personal:Uncategorized``, ``Expenses:Personal:Unknown``,
# ``Expenses:Personal:Unclassified`` all count.
UNRESOLVED_LEAVES: frozenset[str] = frozenset({
    "FIXME",
    "UNKNOWN",
    "UNCATEGORIZED",
    "UNCLASSIFIED",
})


def _normalize_payee(text: str | None) -> str:
    if not text:
        return ""
    return _WS.sub(" ", text.lower()).strip()


def _is_unresolved_leaf(
    account: str | None,
    *,
    extra_ok_leaves: frozenset[str] = frozenset(),
) -> bool:
    """True when the account's last path segment indicates "not yet
    classified." ``extra_ok_leaves`` lets callers opt out of flagging
    specific tokens — e.g., if you treat ``Uncategorized`` as a
    legitimate catch-all category, pass
    ``extra_ok_leaves=frozenset({"UNCATEGORIZED"})`` and it stops
    counting as unresolved."""
    if not account:
        return False
    leaf = account.split(":")[-1].upper()
    if leaf in extra_ok_leaves:
        return False
    return leaf in UNRESOLVED_LEAVES


@dataclass(frozen=True)
class FixmePayee:
    """One payee whose transactions skew heavily to FIXME."""
    normalized_payee: str
    total: int
    fixme_count: int
    fixme_share: float
    sample_accounts: tuple[tuple[str, int], ...]   # non-FIXME accounts seen, with counts


@dataclass
class PreflightReport:
    """Output of ``fixme_heavy_payees``."""
    payees: list[FixmePayee] = field(default_factory=list)
    total_fixme_txns: int = 0
    total_scanned_txns: int = 0

    @property
    def is_clean(self) -> bool:
        return not self.payees


def fixme_heavy_payees(
    entries: Iterable,
    *,
    min_count: int = 10,
    min_share: float = 0.6,
    top_n: int = 100,
    extra_ok_leaves: frozenset[str] = frozenset(),
) -> PreflightReport:
    """Walk the ledger and flag payees whose transactions land on
    a FIXME account at or above ``min_share`` across at least
    ``min_count`` occurrences.

    Defaults (10 occurrences, 60% FIXME share) are conservative —
    a one-off with no resolution isn't a flagged pattern, but a
    payee with 10+ hits that 6+ were never categorized is.

    Returns a ``PreflightReport`` ranked by FIXME count descending
    so the most-impactful fixes surface first.
    """
    # Per-payee histogram of accounts observed. We count every
    # Expenses/Income/Equity posting (the classification target
    # side) exactly like mine_rules does, so FIXME vs non-FIXME
    # shares are computed from the same baseline.
    observed: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    total_fixme_txns = 0
    total_scanned = 0

    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        payee_src = entry.payee or entry.narration or ""
        norm = _normalize_payee(payee_src)
        if not norm:
            continue
        touched_fixme = False
        counted_something = False
        for posting in entry.postings or []:
            account = posting.account or ""
            root = account.split(":", 1)[0]
            if root not in {"Expenses", "Income", "Equity"}:
                continue
            observed[norm][account] += 1
            counted_something = True
            if _is_unresolved_leaf(account, extra_ok_leaves=extra_ok_leaves):
                touched_fixme = True
        if counted_something:
            total_scanned += 1
        if touched_fixme:
            total_fixme_txns += 1

    flagged: list[FixmePayee] = []
    for payee, accounts in observed.items():
        total = sum(accounts.values())
        if total < min_count:
            continue
        fixme_count = sum(
            count for acct, count in accounts.items()
            if _is_unresolved_leaf(acct, extra_ok_leaves=extra_ok_leaves)
        )
        if fixme_count == 0:
            continue
        share = fixme_count / total
        if share < min_share:
            continue
        non_fixme = sorted(
            ((a, c) for a, c in accounts.items()
             if not _is_unresolved_leaf(a, extra_ok_leaves=extra_ok_leaves)),
            key=lambda kv: kv[1], reverse=True,
        )
        flagged.append(
            FixmePayee(
                normalized_payee=payee,
                total=total,
                fixme_count=fixme_count,
                fixme_share=round(share, 3),
                sample_accounts=tuple(non_fixme[:5]),
            )
        )
    flagged.sort(
        key=lambda p: (-p.fixme_count, -p.total, p.normalized_payee),
    )
    return PreflightReport(
        payees=flagged[:top_n],
        total_fixme_txns=total_fixme_txns,
        total_scanned_txns=total_scanned,
    )


def report_hash(report: PreflightReport) -> str:
    """Stable hash over the flagged-payee set. Used to track the
    acknowledgment: when new FIXME-heavy patterns accumulate, the
    hash changes and the acknowledgment goes stale, forcing a
    fresh review before reboot-apply can proceed."""
    payload = "\n".join(
        f"{p.normalized_payee}|{p.fixme_count}|{p.total}"
        for p in report.payees
    )
    return hashlib.sha1(
        payload.encode("utf-8"), usedforsecurity=False,
    ).hexdigest()

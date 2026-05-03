# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Multi-leg payment group proposer (WP5).

Some loan payments arrive as several transactions inside a short
window — separate principal / escrow / insurance pulls from the
servicer, or the user chunking a payment across two checking draws.
Treated individually, they look like N unclassified FIXMEs; the
WP6 auto-classifier preempts each one but has nothing sensible to
say about the one that's 600 while the monthly is 3500.

``propose_groups`` scans a sliding window over FIXME candidates and
enumerates subsets whose amounts sum to the configured monthly
payment (± tolerance). One subset per window, preferring minimum
date-span on ties. The hard enumeration cap is pre-flight: subsets
counted via combinatorics; above ``subset_cap`` the window is skipped
with an info-tier signal and the FIXMEs fall back to one-at-a-time
surfacing. 500 is generous for normal windows and only trips on
backfill-scale density where proposer output would be noise anyway.

The proposer is pure with respect to its inputs — no writes, no
DB. Callers persist ``ProposedGroup`` rows as ``status='proposed'``
in ``loan_payment_groups`` (cache) and route the user to
``POST /settings/loans/{slug}/groups/{group_id}/confirm`` for write.

Reconstruct rebuilds ``status='confirmed'`` rows from the
``lamella-loan-group-members`` meta on override blocks; proposed /
dismissed rows are ephemeral UI state and are re-derivable by
re-running this proposer.
"""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from itertools import combinations
from math import comb
from pathlib import Path
from typing import Any, Sequence

from beancount.core.data import Open, Transaction

log = logging.getLogger(__name__)


# Default safety cap. Above this the proposer declines to enumerate
# and emits an info-tier next-action; pick your groupings manually.
DEFAULT_SUBSET_CAP = 500

# Default subset member cap. 2 ≤ k ≤ 4 covers every realistic split
# we've seen (principal+escrow, principal+escrow+insurance,
# principal+escrow+tax+insurance). 5+ would mean either the window
# is too wide or the loan has a pathological payment structure we
# should flag manually.
DEFAULT_MAX_MEMBERS = 4


# --------------------------------------------------------------------- types


@dataclass(frozen=True)
class FixmeLeg:
    """A normalized FIXME candidate the proposer operates on.

    Kept separate from ``Transaction`` so the caller can feed either
    raw ledger entries (via ``from_transactions``) or dict-shaped
    rows assembled from any other source (e.g. the coverage engine's
    extras list). ``amount`` is unsigned — the total dollar value of
    the payment leg, whatever its sign in the ledger.
    """
    txn_hash: str
    date: date
    amount: Decimal
    touches_liability: bool = False   # true if the txn has a posting
                                      # to the loan's liability path —
                                      # candidate for primary member


@dataclass(frozen=True)
class ProposedGroup:
    group_id: str
    loan_slug: str
    member_hashes: tuple[str, ...]
    aggregate_amount: Decimal
    date_span_start: date
    date_span_end: date
    suggested_in_flight_path: str
    suggested_primary_hash: str
    sum_delta: Decimal                # (aggregate - expected_total); signed


@dataclass(frozen=True)
class ProposerReport:
    groups: list[ProposedGroup]
    dense_windows: list[tuple[date, date, int]]  # (start, end, candidate_count)
    info_signals: list[str] = field(default_factory=list)


# ---------------------------------------------------------- id + path helpers


def compute_group_id(member_hashes: Sequence[str]) -> str:
    """Stable 16-char id derived from the sorted, comma-joined hashes.

    Same inputs → same id, so re-running the proposer on the same
    ledger state produces the same group ids and the UI can dedup
    against the cache without extra bookkeeping.
    """
    joined = ",".join(sorted(member_hashes))
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:16]


def in_flight_path_for(loan: dict) -> str:
    """The default in-flight staging account for a loan. Used to zero
    out non-primary members' FIXMEs during group confirmation.

    Shape: ``Assets:{Entity}:InFlight:Loans:{Slug}``.
    """
    entity = loan.get("entity_slug") or "Personal"
    slug = loan.get("slug") or "Unknown"
    return f"Assets:{entity}:InFlight:Loans:{slug}"


# --------------------------------------------------------- input adaptation


def _as_decimal(v: Any) -> Decimal | None:
    if v is None or v == "":
        return None
    try:
        return Decimal(str(v))
    except (InvalidOperation, ValueError, TypeError):
        return None


def from_transactions(
    entries: Sequence[Any],
    fixme_account_prefix: str,
    liability_path: str | None,
) -> list[FixmeLeg]:
    """Extract FixmeLegs from raw Transaction entries.

    A FIXME candidate is any transaction with at least one posting
    landing on an account starting with ``fixme_account_prefix``
    (typically ``Expenses:FIXME``). Amount is the unsigned sum of
    such postings; ``touches_liability`` notes whether the same txn
    has a posting to the loan's liability path (makes it the natural
    primary candidate).
    """
    from lamella.core.beancount_io.txn_hash import txn_hash as _hash

    legs: list[FixmeLeg] = []
    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        fixme_amount = Decimal("0")
        touches_liability = False
        for p in entry.postings or []:
            acct = getattr(p, "account", None) or ""
            units = getattr(p, "units", None)
            num = getattr(units, "number", None) if units is not None else None
            if num is None:
                continue
            amt = Decimal(num)
            if acct.startswith(fixme_account_prefix):
                fixme_amount += amt.copy_abs()
            if liability_path and acct == liability_path:
                touches_liability = True
        if fixme_amount <= 0:
            continue
        legs.append(FixmeLeg(
            txn_hash=_hash(entry),
            date=entry.date,
            amount=fixme_amount,
            touches_liability=touches_liability,
        ))
    legs.sort(key=lambda l: l.date)
    return legs


# ---------------------------------------------------------- proposer core


def _subset_count(window_size: int, max_members: int) -> int:
    """Exact count of 2..max_members-sized subsets of a window."""
    if window_size < 2:
        return 0
    total = 0
    for k in range(2, min(max_members, window_size) + 1):
        total += comb(window_size, k)
    return total


def _window_slices(
    legs: Sequence[FixmeLeg], window_days: int,
) -> list[list[FixmeLeg]]:
    """Unique left-anchored sliding windows over date-sorted legs.

    A window starts at each leg that isn't already fully contained in
    the previous window's span. Result is a list of (legs) covering
    each distinct neighborhood exactly once.
    """
    if not legs:
        return []
    span = timedelta(days=window_days)
    windows: list[list[FixmeLeg]] = []
    i = 0
    n = len(legs)
    while i < n:
        anchor = legs[i].date
        end = anchor + span
        j = i
        while j < n and legs[j].date <= end:
            j += 1
        # legs[i:j] forms one window.
        if j - i >= 2:
            windows.append(list(legs[i:j]))
        i += 1
    return windows


def _best_subset(
    window: Sequence[FixmeLeg],
    expected_total: Decimal,
    tolerance: Decimal,
    max_members: int,
) -> tuple[tuple[FixmeLeg, ...], Decimal] | None:
    """Pick the best matching subset within one window.

    Returns (subset, delta) where delta = aggregate - expected_total.
    Preference order on matching subsets:
      1. Smaller |delta|.
      2. Smaller date span (tighter grouping wins).
      3. More members (tightness tiebreak).
      4. Lexicographic hash order (deterministic).
    """
    best: tuple[tuple[FixmeLeg, ...], Decimal] | None = None
    best_key: tuple[Decimal, int, int, tuple[str, ...]] | None = None
    upper = max_members if max_members <= len(window) else len(window)
    for k in range(2, upper + 1):
        for combo in combinations(window, k):
            agg = sum((leg.amount for leg in combo), Decimal("0"))
            delta = agg - expected_total
            if abs(delta) > tolerance:
                continue
            span = (combo[-1].date - combo[0].date).days
            # Negative member count → prefer more members.
            hashes = tuple(sorted(leg.txn_hash for leg in combo))
            key = (abs(delta), span, -len(combo), hashes)
            if best_key is None or key < best_key:
                best_key = key
                best = (combo, delta)
    return best


def propose_groups(
    loan: dict,
    fixme_candidates: Sequence[FixmeLeg],
    expected_total: Decimal,
    *,
    already_grouped_hashes: Sequence[str] | None = None,
    window_days: int = 5,
    tolerance: Decimal = Decimal("2.00"),
    max_members: int = DEFAULT_MAX_MEMBERS,
    subset_cap: int = DEFAULT_SUBSET_CAP,
) -> ProposerReport:
    """Propose one group per eligible window.

    ``already_grouped_hashes`` is the set of txn_hashes that are
    already members of a confirmed group — they're excluded from
    the candidate set so the proposer doesn't re-surface them.

    Windows with > ``subset_cap`` possible subsets are skipped and
    reported via ``dense_windows`` + ``info_signals``. The cap is a
    pre-flight combinatoric check, not a post-hoc trim: we never
    enumerate past the cap.
    """
    slug = loan.get("slug") or ""
    liability = loan.get("liability_account_path")
    in_flight = in_flight_path_for(loan)

    skip = set(already_grouped_hashes or ())
    legs = [c for c in fixme_candidates if c.txn_hash not in skip]
    legs.sort(key=lambda l: l.date)

    groups: list[ProposedGroup] = []
    dense: list[tuple[date, date, int]] = []
    signals: list[str] = []
    consumed: set[str] = set()

    for window in _window_slices(legs, window_days):
        window = [leg for leg in window if leg.txn_hash not in consumed]
        if len(window) < 2:
            continue
        count = _subset_count(len(window), max_members)
        if count > subset_cap:
            start = window[0].date
            end = window[-1].date
            dense.append((start, end, len(window)))
            signals.append(
                f"dense window {start.isoformat()}–{end.isoformat()}: "
                f"{len(window)} FIXME candidates, groups not proposed "
                f"automatically (would require enumerating {count:,} "
                f"subsets; cap is {subset_cap:,})."
            )
            continue

        picked = _best_subset(window, expected_total, tolerance, max_members)
        if picked is None:
            continue
        subset, delta = picked
        hashes = tuple(sorted(leg.txn_hash for leg in subset))
        primary = _pick_primary(subset, liability)
        aggregate = sum((leg.amount for leg in subset), Decimal("0"))
        start = min(leg.date for leg in subset)
        end = max(leg.date for leg in subset)
        groups.append(ProposedGroup(
            group_id=compute_group_id(hashes),
            loan_slug=slug,
            member_hashes=hashes,
            aggregate_amount=aggregate,
            date_span_start=start,
            date_span_end=end,
            suggested_in_flight_path=in_flight,
            suggested_primary_hash=primary,
            sum_delta=delta,
        ))
        consumed.update(hashes)

    return ProposerReport(
        groups=groups, dense_windows=dense, info_signals=signals,
    )


def _pick_primary(
    subset: Sequence[FixmeLeg], liability_path: str | None,
) -> str:
    """Pick the member whose posting most plausibly carries the real
    split. Preference:
      1. A member that actually touches the liability account.
      2. Otherwise the largest-amount member (the real payment leg
         when the rest are just transfer splits).
      3. Otherwise first-by-date then first-by-hash (deterministic).
    """
    if liability_path:
        candidates = [leg for leg in subset if leg.touches_liability]
        if candidates:
            return max(
                candidates,
                key=lambda leg: (leg.amount, -leg.date.toordinal(), leg.txn_hash),
            ).txn_hash
    return max(
        subset,
        key=lambda leg: (leg.amount, -leg.date.toordinal(), leg.txn_hash),
    ).txn_hash


# ---------------------------------------------------------- reader: confirmed


def read_loan_payment_groups(entries: Sequence[Any]) -> list[dict[str, Any]]:
    """Rebuild confirmed ``loan_payment_groups`` rows from the ledger.

    Source of truth is the ``lamella-loan-group-members`` meta stamped
    on override blocks (see ``OverrideWriter.append_split``). One
    row per unique ``group_id``; members are the comma-separated hash
    list, lex-sorted. The override carrying the primary split (the
    one that posts to principal/interest/escrow rather than to the
    in-flight account) is recorded as ``primary_hash``.

    Proposed / dismissed statuses are ephemeral UI state and are NOT
    reconstructed — the live proposer re-derives them on each render.
    """
    by_gid: dict[str, dict[str, Any]] = {}
    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        meta = getattr(entry, "meta", None) or {}
        members_raw = meta.get("lamella-loan-group-members")
        slug = meta.get("lamella-loan-slug") or meta.get("lamella-loan-group-slug")
        if not members_raw or not slug:
            continue
        members = tuple(sorted(
            m.strip() for m in str(members_raw).split(",") if m.strip()
        ))
        if len(members) < 2:
            continue
        gid = compute_group_id(members)
        primary_hash = _str(meta.get("lamella-override-of"))
        # Aggregate = absolute value of the txn's from-account leg
        # (the -sum leg). Alternatively we can sum the positive split
        # legs — same value, same sign. Pick positive sum of non-from
        # postings touching leg paths.
        agg = Decimal("0")
        for p in entry.postings or []:
            units = getattr(p, "units", None)
            num = getattr(units, "number", None) if units is not None else None
            if num is None:
                continue
            n = Decimal(num)
            if n > 0:
                agg += n
        existing = by_gid.get(gid)
        if existing is None:
            by_gid[gid] = {
                "group_id": gid,
                "loan_slug": str(slug),
                "member_hashes": ",".join(members),
                "aggregate_amount": str(agg),
                "date_span_start": entry.date.isoformat(),
                "date_span_end": entry.date.isoformat(),
                "primary_hash": primary_hash,
                "status": "confirmed",
            }
        else:
            # A group can have multiple override blocks (one per
            # member). Keep the widest date span, the primary we saw
            # (the block touching principal/interest meta), and the
            # largest aggregate (the primary-member block).
            d = entry.date.isoformat()
            if d < existing["date_span_start"]:
                existing["date_span_start"] = d
            if d > existing["date_span_end"]:
                existing["date_span_end"] = d
            if agg > Decimal(existing["aggregate_amount"] or "0"):
                existing["aggregate_amount"] = str(agg)
                if primary_hash:
                    existing["primary_hash"] = primary_hash
    return list(by_gid.values())


def _str(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


# ------------------------------------------------ in-flight account helper


def ensure_in_flight_account(
    loan: dict,
    entries: Sequence[Any],
    *,
    main_bean: Path,
    connector_accounts: Path,
    earliest_member_date: date,
    run_check: bool = True,
) -> str:
    """Auto-open the default ``Assets:{Entity}:InFlight:Loans:{Slug}``
    account, dated on or before the earliest member transaction so a
    group-confirmation write can't be rejected for ``inactive account``.

    Idempotent: skips the write if the path is already open, and dates
    any new write to ``min(earliest_member_date, today)``.

    Returns the path either way so the caller can use it.
    """
    from lamella.core.registry.accounts_writer import AccountsWriter

    path = in_flight_path_for(loan)

    existing: set[str] = set()
    earliest_refs: dict[str, date] = {}
    for entry in entries:
        if isinstance(entry, Open) and entry.account == path:
            existing.add(path)
        if isinstance(entry, Transaction):
            for p in entry.postings or []:
                if getattr(p, "account", None) == path:
                    prev = earliest_refs.get(path)
                    if prev is None or entry.date < prev:
                        earliest_refs[path] = entry.date

    if path in existing:
        return path

    writer = AccountsWriter(
        main_bean=main_bean,
        connector_accounts=connector_accounts,
        run_check=run_check,
    )
    writer.write_opens(
        [path],
        opened_on=earliest_member_date,
        comment=f"Loan {loan.get('slug') or ''} in-flight staging (WP5 groups)",
        existing_paths=existing,
        earliest_ref_by_path=earliest_refs,
    )
    return path


# ------------------------------------------------ group confirmation writer


@dataclass(frozen=True)
class MemberPosting:
    """One member's contribution to a confirmed group.

    ``from_account`` is the FIXME leaf the original txn posted to
    (``Expenses:FIXME:<card>`` or equivalent); ``amount`` is the
    positive dollar value that should move off that account.
    """
    txn_hash: str
    txn_date: date
    amount: Decimal
    from_account: str


@dataclass(frozen=True)
class GroupApplyResult:
    group_id: str
    member_hashes: tuple[str, ...]
    primary_hash: str
    in_flight_path: str
    blocks_written: int


def apply_group(
    loan: dict,
    *,
    group_id: str,
    members: Sequence[MemberPosting],
    primary_hash: str,
    primary_splits: Sequence[tuple[str, Decimal]],
    in_flight_path: str,
    writer,  # OverrideWriter (typed via duck-type to avoid circular import)
) -> GroupApplyResult:
    """Write the full override-block chain for a confirmed group.

    Shape:
      * One block per **non-primary** member that moves ``amount``
        from that member's FIXME account → in-flight staging.
      * One block on the **primary** member that (a) pulls the
        non-primary legs back out of in-flight and (b) applies
        ``primary_splits`` across the real accounts
        (principal / interest / escrow / tax / insurance).

      After the chain in-flight nets to $0 and the primary's splits
      carry the real categorization. Every block is stamped with:
        - ``lamella-loan-group-id`` (opaque 16-char id)
        - ``lamella-loan-group-members`` (comma-separated sorted hashes)
        - ``lamella-loan-slug``

    ``primary_splits`` sums to the primary's own amount plus the
    non-primary aggregate — i.e. the group's full aggregate. The
    caller validates that before calling; we re-check to keep the
    invariant obvious.
    """
    if not members:
        raise ValueError("members cannot be empty")
    if len(members) < 2:
        raise ValueError("a group needs at least 2 members")
    slug = loan.get("slug") or ""
    hashes = tuple(sorted(m.txn_hash for m in members))
    if primary_hash not in hashes:
        raise ValueError(f"primary_hash {primary_hash} not in members {hashes}")

    aggregate = sum((m.amount for m in members), Decimal("0"))
    primary_split_total = sum((amt for _, amt in primary_splits), Decimal("0"))
    if abs(primary_split_total - aggregate) > Decimal("0.01"):
        raise ValueError(
            f"primary_splits total {primary_split_total} does not match "
            f"group aggregate {aggregate}"
        )

    members_by_hash = {m.txn_hash: m for m in members}
    primary = members_by_hash[primary_hash]

    members_csv = ",".join(hashes)
    common_meta = {
        "lamella-loan-slug": slug,
        "lamella-loan-group-id": group_id,
        "lamella-loan-group-members": members_csv,
    }

    blocks = 0

    # Non-primary members → route their FIXME onto in-flight. Each one
    # becomes an override_block with a single split to in-flight.
    for m in members:
        if m.txn_hash == primary_hash:
            continue
        writer.append_split(
            txn_date=m.txn_date,
            txn_hash=m.txn_hash,
            from_account=m.from_account,
            splits=[(in_flight_path, m.amount)],
            narration=f"loan-group {group_id} member leg",
            extra_meta={
                **common_meta,
                "lamella-loan-group-role": "member",
            },
        )
        blocks += 1

    # Primary member → pull the non-primary sum out of in-flight and
    # land the full aggregate across the real accounts. The primary's
    # own FIXME leaf contributes ``primary.amount``, so the block's
    # ``from_account`` leg is ``primary.amount + non_primary_sum``
    # debited in total, split as: credit in-flight for the non-primary
    # sum (moves the holding balance back out), and debit each real
    # account per the split plan.
    non_primary_sum = aggregate - primary.amount

    # Composite from-side: primary FIXME account absorbs its own
    # amount; in-flight absorbs the non-primary sum (negative, so it
    # empties out). The OverrideWriter.append_split contract is
    # from_account gets a single -total leg; for the group we need a
    # two-source shape, so we emit two synthetic "splits" where the
    # in-flight goes in with a NEGATIVE amount. The writer handles
    # that naturally — splits can be negative, they just sum to the
    # -from leg.
    final_splits: list[tuple[str, Decimal]] = []
    if non_primary_sum > 0:
        # Pull non-primary-sum out of in-flight: post -non_primary_sum
        # on in-flight (which, combined with the +non_primary_sum
        # postings from each non-primary block, zeroes in-flight).
        final_splits.append((in_flight_path, -non_primary_sum))
    final_splits.extend(primary_splits)

    writer.append_split(
        txn_date=primary.txn_date,
        txn_hash=primary.txn_hash,
        from_account=primary.from_account,
        splits=final_splits,
        narration=f"loan-group {group_id} aggregate payment",
        extra_meta={
            **common_meta,
            "lamella-loan-group-role": "primary",
        },
    )
    blocks += 1

    return GroupApplyResult(
        group_id=group_id,
        member_hashes=hashes,
        primary_hash=primary_hash,
        in_flight_path=in_flight_path,
        blocks_written=blocks,
    )

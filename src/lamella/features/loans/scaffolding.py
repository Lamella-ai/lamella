# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Loan scaffolding self-check.

Inspects a loan against the ledger state and returns an ordered list
of `Issue` records — things that would break record-payment,
fund-initial, or reporting if left unresolved. Each issue carries an
autofix hint so the route handler can offer a one-click repair; the
actual autofix dispatcher lives here too so page render and POST
autofix share one code path.

Two entry points:

- `check(loan, entries, conn, settings) -> list[Issue]` — pure with
  respect to inputs; no writes.
- `autofix(issue_kind, loan, path, settings, reader, conn)` — writes
  to the ledger and SQLite; raises `ScaffoldingError` on
  can't-act-without-user-input cases.

The `ensure_open_on_or_before` helper that was previously in
routes/loans.py lives here now. Both the existing route-handler paths
(funding, record-payment) and the new autofix endpoint call through
this one function so Open-date management stays in one place.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Literal, Sequence

from beancount.core.data import Open, Transaction

from lamella.core.beancount_io import LedgerReader
from lamella.core.ledger_writer import BeanCheckError
from lamella.core.registry.accounts_writer import AccountsWriter
from lamella.core.registry.service import suggest_slug

log = logging.getLogger(__name__)


Severity = Literal["blocking", "attention", "info"]


class ScaffoldingError(Exception):
    """Raised by autofix helpers when a fix can't proceed without
    user action (e.g. an Open directive living in a user-authored
    file that we refuse to rewrite). Carries a recommended HTTP
    status so the route handler can map it to HTTPException.
    """

    def __init__(self, message: str, *, status: int = 400):
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class Issue:
    kind: str
    severity: Severity
    path: str | None
    message: str
    can_autofix: bool
    fix_endpoint: str | None
    fix_payload: dict = field(default_factory=dict)


# --------------------------------------------------------------------- helpers


def _to_date_safe(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    try:
        return datetime.fromisoformat(str(value)[:10]).date()
    except (TypeError, ValueError):
        return None


def _default_escrow_path(entity: str | None, institution: str | None,
                         slug: str) -> str | None:
    if not entity or not slug:
        return None
    if institution:
        inst_slug = suggest_slug(institution) or institution.replace(" ", "")
        return f"Assets:{entity}:{inst_slug}:{slug}:Escrow"
    return f"Assets:{entity}:{slug}:Escrow"


def _default_tax_path(entity: str | None, slug: str) -> str | None:
    if not entity or not slug:
        return None
    return f"Expenses:{entity}:{slug}:PropertyTax"


def _default_insurance_path(entity: str | None, slug: str) -> str | None:
    if not entity or not slug:
        return None
    return f"Expenses:{entity}:{slug}:Insurance"


def _default_late_fee_path(entity: str | None, slug: str) -> str | None:
    """WP12: late-fee leg expense account. Auto-derives so a user
    recording their first late fee gets the Open scaffolded
    automatically rather than hitting bean-check."""
    if not entity or not slug:
        return None
    return f"Expenses:{entity}:{slug}:LateFees"


def _open_index(entries: Sequence[Any]) -> dict[str, Open]:
    """Account → its Open directive (first seen). Used by every check."""
    idx: dict[str, Open] = {}
    for entry in entries:
        if isinstance(entry, Open) and entry.account not in idx:
            idx[entry.account] = entry
    return idx


def _earliest_txn_date_on(entries: Sequence[Any], path: str) -> date | None:
    earliest: date | None = None
    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        for p in entry.postings:
            if p.account == path:
                d = _to_date_safe(entry.date)
                if d is not None and (earliest is None or d < earliest):
                    earliest = d
                break
    return earliest


# ---------------------------------------------------------- shared Open helper


def ensure_open_on_or_before(
    reader: LedgerReader,
    opener: AccountsWriter,
    path: str,
    on_or_before: date | None,
    *,
    connector_accounts_path: Path,
    comment_tag: str,
) -> None:
    """Guarantee that `path` has an Open directive dated <= on_or_before.

    - No Open → write one dated `on_or_before`.
    - Open exists at an earlier date → no-op.
    - Open in connector_accounts.bean at a later date → rewrite the date.
    - Open in a user-authored file at a later date → raise
      `ScaffoldingError` with a filename:lineno hint; we never rewrite
      someone else's ledger file.
    """
    if not on_or_before:
        return

    existing_open: Open | None = None
    for entry in reader.load().entries:
        if isinstance(entry, Open) and entry.account == path:
            existing_open = entry
            break

    if existing_open is None:
        opener.write_opens(
            [path],
            opened_on=on_or_before,
            comment=comment_tag,
        )
        return

    open_date = _to_date_safe(existing_open.date)
    if open_date and open_date <= on_or_before:
        return

    meta = getattr(existing_open, "meta", None) or {}
    filename = meta.get("filename")
    lineno = meta.get("lineno")

    if filename and Path(filename).resolve() == Path(connector_accounts_path).resolve():
        if not opener.rewrite_open_date(path, on_or_before):
            raise ScaffoldingError(
                f"Could not rewrite Open directive for {path} in "
                f"{connector_accounts_path.name}. Edit the file and "
                f"set the date to {on_or_before.isoformat()} or earlier.",
                status=500,
            )
        return

    raise ScaffoldingError(
        f"Account {path} is opened at {open_date} in "
        f"{filename}:{lineno}, which is after the loan's funded date "
        f"{on_or_before.isoformat()}. Edit that Open directive to use "
        f"{on_or_before.isoformat()} (or earlier) and try again.",
        status=400,
    )


# ------------------------------------------------------------ individual checks


def _check_opens(loan: dict, entries: Sequence[Any]) -> list[Issue]:
    slug = loan.get("slug") or ""
    funded_date = _to_date_safe(loan.get("funded_date"))
    configured = [
        (loan.get("liability_account_path"), "liability"),
        (loan.get("interest_account_path"),  "interest"),
        (loan.get("escrow_account_path"),    "escrow"),
    ]

    opens = _open_index(entries)
    issues: list[Issue] = []

    for path, label in configured:
        if not path:
            continue
        if path not in opens:
            issues.append(Issue(
                kind="open-missing",
                severity="blocking",
                path=path,
                message=f"Configured {label} account {path} has no Open directive.",
                can_autofix=True,
                fix_endpoint=f"/settings/loans/{slug}/autofix",
                fix_payload={"kind": "open-missing", "path": path},
            ))
            continue
        open_date = _to_date_safe(opens[path].date)
        earliest = _earliest_txn_date_on(entries, path)
        if open_date and earliest and earliest < open_date:
            issues.append(Issue(
                kind="open-date-too-late",
                severity="blocking",
                path=path,
                message=(
                    f"{path} is Opened at {open_date} but has a transaction "
                    f"dated {earliest}. bean-check will reject the earlier "
                    f"posting as inactive-account."
                ),
                can_autofix=True,
                fix_endpoint=f"/settings/loans/{slug}/autofix",
                fix_payload={
                    "kind": "open-date-too-late",
                    "path": path,
                    "target_date": (
                        min(earliest, funded_date) if funded_date else earliest
                    ).isoformat(),
                },
            ))

    return issues


def _check_escrow_path(loan: dict) -> list[Issue]:
    if not loan.get("escrow_monthly"):
        return []
    if loan.get("escrow_account_path"):
        return []
    slug = loan.get("slug") or ""
    default = _default_escrow_path(
        loan.get("entity_slug"), loan.get("institution"), slug,
    )
    return [Issue(
        kind="escrow-path-missing",
        severity="attention",
        path=default,
        message=(
            f"Escrow monthly {loan['escrow_monthly']} is configured but "
            f"no escrow account path is set. Record-payment has nowhere "
            f"to post escrow legs."
        ),
        can_autofix=bool(default),
        fix_endpoint=f"/settings/loans/{slug}/autofix",
        fix_payload={"kind": "escrow-path-missing", "path": default or ""},
    )]


def _check_tax_path(loan: dict, entries: Sequence[Any]) -> list[Issue]:
    if not loan.get("property_tax_monthly"):
        return []
    slug = loan.get("slug") or ""
    default = _default_tax_path(loan.get("entity_slug"), slug)
    if not default:
        return []
    opens = _open_index(entries)
    if default in opens:
        return []
    return [Issue(
        kind="tax-path-missing",
        severity="attention",
        path=default,
        message=(
            f"Property tax monthly {loan['property_tax_monthly']} is "
            f"configured but the expected expense account {default} "
            f"isn't opened in the ledger."
        ),
        can_autofix=True,
        fix_endpoint=f"/settings/loans/{slug}/autofix",
        fix_payload={"kind": "tax-path-missing", "path": default},
    )]


def _check_insurance_path(loan: dict, entries: Sequence[Any]) -> list[Issue]:
    if not loan.get("insurance_monthly"):
        return []
    slug = loan.get("slug") or ""
    default = _default_insurance_path(loan.get("entity_slug"), slug)
    if not default:
        return []
    opens = _open_index(entries)
    if default in opens:
        return []
    return [Issue(
        kind="insurance-path-missing",
        severity="attention",
        path=default,
        message=(
            f"Insurance monthly {loan['insurance_monthly']} is configured "
            f"but the expected expense account {default} isn't opened."
        ),
        can_autofix=True,
        fix_endpoint=f"/settings/loans/{slug}/autofix",
        fix_payload={"kind": "insurance-path-missing", "path": default},
    )]


def _check_property_slug(loan: dict, conn: Any) -> list[Issue]:
    prop = loan.get("property_slug")
    if not prop or conn is None:
        return []
    row = conn.execute(
        "SELECT slug FROM properties WHERE slug = ?", (prop,),
    ).fetchone()
    if row is not None:
        return []
    slug = loan.get("slug") or ""
    return [Issue(
        kind="property-slug-dangling",
        severity="attention",
        path=None,
        message=(
            f"This loan references property '{prop}', which no longer "
            f"exists in the registry. Unlink or re-link on the edit page."
        ),
        can_autofix=False,
        fix_endpoint=f"/settings/loans/{slug}/edit",
        fix_payload={"property_slug": prop},
    )]


def _check_simplefin_stale(loan: dict, entries: Sequence[Any]) -> list[Issue]:
    sf_id = loan.get("simplefin_account_id")
    if not sf_id:
        return []
    # A SimpleFIN-sourced txn on this loan carries the account-id in
    # its lamella-simplefin-account-id meta. Scan recent transactions.
    cutoff = datetime.now(timezone.utc).date().toordinal() - 45
    latest_ord: int | None = None
    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        meta = getattr(entry, "meta", None) or {}
        if meta.get("lamella-simplefin-account-id") != sf_id:
            continue
        d = _to_date_safe(entry.date)
        if d is None:
            continue
        o = d.toordinal()
        if latest_ord is None or o > latest_ord:
            latest_ord = o
    if latest_ord is None or latest_ord < cutoff:
        slug = loan.get("slug") or ""
        days = 45 if latest_ord is None else (
            datetime.now(timezone.utc).date().toordinal() - latest_ord
        )
        return [Issue(
            kind="simplefin-stale",
            severity="info",
            path=None,
            message=(
                f"No SimpleFIN transactions on this loan in {days}+ days. "
                f"The servicer connection may have lapsed — re-authenticate "
                f"from /settings/simplefin if needed."
            ),
            can_autofix=False,
            fix_endpoint="/settings/simplefin",
            fix_payload={},
        )]
    return []


# ---------------------------------------------------------------------- check()


def check(
    loan: dict, entries: Sequence[Any], conn: Any, settings: Any,
) -> list[Issue]:
    """Inspect the loan; return every issue. Pure — no writes."""
    issues: list[Issue] = []
    issues.extend(_check_opens(loan, entries))
    issues.extend(_check_escrow_path(loan))
    issues.extend(_check_tax_path(loan, entries))
    issues.extend(_check_insurance_path(loan, entries))
    issues.extend(_check_property_slug(loan, conn))
    issues.extend(_check_simplefin_stale(loan, entries))
    return issues


# --------------------------------------------------------------------- autofix


def autofix(
    issue_kind: str,
    loan: dict,
    path: str | None,
    *,
    settings: Any,
    reader: LedgerReader,
    conn: Any,
) -> None:
    """Apply the fix for one issue.

    The caller is expected to have re-run `check()` first and verified
    the issue still exists (stale-click guard). This function raises
    `ScaffoldingError` when a fix can't proceed without user input.
    """
    slug = loan.get("slug") or ""
    funded_date = _to_date_safe(loan.get("funded_date"))

    if issue_kind in ("open-missing", "open-date-too-late"):
        if not path:
            raise ScaffoldingError("path required for open-* autofix")
        target_date = _earliest_txn_date_on(reader.load().entries, path)
        if funded_date:
            target_date = (
                min(target_date, funded_date) if target_date else funded_date
            )
        if target_date is None:
            target_date = funded_date
        if target_date is None:
            raise ScaffoldingError(
                f"No funded_date on loan {slug} — set it on the edit page "
                f"before autofixing Open directives."
            )
        opener = AccountsWriter(
            main_bean=settings.ledger_main,
            connector_accounts=settings.connector_accounts_path,
        )
        try:
            ensure_open_on_or_before(
                reader, opener, path, target_date,
                connector_accounts_path=settings.connector_accounts_path,
                comment_tag=f"Loan scaffold for {slug} (autofix)",
            )
        except BeanCheckError as exc:
            raise ScaffoldingError(
                f"bean-check failed after writing Open: {exc}", status=500,
            )
        reader.invalidate()
        return

    if issue_kind == "escrow-path-missing":
        default = path or _default_escrow_path(
            loan.get("entity_slug"), loan.get("institution"), slug,
        )
        if not default:
            raise ScaffoldingError(
                "Can't derive an escrow path without entity + slug. "
                "Set entity on the edit page first."
            )
        # Persist the derived path on both SQLite and the ledger
        # directive so reconstruct rebuilds it, then scaffold the
        # Open if needed.
        conn.execute(
            "UPDATE loans SET escrow_account_path = ? WHERE slug = ?",
            (default, slug),
        )
        _rewrite_loan_directive(loan, settings, escrow_override=default)
        reader.invalidate()
        opener = AccountsWriter(
            main_bean=settings.ledger_main,
            connector_accounts=settings.connector_accounts_path,
        )
        try:
            ensure_open_on_or_before(
                reader, opener, default, funded_date,
                connector_accounts_path=settings.connector_accounts_path,
                comment_tag=f"Loan scaffold for {slug} (escrow autofix)",
            )
        except BeanCheckError as exc:
            raise ScaffoldingError(
                f"bean-check failed writing escrow Open: {exc}", status=500,
            )
        reader.invalidate()
        return

    if issue_kind in ("tax-path-missing", "insurance-path-missing"):
        if not path:
            raise ScaffoldingError(
                "Default expense path could not be derived — set entity."
            )
        opener = AccountsWriter(
            main_bean=settings.ledger_main,
            connector_accounts=settings.connector_accounts_path,
        )
        try:
            ensure_open_on_or_before(
                reader, opener, path, funded_date,
                connector_accounts_path=settings.connector_accounts_path,
                comment_tag=f"Loan scaffold for {slug} ({issue_kind} autofix)",
            )
        except BeanCheckError as exc:
            raise ScaffoldingError(
                f"bean-check failed writing Open: {exc}", status=500,
            )
        reader.invalidate()
        return

    raise ScaffoldingError(f"No autofix implemented for {issue_kind!r}")


def _rewrite_loan_directive(
    loan: dict, settings: Any, *, escrow_override: str | None = None,
) -> None:
    """Re-emit the `custom "loan"` directive with current SQLite values
    so reconstruct can rebuild whatever autofix just changed."""
    from lamella.features.loans.writer import append_loan

    escrow_path = escrow_override or loan.get("escrow_account_path")

    try:
        append_loan(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            slug=loan["slug"],
            display_name=loan.get("display_name"),
            loan_type=loan.get("loan_type") or "other",
            entity_slug=loan.get("entity_slug"),
            institution=loan.get("institution"),
            original_principal=loan.get("original_principal") or "0",
            funded_date=loan.get("funded_date"),
            first_payment_date=loan.get("first_payment_date"),
            payment_due_day=loan.get("payment_due_day"),
            term_months=loan.get("term_months"),
            interest_rate_apr=loan.get("interest_rate_apr"),
            monthly_payment_estimate=loan.get("monthly_payment_estimate"),
            escrow_monthly=loan.get("escrow_monthly"),
            property_tax_monthly=loan.get("property_tax_monthly"),
            insurance_monthly=loan.get("insurance_monthly"),
            liability_account_path=loan.get("liability_account_path"),
            interest_account_path=loan.get("interest_account_path"),
            escrow_account_path=escrow_path,
            simplefin_account_id=loan.get("simplefin_account_id"),
            property_slug=loan.get("property_slug"),
            payoff_date=loan.get("payoff_date"),
            payoff_amount=loan.get("payoff_amount"),
            is_active=bool(loan.get("is_active", 1)),
            notes=loan.get("notes"),
        )
    except Exception as exc:  # noqa: BLE001
        # Non-fatal — SQLite is committed and the user can re-save
        # later if the ledger write was transiently rejected.
        log.warning(
            "loans.scaffolding: loan-directive rewrite failed for %s: %s",
            loan.get("slug"), exc,
        )

# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""WP10 wizard framework.

State lives in the form body (hidden inputs round-trip every prior
step's values) — NOT in URL params. Refresh on a deep-linked step
will restart at step 1, because the URL only carries
``?step={name}`` for routing, not the accumulated state.

Four moving parts:

1. ``WizardFlow`` Protocol — each flow module implements steps(),
   validate(), next_step(), write_plan(), commit().
2. ``PlannedWrite`` hierarchy — pure data describing what a flow's
   commit phase will write. ``render_preview()`` for step-N display,
   ``execute()`` for commit. No I/O at construction.
3. ``WizardCommitTxn`` — context manager that snapshots all connector
   files, runs the body's writes with ``run_check=False``, and either
   bean-checks once at exit (success) or restores all files atomically
   (failure). Mirrors ``OverrideWriter``'s single-write pattern at the
   level of a multi-write batch.
4. ``ValidationError`` — currently a string + optional field name;
   the dataclass shape leaves room for richer field association
   (e.g., per-field error rendering) without breaking existing flows.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from lamella.core.ledger_writer import (
    BeanCheckError,
    capture_bean_check,
    run_bean_check_vs_baseline,
)

log = logging.getLogger(__name__)


# ============================================================ ValidationError


@dataclass(frozen=True)
class ValidationError:
    """One failed validation. ``field`` is the form input name when
    the error is field-specific; None when it's flow-level (e.g.,
    "this combination of choices isn't supported").

    Templates render field-associated errors next to the matching
    input; flow-level errors render in a top-of-form summary box.
    Future: add ``severity``, ``code``, etc. without breaking the
    string-only call sites.
    """
    message: str
    field: str | None = None


# Convenience constructors so flow code reads cleanly without having
# to remember the dataclass name on every call site:
def err(message: str, *, field: str | None = None) -> ValidationError:
    return ValidationError(message=message, field=field)


# ============================================================ WizardStep


@dataclass(frozen=True)
class WizardStep:
    """A single step in a flow. ``name`` is the URL slug
    (?step=loan_terms); ``template`` is the partial path Jinja
    renders; ``title`` is the human-readable header."""
    name: str
    template: str
    title: str


# ============================================================ FlowResult


@dataclass(frozen=True)
class FlowResult:
    """Returned from a flow's commit() — directs the dispatcher to
    redirect somewhere meaningful (typically the new loan's detail
    page or a confirmation summary)."""
    redirect_to: str
    saved_message: str = "wizard-committed"


# ============================================================ PlannedWrite


@dataclass(frozen=True)
class PlannedWrite:
    """Base class for one write the commit phase will execute.

    Concrete subclasses live below — each holds the exact params it
    needs to render a preview row AND to execute the actual write.
    The framework guarantees execute() is called inside a
    WizardCommitTxn so the writer can pass run_check=False without
    risking ledger corruption — the txn check happens once at the
    end of the batch.
    """

    def render_preview(self) -> dict[str, Any]:
        """Return a flat dict the preview template iterates as a row.
        Discriminator key ``kind`` selects the preview row template;
        keys like ``account`` / ``amount`` / ``date`` are rendered as
        cells."""
        raise NotImplementedError

    def execute(self, *, settings: Any, conn: Any, reader: Any) -> None:
        """Apply the write. Must accept ``run_check=False`` semantics
        — the WizardCommitTxn handles the collective bean-check."""
        raise NotImplementedError


@dataclass(frozen=True)
class PlannedPropertyWrite(PlannedWrite):
    slug: str
    display_name: str
    property_type: str  # 'primary_residence' | 'rental' | 'investment' | etc.
    entity_slug: str | None
    address: str | None = None
    purchase_date: str | None = None
    purchase_price: str | None = None
    is_primary_residence: bool = False
    is_rental: bool = False
    notes: str | None = None

    def render_preview(self) -> dict[str, Any]:
        return {
            "kind": "property",
            "summary": f"Create property '{self.slug}' ({self.display_name})",
            "details": [
                ("Slug", self.slug),
                ("Display name", self.display_name),
                ("Type", self.property_type),
                ("Entity", self.entity_slug or "—"),
                ("Address", self.address or "—"),
                ("Purchase date", self.purchase_date or "—"),
                ("Purchase price",
                 f"${self.purchase_price}" if self.purchase_price else "—"),
            ],
        }

    def execute(self, *, settings, conn, reader) -> None:
        from lamella.features.properties.writer import append_property
        append_property(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            slug=self.slug,
            display_name=self.display_name,
            property_type=self.property_type,
            entity_slug=self.entity_slug,
            address=self.address,
            purchase_date=self.purchase_date,
            purchase_price=self.purchase_price,
            is_primary_residence=self.is_primary_residence,
            is_rental=self.is_rental,
            notes=self.notes,
            run_check=False,
        )


@dataclass(frozen=True)
class PlannedLoanWrite(PlannedWrite):
    slug: str
    display_name: str
    loan_type: str
    entity_slug: str | None
    institution: str | None
    original_principal: str
    funded_date: str
    first_payment_date: str | None = None
    payment_due_day: int | None = None
    term_months: int | None = None
    interest_rate_apr: str | None = None
    monthly_payment_estimate: str | None = None
    escrow_monthly: str | None = None
    property_tax_monthly: str | None = None
    insurance_monthly: str | None = None
    liability_account_path: str | None = None
    interest_account_path: str | None = None
    escrow_account_path: str | None = None
    property_slug: str | None = None
    notes: str | None = None

    def render_preview(self) -> dict[str, Any]:
        return {
            "kind": "loan",
            "summary": f"Create loan '{self.slug}' ({self.loan_type})",
            "details": [
                ("Slug", self.slug),
                ("Display name", self.display_name),
                ("Type", self.loan_type),
                ("Entity", self.entity_slug or "—"),
                ("Institution", self.institution or "—"),
                ("Principal", f"${self.original_principal}"),
                ("Funded", self.funded_date),
                ("Term (months)", str(self.term_months or "—")),
                ("APR", f"{self.interest_rate_apr}%" if self.interest_rate_apr else "—"),
                ("Liability account", self.liability_account_path or "—"),
                ("Property", self.property_slug or "—"),
            ],
        }

    def execute(self, *, settings, conn, reader) -> None:
        from lamella.features.loans.writer import append_loan
        append_loan(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            slug=self.slug,
            display_name=self.display_name,
            loan_type=self.loan_type,
            entity_slug=self.entity_slug,
            institution=self.institution,
            original_principal=self.original_principal,
            funded_date=self.funded_date,
            first_payment_date=self.first_payment_date,
            payment_due_day=self.payment_due_day,
            term_months=self.term_months,
            interest_rate_apr=self.interest_rate_apr,
            monthly_payment_estimate=self.monthly_payment_estimate,
            escrow_monthly=self.escrow_monthly,
            property_tax_monthly=self.property_tax_monthly,
            insurance_monthly=self.insurance_monthly,
            liability_account_path=self.liability_account_path,
            interest_account_path=self.interest_account_path,
            escrow_account_path=self.escrow_account_path,
            property_slug=self.property_slug,
            notes=self.notes,
            run_check=False,
        )


@dataclass(frozen=True)
class PlannedAccountsOpen(PlannedWrite):
    paths: tuple[str, ...]
    opened_on: str  # ISO date
    comment: str | None = None

    def render_preview(self) -> dict[str, Any]:
        return {
            "kind": "accounts_open",
            "summary": f"Open {len(self.paths)} account(s) dated {self.opened_on}",
            "details": [(p, self.opened_on) for p in self.paths],
        }

    def execute(self, *, settings, conn, reader) -> None:
        from datetime import date
        from lamella.core.registry.accounts_writer import AccountsWriter
        opener = AccountsWriter(
            main_bean=settings.ledger_main,
            connector_accounts=settings.connector_accounts_path,
            run_check=False,
        )
        # Filter against existing opens to avoid duplicate-open errors.
        existing: set[str] = set()
        if reader is not None:
            from beancount.core.data import Open
            for entry in reader.load().entries:
                if isinstance(entry, Open):
                    existing.add(entry.account)
        opener.write_opens(
            list(self.paths),
            opened_on=date.fromisoformat(self.opened_on[:10]),
            comment=self.comment,
            existing_paths=existing,
        )


@dataclass(frozen=True)
class PlannedLoanFunding(PlannedWrite):
    slug: str
    display_name: str
    funded_date: str
    principal: str
    offset_account: str
    liability_account_path: str
    narration: str | None = None

    def render_preview(self) -> dict[str, Any]:
        return {
            "kind": "loan_funding",
            "summary": f"Fund loan '{self.slug}' for ${self.principal}",
            "details": [
                ("Date", self.funded_date),
                ("Liability", f"{self.liability_account_path}  -${self.principal}"),
                ("Offset", f"{self.offset_account}  +${self.principal}"),
                ("Narration", self.narration or "—"),
            ],
        }

    def execute(self, *, settings, conn, reader) -> None:
        from datetime import date
        from decimal import Decimal
        from lamella.features.loans.writer import write_loan_funding
        # Pull the loan dict back from SQLite — the writer needs it
        # for liability_account_path lookup, and at commit time it's
        # been freshly INSERTed by the prior PlannedLoanWrite.
        row = conn.execute(
            "SELECT * FROM loans WHERE slug = ?", (self.slug,),
        ).fetchone()
        loan = dict(row) if row else {
            "slug": self.slug,
            "display_name": self.display_name,
            "liability_account_path": self.liability_account_path,
        }
        write_loan_funding(
            loan=loan,
            settings=settings,
            funded_date=date.fromisoformat(self.funded_date[:10]),
            principal=Decimal(self.principal),
            offset_account=self.offset_account,
            narration=self.narration,
            run_check=False,
        )


@dataclass(frozen=True)
class PlannedLoanBalanceAnchor(PlannedWrite):
    loan_slug: str
    as_of_date: str
    balance: str
    source: str | None = None
    notes: str | None = None

    def render_preview(self) -> dict[str, Any]:
        return {
            "kind": "loan_balance_anchor",
            "summary": (
                f"Anchor {self.loan_slug} = ${self.balance} on {self.as_of_date}"
            ),
            "details": [
                ("Loan", self.loan_slug),
                ("As-of", self.as_of_date),
                ("Balance", f"${self.balance}"),
                ("Source", self.source or "—"),
            ],
        }

    def execute(self, *, settings, conn, reader) -> None:
        from lamella.features.loans.writer import append_loan_balance_anchor
        append_loan_balance_anchor(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            loan_slug=self.loan_slug,
            as_of_date=self.as_of_date,
            balance=self.balance,
            source=self.source,
            notes=self.notes,
            run_check=False,
        )


@dataclass(frozen=True)
class PlannedSqliteUpsert(PlannedWrite):
    """A SQLite UPSERT planned alongside the ledger writes — rare, but
    needed when the wizard's flow output includes registry rows the
    ledger doesn't carry directly (e.g., a property record where the
    SQLite row is the source of truth and the directive is the cache
    feeder). Most flows DON'T need this; PlannedPropertyWrite already
    handles the property case via its writer.
    """
    table: str
    columns: tuple[str, ...]
    values: tuple[Any, ...]
    on_conflict: str | None = None
    summary_text: str = "SQLite upsert"

    def render_preview(self) -> dict[str, Any]:
        return {
            "kind": "sqlite_upsert",
            "summary": self.summary_text,
            "details": list(zip(self.columns, [str(v) for v in self.values])),
        }

    def execute(self, *, settings, conn, reader) -> None:
        cols = ", ".join(self.columns)
        placeholders = ", ".join("?" for _ in self.columns)
        sql = f"INSERT INTO {self.table} ({cols}) VALUES ({placeholders})"
        if self.on_conflict:
            sql += f" {self.on_conflict}"
        conn.execute(sql, self.values)


@dataclass(frozen=True)
class PlannedSqliteRowUpdate(PlannedWrite):
    """A narrow-purpose UPDATE: set columns on rows matching a single
    WHERE predicate. Used by refi/payoff to flip is_active=0 +
    payoff_date/payoff_amount on the old loan's row when reconstruct
    hasn't yet caught the new directive.

    The savepoint discipline in WizardCommitTxn rolls these back
    alongside the file writes — if any later PlannedWrite or the
    final bean-check fails, the SQLite UPDATE is undone.
    """
    table: str
    set_columns: tuple[tuple[str, Any], ...]   # ((col_name, value), ...)
    where_clause: str
    where_values: tuple[Any, ...]
    summary_text: str = "SQLite UPDATE"

    def render_preview(self) -> dict[str, Any]:
        return {
            "kind": "sqlite_row_update",
            "summary": self.summary_text,
            "details": [(c, str(v)) for c, v in self.set_columns],
        }

    def execute(self, *, settings, conn, reader) -> None:
        set_sql = ", ".join(f"{c} = ?" for c, _ in self.set_columns)
        sql = f"UPDATE {self.table} SET {set_sql} WHERE {self.where_clause}"
        values = tuple(v for _, v in self.set_columns) + self.where_values
        conn.execute(sql, values)


# ============================================================ WizardCommitTxn


class WizardCommitError(Exception):
    """Raised on bean-check rejection at commit-end (after rollback).
    Carries the rejected check's stderr so the dispatcher can show the
    user what specifically failed."""
    def __init__(self, message: str, *, status: int = 500):
        super().__init__(message)
        self.status = status


_SNAPSHOTTED_PATHS_FIELDS = (
    "ledger_main",
    "connector_overrides_path",
    "connector_accounts_path",
    "connector_config_path",
)


class WizardCommitTxn:
    """Context manager wrapping a multi-write commit phase.

    Snapshots every connector file at entry, captures the
    pre-execution bean-check baseline, opens a SQLite SAVEPOINT (when
    ``conn`` is provided), lets the body run N writes each with
    ``run_check=False``, then runs ONE ``run_bean_check_vs_baseline``
    at exit. On any exception OR bean-check failure, restores every
    snapshot byte-for-byte AND rolls back the SAVEPOINT — the ledger
    AND the SQLite cache end up identical to where they started.

    The SAVEPOINT is what makes refi safe: refi's plan UPDATEs the
    old loan's SQLite row (is_active=0) AND emits a cross-ref-anchor
    write. If the anchor write fails, the SAVEPOINT rollback undoes
    the SQLite UPDATE — without it, you'd have an inactive old loan
    in SQLite but no record of why in the ledger.

    Usage::

        with WizardCommitTxn(settings, conn=conn) as txn:
            for planned in plan:
                planned.execute(settings=settings, conn=conn, reader=reader)
        # exit runs the check; raises WizardCommitError on failure.
    """

    _SAVEPOINT_NAME = "wizard_commit"

    def __init__(
        self, settings: Any, *,
        run_check: bool = True,
        conn: Any = None,
    ):
        self.settings = settings
        self.run_check = run_check
        self.conn = conn
        self._snapshots: dict[str, tuple[bytes | None, bool]] = {}
        self._baseline: str = ""
        self._savepoint_open = False

    def __enter__(self) -> "WizardCommitTxn":
        for field_name in _SNAPSHOTTED_PATHS_FIELDS:
            path: Path = getattr(self.settings, field_name, None)
            if path is None:
                continue
            existed = path.exists()
            data = path.read_bytes() if existed else None
            self._snapshots[field_name] = (data, existed)
        if self.run_check:
            try:
                _, self._baseline = capture_bean_check(self.settings.ledger_main)
            except Exception as exc:  # noqa: BLE001
                # If we can't establish a baseline, treat it as empty —
                # any new error becomes a failure. Better than silently
                # accepting all errors as pre-existing.
                log.warning("WizardCommitTxn: baseline capture failed: %s", exc)
                self._baseline = ""
        if self.conn is not None:
            try:
                self.conn.execute(f"SAVEPOINT {self._SAVEPOINT_NAME}")
                self._savepoint_open = True
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "WizardCommitTxn: SAVEPOINT open failed: %s", exc,
                )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        # Body raised → restore + propagate. The framework caller is
        # expected to convert the propagated exception to an HTTP
        # error; we don't swallow exceptions here.
        if exc_type is not None:
            self._restore()
            self._rollback_savepoint()
            log.warning(
                "WizardCommitTxn: body raised %s — rolled back",
                exc_type.__name__,
            )
            return False  # re-raise

        if not self.run_check:
            self._release_savepoint()
            return False

        try:
            run_bean_check_vs_baseline(self.settings.ledger_main, self._baseline)
        except BeanCheckError as exc:
            self._restore()
            self._rollback_savepoint()
            raise WizardCommitError(
                f"Bean-check rejected the wizard commit: {exc}",
            ) from exc
        self._release_savepoint()
        return False

    def _restore(self) -> None:
        for field_name, (data, existed) in self._snapshots.items():
            path: Path = getattr(self.settings, field_name, None)
            if path is None:
                continue
            try:
                if existed:
                    path.write_bytes(data or b"")
                else:
                    path.unlink(missing_ok=True)
            except Exception as exc:  # noqa: BLE001
                # Best-effort rollback. Log loudly — partial restore
                # leaves the ledger in a state we can't reason about.
                log.error(
                    "WizardCommitTxn: failed to restore %s: %s",
                    field_name, exc,
                )

    def _rollback_savepoint(self) -> None:
        if not self._savepoint_open or self.conn is None:
            return
        try:
            self.conn.execute(f"ROLLBACK TO SAVEPOINT {self._SAVEPOINT_NAME}")
            self.conn.execute(f"RELEASE SAVEPOINT {self._SAVEPOINT_NAME}")
        except Exception as exc:  # noqa: BLE001
            log.error("WizardCommitTxn: SAVEPOINT rollback failed: %s", exc)
        finally:
            self._savepoint_open = False

    def _release_savepoint(self) -> None:
        if not self._savepoint_open or self.conn is None:
            return
        try:
            self.conn.execute(f"RELEASE SAVEPOINT {self._SAVEPOINT_NAME}")
        except Exception as exc:  # noqa: BLE001
            log.error("WizardCommitTxn: SAVEPOINT release failed: %s", exc)
        finally:
            self._savepoint_open = False


# ============================================================ WizardFlow


@runtime_checkable
class WizardFlow(Protocol):
    """A wizard flow module implements this protocol.

    ``name`` is the URL flow slug (e.g., ``purchase``); the dispatcher
    routes ``/settings/loans/wizard/purchase/...`` to the flow whose
    name matches.

    Validation runs at every step (idempotent re-validation) so a
    back-edit that invalidates an earlier step's value lands the user
    back at that step instead of carrying a bad value forward.

    write_plan() is pure — no DB writes, no ledger I/O. The framework
    calls it from both the preview step (for rendering) and the
    commit step (for execution).
    """
    name: str
    title: str

    def steps(self) -> dict[str, WizardStep]:
        """Map step name → WizardStep. Order is determined by
        next_step(), not this dict."""
        ...

    def initial_step(self) -> str:
        """Name of the first step — always rendered on GET /flow."""
        ...

    def validate(
        self, step_name: str, params: dict, conn: Any,
    ) -> list[ValidationError]:
        """Validate params for the given step. Returns ALL errors
        (not just the first) so the template can render every
        field-level marker in one pass.

        Defensive note: params here are round-tripped from the form,
        which means hidden fields from prior steps may have been
        edited via View Source. validate() must not assume hidden
        values are sanitized — re-coerce types, re-check ranges, etc.
        Trust no client-side state.
        """
        ...

    def next_step(
        self, current_step: str, params: dict, conn: Any,
    ) -> str | None:
        """Return the name of the step to render after current_step,
        or None when the flow is ready to preview/commit. Linear flows
        return the next step in sequence; branching flows inspect
        params and pick a path."""
        ...

    def write_plan(
        self, params: dict, conn: Any,
    ) -> list[PlannedWrite]:
        """Build the full ordered write plan from validated params.
        Pure — no I/O, no DB writes. Called by both preview and
        commit so the user sees exactly what will be written."""
        ...

    def commit(
        self, params: dict, settings: Any, conn: Any, reader: Any,
    ) -> FlowResult:
        """Execute write_plan() under a WizardCommitTxn. Returns the
        redirect target on success; raises WizardCommitError when
        bean-check rejects the batch."""
        ...

    def template_context(
        self, step_name: str, params: dict, conn: Any,
    ) -> dict:
        """Per-step template context the dispatcher merges into the
        rendered context — e.g., a list of existing property slugs
        for the choose_property datalist. Flows return an empty
        dict when no extra context is needed."""
        ...

# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""First-run onboarding wizard.

Spec: ``FIRST-RUN.md``. Reached on a true fresh install (no
``main.bean``, no prior state) instead of the maintenance setup
checklist at ``/setup``. The checklist is for installs that already
exist and have drifted; this wizard is for someone who has never seen
the system before.

Entry point: ``GET /setup/wizard`` redirects to the user's current
step. Each step renders ``wizard_layout.html`` (no main app chrome)
plus a step-specific content block.

State lives in ``setup.wizard_state`` (single-row JSON blob in
SQLite). Every step writes the user's answers BEFORE rendering the
next page so closing the tab loses nothing.

Actual entities, accounts, properties, and vehicles are written to
their canonical tables + ledger directives via the existing writers
(``upsert_entity``, ``AccountsWriter``, etc.) — the wizard is a
guided front-end on top of the same service layer the maintenance
pages use.
"""
from __future__ import annotations

import logging
import re
import sqlite3
from datetime import UTC, date, datetime
from pathlib import Path

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from lamella.core.bootstrap.scaffold import ScaffoldError, scaffold_fresh
from lamella.core.config import Settings
from lamella.web.deps import get_db, get_settings
from lamella.core.registry.accounts_writer import AccountsWriter
from lamella.core.registry.entity_writer import (
    append_entity_deleted,
    append_entity_directive,
)
from lamella.core.registry.service import (
    is_valid_slug,
    load_categories_yaml_for_entity,
    scaffold_paths_for_entity,
    suggest_slug,
    upsert_entity,
)
from lamella.features.setup.wizard_state import (
    STEP_ACCOUNTS,
    STEP_BANK,
    STEP_DONE,
    STEP_ENTITIES,
    STEP_ORDER,
    STEP_PROPVEHICLE,
    STEP_WELCOME,
    VALID_INTENTS,
    WizardState,
    is_wizard_complete,
    load_state,
    reset_state,
    save_state,
)

log = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Step metadata for the progress bar.
# ---------------------------------------------------------------------------


STEP_META = (
    {"id": STEP_WELCOME, "label": "Welcome", "url": "/setup/wizard/welcome"},
    {"id": STEP_ENTITIES, "label": "Entities", "url": "/setup/wizard/entities"},
    {"id": STEP_BANK, "label": "Bank", "url": "/setup/wizard/bank"},
    {"id": STEP_ACCOUNTS, "label": "Accounts", "url": "/setup/wizard/accounts"},
    {"id": STEP_PROPVEHICLE, "label": "Property", "url": "/setup/wizard/property-vehicle"},
    {"id": STEP_DONE, "label": "Done", "url": "/setup/wizard/done"},
)


ACCOUNT_KIND_OPTIONS = (
    ("checking", "Checking"),
    ("savings", "Savings"),
    ("credit_card", "Credit card"),
    ("line_of_credit", "Line of credit"),
    ("loan", "Loan"),
    ("brokerage", "Brokerage"),
    ("cash", "Cash"),
)

LOAN_KIND_OPTIONS = (
    ("Mortgage", "Mortgage"),
    ("Auto", "Auto"),
    ("Student", "Student"),
    ("Personal", "Personal"),
    ("Other", "Other"),
)

INTENT_OPTIONS = (
    {
        "value": "personal",
        "title": "Just my personal finances",
        "blurb": "Track one personal household. Schedule A categories plus everyday budgeting buckets.",
    },
    {
        "value": "business",
        "title": "A business I own or run",
        "blurb": "Set up one business entity. Schedule C (sole prop / single-member LLC) or Schedule F (farm).",
    },
    {
        "value": "both",
        "title": "Both personal and business",
        "blurb": "One personal entity plus one business entity. Cleanly separates personal and self-employed flows.",
    },
    {
        "value": "household",
        "title": "A whole household",
        "blurb": "Multiple individuals tracked together. Add another person whenever you need.",
    },
    {
        "value": "everything",
        "title": "Everything — multiple people and businesses",
        "blurb": "Households plus multiple businesses. We'll start you with one of each, expandable from there.",
    },
    {
        "value": "manual",
        "title": "Let me configure it myself",
        "blurb": "Skip the wizard and use the existing setup checklist. Recommended only if you've used the app before.",
    },
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ensure_main_bean(settings: Settings) -> None:
    """Scaffold the canonical ledger files if main.bean is missing.

    The wizard is reached on truly-fresh installs, which often means
    no main.bean exists yet. Before the first entity/account write,
    run the existing scaffold_fresh helper so connector_accounts.bean
    + main.bean are in place. Idempotent: refuses gracefully when
    files already exist (which is fine — that means we already ran).
    """
    if settings.ledger_main.exists() and settings.ledger_main.stat().st_size > 0:
        return
    settings.ledger_dir.mkdir(parents=True, exist_ok=True)
    try:
        scaffold_fresh(settings.ledger_dir)
    except ScaffoldError as exc:
        # If the scaffold refuses because some files already exist,
        # keep going — the user may have created them by other means.
        log.info("scaffold_fresh refused (continuing): %s", exc)


def _render(
    request: Request,
    template_name: str,
    context: dict,
    *,
    state: WizardState,
) -> HTMLResponse:
    """Render a wizard step with shared progress-bar context."""
    base_ctx = {
        "wizard_state": state,
        "step_meta": STEP_META,
        "current_step": state.step,
        "step_index": _step_index(state.step),
        "step_total": len(STEP_ORDER),
    }
    base_ctx.update(context)
    return request.app.state.templates.TemplateResponse(
        request, template_name, base_ctx,
    )


def _step_index(step: str) -> int:
    try:
        return STEP_ORDER.index(step)
    except ValueError:
        return 0


def _redirect_to(step: str) -> RedirectResponse:
    for meta in STEP_META:
        if meta["id"] == step:
            return RedirectResponse(meta["url"], status_code=303)
    return RedirectResponse("/setup/wizard/welcome", status_code=303)


def _earliest_incomplete_step(state: WizardState) -> str | None:
    """Return the earliest step the user has not legitimately completed.

    Used to guard ``/setup/wizard/done`` against drive-by visits via
    a stale browser link: a user who never touched welcome / entities
    / accounts shouldn't be able to land on the finalize page and
    submit an empty install.

    Returns ``None`` when every prerequisite is satisfied — i.e. it's
    safe to show the done screen and run the finalize POST.
    """
    if not state.intent:
        return STEP_WELCOME
    if not state.draft_entities:
        return STEP_ENTITIES
    if not any(_account_draft_is_complete(d) for d in state.draft_accounts):
        return STEP_ACCOUNTS
    return None


def _slug_from_name(name: str, fallback: str = "Personal") -> str:
    candidate = suggest_slug(name) if name else ""
    if candidate and is_valid_slug(candidate):
        return candidate
    return fallback


def _scaffold_entity(
    *,
    conn: sqlite3.Connection,
    settings: Settings,
    slug: str,
    display_name: str,
    entity_type: str,
    tax_schedule: str | None,
) -> None:
    """Create or update an entity and stamp its directive.

    Best-effort: a directive-write failure (bean-check) is logged but
    not fatal — the SQLite row still exists, and the next save (e.g.
    when the user edits this entity later) will retry the stamp.
    """
    upsert_entity(
        conn,
        slug=slug,
        display_name=display_name or None,
        entity_type=entity_type or None,
        tax_schedule=tax_schedule or None,
    )
    try:
        append_entity_directive(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            entity_slug=slug,
            display_name=display_name or None,
            entity_type=entity_type or None,
            tax_schedule=tax_schedule or None,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("entity directive stamp failed for %s: %s", slug, exc)


def _scaffold_categories(
    *,
    conn: sqlite3.Connection,
    settings: Settings,
    entity_slug: str,
) -> list[str]:
    """Open the default expense category tree for an entity.

    Returns the list of account paths that were opened (or that we
    *intended* to open — safe to record either way; rollback's Close
    writer skips paths that aren't currently open). Best-effort:
    failures are logged and an empty list is returned.
    """
    row = conn.execute(
        "SELECT slug, display_name, entity_type, tax_schedule "
        "FROM entities WHERE slug = ?",
        (entity_slug,),
    ).fetchone()
    if row is None:
        return []
    yaml_data = load_categories_yaml_for_entity(settings, row)
    paths = [
        p["path"] for p in scaffold_paths_for_entity(yaml_data, entity_slug)
    ]
    if not paths:
        return []
    writer = AccountsWriter(
        main_bean=settings.ledger_main,
        connector_accounts=settings.connector_accounts_path,
    )
    try:
        writer.write_opens(
            paths,
            opened_on=_default_open_date(settings),
            comment=f"Default categories for {entity_slug}",
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("category scaffold failed for %s: %s", entity_slug, exc)
        return []
    return paths


def _default_open_date(settings: Settings) -> date:
    """Resolve settings.account_default_open_date into a real date,
    falling back to 1900-01-01 if the user typo'd the setting.

    Why a far-back default? Beancount requires every Open directive
    to be dated on or before any posting against that account. If
    we use today's date and the user later imports a 2024
    transaction, bean-check rejects it as "inactive account at the
    time of transaction." Most users don't know the actual opening
    date of their personal accounts (CLAUDE.md observation: "I
    don't think anyone actually knows the opening date of their
    accounts unless it is a loan or mortgage"), so we default to a
    date so far back that any historical import stays valid. The
    setting is editable for users who want to go even further back."""
    raw = (settings.account_default_open_date or "").strip()
    try:
        return date.fromisoformat(raw)
    except (TypeError, ValueError):
        log.info(
            "wizard: bad account_default_open_date %r — using 1900-01-01",
            raw,
        )
        return date(1900, 1, 1)


def _close_scaffolded_accounts(
    *,
    settings: Settings,
    paths: list[str],
) -> None:
    """Issue Close directives for category accounts the wizard
    previously opened, when an entity is being rolled back.

    Each Close is independently bean-checked + atomic — a single
    bad path doesn't block the others. Skips paths that are no
    longer Open (e.g., already closed by another rollback pass).
    """
    if not paths:
        return
    writer = AccountsWriter(
        main_bean=settings.ledger_main,
        connector_accounts=settings.connector_accounts_path,
    )
    for path in paths:
        try:
            writer.write_close(path)
        except Exception as exc:  # noqa: BLE001
            # A Close that fails (e.g., account isn't actually open
            # anymore, or has had postings written against it) is
            # logged and skipped — better to leave one orphan Open
            # than to abort the whole rollback.
            log.info("close-account skipped for %s: %s", path, exc)


def _account_path_for(*, entity_slug: str, kind: str, display_name: str,
                     last_four: str | None, institution: str | None) -> str:
    """Compose a Beancount account path for a wizard-created account.

    Asset / liability roots:
      checking, savings, brokerage, cash → Assets
      credit_card, line_of_credit, loan, tax_liability → Liabilities

    Path shape: Assets|Liabilities:<Entity>:<Bank>:<DisplayLeaf>[:<Last4>]
    Falls back to Assets:<Entity>:<Cash> when no institution is set.
    """
    is_liability = kind in {"credit_card", "line_of_credit", "loan", "tax_liability"}
    root = "Liabilities" if is_liability else "Assets"
    inst = _to_path_segment(institution) if institution else ""
    leaf = _to_path_segment(display_name) or _kind_default_leaf(kind)
    parts = [root, entity_slug]
    if inst:
        parts.append(inst)
    parts.append(leaf)
    if last_four and last_four.strip():
        digits = re.sub(r"\D", "", last_four.strip())[-4:]
        if digits:
            parts.append(f"X{digits}")
    return ":".join(parts)


def _kind_default_leaf(kind: str) -> str:
    return {
        "checking": "Checking",
        "savings": "Savings",
        "credit_card": "Card",
        "line_of_credit": "LineOfCredit",
        "loan": "Loan",
        "tax_liability": "TaxPayable",
        "brokerage": "Brokerage",
        "cash": "Cash",
    }.get(kind, "Account")


_PATH_SEG_STRIP = re.compile(r"[^A-Za-z0-9]+")


def _to_path_segment(value: str) -> str:
    """Convert a free-form bank/display name into a Beancount-legal segment."""
    cleaned = _PATH_SEG_STRIP.sub(" ", value or "").strip()
    if not cleaned:
        return ""
    parts = [w[0].upper() + w[1:] for w in cleaned.split() if w]
    seg = "".join(parts)
    if seg and not seg[0].isalpha():
        seg = "X" + seg
    return seg


def _list_intent_entities(state: WizardState) -> tuple[list[str], list[str]]:
    """Return (individuals_planned, businesses_planned) for the current intent.

    Used by the entities step on first render to seed defaults if the
    state hasn't been populated yet, and by the accounts step to know
    which entities are pickable.
    """
    return list(state.individuals_planned), list(state.businesses_planned)


def _account_paths_for_dropdowns(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT account_path FROM accounts_meta "
        "WHERE closed_on IS NULL ORDER BY account_path"
    ).fetchall()
    return [r["account_path"] for r in rows]


# ---------------------------------------------------------------------------
# Entry / dispatch
# ---------------------------------------------------------------------------


@router.get("/setup/wizard", response_class=HTMLResponse)
def wizard_entry(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Bounce the user to whichever step they're on (or welcome on first hit).

    Wizard already complete? Send them home — the wizard is one-shot.
    """
    if is_wizard_complete(conn):
        return RedirectResponse("/", status_code=303)
    state = load_state(conn)
    return _redirect_to(state.step)


# ---------------------------------------------------------------------------
# Step 1 — Welcome
# ---------------------------------------------------------------------------


def _has_progress(state: WizardState) -> bool:
    """True when the user has already advanced past the welcome step
    enough that switching intent would reset scaffolded entities."""
    return bool(state.individuals_planned or state.businesses_planned)


# Per-intent capacity for the destructive-change check. Values are
# the cap the wizard's UI enforces:
#   "personal"   : 1 individual, 0 businesses
#   "business"   : 0 individuals, unlimited businesses
#   "both"       : 1 individual, unlimited businesses
#   "household"  : unlimited individuals, 0 businesses
#   "everything" : unlimited individuals, unlimited businesses
# Used to decide whether a proposed intent change would actually
# discard planned entities. Mirrored client-side in welcome.html so
# the warning shows up only for the specific cases the user asked
# for ("only show in very specific cases").
INTENT_CAPS: dict[str, dict[str, int | None]] = {
    "personal":   {"individuals": 1, "businesses": 0},
    "business":   {"individuals": 0, "businesses": None},
    "both":       {"individuals": 1, "businesses": None},
    "household":  {"individuals": None, "businesses": 0},
    "everything": {"individuals": None, "businesses": None},
}


def _welcome_context(
    state: WizardState, conn: sqlite3.Connection,
) -> dict:
    """Build the context the welcome template + capacity check need.

    Both planned (wizard-draft) AND existing entities count toward
    capacity. If the user already has 2+ entities of any mix, we
    force the intent to ``everything`` and disable the radio: that
    intent has no capacity limit, so the wizard can safely add to
    their setup without ever needing to discard anything.
    """
    existing_indiv, existing_biz, account_count = _existing_entity_summary(
        conn, state,
    )
    existing_indiv_names = [e["display_name"] for e in existing_indiv]
    existing_biz_names = [e["display_name"] for e in existing_biz]
    planned_indiv_count = len(state.individuals_planned)
    planned_biz_count = len(state.businesses_planned)
    existing_total = len(existing_indiv) + len(existing_biz)
    intent_locked = existing_total >= 2
    return {
        "intent_options": INTENT_OPTIONS,
        "original_intent": state.intent or "",
        "has_progress": _has_progress(state),
        "indiv_count": planned_indiv_count,
        "biz_count": planned_biz_count,
        "indiv_names": list(state.individuals_planned),
        "biz_names": list(state.businesses_planned),
        "existing_indiv_count": len(existing_indiv),
        "existing_biz_count": len(existing_biz),
        "existing_indiv_names": existing_indiv_names,
        "existing_biz_names": existing_biz_names,
        "existing_account_count": account_count,
        "has_existing": bool(existing_indiv or existing_biz),
        "intent_locked": intent_locked,
        "intent_locked_value": "everything" if intent_locked else "",
    }


@router.get("/setup/wizard/welcome", response_class=HTMLResponse)
def wizard_welcome(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    if is_wizard_complete(conn):
        return RedirectResponse("/", status_code=303)
    state = load_state(conn)
    state.step = STEP_WELCOME
    save_state(conn, state)
    conn.commit()
    return _render(
        request, "wizard/welcome.html",
        _welcome_context(state, conn),
        state=state,
    )


@router.post("/setup/wizard/welcome", response_class=HTMLResponse)
def wizard_welcome_submit(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
    name: str = Form(""),
    intent: str = Form(""),
    confirm_intent_change: str | None = Form(default=None),
):
    if is_wizard_complete(conn):
        return RedirectResponse("/", status_code=303)
    state = load_state(conn)
    name = (name or "").strip()
    intent = (intent or "").strip()

    # If the user already has 2+ existing entities, the wizard locks
    # intent to "everything" — coerce the submission so a tampered
    # form (or a stale field) can't slip through with a different
    # intent that wouldn't fit their data.
    existing_indiv_full, existing_biz_full, _ = _existing_entity_summary(
        conn, state,
    )
    existing_total = len(existing_indiv_full) + len(existing_biz_full)
    if existing_total >= 2:
        intent = "everything"

    errors: dict[str, str] = {}
    if not name:
        errors["name"] = "Tell us what to call you."
    if intent not in VALID_INTENTS:
        errors["intent"] = "Pick one option."
    if errors:
        ctx = _welcome_context(state, conn)
        ctx.update({
            "errors": errors,
            "form_name": name,
            "form_intent": intent,
        })
        return _render(request, "wizard/welcome.html", ctx, state=state)

    # "Configure myself" is a hard exit to the maintenance checklist.
    if intent == "manual":
        state.name = name
        state.intent = intent
        save_state(conn, state)
        conn.commit()
        # Scaffold the empty ledger so the checklist's gate stops
        # bouncing them between /setup and /setup/wizard.
        _ensure_main_bean(settings)
        return RedirectResponse("/setup", status_code=303)

    # Detect intent change. We only warn (and require the confirm
    # checkbox) when the change is actually destructive — i.e., the
    # new intent's capacity can't hold the entities the user has
    # already planned. Cases the user explicitly listed:
    #   - both -> personal:    business gets deleted
    #   - both -> business:    personal gets deleted
    #   - everything -> business (with people planned)
    #   - everything -> personal (with businesses or 2+ people)
    #   - household with N>=2 people -> personal: extras deleted
    #   - household -> business: people deleted
    # Cases that are NOT destructive (no warning):
    #   - personal -> both:    just adds a slot for business
    #   - household with 1 person -> personal
    #   - any -> everything:   capacity grows
    intent_changed = (
        state.intent
        and state.intent != intent
        and _has_progress(state)
    )
    is_destructive = False
    if intent_changed:
        cap = INTENT_CAPS.get(intent)
        if cap is None:
            # "manual" or unknown: don't try to capacity-check, treat
            # as non-destructive (manual exits the wizard anyway and
            # is handled above this branch).
            is_destructive = False
        else:
            cap_indiv = cap["individuals"]
            cap_biz = cap["businesses"]
            indiv_overflow = (
                cap_indiv is not None
                and len(state.individuals_planned) > cap_indiv
            )
            biz_overflow = (
                cap_biz is not None
                and len(state.businesses_planned) > cap_biz
            )
            is_destructive = bool(indiv_overflow or biz_overflow)

    if is_destructive and not confirm_intent_change:
        ctx = _welcome_context(state, conn)
        ctx.update({
            "form_name": name,
            "form_intent": intent,
            "errors": {
                "intent_change": (
                    "Please confirm the reset by checking the box "
                    "before continuing."
                ),
            },
        })
        return _render(request, "wizard/welcome.html", ctx, state=state)

    if intent_changed:
        # Drop the draft entities that don't fit the new intent. With
        # the wizard in draft mode (no DB writes until Done), this is
        # purely a state edit — nothing to roll back in the books.
        # We trim drafts based on the new intent's capacity so the
        # entities step starts from a clean slate that matches the
        # user's new selection.
        cap = INTENT_CAPS.get(intent) or {"individuals": None, "businesses": None}
        cap_indiv = cap["individuals"]
        cap_biz = cap["businesses"]
        kept_indiv = [d for d in state.draft_entities if d["kind"] == "individual"]
        kept_biz = [d for d in state.draft_entities if d["kind"] == "business"]
        if cap_indiv is not None:
            kept_indiv = kept_indiv[:cap_indiv]
        if cap_biz is not None:
            kept_biz = kept_biz[:cap_biz]
        state.draft_entities = kept_indiv + kept_biz
        state.individuals_planned = [d["slug"] for d in kept_indiv]
        state.businesses_planned = [d["slug"] for d in kept_biz]

    state.name = name
    state.intent = intent
    state.step = STEP_ENTITIES

    # No auto-seeding: per user UX feedback, the entities step starts
    # empty and the user explicitly clicks "+ Add Person" /
    # "+ Add Business" to enter rows. Pre-rendered forms felt
    # forced. The capacity check uses existing real entities + drafts
    # both — so a user with existing entities can continue without
    # adding anything new, and a fresh user has to add at least one.

    save_state(conn, state)
    conn.commit()
    return _redirect_to(STEP_ENTITIES)


# NOTE: the previous version of this module had a
# `_rollback_planned_entities` helper that wrote Close directives and
# deleted entity rows when the user changed intent or removed a row.
# In draft mode there's no rollback to perform — entities only enter
# the entities table when the Done step's `_commit_entity_drafts`
# runs. State edits (intent change, row removal) just modify
# `state.draft_entities` in JSON.


def _existing_entity_summary(
    conn: sqlite3.Connection,
    state: WizardState,
) -> tuple[list[dict], list[dict], int]:
    """Return ``(existing_indiv, existing_biz, account_count)``.

    Every active row in the entities table counts as "existing" —
    the wizard runs in DRAFT MODE and never writes to the entities
    table until the Done step's commit, so anything currently
    present is pre-wizard real data we must show but never modify.

    The account_count is the number of non-closed accounts (any
    entity) — used by the welcome banner to give the user a sense
    of scope ("you already have N entities and M accounts").
    """
    existing_indiv: list[dict] = []
    existing_biz: list[dict] = []
    try:
        rows = conn.execute(
            "SELECT slug, display_name, entity_type, tax_schedule "
            "FROM entities WHERE is_active = 1 ORDER BY slug"
        ).fetchall()
    except Exception:  # noqa: BLE001
        return [], [], 0
    for r in rows:
        et = (r["entity_type"] or "").lower()
        ts = (r["tax_schedule"] or "").strip()
        item = {
            "slug": r["slug"],
            "display_name": r["display_name"] or r["slug"],
            "entity_type": r["entity_type"] or "",
            "tax_schedule": r["tax_schedule"] or "",
        }
        # Bucket as business only when there are explicit business
        # markers — a known business entity_type or a filed tax
        # schedule. Anything else (entity_type='personal', or both
        # NULL because the row was seeded from ledger discovery
        # before the user filled metadata in) defaults to
        # individual. Defaulting to business here used to land
        # Schedule-C-shaped read-only chips on rows the user never
        # claimed were businesses, which is wrong on tax-filing
        # grounds, not just visual.
        is_business = et in VALID_BIZ_ENTITY_TYPE_VALUES or ts != ""
        if is_business:
            existing_biz.append(item)
        else:
            existing_indiv.append(item)
    try:
        acct_row = conn.execute(
            "SELECT COUNT(*) AS n FROM accounts_meta WHERE closed_on IS NULL"
        ).fetchone()
        account_count = int(acct_row["n"] if acct_row else 0)
    except Exception:  # noqa: BLE001
        account_count = 0
    return existing_indiv, existing_biz, account_count


# ---------------------------------------------------------------------------
# Step 2 — Entity scaffold review
# ---------------------------------------------------------------------------


VALID_BIZ_ENTITY_TYPES = (
    ("sole_proprietorship", "Sole proprietorship",
     "One owner, no formal entity. Income flows to your personal 1040."),
    ("llc", "LLC (single- or multi-member)",
     "Legal liability shield. Tax treatment depends on how it was elected."),
    ("partnership", "Partnership",
     "Two or more owners, files Form 1065."),
    ("s_corp", "S-Corp",
     "Files Form 1120-S, distributes income via K-1s."),
    ("c_corp", "C-Corp",
     "Files Form 1120, taxed separately from owners."),
)
VALID_BIZ_ENTITY_TYPE_VALUES = frozenset(t[0] for t in VALID_BIZ_ENTITY_TYPES)

BIZ_TAX_SCHEDULES = (
    ("C", "Schedule C", "Most common — services, retail, consulting, products."),
    ("F", "Schedule F", "Farm income."),
)
VALID_BIZ_TAX_SCHEDULES = frozenset(s[0] for s in BIZ_TAX_SCHEDULES)


def _drafts_split(state: WizardState) -> tuple[list[dict], list[dict]]:
    """Return (individuals, businesses) drafted in this session."""
    indiv: list[dict] = []
    biz: list[dict] = []
    for d in state.draft_entities:
        if d.get("kind") == "individual":
            indiv.append(d)
        else:
            biz.append(d)
    return indiv, biz


def _entities_view_context(
    *,
    conn: sqlite3.Connection,
    state: WizardState,
    open_modal: str = "",
    edit_slug: str = "",
    field_errors: dict | None = None,
    form_values: dict | None = None,
) -> dict:
    """Common context for both the GET render and the POST error
    re-render of the entities list view."""
    existing_indiv, existing_biz, _ = _existing_entity_summary(conn, state)
    indiv_drafts, biz_drafts = _drafts_split(state)
    intent = state.intent

    # Capacity rules: how many of each kind can the current intent
    # accept (counting existing + drafted)? Drives whether the
    # "+ Add Person" / "+ Add Business" buttons are visible.
    cap = INTENT_CAPS.get(intent) or {"individuals": None, "businesses": None}
    indiv_total = len(existing_indiv) + len(indiv_drafts)
    biz_total = len(existing_biz) + len(biz_drafts)
    can_add_indiv = (
        intent in {"personal", "both", "household", "everything"}
        and (cap["individuals"] is None or indiv_total < cap["individuals"])
    )
    can_add_biz = (
        intent in {"business", "both", "everything"}
        and (cap["businesses"] is None or biz_total < cap["businesses"])
    )

    # Pre-fill the modal form when editing.
    edit_target: dict | None = None
    if edit_slug:
        for d in state.draft_entities:
            if d.get("slug") == edit_slug:
                edit_target = d
                break

    return {
        "existing_indiv": existing_indiv,
        "existing_biz": existing_biz,
        "indiv_drafts": indiv_drafts,
        "biz_drafts": biz_drafts,
        "indiv_total": indiv_total,
        "biz_total": biz_total,
        "can_add_indiv": can_add_indiv,
        "can_add_biz": can_add_biz,
        "biz_entity_types": VALID_BIZ_ENTITY_TYPES,
        "biz_tax_schedules": BIZ_TAX_SCHEDULES,
        "open_modal": open_modal,           # "" | "person" | "business"
        "edit_slug": edit_slug,
        "edit_target": edit_target,
        "field_errors": field_errors or {},
        "form_values": form_values or {},
        "name": state.name,
    }


@router.get("/setup/wizard/entities", response_class=HTMLResponse)
def wizard_entities(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
    add: str | None = None,
    edit: str | None = None,
):
    if is_wizard_complete(conn):
        return RedirectResponse("/", status_code=303)
    state = load_state(conn)
    if not state.intent:
        return _redirect_to(STEP_WELCOME)
    state.step = STEP_ENTITIES
    save_state(conn, state)
    conn.commit()

    open_modal = ""
    edit_slug = ""
    if add in {"person", "business"}:
        open_modal = add
    elif edit:
        edit_slug = edit
        # Look up the draft to pick which modal to open.
        for d in state.draft_entities:
            if d.get("slug") == edit:
                open_modal = (
                    "person" if d.get("kind") == "individual" else "business"
                )
                break

    return _render(
        request, "wizard/entities.html",
        _entities_view_context(
            conn=conn, state=state,
            open_modal=open_modal, edit_slug=edit_slug,
        ),
        state=state,
    )


_INVALID_SLUG_MSG = (
    "Slugs must start with an uppercase letter (A–Z) and "
    "contain only letters, digits, and hyphens."
)


def _validate_draft_slug(
    *,
    chosen: str,
    display_name: str,
    fallback_seed: str,
    state: WizardState,
    existing_real_slugs: set[str],
    skip_slug: str = "",
) -> tuple[str, str | None]:
    """Resolve and validate a slug for a draft entity.

    Returns ``(slug, error_message)``. The slug is only valid if
    error_message is None. ``skip_slug`` is the current draft's
    own slug when editing (so a no-op edit doesn't trip the
    "already taken in drafts" check against itself).
    """
    typed = (chosen or "").strip()
    suggested = _slug_from_name(display_name, fallback=fallback_seed)
    s = typed if typed else suggested
    if not s:
        return "", "Required."
    if not is_valid_slug(s):
        return s, _INVALID_SLUG_MSG
    if s in existing_real_slugs:
        return s, (
            f'Slug "{s}" is already used by an existing entity in your '
            "books. Pick another."
        )
    for d in state.draft_entities:
        if d.get("slug") == s and d.get("slug") != skip_slug:
            return s, f'Slug "{s}" is already used by another draft.'
    return s, None


def _resync_planned_caches(state: WizardState) -> None:
    """Rebuild the convenience caches over draft_entities. Called
    after every mutation so the welcome step's capacity check and
    other readers see consistent counts."""
    state.individuals_planned = [
        d["slug"] for d in state.draft_entities if d.get("kind") == "individual"
    ]
    state.businesses_planned = [
        d["slug"] for d in state.draft_entities if d.get("kind") == "business"
    ]


@router.post("/setup/wizard/entities/save-person")
async def wizard_entities_save_person(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Add or update a person draft. Form fields:
       - display_name (required)
       - slug (optional; auto-suggested from display_name if blank)
       - edit_slug (when editing an existing draft)
    Redirects back to the entities list. On validation failure,
    re-renders with the modal still open + inline errors."""
    if is_wizard_complete(conn):
        return RedirectResponse("/", status_code=303)
    state = load_state(conn)
    if not state.intent:
        return _redirect_to(STEP_WELCOME)
    form = await request.form()
    display_name = (form.get("display_name") or "").strip()
    slug_typed = (form.get("slug") or "").strip()
    edit_slug = (form.get("edit_slug") or "").strip()

    field_errors: dict[str, str] = {}
    if not display_name:
        field_errors["display_name"] = "Required."

    existing_indiv, existing_biz, _ = _existing_entity_summary(conn, state)
    existing_real = {e["slug"] for e in existing_indiv + existing_biz}
    slug, slug_err = _validate_draft_slug(
        chosen=slug_typed,
        display_name=display_name,
        fallback_seed="Personal",
        state=state,
        existing_real_slugs=existing_real,
        skip_slug=edit_slug,
    )
    if slug_err and "slug" not in field_errors:
        field_errors["slug"] = slug_err

    if field_errors:
        ctx = _entities_view_context(
            conn=conn, state=state,
            open_modal="person", edit_slug=edit_slug,
            field_errors=field_errors,
            form_values={"display_name": display_name, "slug": slug_typed},
        )
        return _render(request, "wizard/entities.html", ctx, state=state)

    # Mutate state.draft_entities — replace if editing, else append.
    new_draft = {
        "slug": slug,
        "display_name": display_name,
        "kind": "individual",
        "entity_type": "personal",
        "tax_schedule": "A",
    }
    if edit_slug:
        state.draft_entities = [
            new_draft if d.get("slug") == edit_slug else d
            for d in state.draft_entities
        ]
    else:
        state.draft_entities.append(new_draft)
    _resync_planned_caches(state)
    save_state(conn, state)
    conn.commit()
    return RedirectResponse("/setup/wizard/entities", status_code=303)


@router.post("/setup/wizard/entities/save-business")
async def wizard_entities_save_business(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Add or update a business draft."""
    if is_wizard_complete(conn):
        return RedirectResponse("/", status_code=303)
    state = load_state(conn)
    if not state.intent:
        return _redirect_to(STEP_WELCOME)
    form = await request.form()
    display_name = (form.get("display_name") or "").strip()
    slug_typed = (form.get("slug") or "").strip()
    entity_type = (form.get("entity_type") or "").strip()
    tax_schedule = (form.get("tax_schedule") or "").strip()
    edit_slug = (form.get("edit_slug") or "").strip()

    field_errors: dict[str, str] = {}
    if not display_name:
        field_errors["display_name"] = "Required."
    if entity_type not in VALID_BIZ_ENTITY_TYPE_VALUES:
        field_errors["entity_type"] = "Pick a business type."
    if tax_schedule not in VALID_BIZ_TAX_SCHEDULES:
        field_errors["tax_schedule"] = "Pick a tax schedule."

    existing_indiv, existing_biz, _ = _existing_entity_summary(conn, state)
    existing_real = {e["slug"] for e in existing_indiv + existing_biz}
    slug, slug_err = _validate_draft_slug(
        chosen=slug_typed,
        display_name=display_name,
        fallback_seed="Business",
        state=state,
        existing_real_slugs=existing_real,
        skip_slug=edit_slug,
    )
    if slug_err and "slug" not in field_errors:
        field_errors["slug"] = slug_err

    if field_errors:
        ctx = _entities_view_context(
            conn=conn, state=state,
            open_modal="business", edit_slug=edit_slug,
            field_errors=field_errors,
            form_values={
                "display_name": display_name,
                "slug": slug_typed,
                "entity_type": entity_type or "sole_proprietorship",
                "tax_schedule": tax_schedule or "C",
            },
        )
        return _render(request, "wizard/entities.html", ctx, state=state)

    new_draft = {
        "slug": slug,
        "display_name": display_name,
        "kind": "business",
        "entity_type": entity_type,
        "tax_schedule": tax_schedule,
    }
    if edit_slug:
        state.draft_entities = [
            new_draft if d.get("slug") == edit_slug else d
            for d in state.draft_entities
        ]
    else:
        state.draft_entities.append(new_draft)
    _resync_planned_caches(state)
    save_state(conn, state)
    conn.commit()
    return RedirectResponse("/setup/wizard/entities", status_code=303)


@router.post("/setup/wizard/entities/remove")
async def wizard_entities_remove(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Remove a draft entity from the list. Only touches drafts —
    existing entities in the books are off-limits to the wizard."""
    if is_wizard_complete(conn):
        return RedirectResponse("/", status_code=303)
    state = load_state(conn)
    form = await request.form()
    slug = (form.get("slug") or "").strip()
    if slug:
        state.draft_entities = [
            d for d in state.draft_entities if d.get("slug") != slug
        ]
        _resync_planned_caches(state)
        save_state(conn, state)
        conn.commit()
    return RedirectResponse("/setup/wizard/entities", status_code=303)


@router.post("/setup/wizard/entities", response_class=HTMLResponse)
async def wizard_entities_continue(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Advance to the next step.

    Validation rule from user feedback: continue is allowed when
    there's at least 1 entity total — counting BOTH existing real
    entities AND drafts. So a user with an existing setup can
    advance without adding anything new; a fresh user needs to
    have added at least one draft.
    """
    if is_wizard_complete(conn):
        return RedirectResponse("/", status_code=303)
    state = load_state(conn)
    if not state.intent:
        return _redirect_to(STEP_WELCOME)

    existing_indiv, existing_biz, _ = _existing_entity_summary(conn, state)
    total_entities = (
        len(existing_indiv) + len(existing_biz) + len(state.draft_entities)
    )
    if total_entities == 0:
        ctx = _entities_view_context(conn=conn, state=state)
        ctx["errors"] = [
            "Add at least one person or business before continuing."
        ]
        return _render(request, "wizard/entities.html", ctx, state=state)

    state.step = STEP_BANK
    save_state(conn, state)
    conn.commit()
    return _redirect_to(STEP_BANK)


def _commit_entity_drafts(
    *,
    conn: sqlite3.Connection,
    settings: Settings,
    state: WizardState,
) -> None:
    """Materialize the wizard's draft entities into real DB rows +
    ledger directives + scaffolded category trees.

    Called from the Done step's POST. Idempotent: a draft whose slug
    already exists in the entities table is upserted (existing
    user-typed display_name / entity_type / tax_schedule are
    preserved unless the wizard explicitly set them this run).

    Wizard never overwrites entities the user already had — slug
    collisions are blocked at validation time, so any slug that
    reaches this function is either brand-new or was created earlier
    in this same wizard session and is being re-applied.
    """
    if not state.draft_entities:
        return
    _ensure_main_bean(settings)
    for d in state.draft_entities:
        slug = d.get("slug")
        if not slug:
            continue
        try:
            _scaffold_entity(
                conn=conn, settings=settings,
                slug=slug,
                display_name=d.get("display_name") or slug,
                entity_type=d.get("entity_type") or "personal",
                tax_schedule=d.get("tax_schedule") or None,
            )
            _scaffold_categories(conn=conn, settings=settings, entity_slug=slug)
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "commit: entity draft %s failed (continuing): %s",
                slug, exc,
            )


def _disambiguate_slug(
    slug: str, seen: set[str], conn: sqlite3.Connection,
) -> str:
    """Append -2, -3 to a slug if it's already taken in this batch or DB.

    Slugs from the DB use no hyphen, so we go PascalCase numeric: Foo,
    Foo2, Foo3, etc. (matches `service.suggest_slug`'s style.)
    """
    if not slug:
        slug = "Entity"
    base = slug
    n = 2
    while slug in seen or _entity_exists(conn, slug):
        slug = f"{base}{n}"
        n += 1
        if n > 99:
            return f"{base}{datetime.now(UTC).timestamp():.0f}"
    return slug


def _entity_exists(conn: sqlite3.Connection, slug: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM entities WHERE slug = ?", (slug,),
    ).fetchone()
    return row is not None


# ---------------------------------------------------------------------------
# Step 3 — SimpleFIN connect (optional)
# ---------------------------------------------------------------------------


@router.get("/setup/wizard/bank", response_class=HTMLResponse)
def wizard_bank(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    if is_wizard_complete(conn):
        return RedirectResponse("/", status_code=303)
    state = load_state(conn)
    if not state.intent:
        return _redirect_to(STEP_WELCOME)
    state.step = STEP_BANK
    save_state(conn, state)
    conn.commit()

    access_configured = bool(
        settings.simplefin_access_url
        and settings.simplefin_access_url.get_secret_value()
    )
    return _render(
        request, "wizard/bank.html",
        {
            "access_configured": access_configured,
            "connect_error": None,
            "form_token": "",
        },
        state=state,
    )


@router.post("/setup/wizard/bank/skip")
def wizard_bank_skip(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
):
    if is_wizard_complete(conn):
        return RedirectResponse("/", status_code=303)
    state = load_state(conn)
    state.simplefin_connected = False
    state.step = STEP_ACCOUNTS
    save_state(conn, state)
    conn.commit()
    return _redirect_to(STEP_ACCOUNTS)


@router.post("/setup/wizard/bank/connected")
async def wizard_bank_connected(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Continue handler for the bank step.

    Marks the wizard as SimpleFIN-connected and ALWAYS re-fetches
    accounts from the bridge so any new accounts the user added in
    SimpleFIN since the last visit show up on step 4. The
    simplefin_discovered_accounts table is upserted (the bridge's
    account IDs are stable), so existing rows just get refreshed
    balances; brand-new accounts get appended.

    Drafts the user previously dismissed via the "Remove" button on
    step 4 are tracked in state.simplefin_dismissed_ids and stay
    dismissed across re-fetches — those don't resurrect.

    A bridge fetch failure here is non-fatal; we log it and advance.
    The accounts step will render whatever's already in the
    discovered table.
    """
    if is_wizard_complete(conn):
        return RedirectResponse("/", status_code=303)
    state = load_state(conn)
    state.simplefin_connected = True

    try:
        access_url = (
            settings.simplefin_access_url.get_secret_value()
            if settings.simplefin_access_url else ""
        )
    except Exception:  # noqa: BLE001
        access_url = ""
    if access_url:
        try:
            from lamella.adapters.simplefin.client import SimpleFINClient
            async with SimpleFINClient(access_url=access_url) as client:
                response = await client.fetch_accounts(
                    lookback_days=90, include_pending=False,
                )
            from lamella.web.routes.simplefin import (
                _upsert_discovered_accounts,
            )
            _upsert_discovered_accounts(conn, response)
        except Exception as exc:  # noqa: BLE001
            log.info(
                "wizard /bank/connected: bridge fetch skipped: %s", exc,
            )

    state.step = STEP_ACCOUNTS
    save_state(conn, state)
    conn.commit()
    return _redirect_to(STEP_ACCOUNTS)


@router.post("/setup/wizard/bank/connect")
async def wizard_bank_connect(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
    simplefin_token: str = Form(""),
):
    """Wizard-owned SimpleFIN connect.

    Handles the full claim → fetch → record → advance flow inside
    the wizard so the user never bounces to the legacy /simplefin
    settings page mid-onboarding. Steps:

      1. Validate the input is non-empty.
      2. If it looks like an access URL, store it directly. Else
         claim the setup token (POST to bridge → returns the access
         URL). Network/auth errors render an inline error and keep
         the user on /setup/wizard/bank.
      3. Persist the access URL via AppSettingsStore.
      4. Connect to the bridge and fetch the discovered accounts.
         An empty list is fine (some users have a bridge with no
         linked accounts yet) — we still mark the connection as
         successful and let step 4's manual-add path show.
      5. Upsert into simplefin_discovered_accounts so step 4 can
         render the table without re-hitting the network.
      6. Mark state.simplefin_connected and advance to step 4.
    """
    if is_wizard_complete(conn):
        return RedirectResponse("/", status_code=303)
    state = load_state(conn)

    raw = (simplefin_token or "").strip()
    if not raw:
        return _render(
            request, "wizard/bank.html",
            {
                "access_configured": False,
                "connect_error": "Paste a setup token from SimpleFIN Bridge.",
                "form_token": "",
            },
            state=state,
        )

    # Step 2: claim or pass through.
    from lamella.adapters.simplefin.client import (
        SimpleFINAuthError, SimpleFINError, SimpleFINClient,
        _looks_like_access_url, claim_setup_token,
    )
    try:
        if _looks_like_access_url(raw):
            access_url = raw
        else:
            access_url = claim_setup_token(raw)
    except (SimpleFINAuthError, SimpleFINError) as exc:
        return _render(
            request, "wizard/bank.html",
            {
                "access_configured": False,
                "connect_error": (
                    f"Couldn't claim that token: {exc}. "
                    "Double-check you copied the whole URL from SimpleFIN Bridge "
                    "and that it hasn't been used already."
                ),
                "form_token": raw,
            },
            state=state,
        )

    # Step 3: persist the access URL via the standard settings store
    # (dual-write to SQLite + ledger so reconstruct rebuilds it).
    try:
        from lamella.core.settings.store import AppSettingsStore
        store = AppSettingsStore(
            conn,
            connector_config_path=settings.connector_config_path,
            main_bean_path=settings.ledger_main if settings.ledger_main.exists() else None,
        )
        store.set("simplefin_access_url", access_url)
        # Apply the override to the in-memory Settings so this
        # request's downstream code sees the new value.
        settings.apply_kv_overrides({"simplefin_access_url": access_url})
    except Exception as exc:  # noqa: BLE001
        log.warning("settings store write for simplefin_access_url: %s", exc)

    # Step 4 + 5: fetch + persist discovered accounts.
    fetch_error: str | None = None
    discovered_count = 0
    try:
        async with SimpleFINClient(access_url=access_url) as client:
            response = await client.fetch_accounts(
                lookback_days=90, include_pending=False,
            )
        from lamella.web.routes.simplefin import _upsert_discovered_accounts
        discovered_count = _upsert_discovered_accounts(conn, response)
    except (SimpleFINAuthError, SimpleFINError) as exc:
        fetch_error = str(exc)
    except Exception as exc:  # noqa: BLE001
        fetch_error = f"unexpected error: {exc}"
        log.warning("wizard simplefin fetch: %s", exc, exc_info=True)

    if fetch_error:
        # Token claimed but bridge fetch failed — let the user retry
        # or skip without losing the access URL we just saved.
        return _render(
            request, "wizard/bank.html",
            {
                "access_configured": True,
                "connect_error": (
                    f"Connected to SimpleFIN, but couldn't fetch accounts: "
                    f"{fetch_error}. You can try again or skip and add accounts "
                    "manually below."
                ),
                "form_token": "",
            },
            state=state,
        )

    # Step 6: success — advance to accounts step.
    state.simplefin_connected = True
    state.step = STEP_ACCOUNTS
    save_state(conn, state)
    conn.commit()
    log.info(
        "wizard: SimpleFIN connected; %d account(s) discovered",
        discovered_count,
    )
    return _redirect_to(STEP_ACCOUNTS)


# ---------------------------------------------------------------------------
# Step 4 — Account setup (manual or SimpleFIN-mapped)
# ---------------------------------------------------------------------------


def _existing_accounts_summary(
    conn: sqlite3.Connection,
) -> list[dict]:
    """Read the user's existing accounts (from accounts_meta) for the
    read-only "already in your books" section. Returns an ordered
    list of dicts with display fields the template can render. The
    caller is responsible for adding a formatted ``label`` —
    _accounts_view_context does it via ``_format_account_display``."""
    try:
        rows = conn.execute(
            """
            SELECT account_path, kind, entity_slug, institution,
                   last_four, display_name, simplefin_account_id
              FROM accounts_meta
             WHERE closed_on IS NULL
             ORDER BY entity_slug, account_path
            """
        ).fetchall()
    except Exception:  # noqa: BLE001
        return []
    out = []
    for r in rows:
        last_four = r["last_four"] or ""
        out.append({
            "account_path": r["account_path"],
            "kind": r["kind"] or "",
            "entity_slug": r["entity_slug"] or "",
            "institution": r["institution"] or "",
            "last_four": last_four,
            "display_name": r["display_name"] or r["account_path"],
            "simplefin_account_id": r["simplefin_account_id"] or "",
        })
    return out


def _seed_account_drafts_from_simplefin(
    conn: sqlite3.Connection, state: WizardState,
) -> int:
    """Pull state.draft_accounts up to date with simplefin_discovered_accounts.

    Runs every time the accounts step is rendered (not gated). For
    each discovered SimpleFIN account NOT already mapped to a real
    account AND NOT already in drafts AND NOT explicitly dismissed
    by the user, append a new draft.

    Returns the number of new drafts appended. Caller is responsible
    for save_state.

    The dismissed-IDs list is the key piece: when the user clicks
    Remove on a SimpleFIN-seeded draft, we record the
    simplefin_account_id so the next refetch doesn't bring it back.
    A user who actually wanted that account back can re-add it
    manually via "+ Add Account".
    """
    state.simplefin_seeded = True
    try:
        discovered = list(conn.execute(
            "SELECT account_id, name, org_name, currency, balance "
            "FROM simplefin_discovered_accounts ORDER BY org_name, name"
        ).fetchall())
    except sqlite3.OperationalError:
        return 0
    if not discovered:
        return 0

    try:
        already_mapped = {
            r["simplefin_account_id"]
            for r in conn.execute(
                "SELECT simplefin_account_id FROM accounts_meta "
                "WHERE simplefin_account_id IS NOT NULL "
                "AND simplefin_account_id != ''"
            ).fetchall()
        }
    except Exception:  # noqa: BLE001
        already_mapped = set()

    in_drafts = {
        d.get("simplefin_account_id")
        for d in state.draft_accounts if d.get("simplefin_account_id")
    }
    dismissed = set(state.simplefin_dismissed_ids)

    # Count total available entities. With multi-entity setups,
    # defaulting every SimpleFIN account to one entity is wrong —
    # the user with personal + business + spouse can't have the
    # wizard silently file all their cards under "Personal". Force
    # them to pick by leaving entity_slug empty when there's more
    # than one entity to choose from. With exactly one entity, it's
    # safe to default — there's no other valid option.
    available_entities: list[str] = []
    for d in state.draft_entities:
        if d.get("slug"):
            available_entities.append(d["slug"])
    try:
        for r in conn.execute(
            "SELECT slug FROM entities WHERE is_active = 1 ORDER BY slug"
        ).fetchall():
            if r["slug"] not in available_entities:
                available_entities.append(r["slug"])
    except Exception:  # noqa: BLE001
        pass
    default_entity = (
        available_entities[0] if len(available_entities) == 1 else ""
    )

    added = 0
    for sf in discovered:
        sf_id = sf["account_id"]
        if sf_id in already_mapped or sf_id in in_drafts or sf_id in dismissed:
            continue
        # Pull the last 4 out of the SimpleFIN name when it's
        # parenthesized at the end. Strips an embedded duplicate
        # of the same digits too — see _parse_simplefin_name.
        clean_name, last_four = _parse_simplefin_name(sf["name"] or "")
        state.draft_accounts.append({
            "account_path": "",
            "kind": _guess_kind_from_name(sf["name"] or ""),
            "entity_slug": default_entity,
            "institution": sf["org_name"] or "",
            "last_four": last_four,
            "display_name": clean_name,
            "simplefin_account_id": sf_id,
            "loan_kind": "",
            "opening_balance": "",
            "from_simplefin": "1",
        })
        added += 1
    return added


# Keyword → kind mapping. Order matters — multi-word matches must
# come BEFORE single-word matches that they'd false-positive on.
# "LINE OF CREDIT" must beat "CREDIT" → credit_card; "MORTGAGE" must
# beat the catch-all "loan".
_KIND_KEYWORDS = (
    ("line_of_credit", "line of credit"),
    ("line_of_credit", "businessline"),
    ("loan",     "mortgage"),
    ("loan",     "loan"),
    ("brokerage", "brokerage"),
    ("brokerage", "investment"),
    ("brokerage", "ira"),
    ("brokerage", "401k"),
    ("checking", "checking"),
    ("checking", " chk"),
    ("savings",  "savings"),
    ("savings",  " save"),
    ("credit_card", "credit card"),
    ("credit_card", "credit"),
    ("credit_card", "visa"),
    ("credit_card", "mastercard"),
    ("credit_card", " card"),
    ("cash",     "cash"),
)


def _guess_kind_from_name(name: str) -> str:
    """Heuristic: pick a kind from the SimpleFIN account name. Returns
    "" when nothing matches — the user must then set it via Edit."""
    n = " " + (name or "").lower() + " "
    for kind, keyword in _KIND_KEYWORDS:
        if keyword in n:
            return kind
    return ""


# Trailing "(####)" — what SimpleFIN typically appends when the bridge
# knows the last 4 of the account number. Captured group is the digits.
_TRAILING_LAST4_RE = re.compile(r"\s*\((\d{4})\)\s*$")


# Characters that show up in SimpleFIN account names but add no value
# in the UI. Two categories:
#
#   1. Trademark / registered / copyright / service-mark glyphs that
#      banks like to brand their cards with — ®™©℠. Pure decoration.
#
#   2. Encoding garbage — U+FFFD (the replacement character, "�")
#      that appears when the bridge or upstream system mangled
#      a byte sequence; zero-width chars (U+200B-200D, U+2060,
#      U+FEFF) and ordinary control characters (0x00-0x1F, 0x7F-
#      0x9F) that don't render and just show as boxes or
#      double-replacement glyphs ("��" in the user's example
#      "VISA SIGNATURE�� CARD ...5500").
#
# After sanitization we collapse whitespace so removing a glyph
# doesn't leave a double space.
_SIMPLEFIN_STRIP_RE = re.compile(
    "["
    "\u00AE\u2122\u00A9\u2120"      # \u00AE registered, \u2122 trademark,
                                        # \u00A9 copyright, \u2120 service-mark
    "\uFFFD"                           # \uFFFD replacement-char (the box-glyph)
    "\u200B-\u200D"                   # zero-width space / non-joiner / joiner
    "\u2060\uFEFF"                    # word-joiner + BOM
    "\u0000-\u001F\u007F-\u009F"    # ASCII control + extended control
    "]"
)


def _sanitize_simplefin_name(name: str) -> str:
    """Strip decorative + non-rendering characters from a SimpleFIN
    account name before parsing. See _SIMPLEFIN_STRIP_RE for the
    full set."""
    if not name:
        return ""
    cleaned = _SIMPLEFIN_STRIP_RE.sub("", name)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _parse_simplefin_name(name: str) -> tuple[str, str]:
    """Parse a SimpleFIN account name into ``(clean_name, last_four)``.

    SimpleFIN frequently returns names with the last 4 digits
    appended in parens, sometimes also embedded inline. Examples:

        "Costco Anywhere Visa® Card by Citi-4959 (4959)"
            → ("Costco Anywhere Visa® Card by Citi", "4959")
        "Bank One Checking (1234)"
            → ("Bank One Checking", "1234")
        "Bank One Checking"
            → ("Bank One Checking", "")

    Logic: if a trailing ``(####)`` is present, strip it as
    ``last_four``. Then sweep the remaining name for a standalone
    occurrence of those same digits (the user reported the bridge
    sometimes inlines the digits twice — once before the parens
    too) and remove that, plus any adjacent separator characters
    like dashes or slashes that get orphaned by the removal.

    Names without a trailing ``(####)`` are returned unchanged with
    an empty ``last_four``. The wizard's modal form lets the user
    set last_four manually for those.
    """
    if not name:
        return "", ""
    name = _sanitize_simplefin_name(name)
    if not name:
        return "", ""
    m = _TRAILING_LAST4_RE.search(name)
    if not m:
        return name.strip(), ""
    last_four = m.group(1)
    cleaned = name[: m.start()].strip()
    # Remove an embedded duplicate of the same 4 digits along with
    # adjacent separator characters. Word boundaries keep us from
    # matching part of a longer number (e.g. transaction IDs).
    dup_re = re.compile(
        r"[\s\-_/.]*\b" + re.escape(last_four) + r"\b[\s\-_/.]*"
    )
    cleaned = dup_re.sub(" ", cleaned)
    # Collapse whitespace and strip trailing/leading punctuation we
    # may have left orphaned (e.g. "Citi-" → "Citi").
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    cleaned = cleaned.strip("-_/.,;:").strip()
    return cleaned, last_four


def _format_account_display(display_name: str, last_four: str) -> str:
    """Render a human-readable account label.

    If ``last_four`` is set and isn't already present in
    ``display_name``, append ``(####)`` so list rows show e.g.
    "Bank One Checking (1234)". Idempotent — calling twice on
    the same input doesn't double-append.
    """
    label = (display_name or "").strip()
    lf = (last_four or "").strip()
    if not lf:
        return label
    if not label:
        return f"({lf})"
    if f"({lf})" in label:
        return label
    return f"{label} ({lf})"


def _account_draft_is_complete(d: dict) -> bool:
    """A draft is complete (ready to commit) when display name,
    entity, and kind are all set — plus loan_kind when kind=loan.

    Pre-populated SimpleFIN drafts often have a guessed kind but
    miss entity (multi-entity installs default to "" so the user
    must pick) and miss loan_kind (the bridge doesn't tell us
    Mortgage vs. Auto vs. Personal — the user has to confirm).
    The wizard's Continue validator refuses to advance until every
    draft passes this check.
    """
    if not (
        d.get("display_name") and d.get("entity_slug") and d.get("kind")
    ):
        return False
    if d.get("kind") == "loan" and not d.get("loan_kind"):
        return False
    return True


def _account_draft_missing_fields(d: dict) -> list[str]:
    """List of human-readable field names a draft is still missing.
    Used by the accounts list view to show the user *why* a row
    needs attention, not just a generic "needs setup" badge."""
    missing: list[str] = []
    if not d.get("display_name"):
        missing.append("name")
    if not d.get("kind"):
        missing.append("type")
    if not d.get("entity_slug"):
        missing.append("entity")
    if d.get("kind") == "loan" and not d.get("loan_kind"):
        missing.append("loan kind")
    return missing


def _accounts_view_context(
    *,
    conn: sqlite3.Connection,
    state: WizardState,
    open_modal: bool = False,
    edit_index: int | None = None,
    field_errors: dict | None = None,
    form_values: dict | None = None,
    error: str | None = None,
) -> dict:
    """Common context for both the accounts list view + the modal
    when add/edit is open. Existing + draft accounts both carry a
    pre-formatted display label that includes ``(####)`` when the
    last 4 is known but isn't already in the name."""
    existing_raw = _existing_accounts_summary(conn)
    existing = []
    for a in existing_raw:
        existing.append({
            **a,
            "label": _format_account_display(
                a.get("display_name", ""), a.get("last_four", ""),
            ),
        })
    draft_view = []
    for d in state.draft_accounts:
        missing = _account_draft_missing_fields(d)
        draft_view.append({
            **d,
            "label": _format_account_display(
                d.get("display_name", ""), d.get("last_four", ""),
            ),
            "missing_fields": missing,
            "is_complete": not missing,
        })
    edit_target: dict | None = None
    if edit_index is not None and 0 <= edit_index < len(state.draft_accounts):
        edit_target = state.draft_accounts[edit_index]
    incomplete_count = sum(
        1 for d in state.draft_accounts if not _account_draft_is_complete(d)
    )
    return {
        "existing_accounts": existing,
        "draft_accounts": draft_view,
        "incomplete_count": incomplete_count,
        "entity_options": _entity_options(conn, state),
        "kind_options": ACCOUNT_KIND_OPTIONS,
        "loan_kind_options": LOAN_KIND_OPTIONS,
        "open_modal": open_modal,
        "edit_index": edit_index,
        "edit_target": edit_target,
        "field_errors": field_errors or {},
        "form_values": form_values or {},
        "error": error,
        "simplefin_connected": state.simplefin_connected,
    }


@router.get("/setup/wizard/accounts", response_class=HTMLResponse)
def wizard_accounts(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
    add: str | None = None,
    edit: int | None = None,
):
    if is_wizard_complete(conn):
        return RedirectResponse("/", status_code=303)
    state = load_state(conn)
    if not state.intent:
        return _redirect_to(STEP_WELCOME)
    state.step = STEP_ACCOUNTS

    # Re-seed drafts from SimpleFIN every visit. The seeding logic
    # is idempotent: it only adds accounts that are NOT already in
    # drafts AND NOT already mapped to existing accounts AND NOT
    # explicitly dismissed by the user. So if the user added new
    # accounts in SimpleFIN since their last wizard visit, they
    # show up here automatically.
    if state.simplefin_connected:
        _seed_account_drafts_from_simplefin(conn, state)

    save_state(conn, state)
    conn.commit()

    open_modal = False
    edit_index: int | None = None
    if add == "1":
        open_modal = True
    elif edit is not None:
        try:
            edit_index = int(edit)
            open_modal = True
        except (TypeError, ValueError):
            edit_index = None

    return _render(
        request, "wizard/accounts.html",
        _accounts_view_context(
            conn=conn, state=state,
            open_modal=open_modal, edit_index=edit_index,
        ),
        state=state,
    )


def _entity_options(conn: sqlite3.Connection, state: WizardState) -> list[dict]:
    """Entity dropdown list for the accounts + property/vehicle steps.

    Pulls from BOTH wizard drafts (entities the user is about to
    create) and existing real entities (entities already in the DB).
    Drafts come first so the personal entity the wizard just typed
    is the smart default in dropdowns. Slugs that exist in both
    drafts and the DB only render once (drafts win, since the
    user's typed display name is the freshest).
    """
    options: list[dict] = []
    seen: set[str] = set()
    for d in state.draft_entities:
        slug = (d.get("slug") or "").strip()
        if not slug or slug in seen:
            continue
        options.append({
            "slug": slug,
            "display_name": d.get("display_name") or slug,
        })
        seen.add(slug)
    try:
        rows = conn.execute(
            "SELECT slug, display_name FROM entities "
            "WHERE is_active = 1 ORDER BY slug"
        ).fetchall()
    except Exception:  # noqa: BLE001
        rows = []
    for r in rows:
        if r["slug"] in seen:
            continue
        options.append({
            "slug": r["slug"],
            "display_name": r["display_name"] or r["slug"],
        })
        seen.add(r["slug"])
    return options


@router.post("/setup/wizard/accounts/save")
async def wizard_accounts_save(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Add or update an account draft (modal submit).

    Form fields:
      - display_name (required)
      - kind (required)
      - entity_slug (required)
      - institution (optional)
      - last_four (optional)
      - loan_kind (required when kind=loan)
      - opening_balance (optional)
      - simplefin_account_id (hidden; preserved on edit so we don't
        lose the binding to the SimpleFIN row)
      - edit_index (when editing an existing draft)
    Re-renders with errors on validation failure (modal stays open).
    Otherwise redirects back to /setup/wizard/accounts.
    """
    if is_wizard_complete(conn):
        return RedirectResponse("/", status_code=303)
    state = load_state(conn)
    form = await request.form()

    raw = {
        "display_name": (form.get("display_name") or "").strip(),
        "kind": (form.get("kind") or "").strip(),
        "entity_slug": (form.get("entity_slug") or "").strip(),
        "institution": (form.get("institution") or "").strip(),
        "last_four": (form.get("last_four") or "").strip(),
        "loan_kind": (form.get("loan_kind") or "").strip(),
        "opening_balance": (form.get("opening_balance") or "").strip(),
        "opening_date": (form.get("opening_date") or "").strip(),
        "simplefin_account_id": (form.get("simplefin_account_id") or "").strip(),
    }
    edit_index_raw = (form.get("edit_index") or "").strip()
    try:
        edit_index = int(edit_index_raw) if edit_index_raw else None
    except ValueError:
        edit_index = None

    field_errors: dict[str, str] = {}
    if not raw["display_name"]:
        field_errors["display_name"] = "Required."
    if raw["kind"] not in {k for k, _ in ACCOUNT_KIND_OPTIONS}:
        field_errors["kind"] = "Pick an account type."
    if not raw["entity_slug"]:
        field_errors["entity_slug"] = "Pick the entity that owns this account."
    if raw["kind"] == "loan" and raw["loan_kind"] not in {k for k, _ in LOAN_KIND_OPTIONS}:
        field_errors["loan_kind"] = "Pick a loan kind."
    if raw["opening_date"]:
        try:
            date.fromisoformat(raw["opening_date"])
        except ValueError:
            field_errors["opening_date"] = "Use YYYY-MM-DD."

    if field_errors:
        return _render(
            request, "wizard/accounts.html",
            _accounts_view_context(
                conn=conn, state=state,
                open_modal=True, edit_index=edit_index,
                field_errors=field_errors, form_values=raw,
            ),
            state=state,
        )

    path = _account_path_for(
        entity_slug=raw["entity_slug"],
        kind=raw["kind"],
        display_name=raw["display_name"],
        last_four=raw["last_four"] or None,
        institution=raw["institution"] or None,
    )
    new_draft = {
        "account_path": path,
        "kind": raw["kind"],
        "entity_slug": raw["entity_slug"],
        "institution": raw["institution"],
        "last_four": raw["last_four"],
        "display_name": raw["display_name"],
        "simplefin_account_id": raw["simplefin_account_id"],
        "loan_kind": raw["loan_kind"] if raw["kind"] == "loan" else "",
        "opening_balance": raw["opening_balance"],
        # Per-account Open-directive date. Only honored at commit
        # time when kind=loan (other kinds use the global default
        # because most users don't know the actual opening date).
        "opening_date": raw["opening_date"] if raw["kind"] == "loan" else "",
        "from_simplefin": "1" if raw["simplefin_account_id"] else "",
    }

    if edit_index is not None and 0 <= edit_index < len(state.draft_accounts):
        # Preserve the original from_simplefin marker on edit.
        old = state.draft_accounts[edit_index]
        if old.get("from_simplefin"):
            new_draft["from_simplefin"] = old["from_simplefin"]
        state.draft_accounts[edit_index] = new_draft
    else:
        state.draft_accounts.append(new_draft)

    # Refresh the loan-path / loan-kind caches step 5 reads.
    state.created_account_paths = sorted({
        d["account_path"] for d in state.draft_accounts if d.get("account_path")
    })
    state.created_loan_paths = sorted({
        d["account_path"] for d in state.draft_accounts
        if d.get("kind") == "loan" and d.get("account_path")
    })
    state.loan_kinds = {
        d["account_path"]: (d.get("loan_kind") or "Other")
        for d in state.draft_accounts
        if d.get("kind") == "loan" and d.get("account_path")
    }

    save_state(conn, state)
    conn.commit()
    return RedirectResponse("/setup/wizard/accounts", status_code=303)


@router.post("/setup/wizard/accounts/remove")
async def wizard_accounts_remove(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
):
    """Drop a draft account by index. Existing real accounts are not
    removable from here — the wizard only manages drafts.

    If the dropped draft was seeded from SimpleFIN, record its
    simplefin_account_id in state.simplefin_dismissed_ids so the
    next bridge re-fetch doesn't resurrect it. (User can re-add it
    manually via "+ Add Account" if they change their mind.)
    """
    if is_wizard_complete(conn):
        return RedirectResponse("/", status_code=303)
    state = load_state(conn)
    form = await request.form()
    try:
        idx = int(form.get("index") or "")
    except (TypeError, ValueError):
        idx = -1
    if 0 <= idx < len(state.draft_accounts):
        removed = state.draft_accounts.pop(idx)
        sf_id = (removed.get("simplefin_account_id") or "").strip()
        if sf_id and sf_id not in state.simplefin_dismissed_ids:
            state.simplefin_dismissed_ids.append(sf_id)
        # Refresh caches.
        state.created_account_paths = sorted({
            d["account_path"] for d in state.draft_accounts if d.get("account_path")
        })
        state.created_loan_paths = sorted({
            d["account_path"] for d in state.draft_accounts
            if d.get("kind") == "loan" and d.get("account_path")
        })
        state.loan_kinds = {
            d["account_path"]: (d.get("loan_kind") or "Other")
            for d in state.draft_accounts
            if d.get("kind") == "loan" and d.get("account_path")
        }
        save_state(conn, state)
        conn.commit()
    return RedirectResponse("/setup/wizard/accounts", status_code=303)


@router.post("/setup/wizard/accounts", response_class=HTMLResponse)
async def wizard_accounts_continue(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Advance from accounts to property/vehicle.

    Two validation rules:
      1. At least 1 account total (existing + drafts) — same logic
         the entities continue handler uses.
      2. Every draft must be complete (display_name + entity + kind).
         SimpleFIN-seeded drafts can land in the list with empty
         kind/entity; we refuse to advance until they're filled in.
    """
    if is_wizard_complete(conn):
        return RedirectResponse("/", status_code=303)
    state = load_state(conn)
    existing = _existing_accounts_summary(conn)
    total_accounts = len(existing) + len(state.draft_accounts)
    if total_accounts == 0:
        return _render(
            request, "wizard/accounts.html",
            _accounts_view_context(
                conn=conn, state=state,
                error="Add at least one account before continuing.",
            ),
            state=state,
        )
    incomplete = [
        i for i, d in enumerate(state.draft_accounts)
        if not _account_draft_is_complete(d)
    ]
    if incomplete:
        n = len(incomplete)
        return _render(
            request, "wizard/accounts.html",
            _accounts_view_context(
                conn=conn, state=state,
                error=(
                    f"{n} account{'s' if n != 1 else ''} still "
                    f"need{'s' if n == 1 else ''} setup. Click "
                    '"Set up" on each highlighted row to fill in '
                    "the missing fields, or Remove if you don't "
                    "want that account in your books."
                ),
            ),
            state=state,
        )
    state.step = STEP_PROPVEHICLE
    save_state(conn, state)
    conn.commit()
    return _redirect_to(STEP_PROPVEHICLE)


def _write_account_opening_balances(
    *,
    app,
    conn: sqlite3.Connection,
    settings: Settings,
    state: WizardState,
) -> int:
    """Stamp a balance-anchor directive for every account that has a
    user-typed (or SimpleFIN-reported) opening balance.

    A balance-anchor records "the user says this account had X on
    this date" as a pure reference point — drift between consecutive
    anchors and the postings between them surfaces in the audit
    report. It does NOT synthesize a transaction or write a
    Beancount ``pad`` directive. The user's mental model is
    loan-style: known balances on dates, work backwards from
    today.

    Source priority per draft (same as before, but the output
    changes from pad+balance to a custom directive):

      1. **User-typed** ``opening_balance`` — applied as the user
         thinks of it ("I have $100" / "I owe $5000"). Sign is
         flipped on Liabilities so the directive's amount stays
         positive ("you owe $5000"); the audit reads it as the
         absolute reference, not a credit-side ledger number.
      2. **SimpleFIN bridge** balance for connected/mapped
         accounts — used only when the user left
         ``opening_balance`` blank. Bridge balances are stored
         verbatim with sign as reported.

    Per-account failures (bad amount, bean-check rejection) are
    logged and skipped — better to land most anchors than block
    wizard completion on one bad row.
    """
    from decimal import Decimal, InvalidOperation

    from lamella.features.dashboard.balances.writer import append_balance_anchor
    from lamella.core.beancount_io import LedgerReader
    from lamella.core.ledger_writer import BeanCheckError

    sf_balances: dict[str, tuple[str, str]] = {}
    if state.simplefin_connected:
        try:
            for r in conn.execute(
                "SELECT account_id, balance, currency "
                "FROM simplefin_discovered_accounts"
            ).fetchall():
                bal = (r["balance"] or "").strip()
                if not bal:
                    continue
                sf_balances[r["account_id"]] = (
                    bal, (r["currency"] or "USD").upper(),
                )
        except Exception:  # noqa: BLE001
            sf_balances = {}

    candidates: list[tuple[dict, str, str, str]] = []
    for d in state.draft_accounts:
        path = d.get("account_path") or ""
        if not path:
            continue
        raw = (d.get("opening_balance") or "").strip()
        currency = "USD"
        source_label = "user-entered"
        amount: Decimal | None = None
        if raw:
            cleaned = raw.replace(",", "").replace("$", "").strip()
            try:
                amount = Decimal(cleaned)
            except (InvalidOperation, ValueError):
                # Log only the account path (identifier) and a category
                # for the unparseable value — never the raw amount, which
                # is user-entered financial data.
                if not raw.strip():
                    category = "empty"
                elif len(raw) > 64:
                    category = "too-long"
                else:
                    category = "non-numeric"
                log.warning(
                    "wizard: skipping opening balance for %s — unparseable "
                    "(category=%s, len=%d)",
                    path, category, len(raw),
                )
                continue
        else:
            sf_id = (d.get("simplefin_account_id") or "").strip()
            if sf_id and sf_id in sf_balances:
                bridge_raw, currency = sf_balances[sf_id]
                cleaned = bridge_raw.replace(",", "").replace("$", "").strip()
                try:
                    amount = Decimal(cleaned)
                except (InvalidOperation, ValueError):
                    # Log only the account path (identifier) and a category
                    # for the unparseable value — never the raw amount, which
                    # is bridge-supplied financial data (ADR-0025).
                    if not bridge_raw.strip():
                        category = "empty"
                    elif len(bridge_raw) > 64:
                        category = "too-long"
                    else:
                        category = "non-numeric"
                    log.warning(
                        "wizard: skipping simplefin opening balance for %s — "
                        "unparseable (sf_id=%s, category=%s, len=%d)",
                        path, sf_id, category, len(bridge_raw),
                    )
                    continue
                source_label = "simplefin"
        if amount is None or amount == 0:
            continue
        candidates.append((d, f"{amount:.2f}", currency, source_label))
    if not candidates:
        return 0

    reader = getattr(
        app.state, "ledger_reader", None,
    ) or LedgerReader(settings.ledger_main)
    try:
        reader.invalidate()
    except Exception:  # noqa: BLE001
        pass

    today = date.today()
    written = 0
    for d, amount_str, currency, source_label in candidates:
        path = d["account_path"]
        try:
            append_balance_anchor(
                connector_config=settings.connector_config_path,
                main_bean=settings.ledger_main,
                account_path=path,
                as_of_date=today,
                balance=amount_str,
                currency=currency,
                source=f"wizard-{source_label}",
                notes=f"Wizard opening balance ({source_label})",
            )
            written += 1
        except BeanCheckError as exc:
            log.warning(
                "wizard: balance-anchor bean-check failed for %s: %s",
                path, exc,
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "wizard: balance-anchor write failed for %s: %s",
                path, exc,
            )
    if written:
        try:
            reader.invalidate()
        except Exception:  # noqa: BLE001
            pass
        log.info("wizard: wrote %d opening balance anchor(s)", written)
    return written


def _write_initial_balance_anchors(
    *,
    conn: sqlite3.Connection,
    settings: Settings,
    state: WizardState,
) -> int:
    """Stamp a balance-anchor directive for every SimpleFIN-linked
    account, using the bridge-reported balance as today's
    reconciliation point.

    The wizard runs this AFTER _commit_account_drafts so the Open
    directives are already in place. Without an anchor, the user
    has no record of what their accounts SHOULD have on day one,
    and reconciliation against a SimpleFIN-imported transaction
    history starts from zero. With anchors, the dashboard's
    "balance vs. anchor" diff has a starting point.

    Returns the number of anchors written. Bridge balance lookup
    comes from simplefin_discovered_accounts (populated by the
    /bank/connected handler). Per-account failures (bean-check,
    bad balance string) are logged and skipped — better to land
    most anchors than block the whole commit on one bad row.
    """
    if not state.simplefin_connected:
        return 0
    try:
        rows = conn.execute(
            "SELECT account_id, balance, currency "
            "FROM simplefin_discovered_accounts"
        ).fetchall()
    except Exception:  # noqa: BLE001
        return 0
    by_id = {
        r["account_id"]: r for r in rows
        if (r["balance"] or "").strip()
    }
    if not by_id:
        return 0

    from lamella.features.dashboard.balances.writer import append_balance_anchor

    today = date.today()
    written = 0
    for d in state.draft_accounts:
        sf_id = (d.get("simplefin_account_id") or "").strip()
        path = d.get("account_path") or ""
        if not sf_id or not path or sf_id not in by_id:
            continue
        bal_row = by_id[sf_id]
        try:
            append_balance_anchor(
                connector_config=settings.connector_config_path,
                main_bean=settings.ledger_main,
                account_path=path,
                as_of_date=today,
                balance=bal_row["balance"],
                currency=(bal_row["currency"] or "USD").upper(),
                source="wizard-simplefin-seed",
                notes="Initial balance from SimpleFIN at wizard time",
            )
            written += 1
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "wizard: balance-anchor for %s failed: %s", path, exc,
            )
    if written:
        log.info("wizard: stamped %d balance anchor(s)", written)
    return written


async def _enable_simplefin_and_run_initial_ingest(
    *,
    app,
    conn: sqlite3.Connection,
    settings: Settings,
    state: WizardState,
) -> None:
    """Flip SimpleFIN to active mode + scheduled fetches + run an
    initial ingest so the user lands on a dashboard with their
    real transactions, not "(disabled) never".

    Steps:
      1. Set simplefin_mode=active, simplefin_fetch_interval_hours=6
         via AppSettingsStore (dual-writes to ledger as setting
         directives so reconstruct rebuilds them).
      2. Re-register the scheduler so the next cron tick uses the
         new mode/interval without a server restart.
      3. Run a one-shot ingest (trigger="wizard"). Result is logged
         in simplefin_ingests so the user can audit it from
         /simplefin.

    All best-effort: a failure here doesn't tank the wizard since
    the books are already committed. The user can manually re-run
    a fetch from /simplefin if this fails.
    """
    if not state.simplefin_connected:
        return
    try:
        from lamella.core.settings.store import AppSettingsStore
        store = AppSettingsStore(
            conn,
            connector_config_path=settings.connector_config_path,
            main_bean_path=settings.ledger_main if settings.ledger_main.exists() else None,
        )
        store.set("simplefin_mode", "active")
        store.set("simplefin_fetch_interval_hours", "6")
        settings.apply_kv_overrides({
            "simplefin_mode": "active",
            "simplefin_fetch_interval_hours": "6",
        })
        log.info("wizard: simplefin set to active + 6h interval")
    except Exception as exc:  # noqa: BLE001
        log.warning("wizard: simplefin mode flip failed: %s", exc)

    # Re-register the scheduler (no-op if no scheduler is attached).
    try:
        register_fn = getattr(app.state, "_simplefin_register", None)
        if register_fn is not None:
            register_fn()
    except Exception as exc:  # noqa: BLE001
        log.warning("wizard: scheduler re-register failed: %s", exc)

    # Initial ingest — wires every service the way /simplefin/fetch does.
    try:
        from lamella.core.beancount_io import LedgerReader
        from lamella.features.review_queue.service import ReviewService
        from lamella.features.rules.service import RuleService
        from lamella.adapters.simplefin.client import SimpleFINClient
        from lamella.features.bank_sync.ingest import SimpleFINIngest
        from lamella.features.bank_sync.writer import SimpleFINWriter

        access_url = (
            settings.simplefin_access_url.get_secret_value()
            if settings.simplefin_access_url else ""
        )
        if not access_url:
            log.info("wizard: skipping initial ingest — no access url")
            return

        reader = getattr(
            app.state, "ledger_reader",
            None,
        ) or LedgerReader(settings.ledger_main)
        try:
            reader.invalidate()
        except Exception:  # noqa: BLE001
            pass
        rules = RuleService(conn)
        reviews = ReviewService(conn)
        writer = SimpleFINWriter(
            main_bean=settings.ledger_main,
            simplefin_path=settings.simplefin_transactions_path,
        )
        ai = getattr(app.state, "ai_service", None)

        async with SimpleFINClient(access_url=access_url) as client:
            ingest = SimpleFINIngest(
                conn=conn, settings=settings, reader=reader,
                rules=rules, reviews=reviews, writer=writer, ai=ai,
            )
            # First-time pull reaches back 90 days so a fresh
            # ledger gets a real corpus to classify against.
            # The recurring scheduler still uses the persistent
            # simplefin_lookback_days setting (default 14).
            result = await ingest.run(
                client=client,
                trigger="wizard",
                lookback_days_override=90,
            )
        log.info(
            "wizard: initial simplefin ingest done — accounts=%d, new_txns=%d",
            len(result.per_account), result.new_txns,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "wizard: initial simplefin ingest failed (non-fatal): %s",
            exc, exc_info=True,
        )


def _commit_account_drafts(
    *,
    conn: sqlite3.Connection,
    settings: Settings,
    state: WizardState,
) -> None:
    """Materialize draft accounts into accounts_meta + Open directives.

    Must run AFTER ``_commit_entity_drafts`` so the entity_slug FK
    on accounts_meta resolves. Best-effort per row: a single bad
    write is logged and skipped, not propagated.
    """
    if not state.draft_accounts:
        return
    _ensure_main_bean(settings)
    writer = AccountsWriter(
        main_bean=settings.ledger_main,
        connector_accounts=settings.connector_accounts_path,
    )
    paths_to_open: list[str] = []
    for d in state.draft_accounts:
        path = d.get("account_path") or ""
        # Compose the path on the fly when the draft was seeded from
        # SimpleFIN and the user advanced without ever opening the
        # edit modal (which is what writes account_path). All three
        # required fields must be set; otherwise skip — the Continue
        # validator should have blocked us, but defense in depth.
        if not path and d.get("kind") and d.get("entity_slug") and d.get("display_name"):
            try:
                path = _account_path_for(
                    entity_slug=d["entity_slug"],
                    kind=d["kind"],
                    display_name=d["display_name"],
                    last_four=d.get("last_four") or None,
                    institution=d.get("institution") or None,
                )
            except Exception:  # noqa: BLE001
                path = ""
        if not path:
            log.warning(
                "commit: skipping account draft with no path: %r", d,
            )
            continue
        try:
            existing = conn.execute(
                "SELECT account_path FROM accounts_meta WHERE account_path = ?",
                (path,),
            ).fetchone()
            if existing is None:
                conn.execute(
                    """
                    INSERT INTO accounts_meta
                        (account_path, kind, entity_slug, institution,
                         last_four, display_name, simplefin_account_id,
                         created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    (
                        path,
                        d.get("kind") or None,
                        d.get("entity_slug") or None,
                        d.get("institution") or None,
                        d.get("last_four") or None,
                        d.get("display_name") or None,
                        d.get("simplefin_account_id") or None,
                    ),
                )
                paths_to_open.append(path)
            else:
                conn.execute(
                    """
                    UPDATE accounts_meta SET
                        kind = ?, entity_slug = ?, institution = ?,
                        last_four = ?, display_name = ?,
                        simplefin_account_id = COALESCE(?, simplefin_account_id),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE account_path = ?
                    """,
                    (
                        d.get("kind") or None,
                        d.get("entity_slug") or None,
                        d.get("institution") or None,
                        d.get("last_four") or None,
                        d.get("display_name") or None,
                        d.get("simplefin_account_id") or None,
                        path,
                    ),
                )
        except Exception as exc:  # noqa: BLE001
            log.warning("commit: account draft %s failed: %s", path, exc)
    # Open each newly-inserted account with the right date. Most use
    # the global default (1900-01-01 — see _default_open_date and
    # CLAUDE-style commentary there). Loans whose draft.opening_date
    # is set get THAT date instead — that's the one account class
    # where the user typically knows when it actually opened
    # (mortgage origination, auto loan funding date), and using a
    # real date here keeps the per-loan amortization tables sane.
    default_date = _default_open_date(settings)
    paths_by_date: dict[str, list[str]] = {}
    for d in state.draft_accounts:
        path = d.get("account_path") or ""
        if not path or path not in paths_to_open:
            continue
        chosen = default_date
        if d.get("kind") == "loan" and (d.get("opening_date") or "").strip():
            try:
                chosen = date.fromisoformat(d["opening_date"].strip())
            except (TypeError, ValueError):
                pass
        paths_by_date.setdefault(chosen.isoformat(), []).append(path)
    for iso, paths in paths_by_date.items():
        try:
            writer.write_opens(paths, opened_on=date.fromisoformat(iso))
        except Exception as exc:  # noqa: BLE001
            log.warning("commit: write_opens for %s failed: %s", iso, exc)


# ---------------------------------------------------------------------------
# Step 5 — Property + Vehicle (optional)
# ---------------------------------------------------------------------------


PROPERTY_TYPE_OPTIONS = (
    ("primary", "Primary residence"),
    ("rental", "Rental"),
    ("second", "Second home / vacation"),
    ("land", "Land"),
    ("other", "Other"),
)
VALID_PROPERTY_TYPES = frozenset(t[0] for t in PROPERTY_TYPE_OPTIONS)
US_STATES = (
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC",
)


def _existing_property_slugs(conn: sqlite3.Connection) -> set[str]:
    try:
        return {r["slug"] for r in conn.execute("SELECT slug FROM properties").fetchall()}
    except Exception:  # noqa: BLE001
        return set()


def _existing_vehicle_slugs(conn: sqlite3.Connection) -> set[str]:
    try:
        return {r["slug"] for r in conn.execute("SELECT slug FROM vehicles").fetchall()}
    except Exception:  # noqa: BLE001
        return set()


def _existing_properties_summary(conn: sqlite3.Connection) -> list[dict]:
    """Read existing properties for the read-only "already in your books"
    section. The wizard never modifies these — edit them via
    /settings/properties."""
    try:
        rows = conn.execute(
            "SELECT slug, display_name, property_type, entity_slug, "
            "       address, city, state, postal_code, is_rental, is_active "
            "  FROM properties WHERE is_active = 1 ORDER BY slug"
        ).fetchall()
    except Exception:  # noqa: BLE001
        return []
    out = []
    for r in rows:
        addr_bits = [r["city"] or "", r["state"] or ""]
        addr_summary = ", ".join(b for b in addr_bits if b).strip(", ")
        out.append({
            "slug": r["slug"],
            "display_name": r["display_name"] or r["slug"],
            "property_type": r["property_type"] or "",
            "entity_slug": r["entity_slug"] or "",
            "address_summary": addr_summary,
            "is_rental": bool(r["is_rental"]),
        })
    return out


def _existing_vehicles_summary(conn: sqlite3.Connection) -> list[dict]:
    try:
        rows = conn.execute(
            "SELECT slug, display_name, year, make, model, entity_slug, is_active "
            "  FROM vehicles WHERE is_active = 1 ORDER BY slug"
        ).fetchall()
    except Exception:  # noqa: BLE001
        return []
    out = []
    for r in rows:
        ymm = " ".join(
            str(b) for b in [r["year"], r["make"], r["model"]] if b
        ).strip()
        out.append({
            "slug": r["slug"],
            "display_name": r["display_name"] or r["slug"],
            "year_make_model": ymm,
            "entity_slug": r["entity_slug"] or "",
        })
    return out


def _loan_label(
    path: str,
    draft: dict | None,
    entity_display_by_slug: dict[str, str],
) -> str:
    """Human-readable label for the linked-loan dropdowns.

    Format: "Bank Two Mortgage — Anthony Quick · ****1234"
    Falls back to camel-splitting the leaf segment when the draft
    has no display_name. The raw path is the last resort.
    """
    from lamella.core.registry.alias import _split_camel  # local import: avoid cycle

    name = ""
    entity_slug = ""
    last_four = ""
    if draft:
        name = (draft.get("display_name") or "").strip()
        entity_slug = (draft.get("entity_slug") or "").strip()
        last_four = (draft.get("last_four") or "").strip()
    if not name:
        leaf = path.rsplit(":", 1)[-1] if path else ""
        name = _split_camel(leaf) or path
    if not entity_slug and path:
        parts = path.split(":")
        if len(parts) >= 2:
            entity_slug = parts[1]
    entity_pretty = entity_display_by_slug.get(entity_slug, entity_slug)
    pieces = [name]
    if entity_pretty:
        pieces.append(entity_pretty)
    label = " — ".join(pieces)
    if last_four:
        label = f"{label} · ****{last_four}"
    return label


def _propvehicle_view_context(
    *,
    conn: sqlite3.Connection,
    state: WizardState,
    open_modal: str = "",
    edit_index: int | None = None,
    field_errors: dict | None = None,
    form_values: dict | None = None,
    error: str | None = None,
) -> dict:
    """Common context for the property/vehicle list view + the modal.

    open_modal: "" | "property" | "vehicle"
    edit_index: index into draft_properties or draft_vehicles (depending on
                open_modal) when editing
    """
    existing_props = _existing_properties_summary(conn)
    existing_vehs = _existing_vehicles_summary(conn)

    # Loan options for the dropdowns. Pull from drafts (which are the
    # only loans the wizard knows about right now) plus existing
    # accounts_meta loans.
    entity_display_by_slug: dict[str, str] = {
        opt["slug"]: opt["display_name"] for opt in _entity_options(conn, state)
    }
    draft_by_path: dict[str, dict] = {
        d.get("account_path", ""): d for d in state.draft_accounts if d.get("account_path")
    }
    mortgage_loans: list[dict] = []
    auto_loans: list[dict] = []
    for path in state.created_loan_paths:
        kind = state.loan_kinds.get(path, "")
        item = {
            "path": path,
            "label": _loan_label(path, draft_by_path.get(path), entity_display_by_slug),
        }
        if kind == "Mortgage":
            mortgage_loans.append(item)
        elif kind == "Auto":
            auto_loans.append(item)

    edit_target: dict | None = None
    if open_modal == "property" and edit_index is not None and 0 <= edit_index < len(state.draft_properties):
        edit_target = state.draft_properties[edit_index]
    if open_modal == "vehicle" and edit_index is not None and 0 <= edit_index < len(state.draft_vehicles):
        edit_target = state.draft_vehicles[edit_index]

    return {
        "existing_properties": existing_props,
        "existing_vehicles": existing_vehs,
        "draft_properties": state.draft_properties,
        "draft_vehicles": state.draft_vehicles,
        "entity_options": _entity_options(conn, state),
        "mortgage_loans": mortgage_loans,
        "auto_loans": auto_loans,
        "property_types": PROPERTY_TYPE_OPTIONS,
        "us_states": US_STATES,
        "open_modal": open_modal,
        "edit_index": edit_index,
        "edit_target": edit_target,
        "field_errors": field_errors or {},
        "form_values": form_values or {},
        "error": error,
    }


@router.get("/setup/wizard/property-vehicle", response_class=HTMLResponse)
def wizard_property_vehicle(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
    add: str | None = None,
    edit: str | None = None,
):
    if is_wizard_complete(conn):
        return RedirectResponse("/", status_code=303)
    state = load_state(conn)
    if not state.intent:
        return _redirect_to(STEP_WELCOME)
    state.step = STEP_PROPVEHICLE
    save_state(conn, state)
    conn.commit()

    open_modal = ""
    edit_index: int | None = None
    if add in {"property", "vehicle"}:
        open_modal = add
    elif edit:
        # edit param format: "property:<idx>" or "vehicle:<idx>"
        try:
            kind, idx_s = edit.split(":", 1)
            if kind in {"property", "vehicle"}:
                open_modal = kind
                edit_index = int(idx_s)
        except (ValueError, AttributeError):
            pass

    return _render(
        request, "wizard/property_vehicle.html",
        _propvehicle_view_context(
            conn=conn, state=state,
            open_modal=open_modal, edit_index=edit_index,
        ),
        state=state,
    )


def _disambiguate_against(seen: set[str], base: str) -> str:
    if not base:
        base = "Item"
    candidate = base
    n = 2
    while candidate in seen:
        candidate = f"{base}{n}"
        n += 1
        if n > 99:
            candidate = f"{base}{int(datetime.now(UTC).timestamp())}"
            break
    return candidate


@router.post("/setup/wizard/property-vehicle/save-property")
async def wizard_save_property(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    if is_wizard_complete(conn):
        return RedirectResponse("/", status_code=303)
    state = load_state(conn)
    form = await request.form()
    raw = {
        "display_name": (form.get("display_name") or "").strip(),
        "property_type": (form.get("property_type") or "").strip(),
        "entity_slug": (form.get("entity_slug") or "").strip(),
        "linked_loan": (form.get("linked_loan") or "").strip(),
        "address": (form.get("address") or "").strip(),
        "city": (form.get("city") or "").strip(),
        "state": (form.get("state") or "").strip().upper(),
        "postal_code": (form.get("postal_code") or "").strip(),
    }
    edit_idx_raw = (form.get("edit_index") or "").strip()
    try:
        edit_index = int(edit_idx_raw) if edit_idx_raw else None
    except ValueError:
        edit_index = None

    field_errors: dict[str, str] = {}
    if not raw["display_name"]:
        field_errors["display_name"] = "Required."
    if raw["property_type"] not in VALID_PROPERTY_TYPES:
        field_errors["property_type"] = "Pick a property type."
    if not raw["entity_slug"]:
        field_errors["entity_slug"] = "Pick the entity that owns this property."

    if field_errors:
        return _render(
            request, "wizard/property_vehicle.html",
            _propvehicle_view_context(
                conn=conn, state=state,
                open_modal="property", edit_index=edit_index,
                field_errors=field_errors, form_values=raw,
            ),
            state=state,
        )

    if edit_index is not None and 0 <= edit_index < len(state.draft_properties):
        # Preserve slug on edit (slugs are immutable).
        old = state.draft_properties[edit_index]
        slug = old.get("slug", "")
        new_draft = {**raw, "slug": slug}
        state.draft_properties[edit_index] = new_draft
    else:
        seen = (
            {p.get("slug") for p in state.draft_properties}
            | _existing_property_slugs(conn)
        )
        slug = _disambiguate_against(
            seen, _slug_from_name(raw["display_name"], fallback="Home"),
        )
        state.draft_properties.append({**raw, "slug": slug})

    state.created_properties = sorted(
        set(state.created_properties) | {slug}
    )
    save_state(conn, state)
    conn.commit()
    return RedirectResponse("/setup/wizard/property-vehicle", status_code=303)


@router.post("/setup/wizard/property-vehicle/save-vehicle")
async def wizard_save_vehicle(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    if is_wizard_complete(conn):
        return RedirectResponse("/", status_code=303)
    state = load_state(conn)
    form = await request.form()
    raw = {
        "display_name": (form.get("display_name") or "").strip(),
        "entity_slug": (form.get("entity_slug") or "").strip(),
        "linked_loan": (form.get("linked_loan") or "").strip(),
        "year": (form.get("year") or "").strip(),
        "make": (form.get("make") or "").strip(),
        "model": (form.get("model") or "").strip(),
    }
    edit_idx_raw = (form.get("edit_index") or "").strip()
    try:
        edit_index = int(edit_idx_raw) if edit_idx_raw else None
    except ValueError:
        edit_index = None

    field_errors: dict[str, str] = {}
    if not raw["display_name"]:
        field_errors["display_name"] = "Required."
    if not raw["entity_slug"]:
        field_errors["entity_slug"] = "Pick the entity that owns this vehicle."
    if raw["year"]:
        try:
            yr = int(raw["year"])
            if yr < 1900 or yr > 2100:
                field_errors["year"] = "Year looks off."
        except ValueError:
            field_errors["year"] = "Year must be a number."

    if field_errors:
        return _render(
            request, "wizard/property_vehicle.html",
            _propvehicle_view_context(
                conn=conn, state=state,
                open_modal="vehicle", edit_index=edit_index,
                field_errors=field_errors, form_values=raw,
            ),
            state=state,
        )

    if edit_index is not None and 0 <= edit_index < len(state.draft_vehicles):
        old = state.draft_vehicles[edit_index]
        slug = old.get("slug", "")
        new_draft = {**raw, "slug": slug}
        state.draft_vehicles[edit_index] = new_draft
    else:
        seen = (
            {v.get("slug") for v in state.draft_vehicles}
            | _existing_vehicle_slugs(conn)
        )
        # Use the canonical V<year><Make><Model> format when we have
        # those three fields — matches the rest of the app's slug
        # convention (registry.vehicle_companion). Falls back to the
        # display name otherwise.
        if raw["year"] and raw["make"] and raw["model"]:
            from lamella.features.vehicles.vehicle_companion import (
                vehicle_slug_from_year_make_model,
            )
            base_slug = vehicle_slug_from_year_make_model(
                raw["year"], raw["make"], raw["model"],
            )
        else:
            base_slug = _slug_from_name(
                raw["display_name"], fallback="Vehicle",
            )
        slug = _disambiguate_against(seen, base_slug)
        state.draft_vehicles.append({**raw, "slug": slug})

    state.created_vehicles = sorted(set(state.created_vehicles) | {slug})
    save_state(conn, state)
    conn.commit()
    return RedirectResponse("/setup/wizard/property-vehicle", status_code=303)


@router.post("/setup/wizard/property-vehicle/remove")
async def wizard_propvehicle_remove(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
):
    """Drop a property OR vehicle draft. Body: kind=property|vehicle, index."""
    if is_wizard_complete(conn):
        return RedirectResponse("/", status_code=303)
    state = load_state(conn)
    form = await request.form()
    kind = (form.get("kind") or "").strip()
    try:
        idx = int(form.get("index") or "")
    except (TypeError, ValueError):
        idx = -1
    if kind == "property" and 0 <= idx < len(state.draft_properties):
        slug = state.draft_properties[idx].get("slug", "")
        state.draft_properties.pop(idx)
        if slug:
            state.created_properties = [s for s in state.created_properties if s != slug]
        save_state(conn, state)
        conn.commit()
    elif kind == "vehicle" and 0 <= idx < len(state.draft_vehicles):
        slug = state.draft_vehicles[idx].get("slug", "")
        state.draft_vehicles.pop(idx)
        if slug:
            state.created_vehicles = [s for s in state.created_vehicles if s != slug]
        save_state(conn, state)
        conn.commit()
    return RedirectResponse("/setup/wizard/property-vehicle", status_code=303)


def _property_scaffold_paths(
    *, entity_slug: str, slug: str,
    is_rental: bool, has_mortgage: bool,
) -> list[str]:
    """The full canonical chart for a property.

    Delegates to ``registry.property_companion.property_chart_paths_for``
    so the wizard's scaffold matches what the rest of the app
    (setup-progress checker, property edit, reports) expects. The
    has_mortgage flag is informational here — MortgageInterest is
    always part of the base chart; the linked-loan binding lives
    on the properties row, not in the chart itself.
    """
    from lamella.features.properties.property_companion import (
        property_chart_paths_for,
    )
    paths = [
        p.path for p in property_chart_paths_for(
            property_slug=slug,
            entity_slug=entity_slug,
            is_rental=is_rental,
        )
    ]
    return paths


def _vehicle_scaffold_paths(
    *, entity_slug: str, slug: str, has_auto_loan: bool,
) -> list[str]:
    """The full canonical chart for a vehicle.

    Delegates to ``registry.vehicle_companion.vehicle_chart_paths_for``
    so the wizard's scaffold matches what the rest of the app
    expects (17-account chart matching IRS Schedule C Part IV +
    Pub 463). has_auto_loan is informational — the loan binding is
    captured on the vehicles row.
    """
    from lamella.features.vehicles.vehicle_companion import (
        vehicle_chart_paths_for,
    )
    return [
        p.path for p in vehicle_chart_paths_for(
            vehicle_slug=slug, entity_slug=entity_slug,
        )
    ]


def _commit_property_drafts(
    *,
    conn: sqlite3.Connection,
    settings: Settings,
    state: WizardState,
) -> None:
    """Write draft properties to the properties table + ALWAYS scaffold
    the asset / expense / income tree. Mortgage-linked properties get
    extra Mortgage:Interest + Mortgage:Escrow expense accounts.
    Rentals get Rent + Management + HOA."""
    if not state.draft_properties:
        return
    writer = AccountsWriter(
        main_bean=settings.ledger_main,
        connector_accounts=settings.connector_accounts_path,
    )
    for d in state.draft_properties:
        try:
            slug = d.get("slug") or ""
            ent = d.get("entity_slug") or ""
            if not slug or not ent:
                continue
            ptype = d.get("property_type") or "primary"
            is_rental = ptype == "rental"
            is_primary = ptype == "primary"
            conn.execute(
                """
                INSERT OR IGNORE INTO properties
                    (slug, display_name, property_type, entity_slug,
                     address, city, state, postal_code,
                     is_primary_residence, is_rental)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    slug,
                    d.get("display_name") or slug,
                    ptype,
                    ent,
                    d.get("address") or None,
                    d.get("city") or None,
                    d.get("state") or None,
                    d.get("postal_code") or None,
                    1 if is_primary else 0,
                    1 if is_rental else 0,
                ),
            )
            if d.get("linked_loan"):
                try:
                    conn.execute(
                        "UPDATE properties SET linked_loan_account = ? "
                        "WHERE slug = ?",
                        (d["linked_loan"], slug),
                    )
                except sqlite3.OperationalError:
                    pass
            try:
                writer.write_opens(
                    _property_scaffold_paths(
                        entity_slug=ent, slug=slug,
                        is_rental=is_rental,
                        has_mortgage=bool(d.get("linked_loan")),
                    ),
                    opened_on=_default_open_date(settings),
                    comment=f"Property {slug}",
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("commit: property %s scaffold: %s", slug, exc)
        except Exception as exc:  # noqa: BLE001
            log.warning("commit: property draft %s: %s", d, exc)


def _commit_vehicle_drafts(
    *,
    conn: sqlite3.Connection,
    settings: Settings,
    state: WizardState,
) -> None:
    """Write draft vehicles + ALWAYS scaffold expense tree. Auto-loan-
    linked vehicles get an extra Loan:Interest expense account."""
    if not state.draft_vehicles:
        return
    writer = AccountsWriter(
        main_bean=settings.ledger_main,
        connector_accounts=settings.connector_accounts_path,
    )
    for d in state.draft_vehicles:
        try:
            slug = d.get("slug") or ""
            ent = d.get("entity_slug") or ""
            if not slug or not ent:
                continue
            year_val: int | None = None
            try:
                year_val = int(d.get("year") or "")
            except (TypeError, ValueError):
                year_val = None
            conn.execute(
                """
                INSERT OR IGNORE INTO vehicles
                    (slug, display_name, year, make, model, entity_slug)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    slug,
                    d.get("display_name") or slug,
                    year_val,
                    d.get("make") or None,
                    d.get("model") or None,
                    ent,
                ),
            )
            if d.get("linked_loan"):
                try:
                    conn.execute(
                        "UPDATE vehicles SET linked_loan_account = ? "
                        "WHERE slug = ?",
                        (d["linked_loan"], slug),
                    )
                except sqlite3.OperationalError:
                    pass
            try:
                writer.write_opens(
                    _vehicle_scaffold_paths(
                        entity_slug=ent, slug=slug,
                        has_auto_loan=bool(d.get("linked_loan")),
                    ),
                    opened_on=_default_open_date(settings),
                    comment=f"Vehicle {slug}",
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("commit: vehicle %s scaffold: %s", slug, exc)
        except Exception as exc:  # noqa: BLE001
            log.warning("commit: vehicle draft %s: %s", d, exc)


@router.post("/setup/wizard/property-vehicle/continue")
def wizard_propvehicle_continue(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
):
    if is_wizard_complete(conn):
        return RedirectResponse("/", status_code=303)
    state = load_state(conn)
    state.step = STEP_DONE
    save_state(conn, state)
    conn.commit()
    return _redirect_to(STEP_DONE)


# ---------------------------------------------------------------------------
# Step 6 — Done
# ---------------------------------------------------------------------------


def _wizard_recap_counts(state: WizardState) -> dict:
    """The "what we set up" tally shown on the Done step.

    Counts WIZARD-CREATED drafts only — existing real entities and
    accounts are visible read-only on prior steps but don't belong
    in the recap of "what the wizard just made for you." Accounts
    are counted from draft_accounts (every draft that's complete
    enough to commit) rather than created_account_paths (which
    only has paths from explicitly-edited drafts).
    """
    eligible_accts = [
        d for d in state.draft_accounts if _account_draft_is_complete(d)
    ]
    return {
        "entities": len(state.draft_entities),
        "accounts": len(eligible_accts),
        "properties": len(state.draft_properties),
        "vehicles": len(state.draft_vehicles),
    }


def _finalize_step_keys(state: WizardState) -> list[tuple[str, str]]:
    """Phases the finalize job will report on, in execution order, as
    (key, human_label) tuples. Skipped phases don't appear here so
    both the worker and the template stay in lock-step.
    """
    keys: list[tuple[str, str]] = []
    keys.append(("entities", "Creating entities"))
    keys.append(("accounts", "Setting up accounts"))
    if state.draft_properties:
        keys.append(("properties", "Linking properties"))
    if state.draft_vehicles:
        keys.append(("vehicles", "Linking vehicles"))
    keys.append(("opening", "Stamping opening balances"))
    if state.simplefin_connected:
        keys.append(("simplefin", "Pulling first transactions"))
    keys.append(("bean-check", "Validating ledger"))
    keys.append(("dashboard", "Loading your dashboard"))
    return keys


def _finalize_worker(
    *,
    ctx,
    app,
    settings: Settings,
    state: WizardState,
) -> dict:
    """Background worker for /setup/wizard/done.

    Runs the same commit pipeline the route used to run synchronously,
    but emits one ``info`` event per phase prefixed with ``[phase_key]``
    so the finalizing page can drive a live step list.

    Opens its own SQLite connection rather than borrowing the request
    thread's: this keeps the long-running pipeline off the main app
    connection's RLock (so request threads aren't starved) and survives
    test-client teardown closing the request-scoped conn before the
    worker thread finishes.
    """
    import asyncio as _asyncio

    from lamella.core.db import connect as _db_connect

    phases = _finalize_step_keys(state)
    # The "dashboard" phase is client-side; subtract it from the total.
    ctx.set_total(max(1, len(phases) - 1))

    conn = _db_connect(settings.db_path)
    try:
        # 1. Entities
        n = len(state.draft_entities)
        ctx.emit(
            f"[entities] Creating {n} entit{'ies' if n != 1 else 'y'}…"
            if n else "[entities] Entities ready",
            outcome="info",
        )
        _commit_entity_drafts(conn=conn, settings=settings, state=state)
        ctx.advance()

        # 2. Accounts
        eligible = [
            d for d in state.draft_accounts if _account_draft_is_complete(d)
        ]
        n = len(eligible)
        ctx.emit(
            f"[accounts] Setting up {n} account{'s' if n != 1 else ''}…"
            if n else "[accounts] Accounts ready",
            outcome="info",
        )
        _commit_account_drafts(conn=conn, settings=settings, state=state)
        ctx.advance()

        # 3. Properties
        if state.draft_properties:
            n = len(state.draft_properties)
            ctx.emit(
                f"[properties] Linking {n} propert{'ies' if n != 1 else 'y'}…",
                outcome="info",
            )
            _commit_property_drafts(conn=conn, settings=settings, state=state)
            ctx.advance()

        # 4. Vehicles
        if state.draft_vehicles:
            n = len(state.draft_vehicles)
            ctx.emit(
                f"[vehicles] Linking {n} vehicle{'s' if n != 1 else ''}…",
                outcome="info",
            )
            _commit_vehicle_drafts(conn=conn, settings=settings, state=state)
            ctx.advance()

        # 5. Opening balances + initial balance anchors
        ctx.emit("[opening] Stamping opening balances…", outcome="info")
        try:
            _write_account_opening_balances(
                app=app, conn=conn, settings=settings, state=state,
            )
            _write_initial_balance_anchors(
                conn=conn, settings=settings, state=state,
            )
            conn.commit()
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "wizard finalize: opening balances failed: %s",
                exc, exc_info=True,
            )
            ctx.emit(f"Opening balances skipped: {exc}", outcome="failure")
        ctx.advance()

        # 6. SimpleFIN initial pull (async — run on a fresh loop in this
        #    worker thread).
        if state.simplefin_connected:
            ctx.emit(
                "[simplefin] Pulling first transactions from SimpleFIN…",
                outcome="info",
            )
            loop = _asyncio.new_event_loop()
            try:
                loop.run_until_complete(
                    _enable_simplefin_and_run_initial_ingest(
                        app=app, conn=conn, settings=settings, state=state,
                    )
                )
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "wizard finalize: simplefin pull failed: %s",
                    exc, exc_info=True,
                )
                ctx.emit(
                    f"SimpleFIN pull failed (continuing): {exc}",
                    outcome="failure",
                )
            finally:
                loop.close()
            ctx.advance()

        # 7. Bean-check
        ctx.emit("[bean-check] Validating ledger…", outcome="info")
        try:
            from lamella.core.ledger_writer import BeanCheckError, run_bean_check
            run_bean_check(settings.ledger_main)
        except BeanCheckError as exc:
            ctx.emit(f"bean-check warning: {exc}", outcome="failure")
        except Exception as exc:  # noqa: BLE001
            ctx.emit(f"validation skipped: {exc}", outcome="info")
        ctx.advance()
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass

    return {"redirect_url": "/?welcome=1"}


@router.get("/setup/wizard/done", response_class=HTMLResponse)
def wizard_done(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    state = load_state(conn)
    # If the wizard already finished, don't show the recap again or
    # let the user re-submit /done — send them straight home. Without
    # this, a stale browser tab on /setup/wizard/done lets the user
    # re-trigger the entire commit pipeline.
    if is_wizard_complete(conn):
        return RedirectResponse("/", status_code=303)
    # Drive-by guard. A stale browser link to /setup/wizard/done
    # used to render the finalize page even when the user had not
    # touched welcome / entities / accounts; submitting from here
    # then committed an empty install. Bounce to the earliest
    # incomplete step so the user picks up where the wizard
    # actually expects them to be.
    needed = _earliest_incomplete_step(state)
    if needed is not None:
        return _redirect_to(needed)
    state.step = STEP_DONE
    save_state(conn, state)
    conn.commit()

    return _render(
        request, "wizard/done.html",
        {"counts": _wizard_recap_counts(state)},
        state=state,
    )


@router.post("/setup/wizard/done", response_class=HTMLResponse)
def wizard_finalize(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Lock the wizard, kick off the commit pipeline as a background
    job, and redirect immediately to the finalizing page.

    The actual ledger writes (entities → accounts → properties →
    vehicles → opening balances → SimpleFIN ingest → bean-check) run
    in the job runner so the user lands on the progress page within
    milliseconds and watches real-time status, instead of staring at
    a blank tab while the request thread does 30+ seconds of work.

    Defense in depth:
      * Already-finalized? Skip straight to the dashboard.
      * Earliest-incomplete step set? Bounce back to it.
      * Any incomplete account draft? Bounce to the accounts step.
    """
    state = load_state(conn)

    # Re-submit guard. A stale browser tab on /setup/wizard/done was
    # re-running the entire commit pipeline (40+ seconds) on every
    # POST. Once the wizard is locked, finalize is a no-op redirect.
    if is_wizard_complete(conn):
        return RedirectResponse("/", status_code=303)

    needed = _earliest_incomplete_step(state)
    if needed is not None:
        return _redirect_to(needed)
    incomplete = [
        i for i, d in enumerate(state.draft_accounts)
        if not _account_draft_is_complete(d)
    ]
    if incomplete:
        return RedirectResponse(
            "/setup/wizard/accounts", status_code=303,
        )

    # Lock the wizard up front. Once we hand off to the job runner
    # the user can navigate elsewhere; the wizard must not let them
    # re-enter from /welcome mid-finalize.
    state.completed_at = datetime.now(UTC).isoformat(timespec="seconds")
    state.step = STEP_DONE
    save_state(conn, state)
    if state.name:
        try:
            conn.execute(
                "INSERT OR REPLACE INTO app_settings (key, value, updated_at) "
                "VALUES (?, ?, CURRENT_TIMESTAMP)",
                ("user_display_name", state.name),
            )
        except sqlite3.OperationalError:
            pass
    conn.commit()

    request.app.state.needs_welcome = False
    request.app.state.needs_reconstruct = False
    request.app.state.setup_required_complete = True

    app = request.app
    state_snapshot = state

    def _work(ctx):
        return _finalize_worker(
            ctx=ctx, app=app, settings=settings, state=state_snapshot,
        )

    runner = request.app.state.job_runner
    job_id = runner.submit(
        kind="wizard-finalize",
        title="Setting up your books",
        fn=_work,
        return_url="/?welcome=1",
    )

    return RedirectResponse(
        f"/setup/wizard/finalizing?job={job_id}", status_code=303,
    )


@router.get("/setup/wizard/finalizing", response_class=HTMLResponse)
def wizard_finalizing(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
    job: str = "",
):
    """Live progress page between the wizard and the dashboard.

    The page subscribes to the finalize job's SSE stream
    (``/jobs/{job_id}/stream``) and walks the step list in real
    time. When the job hits a terminal status, the page fades out
    and redirects to the dashboard.

    The ``?job=`` query param is normally set by the redirect from
    POST /setup/wizard/done. A direct hit (no job id) still renders
    the page with the step list pre-marked done — this preserves the
    "see what was set up" view for users who navigate here from a
    bookmark after the wizard already completed.
    """
    state = load_state(conn)
    steps = _finalize_step_keys(state)
    return _render(
        request, "wizard/finalizing.html",
        {
            "steps": [{"key": k, "label": l} for k, l in steps],
            "job_id": job or "",
            "simplefin_connected": state.simplefin_connected,
            "first_name": (state.name or "").split(" ")[0] if state.name else "",
        },
        state=state,
    )


@router.post("/setup/wizard/reset")
def wizard_reset(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
):
    """Dev-only escape hatch. Clears wizard state so the user can
    re-enter the wizard from the welcome step. Does NOT delete
    canonical entities/accounts — those have to be cleaned up from
    their own pages. Useful when iterating during development."""
    reset_state(conn)
    conn.commit()
    return RedirectResponse("/setup/wizard/welcome", status_code=303)

# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Classify directives in an existing ledger for the Import flow.

Walks a source directory's ``main.bean``, loads it with our plugin
allowlist, and classifies every directive into one of three buckets
per ``docs/specs/LEDGER_LAYOUT.md`` §7:

- **Keep** — directive matches our canonical layout; pass through.
- **Transform** — recognized foreign shape we can rewrite; shown to
  user as before/after before applying.
- **Foreign** — unknown directive; kept with a warning unless the
  user explicitly opts to comment it out.

This module does **not** apply transforms. It produces an
``ImportAnalysis`` the Apply step (Part 5c) consumes.

A *blocked* analysis cannot be applied — the ledger declares a
plugin outside the allowlist (§5) or the parser returned fatal
errors. The user resolves the block before Import can proceed.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from beancount import loader
from beancount.core import data as bdata

__all__ = [
    "ALLOWED_PLUGIN_PREFIXES",
    "OWNED_CUSTOM_TARGETS",
    "OWNED_CUSTOM_TYPES",
    "FOREIGN_FAVA_CUSTOM_TYPES",
    "ImportDecision",
    "ImportDecisionGroup",
    "ImportAnalysis",
    "analyze_import",
]


# Core beancount plugins (``beancount.*``) are always allowed — they
# ship with the library itself. ``auto_accounts`` is our one carried
# third-party plugin per LEDGER_LAYOUT.md §5. Anything else blocks.
ALLOWED_PLUGIN_PREFIXES: tuple[str, ...] = (
    "beancount.",
    "beancount_lazy_plugins.auto_accounts",
)


# `custom` directive type names Lamella owns. Each name maps to
# the Connector-owned file the canonical layout writes it to. Any of
# these on import goes straight into the Keep bucket and gets routed
# to the listed file. Sources in code: every `directive_type=` arg to
# `transform.custom_directive.append_custom_directive`, plus the few
# raw f-string writers (`registry/entity_writer`,
# `registry/account_meta_writer`, `receipts/linker`,
# `transform/backfill_hash`).
OWNED_CUSTOM_TARGETS: dict[str, str] = {
    # main.bean (schema version anchor)
    "lamella-ledger-version": "main.bean",
    # connector_links.bean (receipts)
    "receipt-link": "connector_links.bean",
    "receipt-link-hash-backfill": "connector_links.bean",
    "receipt-dismissed": "connector_links.bean",
    "receipt-dismissal-revoked": "connector_links.bean",
    # connector_rules.bean (classification + recurring)
    "classification-rule": "connector_rules.bean",
    "classification-rule-revoked": "connector_rules.bean",
    "recurring-confirmed": "connector_rules.bean",
    "recurring-ignored": "connector_rules.bean",
    "recurring-revoked": "connector_rules.bean",
    # connector_budgets.bean
    "budget": "connector_budgets.bean",
    "budget-revoked": "connector_budgets.bean",
    # connector_config.bean (catch-all for non-secret config + state)
    "setting": "connector_config.bean",
    "setting-unset": "connector_config.bean",
    "paperless-field": "connector_config.bean",
    # entity registry
    "entity": "connector_config.bean",
    "entity-deleted": "connector_config.bean",
    "entity-context": "connector_config.bean",
    # account metadata
    "account-meta": "connector_config.bean",
    "account-meta-deleted": "connector_config.bean",
    "account-description": "connector_config.bean",
    "account-kind": "connector_config.bean",
    "account-kind-cleared": "connector_config.bean",
    # loans
    "loan": "connector_config.bean",
    "loan-balance-anchor": "connector_config.bean",
    "loan-pause": "connector_config.bean",
    "loan-pause-revoked": "connector_config.bean",
    "loan-deleted": "connector_config.bean",
    # vehicles + mileage
    "vehicle": "connector_config.bean",
    "vehicle-deleted": "connector_config.bean",
    "vehicle-yearly-mileage": "connector_config.bean",
    "vehicle-valuation": "connector_config.bean",
    "vehicle-election": "connector_config.bean",
    "vehicle-credit": "connector_config.bean",
    "vehicle-renewal": "connector_config.bean",
    "vehicle-trip-template": "connector_config.bean",
    "vehicle-fuel-entry": "connector_config.bean",
    "mileage-attribution": "connector_config.bean",
    "mileage-attribution-revoked": "connector_config.bean",
    "mileage-trip-meta": "connector_config.bean",
    # properties
    "property": "connector_config.bean",
    "property-deleted": "connector_config.bean",
    "property-valuation": "connector_config.bean",
    # notes
    "note": "connector_config.bean",
    "note-deleted": "connector_config.bean",
    # calendar (day reviews)
    "day-review": "connector_config.bean",
    "day-review-deleted": "connector_config.bean",
    # projects
    "project": "connector_config.bean",
    "project-deleted": "connector_config.bean",
    # balance anchors
    "balance-anchor": "connector_config.bean",
    "balance-anchor-revoked": "connector_config.bean",
    # audit
    "audit-dismissed": "connector_config.bean",
}

OWNED_CUSTOM_TYPES: frozenset[str] = frozenset(OWNED_CUSTOM_TARGETS.keys())


# `custom` directive type names from lazy-beancount's Fava bundle.
# We recognize them as foreign-but-handleable and transform them to
# commented-out form (reversible) since our Fava sidecar runs
# without extensions.
FOREIGN_FAVA_CUSTOM_TYPES: frozenset[str] = frozenset({
    "fava-option",
    "fava-sidebar-link",
    "fava-extension",
})


@dataclass(frozen=True)
class ImportDecision:
    """What to do with one source-ledger directive during Apply."""

    source_file: str
    source_line: int
    directive_label: str  # e.g. "Transaction", "Open", "Custom:fava-extension"
    bucket: str  # "keep" | "transform" | "foreign"
    action: str  # "pass-through" | "comment-out" | "flatten-effective-date" | ...
    reversibility: str  # "reversible" | "lossy"
    reason: str
    target_file: str | None = None  # file in the new canonical layout


@dataclass(frozen=True)
class ImportDecisionGroup:
    """A run of ``ImportDecision`` rows that are interchangeable —
    same file, same directive label, same bucket/action/reason. Used
    by the Import preview UI so a ledger with thousands of identical
    rows (e.g. 1,200 ``custom "account-meta"`` directives) collapses
    into one row per kind. ``lines`` preserves source order so the
    user can still see exactly which lines a group covers."""

    source_file: str
    directive_label: str
    bucket: str
    action: str
    reversibility: str
    reason: str
    target_file: str | None
    lines: tuple[int, ...]

    @property
    def count(self) -> int:
        return len(self.lines)

    @property
    def line_summary(self) -> str:
        """Compact human-readable line spec — ``""`` for none, a single
        number for one, ``"a–b (N)"`` for ≥3 contiguous-or-not, else a
        comma list. Designed to fit a narrow table column."""
        if not self.lines:
            return ""
        if self.count == 1:
            return str(self.lines[0])
        if self.count == 2:
            return f"{self.lines[0]}, {self.lines[1]}"
        return f"{min(self.lines)}–{max(self.lines)} ({self.count})"


@dataclass(frozen=True)
class ImportAnalysis:
    """Result of analyzing a source directory for Import."""

    source_dir: Path
    source_main_bean: Path
    decisions: tuple[ImportDecision, ...] = ()
    plugin_block_reason: str | None = None
    disallowed_plugins: tuple[str, ...] = ()
    parse_errors: tuple[str, ...] = ()

    @property
    def is_blocked(self) -> bool:
        return (
            self.plugin_block_reason is not None
            or bool(self.parse_errors)
        )

    @property
    def count_by_bucket(self) -> dict[str, int]:
        result = {"keep": 0, "transform": 0, "foreign": 0}
        for d in self.decisions:
            result[d.bucket] = result.get(d.bucket, 0) + 1
        return result

    @property
    def decision_groups(self) -> tuple["ImportDecisionGroup", ...]:
        """Decisions collapsed into groups of identical rows. Group
        key: ``(source_file, directive_label, bucket, action,
        reversibility, reason, target_file)`` — everything the preview
        table renders except ``source_line``. Insertion order of the
        key is preserved so the UI shows groups in the order the user
        would have first seen them."""
        groups: dict[tuple, dict] = {}
        for d in self.decisions:
            key = (
                d.source_file,
                d.directive_label,
                d.bucket,
                d.action,
                d.reversibility,
                d.reason,
                d.target_file,
            )
            entry = groups.get(key)
            if entry is None:
                groups[key] = {"lines": [d.source_line]}
            else:
                entry["lines"].append(d.source_line)
        out: list[ImportDecisionGroup] = []
        for key, entry in groups.items():
            (sf, lbl, bk, act, rev, rsn, tgt) = key
            out.append(
                ImportDecisionGroup(
                    source_file=sf,
                    directive_label=lbl,
                    bucket=bk,
                    action=act,
                    reversibility=rev,
                    reason=rsn,
                    target_file=tgt,
                    lines=tuple(entry["lines"]),
                )
            )
        return tuple(out)


def analyze_import(source_dir: Path) -> ImportAnalysis:
    """Walk, parse, and classify the ledger at ``source_dir``.

    Returns an ``ImportAnalysis`` summarizing what the Apply step
    would do. If the analysis is blocked (parse errors, disallowed
    plugin), ``decisions`` is empty and the caller must resolve the
    block before retrying.
    """
    main_bean = source_dir / "main.bean"
    if not main_bean.is_file():
        return ImportAnalysis(
            source_dir=source_dir,
            source_main_bean=main_bean,
            parse_errors=(f"main.bean not found at {main_bean}",),
        )

    try:
        entries, errors, options_map = loader.load_file(str(main_bean))
        from lamella.utils._legacy_meta import normalize_entries
        entries = normalize_entries(entries)
    except Exception as exc:  # loader rarely raises, but be safe
        return ImportAnalysis(
            source_dir=source_dir,
            source_main_bean=main_bean,
            parse_errors=(str(exc),),
        )

    # Plugin allowlist check first — a disallowed plugin blocks the
    # whole import regardless of what else parsed.
    plugin_list = options_map.get("plugin", []) or []
    disallowed = tuple(
        name for (name, _cfg) in plugin_list if not _is_plugin_allowed(name)
    )
    if disallowed:
        msg = (
            "Ledger declares plugin(s) not in the Lamella allowlist: "
            + ", ".join(repr(n) for n in disallowed)
            + ". Remove these plugin directives from main.bean or ask us to add "
              "support before importing."
        )
        return ImportAnalysis(
            source_dir=source_dir,
            source_main_bean=main_bean,
            plugin_block_reason=msg,
            disallowed_plugins=disallowed,
        )

    fatal = _fatal_error_messages(errors)
    if fatal:
        return ImportAnalysis(
            source_dir=source_dir,
            source_main_bean=main_bean,
            parse_errors=tuple(fatal),
        )

    decisions: list[ImportDecision] = []
    for entry in entries:
        meta = getattr(entry, "meta", None) or {}
        src_file = str(meta.get("filename", "") or "")
        src_line = int(meta.get("lineno", 0) or 0)

        # Skip synthetic entries (auto_accounts inserts Opens from a
        # pseudo-source like "<auto_insert_open>") — they aren't in
        # the user's source files, so there's nothing to import.
        if src_file.startswith("<"):
            continue

        directive_label = type(entry).__name__
        if isinstance(entry, bdata.Custom):
            directive_label = f"Custom:{entry.type}"

        bucket, action, reversibility, reason, target = _classify_entry(entry)
        decisions.append(
            ImportDecision(
                source_file=src_file,
                source_line=src_line,
                directive_label=directive_label,
                bucket=bucket,
                action=action,
                reversibility=reversibility,
                reason=reason,
                target_file=target,
            )
        )

    return ImportAnalysis(
        source_dir=source_dir,
        source_main_bean=main_bean,
        decisions=tuple(decisions),
    )


# --- helpers ---------------------------------------------------------------


def _is_plugin_allowed(name: str) -> bool:
    for prefix in ALLOWED_PLUGIN_PREFIXES:
        if name == prefix or name.startswith(prefix + "."):
            return True
        # The prefix "beancount." also matches anything under the
        # core tree like "beancount.plugins.implicit_prices".
        if prefix.endswith(".") and name.startswith(prefix):
            return True
    return False


def _has_bcg_metadata(meta) -> bool:
    if not meta:
        return False
    for key in meta.keys():
        if isinstance(key, str) and key.startswith("lamella-"):
            return True
    return False


def _classify_custom(entry) -> tuple[str, str, str, str, str | None]:
    t = entry.type
    if t in OWNED_CUSTOM_TYPES:
        return (
            "keep",
            "pass-through",
            "reversible",
            f'custom "{t}" is in our schema',
            _target_for_owned_custom(t),
        )
    if t in FOREIGN_FAVA_CUSTOM_TYPES:
        return (
            "transform",
            "comment-out",
            "reversible",
            f'custom "{t}" is a Fava-extension config; commented out '
            "(our Fava sidecar runs without extensions)",
            None,
        )
    return (
        "foreign",
        "pass-through",
        "reversible",
        f'custom "{t}" is not in our schema; kept as-is',
        None,
    )


def _classify_entry(entry) -> tuple[str, str, str, str, str | None]:
    """Classify one entry → (bucket, action, reversibility, reason, target)."""
    if isinstance(entry, bdata.Custom):
        return _classify_custom(entry)

    if isinstance(entry, bdata.Transaction):
        target = (
            "simplefin_transactions.bean"
            if _has_bcg_metadata(entry.meta)
            else "manual_transactions.bean"
        )
        return "keep", "pass-through", "reversible", "transaction", target

    if isinstance(entry, bdata.Open):
        target = (
            "connector_accounts.bean"
            if _has_bcg_metadata(entry.meta)
            else "accounts.bean"
        )
        return "keep", "pass-through", "reversible", "account open", target

    if isinstance(entry, bdata.Close):
        target = (
            "connector_accounts.bean"
            if _has_bcg_metadata(entry.meta)
            else "accounts.bean"
        )
        return "keep", "pass-through", "reversible", "account close", target

    if isinstance(entry, bdata.Commodity):
        return "keep", "pass-through", "reversible", "commodity declaration", "commodities.bean"

    if isinstance(entry, bdata.Price):
        return "keep", "pass-through", "reversible", "price directive", "prices.bean"

    if isinstance(entry, bdata.Event):
        return "keep", "pass-through", "reversible", "event directive", "events.bean"

    if isinstance(entry, bdata.Note):
        return "keep", "pass-through", "reversible", "note directive", None

    if isinstance(entry, bdata.Document):
        return "keep", "pass-through", "reversible", "document directive", None

    if isinstance(entry, bdata.Balance):
        return "keep", "pass-through", "reversible", "balance assertion", None

    if isinstance(entry, bdata.Pad):
        return "keep", "pass-through", "reversible", "pad directive", None

    if isinstance(entry, bdata.Query):
        return "keep", "pass-through", "reversible", "query directive", None

    return (
        "foreign",
        "pass-through",
        "reversible",
        f"unrecognized directive type {type(entry).__name__}",
        None,
    )


def _target_for_owned_custom(custom_type: str) -> str | None:
    """Route an owned custom type to its canonical Connector file."""
    return OWNED_CUSTOM_TARGETS.get(custom_type)


def _fatal_error_messages(errors) -> list[str]:
    """Mirrors ``bootstrap.detection._fatal_error_messages`` — drop
    informational / auto-insert messages; keep real parse failures.
    """
    out: list[str] = []
    for e in errors:
        msg = getattr(e, "message", str(e))
        if "Auto-inserted" in msg:
            continue
        source = getattr(e, "source", None)
        fn = ""
        if isinstance(source, dict):
            fn = source.get("filename", "") or ""
        if isinstance(fn, str) and fn.startswith("<"):
            continue
        out.append(msg)
    return out

# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Apply an ImportAnalysis to transform a ledger into canonical shape.

Implements ``docs/specs/LEDGER_LAYOUT.md`` §9 Apply. Given an analysis
from ``bootstrap.classifier.analyze_import``, applies every
Transform decision to the target ledger directory, ensures the
canonical scaffold files exist, stamps the version marker if
absent, runs bean-check with rollback on failure, and (when a
``seed_conn`` is provided) reconstructs SQLite state from the
imported ledger — with rollback on reconstruct failure per §9
step 8.5.

Scope: operates in-place — ``analysis.source_dir`` is the target
directory. Every .bean file in the target is snapshotted before
any write so a bean-check or reconstruct failure restores the
pre-apply state exactly.

Dry-run: ``plan_import`` produces a ``DryRunReport`` describing
what Apply *would* do without touching disk. This backs the
preview UI and the convergence integration test.
"""
from __future__ import annotations

import difflib
import logging
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import date as _date
from pathlib import Path
from typing import Any, Callable

from .classifier import ImportAnalysis
from .detection import LATEST_LEDGER_VERSION
from .templates import (
    CANONICAL_FILES,
    render_connector_header,
    render_user_header,
)
from .transforms import CommentOutTransform, apply_transforms

__all__ = [
    "ImportApplyError",
    "ImportApplyResult",
    "DryRunReport",
    "TransformPlan",
    "InstallCopyResult",
    "apply_import",
    "plan_import",
    "copy_install_tree",
    "copy_bean_tree",  # deprecated alias; kept for one release
]


# Non-`.bean` files that are part of an install and should travel with
# a skeleton import. Anything outside this allowlist is silently
# skipped — the importer is intentionally conservative about what it
# moves between hosts.
INSTALL_NON_BEAN_GLOBS: tuple[str, ...] = (
    "mileage/*.csv",
    "simplefin_account_map.yml",
    "simplefin_account_map.yaml",
    "importers_config.yml",
    "importers_config.yaml",
    "prices_config.yml",
    "prices_config.yaml",
    "accounts_config.yml",
    "accounts_config.yaml",
    "importers/**/*.py",
    "scripts/**/*.py",
    "scripts/**/*.sh",
)

# Filename patterns that are explicitly skipped even if they match an
# allowlist glob — protect against an accidental token / key drag
# across hosts when someone copies a working install for sharing.
_SECRET_NAME_FRAGMENTS: tuple[str, ...] = (
    "token", "secret", "credentials", "password",
)
_SECRET_SUFFIXES: tuple[str, ...] = (".key", ".pem", ".env")


def _looks_like_secret(path: Path) -> bool:
    name = path.name.lower()
    if any(frag in name for frag in _SECRET_NAME_FRAGMENTS):
        return True
    return any(name.endswith(suf) for suf in _SECRET_SUFFIXES)


@dataclass(frozen=True)
class InstallCopyResult:
    """Result of :func:`copy_install_tree`. Splits the copied files
    into the .bean half (always copied if present) and the
    non-.bean half (allowlist-filtered) so the post-import landing
    page can show "we also brought over your mileage log + SimpleFIN
    map" separately from the ledger files."""
    bean_files: tuple[Path, ...]
    extra_files: tuple[Path, ...]
    skipped_secrets: tuple[Path, ...]

    @property
    def total(self) -> int:
        return len(self.bean_files) + len(self.extra_files)

_LOG = logging.getLogger(__name__)

BeanCheck = Callable[[Path], list[str]]
SeedReader = Callable[[Path], list[Any]]


class ImportApplyError(Exception):
    """Apply refused or failed. Message is user-safe."""


@dataclass(frozen=True)
class TransformPlan:
    """What Apply would do to one source file."""
    path: Path
    directives_commented: int
    unified_diff: str


@dataclass(frozen=True)
class DryRunReport:
    """Planned changes — what ``apply_import`` would do without running."""
    ledger_dir: Path
    files_to_create: tuple[Path, ...]
    transforms_planned: tuple[TransformPlan, ...]
    version_stamp_planned: bool


@dataclass(frozen=True)
class ImportApplyResult:
    ledger_dir: Path
    files_touched: tuple[Path, ...]
    files_created: tuple[Path, ...]
    version_stamped: bool
    seed_ran: bool = False
    seed_reports: tuple[Any, ...] = ()  # tuple[ReconstructReport, ...]


# --- planning (shared by plan_import and apply_import) --------------------


def _group_transforms(analysis: ImportAnalysis) -> dict[str, list[CommentOutTransform]]:
    grouped: dict[str, list[CommentOutTransform]] = defaultdict(list)
    for d in analysis.decisions:
        if d.bucket != "transform" or d.action != "comment-out":
            continue
        if not d.source_file:
            continue
        reason = "foreign-fava" if "fava" in d.reason.lower() else "foreign"
        grouped[d.source_file].append(
            CommentOutTransform(
                line=d.source_line,
                reason=reason,
                tool="ws3-import",
            )
        )
    return grouped


def _compute_version_stamp(main_bean: Path) -> tuple[bool, str | None]:
    """Return ``(stamp_planned, new_text_or_None)``. If main.bean is
    missing or already stamped, no stamp is planned."""
    if not main_bean.is_file():
        return False, None
    text = main_bean.read_text(encoding="utf-8")
    # Check for either prefix — a legacy bcg-ledger-version directive
    # also counts as "stamped". The v1→v2 migration is responsible for
    # rewriting bcg- → lamella- on disk; we don't double-stamp here.
    if "lamella-ledger-version" in text or "bcg-ledger-version" in text:
        return False, None
    return True, _inject_version_stamp(text)


def _plan_scaffold_creations(ledger_dir: Path) -> list[Path]:
    """Which canonical files are missing and would be created."""
    return [ledger_dir / c.name for c in CANONICAL_FILES if not (ledger_dir / c.name).exists()]


def plan_import(
    ledger_dir: Path,
    analysis: ImportAnalysis,
    *,
    on: _date | None = None,
) -> DryRunReport:
    """Compute what :func:`apply_import` would do without touching disk.

    Backs the §9 "dry-run mode for this same flow" requirement and the
    preview UI. Raises ``ImportApplyError`` on the same preconditions
    as ``apply_import`` (blocked analysis, missing directory)."""
    if analysis.is_blocked:
        raise ImportApplyError(
            "cannot plan a blocked analysis; resolve parse errors or "
            "disallowed plugins first"
        )
    if not ledger_dir.is_dir():
        raise ImportApplyError(
            f"ledger directory does not exist: {ledger_dir}"
        )

    transforms_by_file = _group_transforms(analysis)
    plans: list[TransformPlan] = []
    for src_path_str, ts in transforms_by_file.items():
        src_path = Path(src_path_str)
        if not src_path.is_file():
            continue
        original = src_path.read_text(encoding="utf-8")
        new_text = apply_transforms(original, ts, on=on)
        if new_text == original:
            continue
        diff = "".join(
            difflib.unified_diff(
                original.splitlines(keepends=True),
                new_text.splitlines(keepends=True),
                fromfile=str(src_path),
                tofile=str(src_path),
                n=2,
            )
        )
        plans.append(
            TransformPlan(
                path=src_path,
                directives_commented=len(ts),
                unified_diff=diff,
            )
        )

    to_create = _plan_scaffold_creations(ledger_dir)
    stamp_planned, _ = _compute_version_stamp(ledger_dir / "main.bean")

    return DryRunReport(
        ledger_dir=ledger_dir,
        files_to_create=tuple(to_create),
        transforms_planned=tuple(plans),
        version_stamp_planned=stamp_planned,
    )


# --- execution ------------------------------------------------------------


def apply_import(
    ledger_dir: Path,
    analysis: ImportAnalysis,
    *,
    on: _date | None = None,
    bean_check: BeanCheck | None = None,
    seed_conn: sqlite3.Connection | None = None,
    seed_reader: SeedReader | None = None,
    dry_run: bool = False,
) -> ImportApplyResult | DryRunReport:
    """Apply ``analysis``'s Transform decisions to ``ledger_dir``.

    Raises ``ImportApplyError`` if the analysis is blocked, the
    directory doesn't exist, bean-check fails after write, or (when
    ``seed_conn`` is given) the post-apply reconstruct pass fails.
    On any failure, every file touched is restored from its
    pre-apply snapshot and every newly-created file is deleted. If
    seed fails, state tables populated by the failed pass are
    wiped so the DB stays consistent with the rolled-back ledger.

    Args:
        seed_conn: if provided, after bean-check passes the function
            runs ``transform.reconstruct.run_all(force=True)`` against
            the imported ledger to hydrate SQLite state. Failure
            triggers §9 step 8.5 rollback.
        seed_reader: override the default ledger parser (defaults to
            ``beancount.loader.load_file``). Tests inject a fake to
            drive the reconstruct-failure path.
        dry_run: return a ``DryRunReport`` describing planned changes
            without writing anything. ``bean_check`` / ``seed_conn``
            are ignored when ``dry_run`` is true.
    """
    if dry_run:
        return plan_import(ledger_dir, analysis, on=on)

    if analysis.is_blocked:
        raise ImportApplyError(
            "cannot apply a blocked analysis; resolve parse errors or "
            "disallowed plugins first"
        )

    if not ledger_dir.is_dir():
        raise ImportApplyError(
            f"ledger directory does not exist: {ledger_dir}"
        )

    # Phase 3 of /setup/recovery: replace the inline snapshot/restore
    # pair with `with_bean_snapshot` so import-apply, schema-migration
    # apply, and recovery heal actions all share one tested envelope.
    # Declared path set covers every existing .bean file plus every
    # canonical scaffold file we might create plus every transform
    # source. Files outside this set won't be restored, so we have
    # to enumerate up front.
    from lamella.features.recovery.snapshot import with_bean_snapshot

    declared_paths: list[Path] = list(ledger_dir.glob("*.bean"))
    declared_set: set[Path] = set(declared_paths)
    for cfile in CANONICAL_FILES:
        p = ledger_dir / cfile.name
        if p not in declared_set:
            declared_paths.append(p)
            declared_set.add(p)
    transforms_by_file = _group_transforms(analysis)
    for src_path_str in transforms_by_file:
        sp = Path(src_path_str)
        if sp not in declared_set:
            declared_paths.append(sp)
            declared_set.add(sp)

    created: list[Path] = []
    touched: list[Path] = []
    version_stamped = False
    seed_ran = False
    seed_reports: tuple[Any, ...] = ()

    with with_bean_snapshot(declared_paths) as snap:
        # 1. Apply transforms file by file (group computed above for
        #    declared-path enumeration).
        for src_path_str, ts in transforms_by_file.items():
            src_path = Path(src_path_str)
            if not src_path.is_file():
                _LOG.warning("transform source file missing: %s", src_path)
                continue
            original = src_path.read_text(encoding="utf-8")
            new_text = apply_transforms(original, ts, on=on)
            if new_text != original:
                src_path.write_text(new_text, encoding="utf-8")
                touched.append(src_path)
                snap.add_touched(src_path)

        # 2. Ensure canonical scaffold files exist in the ledger dir.
        for cfile in CANONICAL_FILES:
            path = ledger_dir / cfile.name
            if path.exists():
                continue
            content = (
                render_user_header(cfile.name, on=on)
                if cfile.owner == "user"
                else render_connector_header(cfile.name, on=on)
            )
            path.write_text(content, encoding="utf-8")
            created.append(path)
            snap.add_touched(path)

        # 3. Stamp lamella-ledger-version in main.bean if absent.
        main_bean = ledger_dir / "main.bean"
        stamp_planned, new_main = _compute_version_stamp(main_bean)
        if stamp_planned and new_main is not None:
            main_bean.write_text(new_main, encoding="utf-8")
            if main_bean not in touched:
                touched.append(main_bean)
            snap.add_touched(main_bean)
            version_stamped = True

        # 4. Run bean-check. Failure rolls back everything via the
        #    context manager. We raise ImportApplyError (not
        #    BeanSnapshotCheckError) to preserve the public API
        #    callers depend on.
        if bean_check is not None:
            errors = bean_check(main_bean)
            if errors:
                detail = "; ".join(errors[:3])
                if len(errors) > 3:
                    detail += f" ... ({len(errors) - 3} more)"
                raise ImportApplyError(f"bean-check failed after apply: {detail}")

        # 5. §9 step 8 — seed SQLite state from the imported ledger.
        #    If this throws, step 8.5 kicks in: state tables wiped,
        #    snapshot restored.
        if seed_conn is not None:
            try:
                reports = _seed_reconstruct(
                    seed_conn, main_bean, reader=seed_reader
                )
            except Exception as exc:
                _wipe_all_state_tables(seed_conn)
                raise ImportApplyError(
                    "Ledger is valid beancount (bean-check passed) but "
                    "Lamella cannot interpret some of it — the "
                    "import was not applied. Edit the flagged directives "
                    "and retry, or file a bug with the snippets attached. "
                    f"Detail: {exc}"
                ) from exc
            seed_ran = True
            seed_reports = tuple(reports)

    _LOG.info(
        "import applied to %s: touched=%d created=%d version_stamped=%s seed_ran=%s",
        ledger_dir,
        len(touched),
        len(created),
        version_stamped,
        seed_ran,
    )
    return ImportApplyResult(
        ledger_dir=ledger_dir,
        files_touched=tuple(touched),
        files_created=tuple(created),
        version_stamped=version_stamped,
        seed_ran=seed_ran,
        seed_reports=seed_reports,
    )


# --- helpers --------------------------------------------------------------


def copy_install_tree(src: Path, dst: Path) -> InstallCopyResult:
    """Mirror an install from ``src`` into ``dst``.

    Copies every ``.bean`` file unconditionally (preserving the
    relative directory structure) and every non-``.bean`` file that
    matches :data:`INSTALL_NON_BEAN_GLOBS` — mileage CSVs, SimpleFIN
    account map, importer configs, prices configs, custom importer
    scripts. Anything else under ``src`` is silently skipped.

    Files whose names look secret-shaped (``*token*``, ``*secret*``,
    ``*.key``, ``*.env``, …) are explicitly skipped even if they
    match an allowlist glob — a working install carries credentials
    we don't want dragged across hosts. Skipped secrets are
    surfaced in the result so the caller can show "we left N
    secret-shaped files behind."

    Refuses to overwrite when ``dst`` already has a top-level ``.bean``
    file — main.bean / connector_*.bean is where ledger identity
    lives, and silently merging two ledgers there is the failure mode
    the guard exists for. Subdirectories of ``dst`` are not part of
    the check (so a source nested inside an empty ``dst`` is allowed).

    Refuses outright when ``dst`` would land inside ``src`` — that
    configuration would have the copy write into the source tree
    while we're still walking it.
    """
    if not src.is_dir():
        raise FileNotFoundError(f"source directory does not exist: {src}")

    src_resolved = src.resolve()
    # Resolve dst's eventual location even if it doesn't exist yet
    # (mkdir hasn't run); we only need it to detect containment.
    if dst.exists():
        dst_resolved = dst.resolve()
    else:
        parent = dst.parent.resolve() if dst.parent.exists() else dst.parent
        dst_resolved = parent / dst.name
    if dst_resolved == src_resolved:
        # Same directory — caller should have skipped the copy step;
        # treat as a no-op rather than self-copy.
        return InstallCopyResult(bean_files=(), extra_files=(), skipped_secrets=())
    if dst_resolved.is_relative_to(src_resolved):
        raise ValueError(
            f"destination {dst} is inside source {src}; refusing to "
            "copy a directory into its own subtree"
        )

    dst.mkdir(parents=True, exist_ok=True)
    existing = [p for p in dst.glob("*.bean") if p.is_file()]
    if existing:
        raise FileExistsError(
            f"destination already has {len(existing)} top-level .bean "
            f"file(s); refusing to overwrite ({dst})"
        )

    bean_files: list[Path] = []
    extra_files: list[Path] = []
    skipped_secrets: list[Path] = []

    def _copy_one(src_file: Path) -> Path:
        rel = src_file.relative_to(src)
        dst_file = dst / rel
        dst_file.parent.mkdir(parents=True, exist_ok=True)
        dst_file.write_bytes(src_file.read_bytes())
        return dst_file

    # 1. Every .bean file, no allowlist gate.
    for src_file in sorted(src.rglob("*.bean")):
        if not src_file.is_file():
            continue
        bean_files.append(_copy_one(src_file))

    # 2. Allowlisted non-.bean files. Dedup via a set keyed on the
    # absolute resolved path so overlapping globs don't double-copy.
    seen: set[Path] = set()
    for pattern in INSTALL_NON_BEAN_GLOBS:
        for src_file in sorted(src.glob(pattern)):
            if not src_file.is_file():
                continue
            if src_file.suffix == ".bean":
                continue  # already handled in pass 1
            resolved = src_file.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            if _looks_like_secret(src_file):
                skipped_secrets.append(src_file.relative_to(src))
                continue
            extra_files.append(_copy_one(src_file))

    return InstallCopyResult(
        bean_files=tuple(bean_files),
        extra_files=tuple(extra_files),
        skipped_secrets=tuple(skipped_secrets),
    )


def copy_bean_tree(src: Path, dst: Path) -> list[Path]:
    """Deprecated thin wrapper around :func:`copy_install_tree`.

    Returns only the ``.bean`` files copied — preserves the old
    return shape so existing callers (and the test suite) keep
    working. New callers should use ``copy_install_tree`` directly
    so they can surface mileage / config / importer files in the
    post-import UI.
    """
    result = copy_install_tree(src, dst)
    return list(result.bean_files)




def _default_seed_reader(main_bean: Path) -> list[Any]:
    from beancount import loader
    from lamella.utils._legacy_meta import normalize_entries

    entries, _errors, _opts = loader.load_file(str(main_bean))
    return normalize_entries(entries)


def _seed_reconstruct(
    conn: sqlite3.Connection,
    main_bean: Path,
    *,
    reader: SeedReader | None = None,
) -> list[Any]:
    """Load the imported ledger and run every registered reconstruct
    pass with ``force=True`` so state is rebuilt from the ledger even
    if the DB already has rows from a prior scaffold or partial import."""
    from lamella.core.transform import reconstruct as _recon

    _recon._import_all_steps()
    parse = reader or _default_seed_reader
    entries = parse(main_bean)
    return _recon.run_all(conn, entries, force=True)


def _wipe_all_state_tables(conn: sqlite3.Connection) -> None:
    """Rollback companion to seed failure: empty every reconstruct
    state table so the DB matches the rolled-back ledger. Caches are
    left alone — they repopulate naturally."""
    from lamella.core.transform import reconstruct as _recon

    _recon._import_all_steps()
    tables: set[str] = set()
    for p in _recon.registered_passes():
        tables.update(p.state_tables)
    if not tables:
        return
    try:
        for table in sorted(tables):
            try:
                conn.execute(f"DELETE FROM {table}")
            except sqlite3.OperationalError:
                continue
        conn.commit()
    except Exception:  # noqa: BLE001
        _LOG.exception("state-table wipe on seed-failure rollback failed")
        try:
            conn.rollback()
        except Exception:
            pass


def _inject_version_stamp(main_bean_text: str) -> str:
    """Insert a ``custom "lamella-ledger-version" "<LATEST>"`` directive.

    Placement: after the last leading option/plugin/comment line,
    before the first include / directive / blank gap that follows.
    The date is the fixed epoch ``2026-01-01`` (deterministic, not
    dependent on install date).
    """
    lines = main_bean_text.splitlines(keepends=True)
    insert_at = 0
    for i, line in enumerate(lines):
        stripped = line.strip()
        if (
            stripped == ""
            or stripped.startswith(";")
            or stripped.startswith("option")
            or stripped.startswith("plugin")
        ):
            # Options and plugins belong before the version stamp;
            # trailing blanks get absorbed on either side.
            if stripped.startswith("option") or stripped.startswith("plugin"):
                insert_at = i + 1
            continue
        # Hit non-header content — stop scanning.
        break

    stamp = (
        f"\n;; Schema version marker — written by Lamella on "
        f"the gentle path. Do not edit.\n"
        f'2026-01-01 custom "lamella-ledger-version" "{LATEST_LEDGER_VERSION}"\n\n'
    )
    return "".join(lines[:insert_at]) + stamp + "".join(lines[insert_at:])

# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Ledger backup + restore.

Tarballs every `.bean` file under the ledger directory (plus relevant
config files it stores alongside) into `$LAMELLA_DATA_DIR/backups/
ledger/{timestamp}.tar.gz`. Safe to call anytime; intended as the
pre-flight before any cross-file rewrite.

Restore:
  1. Snapshot CURRENT ledger state first (never lose what the user has
     now — create a safety tarball automatically).
  2. Open the restore archive, extract into a temp dir.
  3. Walk current ledger dir, delete/replace files based on the
     archive's layout.
  4. Run bean-check. If it fails, roll back to the safety snapshot.

We keep every backup on disk until the user deletes one — no automatic
pruning. Small text files gzip to a few MB at most; disk cost is
negligible.
"""
from __future__ import annotations

import logging
import shutil
import tarfile
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from lamella.core.fs import UnsafePathError, validate_safe_path

log = logging.getLogger(__name__)


# Files we include in the backup. Always all .bean, plus yaml configs
# the ledger references (accounts_config.yml, etc.).
_INCLUDE_EXTS = (".bean", ".yml", ".yaml")

# Files/dirs we never tarball (binary caches, logs, etc.).
_EXCLUDE_DIR_NAMES = frozenset({
    ".git", "__pycache__", "logs", "beancount_import_data",
    "beancount_import_output",
})


@dataclass(frozen=True)
class BackupInfo:
    filename: str
    path: Path
    size_bytes: int
    created_at: datetime
    label: str | None


def _backups_dir(data_dir: Path) -> Path:
    d = data_dir / "backups" / "ledger"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _collect_files(ledger_dir: Path) -> list[Path]:
    """Every .bean / .yml file under ledger_dir, skipping excluded dirs."""
    out: list[Path] = []
    for path in ledger_dir.rglob("*"):
        if not path.is_file():
            continue
        # Skip excluded dirs anywhere in the path.
        if any(part in _EXCLUDE_DIR_NAMES for part in path.parts):
            continue
        if path.suffix.lower() in _INCLUDE_EXTS:
            out.append(path)
    return sorted(out)


def create_backup(
    *,
    ledger_dir: Path,
    data_dir: Path,
    label: str | None = None,
) -> BackupInfo:
    """Tarball every .bean/.yml under ledger_dir into
    $data_dir/backups/ledger/{timestamp}.tar.gz.

    Returns info about the created backup. `label` (optional) gets
    encoded in the filename so the user can see at a glance why the
    backup was taken (e.g. "pre-slug-merge-WidgetCo-into-CncInc").
    """
    ledger_dir = ledger_dir.resolve()
    if not ledger_dir.is_dir():
        raise ValueError(f"ledger_dir {ledger_dir} is not a directory")
    backups_dir = _backups_dir(data_dir)
    stamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    safe_label = ""
    if label:
        safe_label = "-" + "".join(
            c if c.isalnum() or c in "-_" else "-" for c in label
        )[:60]
    filename = f"{stamp}{safe_label}.tar.gz"
    tar_path = backups_dir / filename

    files = _collect_files(ledger_dir)
    with tarfile.open(tar_path, "w:gz") as tar:
        for f in files:
            arcname = f.relative_to(ledger_dir)
            tar.add(f, arcname=str(arcname))

    size = tar_path.stat().st_size
    log.info("Created backup %s (%d bytes, %d files)", tar_path, size, len(files))
    return BackupInfo(
        filename=filename,
        path=tar_path,
        size_bytes=size,
        created_at=datetime.now(UTC),
        label=label,
    )


def list_backups(data_dir: Path) -> list[BackupInfo]:
    backups_dir = _backups_dir(data_dir)
    out: list[BackupInfo] = []
    for p in sorted(backups_dir.glob("*.tar.gz"), reverse=True):
        stem = p.stem  # "YYYYMMDD-HHMMSS" or "YYYYMMDD-HHMMSS-label"
        dash_idx = stem.find("-", 8)  # after the date
        label = None
        try:
            if dash_idx > 0 and len(stem) > dash_idx + 7:  # past "-HHMMSS"
                label = stem[dash_idx + 7:].lstrip("-") or None
        except Exception:
            label = None
        try:
            dt = datetime.strptime(stem[:15], "%Y%m%d-%H%M%S").replace(tzinfo=UTC)
        except ValueError:
            dt = datetime.fromtimestamp(p.stat().st_mtime, tz=UTC)
        out.append(BackupInfo(
            filename=p.name,
            path=p,
            size_bytes=p.stat().st_size,
            created_at=dt,
            label=label,
        ))
    return out


def delete_backup(data_dir: Path, filename: str) -> bool:
    backups_dir = _backups_dir(data_dir)
    # ADR-0030: validate before any FS operation. An escape attempt
    # returns False so callers see the same "not found" outcome as a
    # missing file (consistent contract).
    try:
        target = validate_safe_path(filename, allowed_roots=[backups_dir])
    except UnsafePathError:
        return False
    if not target.exists() or not target.is_file():
        return False
    target.unlink()
    return True


def restore_backup(
    *,
    ledger_dir: Path,
    data_dir: Path,
    filename: str,
    bean_check: callable | None = None,
) -> dict:
    """Extract `filename` into `ledger_dir`, replacing existing files.

    Steps:
      1. Create an automatic "pre-restore" safety snapshot of the
         current state so this operation is itself reversible.
      2. Extract the archive into a temp dir.
      3. Replace files in ledger_dir with the archive contents.
      4. Run bean_check (callable takes no args; raises on failure).
         If it fails, roll back from the safety snapshot.

    Returns a dict with counts: {files_replaced, files_added, safety_backup}.
    """
    ledger_dir = ledger_dir.resolve()
    backups_dir = _backups_dir(data_dir)
    # ADR-0030: validate the user-supplied filename resolves inside the
    # backups dir before we hand it to tarfile.open. Reject path
    # traversal, absolute paths, and symlinks.
    try:
        archive = validate_safe_path(filename, allowed_roots=[backups_dir])
    except UnsafePathError as exc:
        raise FileNotFoundError(filename) from exc
    if not archive.exists():
        raise FileNotFoundError(filename)

    safety = create_backup(
        ledger_dir=ledger_dir, data_dir=data_dir, label="pre-restore"
    )

    with tempfile.TemporaryDirectory() as td:
        temp_root = Path(td)
        with tarfile.open(archive, "r:gz") as tar:
            # Python 3.12 deprecates the default extraction filter — use "data"
            # to stay safe against path-traversal archives.
            tar.extractall(path=temp_root, filter="data")

        replaced = 0
        added = 0
        for extracted in temp_root.rglob("*"):
            if not extracted.is_file():
                continue
            rel = extracted.relative_to(temp_root)
            target = ledger_dir / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists():
                replaced += 1
            else:
                added += 1
            shutil.copy2(extracted, target)

    # Optional validation.
    if bean_check is not None:
        try:
            bean_check()
        except Exception:
            # Roll back to the safety snapshot.
            log.warning("bean-check failed after restore — rolling back to %s",
                        safety.filename)
            restore_backup(
                ledger_dir=ledger_dir,
                data_dir=data_dir,
                filename=safety.filename,
                bean_check=None,  # skip re-check; we're restoring to a known-good state
            )
            raise

    return {
        "files_replaced": replaced,
        "files_added": added,
        "safety_backup": safety.filename,
    }

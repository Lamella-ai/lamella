# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Imported-file archive surface — ADR-0060.

* ``GET /imports`` — paginated list of every archived intake file
  with format / size / hash / imported_at / count of staged rows
  referencing it.
* ``GET /imports/{file_id}`` — file detail. Preview header (first
  ~50 rows of CSV-shaped content) plus the staged rows that point
  back at this file via ``source_ref.file_id``.
* ``GET /imports/{file_id}/download`` — serve the archived bytes
  back to the user. Useful when they want to re-open the original
  in their preferred tool. Read-only; never proxies a writeback.

The routes are intentionally thin — listing + read. The archive is
authoritative on disk; mutating endpoints (rename, prune) are
deferred follow-ups.
"""
from __future__ import annotations

import json
import sqlite3

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse

from lamella.core.config import Settings
from lamella.features.import_.archive import (
    get_archived_path,
    list_archived,
)
from lamella.web.deps import get_db, get_settings


router = APIRouter()


@router.get("/imports", response_class=HTMLResponse)
def imports_index(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
):
    """Newest-first list of every archived intake file. The
    ``staged_count`` column counts staged rows whose
    ``source_ref.file_id`` matches each archive row — the user's
    visual answer to "how many txns did this CSV produce?"
    """
    archived = list_archived(conn, limit=500)
    # Per-file staged-row counts. SQLite has no JSON path index here,
    # so we do a single scan that materializes file_id from
    # source_ref's JSON body. Cheap relative to the row counts we
    # serve from this surface (≤ 500 archive rows × constant work).
    counts: dict[int, int] = {}
    for row in conn.execute(
        "SELECT source_ref FROM staged_transactions"
    ):
        try:
            ref = json.loads(row["source_ref"])
        except (TypeError, ValueError):
            continue
        fid = ref.get("file_id") if isinstance(ref, dict) else None
        if isinstance(fid, int):
            counts[fid] = counts.get(fid, 0) + 1
    items = []
    for af in archived:
        items.append({
            "file_id": af.file_id,
            "original_filename": af.original_filename,
            "archived_path": af.archived_path,
            "content_sha256": af.content_sha256,
            "source_format": af.source_format,
            "byte_size": af.byte_size,
            "imported_at": af.imported_at,
            "staged_count": counts.get(af.file_id, 0),
        })
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "imports_archive.html",
        {"items": items, "total": len(items)},
    )


@router.get("/imports/{file_id}", response_class=HTMLResponse)
def imports_detail(
    request: Request,
    file_id: int,
    conn: sqlite3.Connection = Depends(get_db),
    settings: Settings = Depends(get_settings),
):
    """File detail — header info + the staged rows pointing at it.

    The preview is best-effort: a CSV / paste file shows a
    table-rendered first chunk; binary formats (XLSX, ODS) just
    show the metadata + download link. The "re-open in your tool"
    workflow is via the download endpoint."""
    row = conn.execute(
        "SELECT id, original_filename, archived_path, content_sha256, "
        "source_format, byte_size, imported_at, imported_by, notes "
        "FROM imported_files WHERE id = ?",
        (file_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="archive row not found")

    file_meta = dict(row)
    abs_path = settings.ledger_dir / row["archived_path"]
    file_meta["exists_on_disk"] = abs_path.exists()

    preview_rows: list[list[str]] = []
    preview_truncated = False
    if file_meta["exists_on_disk"] and row["source_format"] in (
        "csv", "paste",
    ):
        try:
            text = abs_path.read_text(encoding="utf-8", errors="replace")
            lines = text.splitlines()
            for line in lines[:50]:
                # Naive split — the table is for visual scan, not
                # parsing fidelity. The actual parsing happened at
                # ingest time.
                preview_rows.append(line.split(","))
            if len(lines) > 50:
                preview_truncated = True
        except OSError:
            preview_rows = []

    staged_rows = conn.execute(
        """
        SELECT id, source, source_ref, posting_date, amount, currency,
               payee, description, status
          FROM staged_transactions
         ORDER BY posting_date DESC, id DESC
         LIMIT 1000
        """,
    ).fetchall()
    matching: list[dict] = []
    for s in staged_rows:
        try:
            ref = json.loads(s["source_ref"])
        except (TypeError, ValueError):
            continue
        if not isinstance(ref, dict):
            continue
        if ref.get("file_id") != file_id:
            continue
        matching.append({
            "id": int(s["id"]),
            "source": s["source"],
            "row": ref.get("row"),
            "sheet": ref.get("sheet"),
            "posting_date": s["posting_date"],
            "amount": s["amount"],
            "currency": s["currency"],
            "payee": s["payee"],
            "description": s["description"],
            "status": s["status"],
        })
    matching.sort(key=lambda x: (x.get("sheet") or "", x.get("row") or 0))

    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "imports_archive_detail.html",
        {
            "file": file_meta,
            "preview_rows": preview_rows,
            "preview_truncated": preview_truncated,
            "staged_rows": matching,
            "staged_count": len(matching),
        },
    )


@router.get("/imports/{file_id}/download")
def imports_download(
    file_id: int,
    conn: sqlite3.Connection = Depends(get_db),
    settings: Settings = Depends(get_settings),
):
    """Serve the archived file bytes verbatim. The user gets the
    same content they uploaded — useful for re-opening the original
    in their tool of choice.

    Read-only by design; this surface never accepts uploads or
    modifications."""
    abs_path = get_archived_path(
        conn, ledger_dir=settings.ledger_dir, file_id=file_id,
    )
    if abs_path is None or not abs_path.exists():
        raise HTTPException(status_code=404, detail="archive file not found")
    row = conn.execute(
        "SELECT original_filename FROM imported_files WHERE id = ?",
        (file_id,),
    ).fetchone()
    download_name = (
        row["original_filename"] if row else abs_path.name
    )
    return FileResponse(
        path=abs_path,
        filename=download_name,
        media_type="application/octet-stream",
    )

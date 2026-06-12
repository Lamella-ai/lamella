-- Migration 063 — ADR-0060 imported-file archive manifest.
--
-- Cache of every tabular file (CSV, OFX, QIF, IIF, XLSX, ODS,
-- paste) imported into Lamella. The actual files live under
-- <ledger_dir>/imports/ and are the source of truth (ADR-0001);
-- this table is a reconstructible index over them.
--
-- See `step25:imported_files` for the reconstruct pass that
-- rebuilds this table from the directory.

CREATE TABLE IF NOT EXISTS imported_files (
    -- The 5-digit zero-padded prefix on the archived filename
    -- (e.g., "00001-bankone-statement-2026-04.csv" → id 1). Stable
    -- identity used by every staged row's source_ref to point back
    -- at the artifact.
    id                 INTEGER PRIMARY KEY,

    -- Original filename as the user provided it. Used in the
    -- archive filename after the numeric prefix and surfaced in
    -- the UI so the user recognizes their file.
    original_filename  TEXT NOT NULL,

    -- Path on disk relative to ledger_dir. e.g.
    -- "imports/00001-bankone-statement-2026-04.csv". Unique so a
    -- collision (race / restore from backup) can't double-create.
    archived_path      TEXT NOT NULL UNIQUE,

    -- SHA-256 hex digest of the file content. Drives full-file
    -- dedup at archive time: same bytes → reuse the existing
    -- file_id rather than archiving twice.
    content_sha256     TEXT NOT NULL,

    -- File class: 'csv' | 'ofx' | 'qif' | 'iif' | 'xlsx' | 'ods'
    -- | 'paste'. Free-text on purpose so future formats land
    -- without a schema change; the archive helper validates
    -- against an allowlist at insert time.
    source_format      TEXT NOT NULL,

    byte_size          INTEGER NOT NULL,

    -- ISO 8601 UTC, TZ-aware. ADR-0023 timestamp convention.
    imported_at        TEXT NOT NULL DEFAULT (datetime('now')),

    -- Auth user id when ADR-0050 auth is enabled, NULL otherwise.
    -- Stays NULL after reconstruct (the directory walk has no way
    -- to recover this; live drift is tolerated).
    imported_by        TEXT,

    -- Free-form user note (optional). User can annotate "got this
    -- from BankOne support after the corrupted statement" etc.
    notes              TEXT
);

-- content_sha256 is the natural full-file dedup index. Unique so
-- two rows can't claim the same hash (would imply two distinct
-- file_ids for the same bytes — corruption).
CREATE UNIQUE INDEX IF NOT EXISTS imported_files_sha256_idx
    ON imported_files(content_sha256);

CREATE INDEX IF NOT EXISTS imported_files_format_idx
    ON imported_files(source_format);

CREATE INDEX IF NOT EXISTS imported_files_imported_at_idx
    ON imported_files(imported_at);

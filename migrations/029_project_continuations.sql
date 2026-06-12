-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- Project continuations.
--
-- The fence-project pattern: start July 2025, pause in September,
-- restart in November as a NEW project. The second project
-- references the first via previous_project_slug so totals can
-- roll across both, and the classify prompt can surface the
-- continuation relationship as context ("this fence project is
-- a continuation of fence-2025-summer which ran Jul–Sep 2025").

ALTER TABLE projects ADD COLUMN previous_project_slug TEXT;

CREATE INDEX IF NOT EXISTS projects_previous_idx
    ON projects (previous_project_slug);

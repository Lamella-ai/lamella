-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- 062 — UNIQUE (entity_slug, slug) indexes on scoped tables (ADR-0031).
--
-- ADR-0031 requires that slug uniqueness is scoped to an entity: the same
-- slug may appear under different entities (EntityA:car, EntityB:car are
-- both valid), but the composite pair (entity_slug, slug) must be unique
-- within each table.
--
-- Four tables receive this constraint. Forward-only per ADR-0026; no
-- rollback DDL is needed.

CREATE UNIQUE INDEX IF NOT EXISTS vehicles_entity_slug_uidx
    ON vehicles (entity_slug, slug);

CREATE UNIQUE INDEX IF NOT EXISTS properties_entity_slug_uidx
    ON properties (entity_slug, slug);

CREATE UNIQUE INDEX IF NOT EXISTS loans_entity_slug_uidx
    ON loans (entity_slug, slug);

CREATE UNIQUE INDEX IF NOT EXISTS projects_entity_slug_uidx
    ON projects (entity_slug, slug);

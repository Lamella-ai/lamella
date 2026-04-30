-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- User-written rich context for an entity, surfaced to the AI
-- classifier on every call where the card binding resolves to
-- this entity.
--
-- Motivating example: when classifying a Acme charge, the
-- classifier knows the entity is Acme. But a free-form
-- paragraph like "Acme LLC, formed 2008, handles widget line
-- merchandise. Income via eBay + Shopify + direct. Expenses
-- heavy on shipping, supplies, advertising. Intercompany with
-- Personal via reimbursement entries." gives the AI a much
-- richer picture than the bare entity name alone.
--
-- Stored on entities.classify_context as plain markdown-ish text.
-- Reconstruct via a `custom "entity-context"` directive in
-- connector_config.bean (future work).

ALTER TABLE entities ADD COLUMN classify_context TEXT;

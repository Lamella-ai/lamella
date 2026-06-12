-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- Capture the actual prompt sent to the AI so the user can audit
-- what the model saw when making a classification decision.
--
-- Before this: the ai_decisions table stored the response, the
-- model, token counts, and a prompt_hash (for cache dedup) — but
-- not the prompt itself. That made it impossible to answer "what
-- did the AI actually see?" when debugging why a classification
-- came back weak or wrong.
--
-- After this: prompt_system + prompt_user hold the exact strings
-- that went into the chat call. They're nullable so existing rows
-- stay valid and old decisions just don't have prompt-text to
-- render (the viewer falls back to "(pre-capture era)").

ALTER TABLE ai_decisions ADD COLUMN prompt_system TEXT;
ALTER TABLE ai_decisions ADD COLUMN prompt_user TEXT;

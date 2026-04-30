# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import sqlite3
from typing import AsyncIterator

from fastapi import Depends, HTTPException, Request

from lamella.features.ai_cascade.service import AIService
from lamella.core.beancount_io import LedgerReader
from lamella.core.config import Settings
from lamella.features.notes.service import NoteService
from lamella.adapters.paperless.client import PaperlessClient
from lamella.features.review_queue.service import ReviewService
from lamella.features.rules.overrides import OverrideWriter
from lamella.features.rules.scanner import FixmeScanner
from lamella.features.rules.service import RuleService
from lamella.core.settings.store import AppSettingsStore


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


def get_db(request: Request) -> sqlite3.Connection:
    return request.app.state.db


def get_ledger_reader(request: Request) -> LedgerReader:
    return request.app.state.ledger_reader


def get_note_service(
    conn: sqlite3.Connection = Depends(get_db),
) -> NoteService:
    return NoteService(conn)


def get_review_service(
    conn: sqlite3.Connection = Depends(get_db),
) -> ReviewService:
    return ReviewService(conn)


def get_rule_service(
    conn: sqlite3.Connection = Depends(get_db),
) -> RuleService:
    return RuleService(conn)


def get_app_settings_store(
    conn: sqlite3.Connection = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> AppSettingsStore:
    return AppSettingsStore(
        conn,
        connector_config_path=settings.connector_config_path,
        main_bean_path=settings.ledger_main,
    )


def get_ai_service(
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
) -> AIService:
    return AIService(settings=settings, conn=conn)


def get_fixme_scanner(
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
    reviews: ReviewService = Depends(get_review_service),
    rules: RuleService = Depends(get_rule_service),
    conn: sqlite3.Connection = Depends(get_db),
) -> FixmeScanner:
    return FixmeScanner(
        reader=reader,
        reviews=reviews,
        rules=rules,
        override_writer=OverrideWriter(
            main_bean=settings.ledger_main,
            overrides=settings.connector_overrides_path,
            conn=conn,
        ),
    )


async def get_paperless(
    settings: Settings = Depends(get_settings),
) -> AsyncIterator[PaperlessClient]:
    if not settings.paperless_configured:
        raise HTTPException(
            status_code=503,
            detail="Paperless is not configured (set PAPERLESS_URL + PAPERLESS_API_TOKEN).",
        )
    client = PaperlessClient(
        base_url=settings.paperless_url,  # type: ignore[arg-type]
        api_token=settings.paperless_api_token.get_secret_value(),  # type: ignore[union-attr]
        extra_headers=settings.paperless_extra_headers(),
    )
    try:
        yield client
    finally:
        await client.aclose()


__all__ = [
    "get_ai_service",
    "get_app_settings_store",
    "get_db",
    "get_fixme_scanner",
    "get_ledger_reader",
    "get_note_service",
    "get_paperless",
    "get_review_service",
    "get_rule_service",
    "get_settings",
]

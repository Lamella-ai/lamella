# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""AI-assisted column mapping for generic_csv / generic_xlsx uploads.

Flow:
  1. `build_prompt(preview)` assembles a system + user prompt from the
     first N rows of the chosen sheet plus our canonical column list.
  2. `propose_mapping()` calls OpenRouter via `AIService` and validates the
     response against `ColumnMapResponse`. On parse failure we log to
     `ai_decisions` with success=False and fall back to `heuristic_map()`.
  3. Every call logs to `ai_decisions` (Phase 3 contract). Re-opening the
     mapping page serves the cached decision — no second API call.

The canonical target columns match the `raw_rows` normalized fields plus
the 13-col annotation columns, so a generic sheet can flow through the
same ingester contract as the known source classes.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, Field, ValidationError

from lamella.adapters.openrouter.client import AIError
from lamella.features.ai_cascade.service import AIService
from lamella.features.import_.preview import SheetPreview

log = logging.getLogger(__name__)


CANONICAL_COLUMNS: tuple[str, ...] = (
    "date",
    "amount",
    "currency",
    "payee",
    "description",
    "memo",
    "location",
    "payment_method",
    "transaction_id",
    # 13-col annotations
    "ann_master_category",
    "ann_subcategory",
    "ann_business_expense",
    "ann_business",
    "ann_expense_category",
    "ann_expense_memo",
    "ann_amount2",
)


class ColumnMapResponse(BaseModel):
    column_map: dict[str, str | None] = Field(
        description=(
            "Mapping from source column name (verbatim) to one of the "
            "canonical column names, or null to drop the column."
        ),
    )
    header_row_index: int = Field(
        default=0,
        description=(
            "0-based row index of the header row. 0 means the first row "
            "is the header."
        ),
    )
    confidence: float = Field(
        default=0.5,
        ge=0.0,
        le=1.0,
        description="Overall confidence in the column mapping.",
    )
    notes: str = Field(default="")


@dataclass
class MappingResult:
    column_map: dict[str, str | None]
    header_row_index: int
    confidence: float
    notes: str
    source: str  # 'ai' | 'heuristic' | 'cache'
    decision_id: int | None = None


_SYSTEM_PROMPT = (
    "You are helping a bookkeeping tool map the columns of an uploaded "
    "spreadsheet onto a fixed canonical schema. Return ONLY valid JSON "
    "matching the provided schema. For every source column, choose exactly "
    "one canonical name from the allowed list, or null if the column should "
    "be dropped. Two source columns MAY map to the same canonical name only "
    "when necessary (e.g. if the file has both `Amount` and `Debit`/`Credit` "
    "— prefer whichever best represents the signed transaction amount).\n\n"
    "Canonical names:\n"
    "  date — transaction date (ISO 8601 preferred)\n"
    "  amount — signed amount (+ money in, - money out)\n"
    "  currency — ISO 4217 code, default USD\n"
    "  payee — counterparty name\n"
    "  description — transaction description\n"
    "  memo — optional free-form memo\n"
    "  location — city/merchant location\n"
    "  payment_method — account or instrument identifier\n"
    "  transaction_id — vendor-assigned id (order #, TRN, etc.)\n"
    "  ann_master_category — user-annotated master category\n"
    "  ann_subcategory — user-annotated subcategory\n"
    "  ann_business_expense — Yes/No flag for business expense\n"
    "  ann_business — the business entity (Acme, Personal, ...)\n"
    "  ann_expense_category — Schedule C taxonomy label\n"
    "  ann_expense_memo — per-row annotation memo\n"
    "  ann_amount2 — duplicate-amount column for reconciliation\n"
)


def build_prompt(preview: SheetPreview) -> tuple[str, str]:
    """Return (system, user) prompts for OpenRouter."""
    lines: list[str] = []
    lines.append(f"Sheet name: {preview.sheet_name!r}")
    lines.append(f"Detected header row (0-based): {preview.header_row_index}")
    lines.append(f"Total data rows in file: {preview.row_count}")
    lines.append("")
    lines.append("Source columns (verbatim):")
    for i, col in enumerate(preview.columns):
        lines.append(f"  [{i}] {col!r}")
    lines.append("")
    lines.append("First rows of data:")
    for i, row in enumerate(preview.rows[:10]):
        # Truncate very long cells to keep the prompt compact.
        truncated = [str(c)[:60] for c in row]
        lines.append(f"  row {i + 1}: {truncated}")
    lines.append("")
    lines.append(
        "Produce a JSON object matching the schema. Every key in "
        "`column_map` must be a verbatim source column name from the "
        "list above. Every value must be one of the canonical names or null."
    )
    return _SYSTEM_PROMPT, "\n".join(lines)


_HEURISTIC_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\b(txn|trans|transaction|post|posted|date|when)\b.*date", re.I), "date"),
    (re.compile(r"^(?:date|day)$", re.I), "date"),
    (re.compile(r"\btime\b", re.I), None),  # drop
    (re.compile(r"\bamount\.1\b", re.I), "ann_amount2"),
    (re.compile(r"\b(amt|amount|gross|net|value|total)\b", re.I), "amount"),
    (re.compile(r"\bdebit\b|\bwithdrawal\b", re.I), "amount"),
    (re.compile(r"\bcredit\b|\bdeposit\b", re.I), "amount"),
    (re.compile(r"\bcurrency\b", re.I), "currency"),
    (re.compile(r"\bpayee\b|\bname\b|\bmerchant\b|\bbuyer\b", re.I), "payee"),
    (re.compile(r"\bdescription\b|\bdetails\b|\bnarration\b|\bitem\b", re.I), "description"),
    (re.compile(r"\bmemo\b|\bnote\b", re.I), "memo"),
    (re.compile(r"\blocation\b|\bcity\b|\baddress\b", re.I), "location"),
    (re.compile(r"\bpayment\s*method\b|\bcard\b|\baccount\b", re.I), "payment_method"),
    (re.compile(r"\border\s*id\b|\btxn\s*id\b|\btransaction\s*id\b|\bconfirmation\b|\bref(?:\s*(?:no|num|erence))?\b", re.I), "transaction_id"),
    (re.compile(r"\bmaster\s*category\b", re.I), "ann_master_category"),
    (re.compile(r"\bsubcategory\b", re.I), "ann_subcategory"),
    (re.compile(r"\bbusiness\s*expense\??\b", re.I), "ann_business_expense"),
    (re.compile(r"\bbusiness\b", re.I), "ann_business"),
    (re.compile(r"\bexpense\s*category\b", re.I), "ann_expense_category"),
    (re.compile(r"\bexpense\s*memo\b", re.I), "ann_expense_memo"),
)


def heuristic_map(columns: list[str]) -> dict[str, str | None]:
    out: dict[str, str | None] = {}
    for col in columns:
        target: str | None = None
        for pat, canonical in _HEURISTIC_PATTERNS:
            if pat.search(col):
                target = canonical
                break
        out[col] = target
    return out


async def propose_mapping(
    ai: AIService,
    *,
    preview: SheetPreview,
    input_ref: str,
    model: str | None = None,
) -> MappingResult:
    """Call OpenRouter to propose a column map. On failure, fall back to
    the heuristic mapper. Every attempt logs to `ai_decisions`.

    `input_ref` should be a stable identifier for the sheet (e.g.
    `import_id:{id}:sheet:{sheet_name}`) so cache hits work across reloads
    of the mapping page.
    """
    client = ai.new_client() if ai.enabled else None
    if client is None:
        # AI disabled or spend cap reached — fall back immediately.
        fallback = heuristic_map(preview.columns)
        return MappingResult(
            column_map=fallback,
            header_row_index=preview.header_row_index,
            confidence=0.4,
            notes="AI disabled or spend cap reached; heuristic mapping used.",
            source="heuristic",
        )

    system, user = build_prompt(preview)
    try:
        try:
            ai_result = await client.chat(
                decision_type="column_map",
                input_ref=input_ref,
                system=system,
                user=user,
                schema=ColumnMapResponse,
                model=model or ai.model_for("column_map"),
            )
        except AIError as exc:
            log.warning("AI column-map failed (%s); falling back to heuristic", exc)
            # Error is already logged by OpenRouterClient._log_error.
            fallback = heuristic_map(preview.columns)
            return MappingResult(
                column_map=fallback,
                header_row_index=preview.header_row_index,
                confidence=0.4,
                notes=f"AI failed ({exc}); heuristic mapping used.",
                source="heuristic",
            )
    finally:
        await client.aclose()

    data = ai_result.data
    # Keep only source columns we actually saw — drop any hallucinated keys.
    clean_map: dict[str, str | None] = {}
    valid = set(CANONICAL_COLUMNS)
    for col in preview.columns:
        target = data.column_map.get(col)
        if target == "" or target == "null":
            target = None
        if target is not None and target not in valid:
            target = None
        clean_map[col] = target
    return MappingResult(
        column_map=clean_map,
        header_row_index=data.header_row_index,
        confidence=float(data.confidence),
        notes=data.notes,
        source="cache" if ai_result.cached else "ai",
        decision_id=ai_result.decision_id,
    )


def serialize_mapping(result: MappingResult) -> str:
    return json.dumps(
        {
            "column_map": result.column_map,
            "header_row_index": result.header_row_index,
            "confidence": result.confidence,
            "notes": result.notes,
            "source": result.source,
            "decision_id": result.decision_id,
        },
        default=str,
    )


def deserialize_mapping(blob: str | None) -> MappingResult | None:
    if not blob:
        return None
    try:
        payload = json.loads(blob)
    except ValueError:
        return None
    return MappingResult(
        column_map={
            str(k): (v if v is None else str(v))
            for k, v in (payload.get("column_map") or {}).items()
        },
        header_row_index=int(payload.get("header_row_index") or 0),
        confidence=float(payload.get("confidence") or 0.0),
        notes=str(payload.get("notes") or ""),
        source=str(payload.get("source") or "user"),
        decision_id=payload.get("decision_id"),
    )

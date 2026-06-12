# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field, field_validator


class SimpleFINTransaction(BaseModel):
    """One posted transaction on a SimpleFIN account.

    The bridge returns `posted` as a Unix epoch (int). `description` is the
    only merchant text available in the stable spec; `payee` is present on
    some bridges and preferred when available.
    """

    id: str
    posted: int
    amount: Decimal
    description: str = ""
    payee: str | None = None
    memo: str | None = None
    pending: bool = False
    transacted_at: int | None = None

    @field_validator("amount", mode="before")
    @classmethod
    def _coerce_amount(cls, v: Any) -> Any:
        if isinstance(v, (int, float)):
            return Decimal(str(v))
        return v

    @property
    def posted_date(self) -> date:
        return datetime.fromtimestamp(int(self.posted), tz=timezone.utc).date()

    @property
    def merchant(self) -> str:
        """Pick the best merchant-text field. Payee wins when present, else
        the description. Memo is never the merchant."""
        p = (self.payee or "").strip()
        if p:
            return p
        return (self.description or "").strip()


class SimpleFINAccount(BaseModel):
    """One account in the SimpleFIN bridge response."""

    id: str
    name: str = ""
    currency: str = "USD"
    balance: Decimal | None = None
    available_balance: Decimal | None = None
    transactions: list[SimpleFINTransaction] = Field(default_factory=list)
    org: dict[str, Any] | None = None

    @field_validator("balance", "available_balance", mode="before")
    @classmethod
    def _coerce_balance(cls, v: Any) -> Any:
        if v is None or v == "":
            return None
        if isinstance(v, (int, float)):
            return Decimal(str(v))
        return v


class SimpleFINBridgeResponse(BaseModel):
    """Top-level bridge response. Some bridges return `errors` alongside
    accounts; we surface them but don't fail the whole fetch if accounts
    are present."""

    errors: list[str] = Field(default_factory=list)
    accounts: list[SimpleFINAccount] = Field(default_factory=list)

# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Bank data port — abstracts SimpleFIN-like account/transaction fetch.

The concrete adapter today is :mod:`lamella.adapters.simplefin.client`.
Stage-2 may add a Plaid adapter that fulfills the same contract.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class BankDataPort(Protocol):
    """Read-only contract for pulling bank accounts + transactions."""

    async def fetch_accounts(self, *args: Any, **kwargs: Any) -> Any:
        """Return parsed account+transaction payload (adapter-defined shape)."""
        ...


def claim_setup_token(token: str, *, timeout: float = 30.0) -> str:
    """Exchange a one-time setup token for a long-lived access URL.

    Default contract; concrete implementation lives in the adapter.
    Re-exported here so callers can import from ``lamella.ports.bank_data``
    once Stage-2 introduces multi-vendor support.
    """
    raise NotImplementedError(
        "claim_setup_token is provided by the active bank-data adapter; "
        "import from lamella.adapters.<vendor>.client until ADR-0040 wiring lands."
    )

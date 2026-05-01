# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Adapters — concrete implementations of ports.

Each subpackage talks to one external service (SimpleFIN, Paperless,
OpenRouter, ntfy, Pushover) and fulfills the matching port contract
under :mod:`lamella.ports`. ADR-0020.
"""

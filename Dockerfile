# Copyright 2026 Lamella
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

# syntax=docker/dockerfile:1.6
#
# App image. Builds in ~30s on top of the prebuilt base image
# (Dockerfile.base), which carries torch, sentence-transformers,
# pymupdf, weasyprint, fava, and every other pyproject.toml dep.
#
# If `pip install --no-deps` errors with a missing dist or the app
# ImportErrors at runtime, you added a dep to pyproject.toml
# without rebuilding the base — trigger the
# `Build and publish base Docker image` workflow
# (docker-publish-base.yml) on GitHub.
#
# The old single-stage all-in-one build is preserved as
# Dockerfile.legacy and can be used by pointing
# `docker build -f Dockerfile.legacy .` at it.

ARG BASE_IMAGE=ghcr.io/YOUR_GH_USER/lamella-base:latest
FROM ${BASE_IMAGE}

ENV PYTHONUNBUFFERED=1 \
    LAMELLA_DATA_DIR=/data \
    LAMELLA_MIGRATIONS_DIR=/app/migrations \
    LAMELLA_CONFIG_DIR=/app/config \
    LEDGER_DIR=/ledger \
    PORT=8080 \
    HOME=/app \
    XDG_CACHE_HOME=/data/cache \
    HF_HOME=/data/huggingface \
    TRANSFORMERS_CACHE=/data/huggingface \
    SENTENCE_TRANSFORMERS_HOME=/data/huggingface
# Legacy CONNECTOR_*_DIR / BCG_SKIP_DISCOVERY_GUARD names are still
# accepted at runtime via lamella._legacy_env. They aren't set here
# so the image emits the new names; an operator with existing
# overrides on either side will see one DeprecationWarning per
# legacy name in the logs.
# Why: deploys that override the runtime uid (e.g. PUID/PGID env
# vars on self-hosted images, orchestrator pod-security policies)
# typically map the container user to a uid not in /etc/passwd.
# Any library that calls getpwuid() to resolve HOME —
# sentence-transformers + huggingface_hub do this — raises
# `KeyError: 'getpwuid(): uid not found: N'` and the vector-index
# startup task dies silently. Setting HOME + HF_HOME +
# TRANSFORMERS_CACHE explicitly avoids the lookup.

COPY --chown=app:app pyproject.toml /app/pyproject.toml
COPY --chown=app:app src /app/src
COPY --chown=app:app migrations /app/migrations
COPY --chown=app:app config /app/config
COPY --chown=app:app docker-entrypoint.sh /app/docker-entrypoint.sh

RUN /opt/venv/bin/pip install --no-deps /app \
    && chmod +x /app/docker-entrypoint.sh

WORKDIR /app
USER app

EXPOSE 8080 5003

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -fsS http://localhost:${PORT:-8080}/healthz || exit 1

# Single-container entrypoint: backgrounds fava on :5003, execs uvicorn
# on :8080. See docker-entrypoint.sh for details. Fava is a fallback
# UI; the primary app is uvicorn.
CMD ["/app/docker-entrypoint.sh"]

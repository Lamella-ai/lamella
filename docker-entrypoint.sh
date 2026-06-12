#!/bin/bash
# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

# Single-container entrypoint: Fava runs in the background as the
# fallback / read-inspection UI alongside the primary uvicorn app.
#
# Uvicorn is the primary process (PID 1 after exec), so signals from
# `docker stop` go straight to the app for a clean shutdown. Fava is a
# background child; it will be SIGKILL'd when the container stops.
# That's fine — Fava has no state to persist and is treated as a
# restart-anytime fallback tool.
#
# Once the in-app replacement features ship (error surface, bean-query,
# account autocomplete, in-browser editor), Fava is removed from the
# image entirely and this script collapses back to a single uvicorn
# invocation — at which point just delete the script and restore the
# plain uvicorn CMD in the Dockerfile.

set -eu

LEDGER_DIR="${LEDGER_DIR:-/ledger}"
LEDGER_MAIN="${LEDGER_DIR}/main.bean"

if [ -f "${LEDGER_MAIN}" ]; then
    fava --host 0.0.0.0 --port 5003 "${LEDGER_MAIN}" \
        > /tmp/fava.log 2>&1 &
    echo "entrypoint: fava started on :5003 against ${LEDGER_MAIN}"
else
    echo "entrypoint: ${LEDGER_MAIN} not found, skipping fava startup"
fi

exec uvicorn lamella.main:app --host 0.0.0.0 --port 8080

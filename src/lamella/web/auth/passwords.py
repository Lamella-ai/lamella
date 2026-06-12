# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Argon2id password hashing — ADR-0050.

Argon2id is the OWASP / NIST SP 800-63B current recommendation for
password storage. Bcrypt is acceptable but migrating bcrypt → argon2id
later forces a password reset for every user; we pay the dependency
cost (one wheel: argon2-cffi) once, here.

The DUMMY_HASH is verified against when the username does not exist,
so the unknown-user code path takes the same time as the wrong-password
path. No username-existence oracle.
"""

from __future__ import annotations

import hmac

from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError, InvalidHashError

# OWASP Password Storage Cheat Sheet (2024) recommended argon2id params
# for interactive logins. time_cost=2, memory_cost=19MiB, parallelism=1
# is the "minimum baseline" tier; we sit one notch higher (memory_cost
# 64MiB) since this is a financial app and we run on dedicated hardware
# rather than shared hosting.
_HASHER = PasswordHasher(
    time_cost=3,
    memory_cost=64 * 1024,
    parallelism=2,
    hash_len=32,
    salt_len=16,
)

# Pre-computed hash of a fixed throwaway value, used only to keep the
# unknown-user verification path the same shape and timing as the
# user-exists path. The value verifies against, but the equal-or-not
# decision is independent — the constant-time-compare against the
# real lookup is what determines login success.
DUMMY_HASH = _HASHER.hash("lamella-dummy-not-a-real-password-2026")


def hash_password(plaintext: str) -> str:
    """Hash a password with Argon2id. Returns the encoded hash string
    (algorithm + params + salt + hash, all in one)."""
    if not plaintext:
        raise ValueError("password must not be empty")
    return _HASHER.hash(plaintext)


def verify_password(stored_hash: str, plaintext: str) -> bool:
    """Constant-time-compare-equivalent verify.

    argon2-cffi's `verify` does an internal constant-time compare and
    raises VerifyMismatchError on bad password (returning True on
    success). We collapse all argon2 exceptions to False so callers
    cannot distinguish "wrong password" from "malformed hash" via
    the exception type.
    """
    try:
        _HASHER.verify(stored_hash, plaintext)
        return True
    except (VerifyMismatchError, InvalidHashError):
        return False
    except Exception:
        # Any other argon2 failure is treated as a hash-rejection.
        # We never want a verify error to look like a verify success.
        return False


def needs_rehash(stored_hash: str) -> bool:
    """True when the parameters baked into the stored hash are weaker
    than today's `_HASHER` config (e.g., we tightened time/memory cost
    after the user's hash was generated). On True, the caller should
    re-hash and update the row on the next successful login."""
    try:
        return _HASHER.check_needs_rehash(stored_hash)
    except Exception:
        return False


def constant_time_compare(a: str, b: str) -> bool:
    """Constant-time string compare for non-hash secrets (CSRF tokens,
    session ids, etc.). Wraps hmac.compare_digest with a unicode-safe
    encoding step."""
    return hmac.compare_digest(a.encode("utf-8"), b.encode("utf-8"))

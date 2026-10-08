"""A password reset ends every session signed in before it.

Session cookies were stateless and trusted for their full 24 hours, so a reset
-- the moment a stolen session most needs to stop working -- left them alive.
"""

import asyncio
import base64
import hashlib
import hmac
import pathlib
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.session_revocation import (  # noqa: E402
    SESSION_LIFETIME_SEC, is_revoked, revoke_sessions, token_expiry,
)


class _Raw:
    def __init__(self):
        self.kv = {}

    async def set(self, k, v, ex=None):
        self.kv[k] = v

    async def get(self, k):
        return self.kv.get(k)


class _Redis:
    raw = None

    def __init__(self):
        self.raw = _Raw()


def _cookie(email: str, issued: float) -> str:
    """The two-part cookie the frontend mints: base64url(payload).hexsig."""
    expires_ms = int((issued + SESSION_LIFETIME_SEC) * 1000)
    payload = f"{email}:ADMIN:{expires_ms}"
    sig = hmac.new(b"k", payload.encode(), hashlib.sha256).hexdigest()
    return base64.urlsafe_b64encode(payload.encode()).rstrip(b"=").decode() + "." + sig


def test_expiry_is_read_from_the_cookie_in_seconds():
    now = time.time()
    assert abs(token_expiry(_cookie("a@b.c", now)) - (now + SESSION_LIFETIME_SEC)) < 1


def test_a_session_from_before_the_reset_is_revoked_and_a_new_one_is_not():
    r = _Redis()
    t0 = time.time()
    old = _cookie("op@example.org", t0 - 3600)
    asyncio.run(revoke_sessions(r, "Op@Example.org", now=t0))
    assert asyncio.run(is_revoked(r, "op@example.org", old))
    new = _cookie("op@example.org", t0 + 5)
    assert not asyncio.run(is_revoked(r, "op@example.org", new))


def test_other_accounts_are_untouched():
    r = _Redis()
    asyncio.run(revoke_sessions(r, "op@example.org"))
    assert not asyncio.run(is_revoked(r, "someone@example.org", _cookie("someone@example.org", time.time() - 60)))


def test_a_redis_outage_does_not_sign_everyone_out():
    class _Broken:
        async def get(self, k):
            raise ConnectionError("down")

    class _R:
        raw = _Broken()

    assert not asyncio.run(is_revoked(_R(), "op@example.org", _cookie("op@example.org", 0)))


def test_the_gateway_checks_revocation_on_http_and_websocket():
    src = (ROOT / "services/api_gateway/dependencies.py").read_text(encoding="utf-8")
    assert src.count("await _session_revoked(") == 2


def test_both_reset_paths_revoke():
    auth = (ROOT / "services/api_gateway/routes/auth.py").read_text(encoding="utf-8")
    block = auth[auth.index('@router.post("/reset-password")'):]
    assert "await revoke_sessions(redis" in block
    script = (ROOT / "scripts/reset_password.py").read_text(encoding="utf-8")
    assert "await revoke_sessions(" in script


def test_the_lifetime_matches_the_cookie_the_frontend_sets():
    route = (ROOT / "frontend/src/app/api/auth/login/route.ts").read_text(encoding="utf-8")
    assert "Date.now() + 24 * 60 * 60 * 1000" in route
    assert SESSION_LIFETIME_SEC == 24 * 3600

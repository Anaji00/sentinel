"""Rate limiting, identity and credential handling.

Five defects, each of which held under the conditions it was tested in and
failed under the conditions it existed for.

  * the token bucket read, computed and wrote in three round trips, so N
    concurrent requests consumed one token between them;
  * its except clause returned True, so a Redis outage disabled rate limiting
    everywhere at once;
  * X-Forwarded-For was believed from any caller, so a fresh header value bought
    a fresh bucket and let traffic be attributed to somebody else's address;
  * an admin API key -- Role.ADMIN -- was accepted from the query string, where
    the proxy log, browser history and Referer header all keep a copy;
  * a session token whose payload contained no colon skipped the expiry check
    entirely and stayed valid forever.
"""
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DEPS_SRC = (ROOT / "services" / "api_gateway" / "dependencies.py").read_text(encoding="utf-8")


# ── the limiter is atomic ─────────────────────────────────────────────────

def test_rate_limit_is_evaluated_inside_redis():
    assert "_RATE_LIMIT_LUA" in DEPS_SRC
    assert "raw_redis.eval(" in DEPS_SRC, (
        "read-compute-write across three round trips is not a limiter under "
        "concurrency: two callers read the same count and both spend it"
    )


def test_rate_limit_no_longer_reads_then_writes_in_python():
    assert 'pipe.get(f"{bucket_key}:tokens")' not in DEPS_SRC


@pytest.mark.anyio
async def test_concurrent_callers_cannot_share_one_token():
    """The property the Lua script buys, exercised against a fake Redis that
    serialises eval the way a single-threaded server does."""
    import asyncio

    from services.api_gateway.dependencies import check_rate_limit

    class _AtomicFake:
        """Enough of Redis to run the bucket: eval is indivisible."""
        def __init__(self):
            self.kv = {}
            self._lock = asyncio.Lock()

        async def eval(self, script, nkeys, *args):
            tokens_key, ts_key = args[0], args[1]
            max_tokens, refill, now = float(args[2]), float(args[3]), float(args[4])
            async with self._lock:
                tokens = float(self.kv.get(tokens_key, max_tokens))
                last = float(self.kv.get(ts_key, now))
                tokens = min(max_tokens, tokens + max(0.0, now - last) * refill)
                allowed = 0
                if tokens >= 1.0:
                    tokens -= 1.0
                    allowed = 1
                self.kv[tokens_key] = tokens
                self.kv[ts_key] = now
                return allowed

    class _Client:
        def __init__(self):
            self.raw = _AtomicFake()

    client = _Client()
    # Three tokens, no refill within the test's span.
    results = await asyncio.gather(*[
        check_rate_limit(client, "concurrent", max_tokens=3, refill_rate_per_sec=0.0)
        for _ in range(10)
    ])
    assert sum(results) == 3, (
        f"expected exactly 3 of 10 concurrent callers to be admitted, got {sum(results)}"
    )


@pytest.mark.anyio
async def test_sensitive_callers_can_fail_closed():
    from services.api_gateway.dependencies import check_rate_limit

    class _Broken:
        class raw:
            @staticmethod
            async def eval(*a, **k):
                raise RuntimeError("redis is down")

    assert await check_rate_limit(_Broken(), "k", fail_open=True) is True
    assert await check_rate_limit(_Broken(), "k", fail_open=False) is False


# ── identity is not caller-supplied ───────────────────────────────────────

def test_forwarding_headers_are_ignored_without_a_trusted_proxy(monkeypatch):
    import services.api_gateway.dependencies as deps

    monkeypatch.setattr(deps, "TRUSTED_PROXY_CIDRS", [])

    class _Req:
        headers = {"X-Forwarded-For": "1.2.3.4"}
        class client:
            host = "203.0.113.9"

    assert deps.client_address(_Req()) == "203.0.113.9", (
        "believing X-Forwarded-For from any caller lets one header buy a fresh "
        "rate-limit bucket per request"
    )


def test_forwarding_headers_are_honoured_from_a_trusted_proxy(monkeypatch):
    import services.api_gateway.dependencies as deps

    monkeypatch.setattr(deps, "TRUSTED_PROXY_CIDRS", ["10.0.0.0/8"])

    class _Req:
        headers = {"X-Forwarded-For": "1.2.3.4, 10.0.0.7"}
        class client:
            host = "10.0.0.7"

    assert deps.client_address(_Req()) == "1.2.3.4"


def test_a_forwarded_value_that_is_not_an_address_is_refused(monkeypatch):
    import services.api_gateway.dependencies as deps

    monkeypatch.setattr(deps, "TRUSTED_PROXY_CIDRS", ["10.0.0.0/8"])

    class _Req:
        headers = {"X-Forwarded-For": "not-an-ip"}
        class client:
            host = "10.0.0.7"

    assert deps.client_address(_Req()) == "10.0.0.7"


# ── credentials travel in headers ─────────────────────────────────────────

def test_the_http_api_key_is_not_read_from_the_query_string():
    """Scoped to the HTTP handler.

    The WebSocket handshake still accepts a query parameter and is right to: a
    browser cannot set custom headers on one, so there it is sometimes the only
    mechanism. On HTTP a header is always available, which is what makes the
    query string there a needless copy of an ADMIN credential in three logs.
    """
    http_handler = DEPS_SRC[DEPS_SRC.index("async def verify_api_key("):
                            DEPS_SRC.index("async def verify_websocket_api_key(")]
    assert "query_params.get(\"api_key\")" in http_handler, (
        "the handler should still detect the parameter in order to refuse it"
    )
    assert "or api_key_query" not in http_handler


def test_a_key_in_the_query_string_is_refused_rather_than_ignored():
    assert "API keys must be sent in the X-API-KEY header" in DEPS_SRC


def test_a_bearer_token_is_accepted():
    assert 'auth_header.lower().startswith("bearer ")' in DEPS_SRC


# ── tokens expire ─────────────────────────────────────────────────────────

def test_a_token_without_an_expiry_is_rejected():
    from services.api_gateway.dependencies import verify_session_token

    # Signed correctly is not enough: with no expiry segment the old parser
    # took the whole payload as an email and never checked a clock.
    ok, _ident, _role = verify_session_token("someone@example.com.deadbeef")
    assert ok is False


def test_the_parser_no_longer_has_a_branch_that_skips_expiry():
    assert "email = payload_str" not in DEPS_SRC, (
        "the no-colon branch assigned the payload as an identity and returned "
        "without ever comparing a clock"
    )


# ── metrics are not public ────────────────────────────────────────────────

def test_metrics_are_no_longer_exempt_from_authentication():
    assert 'if path in ("/metrics", "/metrics/json", "/health"):' not in DEPS_SRC
    assert "Metrics require a scrape token." in DEPS_SRC


def test_health_stays_open():
    """A liveness probe needing a credential fails during a credential outage."""
    assert 'if path == "/health":' in DEPS_SRC

"""
tests/test_oidc_end_to_end.py

The SSO flow, run against a provider that actually answers over HTTP.

test_oidc_sso.py pins the decisions around the flow -- role mapping, the
substitute checks that stand in for a signature, the account-linking edge. Every
one of those tests hands `oidc` a dict it built itself, so the module was fully
covered and had still never completed a sign-on: nothing had exercised discovery,
the token POST, or the UserInfo call against a real socket, and a mock agrees
with whatever the client does. Run for the first time against a server that does
not, the flow worked -- but "unverified because untried" is not a property a
sign-in path should keep, so the provider lives here now.

The stub is deliberately not a security product. It speaks enough of OIDC to
answer the client honestly and to lie in the specific ways an attacker would:
issue a code, mint an ID token with the nonce it was given, and refuse a code it
has already spent. stdlib only -- http.server on a thread -- because taking on a
dependency to test a dependency-free module is a poor trade.
"""

import json
import sys
import threading
import time
import urllib.parse
from base64 import urlsafe_b64encode
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import oidc  # noqa: E402

aiohttp = pytest.importorskip("aiohttp")

CLIENT_ID = "sentinel-web"


def _b64(obj: dict) -> str:
    raw = json.dumps(obj, separators=(",", ":")).encode()
    return urlsafe_b64encode(raw).decode().rstrip("=")


class _StubProvider:
    """A minimal OIDC provider on a background thread."""

    def __init__(self) -> None:
        self.codes: dict = {}
        self.userinfo: dict = {
            "sub": "stub-user-0001",
            "email": "sso.tester@example.com",
            "email_verified": True,
            "name": "SSO Tester",
            "groups": ["analysts"],
        }
        self.issued_tokens = 0
        provider = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args) -> None:
                pass

            def _send(self, obj: dict, status: int = 200) -> None:
                body = json.dumps(obj).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self) -> None:
                parsed = urllib.parse.urlparse(self.path)
                query = dict(urllib.parse.parse_qsl(parsed.query))

                if parsed.path == "/.well-known/openid-configuration":
                    return self._send({
                        "issuer": provider.issuer,
                        "authorization_endpoint": provider.issuer + "/authorize",
                        "token_endpoint": provider.issuer + "/token",
                        "userinfo_endpoint": provider.issuer + "/userinfo",
                        "jwks_uri": provider.issuer + "/jwks",
                        "response_types_supported": ["code"],
                        "id_token_signing_alg_values_supported": ["RS256"],
                    })

                if parsed.path == "/authorize":
                    code = "code-%d-%d" % (len(provider.codes), time.time_ns())
                    provider.codes[code] = query.get("nonce", "")
                    return self._send({"code": code, "state": query.get("state", "")})

                if parsed.path == "/userinfo":
                    if not self.headers.get("Authorization", "").startswith("Bearer "):
                        return self._send({"error": "unauthorized"}, 401)
                    return self._send(provider.userinfo)

                return self._send({"error": "not_found"}, 404)

            def do_POST(self) -> None:
                parsed = urllib.parse.urlparse(self.path)
                length = int(self.headers.get("Content-Length", 0))
                form = dict(urllib.parse.parse_qsl(self.rfile.read(length).decode()))

                if parsed.path != "/token":
                    return self._send({"error": "not_found"}, 404)

                code = form.get("code", "")
                if code not in provider.codes:
                    # A spent code is the same answer as one that never existed.
                    return self._send({"error": "invalid_grant"}, 400)

                nonce = provider.codes.pop(code)
                provider.issued_tokens += 1
                now = int(time.time())
                claims = {
                    "iss": provider.issuer, "aud": CLIENT_ID, "sub": "stub-user-0001",
                    "nonce": nonce, "iat": now, "exp": now + 300,
                }
                header = _b64({"alg": "RS256", "typ": "JWT"})
                return self._send({
                    "access_token": "stub-access-token",
                    "id_token": header + "." + _b64(claims) + ".stub-signature",
                    "token_type": "Bearer",
                    "expires_in": 300,
                })

        self._server = HTTPServer(("127.0.0.1", 0), Handler)
        self.issuer = "http://127.0.0.1:%d" % self._server.server_port
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *_exc) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


@pytest.fixture
def provider(monkeypatch):
    oidc._discovery_cache.clear()
    with _StubProvider() as stub:
        monkeypatch.setenv("OIDC_ISSUER", stub.issuer)
        monkeypatch.setenv("OIDC_CLIENT_ID", CLIENT_ID)
        monkeypatch.setenv("OIDC_CLIENT_SECRET", "stub-secret")
        monkeypatch.setenv("OIDC_REDIRECT_URI", "https://sentinel.example.com/auth/callback")
        monkeypatch.setenv("OIDC_ROLE_MAP", "analysts:ANALYST")
        yield stub
    oidc._discovery_cache.clear()


async def _sign_on(session, stub, nonce_expected=None, code_override=None):
    """One full authorization-code round trip, as the browser would drive it."""
    discovery = await oidc.discover(session)
    state, nonce = oidc.new_state(), oidc.new_nonce()
    verifier, challenge = oidc.new_pkce_pair()

    url = oidc.authorization_url(discovery, state, nonce, challenge)
    async with session.get(url) as response:
        granted = await response.json()

    assert granted["state"] == state, "the provider did not echo state back"
    code = code_override or granted["code"]
    return await oidc.complete_login(
        session, code, verifier, nonce if nonce_expected is None else nonce_expected
    )


# -- the flow, end to end ------------------------------------------------------

@pytest.mark.anyio
async def test_a_full_sign_on_returns_the_federated_identity(provider):
    async with aiohttp.ClientSession() as session:
        identity = await _sign_on(session, provider)

    assert identity["subject"] == "stub-user-0001"
    assert identity["issuer"] == provider.issuer
    assert identity["email"] == "sso.tester@example.com"
    assert identity["email_verified"] is True
    assert identity["display_name"] == "SSO Tester"


@pytest.mark.anyio
async def test_the_idp_group_is_mapped_to_a_local_role(provider):
    """Mapping is unit-tested; this proves the claim survives the wire."""
    async with aiohttp.ClientSession() as session:
        assert (await _sign_on(session, provider))["role"] == "ANALYST"


@pytest.mark.anyio
async def test_identity_comes_from_userinfo_not_the_id_token(provider):
    """The gateway image has no JWT verifier, so the ID token is correlation
    material only. An unsigned token claiming to be someone else must not become
    that someone: UserInfo is fetched with the access token, and it wins."""
    provider.userinfo = dict(provider.userinfo, sub="the-real-subject")

    async with aiohttp.ClientSession() as session:
        identity = await _sign_on(session, provider)

    assert identity["subject"] == "the-real-subject", "the unsigned ID token was believed"


@pytest.mark.anyio
async def test_discovery_is_cached_across_sign_ons(provider):
    """Every sign-in re-fetching discovery makes the IdP a hot dependency."""
    async with aiohttp.ClientSession() as session:
        await _sign_on(session, provider)
        await _sign_on(session, provider)

    assert provider.issued_tokens == 2
    assert len(oidc._discovery_cache) == 1


# -- and the ways it must fail -------------------------------------------------

@pytest.mark.anyio
async def test_a_mismatched_nonce_is_rejected(provider):
    """The replay defence: a token minted for one sign-in attempt cannot be
    presented against another."""
    async with aiohttp.ClientSession() as session:
        with pytest.raises(oidc.OidcError):
            await _sign_on(session, provider, nonce_expected="a-different-nonce")


@pytest.mark.anyio
async def test_a_reused_authorization_code_is_rejected(provider):
    async with aiohttp.ClientSession() as session:
        discovery = await oidc.discover(session)
        nonce = oidc.new_nonce()
        verifier, challenge = oidc.new_pkce_pair()
        url = oidc.authorization_url(discovery, oidc.new_state(), nonce, challenge)
        async with session.get(url) as response:
            code = (await response.json())["code"]

        await oidc.complete_login(session, code, verifier, nonce)
        with pytest.raises(oidc.OidcError):
            await oidc.complete_login(session, code, verifier, nonce)


@pytest.mark.anyio
async def test_a_fabricated_authorization_code_is_rejected(provider):
    async with aiohttp.ClientSession() as session:
        with pytest.raises(oidc.OidcError):
            await _sign_on(session, provider, code_override="not-a-code-we-issued")


@pytest.mark.anyio
async def test_an_unreachable_provider_raises_rather_than_hangs(provider):
    """A dead IdP must fail the sign-in, not the request thread."""
    oidc._discovery_cache.clear()
    provider.__exit__()

    async with aiohttp.ClientSession() as session:
        with pytest.raises(oidc.OidcError):
            await oidc.discover(session)


@pytest.mark.anyio
async def test_a_provider_without_an_email_yields_no_email(provider):
    """The route turns this into a 401; the module must not invent one."""
    provider.userinfo = {"sub": "stub-user-0001", "groups": ["analysts"]}

    async with aiohttp.ClientSession() as session:
        identity = await _sign_on(session, provider)

    assert not identity.get("email")
    assert identity["email_verified"] is False

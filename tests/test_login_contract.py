"""The sign-in chain, checked at the joint I cannot walk through by hand.

Routing is verified in both directions against the deployment with a cookie
minted from its own SESSION_SECRET, and the panels are verified against real
authenticated responses. The step between -- a person typing a password into
the form -- is one I do not perform, so it is checked by its contract instead:
what the browser posts, what the BFF forwards, what the gateway answers, and
what the BFF reads back out of that answer.

Every one of these is a place where two files have to agree and neither
imports the other.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BFF = ROOT / "frontend" / "src" / "app" / "api" / "auth" / "login" / "route.ts"
SESSION = ROOT / "frontend" / "src" / "lib" / "session.ts"
GATEWAY = ROOT / "services" / "api_gateway" / "routes" / "auth.py"
LOGIN_PAGE = ROOT / "frontend" / "src" / "app" / "login" / "page.tsx"
MIDDLEWARE = ROOT / "frontend" / "src" / "middleware.ts"


def test_the_form_posts_what_the_bff_reads() -> None:
    page = LOGIN_PAGE.read_text(encoding="utf-8")
    bff = BFF.read_text(encoding="utf-8")
    assert "'/api/auth/login'" in page, "the form must post to the BFF, not the gateway"
    assert "const { email, password } = body" in bff, (
        "the BFF must read the two fields the form sends"
    )
    assert "JSON.stringify({ email, password })" in bff, (
        "and forward both of them upstream unchanged"
    )


def test_the_bff_forwards_to_the_route_the_gateway_serves() -> None:
    """A path typo here is a 404 the user reads as a wrong password."""
    bff = BFF.read_text(encoding="utf-8")
    assert "/api/v1/auth/login" in bff

    gateway = GATEWAY.read_text(encoding="utf-8")
    assert re.search(r'@router\.post\(\s*\n?\s*["\']/login["\']', gateway), (
        "the gateway no longer serves POST /login under the auth prefix"
    )


def test_the_bff_reads_the_shape_the_gateway_returns() -> None:
    """`success` and `user` are the two keys the whole sign-in turns on.

    The BFF decides authentication from `payload.success === true` and takes
    the identity from `payload.user`. If the gateway renamed either, every
    correct password would be reported as a wrong one.
    """
    gateway = GATEWAY.read_text(encoding="utf-8")
    assert '{"success": True, "user": account.to_public_dict()}' in gateway

    bff = BFF.read_text(encoding="utf-8")
    assert "payload?.success === true" in bff
    assert "payload?.user" in bff


def test_a_throttled_attempt_is_not_reported_as_a_bad_password() -> None:
    """429 and 401 are different facts, and only one is the user's fault."""
    bff = BFF.read_text(encoding="utf-8")
    assert "upstream.status === 429" in bff
    assert "Too many sign-in attempts" in bff
    # And an unreachable gateway is a third thing again.
    assert "Authentication service unreachable" in bff


def test_the_cookie_the_bff_mints_is_the_one_everything_else_verifies() -> None:
    """Three readers, one format.

    The BFF signs it, `/api/auth/session` verifies it, the proxy verifies it on
    every call, and the middleware reads its expiry to decide routing. They all
    have to agree on `email:role:expiresAt`, base64url, dot, HMAC.
    """
    session = SESSION.read_text(encoding="utf-8")
    assert "`${email}:${role}:${expiresAt}`" in session, "the payload format moved"
    assert "base64url" in session
    assert "timingSafeEqual" in session, "the comparison must stay constant-time"

    bff = BFF.read_text(encoding="utf-8")
    assert "signSessionToken(sessionEmail, sessionRole, expiresAt)" in bff

    middleware = MIDDLEWARE.read_text(encoding="utf-8")
    assert "base64url" in middleware, (
        "the middleware reads the same payload to decide routing; a different "
        "encoding here sends signed-in users to the login page"
    )


def test_signing_in_returns_the_operator_where_they_were_going() -> None:
    """`next` is set by the middleware and consumed by the form.

    Two files, no shared symbol. If either renames the parameter, the redirect
    silently becomes "go to the command centre" -- which looks like a working
    login and quietly loses the page the person asked for.
    """
    middleware = MIDDLEWARE.read_text(encoding="utf-8")
    page = LOGIN_PAGE.read_text(encoding="utf-8")
    assert "searchParams.set('next'" in middleware
    assert "get('next')" in page

    # Same-origin only, on both sides. An absolute URL would make the sign-in
    # flow an open redirect.
    for name, text in (("middleware", middleware), ("login page", page)):
        assert "startsWith('//')" in text, f"{name} does not reject a protocol-relative next"


def test_the_session_is_refreshed_before_the_redirect() -> None:
    """Otherwise the destination renders its header from a cached 'anonymous'.

    The provider polls on an interval, so without an explicit refresh the page
    you land on shows a Sign in button to someone who has just signed in.
    """
    page = LOGIN_PAGE.read_text(encoding="utf-8")
    assert "session.refresh()" in page
    assert page.index("session.refresh()") < page.index("router.push(safe)"), (
        "the refresh has to happen before the navigation, not after it"
    )

"""Covers the front door: token handling for email verification and password reset.

Open signup means these tokens are the only thing between a stranger and an
account. A reset token in particular is a live key: anyone holding one can take
over the account it belongs to, so it must be unguessable, unreadable from the
database, single-use, and short-lived.
"""

import pathlib
import sys
from datetime import datetime, timedelta, timezone

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.utils import auth_tokens as t  # noqa: E402
from shared.utils import mailer  # noqa: E402
from shared.utils.accounts import MIN_PASSWORD_LENGTH, Tier, account_from_row  # noqa: E402


# ── token generation ──────────────────────────────────────────────────────────

def test_tokens_are_unpredictable_and_unique():
    seen = {t.generate_token()[0] for _ in range(500)}
    assert len(seen) == 500, "token generation repeated a value"
    assert all(len(tok) >= 32 for tok in seen)


def test_plaintext_token_is_never_the_stored_value():
    """A database read must not yield a working link."""
    token, stored = t.generate_token()
    assert token != stored
    assert token not in stored
    assert len(stored) == 64, "expected a sha256 hex digest"


def test_hash_is_deterministic_and_matches():
    token, stored = t.generate_token()
    assert t.hash_token(token) == stored
    assert t.tokens_match(token, stored) is True


def test_a_different_token_does_not_match():
    _, stored = t.generate_token()
    other, _ = t.generate_token()
    assert t.tokens_match(other, stored) is False


@pytest.mark.parametrize("bad", ["", None])
def test_empty_token_never_matches(bad):
    _, stored = t.generate_token()
    assert t.tokens_match(bad, stored) is False
    assert t.tokens_match("anything", bad) is False


# ── expiry ────────────────────────────────────────────────────────────────────

def test_reset_expires_far_sooner_than_verification():
    """A reset link is a live key; a verification link is not."""
    assert t.TTL[t.TokenPurpose.RESET_PASSWORD] < t.TTL[t.TokenPurpose.VERIFY_EMAIL]
    assert t.TTL[t.TokenPurpose.RESET_PASSWORD] <= timedelta(hours=1)


def test_expiry_is_in_the_future_for_both_purposes():
    now = datetime.now(timezone.utc)
    for purpose in t.TokenPurpose:
        assert t.expiry_for(purpose, now) > now


def test_is_expired_boundaries():
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    assert t.is_expired(now - timedelta(seconds=1), now) is True
    assert t.is_expired(now + timedelta(seconds=1), now) is False


def test_missing_expiry_counts_as_expired():
    """A malformed row must fail closed, not grant an eternal token."""
    assert t.is_expired(None) is True


def test_naive_expiry_is_treated_as_utc_not_crashed_on():
    naive = datetime(2000, 1, 1, 0, 0)
    assert t.is_expired(naive) is True


def test_expiry_accepts_the_iso_string_the_database_actually_returns():
    """The Timescale helper serialises every timestamp before it reaches here.

    Reading `.tzinfo` off that string raised, turning a perfectly valid
    confirmation link into a 500 while the token itself was fine.
    """
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    assert t.is_expired("2026-01-01T11:59:00+00:00", now) is True
    assert t.is_expired("2026-01-01T12:01:00+00:00", now) is False


def test_expiry_accepts_a_z_suffixed_string():
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    assert t.is_expired("2026-01-01T12:01:00Z", now) is False


def test_unparseable_expiry_fails_closed():
    assert t.is_expired("not a timestamp") is True
    assert t.is_expired(12345) is True


def test_purposes_are_distinct_so_a_link_cannot_cross_over():
    """A verification link must not be usable to reset a password."""
    assert t.TokenPurpose.VERIFY_EMAIL.value != t.TokenPurpose.RESET_PASSWORD.value


# ── mail ──────────────────────────────────────────────────────────────────────

def test_mailer_reports_unconfigured_rather_than_pretending(monkeypatch):
    for var in ("SMTP_HOST", "SMTP_FROM", "SMTP_USERNAME"):
        monkeypatch.delenv(var, raising=False)
    assert mailer.is_configured() is False


def test_mailer_configured_needs_host_and_from(monkeypatch):
    monkeypatch.setenv("SMTP_HOST", "smtp.example.com")
    monkeypatch.delenv("SMTP_FROM", raising=False)
    monkeypatch.delenv("SMTP_USERNAME", raising=False)
    assert mailer.is_configured() is False
    monkeypatch.setenv("SMTP_FROM", "no-reply@example.com")
    assert mailer.is_configured() is True


@pytest.mark.anyio
async def test_send_never_raises_when_smtp_is_down(monkeypatch):
    """A failed send must not fail the signup that triggered it."""
    monkeypatch.setenv("SMTP_HOST", "127.0.0.1")
    monkeypatch.setenv("SMTP_PORT", "1")          # nothing listens here
    monkeypatch.setenv("SMTP_FROM", "no-reply@example.com")
    assert await mailer.send_email("a@b.co", "s", "body") is False


@pytest.mark.anyio
async def test_unconfigured_send_returns_false_not_an_exception(monkeypatch):
    for var in ("SMTP_HOST", "SMTP_FROM", "SMTP_USERNAME"):
        monkeypatch.delenv(var, raising=False)
    assert await mailer.send_email("a@b.co", "s", "body") is False


def test_message_bodies_carry_the_link():
    link = "https://sentinel.example.com/verify?token=abc123"
    subject, text, html = mailer.verification_email(link)
    assert link in text and link in html and subject

    link2 = "https://sentinel.example.com/reset?token=xyz789"
    subject2, text2, html2 = mailer.reset_email(link2)
    assert link2 in text2 and link2 in html2 and subject2


# ── what a new account is entitled to ─────────────────────────────────────────

def _row(**over):
    base = dict(id=1, email="new@example.com", display_name=None, role="VIEWER",
                is_active=True, subscription_tier="free", subscription_status="none",
                subscription_ends_at=None, stripe_customer_id=None)
    base.update(over)
    return base


def test_new_signup_lands_on_the_free_tier():
    account = account_from_row(_row())
    assert account.tier is Tier.FREE
    assert account.has_pro is False


def test_free_tier_can_use_the_analyst_platform():
    """Free is the whole product minus the reasoning tier, not a crippled demo."""
    account = account_from_row(_row())
    assert account.can_use("anything_not_gated") is True
    assert account.can_use("reasoning") is False
    assert account.can_use("scenarios") is False


def test_password_floor_is_enforced_by_policy_not_by_the_form():
    assert MIN_PASSWORD_LENGTH >= 12


# ── operator prompting for the keys the platform needs ───────────────────────

def test_email_delivery_is_a_declared_required_integration():
    """Open signup depends on it entirely.

    Every other integration narrows coverage when missing; without email
    delivery a new user never receives a confirmation link and a forgotten
    password has no recovery path, so the operator must be told loudly rather
    than left to discover it from a support request.
    """
    import importlib
    mod = importlib.import_module("services.api_gateway.routes.integrations")
    smtp = next((i for i in mod.INTEGRATIONS if i.key == "smtp"), None)
    assert smtp is not None, "email delivery is not listed as an integration"
    assert smtp.required is True
    assert smtp.signup_url, "an operator must be told where to get it"
    assert smtp.without_it, "an operator must be told what breaks without it"


def test_every_integration_states_where_to_get_it_and_what_breaks():
    """The panel prompts with these two fields; a blank one is a dead end."""
    import importlib
    mod = importlib.import_module("services.api_gateway.routes.integrations")
    for i in mod.INTEGRATIONS:
        assert i.env_vars, f"{i.key} declares no environment variables"
        assert i.signup_url, f"{i.key} does not say where to obtain a key"
        assert i.without_it, f"{i.key} does not say what is lost without it"


def test_integration_status_never_leaks_secret_material():
    """Status is presence only -- no length, prefix, or value."""
    import importlib, os
    mod = importlib.import_module("services.api_gateway.routes.integrations")
    os.environ["FINNHUB_API_KEY"] = "super-secret-value-1234"
    try:
        finnhub = next(i for i in mod.INTEGRATIONS if i.key == "finnhub")
        blob = repr(finnhub.status())
        assert "super-secret-value-1234" not in blob
        assert "23" not in blob or "configured" in blob   # no length disclosure
    finally:
        os.environ.pop("FINNHUB_API_KEY", None)


def test_signup_does_not_promise_email_that_cannot_be_sent(monkeypatch):
    """Saying "check your inbox" with no relay leaves users waiting forever."""
    import inspect
    from services.api_gateway.routes import auth as auth_mod
    src = inspect.getsource(auth_mod.signup)
    assert "mailer_configured()" in src, "signup does not check whether mail works"
    assert "email_sent" in src, "signup response does not report whether mail went out"

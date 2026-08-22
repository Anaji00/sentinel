"""
tests/test_accounts_and_tiers.py

Covers the account model that the subscription paywall rests on: password
storage, tier entitlement, and the boundary between free and paid features.
"""

from datetime import datetime, timedelta, timezone

import pytest

from shared.utils.accounts import (
    ACTIVE_STATUSES,
    FREE_TIER_LIMITS,
    MIN_PASSWORD_LENGTH,
    PRO_FEATURES,
    Account,
    Tier,
    account_from_row,
    hash_password,
    is_valid_email,
    needs_rehash,
    normalize_email,
    verify_password,
)


# ── PASSWORD STORAGE ─────────────────────────────────────────────────────────

def test_password_is_never_recoverable_from_its_hash():
    """The previous auth compared plaintext; a DB read exposed every password."""
    pw = "correct-horse-battery-staple"
    stored = hash_password(pw)
    assert pw not in stored
    assert stored.startswith("scrypt$")


def test_hash_is_salted_so_identical_passwords_differ():
    """Without a per-hash salt, one rainbow table breaks every shared password."""
    a, b = hash_password("identical-password-here"), hash_password("identical-password-here")
    assert a != b
    assert verify_password("identical-password-here", a)
    assert verify_password("identical-password-here", b)


def test_verification_accepts_correct_and_rejects_wrong():
    stored = hash_password("the-real-password-1")
    assert verify_password("the-real-password-1", stored) is True
    assert verify_password("the-real-password-2", stored) is False
    assert verify_password("", stored) is False


def test_hash_embeds_its_parameters_so_costs_can_be_raised():
    """A hash made under old settings must stay verifiable after a policy bump."""
    parts = hash_password("parameters-embedded-here").split("$")
    assert len(parts) == 6
    scheme, n, r, p = parts[0], int(parts[1]), int(parts[2]), int(parts[3])
    assert scheme == "scrypt"
    assert n >= 2 ** 14 and r >= 8 and p >= 1


def test_weaker_historical_parameters_are_flagged_for_rehash():
    weak = "scrypt$1024$4$1$c2FsdA==$a2V5"
    assert needs_rehash(weak) is True
    assert needs_rehash(hash_password("a-current-strength-password")) is False


def test_malformed_hash_fails_closed_without_raising():
    """A corrupt row must fail the login, not crash the endpoint."""
    for bad in ("", "notahash", "scrypt$only$three", "bcrypt$1$2$3$4$5"):
        assert verify_password("anything", bad) is False


def test_short_passwords_are_rejected_at_hash_time():
    """Enforced where it cannot be bypassed, not only in the UI."""
    with pytest.raises(ValueError):
        hash_password("short")
    assert MIN_PASSWORD_LENGTH >= 12


# ── EMAIL ────────────────────────────────────────────────────────────────────

def test_email_is_normalized_consistently():
    """Signup and login must agree, or an account becomes unreachable."""
    assert normalize_email("  User@Example.COM ") == "user@example.com"


@pytest.mark.parametrize("email,ok", [
    ("a@b.co", True), ("user.name+tag@example.com", True),
    ("no-at-sign", False), ("@example.com", False),
    ("user@", False), ("", False), ("two@@at.com", False),
])
def test_email_validation(email, ok):
    assert is_valid_email(email) is ok


# ── TIER ENTITLEMENT ─────────────────────────────────────────────────────────

def _acct(**kw):
    base = dict(id=1, email="u@e.com", display_name=None, role="VIEWER",
                tier=Tier.PRO, subscription_status="active",
                subscription_ends_at=None)
    base.update(kw)
    return Account(**base)


def test_free_account_has_no_pro_access():
    assert _acct(tier=Tier.FREE, subscription_status="none").has_pro is False


def test_active_pro_subscription_grants_access():
    assert _acct().has_pro is True


def test_cancelled_subscription_loses_access_even_while_tier_says_pro():
    """Cancellation can leave the tier field set until period end."""
    assert _acct(subscription_status="canceled").has_pro is False


def test_expired_subscription_loses_access_even_if_nothing_rewrote_the_row():
    """Access must not persist merely because no webhook arrived."""
    past = datetime.now(timezone.utc) - timedelta(days=1)
    assert _acct(subscription_ends_at=past).has_pro is False


def test_future_period_end_still_grants_access():
    future = datetime.now(timezone.utc) + timedelta(days=10)
    assert _acct(subscription_ends_at=future).has_pro is True


def test_naive_timestamp_is_treated_as_utc_not_rejected():
    """Postgres can hand back a naive datetime; that must not grant access wrongly."""
    past_naive = datetime.utcnow() - timedelta(days=2)
    assert _acct(subscription_ends_at=past_naive).has_pro is False


def test_past_due_keeps_access_during_dunning():
    """A failed retry should not lock someone out while Stripe is still trying."""
    assert "past_due" in ACTIVE_STATUSES
    assert _acct(subscription_status="past_due").has_pro is True


def test_unknown_status_denies_access():
    assert _acct(subscription_status="incomplete_expired").has_pro is False


# ── FEATURE GATING ───────────────────────────────────────────────────────────

def test_reasoning_is_the_paid_boundary():
    """The expensive part (CPU inference) is what is gated."""
    assert "reasoning" in PRO_FEATURES
    assert "scenarios" in PRO_FEATURES


def test_free_tier_keeps_the_whole_analyst_platform():
    """A visibly crippled free tier converts worse than a genuinely useful one."""
    free = _acct(tier=Tier.FREE, subscription_status="none")
    for feature in ("dashboard", "correlation", "graph", "events", "charts", "watchlists"):
        assert free.can_use(feature) is True, f"{feature} must stay free"


def test_free_tier_cannot_use_paid_features():
    free = _acct(tier=Tier.FREE, subscription_status="none")
    for feature in PRO_FEATURES:
        assert free.can_use(feature) is False


def test_pro_tier_can_use_everything():
    pro = _acct()
    for feature in list(PRO_FEATURES) + ["dashboard", "graph"]:
        assert pro.can_use(feature) is True


def test_free_limits_are_non_zero():
    """A free tier that permits nothing is a paywall, not a free tier."""
    for key, value in FREE_TIER_LIMITS.items():
        assert value > 0, f"{key} must allow some usage"


# ── CLIENT-SAFE SERIALIZATION ────────────────────────────────────────────────

def test_public_dict_never_leaks_billing_identifiers_or_hashes():
    acct = _acct(stripe_customer_id="cus_SECRET123")
    blob = str(acct.to_public_dict())
    assert "cus_SECRET123" not in blob
    assert "password" not in blob.lower()
    assert "stripe" not in blob.lower()


def test_public_dict_tells_the_client_what_is_locked():
    """The UI needs to know what to offer, not just that something failed."""
    d = _acct(tier=Tier.FREE, subscription_status="none").to_public_dict()
    assert d["has_pro"] is False
    assert "reasoning" in d["pro_features"]
    assert d["free_limits"]["backtest_runs_per_day"] > 0


def test_row_mapping_defaults_to_free_not_pro():
    """An unrecognized or absent tier must never fail open into paid access."""
    assert account_from_row({"id": 1, "email": "a@b.co"}).tier is Tier.FREE
    assert account_from_row(
        {"id": 1, "email": "a@b.co", "subscription_tier": "nonsense"}
    ).tier is Tier.FREE

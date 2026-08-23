"""Covers the parts of billing where a mistake grants access or loses money.

Webhook signature verification is the only thing standing between the public
internet and free subscriptions: the endpoint must be unauthenticated because
Stripe calls it, so a forged `checkout.session.completed` would otherwise
upgrade any account an attacker names.
"""
import hashlib
import hmac
import json
import pathlib
import sys
import time

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.utils import stripe_client  # noqa: E402
from shared.utils.accounts import ACTIVE_STATUSES, Tier  # noqa: E402

SECRET = "whsec_test_secret"


def _sign(body: bytes, secret: str = SECRET, timestamp: int = None) -> str:
    ts = timestamp if timestamp is not None else int(time.time())
    mac = hmac.new(secret.encode(), f"{ts}".encode() + b"." + body, hashlib.sha256).hexdigest()
    return f"t={ts},v1={mac}"


BODY = json.dumps({"id": "evt_1", "type": "checkout.session.completed"}).encode()


# ── signature verification ────────────────────────────────────────────────────

def test_valid_signature_accepted():
    assert stripe_client.verify_webhook_signature(BODY, _sign(BODY), SECRET) is True


def test_forged_signature_rejected():
    forged = f"t={int(time.time())},v1={'0' * 64}"
    assert stripe_client.verify_webhook_signature(BODY, forged, SECRET) is False


def test_signature_from_another_secret_rejected():
    assert stripe_client.verify_webhook_signature(BODY, _sign(BODY, "whsec_attacker"), SECRET) is False


def test_tampered_body_rejected():
    """The signature covers the body; changing the account must invalidate it."""
    sig = _sign(BODY, SECRET)
    tampered = BODY.replace(b"evt_1", b"evt_2")
    assert stripe_client.verify_webhook_signature(tampered, sig, SECRET) is False


def test_replayed_delivery_rejected_once_stale():
    """A captured delivery stays mathematically valid forever; the window stops it."""
    old = int(time.time()) - 3600
    assert stripe_client.verify_webhook_signature(BODY, _sign(BODY, SECRET, old), SECRET) is False


def test_replay_window_accepts_recent_delivery():
    recent = int(time.time()) - 60
    assert stripe_client.verify_webhook_signature(BODY, _sign(BODY, SECRET, recent), SECRET) is True


def test_missing_secret_never_accepts():
    """An unconfigured deployment must reject, not wave everything through."""
    assert stripe_client.verify_webhook_signature(BODY, _sign(BODY), "") is False


@pytest.mark.parametrize("header", ["", "garbage", "t=123", "v1=abc", "t=notanint,v1=abc"])
def test_malformed_signature_headers_rejected(header):
    assert stripe_client.verify_webhook_signature(BODY, header, SECRET) is False


# ── form encoding (Stripe rejects JSON on these endpoints) ────────────────────

def test_nested_line_items_encoded_as_stripe_expects():
    encoded = stripe_client.encode_form(
        {"mode": "subscription", "line_items": [{"price": "price_123", "quantity": 1}]}
    )
    assert "mode=subscription" in encoded
    assert "line_items%5B0%5D%5Bprice%5D=price_123" in encoded or \
           "line_items[0][price]=price_123" in encoded


def test_booleans_are_stripe_literals_not_python():
    assert "allow_promotion_codes=true" in stripe_client.encode_form({"allow_promotion_codes": True})


def test_none_values_are_omitted_not_sent_as_null():
    assert "customer" not in stripe_client.encode_form({"customer": None, "mode": "subscription"})


# ── entitlement mapping ───────────────────────────────────────────────────────

def _tier_for(status):
    from services.api_gateway.routes import billing
    return billing._tier_for_status(status)


@pytest.mark.parametrize("status", sorted(ACTIVE_STATUSES))
def test_granting_statuses_map_to_pro(status):
    assert _tier_for(status) == Tier.PRO.value


@pytest.mark.parametrize("status", ["canceled", "unpaid", "incomplete_expired", "none", ""])
def test_non_granting_statuses_revoke_to_free(status):
    assert _tier_for(status) == Tier.FREE.value


def test_past_due_still_grants_access():
    """A failed retry must not lock a paying customer out mid-cycle while
    Stripe is still attempting the charge."""
    assert "past_due" in ACTIVE_STATUSES
    assert _tier_for("past_due") == Tier.PRO.value


def test_credentials_alone_do_not_enable_billing(monkeypatch):
    """Payments are off until explicitly switched on.

    Keys often exist long before a product should charge anyone -- during
    testing, or while a deployment is still being sized. A key appearing in the
    environment must never be enough on its own to start taking money.
    """
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test")
    monkeypatch.setenv("STRIPE_PRICE_ID", "price_1")
    monkeypatch.delenv("BILLING_ENABLED", raising=False)
    assert stripe_client.is_configured() is False, "billing enabled without the switch"


def test_switch_alone_does_not_enable_billing(monkeypatch):
    """Nor can the switch enable checkout without credentials to charge against."""
    monkeypatch.setenv("BILLING_ENABLED", "true")
    monkeypatch.delenv("STRIPE_SECRET_KEY", raising=False)
    monkeypatch.delenv("STRIPE_PRICE_ID", raising=False)
    assert stripe_client.is_configured() is False


def test_switch_plus_credentials_enables_billing(monkeypatch):
    monkeypatch.setenv("BILLING_ENABLED", "true")
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test")
    monkeypatch.setenv("STRIPE_PRICE_ID", "price_1")
    assert stripe_client.is_configured() is True


@pytest.mark.parametrize("value", ["", "0", "false", "no", "off", "maybe"])
def test_switch_is_off_for_anything_but_an_explicit_yes(monkeypatch, value):
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test")
    monkeypatch.setenv("STRIPE_PRICE_ID", "price_1")
    monkeypatch.setenv("BILLING_ENABLED", value)
    assert stripe_client.is_configured() is False


# ── entitlement against serialised timestamps ────────────────────────────────

def test_has_pro_handles_the_iso_string_the_database_returns():
    """`subscription_ends_at` arrives as text, not a datetime.

    Comparing it to `datetime.now()` raises, so an active subscriber with a
    period end would have crashed every entitlement check the first time one
    existed.
    """
    from datetime import datetime, timedelta, timezone
    from shared.utils.accounts import account_from_row

    future = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
    past = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()

    base = dict(id=1, email="a@b.co", display_name=None, role="VIEWER", is_active=True,
                subscription_tier="pro", subscription_status="active",
                stripe_customer_id="cus_1")

    assert account_from_row({**base, "subscription_ends_at": future}).has_pro is True
    assert account_from_row({**base, "subscription_ends_at": past}).has_pro is False
    assert account_from_row({**base, "subscription_ends_at": None}).has_pro is True


def test_public_dict_serialises_a_string_end_date_without_crashing():
    from datetime import datetime, timedelta, timezone
    from shared.utils.accounts import account_from_row
    future = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
    row = dict(id=1, email="a@b.co", display_name=None, role="VIEWER", is_active=True,
               subscription_tier="pro", subscription_status="active",
               stripe_customer_id="cus_1", subscription_ends_at=future)
    out = account_from_row(row).to_public_dict()
    assert out["subscription_ends_at"].startswith(future[:10])

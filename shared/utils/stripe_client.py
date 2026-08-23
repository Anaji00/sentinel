"""
shared/utils/stripe_client.py

Minimal Stripe client over the REST API.

Deliberately not the `stripe` SDK. The three calls this platform makes -- create
a checkout session, create a portal session, verify a webhook signature -- are a
form-encoded POST and an HMAC comparison. The SDK would add a dependency, its
own HTTP stack and its own retry semantics for that.

WHAT IS NEVER HANDLED HERE: card numbers, CVCs, billing addresses. The customer
enters those on Stripe's own hosted checkout page. This process only ever sees
identifiers and a subscription status, which is what keeps the platform entirely
out of PCI scope.
"""

import hashlib
import hmac
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import quote

import aiohttp

logger = logging.getLogger("shared.stripe")

STRIPE_API_BASE = "https://api.stripe.com/v1"
_TIMEOUT = aiohttp.ClientTimeout(total=20)

# Stripe signs the webhook body with a timestamp. Anything older than this is
# rejected so a captured delivery cannot be replayed later.
WEBHOOK_TOLERANCE_SEC = 300


class StripeError(RuntimeError):
    """A Stripe call failed. Carries the API's own message where there is one."""


def secret_key() -> str:
    return (os.getenv("STRIPE_SECRET_KEY") or "").strip()


def webhook_secret() -> str:
    return (os.getenv("STRIPE_WEBHOOK_SECRET") or "").strip()


def price_id() -> str:
    return (os.getenv("STRIPE_PRICE_ID") or "").strip()


def billing_enabled() -> bool:
    """Master switch for taking money.

    Payments are off until the deployment can actually carry paying users. This
    is separate from having credentials on purpose: the same Stripe account may
    be configured for testing long before the product should charge anyone, and
    a key appearing in the environment must never be enough to start billing.
    """
    return (os.getenv("BILLING_ENABLED") or "").strip().lower() in ("1", "true", "yes", "on")


def is_configured() -> bool:
    """True when checkout can actually be created.

    Billing endpoints answer 503 rather than 500 when this is false, so a
    deployment that has not enabled payments reports "not enabled" instead of
    looking broken, and the client offers the waitlist instead of checkout.
    """
    return bool(billing_enabled() and secret_key() and price_id())


def _flatten(payload: Dict[str, Any], prefix: str = "") -> List[Tuple[str, str]]:
    """Encodes nested dicts and lists the way Stripe's form API expects.

    `{"line_items": [{"price": "p"}]}` becomes `line_items[0][price]=p`. Stripe
    does not accept JSON bodies on these endpoints, so this shape is required.
    """
    items: List[Tuple[str, str]] = []
    for key, value in payload.items():
        field = f"{prefix}[{key}]" if prefix else key
        if value is None:
            continue
        if isinstance(value, dict):
            items.extend(_flatten(value, field))
        elif isinstance(value, (list, tuple)):
            for i, entry in enumerate(value):
                if isinstance(entry, dict):
                    items.extend(_flatten(entry, f"{field}[{i}]"))
                else:
                    items.append((f"{field}[{i}]", str(entry)))
        elif isinstance(value, bool):
            items.append((field, "true" if value else "false"))
        else:
            items.append((field, str(value)))
    return items


def encode_form(payload: Dict[str, Any]) -> str:
    return "&".join(f"{quote(k, safe='[]')}={quote(v, safe='')}" for k, v in _flatten(payload))


async def _post(path: str, payload: Dict[str, Any], idempotency_key: Optional[str] = None) -> Dict[str, Any]:
    key = secret_key()
    if not key:
        raise StripeError("STRIPE_SECRET_KEY is not configured.")

    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    # Stripe retries on network failure; without this a retried create could
    # produce a second subscription for the same click.
    if idempotency_key:
        headers["Idempotency-Key"] = idempotency_key

    async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
        async with session.post(
            f"{STRIPE_API_BASE}{path}", data=encode_form(payload), headers=headers
        ) as resp:
            text = await resp.text()
            try:
                body = json.loads(text)
            except ValueError:
                raise StripeError(f"Stripe returned a non-JSON response ({resp.status}).")
            if resp.status >= 400:
                message = (body.get("error") or {}).get("message") or f"HTTP {resp.status}"
                # Never log the response wholesale: it echoes request parameters.
                logger.error("Stripe %s failed: %s", path, message)
                raise StripeError(message)
            return body


async def create_checkout_session(
    *,
    customer_email: Optional[str],
    client_reference_id: str,
    success_url: str,
    cancel_url: str,
    customer_id: Optional[str] = None,
    idempotency_key: Optional[str] = None,
) -> Dict[str, Any]:
    """Hosted subscription checkout. Returns the session, including its `url`.

    `client_reference_id` carries our own account id through Stripe and back on
    the webhook, so a completed payment can be matched to an account even if the
    customer paid with a different email than they signed up with.
    """
    payload: Dict[str, Any] = {
        "mode": "subscription",
        "line_items": [{"price": price_id(), "quantity": 1}],
        "success_url": success_url,
        "cancel_url": cancel_url,
        "client_reference_id": client_reference_id,
        "allow_promotion_codes": True,
    }
    # An existing customer is reused so a returning subscriber does not end up
    # with duplicate customer records.
    if customer_id:
        payload["customer"] = customer_id
    elif customer_email:
        payload["customer_email"] = customer_email
    return await _post("/checkout/sessions", payload, idempotency_key)


async def create_portal_session(*, customer_id: str, return_url: str) -> Dict[str, Any]:
    """Stripe-hosted portal where a customer updates or cancels their plan.

    Cancellation and card changes are handled entirely by Stripe; implementing
    them here would mean handling payment details, which this platform avoids.
    """
    return await _post(
        "/billing_portal/sessions",
        {"customer": customer_id, "return_url": return_url},
    )


def verify_webhook_signature(
    payload: Union[str, bytes],
    signature_header: str,
    secret: Optional[str] = None,
    tolerance_sec: int = WEBHOOK_TOLERANCE_SEC,
    now: Optional[float] = None,
) -> bool:
    """Validates the `Stripe-Signature` header against the raw request body.

    Without this, anyone who learns the webhook URL can grant themselves a
    subscription by posting a fabricated `checkout.session.completed`. The body
    must be the exact bytes received: re-serialising parsed JSON changes the
    payload and the signature will not match.
    """
    secret = secret if secret is not None else webhook_secret()
    if not secret or not signature_header:
        return False
    if isinstance(payload, str):
        payload = payload.encode("utf-8")

    timestamp: Optional[str] = None
    signatures: List[str] = []
    for part in signature_header.split(","):
        piece = part.strip()
        if piece.startswith("t="):
            timestamp = piece[2:]
        elif piece.startswith("v1="):
            signatures.append(piece[3:])

    if not timestamp or not signatures:
        return False

    try:
        sent_at = int(timestamp)
    except ValueError:
        return False

    # Replay window. A delivery captured off the wire stops being usable once it
    # ages out, even though its signature stays mathematically valid forever.
    current = now if now is not None else time.time()
    if tolerance_sec and abs(current - sent_at) > tolerance_sec:
        logger.warning("Rejected Stripe webhook outside the %ss replay window.", tolerance_sec)
        return False

    signed_payload = timestamp.encode("utf-8") + b"." + payload
    expected = hmac.new(secret.encode("utf-8"), signed_payload, hashlib.sha256).hexdigest()
    # compare_digest, not ==: a short-circuiting comparison leaks how much of the
    # signature matched through timing.
    return any(hmac.compare_digest(expected, candidate) for candidate in signatures)

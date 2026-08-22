"""
services/api_gateway/routes/billing.py

Subscription lifecycle: checkout, the customer portal, and the webhook that
keeps local entitlement in step with Stripe.

Stripe is the source of truth for whether someone has paid. The `users` row
caches enough of that answer -- tier, status, period end -- to gate a request
without a network call on every page load, and the webhook reconciles drift.

The webhook is the only endpoint here that is unauthenticated, because Stripe
calls it directly. It is protected by signature verification instead: without
that, anyone who learns the URL could grant themselves a subscription by posting
a fabricated `checkout.session.completed`.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from services.api_gateway.dependencies import get_db_optional
from shared.utils import stripe_client
from shared.utils.accounts import ACTIVE_STATUSES, Tier, account_from_row, normalize_email

logger = logging.getLogger("api-gateway.billing")

router = APIRouter(prefix="/api/v1/billing", tags=["Billing"])

_ACCOUNT_COLUMNS = """
    id, email, password_hash, display_name, role, is_active,
    subscription_tier, subscription_status, subscription_ends_at,
    stripe_customer_id
"""


def _public_base_url() -> str:
    """Origin Stripe redirects back to after checkout."""
    return (os.getenv("PUBLIC_BASE_URL") or "https://localhost").rstrip("/")


async def _load_account(db, email: str):
    row = await db.query_one(
        f"SELECT {_ACCOUNT_COLUMNS} FROM users WHERE email = $1", normalize_email(email)
    )
    if not row:
        raise HTTPException(status_code=404, detail="Account not found.")
    return row


def _caller_email(request: Request) -> str:
    """The authenticated caller, established by `verify_api_key` upstream.

    Taking the email from the request body instead would let any authenticated
    user start a checkout that upgrades somebody else's account.
    """
    identity = getattr(getattr(request, "state", None), "identity", "") or ""
    if identity.startswith("session:"):
        return identity.split("session:", 1)[1]
    if "@" in identity:
        return identity
    raise HTTPException(
        status_code=403,
        detail="Billing requires a signed-in user session, not an API key.",
    )


class CheckoutResponse(BaseModel):
    url: str
    session_id: str


@router.get("/status")
async def billing_status(request: Request, db=Depends(get_db_optional)):
    """What this account is entitled to, and whether billing is available."""
    if db is None:
        raise HTTPException(status_code=503, detail="Account store unavailable.")
    account = account_from_row(await _load_account(db, _caller_email(request)))
    return {
        **account.to_public_dict(),
        "billing_enabled": stripe_client.is_configured(),
        "manageable": bool(account.stripe_customer_id),
    }


@router.post("/checkout", response_model=CheckoutResponse)
async def create_checkout(request: Request, db=Depends(get_db_optional)):
    """Starts a hosted Stripe checkout for the signed-in account."""
    if not stripe_client.is_configured():
        raise HTTPException(
            status_code=503,
            detail="Billing is not configured on this deployment.",
        )
    if db is None:
        raise HTTPException(status_code=503, detail="Account store unavailable.")

    row = await _load_account(db, _caller_email(request))
    account = account_from_row(row)

    # Paying twice is a support ticket, not a feature.
    if account.has_pro:
        raise HTTPException(status_code=409, detail="This account already has an active subscription.")

    base = _public_base_url()
    try:
        session = await stripe_client.create_checkout_session(
            customer_email=account.email,
            client_reference_id=str(account.id),
            customer_id=account.stripe_customer_id,
            success_url=f"{base}/account?checkout=success",
            cancel_url=f"{base}/account?checkout=cancelled",
            # Scoped to the account so a double-click cannot open two
            # subscriptions, while a genuine retry later still can.
            idempotency_key=f"checkout:{account.id}",
        )
    except stripe_client.StripeError as e:
        raise HTTPException(status_code=502, detail=f"Stripe could not start checkout: {e}")

    return CheckoutResponse(url=session["url"], session_id=session["id"])


@router.post("/portal")
async def create_portal(request: Request, db=Depends(get_db_optional)):
    """Opens the Stripe-hosted portal for changing or cancelling a plan."""
    if db is None:
        raise HTTPException(status_code=503, detail="Account store unavailable.")
    account = account_from_row(await _load_account(db, _caller_email(request)))
    if not account.stripe_customer_id:
        raise HTTPException(status_code=409, detail="This account has never subscribed.")
    try:
        session = await stripe_client.create_portal_session(
            customer_id=account.stripe_customer_id,
            return_url=f"{_public_base_url()}/account",
        )
    except stripe_client.StripeError as e:
        raise HTTPException(status_code=502, detail=f"Stripe could not open the portal: {e}")
    return {"url": session["url"]}


# ── Webhook ───────────────────────────────────────────────────────────────────

# Subscription states Stripe reports that should grant access. Anything else --
# canceled, unpaid, incomplete_expired -- revokes it.
def _tier_for_status(status: str) -> str:
    return Tier.PRO.value if status in ACTIVE_STATUSES else Tier.FREE.value


def _period_end(sub: Dict[str, Any]) -> Optional[datetime]:
    ts = sub.get("current_period_end")
    if not ts:
        return None
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


async def _apply_subscription(db, *, user_id: int, customer_id: Optional[str],
                              subscription_id: Optional[str], status: str,
                              ends_at: Optional[datetime]) -> None:
    await db.execute(
        """
        UPDATE users
           SET stripe_customer_id     = COALESCE($1, stripe_customer_id),
               stripe_subscription_id = COALESCE($2, stripe_subscription_id),
               subscription_status    = $3,
               subscription_tier      = $4,
               subscription_ends_at   = $5
         WHERE id = $6
        """,
        customer_id, subscription_id, status, _tier_for_status(status), ends_at, user_id,
    )


async def _resolve_user_id(db, *, client_reference_id: Optional[str],
                           customer_id: Optional[str], email: Optional[str]) -> Optional[int]:
    """Finds the local account a Stripe object belongs to.

    Three routes, in descending reliability: the id we planted on the checkout
    session, the stored customer id, then the email. The last is a fallback
    because a customer can change their email in Stripe at any time.
    """
    if client_reference_id:
        try:
            row = await db.query_one("SELECT id FROM users WHERE id = $1", int(client_reference_id))
            if row:
                return int(row["id"])
        except (TypeError, ValueError):
            pass
    if customer_id:
        row = await db.query_one("SELECT id FROM users WHERE stripe_customer_id = $1", customer_id)
        if row:
            return int(row["id"])
    if email:
        row = await db.query_one("SELECT id FROM users WHERE email = $1", normalize_email(email))
        if row:
            return int(row["id"])
    return None


@router.post("/webhook")
async def stripe_webhook(request: Request, db=Depends(get_db_optional)):
    """Reconciles local entitlement with Stripe.

    Unauthenticated by necessity -- Stripe calls it -- and therefore verified by
    signature over the raw body. The body must be read as bytes: parsing and
    re-serialising it changes the payload and invalidates the signature.
    """
    raw = await request.body()
    signature = request.headers.get("Stripe-Signature") or request.headers.get("stripe-signature") or ""

    if not stripe_client.webhook_secret():
        logger.error("Stripe webhook received but STRIPE_WEBHOOK_SECRET is unset; refusing.")
        raise HTTPException(status_code=503, detail="Webhook is not configured.")

    if not stripe_client.verify_webhook_signature(raw, signature):
        logger.warning("Rejected Stripe webhook with an invalid or stale signature.")
        raise HTTPException(status_code=400, detail="Invalid signature.")

    try:
        event = json.loads(raw)
    except ValueError:
        raise HTTPException(status_code=400, detail="Malformed payload.")

    event_id = event.get("id")
    event_type = event.get("type", "")
    obj = (event.get("data") or {}).get("object") or {}

    if db is None:
        # 503 rather than 200: Stripe retries on failure, so the event is not
        # lost. Answering 200 here would acknowledge an event we never applied.
        raise HTTPException(status_code=503, detail="Account store unavailable.")

    # Stripe redelivers on any non-2xx, and at-least-once delivery is normal even
    # on success. The unique constraint on stripe_event_id makes replays no-ops.
    if event_id:
        seen = await db.query_one(
            "SELECT id FROM subscription_events WHERE stripe_event_id = $1", event_id
        )
        if seen:
            return {"received": True, "duplicate": True}

    handled = {
        "checkout.session.completed",
        "customer.subscription.created",
        "customer.subscription.updated",
        "customer.subscription.deleted",
        "invoice.payment_failed",
    }
    if event_type not in handled:
        # Acknowledged deliberately: an unhandled type is not a failure, and
        # returning an error would make Stripe retry it forever.
        return {"received": True, "ignored": event_type}

    customer_id = obj.get("customer")
    subscription_id = obj.get("subscription") or (obj.get("id") if event_type.startswith("customer.subscription") else None)
    email = (obj.get("customer_details") or {}).get("email") or obj.get("customer_email")

    user_id = await _resolve_user_id(
        db,
        client_reference_id=obj.get("client_reference_id"),
        customer_id=customer_id,
        email=email,
    )
    if user_id is None:
        logger.error("Stripe event %s (%s) matched no local account.", event_id, event_type)
        # 200: retrying will not make the account appear, and an endlessly
        # retried event buries real failures in Stripe's dashboard.
        return {"received": True, "unmatched": True}

    before = await db.query_one("SELECT subscription_status FROM users WHERE id = $1", user_id)
    status_before = (before or {}).get("subscription_status")

    if event_type == "customer.subscription.deleted":
        status_after = "canceled"
        ends_at = _period_end(obj)
    elif event_type == "invoice.payment_failed":
        # Not a revocation. Stripe keeps retrying the card, and `past_due` is a
        # granting status by design so a failed retry does not lock a paying
        # customer out mid-cycle.
        status_after = "past_due"
        ends_at = None
    elif event_type == "checkout.session.completed":
        status_after = "active"
        ends_at = None
    else:
        status_after = obj.get("status") or "active"
        ends_at = _period_end(obj)

    await _apply_subscription(
        db, user_id=user_id, customer_id=customer_id,
        subscription_id=subscription_id, status=status_after, ends_at=ends_at,
    )

    try:
        await db.execute(
            """
            INSERT INTO subscription_events
                (user_id, stripe_event_id, event_type, status_before, status_after, payload)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (stripe_event_id) DO NOTHING
            """,
            user_id, event_id, event_type, status_before, status_after, json.dumps(event),
        )
    except Exception as e:
        # The entitlement is already applied; losing the audit row must not cause
        # Stripe to retry and re-apply it.
        logger.error("Could not record subscription event %s: %s", event_id, e)

    logger.info(
        "Applied %s for account %s: %s -> %s", event_type, user_id, status_before, status_after
    )
    return {"received": True}

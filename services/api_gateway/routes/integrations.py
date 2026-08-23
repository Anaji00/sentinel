"""
services/api_gateway/routes/integrations.py

Which upstream data sources are configured, and which are not.

Sentinel degrades gracefully when an upstream key is absent: that collector
reports no data rather than fabricating any. This is the correct behaviour and
it produces a bad experience, because an empty panel looks identical whether the
market is quiet, the collector crashed, or nobody ever set the key.

This endpoint makes the third case distinguishable. It reports configuration
status only -- never a key value, never a prefix, never a length that would
narrow a guess. An operator learns *that* something is unset and what to set;
they learn nothing about what is already set.

Deliberately not a credential-writing surface. Keys are supplied through the
environment (`.env`, or a real secrets manager in production) so they are never
persisted by the application, never travel through a browser, and never appear
in a database backup. A UI that accepted and stored brokerage credentials would
make this application a custodian of them, which is a materially different
security posture from reading them at startup.
"""

import logging
import os
from typing import Any, Dict, List

from fastapi import APIRouter, Depends

from shared.utils.rbac import require_role, Role

logger = logging.getLogger("api-gateway.integrations")

router = APIRouter(prefix="/api/v1/integrations", tags=["Integrations"])


class Integration:
    """One upstream data source and the environment it needs."""

    def __init__(
        self,
        key: str,
        label: str,
        domain: str,
        env_vars: List[str],
        *,
        required: bool = False,
        free_tier: bool = True,
        signup_url: str = "",
        without_it: str = "",
    ):
        self.key = key
        self.label = label
        self.domain = domain
        self.env_vars = env_vars
        self.required = required
        self.free_tier = free_tier
        self.signup_url = signup_url
        self.without_it = without_it

    def status(self) -> Dict[str, Any]:
        # Presence only. Reporting a length or prefix would leak information
        # about a configured secret to anyone who can read this endpoint.
        missing = [v for v in self.env_vars if not (os.getenv(v) or "").strip()]
        configured = not missing
        return {
            "key": self.key,
            "label": self.label,
            "domain": self.domain,
            "configured": configured,
            "required": self.required,
            "free_tier": self.free_tier,
            "missing_env_vars": missing,
            "signup_url": self.signup_url,
            "impact_when_unset": self.without_it,
        }


# Ordered by how visibly the dashboard degrades without each one.
INTEGRATIONS: List[Integration] = [
    Integration(
        "finnhub", "Finnhub — equity reference data", "tradfi",
        ["FINNHUB_API_KEY"], free_tier=True,
        signup_url="https://finnhub.io/register",
        without_it="Sector, industry and index membership stay unresolved, so "
                   "graph nodes carry no structural relationships.",
    ),
    Integration(
        "aisstream", "AISStream — maritime vessel positions", "maritime",
        ["AISSTREAM_API_KEY"], free_tier=True,
        signup_url="https://aisstream.io/",
        without_it="No vessel tracking. The maritime map and chokepoint "
                   "geofencing stay empty.",
    ),
    Integration(
        "opensky", "OpenSky — aircraft state vectors", "aviation",
        ["OPENSKY_CLIENT_ID", "OPENSKY_CLIENT_SECRET"], free_tier=True,
        signup_url="https://opensky-network.org/",
        without_it="Anonymous access is heavily rate limited; flight coverage "
                   "will be sparse and intermittent.",
    ),
    Integration(
        "alpaca", "Alpaca — brokerage execution", "execution",
        ["ALPACA_API_KEY", "ALPACA_SECRET_KEY"], free_tier=True,
        signup_url="https://alpaca.markets/",
        without_it="Order execution stays on the internal paper simulator. "
                   "No external orders are placed.",
    ),
    Integration(
        "telegram", "Telegram — OSINT channels", "osint",
        ["TELEGRAM_BOT_TOKEN"], free_tier=True,
        signup_url="https://core.telegram.org/bots#botfather",
        without_it="Social OSINT ingestion is limited to RSS wire services.",
    ),
    # Not a data source, but the platform is unusable to strangers without it.
    # Open signup means every new account waits on a confirmation link, and a
    # forgotten password has no recovery path at all. Marked required because
    # this is the one integration whose absence blocks people rather than
    # narrowing coverage.
    Integration(
        "smtp", "Email delivery — sign-up and password reset", "accounts",
        ["SMTP_HOST", "SMTP_FROM"], required=True, free_tier=True,
        signup_url="https://resend.com/",
        without_it="Confirmation and password-reset links are written to the "
                   "gateway log instead of being sent. Usable for local "
                   "development; with open signup, nobody can confirm an "
                   "address or recover an account.",
    ),
    Integration(
        "stripe", "Stripe — subscriptions", "billing",
        ["STRIPE_SECRET_KEY", "STRIPE_PRICE_ID", "STRIPE_WEBHOOK_SECRET"],
        free_tier=True,
        signup_url="https://dashboard.stripe.com/register",
        without_it="Payments stay switched off and the account page offers a "
                   "waitlist instead of checkout. Note BILLING_ENABLED must "
                   "also be set: credentials alone never start billing.",
    ),
]

# Sources that need no key at all. Listed so the absence of an entry above is
# not read as "unconfigured" -- most of the platform works with no keys.
KEYLESS_SOURCES = [
    {"key": "yahoo", "label": "Yahoo Finance — historical bars", "domain": "tradfi"},
    {"key": "coinbase", "label": "Coinbase — crypto spot", "domain": "crypto"},
    {"key": "binance", "label": "Binance — perpetual funding", "domain": "crypto"},
    {"key": "sec_edgar", "label": "SEC EDGAR — filings and 13F", "domain": "filings"},
    {"key": "polymarket", "label": "Polymarket — prediction odds", "domain": "prediction"},
    {"key": "rss", "label": "RSS wire services", "domain": "osint"},
]


@router.get("", dependencies=[Depends(require_role(Role.VIEWER))])
async def list_integrations():
    """Configuration status for every upstream, with remediation guidance.

    Returns no secret material. An unconfigured integration is reported with the
    variable names to set and what the platform loses without them, so an
    operator can act without reading source code.
    """
    statuses = [i.status() for i in INTEGRATIONS]
    configured = [s for s in statuses if s["configured"]]
    unconfigured = [s for s in statuses if not s["configured"]]

    return {
        "configured_count": len(configured),
        "total_count": len(statuses),
        "keyless_sources": KEYLESS_SOURCES,
        "integrations": statuses,
        # Surfaced separately so a dashboard can prompt without filtering.
        "needs_attention": [
            {
                "key": s["key"],
                "label": s["label"],
                "missing_env_vars": s["missing_env_vars"],
                "signup_url": s["signup_url"],
                "impact_when_unset": s["impact_when_unset"],
            }
            for s in unconfigured
        ],
        "guidance": (
            "Keys are read from the environment, never stored by the "
            "application. Add them to .env and restart the affected service. "
            "The platform runs without any of them; each one widens coverage."
        ),
    }


@router.get("/execution", dependencies=[Depends(require_role(Role.ANALYST))])
async def execution_status():
    """Which broker will receive an order, stated unambiguously.

    Worth its own endpoint: the difference between the paper simulator and a
    live brokerage is one environment variable, and an operator should never
    have to infer which one is active from the absence of a warning.
    """
    broker_type = (os.getenv("BROKER_TYPE") or "paper").strip().lower()
    has_creds = bool(
        (os.getenv("ALPACA_API_KEY") or "").strip()
        and (os.getenv("ALPACA_SECRET_KEY") or "").strip()
    )

    if broker_type in ("alpaca_live", "live"):
        venue, is_live = "Alpaca LIVE — real capital", True
    elif broker_type == "alpaca":
        venue, is_live = "Alpaca paper endpoint", False
    else:
        venue, is_live = "Internal paper simulator — no external calls", False

    return {
        "broker_type": broker_type,
        "venue": venue,
        "is_live_capital": is_live,
        "credentials_present": has_creds,
        # An Alpaca venue without credentials fails at submission time; saying so
        # here turns a runtime error into a configuration warning.
        "operational": (broker_type == "paper") or has_creds,
        "warning": (
            "Orders placed here execute against real capital."
            if is_live else None
        ),
    }

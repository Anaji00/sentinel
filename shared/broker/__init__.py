"""
shared/broker/__init__.py

Brokerage abstraction layer for automated execution and portfolio management.
"""

from typing import Optional, Any
import os

from shared.broker.base import (
    BrokerInterface,
    AccountSummary,
    Position,
    Order,
    OrderSide,
    OrderType,
    OrderStatus,
)
from shared.broker.paper import PaperBroker
from shared.broker.alpaca import AlpacaBroker


import logging

logger = logging.getLogger("shared.broker")

# Selecting a live venue must take two independent, deliberate signals. A single
# environment variable is the kind of value that gets copied between .env files,
# and the failure mode is real capital rather than a bad log line.
LIVE_TRADING_CONFIRMATION = "I_UNDERSTAND_THIS_TRADES_REAL_MONEY"


class LiveTradingNotArmed(RuntimeError):
    """Raised when a live venue is requested without the explicit second signal."""


def get_broker(broker_type: Optional[str] = None, redis_client: Any = None) -> BrokerInterface:
    """
    Factory function to instantiate the active broker based on environment configuration.
    broker_type options: 'paper', 'alpaca', 'alpaca_live'.

    Live venues additionally require ALPACA_LIVE_CONFIRM to be set to
    LIVE_TRADING_CONFIRMATION; without it this raises rather than silently
    falling back, so a misconfiguration is loud instead of ambiguous.
    """
    b_type = (broker_type or os.getenv("BROKER_TYPE", "paper")).lower()

    if b_type == "alpaca":
        return AlpacaBroker(paper=True)

    if b_type in ("alpaca_live", "live"):
        confirmation = (os.getenv("ALPACA_LIVE_CONFIRM") or "").strip()
        if confirmation != LIVE_TRADING_CONFIRMATION:
            raise LiveTradingNotArmed(
                f"BROKER_TYPE={b_type!r} selects a LIVE trading venue, but "
                f"ALPACA_LIVE_CONFIRM is not set to the required confirmation "
                f"value. Refusing to route orders to real capital."
            )
        logger.warning(
            "LIVE TRADING ARMED — orders will be routed to the production "
            "Alpaca venue against real capital."
        )
        return AlpacaBroker(paper=False)

    return _paper_broker(redis_client)


# One paper book per process.
#
# This used to be `return PaperBroker(redis_client=redis_client)`, and every
# caller builds a broker per request: `POST /portfolio/orders` filled into a
# fresh instance with 100,000 in cash, updated that instance's position dict,
# recorded the order in the audit ledger, and then dropped the object. The next
# request built another empty one -- so `GET /portfolio/positions` returned [],
# `GET /portfolio/account` reported 100,000 and zero positions whatever had been
# traded, `GET /portfolio/risk` always took its no-positions branch, and
# cancelling an order could never find it.
#
# The paper book has therefore never held a position for longer than one HTTP
# request. Every order succeeded, every order was audited, and nothing was ever
# held.
#
# Process-local, and that is the honest scope of it: the book starts empty on
# every restart, and a deployment running more than one gateway worker would
# have one book per worker. This gateway runs a single uvicorn process with no
# --workers flag. Persisting it belongs in Postgres rather than Redis -- not
# because Redis would drop it (volatile-lru evicts only keys carrying a TTL,
# and a position would carry none) but because a book wants a schema, a
# constraint that two fills cannot race, and a history that can be queried.
_PAPER_BOOK: Optional[PaperBroker] = None


def _paper_broker(redis_client: Any = None) -> PaperBroker:
    global _PAPER_BOOK
    if _PAPER_BOOK is None:
        _PAPER_BOOK = PaperBroker(redis_client=redis_client)
    elif redis_client is not None and _PAPER_BOOK.redis is None:
        # The first caller of the process may have had no Redis yet; adopt one
        # when it appears, so marks and fills can reach the quote cache.
        _PAPER_BOOK.redis = redis_client
    return _PAPER_BOOK


def reset_paper_book() -> None:
    """Discard the process's paper book. For tests, and for nothing else."""
    global _PAPER_BOOK
    _PAPER_BOOK = None


__all__ = [
    "BrokerInterface",
    "AccountSummary",
    "Position",
    "Order",
    "OrderSide",
    "OrderType",
    "OrderStatus",
    "PaperBroker",
    "AlpacaBroker",
    "get_broker",
    "reset_paper_book",
    "LiveTradingNotArmed",
    "LIVE_TRADING_CONFIRMATION",
]

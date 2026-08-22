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

    return PaperBroker(redis_client=redis_client)


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
    "LiveTradingNotArmed",
    "LIVE_TRADING_CONFIRMATION",
]

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


def get_broker(broker_type: Optional[str] = None, redis_client: Any = None) -> BrokerInterface:
    """
    Factory function to instantiate the active broker based on environment configuration.
    broker_type options: 'paper', 'alpaca', 'alpaca_live'.
    """
    b_type = (broker_type or os.getenv("BROKER_TYPE", "paper")).lower()

    if b_type == "alpaca":
        return AlpacaBroker(paper=True)
    elif b_type in ("alpaca_live", "live"):
        return AlpacaBroker(paper=False)
    else:
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
]

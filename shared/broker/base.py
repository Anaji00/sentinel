"""
shared/broker/base.py

Abstract Broker Interface defining execution and portfolio tracking standard.
Allows seamless switching between Paper, Alpaca, and IBKR without changing trading logic.
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field
from datetime import datetime, timezone
import uuid


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"
    STOP_LIMIT = "STOP_LIMIT"


class OrderStatus(str, Enum):
    PENDING = "PENDING"
    FILLED = "FILLED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


class Position(BaseModel):
    symbol: str
    qty: float
    avg_entry_price: float
    current_price: float
    market_value: float
    unrealized_pl: float
    unrealized_pl_pct: float
    asset_class: str = "US_EQUITY"
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class Order(BaseModel):
    order_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    client_order_id: Optional[str] = None
    symbol: str
    side: OrderSide
    qty: float
    order_type: OrderType = OrderType.MARKET
    limit_price: Optional[float] = None
    stop_price: Optional[float] = None
    # Set only when a bracket was actually submitted. The API previously echoed
    # the caller's target_price as though it were part of the executed order
    # while no take-profit leg was ever sent.
    take_profit_price: Optional[float] = None
    is_bracket: bool = False
    status: OrderStatus = OrderStatus.PENDING
    filled_qty: float = 0.0
    filled_avg_price: Optional[float] = None
    submitted_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    filled_at: Optional[datetime] = None
    commission: float = 0.0


class AccountSummary(BaseModel):
    broker_name: str
    account_id: str
    cash: float
    portfolio_value: float
    buying_power: float
    currency: str = "USD"
    positions_count: int = 0
    day_trades_count: int = 0
    is_live: bool = False


class BrokerInterface(ABC):
    """Abstract interface for all brokerage adapters."""

    @abstractmethod
    async def get_account(self) -> AccountSummary:
        """Fetch account balance, buying power, and summary."""
        pass

    @abstractmethod
    async def get_positions(self) -> List[Position]:
        """Fetch all currently open positions."""
        pass

    @abstractmethod
    async def get_position(self, symbol: str) -> Optional[Position]:
        """Fetch position for a specific symbol."""
        pass

    @abstractmethod
    async def submit_order(
        self,
        symbol: str,
        qty: float,
        side: OrderSide,
        order_type: OrderType = OrderType.MARKET,
        limit_price: Optional[float] = None,
        stop_price: Optional[float] = None,
        client_order_id: Optional[str] = None,
        estimated_market_price: Optional[float] = None,
        take_profit_price: Optional[float] = None,
    ) -> Order:
        """Submit a new trade order."""
        pass

    @abstractmethod
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an open order."""
        pass

    @abstractmethod
    async def get_order_status(self, order_id: str) -> Optional[Order]:
        """Fetch status of an order."""
        pass

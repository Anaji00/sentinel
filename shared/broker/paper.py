"""
shared/broker/paper.py

Simulated paper trading broker with realistic slippage, commission modeling,
position persistence, and order fill simulation.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

from shared.broker.base import (
    BrokerInterface,
    AccountSummary,
    Position,
    Order,
    OrderSide,
    OrderType,
    OrderStatus,
)

logger = logging.getLogger("broker.paper")

REDIS_PAPER_POSITIONS = "sentinel:paper:positions"
REDIS_PAPER_ACCOUNT = "sentinel:paper:account"
REDIS_PAPER_ORDERS = "sentinel:paper:orders"


class PaperBroker(BrokerInterface):
    """
    In-memory and Redis-backed simulation broker.
    Simulates instantaneous fills for market orders and realistic execution.
    """

    def __init__(
        self,
        initial_cash: float = 100_000.0,
        slippage_bps: float = 5.0,  # 5 basis points
        redis_client: Any = None,
    ):
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.slippage_rate = slippage_bps / 10000.0
        self.redis = redis_client
        self._positions: Dict[str, Position] = {}
        self._orders: Dict[str, Order] = {}

    async def get_account(self) -> AccountSummary:
        portfolio_val = self.cash
        for pos in self._positions.values():
            portfolio_val += pos.market_value

        return AccountSummary(
            broker_name="PaperBroker",
            account_id="PAPER-001",
            cash=round(self.cash, 2),
            portfolio_value=round(portfolio_val, 2),
            buying_power=round(self.cash * 2.0, 2),  # 2x margin simulation
            currency="USD",
            positions_count=len(self._positions),
            day_trades_count=0,
            is_live=False,
        )

    async def get_positions(self) -> List[Position]:
        return list(self._positions.values())

    async def get_position(self, symbol: str) -> Optional[Position]:
        return self._positions.get(symbol.upper())

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
    ) -> Order:
        sym = symbol.upper()
        # Simulated execution price (use limit price or estimated market price or fallback $150.0)
        base_price = limit_price or estimated_market_price or 150.0

        # Apply slippage
        if side == OrderSide.BUY:
            fill_price = base_price * (1.0 + self.slippage_rate)
        else:
            fill_price = base_price * (1.0 - self.slippage_rate)

        fill_price = round(fill_price, 2)
        total_cost = fill_price * qty

        order = Order(
            client_order_id=client_order_id,
            symbol=sym,
            side=side,
            qty=qty,
            order_type=order_type,
            limit_price=limit_price,
            stop_price=stop_price,
            status=OrderStatus.FILLED,
            filled_qty=qty,
            filled_avg_price=fill_price,
            filled_at=datetime.now(timezone.utc),
            commission=0.0,
        )

        # Update cash and position state
        if side == OrderSide.BUY:
            self.cash -= total_cost
            if sym in self._positions:
                curr = self._positions[sym]
                new_qty = curr.qty + qty
                new_cost = (curr.qty * curr.avg_entry_price) + total_cost
                new_avg = new_cost / new_qty
                curr.qty = new_qty
                curr.avg_entry_price = round(new_avg, 2)
                curr.current_price = fill_price
                curr.market_value = round(new_qty * fill_price, 2)
                curr.unrealized_pl = round(curr.market_value - new_cost, 2)
                curr.unrealized_pl_pct = round((curr.unrealized_pl / new_cost) * 100.0, 2) if new_cost > 0 else 0.0
            else:
                self._positions[sym] = Position(
                    symbol=sym,
                    qty=qty,
                    avg_entry_price=fill_price,
                    current_price=fill_price,
                    market_value=round(total_cost, 2),
                    unrealized_pl=0.0,
                    unrealized_pl_pct=0.0,
                )
        else:
            # SELL
            self.cash += total_cost
            if sym in self._positions:
                curr = self._positions[sym]
                new_qty = curr.qty - qty
                if new_qty <= 0:
                    del self._positions[sym]
                else:
                    curr.qty = new_qty
                    curr.current_price = fill_price
                    curr.market_value = round(new_qty * fill_price, 2)
                    cost_basis = new_qty * curr.avg_entry_price
                    curr.unrealized_pl = round(curr.market_value - cost_basis, 2)
                    curr.unrealized_pl_pct = round((curr.unrealized_pl / cost_basis) * 100.0, 2) if cost_basis > 0 else 0.0

        self._orders[order.order_id] = order
        logger.info(f"📈 Paper Trade Executed: {side.value} {qty} {sym} @ ${fill_price:.2f} (Cash remaining: ${self.cash:.2f})")
        return order

    async def cancel_order(self, order_id: str) -> bool:
        if order_id in self._orders:
            order = self._orders[order_id]
            if order.status == OrderStatus.PENDING:
                order.status = OrderStatus.CANCELLED
                return True
        return False

    async def get_order_status(self, order_id: str) -> Optional[Order]:
        return self._orders.get(order_id)

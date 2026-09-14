"""
shared/broker/paper.py

Simulated paper trading broker with realistic slippage and order fill
simulation, marked to the platform's own live quote cache.

The book is process-local and does not survive a restart. It previously said
"position persistence" here and "In-memory and Redis-backed" on the class, and
three module constants -- REDIS_PAPER_POSITIONS, REDIS_PAPER_ACCOUNT,
REDIS_PAPER_ORDERS -- named the keys that would have held it. None of the three
was ever read or written. They are gone rather than wired up, and not because
Redis is evictable -- it is not, for a key with no TTL, under this
deployment's volatile-lru policy -- but because a trading book wants what the
audit ledger wants from Postgres: a schema, constraints, and a history that can
be queried rather than a blob that can be overwritten.
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
from shared.utils.quiet_failures import swallowed
from shared.utils.quote_cache import parse_quote, quote_key

logger = logging.getLogger("broker.paper")


class PaperBroker(BrokerInterface):
    """
    In-memory simulation broker, marked to the live quote cache.
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

    async def _live_price(self, symbol: str) -> Optional[float]:
        """The platform's current quote for a symbol, or None.

        The same cache `submit_order` prices fills from, read through one
        helper so marking and filling cannot drift apart.
        """
        if not self.redis:
            return None
        try:
            raw_redis = getattr(self.redis, "raw", self.redis)
            # Parsed by the module that owns the key. This read
            # `json.loads(...).get("price")`, which raises AttributeError on the
            # bare number every writer actually stores -- so the mark-to-market
            # this helper exists for never ran against a real cache, and the
            # test that covered it invented the object format in its fixture.
            return parse_quote(await raw_redis.get(quote_key(symbol.upper())))
        except Exception as _exc:
            swallowed("broker.paper._live_price", _exc, logger)
            return None

    async def _mark_to_market(self) -> None:
        """Revalue open positions at the current quote.

        Nothing did this. `current_price` was written once, at fill, and never
        again -- so `market_value` was the cost of the position, `unrealized_pl`
        was structurally zero, and a paper book could not show a gain or a loss
        however the market moved. Those values are what
        /portfolio/positions renders, what /portfolio/risk weights positions by,
        and what `get_account().portfolio_value` sums.

        It also fed back into execution: the fill price fell through to
        `existing_pos.current_price` before consulting the quote cache, so every
        later order in a symbol already held filled at the first fill's price.

        A symbol with no quote keeps its last mark rather than being zeroed; an
        absent quote is not a price of zero.
        """
        for symbol, pos in self._positions.items():
            price = await self._live_price(symbol)
            if price is None:
                continue
            pos.current_price = round(price, 2)
            pos.market_value = round(pos.qty * pos.current_price, 2)
            cost_basis = pos.qty * pos.avg_entry_price
            pos.unrealized_pl = round(pos.market_value - cost_basis, 2)
            pos.unrealized_pl_pct = (
                round((pos.unrealized_pl / cost_basis) * 100.0, 2) if cost_basis else 0.0
            )

    async def get_account(self) -> AccountSummary:
        await self._mark_to_market()
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
        await self._mark_to_market()
        return list(self._positions.values())

    async def get_position(self, symbol: str) -> Optional[Position]:
        await self._mark_to_market()
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
        take_profit_price: Optional[float] = None,
    ) -> Order:
        sym = symbol.upper()
        existing_pos = self._positions.get(sym)

        # The market before the position's own mark.
        #
        # This read `limit_price or estimated_market_price or
        # existing_pos.current_price` and consulted the quote cache only when
        # all three were absent -- so for a symbol already held, the third term
        # always answered and the cache was never reached. Combined with a
        # `current_price` that was only ever written at fill time, every order
        # after the first in a given symbol executed at the first fill's price,
        # for the life of the process.
        #
        # The position's mark is still the last resort, because filling at a
        # stale price beats refusing to simulate at all -- but it is now the
        # fallback rather than the answer.
        base_price = limit_price or estimated_market_price
        if not base_price:
            base_price = await self._live_price(sym)
        if not base_price and existing_pos:
            base_price = existing_pos.current_price

        if not base_price or base_price <= 0:
            raise ValueError(f"Cannot execute paper order for {sym}: missing limit_price, estimated_market_price, or live quote.")

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
            # Recorded so the simulator reports the same shape as a live bracket.
            take_profit_price=take_profit_price,
            is_bracket=take_profit_price is not None and stop_price is not None,
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

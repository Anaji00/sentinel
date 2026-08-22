"""
shared/broker/alpaca.py

Alpaca Markets Brokerage Adapter (Paper & Live API v2).
"""

import os
import aiohttp
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone

from shared.broker.base import (
    BrokerInterface,
    AccountSummary,
    Position,
    Order,
    OrderSide,
    OrderType,
    OrderStatus,
)

logger = logging.getLogger("broker.alpaca")


class AlpacaBroker(BrokerInterface):
    """
    Alpaca Markets REST API v2 brokerage adapter.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        paper: bool = True,
    ):
        self.api_key = api_key or os.getenv("ALPACA_API_KEY")
        self.secret_key = secret_key or os.getenv("ALPACA_SECRET_KEY")
        self.paper = paper
        self.base_url = (
            "https://paper-api.alpaca.markets/v2"
            if paper
            else "https://api.alpaca.markets/v2"
        )
        self.headers = {
            "APCA-API-KEY-ID": self.api_key or "",
            "APCA-API-SECRET-KEY": self.secret_key or "",
            "Accept": "application/json",
        }

    async def get_account(self) -> AccountSummary:
        if not self.api_key or not self.secret_key:
            return AccountSummary(
                broker_name="Alpaca (Unconfigured)",
                account_id="N/A",
                cash=0.0,
                portfolio_value=0.0,
                buying_power=0.0,
                currency="USD",
                positions_count=0,
                is_live=not self.paper,
            )

        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.get(f"{self.base_url}/account", timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return AccountSummary(
                        broker_name="Alpaca",
                        account_id=data.get("id", "alpaca-acct"),
                        cash=float(data.get("cash", 0.0)),
                        portfolio_value=float(data.get("portfolio_value", 0.0)),
                        buying_power=float(data.get("buying_power", 0.0)),
                        currency=data.get("currency", "USD"),
                        day_trades_count=int(data.get("daytrade_count", 0)),
                        is_live=not self.paper,
                    )
                else:
                    text = await resp.text()
                    raise RuntimeError(f"Alpaca get_account failed ({resp.status}): {text}")

    async def get_positions(self) -> List[Position]:
        if not self.api_key or not self.secret_key:
            return []

        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.get(f"{self.base_url}/positions", timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    positions = []
                    for item in data:
                        positions.append(
                            Position(
                                symbol=item.get("symbol"),
                                qty=float(item.get("qty", 0.0)),
                                avg_entry_price=float(item.get("avg_entry_price", 0.0)),
                                current_price=float(item.get("current_price", 0.0)),
                                market_value=float(item.get("market_value", 0.0)),
                                unrealized_pl=float(item.get("unrealized_pl", 0.0)),
                                unrealized_pl_pct=float(item.get("unrealized_plpc", 0.0)) * 100.0,
                                asset_class=item.get("asset_class", "us_equity").upper(),
                            )
                        )
                    return positions
                else:
                    return []

    async def get_position(self, symbol: str) -> Optional[Position]:
        positions = await self.get_positions()
        for p in positions:
            if p.symbol.upper() == symbol.upper():
                return p
        return None

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
        if not self.api_key or not self.secret_key:
            raise RuntimeError("Alpaca API credentials missing")

        # estimated_market_price is accepted for interface parity with
        # PaperBroker, which needs a reference price to simulate a fill. A live
        # venue determines its own fill price, so it is deliberately not sent:
        # promoting it to limit_price would turn a market order into a limit
        # order and could leave the position unfilled.

        # A protective stop and a profit target are legs of a bracket, not fields
        # on a plain order. Sending limit_price together with stop_price is
        # rejected outright -- 422 "limit orders require no stop price" -- so
        # every protected order failed, and take_profit_price was never
        # transmitted at all even though the API reported it back to the caller.
        is_bracket = take_profit_price is not None and stop_price is not None

        payload = {
            "symbol": symbol.upper(),
            "qty": str(qty),
            "side": side.value.lower(),
            "type": order_type.value.lower(),
            "time_in_force": "day",
        }
        if limit_price is not None:
            payload["limit_price"] = str(limit_price)
        if client_order_id:
            payload["client_order_id"] = client_order_id

        if is_bracket:
            # Alpaca rejects bracket and OCO orders on fractional quantities.
            whole_qty = int(qty)
            if whole_qty < 1:
                raise ValueError(
                    f"Bracket order for {symbol} requires at least 1 whole share "
                    f"(qty={qty}); fractional brackets are rejected by the venue."
                )
            payload["qty"] = str(whole_qty)
            payload["order_class"] = "bracket"
            payload["take_profit"] = {"limit_price": str(take_profit_price)}
            payload["stop_loss"] = {"stop_price": str(stop_price)}
        elif stop_price is not None:
            payload["stop_price"] = str(stop_price)

        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.post(f"{self.base_url}/orders", json=payload, timeout=10) as resp:
                if resp.status in (200, 201):
                    data = await resp.json()
                    return Order(
                        order_id=data.get("id"),
                        client_order_id=data.get("client_order_id"),
                        symbol=data.get("symbol"),
                        side=side,
                        qty=float(data.get("qty", qty)),
                        order_type=order_type,
                        limit_price=limit_price,
                        stop_price=stop_price,
                        take_profit_price=take_profit_price if is_bracket else None,
                        is_bracket=is_bracket,
                        status=OrderStatus.PENDING,
                        submitted_at=datetime.now(timezone.utc),
                    )
                else:
                    text = await resp.text()
                    raise RuntimeError(f"Alpaca submit_order failed ({resp.status}): {text}")

    async def cancel_order(self, order_id: str) -> bool:
        if not self.api_key or not self.secret_key:
            return False

        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.delete(f"{self.base_url}/orders/{order_id}", timeout=10) as resp:
                return resp.status in (200, 204)

    async def get_order_status(self, order_id: str) -> Optional[Order]:
        if not self.api_key or not self.secret_key:
            return None

        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.get(f"{self.base_url}/orders/{order_id}", timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    status_raw = data.get("status", "pending").upper()
                    status = OrderStatus.PENDING
                    if status_raw == "FILLED":
                        status = OrderStatus.FILLED
                    elif status_raw in ("CANCELED", "CANCELLED"):
                        status = OrderStatus.CANCELLED
                    elif status_raw == "REJECTED":
                        status = OrderStatus.REJECTED

                    return Order(
                        order_id=data.get("id"),
                        client_order_id=data.get("client_order_id"),
                        symbol=data.get("symbol"),
                        side=OrderSide.BUY if data.get("side") == "buy" else OrderSide.SELL,
                        qty=float(data.get("qty", 0.0)),
                        status=status,
                        filled_qty=float(data.get("filled_qty", 0.0)),
                        filled_avg_price=float(data.get("filled_avg_price")) if data.get("filled_avg_price") else None,
                    )
                return None

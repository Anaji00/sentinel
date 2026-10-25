"""
services/agents/auto_trader.py

Takes the monthly model's strongest calls as positions in a paper brokerage
account (book B1, shared/utils/auto_trade.py): the bottom tenth that can be
borrowed sold short, the 90th to 99th percentile bought, equal weight, no index
fund. The book is rebuilt from the model's monthly scores every 21 sessions and
held in between. Once each session, a few minutes after the open, it sizes the
book to the account and sends the orders that move the account there.

Paper only. The venue is Alpaca's paper endpoint, fixed here, whatever
BROKER_TYPE says: this runs unattended, and an unattended loop must not be one
environment variable away from real money. Off unless AUTO_TRADE_ENABLED is set.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import date, datetime, time
from typing import Any, Dict, Optional, Set
from zoneinfo import ZoneInfo

from shared.broker.alpaca import AlpacaBroker
from shared.broker.base import OrderSide, OrderStatus
from shared.utils import auto_trade as at
from shared.utils import model_portfolio as mp

logger = logging.getLogger("agent.auto_trader")

INTERVAL_SEC = 5 * 60
NY = ZoneInfo("America/New_York")
# After the opening auction, so a market order meets a settled book.
TRADE_AFTER = time(9, 40)
# The venue allows 200 requests a minute.
ORDER_GAP_SEC = 0.4
FLIP_WAIT_SEC = 30
DONE_KEY = "auto_trade:done:{portfolio}:{day}"
DEFAULT_LEG = 0.5
PORTFOLIO = "b1_extremes"
BOOK_KEY = "sentinel:auto_trade:book"          # {"rebalanced_on", "long", "short", "scores_as_of"}

_SESSIONS_SINCE_SQL = "SELECT count(*) AS n FROM daily_bars WHERE ticker = 'SPY' AND day > $1::date AND day < $2::date"
# A short costing more than the hard-to-borrow fee a year is not taken.
_FEES_SQL = """
    SELECT ticker, fee_rate FROM borrow_rates
    WHERE day = (SELECT max(day) FROM borrow_rates) AND fee_rate IS NOT NULL
"""
_PRICES_SQL = """
    SELECT DISTINCT ON (ticker) ticker, close FROM daily_bars
    WHERE ticker = ANY($1::text[]) AND close > 0 ORDER BY ticker, day DESC
"""
_MANAGED_SQL = "SELECT DISTINCT symbol FROM auto_trade_orders WHERE portfolio = $1"
_LOG_SQL = """
    INSERT INTO auto_trade_orders (day, portfolio, symbol, side, qty, reason, order_id, status, error)
    VALUES ($1::date, $2, $3, $4, $5, $6, $7, $8, $9)
"""


def enabled() -> bool:
    return (os.getenv("AUTO_TRADE_ENABLED") or "").strip().lower() in ("1", "true", "yes", "on")


def capital_cap() -> Optional[float]:
    """AUTO_TRADE_CAPITAL_USD: trade at most this much of the account (all of it when unset)."""
    raw = (os.getenv("AUTO_TRADE_CAPITAL_USD") or "").strip()
    try:
        v = float(raw) if raw else None
    except ValueError:
        return None
    return v if v and v > 0 else None


def leg_fraction() -> float:
    """AUTO_TRADE_LEG_FRACTION: each side's share of the capital (default half
    long, half short: dollar-neutral, so the result is the picking alone)."""
    raw = (os.getenv("AUTO_TRADE_LEG_FRACTION") or "").strip()
    try:
        v = float(raw) if raw else DEFAULT_LEG
    except ValueError:
        return DEFAULT_LEG
    return v if 0 < v <= 1 else DEFAULT_LEG


def _json(x: Any, default: Any) -> Any:
    if x is None:
        return default
    if isinstance(x, (str, bytes)):
        try:
            return json.loads(x)
        except ValueError:
            return default
    return x


class AutoTrader:
    def __init__(self, db: Any, redis: Any = None, portfolio: str = PORTFOLIO,
                 broker: Optional[AlpacaBroker] = None):
        self.db = db
        self.raw = getattr(redis, "raw", redis)
        self.portfolio = portfolio
        self.broker = broker or AlpacaBroker(paper=True)

    async def run(self) -> None:
        if not enabled():
            logger.info("Auto-trader off (AUTO_TRADE_ENABLED is not set)")
            return
        if not self.broker.paper:
            logger.error("Auto-trader refuses a live venue")
            return
        logger.info("Auto-trader on: book %s in the Alpaca paper account", self.portfolio)
        while True:
            try:
                out = await self.run_once()
                if out.get("orders") is not None:
                    logger.info("Auto-trader: %s", out)
            except Exception as exc:
                logger.error("Auto-trader failed: %s", exc, exc_info=True)
            await asyncio.sleep(INTERVAL_SEC)

    async def _done(self, today: date) -> bool:
        if self.raw is None:
            return False
        return bool(await self.raw.get(DONE_KEY.format(portfolio=self.portfolio, day=today.isoformat())))

    async def _mark_done(self, today: date) -> None:
        if self.raw is not None:
            await self.raw.set(DONE_KEY.format(portfolio=self.portfolio, day=today.isoformat()), "1", ex=2 * 86400)

    async def run_once(self, now: Optional[datetime] = None) -> Dict[str, Any]:
        now = now or datetime.now(NY)
        today = now.date()
        if now.time() < TRADE_AFTER or await self._done(today):
            return {}
        if not await self.broker.is_market_open():
            return {}
        book = await self._book(today)
        if not book or not (book["long"] or book["short"]):
            return {}
        weights = at.target_weights(book["long"], book["short"], leg_fraction())
        account = await self.broker.get_account()
        positions = await self.broker.get_positions()
        held = {p.symbol.upper(): float(p.qty) for p in positions}
        managed: Set[str] = {str(r["symbol"]) for r in await self.db.query(_MANAGED_SQL, self.portfolio) or []}
        names = sorted(set(weights) | (set(held) & managed))
        prices = {str(r["ticker"]).upper(): float(r["close"])
                  for r in await self.db.query(_PRICES_SQL, names) or []}
        for p in positions:                         # the venue's own mark, where it has one
            if p.current_price > 0:
                prices[p.symbol.upper()] = p.current_price
        # Size from what this trader holds plus the account's free value, so a
        # stranger's position in the same account is neither sold nor counted.
        foreign = sum(p.market_value for p in positions if p.symbol.upper() not in managed | set(weights))
        equity = max(0.0, float(account.portfolio_value) - foreign)
        cap = capital_cap()
        if cap is not None:
            equity = min(equity, cap)
        target = at.target_shares(weights, equity, prices)
        plans = at.plan_orders(target, held, managed, prices)
        sent = failed = 0
        for plan in plans:
            ok = await self._send(today, plan)
            sent += ok
            failed += not ok
            await asyncio.sleep(ORDER_GAP_SEC)
        await self._mark_done(today)
        return {"day": today.isoformat(), "equity": round(equity, 2), "orders": sent, "failed": failed,
                "names": len(target), "unplaced": len(at.unplaced(weights, target))}

    async def _book(self, today: date) -> Optional[Dict[str, Any]]:
        """The book to hold: the stored one until REBALANCE_SESSIONS have passed,
        then a new one from the model's latest monthly scores."""
        stored = _json(await self.raw.get(BOOK_KEY), None) if self.raw is not None else None
        since = None
        if stored and stored.get("rebalanced_on"):
            row = await self.db.query_one(_SESSIONS_SINCE_SQL, date.fromisoformat(stored["rebalanced_on"]), today)
            since = int((row or {}).get("n") or 0)
        if stored and not at.rebalance_due(since):
            return stored
        from shared.utils import short_data as sd
        from shared.utils.asset_universe import load_asset_meta
        from shared.utils.signal_store import factor_scores_key

        scores = _json(await self.raw.get(factor_scores_key(mp.HORIZON)), {}) if self.raw is not None else {}
        pct = {str(t).upper(): float(v["percentile"]) for t, v in (scores or {}).items()
               if isinstance(v, dict) and v.get("percentile") is not None}
        if not pct:
            return stored
        meta = await load_asset_meta(self.raw)
        borrowable = {t for t, m in meta.items() if len(m) > 2 and m[1] and m[2]}
        borrowable -= {str(r["ticker"]).upper() for r in await self.db.query(_FEES_SQL) or []
                       if float(r["fee_rate"]) > sd.HARD_TO_BORROW_FEE}
        long, short = at.b1_book(pct, borrowable)
        book = {"rebalanced_on": today.isoformat(), "long": long, "short": short}
        await self.raw.set(BOOK_KEY, json.dumps(book))
        logger.info("Auto-trader: new B1 book, %s long and %s short", len(long), len(short))
        return book

    async def _send(self, today: date, plan: at.Plan) -> bool:
        side = OrderSide.BUY if plan.side == "buy" else OrderSide.SELL
        try:
            order = await self.broker.submit_order(plan.symbol, plan.qty, side)
        except Exception as exc:
            await self.db.execute(_LOG_SQL, today, self.portfolio, plan.symbol, plan.side, plan.qty, plan.reason,
                                  None, "rejected", str(exc)[:500])
            return False
        await self.db.execute(_LOG_SQL, today, self.portfolio, plan.symbol, plan.side, plan.qty, plan.reason,
                              order.order_id, "submitted", None)
        if plan.reason == "flip-close":
            await self._await_fill(order.order_id)
        return True

    async def _await_fill(self, order_id: str) -> None:
        """A side change opens only once the close has filled."""
        for _ in range(FLIP_WAIT_SEC):
            got = await self.broker.get_order_status(order_id)
            if got is None or got.status in (OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED):
                return
            await asyncio.sleep(1)


__all__ = ["AutoTrader", "BOOK_KEY", "DEFAULT_LEG", "DONE_KEY", "PORTFOLIO", "capital_cap", "enabled",
           "leg_fraction"]

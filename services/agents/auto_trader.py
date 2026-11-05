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
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional, Set
from zoneinfo import ZoneInfo

from shared.broker.alpaca import AlpacaBroker
from shared.broker.base import OrderSide, OrderStatus, OrderType
from shared.utils import auto_trade as at
from shared.utils import model_portfolio as mp
from shared.utils import market_cap as mc
from shared.utils.quiet_failures import dropped, swallowed

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
# Every name this trader has ever opened, under any book name: the account's
# positions from an earlier book (spy_plus_overlay_industry_neutral, to 24 Oct
# 2026) are its own to close, not a stranger's to leave alone.
_MANAGED_SQL = "SELECT DISTINCT symbol FROM auto_trade_orders"
# The book's names with an alert in the last ALERT_RISK_DAYS, and the rule that fired.
# Three days, not seven (30 Oct 2026): the graded lift is a 1- and 5-session
# effect, and a week of alerts covered half the book.
ALERT_RISK_DAYS = 3
_RECENT_ALERTS_SQL = f"""
    SELECT DISTINCT primary_entity_id AS ticker, rule_name FROM correlations
    WHERE detected_at > NOW() - INTERVAL '{ALERT_RISK_DAYS} days' AND primary_entity_id = ANY($1::text[])
      AND rule_id IS DISTINCT FROM 'USER_SCENARIO_REQUEST'
"""
_LOG_SQL = """
    INSERT INTO auto_trade_orders (day, portfolio, symbol, side, qty, reason, order_id, status, error, ref_price)
    VALUES ($1::date, $2, $3, $4, $5, $6, $7, $8, $9, $10)
"""
# Orders whose outcome is not yet on record, once their session is over. Read only
# from the venue: this path places nothing and decides nothing.
FILLS_PER_PASS = 100
FILL_GAP_SEC = 0.4
FILLS_AFTER = time(16, 5)
_UNCHECKED_FILLS_SQL = """
    SELECT id, order_id, qty FROM auto_trade_orders
    WHERE order_id IS NOT NULL AND fill_status IS NULL AND day <= $1::date
    ORDER BY id LIMIT $2
"""
_FILL_SQL = "UPDATE auto_trade_orders SET filled_qty = $2, fill_price = $3, fill_status = $4, fill_checks = fill_checks + 1 WHERE id = $1"
_FILL_MISS_SQL = """
    UPDATE auto_trade_orders SET fill_checks = fill_checks + 1,
        fill_status = CASE WHEN fill_checks + 1 >= $2 THEN 'unknown' END
    WHERE id = $1
"""
# Safety (shared/utils/auto_trade.py): the halt flag, the book's equity record,
# SPY's latest close, and 20-session average dollar volume per name.
HALT_KEY = "sentinel:auto_trade:halt:{portfolio}"
_EQUITY_HISTORY_SQL = """
    SELECT equity, spy_close FROM auto_trade_equity WHERE portfolio = $1 AND day < $2 ORDER BY day DESC LIMIT 60
"""
_EQUITY_UPSERT_SQL = """
    INSERT INTO auto_trade_equity (day, portfolio, equity, spy_close) VALUES ($1, $2, $3, $4)
    ON CONFLICT (day, portfolio) DO UPDATE SET equity = EXCLUDED.equity, spy_close = EXCLUDED.spy_close
"""
_SPY_SQL = "SELECT close FROM daily_bars WHERE ticker = 'SPY' AND close > 0 ORDER BY day DESC LIMIT 1"
_ADV_SQL = """
    SELECT ticker, avg(close * volume) AS adv FROM (
        SELECT ticker, close, volume, row_number() OVER (PARTITION BY ticker ORDER BY day DESC) AS k
        FROM daily_bars WHERE ticker = ANY($1::text[]) AND day > CURRENT_DATE - 45
    ) x WHERE k <= 20 GROUP BY ticker
"""


def _setting(name: str, default: float) -> float:
    """A positive number from the environment, or the default."""
    try:
        v = float((os.getenv(name) or "").strip() or default)
    except ValueError:
        return default
    return v if v > 0 else default


def enabled() -> bool:
    return (os.getenv("AUTO_TRADE_ENABLED") or "").strip().lower() in ("1", "true", "yes", "on")


def alert_risk_scale() -> float:
    """B1_ALERT_RISK_SCALE: the weight, within its leg, of a name with a recent
    alert from a family graded to precede bigger moves (default 0.5; 1 = off)."""
    try:
        return min(1.0, max(0.0, float(os.getenv("B1_ALERT_RISK_SCALE", "0.5"))))
    except ValueError:
        return 0.5


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
            try:
                await self.record_fills()
            except Exception as exc:
                swallowed("agents.auto_trader.record_fills", exc, logger)
            await asyncio.sleep(INTERVAL_SEC)

    async def record_fills(self, now: Optional[datetime] = None) -> int:
        """Store what finished orders did. Today's wait for the close, since a day
        order can still fill; one the venue will not describe is parked as unknown."""
        now = now or datetime.now(NY)
        last_day = now.date() if now.time() >= FILLS_AFTER else now.date() - timedelta(days=1)
        stored = 0
        for r in await self.db.query(_UNCHECKED_FILLS_SQL, last_day, FILLS_PER_PASS) or []:
            order = await self.broker.get_order_status(str(r["order_id"]))
            if order is None:
                dropped("agents.auto_trader.fill_unread", "the venue did not describe an order", logger,
                        detail=str(r["order_id"]))
                await self.db.execute(_FILL_MISS_SQL, r["id"], at.FILL_MAX_CHECKS)
            else:
                await self.db.execute(_FILL_SQL, r["id"], order.filled_qty, order.filled_avg_price,
                                      at.fill_outcome(float(r["qty"]), order.filled_qty))
                stored += 1
            await asyncio.sleep(FILL_GAP_SEC)
        if stored:
            logger.info("Auto-trader: recorded %s order outcomes", stored)
        return stored

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
        # Picks only, checked daily so a stored book drops funds too: a fund
        # held from an earlier book gets a target of zero and is closed.
        stocks = await self._stocks()
        # Companies of $1bn and up only (30 Oct 2026); a name that falls out is
        # closed like any other dropped from the book. Without the table there
        # is no trading today: unknown size is not a pass.
        large = await mc.large_caps(self.raw)
        if not large:
            dropped("agents.auto_trader.no_market_caps", "no market-cap table; nothing traded today", logger)
            return {"day": str(today), "orders": 0, "skipped": "no market-cap table"}
        allowed = (stocks & large) if stocks is not None else large
        book = {**book, "long": at.picks_only(book["long"], allowed), "short": at.picks_only(book["short"], allowed)}
        weights = at.target_weights(book["long"], book["short"], leg_fraction(),
                                    weights=await self._risk_weights(list(book["long"]) + list(book["short"])))
        account = await self.broker.get_account()
        positions = await self.broker.get_positions()
        held = {p.symbol.upper(): float(p.qty) for p in positions}
        managed: Set[str] = {str(r["symbol"]) for r in await self.db.query(_MANAGED_SQL) or []}
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
        # Recorded first, halted or not, so a resumed book is compared with
        # yesterday rather than with the day it halted. The drift benchmark is
        # the book's net exposure times SPY (about 0 for B1, which is neutral).
        reasons = await self._check_and_record(today, equity, beta=sum(weights.values()))
        # The kill switch, before any order: a manual or tripped halt stops new
        # orders; nothing already held is sold to "make safe".
        halted = await self._halted()
        if halted:
            return {"day": today.isoformat(), "halted": halted}
        if reasons:
            await self._halt("; ".join(reasons))
            return {"day": today.isoformat(), "halted": "; ".join(reasons)}
        cap = capital_cap()
        if cap is not None:
            equity = min(equity, cap)
        # Per-name ceiling on the book's weights (AUTO_TRADE_MAX_NAME_WEIGHT).
        weights = at.cap_weights(weights, _setting("AUTO_TRADE_MAX_NAME_WEIGHT", 0.03))
        target = at.target_shares(weights, equity, prices)
        plans = at.plan_orders(target, held, managed, prices)
        adv = {str(r["ticker"]).upper(): float(r["adv"] or 0.0)
               for r in await self.db.query(_ADV_SQL, [p.symbol for p in plans]) or []}
        participation = _setting("AUTO_TRADE_MAX_ADV_SHARE", 0.01)
        plans = [at.liquidity_capped(p, prices.get(p.symbol, 0.0), adv.get(p.symbol), participation) for p in plans]
        # Limit prices from a live reference: the latest IEX trade, else the
        # venue's own mark. `prices` is yesterday's close for any name not
        # held, so a band around it missed every stock that gapped at the open.
        marks = {p.symbol.upper(): p.current_price for p in positions if p.current_price > 0}
        try:
            live = await self.broker.latest_prices([p.symbol for p in plans])
        except Exception as exc:
            swallowed("agents.auto_trader.latest_prices", exc, logger)
            live = {}
        sent = failed = 0
        for plan in plans:
            ok = await self._send(today, plan, live.get(plan.symbol) or marks.get(plan.symbol))
            sent += ok
            failed += not ok
            await asyncio.sleep(ORDER_GAP_SEC)
        broken = at.execution_halt(sent, failed, _setting("AUTO_TRADE_MAX_FAIL_SHARE", 0.25))
        if broken:
            await self._halt(f"execution: {broken}")
        await self._mark_done(today)
        return {"day": today.isoformat(), "equity": round(equity, 2), "orders": sent, "failed": failed,
                "names": len(target), "unplaced": len(at.unplaced(weights, target))}

    async def _halted(self) -> Optional[str]:
        """Why trading is stopped, or None: AUTO_TRADE_HALT set, or HALT_KEY
        written by a tripped check. Cleared only by hand (delete the key)."""
        if (os.getenv("AUTO_TRADE_HALT") or "").strip().lower() in ("1", "true", "yes", "on"):
            return "AUTO_TRADE_HALT is set"
        if self.raw is None:
            return None
        v = await self.raw.get(HALT_KEY.format(portfolio=self.portfolio))
        return (v.decode() if isinstance(v, bytes) else str(v)) if v else None

    async def _halt(self, reason: str) -> None:
        logger.error("Auto-trader HALTED (%s): %s. No new orders until %s is deleted.", self.portfolio, reason,
                     HALT_KEY.format(portfolio=self.portfolio))
        if self.raw is not None:
            await self.raw.set(HALT_KEY.format(portfolio=self.portfolio), reason)

    async def _check_and_record(self, today: date, equity: float, beta: float = 1.0) -> List[str]:
        """Today's equity against the book's record (day loss, drawdown, drift
        against SPY), then today's row. A failed check never halts trading on
        its own: the reasons list is empty and the failure is counted."""
        try:
            hist = [(float(r["equity"]), float(r["spy_close"] or 0.0))
                    for r in await self.db.query(_EQUITY_HISTORY_SQL, self.portfolio, today) or []][::-1]
            spy_row = await self.db.query_one(_SPY_SQL)
            spy = float(spy_row["close"]) if spy_row else None
            reasons = at.safety_halts(hist, equity, spy, _setting("AUTO_TRADE_MAX_DAY_LOSS", 0.05),
                                      _setting("AUTO_TRADE_MAX_DRAWDOWN", 0.15),
                                      _setting("AUTO_TRADE_MAX_UNDERPERF_21", 0.08), beta=beta)
            await self.db.execute(_EQUITY_UPSERT_SQL, today, self.portfolio, equity, spy)
            return reasons
        except Exception as exc:
            swallowed("agents.auto_trader.safety_check", exc, logger)
            return []

    async def _stocks(self) -> Optional[Set[str]]:
        """Symbols the venue's asset list classifies as common stock, or None
        when the list is unavailable (then only INDEX_FUNDS are excluded)."""
        if self.raw is None:
            return None
        try:
            from shared.utils.asset_universe import load_asset_meta
            meta = await load_asset_meta(self.raw)
        except Exception as exc:
            swallowed("agents.auto_trader.asset_meta", exc, logger)
            return None
        return {t.upper() for t, m in meta.items() if m and m[0] == "stock"} or None

    async def _risk_weights(self, names: List[str]) -> Optional[Dict[str, float]]:
        """Smaller weights for names with a recent alert from a family graded to
        precede bigger-than-usual moves, on both bars (producer_grades.
        strong_move_families). None -- equal weight -- when the scale is 1,
        there are no grades, or the lookup fails: a missing risk input must not
        stop the book trading."""
        scale = alert_risk_scale()
        if scale >= 1.0 or self.raw is None or not names:
            return None
        try:
            from shared.utils import producer_grades as pg
            from shared.utils.signal_store import PRODUCER_GRADES_KEY
            blob = await self.raw.get(PRODUCER_GRADES_KEY)
            # Only the families with a strong graded lift (producer_grades.
            # strong_move_families): every flagging family put 91 of 179 names at half weight.
            families = pg.strong_move_families(json.loads(blob) if blob else None)
            if not families:
                return None
            rows = await self.db.query(_RECENT_ALERTS_SQL, [str(n).upper() for n in names]) or []
            flagged = {str(r["ticker"]).upper() for r in rows if pg.rule_family(str(r["rule_name"])) in families}
        except Exception as exc:
            swallowed("agents.auto_trader.risk_weights", exc, logger)
            return None
        if flagged:
            logger.info("B1 alert risk: %s of %s names at %.2f weight (%s)", len(flagged), len(names), scale,
                        ", ".join(sorted(flagged)[:10]))
        return at.alert_risk_weights(names, flagged, scale)

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
        if len(pct) < at.MIN_SCORES_TO_REBUILD:
            dropped("agents.auto_trader.thin_scores", "too few model scores to rebuild the book; book kept", logger,
                    detail=f"{len(pct)} of at least {at.MIN_SCORES_TO_REBUILD}")
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

    async def _send(self, today: date, plan: at.Plan, ref: Optional[float] = None) -> bool:
        side = OrderSide.BUY if plan.side == "buy" else OrderSide.SELL
        try:
            # A marketable limit no worse than AUTO_TRADE_LIMIT_OFFSET from the
            # live price, so a thin book cannot fill an order anywhere. A
            # flip's close stays a market order: the open waits on it, and an
            # exit that does not complete is the larger risk. With no live
            # reference the order goes at market, bounded by the liquidity cap,
            # rather than at a limit around a stale close.
            if ref and ref > 0 and plan.reason != "flip-close":
                order = await self.broker.submit_order(
                    plan.symbol, plan.qty, side, order_type=OrderType.LIMIT,
                    limit_price=at.limit_price(plan.side, ref, _setting("AUTO_TRADE_LIMIT_OFFSET", 0.005)))
            else:
                order = await self.broker.submit_order(plan.symbol, plan.qty, side)
        except Exception as exc:
            await self.db.execute(_LOG_SQL, today, self.portfolio, plan.symbol, plan.side, plan.qty, plan.reason,
                                  None, "rejected", str(exc)[:500], ref)
            return False
        await self.db.execute(_LOG_SQL, today, self.portfolio, plan.symbol, plan.side, plan.qty, plan.reason,
                              order.order_id, "submitted", None, ref)
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


__all__ = ["AutoTrader", "BOOK_KEY", "DEFAULT_LEG", "DONE_KEY", "HALT_KEY", "PORTFOLIO", "capital_cap", "enabled",
           "leg_fraction"]

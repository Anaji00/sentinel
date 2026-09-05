"""
services/reasoning/strategy_backtester.py

Quantitative Strategy Backtesting Engine & Model Calibration Verifier.
Replays TimescaleDB CAGG historical price data through quantitative signal models
(Covered Calls, Momentum / Trend, Mean-Reversion), computing trade performance,
realized Sharpe, max drawdown, and empirical probability calibration curves.
"""

import json
import logging
import math
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import numpy as np

from shared.utils import quant_calc
from shared.utils.candles import candle_cache_key
from shared.utils.tasks import safe_create_task

logger = logging.getLogger("reasoning.backtester")

# Closed trades required before a backtest may approve a strategy for live use.
# The same number the quant engine uses before it will quote a payoff ratio.
MIN_TRADES_FOR_VALIDATION = 20

# Round-trip cost of a trade, in basis points of notional.
#
# The backtester priced every fill at the exact close, the exact stop and the
# exact target, with no commission, spread, slippage or borrow anywhere in the
# file. A frictionless backtest of a strategy that re-enters every few bars is
# not a conservative estimate of its edge; it is an estimate of a different
# strategy. Ten basis points round trip is deliberately modest for 5-minute
# bars in liquid US equities -- half-spread each way plus commission -- and it
# is charged against every trade before any gate reads the result.
ROUND_TRIP_COST_BPS = float(os.getenv("BACKTEST_ROUND_TRIP_BPS", "10"))

# Below this, the dispersion of a return series is not measurable and any ratio
# built on it is an artefact of the sample rather than a property of the
# strategy.
MIN_MEASURABLE_DISPERSION = 1e-6


def _trades_per_year(trades: list, total_bars: int = 0, timeframe: str = "") -> float:
    """How often this strategy trades.

    Derived from the bar count and the declared timeframe where both are known,
    because that is exact and does not depend on the feed's timestamp format:
    a backtest over N bars of a known width covers a known span, and the trade
    count over that span is the frequency.

    Timestamps are the fallback, not the primary source. They are supplied by
    whatever produced the bars and are not always well formed -- this
    repository's own fixture emits "2026-01-01T99:00:00Z", an hour that does not
    exist, for every bar past the twenty-fourth.

    Returns 0.0 when neither is usable, which the caller treats as "not
    annualisable" rather than substituting a convention. That refusal is the
    point: the previous code multiplied by sqrt(252) regardless, annualising
    per-trade returns as though each trade were a trading day.
    """
    if total_bars > 0 and timeframe:
        try:
            bars_per_year = quant_calc.periods_per_year(timeframe, "equity")
            if bars_per_year > 0:
                return len(trades) * float(bars_per_year) / float(total_bars)
        except Exception:
            pass

    stamps = []
    for t in trades:
        for key in ("entry_time", "exit_time"):
            v = t.get(key)
            if v is None:
                continue
            try:
                if isinstance(v, (int, float)):
                    stamps.append(float(v))
                else:
                    stamps.append(datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp())
            except (ValueError, TypeError, AttributeError):
                continue
    if len(stamps) < 2:
        return 0.0
    span_sec = max(stamps) - min(stamps)
    if span_sec <= 0:
        return 0.0
    return len(trades) * (365.25 * 24 * 3600.0) / span_sec



# Bars per year for the timeframe the backtester is handed, used to annualise.
BACKTEST_BAR_TIMEFRAME = os.getenv("BACKTEST_BAR_TIMEFRAME", "5m")

# Bounds on the volatility used to price the written call. The lower bound
# stops a degenerate window producing a free option; the upper stops one bad
# bar pricing a premium the market would never pay.
MIN_BACKTEST_SIGMA = 0.05
MAX_BACKTEST_SIGMA = 2.50

# Strike placement, in standard deviations of the move available over the
# holding period. Roughly a 30-delta call at one sigma; kept explicit so the
# constant and the comment cannot drift apart again.
STRIKE_SIGMA_MULT = 1.0





def _at(seq: List[Any], i: int) -> Any:
    """Safe positional access — external feeds return ragged parallel arrays."""
    return seq[i] if i < len(seq) else None


def _coerce_bar(c: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Converts a cached candle dict into a bar, or None if a price field is absent.

    Missing prices are dropped rather than defaulted, so a malformed cache entry
    can never enter a backtest disguised as a real observation.
    """
    close = c.get("close")
    if close is None:
        return None
    try:
        close_f = float(close)
        return {
            "timestamp": c.get("timestamp") or c.get("ts") or datetime.now(timezone.utc).isoformat(),
            "open": float(c.get("open", close_f)),
            "high": float(c.get("high", close_f)),
            "low": float(c.get("low", close_f)),
            "close": close_f,
            "volume": float(c.get("volume", 0.0)),
            "vwap": float(c.get("vwap", close_f)),
        }
    except (TypeError, ValueError):
        return None

# Minimum authentic bars required to produce a statistically meaningful backtest.
# Below this the engine reports INSUFFICIENT_DATA — it never interpolates or
# synthesizes bars, because a validation verdict derived from invented prices is
# worse than no verdict at all.
MIN_BARS_FOR_BACKTEST = 25

# Yahoo Finance interval strings keyed by Sentinel timeframe.
_YF_INTERVAL_MAP = {
    "5m": ("5m", "1mo"),
    "10m": ("15m", "1mo"),
    "15m": ("15m", "1mo"),
    "30m": ("30m", "3mo"),
    "1h": ("1h", "6mo"),
    "4h": ("1h", "1y"),
    "1d": ("1d", "2y"),
}


class StrategyBacktester:
    """
    Backtesting engine and validation gatekeeper for Sentinel quantitative models.
    """

    def __init__(self, db_client=None, redis_client=None):
        self.db = db_client
        self.redis = redis_client

    async def fetch_historical_bars(
        self,
        ticker: str,
        timeframe: str = "5m",
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        """
        Fetches historical OHLCV bars from TimescaleDB continuous aggregates or Redis.
        """
        bars = []
        ticker_upper = ticker.strip().upper()

        if self.db:
            table_map = {
                "5m": "tradfi_bars_5m",
                "10m": "tradfi_bars_10m",
                "15m": "tradfi_bars_15m",
                "30m": "tradfi_bars_30m",
                "1h": "tradfi_bars_1h",
                "4h": "tradfi_bars_4h",
                "1d": "tradfi_bars_1d",
            }
            target_table = table_map.get(timeframe, "tradfi_bars_5m")
            try:
                query = f"""
                    SELECT bucket_time, open, high, low, close, volume, vwap
                    FROM {target_table}
                    WHERE ticker = $1
                    ORDER BY bucket_time ASC
                    LIMIT $2;
                """
                rows = await self.db.query(query, ticker_upper, limit)
                for r in rows:
                    bars.append({
                        "timestamp": r["bucket_time"].isoformat() if hasattr(r["bucket_time"], "isoformat") else str(r["bucket_time"]),
                        "open": float(r["open"]),
                        "high": float(r["high"]),
                        "low": float(r["low"]),
                        "close": float(r["close"]),
                        "volume": float(r.get("volume", 0.0)),
                        "vwap": float(r.get("vwap", r["close"])),
                    })
            except Exception as e:
                logger.debug(f"TimescaleDB bar fetch for {ticker_upper} fallback: {e}")

        # 2. Redis candle cache. The key MUST carry the timeframe segment — every
        #    writer in the platform emits `sentinel:candles:{tf}:{ticker}`.
        if not bars and self.redis:
            try:
                cache_key = candle_cache_key(ticker_upper, timeframe)
                raw_candles = await self.redis.raw.lrange(cache_key, 0, limit)
                for item in raw_candles:
                    c = json.loads(item if isinstance(item, str) else item.decode("utf-8"))
                    bar = _coerce_bar(c)
                    if bar:
                        bars.append(bar)
            except Exception as e:
                logger.debug(f"Redis candle fallback for {ticker_upper}: {e}")

        # 3. External historical REST API for the missing window, then hydrate cache.
        if not bars:
            bars = await self._fetch_external_bars(ticker_upper, timeframe, limit)
            if bars:
                await self._hydrate_candle_cache(ticker_upper, timeframe, bars)

        # 4. Defensive fallback: fail closed. No interpolation, no synthesis.
        if not bars:
            logger.warning(
                "No authentic bars available for %s (%s) from TimescaleDB, Redis, or external API. "
                "Failing closed — backtest will report INSUFFICIENT_DATA.",
                ticker_upper,
                timeframe,
            )

        return bars

    async def _fetch_external_bars(
        self,
        ticker: str,
        timeframe: str,
        limit: int,
    ) -> List[Dict[str, Any]]:
        """
        Fetches authentic historical OHLCV bars from an external market data REST API.

        Returns an empty list on any error or empty response — the caller fails
        closed rather than substituting fabricated data.
        """
        interval, range_ = _YF_INTERVAL_MAP.get(timeframe, ("1h", "6mo"))
        url = (
            f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
            f"?range={range_}&interval={interval}"
        )
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        logger.warning(
                            "External bar fetch for %s returned HTTP %s.", ticker, resp.status
                        )
                        return []
                    data = await resp.json(content_type=None)

            results = (data.get("chart") or {}).get("result") or []
            if not results:
                return []

            chart = results[0]
            timestamps = chart.get("timestamp") or []
            quote_blocks = ((chart.get("indicators") or {}).get("quote") or [])
            if not timestamps or not quote_blocks:
                return []

            q = quote_blocks[0]
            opens, highs = q.get("open") or [], q.get("high") or []
            lows, closes = q.get("low") or [], q.get("close") or []
            volumes = q.get("volume") or []

            bars: List[Dict[str, Any]] = []
            for i, ts in enumerate(timestamps):
                o, h, l, c = (
                    _at(opens, i), _at(highs, i), _at(lows, i), _at(closes, i)
                )
                # Skip gap bars rather than forward-filling them.
                if None in (o, h, l, c):
                    continue
                bars.append({
                    "timestamp": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
                    "open": float(o),
                    "high": float(h),
                    "low": float(l),
                    "close": float(c),
                    "volume": float(_at(volumes, i) or 0.0),
                    "vwap": float(c),
                })

            if bars:
                logger.info(
                    "Hydrated %d authentic %s bars for %s from external API.",
                    len(bars), timeframe, ticker,
                )
            return bars[-limit:]

        except Exception as e:
            logger.warning("External historical bar fetch failed for %s: %s", ticker, e)
            return []

    async def _hydrate_candle_cache(
        self,
        ticker: str,
        timeframe: str,
        bars: List[Dict[str, Any]],
    ) -> None:
        """Writes freshly fetched authentic bars into the Redis candle cache."""
        if not self.redis or not bars:
            return
        try:
            cache_key = candle_cache_key(ticker, timeframe)
            pipe = self.redis.raw.pipeline()
            pipe.delete(cache_key)
            for bar in bars:
                pipe.rpush(cache_key, json.dumps(bar))
            pipe.ltrim(cache_key, 0, 999)
            pipe.expire(cache_key, 3600)
            await pipe.execute()
        except Exception as e:
            logger.debug(f"Candle cache hydration skipped for {ticker}: {e}")

    def _insufficient_data_report(
        self,
        ticker: str,
        strategy_type: str,
        initial_capital: float,
        bar_count: int,
    ) -> Dict[str, Any]:
        """
        Builds a fail-closed report for a strategy that could not be evaluated.

        Every metric is None rather than zero: a zero Sharpe is a measurement, and
        no measurement was made. The validation gate is explicitly not passed, so
        this report can never be mistaken for an approval.
        """
        return {
            "strategy_id": f"{strategy_type}_{ticker.lower()}",
            "strategy_name": strategy_type.replace("_", " ").title(),
            "ticker": ticker.upper(),
            "backtested_at": datetime.now(timezone.utc).isoformat(),
            "bar_count": int(bar_count),
            "data_provenance": "insufficient_data",
            "initial_capital_usd": float(initial_capital),
            "final_capital_usd": None,
            "performance_metrics": {
                "total_trades": 0,
                "winning_trades": 0,
                "losing_trades": 0,
                "hit_rate_pct": None,
                "profit_factor": None,
                "total_return_pct": None,
                "benchmark_return_pct": None,
                "alpha_pct": None,
            },
            "risk_metrics": {
                "realized_sharpe_ratio": None,
                "sortino_ratio": None,
                "max_drawdown_pct": None,
                "expected_value_per_trade_usd": None,
                "payoff_ratio": None,
                "avg_win_usd": None,
                "avg_loss_usd": None,
            },
            "calibration_curve": [],
            "brier_score": None,
            "validation_gate": {
                "passed": False,
                "status": "INSUFFICIENT_DATA",
                "reason": (
                    f"{bar_count} authentic bars available; "
                    f"{MIN_BARS_FOR_BACKTEST} required. No synthetic data substituted."
                ),
                "criteria": {},
            },
            "equity_curve": [],
            "recent_trades": [],
        }

    def backtest_strategy(
        self,
        ticker: str,
        bars: List[Dict[str, Any]],
        strategy_type: str = "covered_call",
        initial_capital: float = 100_000.0,
    ) -> Dict[str, Any]:
        """
        Executes discrete chronological backtest across historical bars.
        """
        if len(bars) < MIN_BARS_FOR_BACKTEST:
            # Fail closed. A validation verdict computed from fabricated prices is
            # indistinguishable downstream from a real one, so no verdict is issued.
            logger.warning(
                "Backtest for %s aborted: %d authentic bars available, %d required.",
                ticker.upper(), len(bars), MIN_BARS_FOR_BACKTEST,
            )
            return self._insufficient_data_report(
                ticker=ticker,
                strategy_type=strategy_type,
                initial_capital=initial_capital,
                bar_count=len(bars),
            )

        closes = [b["close"] for b in bars]
        highs = [b["high"] for b in bars]
        lows = [b["low"] for b in bars]
        timestamps = [b["timestamp"] for b in bars]

        trades: List[Dict[str, Any]] = []
        equity_curve: List[Dict[str, Any]] = []
        capital = initial_capital
        equity_curve.append({"timestamp": timestamps[0], "portfolio_equity": capital, "benchmark_equity": capital})

        # Precompute rolling returns & ATR
        returns = np.diff(closes) / (np.array(closes[:-1]) + 1e-8)
        tr_list = [highs[0] - lows[0]]
        for i in range(1, len(closes)):
            tr = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
            tr_list.append(tr)

        benchmark_initial_price = closes[0]

        # ── 1. EXECUTE STRATEGY LOGIC ACROSS CHRONOLOGICAL TIMESTAMPS ───────────
        if strategy_type == "covered_call":
            # Replay Covered Call Overlay Logic (§3.4, §B.1)
            # Sells 30-day Call when Z-Score >= +2.5
            i = 20
            while i < len(closes) - 1:
                window_ret = returns[max(0, i - 20):i]
                std_ret = np.std(window_ret) if len(window_ret) > 1 else 0.01
                mean_ret = np.mean(window_ret) if len(window_ret) > 1 else 0.0
                curr_ret = returns[i - 1] if i > 0 else 0.0
                z_score = (curr_ret - mean_ret) / max(1e-4, std_ret)

                if z_score >= 2.0:  # Statistically elevated rally
                    entry_price = closes[i]

                    # The option's life is the holding period, not thirty days.
                    #
                    # This priced T = 30/365 and then held the position
                    # min(6, ...) bars. On the 5-minute bars this backtester is
                    # actually handed, that is thirty MINUTES against a
                    # thirty-DAY premium -- a 1,440x mismatch -- and the payoff
                    # below booked the full premium in both branches, on losing
                    # trades as well as winning ones. At the parameters the code
                    # used that injected +0.104% of position into every trade,
                    # or +2.09% of capital over the twenty-trade validation
                    # minimum, inflating hit rate, profit factor and total
                    # return together.
                    holding_period = min(6, len(closes) - 1 - i)
                    exit_idx = i + holding_period
                    exit_price = closes[exit_idx]
                    bars_per_year = quant_calc.periods_per_year(BACKTEST_BAR_TIMEFRAME, "equity")
                    T_years = max(1e-6, holding_period / max(1.0, bars_per_year))

                    # Volatility annualised for the bars in hand.
                    #
                    # `std_ret * sqrt(252)` applied the daily constant to
                    # 5-minute returns, understating by sqrt(78) = 8.83x. A
                    # typical 5-minute dispersion then came out at 0.019, far
                    # below the 0.15 floor -- so sigma was pinned at exactly
                    # 0.15 for every trade of every ticker, and the strike sat a
                    # fixed 7.5% out of the money however volatile the name was.
                    sigma = float(std_ret * math.sqrt(max(1.0, bars_per_year)))
                    sigma = max(MIN_BACKTEST_SIGMA, min(MAX_BACKTEST_SIGMA, sigma))

                    # Strike scaled by the move actually available over this
                    # horizon: sigma * sqrt(T), not the bare annual sigma. The
                    # old form multiplied an annual figure by 0.5 and called the
                    # result a 30-delta strike, which it was not in any units.
                    strike = round(entry_price * (1.0 + STRIKE_SIGMA_MULT * sigma * math.sqrt(T_years)), 2)
                    call_premium = round(quant_calc.black_scholes_call_price(
                        S=entry_price, K=strike, T=T_years, r=0.045, sigma=sigma
                    ), 2)
                    conviction = round(min(0.95, 0.50 + 0.10 * z_score), 2)

                    # Payoff: If exit_price > strike: capped gain (strike - entry) + premium
                    # If exit_price <= strike: stock gain/loss (exit - entry) + premium
                    if exit_price > strike:
                        stock_pnl = strike - entry_price
                        option_pnl = call_premium
                    else:
                        stock_pnl = exit_price - entry_price
                        option_pnl = call_premium

                    gross_pnl = stock_pnl + option_pnl
                    # Two legs, so two round trips of cost: the stock and the
                    # option. Charged on notional, like the other strategies.
                    cost = entry_price * (2.0 * ROUND_TRIP_COST_BPS / 10000.0)
                    total_pnl = gross_pnl - cost
                    pnl_pct = (total_pnl / entry_price) * 100.0
                    trade_won = total_pnl > 0

                    position_size = capital * 0.20
                    dollar_pnl = position_size * (total_pnl / entry_price)
                    capital += dollar_pnl

                    trades.append({
                        "trade_id": f"trade_{len(trades)+1}",
                        "entry_time": timestamps[i],
                        "exit_time": timestamps[exit_idx],
                        "strategy": "covered_call",
                        "entry_price": float(entry_price),
                        "strike": float(strike),
                        "call_premium": float(call_premium),
                        "exit_price": float(exit_price),
                        "pnl_usd": float(round(dollar_pnl, 2)),
                        "pnl_pct": float(round(pnl_pct, 2)),
                        "conviction_score": float(conviction),
                        "won": bool(trade_won),
                        "z_score_trigger": float(round(z_score, 2)),
                    })

                    bench_val = initial_capital * (closes[exit_idx] / benchmark_initial_price)
                    equity_curve.append({
                        "timestamp": timestamps[exit_idx],
                        "portfolio_equity": float(round(capital, 2)),
                        "benchmark_equity": float(round(bench_val, 2)),
                    })
                    i = exit_idx + 1
                else:
                    i += 1

        elif strategy_type == "momentum_trend":
            # Momentum / Trend Following strategy
            i = 20
            while i < len(closes) - 1:
                atr = sum(tr_list[max(0, i - 14):i]) / 14.0
                curr_price = closes[i]
                # Divided by what the window actually holds.
                #
                # `sum(closes[max(0, i-26):i]) / 26.0` with the loop starting at
                # i=20 averaged as few as twenty bars over a divisor of
                # twenty-six -- 76.92 where the true mean is 100.00, a 23%
                # understatement. Entry requires fast > slow, so depressing the
                # slow average manufactured entries in the same six bars of
                # every backtest ever run: 846 warm-up entries as written
                # against 485 computed correctly, across 300 synthetic runs.
                #
                # Named as what they are. These are simple moving averages; the
                # names said EMA, which is a different filter and the kind of
                # mismatch that survives review because the name reads correctly.
                _fast_win = closes[max(0, i - 12):i]
                _slow_win = closes[max(0, i - 26):i]
                fast_sma = sum(_fast_win) / max(1, len(_fast_win))
                slow_sma = sum(_slow_win) / max(1, len(_slow_win))

                if fast_sma > slow_sma and closes[i] > fast_sma:
                    entry_price = curr_price
                    stop_loss = entry_price - (atr * 1.5)
                    target_price = entry_price + (atr * 3.0)
                    conviction = 0.72

                    holding_period = min(10, len(closes) - 1 - i)
                    exit_idx = i + holding_period
                    exit_price = closes[exit_idx]

                    # Check intra-period stop/target hit
                    won = exit_price >= entry_price
                    for k in range(i + 1, exit_idx + 1):
                        if lows[k] <= stop_loss:
                            exit_price = stop_loss
                            exit_idx = k
                            won = False
                            break
                        elif highs[k] >= target_price:
                            exit_price = target_price
                            exit_idx = k
                            won = True
                            break

                    gross_pct = (exit_price - entry_price) / entry_price
                    net_pct = gross_pct - (ROUND_TRIP_COST_BPS / 10000.0)
                    dollar_pnl = (capital * 0.15) * net_pct
                    capital += dollar_pnl
                    # A trade that finishes exactly flat, or that loses only to
                    # costs, is not a win. `won = exit_price >= entry_price`
                    # counted both.
                    won = net_pct > 0.0

                    trades.append({
                        "trade_id": f"trade_{len(trades)+1}",
                        "entry_time": timestamps[i],
                        "exit_time": timestamps[exit_idx],
                        "strategy": "momentum_trend",
                        "entry_price": float(entry_price),
                        "exit_price": float(round(exit_price, 2)),
                        "gross_pnl_pct": float(round(gross_pct * 100.0, 4)),
                        "cost_pct": float(ROUND_TRIP_COST_BPS / 100.0),
                        "pnl_usd": float(round(dollar_pnl, 2)),
                        # Net of costs, because this is the field every gate,
                        # the hit rate, the profit factor and the Sharpe all
                        # read. Gross is kept alongside it for inspection.
                        "pnl_pct": float(round(net_pct * 100.0, 4)),
                        "conviction_score": float(conviction),
                        "won": bool(won),
                        "z_score_trigger": 0.0,
                    })

                    bench_val = initial_capital * (closes[exit_idx] / benchmark_initial_price)
                    equity_curve.append({
                        "timestamp": timestamps[exit_idx],
                        "portfolio_equity": float(round(capital, 2)),
                        "benchmark_equity": float(round(bench_val, 2)),
                    })
                    i = exit_idx + 1
                else:
                    i += 1

        else:  # Mean reversion default
            i = 20
            while i < len(closes) - 1:
                window_ret = returns[max(0, i - 20):i]
                std_ret = np.std(window_ret) if len(window_ret) > 1 else 0.01
                mean_ret = np.mean(window_ret) if len(window_ret) > 1 else 0.0
                z_score = (returns[i - 1] - mean_ret) / max(1e-4, std_ret)

                if z_score <= -2.0:  # Oversold mean reversion
                    entry_price = closes[i]
                    atr = sum(tr_list[max(0, i - 14):i]) / 14.0
                    target_price = entry_price + (atr * 2.0)
                    stop_loss = entry_price - (atr * 1.5)
                    conviction = round(min(0.90, 0.50 + 0.10 * abs(z_score)), 2)

                    holding_period = min(8, len(closes) - 1 - i)
                    exit_idx = i + holding_period
                    exit_price = closes[exit_idx]

                    won = exit_price >= entry_price
                    for k in range(i + 1, exit_idx + 1):
                        if lows[k] <= stop_loss:
                            exit_price = stop_loss
                            exit_idx = k
                            won = False
                            break
                        elif highs[k] >= target_price:
                            exit_price = target_price
                            exit_idx = k
                            won = True
                            break

                    gross_pct = (exit_price - entry_price) / entry_price
                    net_pct = gross_pct - (ROUND_TRIP_COST_BPS / 10000.0)
                    dollar_pnl = (capital * 0.15) * net_pct
                    capital += dollar_pnl
                    won = net_pct > 0.0

                    trades.append({
                        "trade_id": f"trade_{len(trades)+1}",
                        "entry_time": timestamps[i],
                        "exit_time": timestamps[exit_idx],
                        "strategy": "mean_reversion",
                        "gross_pnl_pct": float(round(gross_pct * 100.0, 4)),
                        "cost_pct": float(ROUND_TRIP_COST_BPS / 100.0),
                        "entry_price": float(entry_price),
                        "exit_price": float(round(exit_price, 2)),
                        "pnl_usd": float(round(dollar_pnl, 2)),
                        "pnl_pct": float(round(net_pct * 100.0, 4)),
                        "conviction_score": float(conviction),
                        "won": bool(won),
                        "z_score_trigger": float(round(z_score, 2)),
                    })

                    bench_val = initial_capital * (closes[exit_idx] / benchmark_initial_price)
                    equity_curve.append({
                        "timestamp": timestamps[exit_idx],
                        "portfolio_equity": round(capital, 2),
                        "benchmark_equity": round(bench_val, 2),
                    })
                    i = exit_idx + 1
                else:
                    i += 1

        # ── 2. CALCULATE QUANTITATIVE PERFORMANCE & RISK STATISTICS ────────────
        total_trades = len(trades)
        winning_trades = [t for t in trades if t["won"]]
        losing_trades = [t for t in trades if not t["won"]]

        hit_rate_pct = round((len(winning_trades) / max(1, total_trades)) * 100.0, 2)
        total_profit = sum(t["pnl_usd"] for t in winning_trades)
        total_loss = abs(sum(t["pnl_usd"] for t in losing_trades))
        profit_factor = round(total_profit / max(1.0, total_loss), 2)

        total_return_pct = round(((capital - initial_capital) / initial_capital) * 100.0, 2)
        # Guarded: closes[0] is 0.0 for any bar that arrived without a close
        # price, and an unguarded divide made the benchmark -- and therefore
        # alpha -- infinite for the whole backtest.
        benchmark_return_pct = (
            round(((closes[-1] - closes[0]) / closes[0]) * 100.0, 2)
            if closes and closes[0] > 0 else 0.0
        )
        alpha_pct = round(total_return_pct - benchmark_return_pct, 2)

        # Max Drawdown calculation across portfolio equity curve
        eq_values = [p["portfolio_equity"] for p in equity_curve]
        peak = eq_values[0]
        max_dd = 0.0
        for val in eq_values:
            if val > peak:
                peak = val
            dd = (peak - val) / peak
            if dd > max_dd:
                max_dd = dd
        max_drawdown_pct = round(max_dd * 100.0, 2)

        # Realized Sharpe & Sortino ratios
        trade_returns = [t["pnl_pct"] / 100.0 for t in trades]
        if len(trade_returns) >= 2:
            mean_tr = float(np.mean(trade_returns))
            std_tr = float(np.std(trade_returns, ddof=1))
            losers = [r for r in trade_returns if r < 0]
            downside_std = float(np.std(losers, ddof=1)) if len(losers) >= 2 else None

            # Annualised by how often this strategy actually trades.
            #
            # This was `* sqrt(252)` over a series with one entry per *trade*,
            # which annualises as though every trade were a trading day. The
            # correct factor is sqrt(trades per year), and the trade records
            # carry entry_time and exit_time, so the span is measurable. A
            # strategy making 20 trades in three months has 80 a year: factor
            # 8.9, against the 15.9 assumed, overstating Sharpe by 1.8x. A
            # high-frequency strategy is understated by the same argument.
            trades_per_year = _trades_per_year(
                trades, total_bars=len(closes), timeframe=BACKTEST_BAR_TIMEFRAME
            )
            ann = math.sqrt(trades_per_year) if trades_per_year > 0 else None

            # No floor on the denominator.
            #
            # `max(1e-4, std)` divided by a ten-thousandth when a strategy's
            # returns barely varied, which is how five trades produced a Sharpe
            # of 24.91. A dispersion too small to measure makes the ratio
            # undefined, not enormous.
            if ann is None or std_tr < MIN_MEASURABLE_DISPERSION:
                sharpe_ratio = None
            else:
                sharpe_ratio = round((mean_tr / std_tr) * ann, 2)

            # Sortino needs losses to have a downside deviation. Previously a
            # strategy with no losing trades was given 1e-4 and came back with
            # a near-infinite ratio; the honest answer is that it has no
            # measurable downside yet.
            if ann is None or downside_std is None or downside_std < MIN_MEASURABLE_DISPERSION:
                sortino_ratio = None
            else:
                sortino_ratio = round((mean_tr / downside_std) * ann, 2)
        else:
            sharpe_ratio = None
            sortino_ratio = None

        avg_win_usd = round(total_profit / max(1, len(winning_trades)), 2)
        avg_loss_usd = round(total_loss / max(1, len(losing_trades)), 2)
        payoff_ratio = round(avg_win_usd / max(1.0, avg_loss_usd), 2)
        expected_value_usd = round(
            ((hit_rate_pct / 100.0) * avg_win_usd) - (((100.0 - hit_rate_pct) / 100.0) * avg_loss_usd), 2
        )

        # ── 3. EMPIRICAL PROBABILITY CALIBRATION CURVE (§B.1) ───────────────────
        # Bins predicted conviction probabilities [0.4-0.6, 0.6-0.8, 0.8-1.0] vs realized win rate
        bins = [(0.40, 0.60), (0.60, 0.80), (0.80, 1.00)]
        calibration_curve = []
        brier_sum = 0.0

        for b_min, b_max in bins:
            bin_trades = [t for t in trades if b_min <= t["conviction_score"] < b_max]
            if not bin_trades:
                mid = (b_min + b_max) / 2.0
                calibration_curve.append({
                    "probability_bin": f"{int(b_min*100)}-{int(b_max*100)}%",
                    "mean_predicted_prob": round(mid, 2),
                    "empirical_win_rate": None,
                    "trade_count": 0,
                })
            else:
                bin_won = [t for t in bin_trades if t["won"]]
                mean_pred = sum(t["conviction_score"] for t in bin_trades) / max(1, len(bin_trades))
                emp_win = len(bin_won) / max(1, len(bin_trades))
                calibration_curve.append({
                    "probability_bin": f"{int(b_min*100)}-{int(b_max*100)}%",
                    "mean_predicted_prob": round(mean_pred, 2),
                    "empirical_win_rate": round(emp_win, 2),
                    "trade_count": len(bin_trades),
                })

        for t in trades:
            pred = t["conviction_score"]
            actual = 1.0 if t["won"] else 0.0
            brier_sum += (pred - actual) ** 2
        brier_score = round(brier_sum / max(1, total_trades), 4)

        # ── 4. VALIDATION GATE DECISION (§B.1) ──────────────────────────────────
        # A strategy passes validation if Sharpe >= 1.0, Hit Rate >= 50%, Max DD <= 20%
        # Enough trades for the statistics to mean anything, before anything else.
        #
        # The gate had no sample-size term, and live on 4 September it marked
        # mean_reversion_abev APPROVED_FOR_LIVE on five trades with a 100% hit
        # rate and a Sharpe of 24.91. That is not a strategy, it is what a
        # five-sample Sharpe looks like: the ratio divides by a standard
        # deviation estimated from four degrees of freedom, and on a lucky run
        # it goes to the moon.
        #
        # Twenty is the threshold the platform already uses -- the quant engine
        # refuses to trust a backtest payoff ratio below MIN_TRADES_FOR_PAYOFF
        # for exactly this reason. The gate that decides whether a strategy may
        # go live should not be more permissive than the one that decides
        # whether its payoff ratio is quotable.
        passed_validation = bool(
            total_trades >= MIN_TRADES_FOR_VALIDATION and
            hit_rate_pct >= 50.0 and
            sharpe_ratio is not None and
            sharpe_ratio >= 1.0 and
            max_drawdown_pct <= 20.0 and
            profit_factor >= 1.2
        )

        report = {
            "strategy_id": f"{strategy_type}_{ticker.lower()}",
            "strategy_name": strategy_type.replace("_", " ").title(),
            "ticker": ticker.upper(),
            "backtested_at": datetime.now(timezone.utc).isoformat(),
            "bar_count": int(len(bars)),
            "data_provenance": "authentic_market_data",
            "initial_capital_usd": float(initial_capital),
            "final_capital_usd": float(round(capital, 2)),
            "performance_metrics": {
                "total_trades": int(total_trades),
                "winning_trades": int(len(winning_trades)),
                "losing_trades": int(len(losing_trades)),
                "hit_rate_pct": float(hit_rate_pct),
                "profit_factor": float(profit_factor),
                "total_return_pct": float(total_return_pct),
                "benchmark_return_pct": float(benchmark_return_pct),
                "alpha_pct": float(alpha_pct),
            },
            "risk_metrics": {
                "realized_sharpe_ratio": float(sharpe_ratio) if sharpe_ratio is not None else None,
                "sortino_ratio": float(sortino_ratio) if sortino_ratio is not None else None,
                "max_drawdown_pct": float(max_drawdown_pct),
                "expected_value_per_trade_usd": float(expected_value_usd),
                "payoff_ratio": float(payoff_ratio),
                "avg_win_usd": float(avg_win_usd),
                "avg_loss_usd": float(avg_loss_usd),
            },
            "calibration_curve": calibration_curve,
            "brier_score": float(brier_score),
            "validation_gate": {
                "passed": bool(passed_validation),
                "min_trades_met": bool(total_trades >= MIN_TRADES_FOR_VALIDATION),
                "status": "APPROVED_FOR_LIVE" if passed_validation else "REJECTED_GATED",
                "criteria": {
                    "min_trades_20": bool(total_trades >= MIN_TRADES_FOR_VALIDATION),
                    "min_hit_rate_50pct": bool(hit_rate_pct >= 50.0),
                    "min_sharpe_1_0": bool(sharpe_ratio is not None and sharpe_ratio >= 1.0),
                    "max_drawdown_20pct": bool(max_drawdown_pct <= 20.0),
                    "min_profit_factor_1_2": bool(profit_factor >= 1.2),
                }
            },
            "equity_curve": equity_curve[-30:],  # Tail points for plotting
            "recent_trades": trades[-10:],
        }

        # The caller persists this, not this function.
        #
        # The write was `safe_create_task(self.redis.raw.set(...))` inside a
        # *synchronous* method, and the scheduled refresh offloads that method to
        # a worker thread so it does not block the event loop. There is no
        # running loop on a worker thread, so asyncio.create_task raised, the
        # surrounding except logged it at debug, and sentinel:backtest:results:*
        # stayed empty while the backtests themselves ran correctly and reported
        # their results to the log.
        #
        # Returning the report and letting the async caller store it puts the
        # write back on the loop that owns the Redis connection.

        logger.info(
            f"📈 Backtest complete for {report['strategy_id']}: Trades={total_trades} | "
            f"HitRate={hit_rate_pct}% | Sharpe={sharpe_ratio} | Ret={total_return_pct}% | Gate={report['validation_gate']['status']}"
        )
        return report

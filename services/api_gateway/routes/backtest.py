"""
services/api_gateway/routes/backtest.py

REST endpoints for running quantitative strategy backtests and inspecting
validation gates and probability calibration curves before signals are trusted live.
"""

import json
import logging
from typing import Any, List
from fastapi import APIRouter, Depends
from shared.utils.rbac import require_role, Role
from pydantic import BaseModel, Field, field_validator

from services.api_gateway.dependencies import get_db_optional, get_redis_optional
from services.reasoning.strategy_backtester import (
    StrategyBacktester,
    SUPPORTED_TIMEFRAMES,
)

logger = logging.getLogger("api-gateway.backtest")
from shared.utils.rates import risk_free_rate
from shared.utils.quiet_failures import swallowed
from shared.utils.response_cache import cache_key, cached_json

router = APIRouter(prefix="/api/v1/backtest", tags=["Strategy Backtesting & Model Validation"])


class RunBacktestRequest(BaseModel):
    ticker: str = Field(default="NVDA", description="Equity ticker symbol")
    strategy_type: str = Field(default="covered_call", description="'covered_call', 'momentum_trend', or 'mean_reversion'")
    # Validated, not described. The previous free-text hint offered '10m',
    # which no continuous aggregate implements.
    timeframe: str = Field(
        default="5m",
        description="One of " + ", ".join(SUPPORTED_TIMEFRAMES),
    )

    @field_validator("timeframe")
    @classmethod
    def _known_timeframe(cls, v: str) -> str:
        if v not in SUPPORTED_TIMEFRAMES:
            raise ValueError(
                f"unsupported timeframe {v!r}; the platform stores "
                + ", ".join(SUPPORTED_TIMEFRAMES)
            )
        return v
    initial_capital: float = Field(default=100_000.0, ge=1000.0, le=10_000_000.0)


@router.post(
    "/run",
    # Launches a backtest. Unguarded, any authenticated caller could start
    # arbitrary compute on the shared pool.
    dependencies=[Depends(require_role(Role.ANALYST))],
)
async def run_strategy_backtest(
    req: RunBacktestRequest,
    db=Depends(get_db_optional),
    redis=Depends(get_redis_optional),
):
    """
    Replays historical CAGG price series through the specified strategy logic
    and evaluates realized Sharpe, hit rate, max drawdown, and probability calibration.
    """
    backtester = StrategyBacktester(db_client=db, redis_client=redis)
    bars = await backtester.fetch_historical_bars(ticker=req.ticker, timeframe=req.timeframe, limit=500)
    report = backtester.backtest_strategy(
        ticker=req.ticker,
        bars=bars,
        strategy_type=req.strategy_type,
        initial_capital=req.initial_capital,
        # The short rate the platform measured, resolved here because the
        # backtest itself is synchronous. Without it the option leg was priced
        # at a hardcoded 4.5%.
        risk_free=(await risk_free_rate(redis))[0],
    )
    # The list is cached; drop it so the run just made shows up at once.
    if redis:
        try:
            await redis.raw.delete(cache_key("backtest_results"))
        except Exception as e:
            swallowed("api_gateway.routes.backtest.invalidate", e, logger)
    return report


@router.get("/results")
async def get_all_backtest_results(redis=Depends(get_redis_optional), db=Depends(get_db_optional)):
    """
    Returns the latest backtest performance summaries and validation gate statuses across strategies.

    Cached ten minutes: listing the results scans the whole keyspace, 3.7 s
    measured on 30 October.
    """
    return await cached_json(getattr(redis, "raw", None), cache_key("backtest_results"),
                             lambda: _backtest_results(redis, db), ttl=600, max_stale=86400)


# Per-trade detail the summary table never shows: 2.7 MB of the 3.6 MB list
# across 653 results on 30 October. /results/{strategy_id} still returns it.
LIST_OMITS = ("recent_trades", "equity_curve")


def _summary(result: Any) -> Any:
    if not isinstance(result, dict):
        return result
    return {k: v for k, v in result.items() if k not in LIST_OMITS}


async def _backtest_results(redis, db) -> List[Any]:
    results = []
    if redis:
        try:
            # SCAN, not KEYS. KEYS blocks the single-threaded server for the
            # whole traversal, and this instance carries six figures of keys --
            # one unauthenticated call to a reporting endpoint would stall every
            # collector, agent and enricher sharing the connection.
            async for k in redis.raw.scan_iter(match="sentinel:backtest:results:*", count=100):
                raw = await redis.raw.get(k)
                if raw:
                    results.append(json.loads(raw if isinstance(raw, str) else raw.decode("utf-8")))
        except Exception as e:
            logger.debug(f"Redis backtest fetch fallback: {e}")

    # If empty, generate standard baseline reports for default strategies
    if not results:
        backtester = StrategyBacktester(db_client=db, redis_client=redis)
        # Resolved once for the loop rather than per ticker.
        rf, _ = await risk_free_rate(redis)
        for ticker, strat in [("NVDA", "covered_call"), ("AAPL", "momentum_trend"), ("MSFT", "mean_reversion")]:
            bars = await backtester.fetch_historical_bars(ticker=ticker, timeframe="5m", limit=200)
            rep = backtester.backtest_strategy(
                ticker=ticker, bars=bars, strategy_type=strat, risk_free=rf
            )
            results.append(rep)

    return [_summary(r) for r in results]


@router.get("/results/{strategy_id}")
async def get_strategy_backtest_detail(
    strategy_id: str,
    redis=Depends(get_redis_optional),
    db=Depends(get_db_optional),
):
    """
    Returns granular backtest results, trade logs, equity curves, and calibration curve for a specific strategy ID.
    """
    if redis:
        try:
            raw = await redis.raw.get(f"sentinel:backtest:results:{strategy_id.lower()}")
            if raw:
                return json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
        except Exception as e:
            logger.debug(f"Redis lookup for {strategy_id} fallback: {e}")

    # Parse strategy_id e.g. "covered_call_nvda"
    parts = strategy_id.split("_")
    ticker = parts[-1].upper() if parts else "NVDA"
    strat_type = "_".join(parts[:-1]) if len(parts) > 1 else "covered_call"

    backtester = StrategyBacktester(db_client=db, redis_client=redis)
    bars = await backtester.fetch_historical_bars(ticker=ticker, timeframe="5m", limit=300)
    return backtester.backtest_strategy(
        ticker=ticker, bars=bars, strategy_type=strat_type,
        risk_free=(await risk_free_rate(redis))[0],
    )

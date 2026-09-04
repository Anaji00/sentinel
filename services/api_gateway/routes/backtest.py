"""
services/api_gateway/routes/backtest.py

REST endpoints for running quantitative strategy backtests and inspecting
validation gates and probability calibration curves before signals are trusted live.
"""

import json
import logging
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from services.api_gateway.dependencies import get_db_optional, get_redis_optional
from services.reasoning.strategy_backtester import StrategyBacktester

logger = logging.getLogger("api-gateway.backtest")
router = APIRouter(prefix="/api/v1/backtest", tags=["Strategy Backtesting & Model Validation"])


class RunBacktestRequest(BaseModel):
    ticker: str = Field(default="NVDA", description="Equity ticker symbol")
    strategy_type: str = Field(default="covered_call", description="'covered_call', 'momentum_trend', or 'mean_reversion'")
    timeframe: str = Field(default="5m", description="'5m', '10m', '15m', '30m', '1h', '4h', '1d'")
    initial_capital: float = Field(default=100_000.0, ge=1000.0, le=10_000_000.0)


@router.post("/run")
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
    )
    return report


@router.get("/results")
async def get_all_backtest_results(redis=Depends(get_redis_optional), db=Depends(get_db_optional)):
    """
    Returns the latest backtest performance summaries and validation gate statuses across strategies.
    """
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
        for ticker, strat in [("NVDA", "covered_call"), ("AAPL", "momentum_trend"), ("MSFT", "mean_reversion")]:
            bars = await backtester.fetch_historical_bars(ticker=ticker, timeframe="5m", limit=200)
            rep = backtester.backtest_strategy(ticker=ticker, bars=bars, strategy_type=strat)
            results.append(rep)

    return results


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
    return backtester.backtest_strategy(ticker=ticker, bars=bars, strategy_type=strat_type)

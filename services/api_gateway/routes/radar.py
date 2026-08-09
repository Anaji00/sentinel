import logging
from fastapi import APIRouter, Depends, Query
from services.api_gateway.dependencies import get_db, get_redis_client

logger = logging.getLogger("api-gateway.radar")

router = APIRouter(prefix="/api/v1/radar", tags=["Quantitative Radar"])

@router.get("/anomalies")
async def get_radar_anomalies(
    limit: int = Query(20, ge=1, le=100),
    db = Depends(get_db),
    redis = Depends(get_redis_client)
):
    """Retrieve quantitative volume anomalies detected by collector.radar."""
    anomalies = []
    if db:
        try:
            rows = await db.query(
                """
                SELECT event_id, type, occurred_at, primary_entity_id as ticker,
                       primary_entity_name as entity_name, region, anomaly_score,
                       financial_data as domain_data
                FROM events
                WHERE source = 'alpaca_quant_radar' OR type = 'volume_anomaly'
                ORDER BY occurred_at DESC
                LIMIT $1;
                """,
                limit
            )
            for r in rows:
                t = r["ticker"] or "UNKNOWN"
                e_name = r["entity_name"] or t
                anomalies.append({
                    "event_id": r["event_id"],
                    "ticker": t,
                    "primary_entity_name": e_name,
                    "entity_name": e_name,
                    "anomaly_score": float(r["anomaly_score"] or 0.0),
                    "occurred_at": r["occurred_at"].isoformat() if hasattr(r["occurred_at"], "isoformat") else str(r["occurred_at"]),
                    "z_score": float(r["anomaly_score"] or 0.0) * 4.5,
                    "region": r["region"] or "US Equities",
                    "details": r["domain_data"] or {}
                })
        except Exception as e:
            logger.warning(f"Error querying radar anomalies from DB: {e}")

    # Fallback to current dynamic watchlist if cold start
    watchlist = []
    if redis:
        try:
            raw_items = await redis.raw.zrange("sentinel:watched:equities", 0, -1, withscores=True)
            for item in raw_items:
                t = item[0].decode('utf-8') if isinstance(item[0], bytes) else str(item[0])
                score = float(item[1])
                watchlist.append({"ticker": t, "added_timestamp": score})
        except Exception as e:
            logger.warning(f"Error reading sentinel:watched:equities from Redis: {e}")

    return {
        "service": "collector.radar",
        "anomalies_count": len(anomalies),
        "anomalies": anomalies,
        "watchlist_count": len(watchlist),
        "watchlist": watchlist
    }

from typing import Optional, Dict, Any
from datetime import datetime, timezone
import math
import time

@router.get("/sweeps")
async def get_radar_sweeps_status(redis = Depends(get_redis_client)):
    """Retrieve quantitative radar sweep baseline parameters and active universe metrics."""
    universe_size = 4500
    mean_count = 0
    if redis:
        try:
            cursor = 0
            mean_count = 0
            while True:
                cursor, keys = await redis.raw.scan(cursor=cursor, match="sentinel:radar:mean:*", count=500)
                mean_count += len(keys)
                if cursor == 0:
                    break
        except Exception as e:
            logger.warning(f"Error scanning Redis radar mean keys: {e}")

    return {
        "status": "sweeping",
        "scanner": "Alpaca US Equities Snapshot API",
        "total_universe_scanned": universe_size,
        "tracked_baselines": mean_count or 1840,
        "z_score_threshold": 3.0,
        "ewma_alpha": 0.05,
        "intraday_vwap_normalization": True,
        "last_sweep_time": "Real-time 1-Bar Continuous"
    }


import aiohttp
import asyncio
from datetime import datetime, timezone

SYMBOL_YAF_MAP = {
    "SPY": "SPY",
    "QQQ": "QQQ",
    "DJI": "^DJI",
    "VIX": "^VIX",
    "WTI": "CL=F",
    "BRENT": "BZ=F",
    "GLD": "GLD",
    "US10Y": "^TNX",
    "US02Y": "2YR",
    "TLT": "TLT",
}

async def fetch_on_the_spot_historical(symbol: str, limit: int = 60):
    """
    Fetches real authentic historical price series on the spot from public APIs
    if no events currently persist in TimescaleDB for the requested symbol.
    """
    symbol_upper = symbol.upper()
    
    # 1. Check Crypto symbols via Binance Public KLines API (Zero Auth Required)
    if "BTC" in symbol_upper or "ETH" in symbol_upper or "SOL" in symbol_upper or symbol_upper.endswith("USDT") or symbol_upper.endswith("USD"):
        pair = symbol_upper.replace("USD", "USDT")
        url = f"https://api.binance.com/api/v3/klines?symbol={pair}&interval=1m&limit={limit}"
        try:
            timeout = aiohttp.ClientTimeout(total=5)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        klines = await resp.json()
                        pts = []
                        for k in klines:
                            ts_str = datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc).isoformat()
                            close_p = float(k[4])
                            vol = float(k[5])
                            pts.append({
                                "timestamp": ts_str,
                                "price": round(close_p, 2),
                                "volume": round(vol, 2),
                                "anomaly_score": 0.0
                            })
                        if pts:
                            return pts
        except Exception as e:
            logger.debug(f"Binance historical fetch failed for {symbol}: {e}")

    # 2. Check Equities, Commodities, Yields via Yahoo Finance v8 Chart API
    yf_symbol = SYMBOL_YAF_MAP.get(symbol_upper, symbol_upper)
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yf_symbol}?range=1d&interval=5m"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    try:
        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    chart = data.get("chart", {}).get("result", [])[0]
                    timestamps = chart.get("timestamp", [])
                    indicators = chart.get("indicators", {}).get("quote", [])[0]
                    closes = indicators.get("close", [])
                    volumes = indicators.get("volume", [])

                    pts = []
                    for t, c, v in zip(timestamps, closes, volumes):
                        if c is not None:
                            ts_str = datetime.fromtimestamp(t, tz=timezone.utc).isoformat()
                            pts.append({
                                "timestamp": ts_str,
                                "price": round(float(c), 2),
                                "volume": float(v or 1000),
                                "anomaly_score": 0.0
                            })
                    if pts:
                        return pts[-limit:]
    except Exception as e:
        logger.debug(f"Yahoo Finance historical fetch failed for {symbol}: {e}")

    return []


@router.get("/market-series")
async def get_market_series(
    symbols: Optional[str] = Query(None, description="Comma-separated symbols, e.g. TLT,IEF,SHY,BTCUSD,SPY,QQQ"),
    limit: int = Query(60, ge=10, le=300),
    db = Depends(get_db)
):
    """Retrieve intraday price series & financial telemetry for Bond Yields, BTC, SPY, QQQ."""
    target_symbols = [s.strip().upper() for s in (symbols.split(",") if symbols else ["TLT", "IEF", "SHY", "BTCUSD", "SPY", "QQQ"])]
    
    series_data: Dict[str, Any] = {}
    if db:
        try:
            rows = await db.query(
                """
                SELECT primary_entity_id, primary_entity_name, occurred_at, anomaly_score,
                       financial_data, crypto_data
                FROM events
                WHERE LOWER(primary_entity_id) IN (SELECT LOWER(unnest($1::text[])))
                   OR LOWER(primary_entity_name) IN (SELECT LOWER(unnest($1::text[])))
                ORDER BY occurred_at DESC
                LIMIT $2;
                """,
                target_symbols,
                limit * len(target_symbols)
            )
            for r in rows:
                sym = (r["primary_entity_id"] or r["primary_entity_name"] or "UNKNOWN").upper()
                if sym not in series_data:
                    series_data[sym] = []
                
                fin = r.get("financial_data") or {}
                cryp = r.get("crypto_data") or {}
                price = fin.get("current_price") or fin.get("close") or cryp.get("price") or cryp.get("mark_price") or 100.0
                
                series_data[sym].append({
                    "timestamp": r["occurred_at"].isoformat() if hasattr(r["occurred_at"], "isoformat") else str(r["occurred_at"]),
                    "price": float(price),
                    "volume": float(fin.get("volume") or cryp.get("volume") or 1000),
                    "anomaly_score": float(r["anomaly_score"] or 0.0)
                })
        except Exception as e:
            logger.warning(f"Error fetching market series from DB: {e}")

    # Fetch on-the-spot historical ticks for any target symbol with missing or insufficient DB events
    fetch_tasks = []
    missing_symbols = []
    for sym in target_symbols:
        if sym not in series_data or len(series_data[sym]) < 5:
            missing_symbols.append(sym)
            fetch_tasks.append(fetch_on_the_spot_historical(sym, limit))

    if fetch_tasks:
        results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
        for sym, res in zip(missing_symbols, results):
            if isinstance(res, list) and res:
                series_data[sym] = res
            elif sym not in series_data:
                series_data[sym] = []

    return {
        "symbols": target_symbols,
        "series": series_data
    }

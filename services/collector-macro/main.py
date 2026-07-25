"""
services/collector-macro/main.py

ENTERPRISE REAL-MARKET MACRO FEED COLLECTOR
=============================================
3-Tier Live Market Data Provider Chain:
  1. Alpaca Market Data API v2 (STOCKS Snapshots with feed=iex/sip)
  2. Finnhub REST Quote API (Equities & ETF Real Quotes)
  3. yfinance Executor Download (Futures & Index History)

Instruments Tracked:
  CL=F   — Crude Oil Futures      (USO ETF Proxy)
  BZ=F   — Brent Crude Futures    (BNO / USO ETF Proxy)
  NG=F   — Natural Gas Futures    (UNG ETF Proxy)
  GC=F   — Gold Futures           (GLD ETF Proxy)
  SI=F   — Silver Futures         (SLV ETF Proxy)
  ZC=F   — Corn Futures           (CORN ETF Proxy)
  ZW=F   — Wheat Futures          (WEAT ETF Proxy)
  NQ=F   — Nasdaq 100 Futures     (QQQ ETF Proxy)
  ES=F   — S&P 500 Futures        (SPY ETF Proxy)
  VXX    — Volatility Index ETN   (VXX ETN)
  TIP    — iShares TIPS Bond ETF  (TIP ETF)
  TLT    — 20+ Year Treasury ETF  (TLT ETF)
"""

import os
import sys
import time
import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
import uuid
import aiohttp
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from shared.kafka import SentinelProducer, Topics

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("feed.macro")
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

# Macro tickers to track
MACRO_TICKERS = {
    "CL=F": "Crude Oil Futures",
    "BZ=F": "Brent Crude Oil Futures",
    "NG=F": "Natural Gas Futures",
    "GC=F": "Gold Futures",
    "SI=F": "Silver Futures",
    "ZC=F": "Corn Futures",
    "ZW=F": "Wheat Futures",
    "NQ=F": "Nasdaq 100 Futures",
    "ES=F": "S&P 500 Futures",
    "VXX":  "Volatility Index ETN",
    "TIP":  "iShares TIPS Bond ETF",
    "TLT":  "20+ Year Treasury Bond ETF"
}

# Proxy ETF map for Alpaca & Finnhub APIs
FALLBACK_MAP = {
    "CL=F": "USO",
    "BZ=F": "USO",
    "NG=F": "UNG",
    "GC=F": "GLD",
    "SI=F": "SLV",
    "ZC=F": "CORN",
    "ZW=F": "WEAT",
    "NQ=F": "QQQ",
    "ES=F": "SPY",
    "VXX":  "VXX",
    "TIP":  "TIP",
    "TLT":  "TLT",
}

# Credentials
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID") or os.getenv("ALPACA_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY") or os.getenv("ALPACA_API_SECRET") or os.getenv("APCA_API_SECRET_KEY") or os.getenv("ALPACA_SECRET")
ALPACA_DATA_URL = "https://data.alpaca.markets/v2/stocks/snapshots"

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")

POLL_INTERVAL = 30  # 30 seconds for live streaming market updates


async def fetch_alpaca_quotes(tickers: list) -> dict:
    """Tier 1: Fetch ETF proxy quotes from Alpaca Snapshot API."""
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        return {}

    etf_symbols = list(set(FALLBACK_MAP[t] for t in tickers if t in FALLBACK_MAP))
    if not etf_symbols:
        return {}

    headers = {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
        "Accept": "application/json"
    }

    try:
        async with aiohttp.ClientSession() as session:
            url = f"{ALPACA_DATA_URL}?symbols={','.join(etf_symbols)}&feed=iex"
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=8.0)) as resp:
                if resp.status == 403:
                    url_sip = f"{ALPACA_DATA_URL}?symbols={','.join(etf_symbols)}"
                    async with session.get(url_sip, headers=headers, timeout=aiohttp.ClientTimeout(total=8.0)) as resp_sip:
                        if resp_sip.status == 200:
                            payload = await resp_sip.json()
                            snapshots = payload.get("snapshots", {})
                        else:
                            return {}
                elif resp.status == 200:
                    payload = await resp.json()
                    snapshots = payload.get("snapshots", {})
                else:
                    return {}

                results = {}
                for ticker in tickers:
                    etf = FALLBACK_MAP.get(ticker)
                    if etf and etf in snapshots:
                        snap = snapshots[etf]
                        trade = snap.get("latestTrade") or {}
                        quote = snap.get("latestQuote") or {}
                        daily = snap.get("dailyBar") or {}
                        minute = snap.get("minuteBar") or {}
                        prev = snap.get("prevDailyBar") or {}

                        bid = float(quote.get("bp") or 0.0)
                        ask = float(quote.get("ap") or 0.0)
                        mid = (bid + ask) / 2.0 if (bid > 0 and ask > 0) else (bid or ask)

                        price = float(trade.get("p") or mid or minute.get("c") or daily.get("c") or prev.get("c") or 0.0)
                        open_p = float(minute.get("o") or daily.get("o") or prev.get("o") or price)
                        high_p = float(minute.get("h") or daily.get("h") or prev.get("h") or price)
                        low_p  = float(minute.get("l") or daily.get("l") or prev.get("l") or price)
                        vol    = float(minute.get("v") or daily.get("v") or prev.get("v") or 0.0)

                        if price > 0.0:
                            results[ticker] = {
                                "close": price,
                                "open": open_p,
                                "high": high_p,
                                "low": low_p,
                                "volume": vol,
                                "provider": f"Alpaca ({etf})"
                            }
                return results
    except Exception as e:
        logger.debug(f"Alpaca quote fetch bypass: {e}")
        return {}


async def fetch_finnhub_quote(session: aiohttp.ClientSession, ticker: str) -> dict:
    """Tier 2: Fetch ETF proxy quotes from Finnhub REST API."""
    if not FINNHUB_API_KEY:
        return {}

    etf = FALLBACK_MAP.get(ticker, ticker)
    url = f"https://finnhub.io/api/v1/quote?symbol={etf}&token={FINNHUB_API_KEY}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5.0)) as resp:
            if resp.status == 200:
                data = await resp.json()
                current_p = float(data.get("c") or 0.0)
                open_p = float(data.get("o") or data.get("pc") or current_p)
                high_p = float(data.get("h") or current_p)
                low_p = float(data.get("l") or current_p)

                if current_p > 0.0:
                    return {
                        "close": current_p,
                        "open": open_p,
                        "high": high_p,
                        "low": low_p,
                        "volume": 1000.0,
                        "provider": f"Finnhub ({etf})"
                    }
    except Exception as e:
        logger.debug(f"Finnhub quote fetch bypass for {ticker}: {e}")
    return {}


async def fetch_yfinance_single(ticker: str) -> dict:
    """Tier 3: Fetch yfinance single ticker historical quote in thread executor."""
    loop = asyncio.get_running_loop()

    def _sync_yf():
        try:
            import yfinance as yf
            t = yf.Ticker(ticker)
            fi = getattr(t, 'fast_info', {})
            p = 0.0
            if isinstance(fi, dict) or hasattr(fi, '__getitem__'):
                p = float(fi.get('lastPrice') or fi.get('last_price') or fi.get('previousClose') or 0.0)
            if p <= 0:
                df = t.history(period="2d")
                if not df.empty and "Close" in df.columns:
                    p = float(df["Close"].iloc[-1])
            if p > 0.0:
                return {
                    "close": p,
                    "open": p,
                    "high": p,
                    "low": p,
                    "volume": 500.0,
                    "provider": f"yfinance ({ticker})"
                }
        except Exception:
            pass
        return {}

    try:
        return await loop.run_in_executor(None, _sync_yf)
    except Exception:
        return {}


async def fetch_and_publish(producer: SentinelProducer):
    logger.info("Fetching real macro market quotes across 3-Tier API provider chain...")
    tickers = list(MACRO_TICKERS.keys())
    now = datetime.now(timezone.utc).isoformat()
    published = 0

    # 1. Fetch Alpaca snapshots for all ETF proxies simultaneously
    alpaca_quotes = await fetch_alpaca_quotes(tickers)
    if alpaca_quotes:
        logger.info(f"✅ Alpaca API returned real quotes for {len(alpaca_quotes)} instruments.")

    async with aiohttp.ClientSession() as session:
        for ticker in tickers:
            try:
                q = {}

                # Tier 1: Alpaca API
                if ticker in alpaca_quotes:
                    q = alpaca_quotes[ticker]

                # Tier 2: Finnhub REST API
                if not q or q.get("close", 0.0) <= 0.0:
                    q = await fetch_finnhub_quote(session, ticker)

                # Tier 3: yfinance Single-Ticker Fetch
                if not q or q.get("close", 0.0) <= 0.0:
                    q = await fetch_yfinance_single(ticker)

                if not q or q.get("close", 0.0) <= 0.0:
                    logger.warning(f"No real market quote returned for {ticker}. Skipping ticker.")
                    continue

                current_price  = q["close"]
                previous_price = q["open"]
                high_val       = q["high"]
                low_val        = q["low"]
                volume         = q["volume"]
                provider       = q["provider"]

                tick_direction = (
                    "UpTick" if current_price > previous_price
                    else ("DownTick" if current_price < previous_price else "ZeroTick")
                )

                payload = {
                    "source": "finnhub_equities",
                    "raw_payload": {
                        "ticker": ticker,
                        "trade_type": "OHLCV_MINUTE_BAR",
                        "open": previous_price,
                        "close": current_price,
                        "high": high_val,
                        "low": low_val,
                        "volume": volume,
                        "notional_usd": current_price * volume,
                        "tick_direction": tick_direction,
                        "name": MACRO_TICKERS[ticker],
                        "provider": provider
                    },
                    "event_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, f"macro_{ticker}_{int(time.time())}")),
                    "occurred_at": now,
                }

                await producer.send(Topics.RAW_TRADFI, payload, key=ticker)
                published += 1
                logger.info(f"📊 Real Macro Tick Published | {ticker} ({MACRO_TICKERS[ticker]}): ${current_price:.2f} via {provider}")

            except Exception as e:
                logger.error(f"Error processing macro ticker {ticker}: {e}", exc_info=True)

    if published > 0:
        logger.info(f"✅ Published {published}/{len(tickers)} real macro market ticks.")
    else:
        logger.warning("No real macro ticks published this cycle.")


async def main():
    logger.info(f"Starting Real-Market Macro Feed... Tracking {len(MACRO_TICKERS)} instruments.")
    producer = SentinelProducer()
    await producer.start()

    try:
        while True:
            try:
                await fetch_and_publish(producer)
            except Exception as e:
                logger.error(f"Macro feed iteration error: {e}", exc_info=True)

            await asyncio.sleep(POLL_INTERVAL)
    finally:
        await producer.flush()
        await producer.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Macro feed stopped by user.")

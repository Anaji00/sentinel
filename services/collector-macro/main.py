"""
services/collector-macro/main.py

MACRO FEED COLLECTOR (HYBRID FALLBACK EDITION)
==============================================
Polls yfinance for commodity futures, index futures, and VIX.
If yfinance queries fail or are rate-limited, falls back to Alpaca's Snapshot
API using equivalent ETF proxies (USO, GLD, VXX, SPY, QQQ).
Publishes RawEvents to raw.tradfi (spoofed source so TradFi enricher processes them).

Instruments tracked:
  CL=F   — Crude Oil          BZ=F  — Brent Crude
  NG=F   — Natural Gas        GC=F  — Gold
  SI=F   — Silver             ZC=F  — Corn
  ZW=F   — Wheat              NQ=F  — Nasdaq 100
  ES=F   — S&P 500            ^VIX  — Volatility Index
"""

import os
import sys
import time
import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path

import yfinance as yf
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from shared.kafka import SentinelProducer, Topics

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("feed.macro")

# Macro tickers to track via yfinance
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
    "^VIX": "Volatility Index",
    "TIP":  "iShares TIPS Bond ETF",
    "^TNX": "10-Year Treasury Yield"
}

# Alpaca Fallback Proxies mapping
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_API_SECRET")
ALPACA_DATA_URL = "https://data.alpaca.markets/v2/stocks/snapshots"

FALLBACK_MAP = {
    "CL=F": "USO",   # Crude Oil -> USO ETF
    "BZ=F": "USO",   # Brent Oil -> USO ETF (approximation)
    "NG=F": "UNG",   # Natural Gas -> UNG ETF
    "GC=F": "GLD",   # Gold -> GLD ETF
    "SI=F": "SLV",   # Silver -> SLV ETF
    "ZC=F": "CORN",  # Corn -> CORN ETF
    "ZW=F": "WEAT",  # Wheat -> WEAT ETF
    "NQ=F": "QQQ",   # Nasdaq 100 -> QQQ ETF
    "ES=F": "SPY",   # S&P 500 -> SPY ETF
    "^VIX": "VXX",   # Volatility Index -> VXX ETF
    "TIP":  "TIP",   # TIPS Bond ETF -> TIP ETF
    "^TNX": "TLT",   # 10-Yr Treasury Yield -> TLT Bond ETF proxy
}

# Retry config
MAX_RETRIES = 3
POLL_INTERVAL = 300  # 5 minutes


async def _download_with_retry(tickers: list, retries: int = MAX_RETRIES):
    """Download yfinance data with exponential backoff retry."""
    loop = asyncio.get_running_loop()
    last_err = None

    for attempt in range(retries):
        try:
            data = await loop.run_in_executor(
                None,
                lambda: yf.download(
                    tickers,
                    period="1d",
                    interval="5m",
                    progress=False,
                    threads=False,  # Avoid thread-safety issues inside executor
                ),
            )
            if not data.empty:
                return data
            logger.warning(f"yfinance returned empty (attempt {attempt + 1}/{retries})")
        except Exception as e:
            last_err = e
            logger.warning(f"yfinance download failed (attempt {attempt + 1}/{retries}): {e}")

        if attempt < retries - 1:
            wait = 2 ** (attempt + 1)  # 2s, 4s
            logger.info(f"Retrying yfinance in {wait}s...")
            await asyncio.sleep(wait)

    logger.error(f"yfinance failed after {retries} attempts. Last error: {last_err}")
    return None


async def fetch_fallback_quotes(tickers: list) -> dict:
    """Fetch proxy quotes from Alpaca as a failover for yfinance."""
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        logger.error("Alpaca credentials missing in environment. Fallback skipped.")
        return {}

    # Map target futures to their corresponding ETF proxy symbols
    etf_symbols = list(set(FALLBACK_MAP[t] for t in tickers if t in FALLBACK_MAP))
    if not etf_symbols:
        return {}

    headers = {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
        "Accept": "application/json"
    }

    try:
        import aiohttp
        async with aiohttp.ClientSession() as session:
            url = f"{ALPACA_DATA_URL}?symbols={','.join(etf_symbols)}"
            async with session.get(url, headers=headers) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.error(f"Alpaca API snapshot error (HTTP {resp.status}): {body[:200]}")
                    return {}
                
                payload = await resp.json()
                snapshots = payload.get("snapshots", {})
                
                # Map the ETF snapshot back to the requested futures tickers
                fallback_data = {}
                for ticker in tickers:
                    etf = FALLBACK_MAP.get(ticker)
                    if etf and etf in snapshots:
                        snap = snapshots[etf]
                        latest_trade = snap.get("latestTrade", {})
                        daily_bar = snap.get("dailyBar", {})
                        minute_bar = snap.get("minuteBar", {})
                        
                        current_price = float(latest_trade.get("p") or minute_bar.get("c") or daily_bar.get("c") or 0.0)
                        open_price = float(minute_bar.get("o") or daily_bar.get("o") or current_price)
                        high_price = float(minute_bar.get("h") or daily_bar.get("h") or current_price)
                        low_price = float(minute_bar.get("l") or daily_bar.get("l") or current_price)
                        volume = float(minute_bar.get("v") or daily_bar.get("v") or 0.0)
                        
                        if current_price > 0.0:
                            fallback_data[ticker] = {
                                "close": current_price,
                                "open": open_price,
                                "high": high_price,
                                "low": low_price,
                                "volume": volume
                            }
                return fallback_data
    except Exception as e:
        logger.error(f"Failed to fetch Alpaca fallback quotes: {e}", exc_info=True)
        return {}


async def fetch_and_publish(producer: SentinelProducer):
    logger.info("Fetching macro data...")
    loop = asyncio.get_running_loop()

    tickers = list(MACRO_TICKERS.keys())
    data = await _download_with_retry(tickers)

    fallback_quotes = {}
    use_fallback = False

    if data is None or data.empty:
        logger.warning("yfinance returned empty. Activating Alpaca fallback for all tickers...")
        use_fallback = True
        fallback_quotes = await fetch_fallback_quotes(tickers)
        if not fallback_quotes:
            logger.error("Alpaca fallback returned no data. Aborting macro cycle.")
            return

    now = datetime.now(timezone.utc).isoformat()
    published = 0

    for ticker in tickers:
        try:
            current_price = 0.0
            previous_price = 0.0
            high_val = 0.0
            low_val = 0.0
            volume = 0.0
            
            ticker_failed = False
            if not use_fallback:
                # Check if columns exist and can be indexed
                if ticker not in data['Close'].columns if hasattr(data['Close'], 'columns') else False:
                    ticker_failed = True
                else:
                    close_series = data['Close'][ticker].dropna()
                    if len(close_series) < 2:
                        ticker_failed = True
                    else:
                        current_price = float(close_series.iloc[-1])
                        previous_price = float(close_series.iloc[-2])
                        high_val = float(data['High'][ticker].iloc[-1]) if 'High' in data else current_price
                        low_val = float(data['Low'][ticker].iloc[-1]) if 'Low' in data else current_price
                        
                        if 'Volume' in data:
                            vol_col = data['Volume']
                            if hasattr(vol_col, 'columns') and ticker in vol_col.columns:
                                vol_val = vol_col[ticker].iloc[-1]
                                volume = float(vol_val) if vol_val == vol_val else 0.0
                            elif not hasattr(vol_col, 'columns'):
                                vol_val = vol_col.iloc[-1]
                                volume = float(vol_val) if vol_val == vol_val else 0.0

            if use_fallback or ticker_failed:
                # Lazy fetch fallback if not loaded yet
                if not fallback_quotes:
                    logger.warning(f"Ticker {ticker} failed on bulk yfinance. Fetching Alpaca proxy...")
                    fallback_quotes = await fetch_fallback_quotes([ticker])
                
                if ticker in fallback_quotes:
                    q = fallback_quotes[ticker]
                    current_price = q["close"]
                    previous_price = q["open"]
                    high_val = q["high"]
                    low_val = q["low"]
                    volume = q["volume"]
                    fallback_symbol = FALLBACK_MAP.get(ticker, ticker)
                    logger.info(f"Using Alpaca proxy fallback ({fallback_symbol}) for {ticker}: {current_price}")
                else:
                    # Single-ticker yfinance fast_info fallback
                    try:
                        def _single_yf(tk=ticker):
                            t = yf.Ticker(tk)
                            p = float(getattr(t.fast_info, 'last_price', 0.0) or getattr(t.fast_info, 'previous_close', 0.0) or 0.0)
                            if p <= 0:
                                df = t.history(period="5d")
                                if not df.empty and "Close" in df.columns:
                                    p = float(df["Close"].iloc[-1])
                            return p
                        
                        single_price = await loop.run_in_executor(None, _single_yf)
                        if single_price > 0:
                            current_price = single_price
                            previous_price = single_price * 0.999
                            high_val = single_price * 1.002
                            low_val = single_price * 0.998
                            logger.info(f"✅ Single-ticker yfinance fallback recovered {ticker}: ${current_price:.2f}")
                        else:
                            logger.warning(f"No fallback proxy available for {ticker}. Skipping.")
                            continue
                    except Exception as s_err:
                        logger.warning(f"Single-ticker yfinance fallback error for {ticker}: {s_err}. Skipping.")
                        continue

            tick_direction = (
                "UpTick" if current_price > previous_price
                else ("DownTick" if current_price < previous_price else "ZeroTick")
            )

            import uuid
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
                },
                "event_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, f"macro_{ticker}_{int(time.time())}")),
                "occurred_at": now,
            }

            await producer.send(Topics.RAW_TRADFI, payload, key=ticker)
            published += 1

        except Exception as e:
            logger.error(f"Error processing {ticker}: {e}", exc_info=True)

    if published:
        logger.info(f"Published {published}/{len(tickers)} macro ticks.")
    else:
        logger.warning("No macro ticks published this cycle.")


async def main():
    logger.info(f"Starting Async Macro Feed... Tracking {len(MACRO_TICKERS)} instruments.")
    producer = SentinelProducer()
    await producer.start()

    try:
        while True:
            try:
                await fetch_and_publish(producer)
            except Exception as e:
                logger.error(f"Feed error: {e}", exc_info=True)

            await asyncio.sleep(POLL_INTERVAL)
    finally:
        await producer.flush()
        await producer.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Macro feed stopped by user.")

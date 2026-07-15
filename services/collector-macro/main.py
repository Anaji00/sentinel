import os
import os
import sys
import time
import json
import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path

import yfinance as yf
from dotenv import load_dotenv

# Ensure system paths resolve shared microservice assets cleanly
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
    "^VIX": "Volatility Index"
}

async def fetch_and_publish(producer: SentinelProducer):
    logger.info("Fetching macro data from yfinance...")
    
    tickers = list(MACRO_TICKERS.keys())
    
    # Run yfinance blocking HTTP call in a background thread to prevent blocking event loop
    loop = asyncio.get_running_loop()
    try:
        data = await loop.run_in_executor(
            None, 
            lambda: yf.download(tickers, period="1d", interval="5m", progress=False)
        )
    except Exception as e:
        logger.error(f"Failed to fetch from yfinance: {e}")
        return
    
    if data.empty:
        logger.warning("No data returned from yfinance.")
        return

    now = datetime.now(timezone.utc).isoformat()
    
    # Process each ticker
    for ticker in tickers:
        try:
            # Check if we have valid close data for this ticker
            if ticker in data['Close']:
                close_series = data['Close'][ticker].dropna()
                if len(close_series) < 2:
                    continue
                
                current_price = float(close_series.iloc[-1])
                previous_price = float(close_series.iloc[-2])
                volume = float(data['Volume'][ticker].iloc[-1]) if 'Volume' in data and ticker in data['Volume'] else 0.0
                
                # Determine tick direction
                tick_direction = "UpTick" if current_price > previous_price else ("DownTick" if current_price < previous_price else "ZeroTick")
                
                payload = {
                    "source": "finnhub_equities", # Spoofed source so tradfi.py processes it
                    "raw_payload": {
                        "ticker": ticker,
                        "trade_type": "OHLCV_MINUTE_BAR",
                        "open": previous_price,
                        "close": current_price,
                        "high": float(data['High'][ticker].iloc[-1]),
                        "low": float(data['Low'][ticker].iloc[-1]),
                        "volume": volume,
                        "notional_usd": current_price * volume,
                        "tick_direction": tick_direction,
                        "name": MACRO_TICKERS[ticker]
                    },
                    "event_id": f"macro_{ticker}_{int(time.time())}",
                    "occurred_at": now
                }
                
                await producer.send(Topics.RAW_TRADFI, payload, key=ticker)
                logger.info(f"Published macro tick for {ticker}: ${current_price:.2f}")
                
        except Exception as e:
            logger.error(f"Error processing {ticker}: {e}", exc_info=True)

async def main():
    logger.info(f"Starting Async Macro Feed... Tracking {len(MACRO_TICKERS)} instruments.")
    producer = SentinelProducer()
    
    try:
        while True:
            try:
                await fetch_and_publish(producer)
            except Exception as e:
                logger.error(f"Feed error: {e}", exc_info=True)
            
            # Non-blocking poll every 5 minutes
            await asyncio.sleep(300)
    finally:
        await producer.flush()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Macro feed stopped by user.")

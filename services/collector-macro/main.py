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
from typing import Dict
import aiohttp
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from shared.kafka import SentinelProducer, Topics
from shared.db import get_redis
from shared.utils.heartbeat import start_heartbeat_task
from shared.utils.collector_metrics import CollectorMetrics
from shared.utils.quote_cache import QUOTE_CACHE_TTL_SEC, quote_key
from shared.utils.tasks import safe_create_task

try:
    from economic_calendar import EconomicCalendarCollector
except ImportError:
    try:
        from services.collector_macro.economic_calendar import EconomicCalendarCollector
    except ImportError:
        import importlib.util
        spec = importlib.util.spec_from_file_location("economic_calendar", Path(__file__).parent / "economic_calendar.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        EconomicCalendarCollector = mod.EconomicCalendarCollector

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
    "TLT":  "20+ Year Treasury Bond ETF",
    # Read by the macro engine for the credit spread and tracked by nothing.
    # The quote cache is populated from the tradfi collector's equity
    # watchlist, which is where these two fell between the desks: they are
    # macro instruments, so they belong to the macro collector.
    "HYG":  "High Yield Corporate Bond ETF",
    "LQD":  "Investment Grade Corporate Bond ETF",
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


async def fetch_live_sofr_rate(session: aiohttp.ClientSession) -> dict:
    """
    Tier 1 Official Live SOFR Rate Collector via Federal Reserve Bank of New York API.
    Fallback: 13-Week T-Bill Yield (^IRX) via yfinance.
    """
    nyfed_url = "https://markets.newyorkfed.org/api/rates/secured/sofr/last/1.json"
    try:
        async with session.get(nyfed_url, timeout=aiohttp.ClientTimeout(total=5.0)) as resp:
            if resp.status == 200:
                data = await resp.json()
                ref_rates = data.get("refRates", [])
                if ref_rates:
                    pct = float(ref_rates[0].get("percentRate", 0.0))
                    if pct > 0:
                        return {
                            "rate_pct": pct,
                            "rate_decimal": round(pct / 100.0, 5),
                            "effective_date": ref_rates[0].get("effectiveDate", ""),
                            "provider": "Federal Reserve Bank of New York (SOFR)",
                            "source_type": "LIVE_FED_SOFR"
                        }
    except Exception as e:
        logger.debug(f"NY Fed SOFR fetch bypass: {e}")

    # Fallback: 13-Week T-Bill (^IRX)
    yf_res = await fetch_yfinance_single("^IRX")
    if yf_res and yf_res.get("close", 0.0) > 0.0:
        pct = yf_res["close"]
        return {
            "rate_pct": pct,
            "rate_decimal": round(pct / 100.0, 5),
            "effective_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "provider": "Treasury 13-Week T-Bill (^IRX)",
            "source_type": "LIVE_TBILL_YIELD"
        }

    return {
        "rate_pct": 4.5,
        "rate_decimal": 0.045,
        "effective_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "provider": "Default Parametric Rate",
        "source_type": "PARAMETRIC_FALLBACK"
    }


# Last quote published per ticker, as (close, high, low, volume).
#
# The macro tier polls on a fixed cadence and used to publish on every cycle
# whether or not the quote had moved. Outside regular hours the IEX feed's
# latestTrade is the previous session's last print and does not change, so a
# six-hour overnight window produced 211 rows per ticker carrying exactly ONE
# distinct close -- twelve of twelve macro instruments, against QQQ's 45
# distinct closes in 61 rows on a live feed.
#
# That is not a cosmetic duplication. Three things downstream read it as fact:
#
#   * The hourly continuous aggregate SUMs volume, so one real print of 1,000
#     was reported as 111,000 -- the figure measured the poll count.
#   * Bar-over-bar returns are exactly zero on every repeat, so two frozen
#     series correlate at |r| = 1.000 on the handful of bars that did move.
#     The first live peer-graph run published six such pairs.
#   * Any volume-spike or trend detector reading these bars was measuring the
#     polling cadence rather than the market.
#
# A quote that has not moved is not new information. Republishing it invents
# observations, and every consumer that counts bars was counting this one.
_last_published: Dict[str, tuple] = {}


# Constant-maturity Treasury yields, published daily by the Treasury itself.
#
# The macro engine reads sentinel:quotes:latest:US2Y and :US10Y and refuses to
# publish a rates regime without both. Nothing in the codebase had ever written
# either key, so the refusal was permanent: every 2s10s spread, curve-inversion
# signal and rates regime this platform could produce was unavailable, and the
# engine correctly logged that it was skipping rather than inventing a curve.
#
# The audit's standing note said closing this needed a data source rather than a
# code change, and named FRED -- which requires an API key nobody had issued.
# The Treasury publishes the same constant-maturity series itself, as CSV, with
# no key and no registration.
TREASURY_YIELD_CSV = (
    "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/"
    "daily-treasury-rates.csv/{year}/all"
    "?type={series}&field_tdr_date_value={year}&page&_format=csv"
)

# A yield, not a price. The TIP ETF trades near 107 and was once written to a
# yield key, which produced a fabricated 230bp breakeven; the macro engine now
# rejects that on read, and this rejects it on write.
YIELD_PLAUSIBLE_RANGE = (-5.0, 25.0)

# Column headings in the Treasury CSVs.
#
# The two series do not agree on capitalisation -- the nominal curve publishes
# "10 Yr" and the real curve "10 YR" -- so they are listed separately rather
# than shared. A single map would have matched one series and silently returned
# nothing for the other, which is the failure mode this whole collector exists
# to end.
TREASURY_COLUMNS = {
    "US2Y":  "2 Yr",
    "US10Y": "10 Yr",
    "US30Y": "30 Yr",
}

# The real (inflation-indexed) curve. Its ten-year point is the TIPS yield the
# macro engine reads, and nominal minus real is the inflation breakeven -- the
# number that was once fabricated at 230bp by treating the TIP ETF's price as a
# yield. Measured today it is 4.79 - 2.45, or 234bp.
TREASURY_REAL_COLUMNS = {
    "TIPS_YIELD": "10 YR",
}


def _parse_treasury_csv(text: str, columns: dict) -> dict:
    """The most recent row of a Treasury rates CSV, as {key: yield}.

    Rows are newest-first in the published file, but that is not relied on --
    the date column is parsed and the latest row wins, so a change in the
    Treasury's ordering cannot silently start returning January's curve.
    """
    import csv, io
    from datetime import datetime as _dt

    latest_date, latest_row = None, None
    for row in csv.DictReader(io.StringIO(text)):
        raw_date = (row.get("Date") or "").strip()
        if not raw_date:
            continue
        try:
            parsed = _dt.strptime(raw_date, "%m/%d/%Y")
        except ValueError:
            continue
        if latest_date is None or parsed > latest_date:
            latest_date, latest_row = parsed, row

    if latest_row is None:
        return {}

    out = {"__date__": latest_date.strftime("%Y-%m-%d")}
    lo, hi = YIELD_PLAUSIBLE_RANGE
    for key, column in columns.items():
        raw = (latest_row.get(column) or "").strip()
        if not raw:
            continue
        try:
            value = float(raw)
        except ValueError:
            continue
        if not (lo < value < hi):
            logger.warning(
                "Discarding %s=%s from the Treasury feed: outside the range a "
                "yield occupies.", key, value,
            )
            continue
        out[key] = value
    return out


async def fetch_treasury_yields(session: aiohttp.ClientSession, redis_client) -> dict:
    """Writes the constant-maturity curve to the quote cache.

    Returns what was written, empty when nothing could be. A yield that cannot
    be fetched leaves the key absent rather than stale-but-plausible: the macro
    engine's refusal is the correct behaviour on a missing input, and this must
    not defeat it by writing a guess.
    """
    if not redis_client:
        return {}

    year = datetime.now(timezone.utc).year
    written = {}
    series = (
        ("daily_treasury_yield_curve", TREASURY_COLUMNS),
        # Fetched separately because the two curves publish on their own
        # schedules -- the real curve is often a day ahead of the nominal one --
        # and a failure on either must not cost the other.
        ("daily_treasury_real_yield_curve", TREASURY_REAL_COLUMNS),
    )
    try:
        parsed = {}
        for series_name, columns in series:
            url = TREASURY_YIELD_CSV.format(year=year, series=series_name)
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=20.0)) as resp:
                    if resp.status != 200:
                        logger.warning("Treasury %s returned HTTP %s", series_name, resp.status)
                        continue
                    body = await resp.text()
            except Exception as se:
                logger.warning("Treasury %s fetch failed: %s", series_name, se)
                continue
            for key, value in _parse_treasury_csv(body, columns).items():
                # Each series reports its own date; the later one wins the label.
                if key == "__date__":
                    parsed[key] = max(parsed.get(key, ""), value)
                else:
                    parsed[key] = value

        for key, value in parsed.items():
            if key.startswith("__"):
                continue
            # Two days, so a long weekend or a federal holiday does not empty
            # the cache while the curve is simply unchanged.
            await redis_client.raw.set(
                f"sentinel:quotes:latest:{key}", str(value), ex=2 * 86400,
            )
            written[key] = value

        if written:
            logger.info(
                "Treasury curve %s: %s",
                parsed.get("__date__", "?"),
                " ".join(f"{k}={v:.2f}%" for k, v in sorted(written.items())),
            )
    except Exception as e:
        logger.warning("Treasury yield fetch failed: %s", e)

    return written


def _quote_fingerprint(quote: dict) -> tuple:
    """What has to change before a quote counts as a new observation."""
    return (
        round(float(quote.get("close") or 0.0), 6),
        round(float(quote.get("high") or 0.0), 6),
        round(float(quote.get("low") or 0.0), 6),
        round(float(quote.get("volume") or 0.0), 6),
    )


async def fetch_and_publish(producer: SentinelProducer, redis_client=None):
    logger.info("Fetching real macro market quotes across 3-Tier API provider chain...")
    tickers = list(MACRO_TICKERS.keys())
    now = datetime.now(timezone.utc).isoformat()
    published = 0
    unchanged = 0

    async with aiohttp.ClientSession() as session:
        # Fetch and publish live SOFR / Risk-Free Rate
        sofr_data = await fetch_live_sofr_rate(session)
        # SOFR is set once a day. Publishing it on every 30-second poll made a
        # daily fixing look like a stream of ~2,880 observations, which is the
        # same defect as the frozen macro quotes and lands in the same
        # consumers. The effective_date is what changes when the rate is new.
        sofr_fingerprint = (sofr_data.get("effective_date"), sofr_data.get("rate_pct"))
        sofr_is_new = _last_published.get("__SOFR__") != sofr_fingerprint
        sofr_event = {
            "source": "nyfed_sofr",
            "raw_payload": {
                "ticker": "SOFR",
                "rate_pct": sofr_data["rate_pct"],
                "risk_free_rate": sofr_data["rate_decimal"],
                "effective_date": sofr_data["effective_date"],
                "provider": sofr_data["provider"],
                "source_type": sofr_data["source_type"]
            },
            "event_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, f"sofr_{int(time.time())}")),
            "occurred_at": now,
        }
        if sofr_is_new:
            await producer.send(Topics.RAW_TRADFI, sofr_event, key="SOFR")
            _last_published["__SOFR__"] = sofr_fingerprint
            logger.info(f"🏛️ Live Risk-Free Rate (SOFR) Published: {sofr_data['rate_pct']}% ({sofr_data['rate_decimal']}) via {sofr_data['provider']}")

        # 1. Fetch Alpaca snapshots for all ETF proxies simultaneously
        alpaca_quotes = await fetch_alpaca_quotes(tickers)
        if alpaca_quotes:
            logger.info(f"✅ Alpaca API returned real quotes for {len(alpaca_quotes)} instruments.")

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

                fingerprint = _quote_fingerprint(q)
                current_price = q["close"]

                # The cache is refreshed before the dedup check, not after it.
                #
                # This is a latest-known-value store, not an event stream: an
                # unchanged price is still the current price. Writing it below
                # the `continue` meant the cache only refreshed while the market
                # was moving, so the moment it went quiet the entries aged out
                # and HYG, LQD and TIP disappeared from what the macro engine
                # reads. The dedup below is right -- it governs publication,
                # which is a claim that something happened -- and it should not
                # govern a cache whose only claim is "this is the last price we
                # saw".
                if redis_client:
                    try:
                        await redis_client.raw.set(
                            quote_key(ticker), str(current_price), ex=QUOTE_CACHE_TTL_SEC,
                        )
                    except Exception as qe:
                        logger.debug("Quote cache write failed for %s: %s", ticker, qe)

                # A quote that has not moved since the last cycle is the same
                # observation, not a new one. Skipping it is what keeps the
                # overnight window honest: gaps where the market was closed,
                # rather than rows asserting a print that never happened.
                if _last_published.get(ticker) == fingerprint:
                    unchanged += 1
                    continue

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
                    # Derived from the quote itself, not the clock. A clock-derived id is
                    # unique by construction, which means no consumer downstream can
                    # ever recognise a repeat.
                    "event_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, f"macro_{ticker}_{fingerprint}")),
                    "occurred_at": now,
                }

                await producer.send(Topics.RAW_TRADFI, payload, key=ticker)

                _last_published[ticker] = fingerprint
                published += 1
                logger.info(f"📊 Real Macro Tick Published | {ticker} ({MACRO_TICKERS[ticker]}): ${current_price:.2f} via {provider}")

            except Exception as e:
                logger.error(f"Error processing macro ticker {ticker}: {e}", exc_info=True)

    if published > 0:
        logger.info(
            f"✅ Published {published}/{len(tickers)} real macro market ticks "
            f"({unchanged} unchanged since last cycle, not republished)."
        )
    elif unchanged:
        # The normal state of a closed market, and worth saying plainly rather
        # than warning about: nothing moved, so nothing was invented.
        logger.info(f"⏸️ Macro quotes unchanged for all {unchanged} ticker(s); nothing published.")
    else:
        logger.warning("No real macro ticks published this cycle.")


async def main():
    logger.info(f"Starting Real-Market Macro Feed... Tracking {len(MACRO_TICKERS)} instruments.")
    producer = SentinelProducer(service_name="collector-macro")
    await producer.start()

    redis_client = None
    try:
        redis_client = await get_redis()
    except Exception as re:
        logger.warning(f"Redis unavailable for heartbeat: {re}")

    # §1.1 Universal heartbeat
    hb_task = None
    if redis_client:
        # Throughput counters. The heartbeat proves this process is alive;
        # these prove it is still producing.
        metrics = CollectorMetrics("collector-macro")
        await metrics.start(redis_client)
        hb_task = safe_create_task(start_heartbeat_task(redis_client, "collector-macro"))

    calendar_collector = EconomicCalendarCollector(producer=producer, poll_interval_sec=120)

    # Loaded by file path: this package directory is `collector-macro`, and a
    # hyphen cannot appear in a Python module path, so `services.collector_macro`
    # does not resolve however the files are laid out.
    import importlib.util as _ilu

    def _load_sibling(mod_name: str):
        spec = _ilu.spec_from_file_location(
            f"collector_macro_{mod_name}", str(Path(__file__).parent / f"{mod_name}.py")
        )
        module = _ilu.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    _freight = _load_sibling("freight")
    _regulatory = _load_sibling("regulatory")
    poll_freight_indices = _freight.poll_freight_indices
    poll_federal_register = _regulatory.poll_federal_register
    RegulatoryDeduplicator = _regulatory.RegulatoryDeduplicator
    reg_dedup = RegulatoryDeduplicator(redis_client)

    async def _calendar_polling_loop():
        logger.info("📅 Economic Calendar polling loop initialized.")
        while True:
            try:
                events = await calendar_collector.poll_and_publish()
                if events:
                    logger.info(f"📊 Published {len(events)} new economic calendar release events.")
            except Exception as ce:
                logger.error(f"Economic calendar poll error: {ce}")
            await asyncio.sleep(calendar_collector.poll_interval_sec)

    async def _freight_polling_loop():
        logger.info("🚢 Supply Chain & Freight Shipping Rate loop initialized.")
        connector = aiohttp.TCPConnector(limit=10)
        async with aiohttp.ClientSession(connector=connector) as session:
            while True:
                try:
                    count = await poll_freight_indices(session, producer, redis_client)
                    logger.info(f"🚢 Freight Index Poll: Refreshed {count} benchmarks (BDI, FBX, Harpex).")
                except Exception as fe:
                    logger.debug(f"Freight poll notice: {fe}")
                await asyncio.sleep(180)

    async def _regulatory_polling_loop():
        logger.info("🏛️ Federal Register & Regulatory Policy loop initialized.")
        connector = aiohttp.TCPConnector(limit=10)
        async with aiohttp.ClientSession(connector=connector) as session:
            while True:
                try:
                    count = await poll_federal_register(session, producer, reg_dedup)
                    if count > 0:
                        logger.info(f"🏛️ Regulatory Policy Poll: Ingested {count} new rules/notices.")
                except Exception as re_err:
                    logger.debug(f"Regulatory poll notice: {re_err}")
                await asyncio.sleep(300)

    async def _treasury_polling_loop():
        """The constant-maturity curve, refreshed on a business-day cadence.

        Yields are published once each business day, so polling faster only
        re-reads the same row. Hourly is frequent enough to pick up the day's
        publication without waiting most of a day for it, and cheap enough that
        a missed fetch costs nothing -- the keys carry a two-day TTL, which
        spans a weekend or a federal holiday.
        """
        logger.info("🏦 Treasury constant-maturity yield loop initialized.")
        connector = aiohttp.TCPConnector(limit=4)
        async with aiohttp.ClientSession(connector=connector) as session:
            while True:
                try:
                    written = await fetch_treasury_yields(session, redis_client)
                    if not written:
                        # Said out loud. An empty write means the macro engine
                        # will keep refusing to publish a rates regime, and a
                        # silent failure here is indistinguishable from the
                        # years in which nothing wrote these keys at all.
                        logger.warning(
                            "Treasury yields unavailable this cycle; the rates "
                            "regime stays unpublished."
                        )
                except Exception as te:
                    logger.warning(f"Treasury poll notice: {te}")
                await asyncio.sleep(3600)

    treasury_task = safe_create_task(_treasury_polling_loop())
    calendar_task = safe_create_task(_calendar_polling_loop())
    freight_task = safe_create_task(_freight_polling_loop())
    reg_task = safe_create_task(_regulatory_polling_loop())

    try:
        while True:
            try:
                await fetch_and_publish(producer, redis_client)
            except Exception as e:
                logger.error(f"Macro feed iteration error: {e}", exc_info=True)

            await asyncio.sleep(POLL_INTERVAL)
    finally:
        if hb_task:
            hb_task.cancel()
        treasury_task.cancel()
        calendar_task.cancel()
        freight_task.cancel()
        reg_task.cancel()
        await calendar_collector.close()
        await producer.flush()
        await producer.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Macro feed stopped by user.")

"""
services/enrichment/ref_data.py

Daily batch reference data manager for ticker metadata.
Queries Finnhub /stock/profile2 to cache sector, industry, exchange,
market_cap_tier, and asset_type per symbol in Redis with 24h TTL.

This module is designed to be called as a background job, NOT per-trade.
"""

import asyncio
import logging
import os
from typing import Optional, Dict, Any

logger = logging.getLogger("enrichment.ref_data")

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")
REFDATA_TTL = 86400  # 24 hours
REFDATA_PREFIX = "sentinel:refdata:"
ASSET_TYPE_PREFIX = "sentinel:asset_type:"
INDEX_MEMBERSHIP_PREFIX = "sentinel:index_membership:"

# Major US indices to track for index co-membership edges
INDEX_SYMBOLS = [
    ("^GSPC", "SPX"),   # S&P 500
    ("^NDX", "NDX"),    # Nasdaq 100
    ("^RUT", "RUT"),    # Russell 2000
    ("^DJI", "DJIA"),   # Dow Jones
]


def _classify_market_cap_tier(mcap_millions: float) -> str:
    """Classify market cap in millions into human-readable tier."""
    if mcap_millions >= 200_000:
        return "mega"
    elif mcap_millions >= 10_000:
        return "large"
    elif mcap_millions >= 2_000:
        return "mid"
    elif mcap_millions >= 300:
        return "small"
    elif mcap_millions > 0:
        return "micro"
    return "unknown"


async def fetch_and_cache_reference_data(
    redis_client,
    symbol: str,
    session=None,
    graph_writer=None,
) -> Optional[Dict[str, Any]]:
    """
    Fetch reference data for a single symbol from Finnhub /stock/profile2
    and cache it in Redis. Emits structural graph proposals (Company, Sector, Index)
    via GraphWriter when provided (§8.1).
    
    NOT designed for hot-path usage — call from daily batch jobs only.
    """
    import aiohttp

    if not FINNHUB_API_KEY:
        logger.debug("No FINNHUB_API_KEY set, skipping ref data fetch")
        return None

    cache_key = f"{REFDATA_PREFIX}{symbol}"

    # Check if already cached and fresh
    try:
        cached = await redis_client.raw.get(cache_key)
        if cached:
            import json
            return json.loads(cached)
    except Exception:
        pass

    owns_session = session is None
    if owns_session:
        session = aiohttp.ClientSession()

    try:
        profile_url = "https://finnhub.io/api/v1/stock/profile2"
        async with session.get(
            profile_url, params={"symbol": symbol, "token": FINNHUB_API_KEY}
        ) as resp:
            if resp.status == 429:
                logger.debug(f"Finnhub rate-limited during ref data fetch for {symbol}")
                return None
            if resp.status != 200:
                logger.debug(f"Finnhub profile2 returned {resp.status} for {symbol}")
                return None

            profile = await resp.json()

        sector = profile.get("finnhubIndustry", "") or ""
        industry = profile.get("gicsSector", "") or profile.get("finnhubIndustry", "") or ""
        exchange = profile.get("exchange", "") or ""
        mcap_millions = float(profile.get("marketCapitalization", 0) or 0)
        market_cap_tier = _classify_market_cap_tier(mcap_millions)
        asset_type = profile.get("type", "Common Stock") or "Common Stock"
        country = profile.get("country", "") or ""

        ref_data = {
            "symbol": symbol,
            "sector": sector,
            "industry": industry,
            "exchange": exchange,
            "market_cap_tier": market_cap_tier,
            "market_cap_millions": mcap_millions,
            "asset_type": asset_type,
            "country": country,
            "index_membership": [],
        }

        # Merge cached index membership if available
        try:
            idx_cached = await redis_client.raw.get(f"{INDEX_MEMBERSHIP_PREFIX}{symbol}")
            if idx_cached:
                import json as _json
                ref_data["index_membership"] = _json.loads(idx_cached)
        except Exception:
            pass

        import json
        pipe = redis_client.raw.pipeline()
        pipe.set(cache_key, json.dumps(ref_data), ex=REFDATA_TTL)
        pipe.set(f"{ASSET_TYPE_PREFIX}{symbol}", asset_type, ex=REFDATA_TTL)
        # Also update the existing mcap cache for consistency
        if mcap_millions > 0:
            mcap_b = mcap_millions / 1000.0
            pipe.set(f"sentinel:mcap:{symbol}", str(mcap_b), ex=REFDATA_TTL)
        await pipe.execute()

        # Promote to Knowledge Graph via GraphWriter (§8.1)
        if graph_writer:
            try:
                await graph_writer.upsert_equity(
                    ticker=symbol,
                    data={
                        "sector": sector,
                        "industry": industry,
                        "indices": ref_data.get("index_membership", []),
                    }
                )
            except Exception as ge:
                logger.debug(f"GraphWriter promotion failed for {symbol}: {ge}")

        logger.debug(f"Cached ref data for {symbol}: sector={sector}, exchange={exchange}, type={asset_type}")
        return ref_data

    except Exception as e:
        logger.error(f"Failed to fetch ref data for {symbol}: {e}")
        return None
    finally:
        if owns_session:
            await session.close()


async def get_reference_data(redis_client, symbol: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve cached reference data for a symbol. Returns None if not cached.
    This is safe for hot-path usage — Redis read only, no HTTP calls.
    """
    import json
    try:
        cached = await redis_client.raw.get(f"{REFDATA_PREFIX}{symbol}")
        if cached:
            data = json.loads(cached)
            # Merge index membership from separate cache if not already present
            if not data.get("index_membership"):
                try:
                    idx_cached = await redis_client.raw.get(f"{INDEX_MEMBERSHIP_PREFIX}{symbol}")
                    if idx_cached:
                        data["index_membership"] = json.loads(idx_cached)
                except Exception:
                    pass
            return data
    except Exception:
        pass
    return None


async def fetch_index_constituents(redis_client, session=None, graph_writer=None):
    """
    Fetch constituents for major US indices from Finnhub /index/constituents,
    cache reverse map in Redis, and emit MEMBER_OF graph proposals (§8.1).
    
    Designed for daily batch usage only. Rate-limited with 500ms delay between calls.
    """
    import aiohttp
    import json

    if not FINNHUB_API_KEY:
        logger.debug("No FINNHUB_API_KEY set, skipping index constituents fetch")
        return

    owns_session = session is None
    if owns_session:
        session = aiohttp.ClientSession()

    # Build reverse map: ticker → [index_label, ...]
    reverse_map: Dict[str, list] = {}

    try:
        for finnhub_symbol, label in INDEX_SYMBOLS:
            try:
                url = "https://finnhub.io/api/v1/index/constituents"
                async with session.get(
                    url, params={"symbol": finnhub_symbol, "token": FINNHUB_API_KEY}
                ) as resp:
                    if resp.status == 429:
                        logger.debug(f"Finnhub rate-limited during index constituents fetch for {finnhub_symbol}")
                        await asyncio.sleep(1.0)
                        continue
                    if resp.status != 200:
                        logger.debug(f"Finnhub index/constituents returned {resp.status} for {finnhub_symbol}")
                        continue

                    data = await resp.json()
                    constituents = data.get("constituents", [])
                    for ticker in constituents:
                        reverse_map.setdefault(ticker, []).append(label)

                    logger.debug(f"Fetched {len(constituents)} constituents for {label}")
            except Exception as e:
                logger.debug(f"Failed to fetch index constituents for {finnhub_symbol}: {e}")

            await asyncio.sleep(0.5)  # Rate limit between index calls

        # Cache each ticker's index membership in Redis & promote to graph
        if reverse_map:
            pipe = redis_client.raw.pipeline()
            for ticker, indices in reverse_map.items():
                pipe.set(
                    f"{INDEX_MEMBERSHIP_PREFIX}{ticker}",
                    json.dumps(sorted(set(indices))),
                    ex=REFDATA_TTL,
                )
            await pipe.execute()
            logger.info(f"Cached index membership for {len(reverse_map)} tickers across {len(INDEX_SYMBOLS)} indices")

            if graph_writer:
                for ticker, indices in reverse_map.items():
                    for idx in indices:
                        try:
                            await graph_writer.upsert_index(idx)
                            await graph_writer.link_entities(
                                source_id=ticker.upper(),
                                relation_type="MEMBER_OF",
                                target_id=str(idx).upper(),
                                properties={"weight": 1.0},
                                source_label="Company",
                                target_label="Index"
                            )
                        except Exception as ge:
                            logger.debug(f"Graph index link failed for {ticker} -> {idx}: {ge}")

    except Exception as e:
        logger.error(f"Index constituents batch fetch failed: {e}")
    finally:
        if owns_session:
            await session.close()


async def refresh_watchlist_reference_data(redis_client, session=None, graph_writer=None):
    """
    Batch-refresh reference data for all symbols on the active equity watchlist.
    Intended to be called once per day from a scheduled job.
    Respects Finnhub rate limits with a 250ms delay between calls.
    """
    import aiohttp

    try:
        # Get all watched equities
        watched = await redis_client.raw.zrevrange("sentinel:watched:equities", 0, -1)
        if not watched:
            logger.info("No watched equities to refresh reference data for")
            return

        symbols = [s.decode("utf-8") if isinstance(s, bytes) else s for s in watched]
        logger.info(f"Refreshing reference data for {len(symbols)} watched symbols")

        owns_session = session is None
        if owns_session:
            session = aiohttp.ClientSession()

        try:
            # Fetch index constituents first (populates index_membership cache & graph)
            await fetch_index_constituents(redis_client, session, graph_writer=graph_writer)

            success_count = 0
            for symbol in symbols:
                result = await fetch_and_cache_reference_data(redis_client, symbol, session, graph_writer=graph_writer)
                if result:
                    success_count += 1
                # Rate limit: 250ms between Finnhub calls
                await asyncio.sleep(0.25)

            logger.info(f"Reference data refresh complete: {success_count}/{len(symbols)} symbols updated")
        finally:
            if owns_session:
                await session.close()

    except Exception as e:
        logger.error(f"Reference data batch refresh failed: {e}")

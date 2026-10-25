"""
services/collector-filings/vendor_shares.py

Shares outstanding for the ranked stocks the SEC's XBRL frames leave out.

The fundamentals collector reads dimensionless frames, and a company with more
than one class of common stock reports its share count per class, so the frame
holds nothing for it. On 15 Oct 2026 that left 153 of the model's 1,000 ranked
stocks without a share count -- GOOGL, GOOG, META, V, MA, NKE, UPS, CMCSA, PLTR
and COIN among them -- along with foreign filers (AZN, BHP, BP, RY, TD). Their
size and value features were blank. Finnhub's company profile reports the total
outstanding across classes; it is kept in its own table and used only where the
SEC has nothing.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
from pathlib import Path
from typing import Any, Optional

import aiohttp

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.db import get_timescale  # noqa: E402
from shared.utils.quiet_failures import swallowed  # noqa: E402

logger = logging.getLogger("collector_filings.vendor_shares")

FINNHUB_PROFILE = "https://finnhub.io/api/v1/stock/profile2"
INTERVAL_SEC = 7 * 86400
PAUSE_SEC = 1.2                 # Finnhub's free tier allows sixty a minute, shared with other collectors

# Ranked stocks with no SEC share count in the last two years.
_MISSING_SQL = """
    SELECT DISTINCT s.ticker FROM factor_scores_daily s
    WHERE s.as_of = (SELECT max(as_of) FROM factor_scores_daily)
      AND NOT EXISTS (SELECT 1 FROM fundamentals f WHERE f.ticker = s.ticker
                      AND f.concept = 'EntityCommonStockSharesOutstanding'
                      AND f.period_end > CURRENT_DATE - 730)
"""
_INSERT_SQL = """
    INSERT INTO vendor_shares (ticker, as_of, shares, source) VALUES ($1, CURRENT_DATE, $2, 'finnhub')
    ON CONFLICT (ticker, as_of) DO UPDATE SET shares = EXCLUDED.shares
"""


def shares_from_profile(profile: Any) -> Optional[float]:
    """Finnhub reports shareOutstanding in millions."""
    try:
        v = float((profile or {}).get("shareOutstanding"))
    except (TypeError, ValueError):
        return None
    return v * 1e6 if v > 0 else None


async def run_forever() -> None:
    key = os.getenv("FINNHUB_API_KEY", "").strip()
    if not key:
        logger.warning("Vendor shares: no FINNHUB_API_KEY; not running.")
        return
    await asyncio.sleep(300)
    while True:
        try:
            db = await get_timescale()
            tickers = [str(r["ticker"]) for r in await db.query(_MISSING_SQL) or []]
            stored = 0
            async with aiohttp.ClientSession() as session:
                for t in tickers:
                    # Finnhub writes share classes with a dot (BRK.B), as the bars do.
                    async with session.get(FINNHUB_PROFILE, params={"symbol": t, "token": key},
                                           timeout=aiohttp.ClientTimeout(total=20)) as resp:
                        if resp.status == 429:
                            await asyncio.sleep(60)
                            continue
                        shares = shares_from_profile(await resp.json(content_type=None)) if resp.status == 200 else None
                    if shares:
                        await db.execute(_INSERT_SQL, t, shares)
                        stored += 1
                    await asyncio.sleep(PAUSE_SEC)
            logger.info("Vendor shares: %s of %s ranked stocks without an SEC share count filled from Finnhub.",
                        stored, len(tickers))
        except Exception as e:
            swallowed("collector_filings.vendor_shares", e, logger)
        await asyncio.sleep(INTERVAL_SEC)

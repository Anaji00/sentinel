"""One-off: fetch the last 10 days of daily bars for every name that had a bar in the last 45 days (the names the top-ups were missing since
6 Nov). Idempotent upserts. Run inside sentinel-tradfi: python /tmp/rdb.py"""

import asyncio
import sys

sys.path.insert(0, "/app/services/collector-tradfi")

from historical_backfill import backfill_daily_bars  # noqa: E402
from shared.db import get_timescale  # noqa: E402


async def main() -> None:
    db = await get_timescale()
    rows = await db.query("SELECT DISTINCT ticker FROM daily_bars WHERE day > CURRENT_DATE - 45")
    names = sorted({str(r["ticker"]).upper() for r in rows or []})
    print("names to refresh:", len(names), flush=True)
    rep = await backfill_daily_bars(db, names, lookback_days=10)
    print({k: (v if k != "errors" else v[:3]) for k, v in rep.items()}, flush=True)
    r = await db.query("SELECT day, count(*) AS n FROM daily_bars WHERE day > CURRENT_DATE - 8 GROUP BY 1 ORDER BY 1")
    print("tickers per day now:", [(str(x["day"]), x["n"]) for x in r or []])


asyncio.run(main())

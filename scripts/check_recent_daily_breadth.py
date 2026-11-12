"""How many tickers have a daily bar on each recent day, against the factor engine's date filter. Read-only."""

import asyncio

from shared.db import get_timescale


async def main() -> None:
    db = await get_timescale()
    rows = await db.query("SELECT day, count(*) AS n FROM daily_bars WHERE day > CURRENT_DATE - 14 GROUP BY 1 ORDER BY 1")
    print("tickers with a daily bar, by day:", [(str(r["day"]), r["n"]) for r in rows or []])


asyncio.run(main())

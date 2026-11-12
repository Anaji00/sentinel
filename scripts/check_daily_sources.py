"""Why daily-bar coverage halved on 9 Nov: rows per day by source, and what the extra names are. Read-only."""

import asyncio

from shared.db import get_timescale


async def main() -> None:
    db = await get_timescale()
    rows = await db.query("SELECT day, source, count(*) AS n FROM daily_bars WHERE day >= '2026-10-28' GROUP BY 1, 2 ORDER BY 1, 2")
    print("rows by day and source:")
    for r in rows or []:
        print(f"  {r['day']} {r['source']:24s} {r['n']}")
    r = await db.query_one("SELECT count(*) AS n FROM (SELECT ticker FROM daily_bars WHERE day = '2026-11-05' EXCEPT SELECT ticker FROM daily_bars WHERE day = '2026-11-10') x")
    print("names with a bar on 5 Nov and none on 10 Nov:", r["n"])
    rows = await db.query("SELECT ticker, (SELECT max(day) FROM daily_bars d2 WHERE d2.ticker = d.ticker) AS last_day, (SELECT count(*) FROM daily_bars d3 WHERE d3.ticker = d.ticker) AS n "
                          "FROM (SELECT ticker FROM daily_bars WHERE day = '2026-11-05' EXCEPT SELECT ticker FROM daily_bars WHERE day = '2026-11-10') d ORDER BY random() LIMIT 8")
    print("sample of them (ticker, last bar, total bars):", [(x["ticker"], str(x["last_day"]), x["n"]) for x in rows or []])
    rows = await db.query("SELECT d.day, count(*) AS n, count(*) FILTER (WHERE d.volume = 0 OR d.volume IS NULL) AS zero_vol, count(*) FILTER (WHERE d.open = d.close AND d.high = d.low AND d.open = d.high) AS flat "
                          "FROM daily_bars d WHERE d.day IN ('2026-11-05', '2026-11-06', '2026-11-10') GROUP BY 1 ORDER BY 1")
    print("zero-volume and flat bars:", [(str(x["day"]), x["n"], x["zero_vol"], x["flat"]) for x in rows or []])


asyncio.run(main())

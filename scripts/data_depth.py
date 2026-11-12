"""What the stored bars can honestly support: depth per timeframe, universe sizes, and the macro series. Read-only. Run in a service container."""

import asyncio

from shared.db import get_timescale


async def main() -> None:
    db = await get_timescale()
    r = await db.query_one("SELECT count(DISTINCT ticker) AS n, min(day) AS lo, max(day) AS hi FROM daily_bars")
    print("daily_bars: tickers", r["n"], "| from", r["lo"], "to", r["hi"])
    r = await db.query_one("SELECT count(*) AS n FROM (SELECT ticker FROM daily_bars GROUP BY ticker HAVING count(*) >= 1000) x")
    print("  tickers with 1000+ daily bars (about 4 years):", r["n"])
    for t, tbl, col in (("1m", "tradfi_bars", "time"), ("5m", "tradfi_bars_5m", "bucket_time"), ("1h", "tradfi_bars_1h", "bucket_time"), ("1d", "tradfi_bars_1d", "bucket_time")):
        lo = await db.query_one(f"SELECT min({col}) AS lo, max({col}) AS hi FROM {tbl}")
        r7 = await db.query_one(f"SELECT count(DISTINCT ticker) AS n FROM {tbl} WHERE {col} > NOW() - INTERVAL '3 days'")
        print(f"{t}: tickers with data in the last 3 days {r7['n']} | earliest {str(lo['lo'])[:19]} | latest {str(lo['hi'])[:19]}")
    names = ["^DJI", "DJI", "DIA", "^VIX", "VIX", "VIXY", "CL=F", "BZ=F", "WTI", "BRENT", "USO", "BNO", "TNX", "^TNX", "^IRX", "TLT", "IEF", "DXY", "UUP", "EURUSD", "BTC-USD", "ES=F", "GC=F", "SPY", "QQQ"]
    print("\nmacro and index symbols: daily bars (count, latest) | minute bars last 7 days")
    for n in names:
        d = await db.query_one("SELECT count(*) AS c, max(day) AS m FROM daily_bars WHERE ticker = $1", n)
        m = await db.query_one("SELECT count(*) AS c FROM tradfi_bars WHERE ticker = $1 AND time > NOW() - INTERVAL '3 days'", n)
        a = await db.query_one("SELECT count(*) AS c, max(bucket_time) AS m FROM tradfi_bars_1d WHERE ticker = $1", n)
        if d["c"] or m["c"] or a["c"]:
            print(f"  {n:8s} daily_bars {d['c']:5d} {str(d['m'])[:10]} | 1d aggregate {a['c']:5d} {str(a['m'])[:10]} | minute (3 days) {m['c']}")
        else:
            print(f"  {n:8s} none")
    r = await db.query_one("SELECT count(*) AS n FROM (SELECT ticker FROM tradfi_bars WHERE time > NOW() - INTERVAL '3 days' GROUP BY ticker HAVING count(*) > 600) x")
    print("\ntickers with more than 300 minute bars in the last 2 days over 3 days (a real intraday chart):", r["n"])


asyncio.run(main())

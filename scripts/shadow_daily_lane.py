"""Shadow run of the daily market-neutral correlation lane: which stock pairs stay related after the market is removed. Read-only.
Run inside a service container: python /tmp/sdl.py"""

import asyncio

from shared.db import get_redis, get_timescale
from shared.utils.asset_universe import load_asset_meta
from shared.utils import residual_corr as rc

CORE = ["XOM", "CVX", "OXY", "LMT", "RTX", "GD", "NVDA", "AMD", "AVGO", "TSM", "ASML", "MU", "INTC", "AAPL", "MSFT", "GOOGL", "AMZN", "META"]
N_SESSIONS = 250
UNIVERSE = 300


async def main() -> None:
    db = await get_timescale()
    meta = await load_asset_meta((await get_redis()).raw)
    stocks = {s for s, m in meta.items() if m and m[0] == "stock"}
    liquid = await db.query(
        "SELECT ticker FROM (SELECT ticker, avg(close * volume) AS dv, count(*) AS n FROM daily_bars WHERE day > CURRENT_DATE - 400 GROUP BY 1) x "
        "WHERE n >= $1 ORDER BY dv DESC LIMIT $2", N_SESSIONS + 10, UNIVERSE)
    tickers = sorted(({r["ticker"] for r in liquid or []} | set(CORE)) & stocks | {"SPY"})
    rows = await db.query("SELECT ticker, day, close FROM daily_bars WHERE ticker = ANY($1::text[]) AND day > CURRENT_DATE - 450 AND close > 0 ORDER BY day", tickers)
    by = {}
    for r in rows or []:
        by.setdefault(r["ticker"], {})[r["day"]] = float(r["close"])
    spy_days = sorted(by.get("SPY", {}))[-(N_SESSIONS + 1):]
    closes = {}
    for t, d in by.items():
        if t != "SPY" and all(day in d for day in spy_days):
            closes[t] = [d[day] for day in spy_days]
    out = rc.stable_pairs(closes, [by["SPY"][day] for day in spy_days], q=0.05)
    n = len(closes)
    print(f"names {n} | sessions {len(spy_days) - 1} | pairs tested {out['tested']}")
    print(f"significant at FDR 5%: raw {out['raw_significant']} ({out['raw_significant'] / max(1, out['tested']):.1%}) | after removing the market {out['neutral_significant']} ({out['neutral_significant'] / max(1, out['tested']):.1%})")
    print(f"after the effect-size floor (|r| 0.35) and split-half stability: {len(out['stable'])} pairs ({len(out['stable']) / max(1, out['tested']):.2%})")
    for a, b, r, r1, r2 in out["stable"][:22]:
        print(f"  {a:6s} {b:6s} r={r:+.3f} (halves {r1:+.2f}, {r2:+.2f})")
    core = set(CORE)
    inc = [(a, b, r) for a, b, r, *_ in out["stable"] if a in core and b in core]
    print("pairs among the previously untestable and core names:", [(a, b, r) for a, b, r in inc][:20])
    print("untestable names now covered:", {t: (t in closes) for t in ("CVX", "GD", "LMT", "OXY", "RTX")})


asyncio.run(main())

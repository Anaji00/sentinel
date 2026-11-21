"""Read-only. Is the macro engine's date-clustered edge (+84 bps over SPY, t 2.03 over 24 dates) a beta effect?

94% of its calls are "down". A short call on a high-beta stock beats SPY whenever the market falls, with no information about the stock.
For each resolved macro call: stock_return and spy_return over the call's window (agent_predictions, migration 0094), the stock's 126-session
beta to SPY from daily_bars ending the day before the call, and the beta-adjusted excess = stock_return - beta * spy_return. Signed so that
positive = the call was right. One observation per date; t over dates. Also the repeat-name concentration.
Run: docker exec -i -w /app sentinel-api_gateway python - < scripts/research_macro_beta.py
"""
import asyncio
import math
import sys
from collections import Counter, defaultdict
from datetime import date, timedelta

sys.path.insert(0, "/app")
from shared.db import get_timescale  # noqa: E402

AGENT = sys.argv[1] if len(sys.argv) > 1 else "macro_intelligence_engine"


def t_over(xs):
    n = len(xs)
    if n < 3:
        return None
    m = sum(xs) / n
    sd = math.sqrt(sum((x - m) ** 2 for x in xs) / (n - 1))
    return None if sd == 0 else m / (sd / math.sqrt(n))


async def main() -> None:
    db = await get_timescale()
    calls = await db.query("""SELECT ticker, date(occurred_at) AS d, direction, stock_return, spy_return, horizon_hours FROM agent_predictions
        WHERE agent_name = $1 AND resolved_at IS NOT NULL AND direction IN ('up','down') AND stock_return IS NOT NULL AND spy_return IS NOT NULL
        AND occurred_at > NOW() - INTERVAL '90 days'""", AGENT) or []
    tickers = sorted({c["ticker"] for c in calls if c["ticker"]} | {"SPY"})
    bars = defaultdict(dict)
    for r in await db.query("SELECT ticker, day, close FROM daily_bars WHERE ticker = ANY($1::text[]) AND day > CURRENT_DATE - 330 AND close > 0 ORDER BY ticker, day", tickers) or []:
        bars[r["ticker"]][r["day"]] = float(r["close"])
    spy = bars["SPY"]

    def beta(t, end: date):
        b = bars.get(t)
        if not b:
            return None
        days = [d for d in sorted(b) if d < end][-127:]
        rs, rm = [], []
        for a, c in zip(days, days[1:]):
            if a in spy and c in spy:
                rs.append(b[c] / b[a] - 1.0)
                rm.append(spy[c] / spy[a] - 1.0)
        if len(rs) < 60:
            return None
        ms, mm = sum(rs) / len(rs), sum(rm) / len(rm)
        var = sum((x - mm) ** 2 for x in rm)
        return sum((x - ms) * (y - mm) for x, y in zip(rs, rm)) / var if var > 0 else None

    by_raw, by_adj = defaultdict(list), defaultdict(list)
    betas, names, skipped = [], Counter(), 0
    for c in calls:
        b = beta(c["ticker"], c["d"])
        if b is None:
            skipped += 1
            continue
        sign = 1.0 if c["direction"] == "up" else -1.0
        raw = sign * (float(c["stock_return"]) - float(c["spy_return"]))
        adj = sign * (float(c["stock_return"]) - b * float(c["spy_return"]))
        by_raw[c["d"]].append(raw)
        by_adj[c["d"]].append(adj)
        betas.append(b)
        names[c["ticker"]] += 1
    n = sum(len(v) for v in by_raw.values())
    dr = [sum(v) / len(v) for v in by_raw.values()]
    da = [sum(v) / len(v) for v in by_adj.values()]
    spy_mean = sum(float(c["spy_return"]) for c in calls) / max(len(calls), 1)
    print(f"{AGENT}: calls {len(calls)}, usable {n} (no beta for {skipped}), dates {len(dr)}")
    print(f"mean beta of called names {sum(betas) / len(betas):.2f}; mean SPY return over the call windows {1e4 * spy_mean:+.0f} bps")
    print(f"raw signed excess vs SPY, by date: {1e4 * sum(dr) / len(dr):+.1f} bps, t {t_over(dr)}")
    print(f"beta-adjusted signed excess, by date: {1e4 * sum(da) / len(da):+.1f} bps, t {t_over(da)}")
    print("most repeated names:", names.most_common(8), "| distinct names", len(names))


asyncio.run(main())

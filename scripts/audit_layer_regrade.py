"""Read-only. Re-grade the 24-hour agent calls with a session-aligned window and compare with the stored `excess_beta`.

Stored grading (shared/utils/excess_return.call_window): start = the UTC date of the call, end = the UTC date of call + horizon; return = close on or before end
over close on or before start. That (a) gives exactly 0 when the end date is a weekend day (every Friday call), and (b) for a call made before the
US open (UTC hour < 14:30, ET date still the previous session's evening / pre-market) starts from a close that has not happened yet.
Aligned window used here: entry = the last close at or before the call's NEW YORK date if the call is at/after 16:00 NY, else the close of the previous
NY session; exit = `horizon_hours // 24` sessions after the entry. Beta-adjusted with the stored beta_126d against SPY. One observation per entry date.
Writes nothing.
Run: docker exec -i -w /app sentinel-api_gateway python - < scripts/audit_layer_regrade.py
"""
import asyncio
import bisect
import math
import sys
from collections import defaultdict
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

sys.path.insert(0, "/app")
from shared.db import get_timescale  # noqa: E402

NY = ZoneInfo("America/New_York")
_SQL = """
SELECT agent_name, upper(ticker) AS t, direction, occurred_at AS at, horizon_hours AS h, beta_126d AS beta, excess_beta AS stored
FROM agent_predictions
WHERE excess_beta IS NOT NULL AND direction IN ('up','down') AND horizon_hours = 24 AND occurred_at > NOW() - INTERVAL '60 days'
  AND agent_name LIKE ANY (ARRAY['stock_correlation_agent%','macro_intelligence_engine','quant_trading_engine'])
"""
_BARS = "SELECT ticker, day, close FROM daily_bars WHERE ticker = ANY($1::text[]) AND day > CURRENT_DATE - 100 AND close > 0 ORDER BY ticker, day"


def t_over(xs: list[float]) -> float | None:
    n = len(xs)
    if n < 3:
        return None
    m = sum(xs) / n
    sd = math.sqrt(sum((x - m) ** 2 for x in xs) / (n - 1))
    return None if sd == 0 else m / (sd / math.sqrt(n))


def to_dt(x: object) -> datetime:
    return datetime.fromisoformat(x.replace("Z", "+00:00")) if isinstance(x, str) else x  # type: ignore[union-attr]


async def main() -> None:
    db = await get_timescale()
    rows = await db.query(_SQL) or []
    tickers = sorted({r["t"] for r in rows} | {"SPY"})
    bars: dict[str, tuple[list[date], list[float]]] = {}
    for i in range(0, len(tickers), 400):
        for r in await db.query(_BARS, tickers[i:i + 400]) or []:
            d, c = bars.setdefault(r["ticker"], ([], []))
            d.append(r["day"] if isinstance(r["day"], date) else date.fromisoformat(str(r["day"])[:10]))
            c.append(float(r["close"]))
    spy_d, spy_c = bars["SPY"]
    res: dict[str, dict[date, list[tuple[float, float]]]] = defaultdict(lambda: defaultdict(list))
    n_in = n_ok = 0
    for r in rows:
        n_in += 1
        if r["t"] not in bars or r["beta"] is None:
            continue
        at = to_dt(r["at"]).astimezone(NY)
        ref = at.date() if at.time() >= time(16, 0) else at.date().fromordinal(at.date().toordinal() - 1)
        ds, cs = bars[r["t"]]
        i = bisect.bisect_right(ds, ref) - 1
        j = bisect.bisect_right(spy_d, ref) - 1
        if i < 0 or j < 0 or i + 1 >= len(ds) or j + 1 >= len(spy_d) or ds[i + 1] != spy_d[j + 1] or ds[i] != spy_d[j]:
            continue
        s = r["h"] // 24
        if i + s >= len(ds) or j + s >= len(spy_d) or ds[i + s] != spy_d[j + s]:
            continue
        stock = cs[i + s] / cs[i] - 1.0
        spy = spy_c[j + s] / spy_c[j] - 1.0
        ex = stock - float(r["beta"]) * spy
        sign = 1.0 if r["direction"] == "up" else -1.0
        res[r["agent_name"]][ds[i]].append((sign * ex, sign * float(r["stored"])))
        n_ok += 1
    print(f"24h calls {n_in}, re-graded {n_ok}")
    print("agent | units | dates | aligned mean bps by date | t | stored-grade mean bps by date (same units) | t")
    for ag, pd in sorted(res.items()):
        a = [sum(x[0] for x in v) / len(v) for v in pd.values()]
        b = [sum(x[1] for x in v) / len(v) for v in pd.values()]
        n = sum(len(v) for v in pd.values())
        ta, tb = t_over(a), t_over(b)
        f = lambda t: "n/a" if t is None else f"{t:+.2f}"  # noqa: E731
        print(f"{ag} | {n} | {len(a)} | {10000 * sum(a) / len(a):+.0f} | {f(ta)} | {10000 * sum(b) / len(b):+.0f} | {f(tb)}")


asyncio.run(main())

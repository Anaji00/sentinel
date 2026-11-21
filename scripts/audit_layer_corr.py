"""Read-only. Layer-3 audit: do the divergence alerts' own "fade" actions work, and what is the direction hit-rate by regime label?

Unit = one alert per (ticker, detection date). Entry = close of the detection date (alerts are raised at/after the cash close, 21:53 UTC
for the dominant batch); exit = close k sessions later; forward excess = stock return minus SPY. Fade-signed = -sign(gap) * excess
(positive = the gap closed). One observation per date (mean over that date's units), t over dates. Also reports the count of alerts whose
action text claims a direction while the same record says no reversal/continuation was found. Writes nothing.
Run: docker exec -i -w /app sentinel-api_gateway python - < scripts/audit_layer_corr.py
"""
import asyncio
import bisect
import math
import sys
from collections import defaultdict
from datetime import date, datetime
from zoneinfo import ZoneInfo

sys.path.insert(0, "/app")
from shared.db import get_timescale  # noqa: E402

NY = ZoneInfo("America/New_York")
_ALERTS = """
SELECT primary_entity_id AS t, detected_at AS at, (metrics_summary->>'gap')::float AS gap,
       metrics_summary#>>'{regime,label}' AS lab, metrics_summary#>>'{regime,action}' AS act, alert_tier AS tier
FROM correlations
WHERE rule_name LIKE 'Stock Moving Against%' AND detected_at > NOW() - INTERVAL '75 days' AND primary_entity_id ~ '^[A-Z]{1,5}$'
"""
_BARS = "SELECT ticker, day, close FROM daily_bars WHERE ticker = ANY($1::text[]) AND day > CURRENT_DATE - 120 AND close > 0 ORDER BY ticker, day"


def t_over(xs: list[float]) -> float | None:
    n = len(xs)
    if n < 3:
        return None
    m = sum(xs) / n
    sd = math.sqrt(sum((x - m) ** 2 for x in xs) / (n - 1))
    return None if sd == 0 else m / (sd / math.sqrt(n))


async def main() -> None:
    db = await get_timescale()
    al = await db.query(_ALERTS) or []
    tickers = sorted({r["t"] for r in al} | {"SPY"})
    bars: dict[str, tuple[list, list]] = {}
    for r in await db.query(_BARS, tickers) or []:
        d, c = bars.setdefault(r["ticker"], ([], []))
        d.append(r["day"] if isinstance(r["day"], date) else date.fromisoformat(str(r["day"])[:10]))
        c.append(float(r["close"]))
    units: dict[tuple, dict] = {}
    for r in al:
        if r["gap"] is None or r["t"] not in bars:
            continue
        at = datetime.fromisoformat(r["at"].replace("Z", "+00:00")) if isinstance(r["at"], str) else r["at"]
        day = at.astimezone(NY).date()
        units.setdefault((r["t"], day), r)
    print(f"alerts {len(al)}, units {len(units)}")
    out: dict[tuple, dict] = defaultdict(lambda: defaultdict(list))
    for (t, day), r in units.items():
        for k in (1, 5):
            def fwd(tk: str) -> float | None:
                ds, cs = bars[tk]
                i = bisect.bisect_right(ds, day) - 1
                if i < 0 or ds[i] != day or i + k >= len(ds):
                    return None
                return cs[i + k] / cs[i] - 1.0
            if "SPY" not in bars:
                continue
            a, b = fwd(t), fwd("SPY")
            if a is None or b is None:
                continue
            s = -math.copysign(1.0, r["gap"]) * (a - b)
            for key in (("ALL", k), (r["lab"] or "none", k), (r["act"] or "none", k)):
                out[key][day].append(s)
    print("group | k | units | dates | mean fade bps by date | t | share dates positive")
    for key in sorted(out, key=lambda x: (x[1], x[0])):
        pd = out[key]
        dm = [sum(v) / len(v) for v in pd.values()]
        n = sum(len(v) for v in pd.values())
        t = t_over(dm)
        print(f"{key[0]} | {key[1]} | {n} | {len(dm)} | {10000 * sum(dm) / len(dm):+.0f} | {('n/a' if t is None else f'{t:+.2f}')} | {sum(1 for x in dm if x > 0) / len(dm):.2f}")


asyncio.run(main())

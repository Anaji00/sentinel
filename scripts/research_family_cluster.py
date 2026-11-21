"""Read-only. Re-grade correlation rule families with day-level independence and a same-day control.

The stored grades (sentinel:grades:producers) test each alert as independent and compare its forward move with 1.0. Alerts cluster on a few days
(family_days: 13 days for 2,939 peer-decoupling alerts), and a stock that fires an alert has often just moved, so both inflate t. Here, per family:
  units  = one alert per (family, ticker, detection date)
  ratio  = |abnormal move| / usual size over k sessions from the first open after detection, net of SPY (producer_grades.move_ratio)
  control = the mean ratio of up to CONTROL random stocks on the same date and k (what a no-information alert would see that day)
  diff   = unit ratio - that date's control mean; the statistic is the mean of per-date mean diffs, with a t over DATES (not alerts)
Run:  docker exec -i -w /app sentinel-agents-heavy python - < scripts/research_family_cluster.py   (about 1 GiB at most, one minute)
"""
import asyncio
import math
import random
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

sys.path.insert(0, "/app")

from shared.db import get_timescale  # noqa: E402
from shared.utils.producer_grades import move_ratio, rule_family  # noqa: E402

DAYS = 90
CONTROL = 120
KS = (1, 5)
MIN_DAYS = 8
random.seed(20261113)

_SQL = """
SELECT rule_name, primary_entity_id AS t, detected_at AS at FROM correlations
WHERE detected_at > NOW() - ($1 || ' days')::interval AND primary_entity_id ~ '^[A-Z]{1,5}$'
"""
_BARS = "SELECT ticker, day, open, close FROM daily_bars WHERE ticker = ANY($1::text[]) AND day > CURRENT_DATE - 260 AND close > 0 AND open > 0 ORDER BY ticker, day"


def tstat(xs):
    n = len(xs)
    if n < 3:
        return None
    m = sum(xs) / n
    sd = math.sqrt(sum((x - m) ** 2 for x in xs) / (n - 1))
    return None if sd == 0 else m / (sd / math.sqrt(n))


async def main() -> None:
    db = await get_timescale()
    rows = await db.query(_SQL, str(DAYS)) or []
    units = {}
    for r in rows:
        at = r["at"]
        at = datetime.fromisoformat(at.replace("Z", "+00:00")) if isinstance(at, str) else at
        fam = rule_family(r["rule_name"])
        units.setdefault((fam, r["t"], at.astimezone(timezone.utc).date()), at)
    tickers = sorted({k[1] for k in units} | {"SPY"})
    # a control pool: tickers with bars, drawn from the unit tickers' universe plus the most active names
    bars = {}
    for i in range(0, len(tickers), 400):
        for r in await db.query(_BARS, tickers[i:i + 400]) or []:
            b = bars.setdefault(r["ticker"], ([], [], []))
            d = r["day"] if isinstance(r["day"], date) else date.fromisoformat(str(r["day"])[:10])
            b[0].append(d)
            b[1].append(float(r["open"]))
            b[2].append(float(r["close"]))
    spy = bars.get("SPY")
    if not spy:
        print("no SPY bars")
        return
    spy_by_day = {d: (o, c) for d, o, c in zip(*spy)}
    pool = [t for t in bars if t != "SPY" and len(bars[t][0]) > 90]
    print(f"correlations {len(rows)}, units {len(units)}, tickers with bars {len(pool)}")

    def ratio(t, at, k):
        b = bars.get(t)
        return move_ratio(b[0], b[1], b[2], spy_by_day, at, k) if b else None

    dates = sorted({k[2] for k in units})
    ctrl = {}
    for d in dates:
        at = datetime(d.year, d.month, d.day, 15, 0, tzinfo=timezone.utc)
        for k in KS:
            vals = []
            for t in random.sample(pool, min(CONTROL, len(pool))):
                v = ratio(t, at, k)
                if v:
                    vals.append(v[0])
            if len(vals) >= 30:
                ctrl[(d, k)] = sum(vals) / len(vals)
    fam_days = defaultdict(lambda: defaultdict(list))        # (fam, k) -> date -> [diff]
    fam_raw = defaultdict(lambda: defaultdict(list))
    for (fam, t, d), at in units.items():
        for k in KS:
            v = ratio(t, at, k)
            c = ctrl.get((d, k))
            if v and c:
                fam_days[(fam, k)][d].append(v[0] - c)
                fam_raw[(fam, k)][d].append(v[0])
    out = []
    for (fam, k), by in fam_days.items():
        if len(by) < MIN_DAYS:
            continue
        per_day = [sum(x) / len(x) for x in by.values()]
        raw_day = [sum(x) / len(x) for x in fam_raw[(fam, k)].values()]
        n = sum(len(x) for x in by.values())
        top3 = sorted((len(x) for x in by.values()), reverse=True)[:3]
        out.append((fam, k, n, len(by), sum(raw_day) / len(raw_day), sum(per_day) / len(per_day), tstat(per_day), sum(top3) / n))
    out.sort(key=lambda r: (r[1], -(r[6] or -99)))
    print("family | k | units | days | mean ratio (by day) | mean excess over same-day control | t over days | share of units on the 3 biggest days")
    for fam, k, n, nd, raw, diff, t, share in out:
        print(f"{fam[:58]:58s} | {k} | {n:5d} | {nd:3d} | {raw:5.2f} | {diff:+6.2f} | {('%.2f' % t) if t is not None else 'na':>6s} | {share:.2f}")


asyncio.run(main())

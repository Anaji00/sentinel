"""Registered days-to-cover trial on the short signal cells (Upgrade pre-registrations - 12 Nov). Read-only.
Run in agents-heavy in the background: docker exec -w /app sentinel-agents-heavy python /tmp/scr.py > out.txt"""
import asyncio
import bisect
import collections
import math
import statistics
import time
from datetime import datetime, timedelta, timezone

from services.agents import signal_engine as se
from shared.db import get_redis, get_timescale
from shared.utils.reaction_study import HORIZONS, measure, scaled_horizon_stats
from shared.utils.signal_families import episodes

FAMILIES = {"8k_delisting_notice", "8k_dilution", "insider_opportunistic_sell", "8k_auditor_change", "earnings_reaction_down", "stock_news_negative"}
HORIZON_NAMES = ("5d", "30d")
CROWDED = 8.0
LAG_DAYS = 14
MIN_CROWDED_UNITS = 30


def welch(a, b):
    if len(a) < 2 or len(b) < 2:
        return 0.0
    va, vb = statistics.variance(a) / len(a), statistics.variance(b) / len(b)
    return (statistics.fmean(a) - statistics.fmean(b)) / math.sqrt(va + vb) if va + vb > 0 else 0.0


async def main() -> None:
    t0 = time.time()
    db, redis = await get_timescale(), await get_redis()
    eng = se.SignalEngine(db, redis)
    now = datetime.now(timezone.utc)
    since = now - timedelta(days=se.STUDY_LOOKBACK_DAYS)
    obs = await eng._load_observations(since, now)
    deep_since = now - timedelta(days=se.HISTORY_LOOKBACK_DAYS)
    obs += await eng._load_observations(deep_since, since, se.DEEP_HISTORY_TYPES, step_days=30)
    eps = [e for e in await asyncio.to_thread(episodes, obs) if e.family in FAMILIES]
    by_inst = collections.defaultdict(list)
    for e in eps:
        by_inst[e.instrument].append(e)
    print(f"episodes {len(eps)} | instruments {len(by_inst)} | {time.time() - t0:.0f}s", flush=True)
    si = collections.defaultdict(list)
    for r in await db.query("SELECT upper(ticker) AS t, settlement_date, days_to_cover FROM short_interest WHERE days_to_cover IS NOT NULL ORDER BY settlement_date"):
        d = r["settlement_date"]
        d = d if hasattr(d, "toordinal") else datetime.fromisoformat(str(d)).date()
        si[r["t"]].append((d, float(r["days_to_cover"])))
    dates = {t: [d for d, _ in v] for t, v in si.items()}

    def dtc(t, when):
        v = si.get(t)
        if not v:
            return None
        i = bisect.bisect_right(dates[t], (when - timedelta(days=LAG_DAYS)).date()) - 1
        return v[i][1] if i >= 0 else None

    daily_since = min([since] + [e.known_at for e in eps]) - timedelta(days=7)
    cells = collections.defaultdict(lambda: collections.defaultdict(list))        # (family, horizon, crowded) -> week -> returns
    done = 0
    for inst, inst_eps in by_inst.items():
        first = min(e.known_at for e in inst_eps)
        series = se.merge_daily(await eng._daily_series(inst, min(since, first) - timedelta(days=7)), await eng._series(inst, since - timedelta(days=1)))
        if series is None or inst not in si:
            continue
        bench = se.merge_daily(await eng._daily_series("SPY", daily_since), await eng._series("SPY", since - timedelta(days=1)))
        marks = [e.known_at.timestamp() for e in inst_eps]
        for hname in HORIZON_NAMES:
            h = HORIZONS[hname]
            stats = scaled_horizon_stats(series, h, marks)
            for e in inst_eps:
                d = dtc(inst, e.known_at)
                m = measure(e, h, series, bench, stats)
                if d is None or m is None:
                    continue
                cells[(e.family, hname, d >= CROWDED)][e.known_at.isocalendar()[:2]].append(m.abnormal * m.direction)
        done += 1
        if done % 1500 == 0:
            print(f"  {done} instruments, {time.time() - t0:.0f}s", flush=True)
    print("\nfamily | horizon | crowded units | uncrowded units | mean crowded | mean uncrowded | diff (bps) | Welch t | halves agree | verdict")
    for fam in sorted(FAMILIES):
        for hname in HORIZON_NAMES:
            c = {w: statistics.fmean(v) for w, v in cells[(fam, hname, True)].items()}
            u = {w: statistics.fmean(v) for w, v in cells[(fam, hname, False)].items()}
            if len(c) < MIN_CROWDED_UNITS:
                print(f"{fam:28s} {hname:3s} crowded units {len(c):4d} (under {MIN_CROWDED_UNITS}): not judged")
                continue
            cv, uv = list(c.values()), list(u.values())
            diff, t = statistics.fmean(cv) - statistics.fmean(uv), welch(cv, uv)
            weeks = sorted(set(c) | set(u))
            half = len(weeks) // 2
            agree = True
            for part in (weeks[:half], weeks[half:]):
                cc, uu = [c[w] for w in part if w in c], [u[w] for w in part if w in u]
                if len(cc) < 5 or len(uu) < 5 or (statistics.fmean(cc) - statistics.fmean(uu)) * diff <= 0:
                    agree = False
            met = diff <= -0.005 and abs(t) >= 2 and agree
            print(f"{fam:28s} {hname:3s} {len(cv):5d} {len(uv):6d}  {statistics.fmean(cv) * 1e4:+7.0f} {statistics.fmean(uv) * 1e4:+7.0f} {diff * 1e4:+7.0f}  t={t:+.2f}  {'yes' if agree else 'no '}  {'CROWDING EFFECT' if met else 'no effect shown'}")
    print(f"\ntotal {time.time() - t0:.0f}s")


asyncio.run(main())

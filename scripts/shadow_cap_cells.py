"""Registered phase 2 test, shadow run: the reaction study's cells split by size band, with three-level shrinkage and the release rule.
Read-only: nothing is written and the live gate is untouched. Heavy (it re-reads the study's archive); run in agents-heavy in the background:

    docker exec -w /app sentinel-agents-heavy python /tmp/scc.py > out.txt 2>&1
"""
import asyncio
import collections
import time
from datetime import datetime, timedelta, timezone

from services.agents import signal_engine as se
from shared.db import get_redis, get_timescale
from shared.utils import hier_bayes as hb
from shared.utils import tradable_cells
from shared.utils.reaction_study import (COST_BPS_EQUITY, HORIZONS, SIZE_BENCHMARKS, measure, scaled_horizon_stats, size_benchmark,
                                         trailing_dollar_volume)
from shared.utils.signal_families import PRICE_DIRECTED, episodes

BAND_NAME = {etf: name for (_, etf), name in zip(SIZE_BENCHMARKS, ("large", "mid", "small", "micro"))}


async def main() -> None:
    t0 = time.time()
    db, redis = await get_timescale(), await get_redis()
    eng = se.SignalEngine(db, redis)
    now = datetime.now(timezone.utc)
    since = now - timedelta(days=se.STUDY_LOOKBACK_DAYS)
    obs = await eng._load_observations(since, now)
    deep_since = now - timedelta(days=se.HISTORY_LOOKBACK_DAYS)
    if deep_since < since:
        obs += await eng._load_observations(deep_since, since, se.DEEP_HISTORY_TYPES, step_days=30)
    eps = await asyncio.to_thread(episodes, obs)
    by_inst = collections.defaultdict(list)
    for e in eps:
        by_inst[e.instrument].append(e)
    print(f"episodes {len(eps)} | instruments {len(by_inst)} | loaded in {time.time() - t0:.0f}s", flush=True)
    daily_since = min([since] + [e.known_at for e in eps]) - timedelta(days=7)
    bench_intraday = await eng._series("SPY", since - timedelta(days=1))
    bench_merged = se.merge_daily(await eng._daily_series("SPY", daily_since), bench_intraday)
    size_bench = {"SPY": bench_merged}
    for _f, etf in SIZE_BENCHMARKS:
        if etf not in size_bench:
            size_bench[etf] = se.merge_daily(await eng._daily_series(etf, daily_since), await eng._series(etf, since - timedelta(days=1)))
    intel = {i for i, es in by_inst.items() if any(e.family not in PRICE_DIRECTED for e in es)}
    cells = collections.defaultdict(lambda: collections.defaultdict(list))          # (family, horizon, band) -> ISO week -> aligned returns
    done = 0
    for inst in sorted(intel):
        if inst.endswith("=F"):
            continue
        inst_eps = by_inst[inst]
        first = min(e.known_at for e in inst_eps)
        series = se.merge_daily(await eng._daily_series(inst, min(since, first) - timedelta(days=7)), await eng._series(inst, since - timedelta(days=1)))
        if series is None:
            continue
        dv = await eng._dollar_volume(inst, min(since, first) - timedelta(days=40))
        marks = [e.known_at.timestamp() for e in inst_eps]
        for hname, h in HORIZONS.items():
            if h < timedelta(days=1):
                continue                                               # the daily horizons are the ones with deep history
            stats = scaled_horizon_stats(series, h, marks)
            for e in inst_eps:
                etf = size_benchmark(trailing_dollar_volume(dv, e.known_at.timestamp()))
                m = measure(e, h, series, size_bench.get(etf) or bench_merged, stats)
                if m is not None:
                    wk = e.known_at.isocalendar()[:2]
                    cells[(e.family, hname, BAND_NAME.get(etf, etf))][wk].append(m.abnormal * m.direction)
        done += 1
        if done % 1000 == 0:
            print(f"  {done} instruments, {time.time() - t0:.0f}s", flush=True)
    # Overlapping episodes are not independent evidence (one filing wave, one earnings week). Each family-horizon-band-week is one unit:
    # the mean of its episodes, as the live study's independent units do.
    cells = {k: [sum(v) / len(v) for v in weeks.values()] for k, weeks in cells.items()}
    cost = COST_BPS_EQUITY / 1e4
    posts = hb.shrink(cells, cost=cost, min_cell_n=10)
    cand = hb.release_candidates(posts)
    print(f"cells with 10+ weekly units: {len(posts)} | release candidates under the registered rule: {len(cand)} | cost per observation {COST_BPS_EQUITY} bps")
    tradable = {(f, h) for f, h in tradable_cells.TRADABLE_CELLS}
    print("\nthe five tradable cells, by size band (raw mean -> posterior, P(net > 0), effective N):")
    for p in sorted(posts, key=lambda x: x.key):
        if (p.key[0], p.key[1]) in tradable:
            print(f"  {p.key[0]:30s} {p.key[1]:4s} {p.key[2]:6s} units={p.n:5d} raw {p.raw_mean * 1e4:+7.1f} bps -> {p.post_mean * 1e4:+7.1f} bps  P={p.p_positive:.3f}  Neff={p.n_eff:8.0f}  {'CANDIDATE' if p.release_candidate else ''}")
    print("\nrelease candidates that are NOT among the five tradable cells:")
    for p in sorted(cand, key=lambda x: -x.p_positive):
        if (p.key[0], p.key[1]) not in tradable:
            print(f"  {p.key[0]:30s} {p.key[1]:4s} {p.key[2]:6s} units={p.n:5d} raw {p.raw_mean * 1e4:+7.1f} bps -> {p.post_mean * 1e4:+7.1f} bps  P={p.p_positive:.3f}  Neff={p.n_eff:8.0f}")
    print(f"\ntotal {time.time() - t0:.0f}s")


asyncio.run(main())

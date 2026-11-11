"""G4 (reports/Event backtest scope.md): re-score the reaction study's proven cells on liquid and borrowable names.

Run inside an agents container (it needs the database and Redis): python scripts/audit_tradable_signals.py
Read-only: nothing is written to Redis or the database.
"""
import asyncio
import json
from datetime import datetime, timedelta, timezone

from shared.db import get_redis, get_timescale
from shared.utils.asset_universe import load_asset_meta
from shared.utils.reaction_study import (
    HORIZONS, SIZE_BENCHMARKS, evaluate_cell, measure, scaled_horizon_stats, size_benchmark, trailing_dollar_volume,
)
from shared.utils.signal_families import episodes
from shared.utils.signal_store import STUDY_KEY
from services.agents.signal_engine import STUDY_LOOKBACK_DAYS, SignalEngine, merge_daily

# The window the published study used (7 Nov 2026, before headline history and the 10-year EDGAR window were added),
# pinned so that V0 reproduces the published cells whatever the engine's defaults become.
DEEP_HISTORY_TYPES = ["filing", "earnings_reaction", "insider_trade", "thirteen_f"]
HISTORY_LOOKBACK_DAYS = 5 * 365 + 30

CUTOFF_SQL = ("SELECT avg_dv FROM (SELECT ticker, avg(close*volume) AS avg_dv FROM daily_bars WHERE day >= CURRENT_DATE - 60 "
              "GROUP BY 1 ORDER BY 2 DESC LIMIT 1000) t ORDER BY avg_dv ASC LIMIT 1")
SURVIVES = dict(units=20, t=2.0, hit=0.53)


def verdict(cell: dict, v0: dict) -> str:
    if cell["units"] < SURVIVES["units"]:
        return "cannot be tested"
    ok = (cell.get("mean_bps") or 0) > (cell.get("cost_bps") or 10) and abs(cell.get("t") or 0) >= SURVIVES["t"] \
        and (cell.get("hit_rate") or 0) >= SURVIVES["hit"] and cell.get("sign") == v0.get("sign")
    return "survives" if ok else "fails"


async def main() -> None:
    db, redis = await get_timescale(), await get_redis()
    raw = getattr(redis, "raw", redis)
    study = json.loads(await raw.get(STUDY_KEY))
    proven = {(c["family"], c["horizon"]): c for c in study["cells"] if c.get("verdict") == "proven"}
    fams = {f for f, _ in proven}
    meta = await load_asset_meta(raw)
    borrowable = {t for t, m in meta.items() if m[1] and m[2]}
    cutoff = float((await db.query_one(CUTOFF_SQL))["avg_dv"])
    print(f"proven cells {len(proven)}, families {len(fams)}, liquidity cutoff ${cutoff/1e6:.1f}M a day, borrowable names {len(borrowable)}")

    eng = SignalEngine(db, redis)
    now = datetime.now(timezone.utc)
    since = now - timedelta(days=STUDY_LOOKBACK_DAYS)
    obs = await eng._load_observations(since, now)
    obs += await eng._load_observations(now - timedelta(days=HISTORY_LOOKBACK_DAYS), since, DEEP_HISTORY_TYPES, step_days=30)
    eps = [e for e in episodes(obs) if e.family in fams]
    by_inst: dict = {}
    for e in eps:
        by_inst.setdefault(e.instrument, []).append(e)
    print("episodes", len(eps), "instruments", len(by_inst))

    daily_since = min([since] + [e.known_at for e in eps]) - timedelta(days=7)
    bench_intraday = await eng._series("SPY", since - timedelta(days=1))
    bench = merge_daily(await eng._daily_series("SPY", daily_since), bench_intraday)
    size_bench = {"SPY": bench}
    for _f, etf in SIZE_BENCHMARKS:
        if etf not in size_bench:
            size_bench[etf] = merge_daily(await eng._daily_series(etf, daily_since), await eng._series(etf, since - timedelta(days=1)))

    sets = {"V0": {}, "V1": {}, "V2": {}}
    for n, (inst, ies) in enumerate(sorted(by_inst.items())):
        intraday = await eng._series(inst, since - timedelta(days=1))
        first = min(e.known_at for e in ies)
        series = merge_daily(await eng._daily_series(inst, min(since, first) - timedelta(days=7)), intraday)
        if series is None:
            continue
        dv = await eng._dollar_volume(inst, min(since, first) - timedelta(days=40))
        marks = [e.known_at.timestamp() for e in ies]
        for hname, h in HORIZONS.items():
            use = series if h >= timedelta(days=1) else intraday
            if use is None:
                continue
            stats = scaled_horizon_stats(use, h, marks)
            for e in ies:
                tdv = trailing_dollar_volume(dv, e.known_at.timestamp())
                if (e.family, hname) not in proven:
                    continue
                b = (size_bench.get(size_benchmark(tdv)) or bench) if h >= timedelta(days=1) else bench_intraday
                m = measure(e, h, use, b, stats)
                if m is None:
                    continue
                in_sets = ["V0"] + (["V1"] if tdv is not None and tdv >= cutoff else []) + (
                    ["V2"] if tdv is not None and tdv >= cutoff and inst in borrowable else [])
                for s in in_sets:
                    sets[s].setdefault((e.family, hname), []).append(m)
        if n % 500 == 0:
            print("  measured", n, "of", len(by_inst), flush=True)

    print(f"\n{'cell':34s} {'orig q':>7s} | {'V0 units/mean bps/t':>22s} | {'V1 units/mean bps/t/hit':>26s} {'':10s} | {'V2 units/mean bps/t/hit':>26s}")
    for (fam, hz), orig in sorted(proven.items()):
        res = {}
        for s in sets:
            res[s] = evaluate_cell(fam, hz, sets[s].get((fam, hz), []), len(sets[s].get((fam, hz), []))).as_dict()
        v0 = res["V0"]

        def fmt(c: dict) -> str:
            if c["verdict"] in ("unmeasurable",):
                return "none"
            return f"{c['units']:5d} {c.get('mean_bps') if c.get('mean_bps') is not None else float('nan'):8.1f} {c.get('t') if c.get('t') is not None else float('nan'):5.2f} {c.get('hit_rate') if c.get('hit_rate') is not None else float('nan'):.2f}"
        print(f"{fam + ' ' + hz:34s} {orig['q']:7.4f} | {fmt(v0)[:22]:>22s} | {fmt(res['V1']):>26s} {verdict(res['V1'], v0):16s} | {fmt(res['V2']):>26s} {verdict(res['V2'], v0)}")


asyncio.run(main())

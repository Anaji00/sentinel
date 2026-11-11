"""G5 (reports/Event backtest scope.md): how stable is each proven reaction-study cell over time and market regimes.

Run inside an agents container (it needs the database and Redis), at low priority:
    python scripts/audit_signal_stability.py [--cell "family horizon"] [--csv rolling.csv]
Read-only: nothing is written to Redis or the database (--csv writes a local file only).
Episodes are rebuilt and measured exactly as scripts/audit_tradable_signals.py does (the study's own code); the
independent units of each cell are then split by the date of their entry. Rules are registered in the report.
"""
import argparse
import asyncio
import bisect
import json
import math
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Sequence, Tuple

from shared.db import get_redis, get_timescale
from shared.utils.reaction_study import (
    HORIZONS, SIZE_BENCHMARKS, WINSOR_Z, Unit, evaluate_cell, measure, scaled_horizon_stats, size_benchmark, t_stat,
    trailing_dollar_volume, two_sided_p, units,
)
from shared.utils.signal_families import Observation, episodes, observations
from shared.utils.signal_store import STUDY_KEY
from services.agents.signal_engine import (
    SIGNAL_EVENT_TYPES, STUDY_LOOKBACK_DAYS, SignalEngine, _STUDY_EVENTS_SQL, _row,
    merge_daily,
)

# The study window the 16 cells were proven on: 5 years, four deep-history types (the engine's defaults have since grown).
DEEP_HISTORY_TYPES = ["filing", "earnings_reaction", "insider_trade", "thirteen_f"]
HISTORY_LOOKBACK_DAYS = 5 * 365 + 30
MIN_PERIOD_UNITS = 8          # a half-year with fewer units says nothing
MIN_PERIODS = 4               # fewer assessable half-years than this: the cell cannot be rated
STABLE_SHARE = 0.70           # share of assessable half-years with the overall sign
DECAY_ALPHA = 0.05            # one-sided Welch test, last two half-years against all earlier units
MIN_EARLIER_UNITS = 16
TREND_DAYS, VOL_DAYS, VOL_BASE_DAYS = 200, 20, 250
ROLL_DAYS = 365


async def load_observations(eng: SignalEngine, fams: set, start: datetime, end: datetime,
                            types: Optional[List[str]] = None, step_days: int = 1) -> List[Observation]:
    """SignalEngine._load_observations, keeping only the audited families as each chunk is read.

    The engine's version holds every observation of every family (the container's 1 GiB limit killed it);
    an episode's key starts with its family, so filtering before grouping changes nothing."""
    out: List[Observation] = []
    day = start
    while day < end:
        nxt = min(end, day + timedelta(days=step_days))
        for r in await eng.db.query(_STUDY_EVENTS_SQL, day, nxt, types or SIGNAL_EVENT_TYPES):
            out.extend(o for o in observations(_row(r)) if o.family in fams)
        day = nxt
    return out


def half_year(ts: float) -> str:
    d = datetime.fromtimestamp(ts, timezone.utc)
    return f"{d.year}H{1 if d.month <= 6 else 2}"


def winsor(u: Unit) -> float:
    return max(-WINSOR_Z, min(WINSOR_Z, u.aligned))


def summarize(us: Sequence[Unit]) -> Dict[str, Optional[float]]:
    """Units, aligned mean in bps (raw, as the study reports it) and the t of the winsorized aligned means."""
    n = len(us)
    if n == 0:
        return dict(n=0, bps=None, t=None, mean=None)
    xs = [winsor(u) for u in us]
    t = t_stat(xs)
    return dict(n=n, bps=sum(u.aligned_abnormal_bps for u in us) / n, t=t, mean=sum(xs) / n)


def regime_labels(spy_daily: Optional[Tuple[List[float], List[float]]]):
    """Return a function mapping a timestamp to (trend, vol) labels from SPY daily closes strictly before it.

    trend: 'up' when the last close is above its 200-day mean, else 'down'.
    vol: 'high' when the 20-day realised volatility of daily log returns is above its median over the prior
    250 sessions, else 'low'. None while the history is too short."""
    if not spy_daily or not spy_daily[0]:
        return lambda _t: (None, None)
    times, closes = list(spy_daily[0]), list(spy_daily[1])
    rets = [math.log(closes[i] / closes[i - 1]) if closes[i - 1] > 0 and closes[i] > 0 else 0.0 for i in range(1, len(closes))]
    vols: List[Optional[float]] = [None] * len(closes)
    for i in range(VOL_DAYS, len(closes)):
        w = rets[i - VOL_DAYS:i]            # returns ending at close i (rets[j] is close j+1 over close j)
        m = sum(w) / VOL_DAYS
        vols[i] = math.sqrt(sum((r - m) ** 2 for r in w) / (VOL_DAYS - 1))

    def label(t: float) -> Tuple[Optional[str], Optional[str]]:
        i = bisect.bisect_left(times, t) - 1     # last close strictly before t
        if i < TREND_DAYS - 1:
            return None, None
        trend = "up" if closes[i] > sum(closes[i - TREND_DAYS + 1:i + 1]) / TREND_DAYS else "down"
        vol = None
        base = [v for v in vols[max(0, i - VOL_BASE_DAYS):i] if v is not None]
        if vols[i] is not None and len(base) >= VOL_BASE_DAYS // 2:
            base.sort()
            vol = "high" if vols[i] > base[len(base) // 2] else "low"
        return trend, vol

    return label


def rate(us: Sequence[Unit], sign: int, now_ts: float) -> Dict[str, object]:
    """Stability verdict for one cell's units; `sign` is the sign of the whole-cell aligned mean."""
    by_p: Dict[str, List[Unit]] = {}
    for u in us:
        by_p.setdefault(half_year(u.entry), []).append(u)
    periods = {p: summarize(v) for p, v in sorted(by_p.items())}
    ok = {p: s for p, s in periods.items() if s["n"] >= MIN_PERIOD_UNITS}
    same = sum(1 for s in ok.values() if s["mean"] is not None and (s["mean"] > 0) == (sign > 0))
    share = same / len(ok) if ok else None

    # Decay: the two latest half-years that hold any data, against every earlier unit, in the cell's own direction.
    keys = sorted(by_p)
    recent_keys = set(keys[-2:])
    recent = [winsor(u) * sign for p in recent_keys for u in by_p[p]]
    earlier = [winsor(u) * sign for p in keys if p not in recent_keys for u in by_p[p]]
    decay_t = decay_p = None
    decaying = False
    if len(recent) >= MIN_PERIOD_UNITS and len(earlier) >= MIN_EARLIER_UNITS:
        ma, mb = sum(earlier) / len(earlier), sum(recent) / len(recent)
        va = sum((x - ma) ** 2 for x in earlier) / (len(earlier) - 1)
        vb = sum((x - mb) ** 2 for x in recent) / (len(recent) - 1)
        se2 = va / len(earlier) + vb / len(recent)
        if se2 > 0:
            decay_t = (ma - mb) / math.sqrt(se2)
            df = max(1, int(se2 ** 2 / ((va / len(earlier)) ** 2 / (len(earlier) - 1) + (vb / len(recent)) ** 2 / (len(recent) - 1))))
            decay_p = two_sided_p(decay_t, df) / 2 if decay_t > 0 else 1.0
            decaying = mb < ma and decay_p < DECAY_ALPHA
    if len(ok) < MIN_PERIODS:
        verdict = "cannot be rated"
    elif decaying:
        verdict = "decaying"
    elif share is not None and share >= STABLE_SHARE:
        verdict = "stable"
    else:
        verdict = "unstable"
    return dict(periods=periods, assessable=len(ok), same=same, share=share, decay_t=decay_t, decay_p=decay_p,
                recent_mean=(sum(recent) / len(recent) if recent else None),
                earlier_mean=(sum(earlier) / len(earlier) if earlier else None), verdict=verdict)


def rolling(us: Sequence[Unit], now_ts: float) -> List[Tuple[str, Dict[str, Optional[float]]]]:
    """Trailing 12-month summary at each month end, from the first month end with a full year of history."""
    if not us:
        return []
    ordered = sorted(us, key=lambda u: u.entry)
    first = datetime.fromtimestamp(ordered[0].entry, timezone.utc)
    y, m = first.year + 1, first.month
    out: List[Tuple[str, Dict[str, Optional[float]]]] = []
    while True:
        end = datetime(y + (m == 12), m % 12 + 1, 1, tzinfo=timezone.utc)     # first day of the next month
        if end.timestamp() > now_ts + 86400 * 31:
            break
        e = end.timestamp()
        window = [u for u in ordered if e - ROLL_DAYS * 86400 <= u.entry < e]
        out.append(((end - timedelta(days=1)).strftime("%Y-%m"), summarize(window)))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def f(x: Optional[float], spec: str = "7.1f") -> str:
    return f"{'nan':>{spec.split('.')[0]}s}" if x is None else format(x, spec)


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cell", help='only this cell, e.g. "family horizon"')
    ap.add_argument("--csv", help="write the rolling 12-month series here")
    args = ap.parse_args()
    try:
        os.nice(19)
    except (AttributeError, OSError):
        pass

    db, redis = await get_timescale(), await get_redis()
    raw = getattr(redis, "raw", redis)
    study = json.loads(await raw.get(STUDY_KEY))
    orig = {(c["family"], c["horizon"]): c for c in study["cells"] if c.get("verdict") == "proven"}
    proven = dict(orig)
    if args.cell:
        fam, hz = args.cell.rsplit(" ", 1)
        proven = {k: v for k, v in orig.items() if k == (fam, hz)}
        if not proven:
            raise SystemExit(f"{args.cell!r} is not a proven cell: {sorted(orig)}")
    fams = {fam for fam, _ in proven}
    print(f"proven cells in the study {len(orig)}, auditing {len(proven)}, families {len(fams)}")

    eng = SignalEngine(db, redis)
    now = datetime.now(timezone.utc)
    since = now - timedelta(days=STUDY_LOOKBACK_DAYS)
    obs = await load_observations(eng, fams, since, now)
    obs += await load_observations(eng, fams, now - timedelta(days=HISTORY_LOOKBACK_DAYS), since, DEEP_HISTORY_TYPES, 30)
    eps = episodes(obs)
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
    regime = regime_labels(await eng._daily_series("SPY", daily_since - timedelta(days=int((TREND_DAYS + VOL_BASE_DAYS) * 1.6))))

    cells: Dict[Tuple[str, str], list] = {}
    for n, (inst, ies) in enumerate(sorted(by_inst.items())):
        intraday = await eng._series(inst, since - timedelta(days=1))
        first = min(e.known_at for e in ies)
        series = merge_daily(await eng._daily_series(inst, min(since, first) - timedelta(days=7)), intraday)
        if series is None:
            continue
        dv = await eng._dollar_volume(inst, min(since, first) - timedelta(days=40))
        marks = [e.known_at.timestamp() for e in ies]
        for hname, h in HORIZONS.items():
            if not any((e.family, hname) in proven for e in ies):
                continue
            use = series if h >= timedelta(days=1) else intraday
            if use is None:
                continue
            stats = scaled_horizon_stats(use, h, marks)
            for e in ies:
                if (e.family, hname) not in proven:
                    continue
                tdv = trailing_dollar_volume(dv, e.known_at.timestamp())
                b = (size_bench.get(size_benchmark(tdv)) or bench) if h >= timedelta(days=1) else bench_intraday
                m = measure(e, h, use, b, stats)
                if m is not None:
                    cells.setdefault((e.family, hname), []).append(m)
        if n % 500 == 0:
            print("  measured", n, "of", len(by_inst), flush=True)

    now_ts = now.timestamp()
    table = []
    csv_rows = ["family,horizon,month,units,mean_bps,t"]
    for (fam, hz), o in sorted(proven.items()):
        ms = cells.get((fam, hz), [])
        res = evaluate_cell(fam, hz, ms, len(ms)).as_dict()
        us = units(ms, HORIZONS[hz])
        print(f"\n=== {fam} {hz}: re-run {res['units']} units, {f(res.get('mean_bps'))} bps, t {f(res.get('t'), '5.2f')}"
              f" | stored {o.get('units')} units, {f(o.get('mean_bps'))} bps, t {f(o.get('t'), '5.2f')}")
        if res["verdict"] in ("unmeasurable", "insufficient_data") or not us:
            table.append((fam, hz, "cannot be rated", res, None, None))
            print("  cannot be measured now:", res.get("reason"))
            continue
        sign = int(res["sign"])
        r = rate(us, sign, now_ts)
        print(f"  {'half-year':9s} {'units':>5s} {'bps':>8s} {'t':>6s} same-sign")
        for p, s in r["periods"].items():
            flag = "" if s["n"] < MIN_PERIOD_UNITS else ("yes" if (s["mean"] > 0) == (sign > 0) else "NO")
            print(f"  {p:9s} {s['n']:5d} {f(s['bps'], '8.1f')} {f(s['t'], '6.2f')} {flag or '(<8 units)'}")
        print(f"  assessable half-years {r['assessable']}, same sign {r['same']}, share {f(r['share'], '.2f')}; "
              f"recent-two mean {f(r['recent_mean'], '.3f')} vs earlier {f(r['earlier_mean'], '.3f')}, "
              f"decay t {f(r['decay_t'], '.2f')} p {f(r['decay_p'], '.3f')} -> {r['verdict']}")
        # Regimes: the same units split by SPY trend and by SPY volatility at entry.
        tags = {id(u): regime(u.entry) for u in us}
        for dim, idx, names in (("trend", 0, ("up", "down")), ("vol", 1, ("high", "low"))):
            parts = []
            for nm in names:
                s = summarize([u for u in us if tags[id(u)][idx] == nm])
                parts.append(f"{nm}: {s['n']} units {f(s['bps'], '.1f')} bps t {f(s['t'], '.2f')}")
            print(f"  regime {dim:5s} " + " | ".join(parts))
        roll = rolling(us, now_ts)
        for mo, s in roll:
            csv_rows.append(f"{fam},{hz},{mo},{s['n']},{'' if s['bps'] is None else round(s['bps'], 2)},{'' if s['t'] is None else round(s['t'], 3)}")
        live = [(mo, s) for mo, s in roll if s["n"] >= MIN_PERIOD_UNITS]
        if live:
            print("  rolling 12m (last 6 month ends): " + "; ".join(f"{mo} n={s['n']} {f(s['bps'], '.0f').strip()}bps" for mo, s in live[-6:]))
        table.append((fam, hz, r["verdict"], res, r, live[-1] if live else None))

    print(f"\n{'cell':34s} {'units':>5s} {'bps':>7s} {'t':>5s} {'halves':>6s} {'same':>5s} {'share':>5s} {'decay p':>7s} {'rolling12m latest':>22s}  verdict")
    for fam, hz, v, res, r, last in table:
        if r is None:
            print(f"{fam + ' ' + hz:34s} {res['units']:5d} {'':>7s} {'':>5s} {'':>6s} {'':>5s} {'':>5s} {'':>7s} {'':>22s}  {v}")
            continue
        lr = f"{last[0]} n={last[1]['n']} {f(last[1]['bps'], '.0f').strip()}bps" if last else "n/a"
        print(f"{fam + ' ' + hz:34s} {res['units']:5d} {f(res.get('mean_bps'), '7.1f')} {f(res.get('t'), '5.2f')} "
              f"{r['assessable']:6d} {r['same']:5d} {f(r['share'], '5.2f')} {f(r['decay_p'], '7.3f')} {lr:>22s}  {v}")
    if args.csv:
        with open(args.csv, "w", encoding="utf-8") as fh:
            fh.write("\n".join(csv_rows) + "\n")
        print("rolling series written to", args.csv)


asyncio.run(main())

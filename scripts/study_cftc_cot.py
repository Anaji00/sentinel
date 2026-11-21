"""Event study: CFTC Commitments of Traders positioning -> next-5-session excess return of the related ETF (Upgrade pre-registrations - 12 Nov, "Datasets with no live collector").

Signal (defined here before any run): for each contract, managed-money net position as a share of open interest, ranked against its own previous 156 weeks
(at least 104 needed, the current week excluded). Event "crowded_long" when that rank is at or above 0.90, "crowded_short" at or below 0.10. Cells are
group x side (energy, metals, grains x two sides), so a cell pools several contracts. Return: log return of the mapped ETF minus
SPY over five sessions from the close of the first session after the release date (Friday 15:30 ET, so Monday). Bars and t-statistic: shared/utils/event_study.py.
Rates, FX and equity-index futures are in the CFTC Traders in Financial Futures report, not this one, so they are not covered. Reads cftc_cot and daily_bars; writes nothing to the database.

Run in its own capped container (needs the database; about 30 s):
    docker run --rm --memory 2g --network sentinel-network --env-file .env -v <repo>/shared:/app/shared:ro -v <repo>/scripts:/app/scripts:ro \
        -w /app -e PYTHONPATH=/app sentinel-agents-heavy python scripts/study_cftc_cot.py [--out reports/study_cftc_cot.json]
Needs cftc_cot populated (the collector's first pass, 2006 onwards) and daily_bars (2016 onwards; SHY from late 2021, GLD/SLV/UNG/USO from late 2021).
Self-test of the pure helpers: python scripts/study_cftc_cot.py --selftest
"""
import argparse
import asyncio
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from shared.utils.event_study import EventRet, Series, evaluate, forward_excess, trailing_percentile  # noqa: E402

WINDOW_WEEKS = 156
MIN_WEEKS = 104
HIGH, LOW = 0.90, 0.10
# contract code -> (group, ETF). Codes are CFTC contract market codes in the disaggregated futures-only report.
CONTRACTS: Dict[str, Tuple[str, str]] = {
    "067411": ("energy", "USO"), "023651": ("energy", "UNG"),
    "088691": ("metals", "GLD"), "084691": ("metals", "SLV"), "085692": ("metals", "CPER"),
    "002602": ("grains", "CORN"), "001602": ("grains", "WEAT"), "005602": ("grains", "SOYB"),
}


def build_events(weekly: Dict[str, List[Tuple[date, date, float]]], prices: Dict[str, Series], bench: Dict[date, float]) -> List[EventRet]:
    """weekly: contract -> ascending (report_date, release date, managed-money net / open interest). Returns (cell, release date, excess return)."""
    out: List[EventRet] = []
    for code, rows in weekly.items():
        if code not in CONTRACTS or CONTRACTS[code][1] not in prices:
            continue
        group, etf = CONTRACTS[code]
        pct = trailing_percentile([r[2] for r in rows], WINDOW_WEEKS, MIN_WEEKS)
        for (rep, rel, _), p in zip(rows, pct):
            if p is None or LOW < p < HIGH:
                continue
            ret = forward_excess(prices[etf], bench, rel)
            if ret is not None:
                out.append((f"{group}:{'crowded_long' if p >= HIGH else 'crowded_short'}", rel, ret))
    return out


def build_adjusted(weekly: Dict[str, List[Tuple[date, date, float]]], prices: Dict[str, Series], bench: Dict[date, float]):
    """Amendment DS-1: each event's excess return minus its group's unconditional average over EVERY week with a percentile (the baseline), so an instrument's
    own drift against SPY (oil and gas ETF roll decay, gold) is not read as positioning information. Returns (adjusted events, baselines by group)."""
    per_group: Dict[str, List[float]] = {}
    events: List[EventRet] = []
    for code, rows in weekly.items():
        if code not in CONTRACTS or CONTRACTS[code][1] not in prices:
            continue
        group, etf = CONTRACTS[code]
        pct = trailing_percentile([r[2] for r in rows], WINDOW_WEEKS, MIN_WEEKS)
        for (rep, rel, _), p in zip(rows, pct):
            if p is None:
                continue
            ret = forward_excess(prices[etf], bench, rel)
            if ret is None:
                continue
            per_group.setdefault(group, []).append(ret)
            if p >= HIGH or p <= LOW:
                events.append((f"{group}:{'crowded_long' if p >= HIGH else 'crowded_short'}", rel, ret))
    base = {g: sum(v) / len(v) for g, v in per_group.items()}
    return [(cell, rel, ret - base[cell.split(":")[0]]) for cell, rel, ret in events], base


def contrast(events: List[EventRet]) -> List[Dict[str, object]]:
    """Per group: mean adjusted return of crowded_long minus crowded_short, Welch t, and whether both halves (by date) agree in sign."""
    import math
    import statistics
    out = []
    for g in sorted({c.split(":")[0] for c, _, _ in events}):
        lo = [(d, r) for c, d, r in events if c == f"{g}:crowded_long"]
        sh = [(d, r) for c, d, r in events if c == f"{g}:crowded_short"]
        if len(lo) < 100 or len(sh) < 100:
            out.append({"group": g, "n_long": len(lo), "n_short": len(sh), "verdict": "under 100 events a side: not judged"})
            continue
        a, b = [r for _, r in lo], [r for _, r in sh]
        diff = statistics.fmean(a) - statistics.fmean(b)
        t = diff / math.sqrt(statistics.variance(a) / len(a) + statistics.variance(b) / len(b))
        cut = sorted(d for d, _ in lo + sh)[len(lo + sh) // 2]
        halves = []
        for first in (True, False):
            aa = [r for d, r in lo if (d <= cut) == first]
            bb = [r for d, r in sh if (d <= cut) == first]
            halves.append(statistics.fmean(aa) - statistics.fmean(bb) if len(aa) >= 20 and len(bb) >= 20 else 0.0)
        agree = halves[0] * halves[1] > 0 and halves[0] * diff > 0
        out.append({"group": g, "n_long": len(lo), "n_short": len(sh), "mean_long_bps": round(statistics.fmean(a) * 1e4, 1),
                    "mean_short_bps": round(statistics.fmean(b) * 1e4, 1), "long_minus_short_bps": round(diff * 1e4, 1), "t": round(t, 2),
                    "halves_bps": [round(h * 1e4, 1) for h in halves], "same_sign_halves": agree,
                    "passes": bool(diff > 0 and abs(t) >= 2 and agree)})
    return out


def _release_day(published_at: datetime) -> date:
    """Calendar date of the release in US Eastern (the stored time is UTC)."""
    from zoneinfo import ZoneInfo
    if isinstance(published_at, str):          # the database pool returns timestamps as ISO strings
        published_at = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
    return published_at.astimezone(ZoneInfo("America/New_York")).date()


async def run(out_path: Optional[str], adjusted: bool = False) -> None:
    from shared.db import get_timescale
    from study_common import bench_map, load_series
    db = await get_timescale()
    rows = await db.query("SELECT contract_code, report_date, published_at, mm_long, mm_short, open_interest FROM cftc_cot "
                          "WHERE contract_code = ANY($1::text[]) AND open_interest > 0 AND mm_long IS NOT NULL AND mm_short IS NOT NULL "
                          "ORDER BY contract_code, report_date", sorted(CONTRACTS))
    weekly: Dict[str, List[Tuple[date, date, float]]] = {}
    for r in rows or []:
        weekly.setdefault(r["contract_code"], []).append(
            (r["report_date"], _release_day(r["published_at"]), (r["mm_long"] - r["mm_short"]) / r["open_interest"]))
    prices = await load_series_for(db, load_series)
    if adjusted:
        events, base = build_adjusted(weekly, prices, bench_map(prices))
        print("DS-1 baselines (bps per 5 sessions, unconditional):", {g: round(v * 1e4, 1) for g, v in base.items()})
        for row in contrast(events):
            print("CONTRAST", json.dumps(row))
    else:
        events = build_events(weekly, prices, bench_map(prices))
    result = evaluate(events)
    print(f"contracts with data: {sorted(weekly)}  events: {len(events)}")
    for r in result:
        print(json.dumps(r))
    if out_path:
        Path(out_path).write_text(json.dumps(result, indent=1), encoding="utf-8")


async def load_series_for(db, load_series):
    return await load_series(db, [etf for _, etf in CONTRACTS.values()], date(2016, 1, 1))


def selftest() -> None:
    # 120 weeks of cycling positioning, then a spike week: the spike is crowded_long, and an ETF that falls 10% in the next five sessions gives a negative excess.
    start = date(2020, 1, 7)
    weekly = [(start + timedelta(weeks=i), start + timedelta(weeks=i, days=3), 0.01 * (i % 7)) for i in range(120)]
    weekly.append((start + timedelta(weeks=120), start + timedelta(weeks=120, days=3), 0.5))
    rel = weekly[-1][1]
    days = [date(2020, 1, 1) + timedelta(days=i) for i in range(1100) if (date(2020, 1, 1) + timedelta(days=i)).weekday() < 5]
    etf = ([d for d in days], [100.0 if d <= rel + timedelta(days=3) else 90.0 for d in days])
    spy = (list(days), [100.0] * len(days))
    ev = build_events({"067411": weekly}, {"USO": etf, "SPY": spy}, dict(zip(*spy)))
    spike = [e for e in ev if e[1] == rel]
    assert len(spike) == 1 and spike[0][0] == "energy:crowded_long" and spike[0][2] < -0.05, ev
    assert all(e[1] > weekly[MIN_WEEKS - 1][1] for e in ev), "no event before the minimum history"
    print("study_cftc_cot selftest ok:", len(ev), "events")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--out", default=None)
    ap.add_argument("--adjusted", action="store_true", help="amendment DS-1: baseline-adjusted cells and the crowded_long minus crowded_short contrast")
    a = ap.parse_args()
    if a.selftest:
        selftest()
    else:
        asyncio.run(run(a.out, a.adjusted))

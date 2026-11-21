"""Event study: Regulation SHO threshold list entries and exits -> next-5-session excess return of the listed stock (Upgrade pre-registrations - 12 Nov, "Datasets with no live collector").

Events (defined here before any run): an "entry" is a symbol on a list date that was not on the previous list date; an "exit" is a symbol that was on the
previous list date and is not on this one. Both need the previous list date to be at most five calendar days earlier (a gap in collection is not an entry).
Cells: Nasdaq market category (Q global select, G global market, S capital market) x entry or exit. Return: the stock's log return minus SPY over five
sessions from the close of the first session after the list date (the file is created about 23:00 ET on that date). Symbols without daily_bars are dropped.
Only Nasdaq's list is stored; NYSE-listed names are not covered. Bars and t-statistic: shared/utils/event_study.py. Reads reg_sho_threshold and daily_bars;
writes nothing to the database.

Run in its own capped container (needs the database; a few minutes for two years of lists):
    docker run --rm --memory 3g --network sentinel-network --env-file .env -v <repo>/shared:/app/shared:ro -v <repo>/scripts:/app/scripts:ro \
        -w /app -e PYTHONPATH=/app sentinel-agents-heavy python scripts/study_reg_sho.py [--out reports/study_reg_sho.json]
Needs reg_sho_threshold populated (collector first pass reads back 730 days) and daily_bars. A cell below 100 events fails the registered bar by definition.
Self-test of the pure helpers: python scripts/study_reg_sho.py --selftest
"""
import argparse
import asyncio
import json
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from shared.utils.event_study import EventRet, Series, evaluate, forward_excess  # noqa: E402

MAX_GAP_DAYS = 5
CATEGORIES = ("Q", "G", "S")

Lists = Dict[date, Dict[str, str]]    # list date -> {symbol: market category}


def changes(lists: Lists) -> List[Tuple[str, str, date, str]]:
    """(kind, symbol, list date, category) for every entry and exit between consecutive list dates that are at most MAX_GAP_DAYS apart."""
    days = sorted(lists)
    out: List[Tuple[str, str, date, str]] = []
    for prev, cur in zip(days, days[1:]):
        if (cur - prev).days > MAX_GAP_DAYS:
            continue
        for sym, cat in lists[cur].items():
            if sym not in lists[prev]:
                out.append(("entry", sym, cur, cat))
        for sym, cat in lists[prev].items():
            if sym not in lists[cur]:
                out.append(("exit", sym, cur, cat))
    return out


def build_events(lists: Lists, prices: Dict[str, Series], bench: Dict[date, float]) -> List[EventRet]:
    out: List[EventRet] = []
    for kind, sym, d, cat in changes(lists):
        if sym not in prices or cat not in CATEGORIES:
            continue
        ret = forward_excess(prices[sym], bench, d)
        if ret is not None:
            out.append((f"{cat}:{kind}", d, ret))
    return out


def build_events_with_symbols(lists: Lists, prices: Dict[str, Series], bench: Dict[date, float]) -> List[Tuple[str, date, float, str]]:
    """Like build_events, with the symbol kept so a tradability gate can be applied per event (amendment DS-2)."""
    out: List[Tuple[str, date, float, str]] = []
    for kind, sym, d, cat in changes(lists):
        if sym not in prices or cat not in CATEGORIES:
            continue
        ret = forward_excess(prices[sym], bench, d)
        if ret is not None:
            out.append((f"{cat}:{kind}", d, ret, sym))
    return out


def gate_share(events: List[Tuple[str, date, float, str]], passes) -> Dict[str, Dict[str, int]]:
    """Per cell: events, and how many pass `passes(symbol, date)`."""
    out: Dict[str, Dict[str, int]] = {}
    for cell, d, _ret, sym in events:
        row = out.setdefault(cell, {"events": 0, "tradable": 0})
        row["events"] += 1
        row["tradable"] += 1 if passes(sym, d) else 0
    return out


async def run_tradable(out_path: Optional[str]) -> None:
    """Amendment DS-2, first half: how many list events could the book have traded (average dollar volume of at least $140.8M over the 30 sessions before the list
    date, and shortable and easy to borrow today), and the registered statistics on only those. Not run: the matched control (non-listed names have no Nasdaq category here)."""
    import bisect
    from shared.db import get_redis, get_timescale
    from shared.utils.asset_universe import load_asset_meta
    from shared.utils.tradable_cells import instrument_ok
    from study_common import bench_map, load_series
    db = await get_timescale()
    redis = await get_redis()
    rows = await db.query("SELECT list_date, symbol, market_category FROM reg_sho_threshold ORDER BY list_date")
    lists: Lists = {}
    for r in rows or []:
        lists.setdefault(r["list_date"], {})[r["symbol"]] = (r["market_category"] or "").upper()
    symbols = {s for v in lists.values() for s in v}
    since = (min(lists) - timedelta(days=60)) if lists else date(2024, 1, 1)
    prices = await load_series(db, symbols, since)
    dv: Dict[str, Tuple[List[date], List[float]]] = {}
    for r in await db.query("SELECT ticker, day, close * volume AS dv FROM daily_bars WHERE ticker = ANY($1::text[]) AND day >= $2 AND volume > 0 ORDER BY ticker, day",
                            sorted(symbols), since) or []:
        d, v = dv.setdefault(str(r["ticker"]), ([], []))
        d.append(r["day"] if hasattr(r["day"], "toordinal") else date.fromisoformat(str(r["day"])[:10]))
        v.append(float(r["dv"]))
    meta = await load_asset_meta(redis.raw)

    def passes(sym: str, d: date) -> bool:
        pair = dv.get(sym)
        if not pair:
            return False
        k = bisect.bisect_left(pair[0], d)
        window = pair[1][max(0, k - 30):k]
        adv = sum(window) / len(window) if window else None
        return instrument_ok(adv, -1, meta.get(sym))[0]

    events = build_events_with_symbols(lists, prices, bench_map(prices))
    share = gate_share(events, passes)
    print("tradable share per cell (events, passing G4's liquidity and borrow gate):")
    for cell in sorted(share):
        row = share[cell]
        print(f"  {cell}: {row['tradable']} of {row['events']}")
    kept = [(c, d, r) for c, d, r, s in events if passes(s, d)]
    result = evaluate(kept)
    print(f"events passing the gate: {len(kept)} of {len(events)}")
    for r in result:
        print(json.dumps(r))
    if out_path:
        Path(out_path).write_text(json.dumps({"share": share, "result": result}, indent=1), encoding="utf-8")


async def run(out_path: Optional[str]) -> None:
    from shared.db import get_timescale
    from study_common import bench_map, load_series
    db = await get_timescale()
    rows = await db.query("SELECT list_date, symbol, market_category FROM reg_sho_threshold ORDER BY list_date")
    lists: Lists = {}
    for r in rows or []:
        lists.setdefault(r["list_date"], {})[r["symbol"]] = (r["market_category"] or "").upper()
    symbols = {s for v in lists.values() for s in v}
    since = (min(lists) - timedelta(days=7)) if lists else date(2024, 1, 1)
    prices = await load_series(db, symbols, since)
    events = build_events(lists, prices, bench_map(prices))
    result = evaluate(events)
    print(f"list dates: {len(lists)}  symbols: {len(symbols)}  events: {len(events)}")
    for r in result:
        print(json.dumps(r))
    if out_path:
        Path(out_path).write_text(json.dumps(result, indent=1), encoding="utf-8")


def selftest() -> None:
    d0 = date(2024, 3, 4)
    lists = {d0: {"AAA": "S"}, d0 + timedelta(days=1): {"AAA": "S", "BBB": "G"}, d0 + timedelta(days=2): {"BBB": "G"},
             d0 + timedelta(days=30): {"CCC": "Q"}}            # the 28-day gap produces nothing
    ch = changes(lists)
    assert sorted(ch) == sorted([("entry", "BBB", d0 + timedelta(days=1), "G"), ("exit", "AAA", d0 + timedelta(days=2), "S")]), ch
    days = [d for d in (d0 - timedelta(days=5) + timedelta(days=i) for i in range(60)) if d.weekday() < 5]
    px = {s: (days, [10.0] * len(days)) for s in ("AAA", "BBB", "SPY")}
    ev = build_events(lists, px, dict(zip(*px["SPY"])))
    assert sorted(c for c, _, _ in ev) == ["G:entry", "S:exit"] and all(abs(r) < 1e-12 for _, _, r in ev), ev
    print("study_reg_sho selftest ok:", len(ev), "events")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--out", default=None)
    ap.add_argument("--tradable", action="store_true", help="amendment DS-2 (first half): the share of events the book could have traded, and statistics on those only")
    a = ap.parse_args()
    if a.selftest:
        selftest()
    elif a.tradable:
        asyncio.run(run_tradable(a.out))
    else:
        asyncio.run(run(a.out))

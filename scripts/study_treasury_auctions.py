"""Event study: Treasury note and bond auction demand -> next-5-session excess return of a Treasury ETF (Upgrade pre-registrations - 12 Nov, "Datasets with no live collector").

Signal (defined here before any run): for each (security type, term) of notes and bonds, the bid-to-cover ratio and the indirect bidder share, each scored
against the previous 12 auctions of the same type and term (at least 8; current excluded) as a z. "weak" at z <= -1, "strong" at z >= +1. Cells: ETF bucket
(short: 2-, 3-, 5-year -> IEI; mid: 7-, 10-year -> IEF; long: 20-, 30-year -> TLT) x metric (btc, indirect) x side. Bills, TIPS and FRNs are left out. Return:
ETF minus SPY, log, five sessions from the close of the first session after the auction date. The tail against the when-issued yield is not in the free
feed, so it is not tested. Bars and t-statistic: shared/utils/event_study.py. Reads treasury_auctions and daily_bars; writes nothing to the database.

Run in its own capped container (needs the database; about 20 s):
    docker run --rm --memory 2g --network sentinel-network --env-file .env -v <repo>/shared:/app/shared:ro -v <repo>/scripts:/app/scripts:ro \
        -w /app -e PYTHONPATH=/app sentinel-agents-heavy python scripts/study_treasury_auctions.py [--out reports/study_treasury_auctions.json]
Needs treasury_auctions populated (collector first pass, 2016 onwards) and daily_bars.
Self-test of the pure helpers: python scripts/study_treasury_auctions.py --selftest
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

from shared.utils.event_study import EventRet, Series, evaluate, forward_excess, trailing_z  # noqa: E402

WINDOW, MIN_HISTORY = 12, 8
Z_CUT = 1.0
BUCKETS: Dict[str, Tuple[str, str]] = {   # security term -> (bucket, ETF)
    "2-Year": ("short", "IEI"), "3-Year": ("short", "IEI"), "5-Year": ("short", "IEI"),
    "7-Year": ("mid", "IEF"), "10-Year": ("mid", "IEF"),
    "20-Year": ("long", "TLT"), "30-Year": ("long", "TLT"),
}
ETFS = sorted({e for _, e in BUCKETS.values()})

Auction = Tuple[date, str, str, Optional[float], Optional[float]]   # auction_date, type, term, bid_to_cover, indirect_share


def build_events(auctions: List[Auction], prices: Dict[str, Series], bench: Dict[date, float]) -> List[EventRet]:
    groups: Dict[Tuple[str, str], List[Auction]] = {}
    for a in sorted(auctions, key=lambda x: x[0]):
        if a[1] in ("Note", "Bond") and a[2] in BUCKETS:
            groups.setdefault((a[1], a[2]), []).append(a)
    out: List[EventRet] = []
    for (_, term), rows in groups.items():
        bucket, etf = BUCKETS[term]
        if etf not in prices:
            continue
        for metric, idx in (("btc", 3), ("indirect", 4)):
            series = [(r[0], r[idx]) for r in rows if r[idx] is not None]
            zs = trailing_z([v for _, v in series], WINDOW, MIN_HISTORY)
            for (d, _), z in zip(series, zs):
                if z is None or abs(z) < Z_CUT:
                    continue
                ret = forward_excess(prices[etf], bench, d)
                if ret is not None:
                    out.append((f"{bucket}:{metric}:{'strong' if z > 0 else 'weak'}", d, ret))
    return out


async def run(out_path: Optional[str]) -> None:
    from shared.db import get_timescale
    from study_common import bench_map, load_series
    db = await get_timescale()
    rows = await db.query("SELECT auction_date, security_type, security_term, bid_to_cover, indirect_share FROM treasury_auctions "
                          "WHERE security_type IN ('Note','Bond') ORDER BY auction_date")
    auctions = [(r["auction_date"], r["security_type"], r["security_term"], r["bid_to_cover"], r["indirect_share"]) for r in rows or []]
    prices = await load_series(db, ETFS, date(2016, 1, 1))
    events = build_events(auctions, prices, bench_map(prices))
    result = evaluate(events)
    print(f"auctions: {len(auctions)}  events: {len(events)}")
    for r in result:
        print(json.dumps(r))
    if out_path:
        Path(out_path).write_text(json.dumps(result, indent=1), encoding="utf-8")


def selftest() -> None:
    days = [d for d in (date(2024, 1, 1) + timedelta(days=i) for i in range(400)) if d.weekday() < 5]
    spike = date(2024, 9, 10)
    assert spike.weekday() < 5
    auctions = [(date(2024, 1, 3) + timedelta(weeks=i), "Note", "10-Year", 2.5 + 0.02 * (i % 3), 0.65) for i in range(30)]
    auctions.append((spike, "Note", "10-Year", 1.5, 0.65))          # a weak auction
    etf = (days, [100.0 if d <= spike + timedelta(days=1) else 95.0 for d in days])
    spy = (days, [100.0] * len(days))
    ev = build_events(auctions, {"IEF": etf, "SPY": spy}, dict(zip(*spy)))
    weak = [e for e in ev if e[1] == spike]
    assert len(weak) == 1 and weak[0][0] == "mid:btc:weak" and weak[0][2] < -0.04, ev
    assert not [e for e in ev if ":indirect:" in e[0]], "constant indirect share has no z"
    print("study_treasury_auctions selftest ok:", len(ev), "events")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    if a.selftest:
        selftest()
    else:
        asyncio.run(run(a.out))

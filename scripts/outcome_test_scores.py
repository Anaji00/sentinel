"""Registered outcome test (reports/Upgrade pre-registrations - 12 Nov.md, "Revised score bars"), 13 Nov 2026.

For finra_short_volume (two forms: 0.55 + 0.40 x ..., and 0.70 + 0.25 x ...) and alpaca_extended_hours: Spearman of the score with the stock's
5-session absolute move (new >= old, at least 300 events with an outcome), plus the cap, top-1% and no-newly-above-cut bars, the share of events below
0.6 and 0.7, and, informationally, the same Spearman against the producer grader's 5-session move ratio. Read-only; uses the replays' own helpers.
Run inside an agents container:  python outcome_test_scores.py
"""
import asyncio
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.db import get_timescale  # noqa: E402
from shared.utils import producer_grades as pg  # noqa: E402
from shared.utils.score_resolution import (  # noqa: E402
    extended_hours_score, five_session_abs_move, outcome_verdicts, share_below, short_volume_score, short_volume_score_b,
    short_volume_z, spearman,
)

HISTORY_DAYS = 20
MIN_TOTAL_VOLUME = 500_000
_SV_EVENTS = """
    SELECT upper(primary_entity_id) AS ticker, (occurred_at AT TIME ZONE 'UTC')::date AS day, occurred_at AS at, anomaly_score AS score
    FROM events WHERE source = 'finra_short_volume' AND anomaly_score IS NOT NULL
"""
_EXT_EVENTS = """
    SELECT upper(financial_data->>'ticker') AS ticker, (occurred_at AT TIME ZONE 'UTC')::date AS day, occurred_at AS at, anomaly_score AS score,
           (financial_data->>'notional_usd')::float AS notional
    FROM events WHERE source = 'alpaca_extended_hours' AND anomaly_score IS NOT NULL AND financial_data ? 'ticker'
"""
_FINRA = """
    SELECT ticker, day, short_volume / NULLIF(total_volume, 0) AS ratio, total_volume FROM finra_short_volume
    WHERE ticker = ANY($1::text[]) AND day >= $2::date AND day <= $3::date ORDER BY ticker, day
"""
_BARS = """
    SELECT ticker, day, open, close FROM daily_bars WHERE ticker = ANY($1::text[]) AND day >= $2::date AND day <= $3::date ORDER BY ticker, day
"""


def ts(x):
    return datetime.fromisoformat(x.replace("Z", "+00:00")) if isinstance(x, str) else x


def report(name, old, new, moves, ratios):
    print(f"\n--- {name}")
    print(f"events {len(old)}; with a 5-session move {sum(1 for m in moves if m is not None)}; with a grader move ratio "
          f"{sum(1 for r in ratios if r is not None)}")
    print(f"share below 0.6: old {share_below(old, 0.6):.1%} -> new {share_below(new, 0.6):.1%}; "
          f"below 0.7: old {share_below(old, 0.7):.1%} -> new {share_below(new, 0.7):.1%}")
    for bar, value, ok in outcome_verdicts(old, new, moves):
        print(f"  {'PASS' if ok else 'FAIL'}  {bar}: {value}")
    pairs = [(o, n, r) for o, n, r in zip(old, new, ratios) if r is not None]
    ro = spearman([p[0] for p in pairs], [p[2] for p in pairs])
    rn = spearman([p[1] for p in pairs], [p[2] for p in pairs])
    print(f"  (information) Spearman vs grader 5-session move ratio on {len(pairs)} events: old {ro} new {rn}")


async def main() -> None:
    db = await get_timescale()
    sv = await db.query(_SV_EVENTS) or []
    ext = await db.query(_EXT_EVENTS) or []
    tickers = sorted({r["ticker"] for r in sv + ext} | {"SPY"})
    lo = min(r["day"] for r in sv + ext) - timedelta(days=120)
    hi = max(r["day"] for r in sv + ext) + timedelta(days=14)
    bars = defaultdict(lambda: ([], [], []))
    for b in await db.query(_BARS, tickers, lo, hi) or []:
        d, o, c = bars[b["ticker"]]
        d.append(b["day"])
        o.append(float(b["open"] or b["close"]))
        c.append(float(b["close"]))
    sd, so, sc = bars.get("SPY", ([], [], []))
    spy = {d: (o, c) for d, o, c in zip(sd, so, sc)}

    def outcome(r):
        d, o, c = bars.get(r["ticker"], ([], [], []))
        move = five_session_abs_move(d, c, r["day"]) if d else None
        m = pg.move_ratio(d, o, c, spy, ts(r["at"]), 5) if d else None
        return move, (m[0] if m else None)

    series = defaultdict(list)
    for r in await db.query(_FINRA, sorted({r["ticker"] for r in sv}), min(r["day"] for r in sv) - timedelta(days=60), hi) or []:
        if r["ratio"] is not None:
            series[r["ticker"]].append((r["day"], float(r["ratio"]), float(r["total_volume"])))
    old, a, b, moves, ratios = [], [], [], [], []
    for r in sv:
        rows = series.get(r["ticker"], [])
        today = next((x for x in rows if x[0] == r["day"]), None)
        if today is None:
            continue
        z = short_volume_z(today[1], [x[1] for x in rows if x[0] < r["day"] and x[2] >= MIN_TOTAL_VOLUME][-HISTORY_DAYS:])
        if z is None:
            continue
        o = float(r["score"])
        old.append(o)
        a.append(short_volume_score(o, z))
        b.append(short_volume_score_b(o, z))
        m, rt = outcome(r)
        moves.append(m)
        ratios.append(rt)
    report("finra_short_volume, form A: min(old, 0.55 + 0.40 x (1 - exp(-(z-2)/3)))", old, a, moves, ratios)
    report("finra_short_volume, form B: min(old, 0.70 + 0.25 x (1 - exp(-(z-2)/3)))", old, b, moves, ratios)

    old, new, moves, ratios = [], [], [], []
    for r in ext:
        o = float(r["score"])
        old.append(o)
        new.append(extended_hours_score(o, r["notional"]))
        m, rt = outcome(r)
        moves.append(m)
        ratios.append(rt)
    report("alpaca_extended_hours: min(old, 0.5 + 0.45 x min(1, log10(notional/250k)/2))", old, new, moves, ratios)


if __name__ == "__main__":
    asyncio.run(main())

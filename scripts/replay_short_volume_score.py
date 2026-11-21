"""Replay of the registered finra_short_volume score proposal on the stored events (reports/Upgrade pre-registrations - 12 Nov.md).

z is recomputed for every stored event from `finra_short_volume` with the live rule (services/collector-filings/catalysts.py
short_spike_event): the name's ratios on the up-to-20 prior sessions in which it traded at least 500,000 shares, population sd.
The proposal min(old, 0.55 + 0.40 * (1 - exp(-(z - 2) / 3))) is then judged against the four registered bars.
Read-only. Run inside an agents container:  python replay_short_volume_score.py
"""
import asyncio
import sys
from collections import defaultdict
from datetime import timedelta
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.db import get_timescale  # noqa: E402
from shared.utils.score_resolution import (  # noqa: E402
    ALERT_CUTS, counts_above_cut, distribution, five_session_abs_move, short_volume_score, short_volume_verdicts, short_volume_z,
)

HISTORY_DAYS = 20
MIN_TOTAL_VOLUME = 500_000
_EVENTS_SQL = """
    SELECT event_id::text AS id, upper(primary_entity_id) AS ticker, (occurred_at AT TIME ZONE 'UTC')::date AS day,
           anomaly_score AS score, tags
    FROM events WHERE source = 'finra_short_volume' AND anomaly_score IS NOT NULL
"""
_FINRA_SQL = """
    SELECT ticker, day, short_volume / NULLIF(total_volume, 0) AS ratio, total_volume
    FROM finra_short_volume WHERE ticker = ANY($1::text[]) AND day >= $2::date AND day <= $3::date ORDER BY ticker, day
"""
_BARS_SQL = """
    SELECT ticker, day, close FROM daily_bars WHERE ticker = ANY($1::text[]) AND day >= $2::date AND day <= $3::date ORDER BY ticker, day
"""


def stored_z(tags) -> Optional[float]:
    for t in tags or []:
        if str(t).startswith("short_vol_z:"):
            try:
                return float(str(t).split(":", 1)[1])
            except ValueError:
                return None
    return None


async def main() -> None:
    db = await get_timescale()
    events = await db.query(_EVENTS_SQL) or []
    print(f"stored finra_short_volume events: {len(events)}")
    tickers = sorted({e["ticker"] for e in events})
    lo = min(e["day"] for e in events) - timedelta(days=60)
    hi = max(e["day"] for e in events) + timedelta(days=14)
    series = defaultdict(list)
    for r in await db.query(_FINRA_SQL, tickers, lo, hi) or []:
        if r["ratio"] is not None:
            series[r["ticker"]].append((r["day"], float(r["ratio"]), float(r["total_volume"])))
    bars = defaultdict(lambda: ([], []))
    for r in await db.query(_BARS_SQL, tickers, lo, hi) or []:
        bars[r["ticker"]][0].append(r["day"])
        bars[r["ticker"]][1].append(float(r["close"]))

    old, new, moves, agree = [], [], [], []
    skipped = defaultdict(int)
    for e in events:
        rows = series.get(e["ticker"], [])
        today = next((x for x in rows if x[0] == e["day"]), None)
        if today is None:
            skipped["no finra row on the event day"] += 1
            continue
        hist = [x[1] for x in rows if x[0] < e["day"] and x[2] >= MIN_TOTAL_VOLUME][-HISTORY_DAYS:]
        z = short_volume_z(today[1], hist)
        if z is None:
            skipped["history shorter than 2 sessions"] += 1
            continue
        sz = stored_z(e["tags"])
        if sz is not None:
            agree.append(abs(sz - z))
        o = float(e["score"])
        old.append(o)
        new.append(short_volume_score(o, z))
        d, c = bars.get(e["ticker"], ([], []))
        moves.append(five_session_abs_move(d, c, e["day"]) if d else None)

    print(f"replayed: {len(old)}; skipped: {dict(skipped)}")
    if agree:
        print(f"events with a stored short_vol_z tag: {len(agree)}; mean |stored - replayed| z = {sum(agree) / len(agree):.3f}, "
              f"max {max(agree):.3f}")
    print(f"events with a 5-session move: {sum(1 for m in moves if m is not None)}")
    print("old score distribution:", distribution(old, (0.7, 0.8, 0.9, 0.95)))
    print("new score distribution:", distribution(new, (0.7, 0.8, 0.9, 0.95)))
    print("counts at or above each cut (old, new):", counts_above_cut(old, new, ALERT_CUTS))
    print("\nRegistered bars:")
    for bar, value, ok in short_volume_verdicts(old, new, moves):
        print(f"  {'PASS' if ok else 'FAIL'}  {bar}: {value}")


if __name__ == "__main__":
    asyncio.run(main())

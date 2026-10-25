"""
shared/utils/bar_quality.py

A daily check that the minute bars add up to the day. Pure; the tradfi
collector runs the query and stores the report, the gateway serves it.

Minute bars on IEX summed to 2-6% of each stock's consolidated daily volume
for weeks (NVDA 3.4M of 119.4M on 14 Oct 2026) and nothing noticed, because
nothing compared the two. Consolidated minute bars sum to ~95% of the daily
figure -- the opening and closing auctions and some odd-lot prints are not in
minute bars -- so a coverage under 85% is a feed problem, not the market.
"""
from __future__ import annotations

from statistics import median
from typing import Any, Dict, List, Mapping, Optional, Sequence

COVERAGE_FLOOR = 0.85          # minute volume / daily volume below this is flagged
BAR_QUALITY_KEY = "sentinel:quality:bars"   # the latest report, written by collector-tradfi
SESSION_MINUTES = 390
WORST_SHOWN = 15

RECONCILE_SQL = """
    WITH d AS (SELECT max(day) AS day FROM daily_bars WHERE day < CURRENT_DATE),
    m AS (
        SELECT b.ticker, sum(b.volume) AS minute_volume, count(*) AS minutes
        FROM tradfi_bars b, d
        WHERE b.ticker = ANY($1::text[])
          AND b.time >= (d.day + TIME '09:30') AT TIME ZONE 'America/New_York'
          AND b.time <= (d.day + TIME '16:00') AT TIME ZONE 'America/New_York'   -- the 16:00 bar is the closing auction
        GROUP BY b.ticker
    )
    SELECT db.ticker, d.day, db.volume AS daily_volume, m.minute_volume, m.minutes
    FROM daily_bars db JOIN d ON db.day = d.day
    LEFT JOIN m ON m.ticker = db.ticker
    WHERE db.ticker = ANY($1::text[]) AND db.volume > 0
"""


def reconcile(rows: Sequence[Mapping[str, Any]], floor: float = COVERAGE_FLOOR) -> Dict[str, Any]:
    """Coverage of each stock's day by its minute bars, and the summary.

    Rows: ticker, day, daily_volume, minute_volume (None when the stock had no
    minute bars at all), minutes."""
    per: List[Dict[str, Any]] = []
    for r in rows:
        dv = float(r.get("daily_volume") or 0)
        if dv <= 0:
            continue
        mv = float(r.get("minute_volume") or 0)
        per.append({"ticker": r["ticker"], "coverage": round(mv / dv, 4),
                    "minutes": int(r.get("minutes") or 0)})
    if not per:
        return {"day": None, "stocks": 0, "median_coverage": None, "below_floor": 0, "missing": 0,
                "floor": floor, "worst": [], "ok": None}
    cov = [p["coverage"] for p in per]
    below = [p for p in per if p["coverage"] < floor]
    missing = [p for p in per if p["minutes"] == 0]
    med: Optional[float] = round(median(cov), 4)
    return {
        "day": str(rows[0].get("day")) if rows else None,
        "stocks": len(per),
        "median_coverage": med,
        "median_minutes": int(median(p["minutes"] for p in per)),
        "below_floor": len(below),
        "missing": len(missing),
        "floor": floor,
        "worst": sorted(below, key=lambda p: p["coverage"])[:WORST_SHOWN],
        # Healthy when the typical stock's minutes add up to its day and few
        # fall short; one thin name is the market, a thin median is the feed.
        "ok": med >= floor and len(below) <= max(5, len(per) // 20),
    }


__all__ = ["BAR_QUALITY_KEY", "COVERAGE_FLOOR", "RECONCILE_SQL", "reconcile"]

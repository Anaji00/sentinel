"""Read-only report (layer audit 18 Nov, findings 12 and 14): contradictory agent calls over the last 30 days, and flat or zero-volume bars among the names the model
scored on its latest date and the names in the top and bottom 5% of its monthly ranking (the book's candidates). Writes nothing.
Run: docker exec -i -w /app sentinel-api_gateway python - < scripts/report_call_quality.py
"""
import asyncio
import sys
from datetime import datetime

sys.path.insert(0, "/app")
from shared.db import get_timescale  # noqa: E402
from shared.utils.call_contradictions import flag_contradictions, summarize  # noqa: E402
from shared.utils.flat_bars import flat_report  # noqa: E402

_CALLS = """SELECT id, agent_name, upper(ticker) AS ticker, occurred_at, direction FROM agent_predictions
            WHERE occurred_at > NOW() - INTERVAL '30 days' AND ticker IS NOT NULL AND lower(direction) IN ('up','down','long','short','bullish','bearish')"""
_SCORES = "SELECT ticker, percentile FROM factor_scores_daily WHERE as_of = (SELECT max(as_of) FROM factor_scores_daily WHERE horizon = 'monthly') AND horizon = 'monthly'"
_BARS = "SELECT ticker, close, volume FROM daily_bars WHERE ticker = ANY($1::text[]) AND day > CURRENT_DATE - 30 ORDER BY ticker, day"


def _day(x):
    return (datetime.fromisoformat(x.replace("Z", "+00:00")) if isinstance(x, str) else x).date()


async def main() -> None:
    db = await get_timescale()
    calls = [{"id": r["id"], "agent_name": r["agent_name"], "ticker": r["ticker"], "day": _day(r["occurred_at"]), "direction": r["direction"]}
             for r in await db.query(_CALLS) or []]
    print("contradictions:", summarize(flag_contradictions(calls), len(calls)))
    scores = {r["ticker"]: float(r["percentile"]) for r in await db.query(_SCORES) or [] if r["percentile"] is not None}
    top = max(scores.values(), default=100.0)
    lo, hi = (0.05, 0.95) if top <= 1.0 else (5.0, 95.0)      # percentile on 0-1 or 0-100
    book = [t for t, p in scores.items() if p <= lo or p >= hi]
    series = {}
    for label, names in (("scored", sorted(scores)), ("book candidates (top and bottom 5%)", sorted(book))):
        need = [n for n in names if n not in series]
        for i in range(0, len(need), 400):
            for b in await db.query(_BARS, need[i:i + 400]) or []:
                series.setdefault(b["ticker"], []).append((float(b["close"]), float(b["volume"] or 0)))
        print(label, flat_report(series, names))


asyncio.run(main())

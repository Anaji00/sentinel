"""Records the trader's implementation shortfall for one day (quant report T9): each filled order against the prior close, the 09:40 and 10:30 New York prices and the
day's VWAP, from `auto_trade_orders` and one-minute `tradfi_bars`. Read-only on the database; prints one JSON document (summary and per-order rows) to stdout. Run it after
the 30 Nov rebuild, and on any other day to compare the drift trades:
    docker exec -i -w /app sentinel-api_gateway python - 2026-11-30 < scripts/record_shortfall.py > reports/shortfall_2026-11-30.json
No order, timing or limit is touched.
"""
import asyncio
import json
import sys
from datetime import date, datetime, time, timedelta, timezone

sys.path.insert(0, "/app")
from shared.db import get_timescale  # noqa: E402
from shared.utils.shortfall import rows_for, summarize  # noqa: E402

_FILLS = "SELECT symbol, side, reason, filled_qty, fill_price FROM auto_trade_orders WHERE day = $1 AND coalesce(filled_qty, 0) > 0 AND fill_price > 0"
_BARS = ("SELECT ticker, time, close, volume FROM tradfi_bars WHERE ticker = ANY($1::text[]) AND time >= $2::timestamptz AND time < $3::timestamptz ORDER BY ticker, time")
_PRIOR = "SELECT DISTINCT ON (ticker) ticker, close FROM daily_bars WHERE ticker = ANY($1::text[]) AND day < $2::date AND close > 0 ORDER BY ticker, day DESC"


async def main(day: date) -> None:
    db = await get_timescale()
    fills = [dict(r) for r in await db.query(_FILLS, day) or []]
    symbols = sorted({f["symbol"] for f in fills})
    bars = {}
    start = datetime.combine(day - timedelta(days=1), time(0), timezone.utc)
    end = datetime.combine(day + timedelta(days=1), time(12), timezone.utc)
    for i in range(0, len(symbols), 100):
        for r in await db.query(_BARS, symbols[i:i + 100], start, end) or []:
            t = r["time"] if isinstance(r["time"], datetime) else datetime.fromisoformat(str(r["time"]).replace("Z", "+00:00"))
            bars.setdefault(r["ticker"], []).append((t, float(r["close"]), float(r["volume"] or 0)))
    prior = {r["ticker"]: float(r["close"]) for r in await db.query(_PRIOR, symbols, day) or []}
    rows = rows_for(fills, bars, prior, day)
    print(json.dumps({"day": str(day), "summary": summarize(rows), "rows": rows}, default=str))


asyncio.run(main(date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date.today() - timedelta(days=1)))

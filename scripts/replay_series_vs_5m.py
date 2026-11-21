"""Registered replay for proposal P1 (reports/Improvements - 18 Nov.md): does `tradfi_bars_5m` equal the signal engine's raw 5-minute `last(close, time)` series?
Read-only. Samples N tickers (default 500, seeded) over the last D days (default 30) and prints one JSON document: buckets compared, buckets equal to the cent, the
share equal, buckets missing on either side, and the worst ten differences. The bar is 99.9% equal to the cent; below it the raw query stays.
    docker exec -i -w /app sentinel-api_gateway python - 500 30 < scripts/replay_series_vs_5m.py
"""
import asyncio
import json
import random
import sys

sys.path.insert(0, "/app")
from shared.db import get_timescale  # noqa: E402

_RAW = """
    SELECT time_bucket('5 minutes', time) AS b, last(close, time) AS c FROM tradfi_bars
    WHERE ticker = $1 AND time > now() - make_interval(days => $2) GROUP BY 1
"""
_AGG = "SELECT bucket_time AS b, close AS c FROM tradfi_bars_5m WHERE ticker = $1 AND bucket_time > now() - make_interval(days => $2)"


async def main(n: int, days: int) -> None:
    db = await get_timescale()
    names = [r["ticker"] for r in await db.query("SELECT DISTINCT ticker FROM tradfi_bars WHERE time > now() - interval '3 days'") or []]
    random.Random(7).shuffle(names)
    names = sorted(names[:n])
    both = equal = raw_only = agg_only = 0
    worst = []
    for t in names:
        raw = {r["b"]: float(r["c"]) for r in await db.query(_RAW, t, days) or [] if r["c"] is not None}
        agg = {r["b"]: float(r["c"]) for r in await db.query(_AGG, t, days) or [] if r["c"] is not None}
        raw_only += len(raw.keys() - agg.keys())
        agg_only += len(agg.keys() - raw.keys())
        for b in raw.keys() & agg.keys():
            both += 1
            d = abs(raw[b] - agg[b])
            if round(d, 2) == 0:
                equal += 1
            else:
                worst.append((d, t, str(b), raw[b], agg[b]))
    worst.sort(reverse=True)
    print(json.dumps({"tickers": len(names), "days": days, "buckets_compared": both, "equal_to_the_cent": equal,
                      "share_equal": round(equal / both, 6) if both else None, "raw_only": raw_only, "aggregate_only": agg_only,
                      "worst": worst[:10]}, default=str))


asyncio.run(main(int(sys.argv[1]) if len(sys.argv) > 1 else 500, int(sys.argv[2]) if len(sys.argv) > 2 else 30))

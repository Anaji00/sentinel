"""Read-only: distribution of stored anomaly scores per event type over a window (distinct values, mode share, share >= 0.8)."""
import asyncio
import sys

from shared.db import get_timescale
from shared.utils.score_resolution import distribution

DAYS = int(sys.argv[1]) if len(sys.argv) > 1 else 14


async def main() -> None:
    db = await get_timescale()
    rows = await db.query(
        "SELECT type, anomaly_score FROM events WHERE anomaly_score IS NOT NULL AND created_at > NOW() - ($1 || ' days')::interval", str(DAYS)) or []
    by: dict = {}
    for r in rows:
        by.setdefault(r["type"], []).append(float(r["anomaly_score"]))
    print(f"window {DAYS} days, {len(rows)} events")
    for t, v in sorted(by.items(), key=lambda kv: -len(kv[1])):
        if len(v) < 200:
            continue
        d = distribution(v)
        print(f"{t:22s} n={d['n']:6d} distinct={d['distinct']:5d} mode={d['mode']:.3f}({d['mode_share']:.0%}) p50={d['p50']:.3f} p90={d['p90']:.3f} "
              f">=0.8:{d['share_ge_0.8']:.1%} >=0.9:{d['share_ge_0.9']:.1%}")


asyncio.run(main())

"""Equity block score against its dollar size. Read-only. Run in a service container."""

import asyncio

from shared.db import get_timescale


async def main() -> None:
    db = await get_timescale()
    rows = await db.query(
        "SELECT width_bucket(ln(greatest(((financial_data->>'volume')::float) * ((financial_data->>'close_price')::float), 1)), ln(1e4), ln(1e9), 5) AS b, "
        "count(*) AS n, avg(anomaly_score) AS m, count(*) FILTER (WHERE anomaly_score >= 0.8)::float / count(*) AS hi, "
        "count(DISTINCT round(anomaly_score::numeric, 2)) AS d, min(anomaly_score) AS lo, max(anomaly_score) AS hi_s "
        "FROM events WHERE type = 'equity_block' AND created_at > NOW() - INTERVAL '7 days' AND anomaly_score IS NOT NULL "
        "AND financial_data->>'volume' IS NOT NULL AND financial_data->>'close_price' IS NOT NULL GROUP BY 1 ORDER BY 1")
    print("size bucket (log $): n | mean | share>=0.8 | distinct | min-max")
    for r in rows or []:
        print(f"  {r['b']}: {r['n']:5d} {float(r['m']):.2f} {r['hi']:.2f} {r['d']:4d} {float(r['lo']):.2f}-{float(r['hi_s']):.2f}")
    rows = await db.query("SELECT financial_data->>'ticker' AS t, count(*) AS n FROM events WHERE type = 'equity_block' AND created_at > NOW() - INTERVAL '7 days' GROUP BY 1 ORDER BY 2 DESC LIMIT 6")
    print("busiest tickers:", [(r["t"], r["n"]) for r in rows or []])


asyncio.run(main())

"""Baseline for the phase 3 rate test: daily counts of high-anomaly events by type and hour of day (US/Eastern). Read-only.
Run inside a service container."""

import asyncio

from shared.db import get_timescale


async def main() -> None:
    db = await get_timescale()
    cols = await db.query("SELECT column_name FROM information_schema.columns WHERE table_name = 'events' AND column_name IN ('anomaly_score','type','created_at','occurred_at')")
    print("columns:", sorted(c["column_name"] for c in cols or []))
    rows = await db.query(
        "SELECT type, count(*) AS n, count(*) FILTER (WHERE anomaly_score >= 0.8) AS hi, "
        "count(*) FILTER (WHERE anomaly_score >= 0.8)::float / NULLIF(count(*), 0) AS share "
        "FROM events WHERE created_at > NOW() - INTERVAL '14 days' AND anomaly_score IS NOT NULL GROUP BY 1 ORDER BY 3 DESC LIMIT 12")
    print("type | events 14d | score>=0.8 | share")
    for r in rows or []:
        print(f"  {r['type']:24s} {r['n']:8d} {r['hi']:7d} {r['share'] or 0:.3f}")
    rows = await db.query(
        "SELECT CASE WHEN t < '09:30' THEN 'before the open' WHEN t < '10:00' THEN 'opening 09:30-10:00' WHEN t < '11:30' THEN 'morning 10:00-11:30' "
        "WHEN t < '14:00' THEN 'midday 11:30-14:00' WHEN t < '15:30' THEN 'afternoon 14:00-15:30' WHEN t < '16:00' THEN 'closing 15:30-16:00' ELSE 'after the close' END AS bucket, "
        "count(*) AS n, count(*) FILTER (WHERE anomaly_score >= 0.8) AS hi, avg(anomaly_score) AS mean_score FROM ("
        "SELECT (created_at AT TIME ZONE 'America/New_York')::time AS t, anomaly_score FROM events "
        "WHERE created_at > NOW() - INTERVAL '14 days' AND anomaly_score IS NOT NULL AND type = 'market_anomaly' "
        "AND extract(isodow FROM created_at AT TIME ZONE 'America/New_York') < 6) x GROUP BY 1 ORDER BY min(t)")
    print("market_anomaly on weekdays, by Eastern time bucket: events | score>=0.8 | share | mean score")
    for r in rows or []:
        print(f"  {r['bucket']:24s} {r['n']:7d} {r['hi']:5d} {r['hi'] / r['n']:.3f} {float(r['mean_score'] or 0):.3f}")
    rows = await db.query(
        "SELECT date_trunc('day', created_at)::date AS d, count(*) FILTER (WHERE anomaly_score >= 0.8) AS hi FROM events "
        "WHERE created_at > NOW() - INTERVAL '14 days' AND anomaly_score IS NOT NULL GROUP BY 1 ORDER BY 1")
    print("daily score>=0.8 events:", [(str(r["d"]), r["hi"]) for r in rows or []])


asyncio.run(main())

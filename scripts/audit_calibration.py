"""Why filings and equity blocks clear 0.8 so often: score by form type, by item, and by size. Read-only. Run in a service container."""

import asyncio

from shared.db import get_timescale


async def main() -> None:
    db = await get_timescale()
    print("FILINGS by form type, 7 days: n | distinct | share>=0.8 | p50 p90")
    rows = await db.query(
        "SELECT coalesce(filing_data->>'form_type', '?') AS form, count(*) AS n, count(DISTINCT round(anomaly_score::numeric, 2)) AS d, "
        "count(*) FILTER (WHERE anomaly_score >= 0.8)::float / count(*) AS hi, percentile_cont(0.5) WITHIN GROUP (ORDER BY anomaly_score) AS p50, "
        "percentile_cont(0.9) WITHIN GROUP (ORDER BY anomaly_score) AS p90 FROM events WHERE type = 'filing' AND created_at > NOW() - INTERVAL '7 days' "
        "AND anomaly_score IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 10")
    for r in rows or []:
        print(f"  {r['form'][:18]:18s} {r['n']:5d} {r['d']:4d} {r['hi']:.3f} {float(r['p50']):.2f} {float(r['p90']):.2f}")
    print("8-K items (first item), share>=0.8")
    rows = await db.query(
        "SELECT filing_data->'items'->>0 AS item, count(*) AS n, avg(anomaly_score) AS m FROM events WHERE type = 'filing' AND filing_data->>'form_type' = '8-K' "
        "AND created_at > NOW() - INTERVAL '7 days' GROUP BY 1 ORDER BY 2 DESC LIMIT 8")
    for r in rows or []:
        print(f"  item {str(r['item']):6s} n={r['n']:4d} mean score {float(r['m']):.2f}")

    print("\nEQUITY BLOCKS, 7 days: score vs notional")
    cols = await db.query("SELECT column_name FROM information_schema.columns WHERE table_name = 'events' AND column_name IN ('financial_data')")
    rows = await db.query(
        "SELECT width_bucket(least(greatest((financial_data->>'notional')::float, 1), 1e9), 1, 1e9, 6) AS b, count(*) AS n, avg(anomaly_score) AS m, "
        "count(*) FILTER (WHERE anomaly_score >= 0.8)::float / count(*) AS hi FROM events WHERE type = 'equity_block' AND created_at > NOW() - INTERVAL '7 days' "
        "AND anomaly_score IS NOT NULL AND financial_data->>'notional' IS NOT NULL GROUP BY 1 ORDER BY 1")
    for r in rows or []:
        print(f"  notional bucket {r['b']}: n={r['n']:5d} mean score {float(r['m']):.2f} share>=0.8 {r['hi']:.2f}")
    rows = await db.query("SELECT financial_data FROM events WHERE type = 'equity_block' AND created_at > NOW() - INTERVAL '1 day' LIMIT 2")
    for r in rows or []:
        print("  sample financial_data:", str(r["financial_data"])[:300])


asyncio.run(main())

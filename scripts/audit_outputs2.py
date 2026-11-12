"""More output richness: scenario probability triplets, agent predictions, correlation rows. Read-only. Run in a service container."""

import asyncio
import collections

from shared.db import get_timescale


async def cols(db, t):
    r = await db.query("SELECT column_name FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position", t)
    return [x["column_name"] for x in r or []]


async def main() -> None:
    db = await get_timescale()
    rows = await db.query("SELECT (SELECT string_agg((h->>'probability'), '/' ORDER BY ord) FROM jsonb_array_elements(hypotheses) WITH ORDINALITY AS t(h, ord)) AS trip FROM scenarios WHERE created_at > NOW() - INTERVAL '30 days'")
    c = collections.Counter(r["trip"] for r in rows or [])
    print("scenario probability triplets (30 days), n =", sum(c.values()), "| distinct =", len(c), "| top:", c.most_common(6))

    print("\nagent_predictions columns:", await cols(db, "agent_predictions"))
    rows = await db.query("SELECT agent_name, count(*) AS n, count(DISTINCT round(confidence::numeric, 2)) AS conf_distinct, avg(confidence) AS conf FROM agent_predictions WHERE occurred_at > NOW() - INTERVAL '14 days' GROUP BY 1 ORDER BY 2 DESC LIMIT 12")
    for r in rows or []:
        print(f"  {r['agent_name']:34s} n={r['n']:6d} distinct confidences={r['conf_distinct']:3d} mean={float(r['conf'] or 0):.2f}")

    print("\ncorrelations columns:", await cols(db, "correlations"))
    rows = await db.query("SELECT rule_name, alert_tier, count(*) AS n, count(DISTINCT round(confidence_score::numeric, 2)) AS conf_distinct, avg(confidence_score) AS conf FROM correlations WHERE created_at > NOW() - INTERVAL '7 days' GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 14")
    for r in rows or []:
        print(f"  {str(r['rule_name'])[:46]:46s} {r['alert_tier']!s:13s} n={r['n']:5d} distinct conf={r['conf_distinct']:3d} mean={float(r['conf'] or 0):.2f}")


asyncio.run(main())

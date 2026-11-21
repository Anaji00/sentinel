"""Read-only. Date-clustered re-grade of every agent's resolved calls against SPY.

The stored agent_skill / agent_calls grades count each call as an independent observation. Calls cluster on few dates (one regime week can
carry most of an agent's sample), so here each agent gets ONE observation per detection date: the mean signed excess return over SPY
(positive = the call was right; `agent_predictions.excess_spy`, migration 0094) and the date's hit share; the t is over dates. Also reported per
horizon so one long-horizon episode cannot hide in an average. Units with no excess return (not yet graded, or no bars) are skipped and counted.
Run: docker exec -i -w /app sentinel-api_gateway python - < scripts/research_agent_cluster.py
"""
import asyncio
import math
import sys
from collections import defaultdict

sys.path.insert(0, "/app")
from shared.db import get_timescale  # noqa: E402

_SQL = """
SELECT agent_name, horizon_hours AS h, date(occurred_at) AS d, direction,
       CASE direction WHEN 'up' THEN excess_spy WHEN 'down' THEN -excess_spy END AS s
FROM agent_predictions
WHERE resolved_at IS NOT NULL AND direction IN ('up', 'down') AND occurred_at > NOW() - INTERVAL '90 days'
"""


def t_over(xs):
    n = len(xs)
    if n < 3:
        return None
    m = sum(xs) / n
    sd = math.sqrt(sum((x - m) ** 2 for x in xs) / (n - 1))
    return None if sd == 0 else m / (sd / math.sqrt(n))


async def main() -> None:
    db = await get_timescale()
    rows = await db.query(_SQL) or []
    by = defaultdict(lambda: defaultdict(list))
    skipped = defaultdict(int)
    for r in rows:
        if r["s"] is None:
            skipped[r["agent_name"]] += 1
            continue
        by[(r["agent_name"], None)][r["d"]].append(float(r["s"]))
        by[(r["agent_name"], int(r["h"] or 0))][r["d"]].append(float(r["s"]))
    print("agent | horizon_h | calls | dates | mean signed bps (by date) | t over dates | share of calls on the 3 biggest dates | hit by date | skipped")
    for (agent, h), days in sorted(by.items(), key=lambda kv: (kv[0][0], kv[0][1] is not None, kv[0][1] or 0)):
        per_day = [sum(v) / len(v) for v in days.values()]
        n = sum(len(v) for v in days.values())
        if len(days) < 3:
            continue
        top3 = sum(sorted((len(v) for v in days.values()), reverse=True)[:3]) / n
        hit = sum(sum(1 for x in v if x > 0) / len(v) for v in days.values()) / len(days)
        t = t_over(per_day)
        print(f"{agent[:36]:36s} | {('all' if h is None else h):>4} | {n:5d} | {len(days):3d} | {1e4 * sum(per_day) / len(per_day):+8.1f} | "
              f"{('%.2f' % t) if t is not None else 'na':>6s} | {top3:.2f} | {hit:.3f} | {skipped.get(agent, 0) if h is None else ''}")


asyncio.run(main())

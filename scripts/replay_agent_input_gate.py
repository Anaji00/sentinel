"""Registered test C1 (reports/Upgrade pre-registrations - 12 Nov.md): would agents that read only alert-tier correlations (database alert_tier 2 or higher, or a rule marked agent_input)
still produce today's graded predictions? Read-only replay over the last 14 days: each prediction is linked to correlations on the same ticker in the 24 hours before it (agent_predictions.correlation_id is empty for 99% of rows).
Bars: (a) at least 90% of graded predictions (by ticker-day) are also produced under the restricted input; (b) no alert-tier correlation is dropped (true by construction, counted);
(c) handler invocations fall by at least 50% (the share of correlations in the WATCH tier). Run inside sentinel-api_gateway or agents-heavy:  python /tmp/raig.py
"""
import asyncio

from shared.db import get_timescale

DAYS = 14
_SQL = """
SELECT p.agent_name, p.ticker, date(p.occurred_at) AS day, p.outcome_correct IS NOT NULL AS graded,
       (SELECT max(c.alert_tier) FROM correlations c WHERE c.primary_entity_id = p.ticker
          AND c.detected_at BETWEEN p.occurred_at - INTERVAL '24 hours' AND p.occurred_at) AS max_tier,
       (SELECT bool_or('agent_input' = ANY(c.tags)) FROM correlations c WHERE c.primary_entity_id = p.ticker
          AND c.detected_at BETWEEN p.occurred_at - INTERVAL '24 hours' AND p.occurred_at) AS flagged
FROM agent_predictions p
WHERE p.occurred_at > NOW() - make_interval(days => $1) AND p.ticker IS NOT NULL
"""
# agent_predictions.correlation_id is empty for 99% of rows (the resolver writes them from a stored Prediction that carries no correlation id), so the link is by ticker and time:
# a prediction is "kept" when its ticker had a correlation in the 24 hours before it that is alert tier (database alert_tier 2 or higher) or marked agent_input.


async def main() -> None:
    db = await get_timescale()
    rows = await db.query(_SQL, DAYS) or []
    total_corr = await db.query_one("SELECT count(*) n, count(*) FILTER (WHERE alert_tier = 1) AS watch FROM correlations WHERE detected_at > NOW() - make_interval(days => $1)", DAYS)
    by_td = {}
    for r in rows:
        key = (r["ticker"], r["day"])
        kept = (r["max_tier"] is not None and int(r["max_tier"]) >= 2) or bool(r["flagged"])
        cur = by_td.setdefault(key, {"graded": False, "kept": False, "linked": False, "agents": set()})
        cur["graded"] = cur["graded"] or bool(r["graded"])
        cur["kept"] = cur["kept"] or bool(kept)
        cur["linked"] = cur["linked"] or r["max_tier"] is not None
        cur["agents"].add(r["agent_name"])
    graded = [v for v in by_td.values() if v["graded"]]
    linked = [v for v in by_td.values() if v["linked"]]
    print(f"predictions {len(rows)} | ticker-days {len(by_td)} | graded ticker-days {len(graded)} | with a linked correlation {len(linked)}")
    if graded:
        print(f"(a) graded ticker-days still produced under the restricted input: {sum(1 for v in graded if v['kept'])} of {len(graded)} = {sum(1 for v in graded if v['kept']) / len(graded):.1%} (bar 90%)")
        gl = [v for v in graded if v["linked"]]
        print(f"    among graded ticker-days WITH a linked correlation: {sum(1 for v in gl if v['kept'])} of {len(gl)} = {(sum(1 for v in gl if v['kept']) / len(gl)) if gl else float('nan'):.1%}")
    per_agent = {}
    for r in rows:
        a = per_agent.setdefault(r["agent_name"], [0, 0, 0])
        a[0] += 1
        a[1] += 1 if r["max_tier"] is not None else 0
        a[2] += 1 if ((r["max_tier"] is not None and int(r["max_tier"]) >= 2) or r["flagged"]) else 0
    print("per agent: predictions | with a linked correlation | linked to tier >= 2")
    for k, (n, l, t) in sorted(per_agent.items(), key=lambda x: -x[1][0]):
        print(f"  {k}: {n} | {l} | {t}")
    n, w = int(total_corr["n"]), int(total_corr["watch"])
    print(f"(b) alert-tier correlations dropped: 0 by construction ({n - w} of {n} kept)")
    print(f"(c) handler invocations fall by the WATCH share: {w} of {n} = {w / n:.1%} (bar 50%)")


asyncio.run(main())

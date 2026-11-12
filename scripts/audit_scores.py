"""Shape of the platform's scores: anomaly score and sentiment by event type, and the news tone against the language-model score.
Read-only. Run inside a service container: python /tmp/as.py"""

import asyncio

from shared.db import get_timescale


def fmt(x):
    return "n/a" if x is None else f"{float(x):.3f}"


async def main() -> None:
    db = await get_timescale()
    for tbl in ("events", "stock_news", "stock_news_llm"):
        cols = await db.query("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position", tbl)
        print(tbl, "->", ", ".join(f"{c['column_name']}:{c['data_type'][:6]}" for c in cols or []))

    print("\nANOMALY SCORE by event type, 7 days: n | distinct | p10 p50 p90 p99 | modal value share | share>=0.8")
    rows = await db.query(
        "SELECT type, count(*) AS n, count(DISTINCT round(anomaly_score::numeric, 3)) AS distinct_vals, "
        "percentile_cont(0.1) WITHIN GROUP (ORDER BY anomaly_score) AS p10, percentile_cont(0.5) WITHIN GROUP (ORDER BY anomaly_score) AS p50, "
        "percentile_cont(0.9) WITHIN GROUP (ORDER BY anomaly_score) AS p90, percentile_cont(0.99) WITHIN GROUP (ORDER BY anomaly_score) AS p99, "
        "count(*) FILTER (WHERE anomaly_score >= 0.8)::float / count(*) AS hi "
        "FROM events WHERE created_at > NOW() - INTERVAL '7 days' AND anomaly_score IS NOT NULL GROUP BY 1 HAVING count(*) > 100 ORDER BY 2 DESC")
    modal = {r["type"]: r for r in await db.query(
        "SELECT DISTINCT ON (type) type, round(anomaly_score::numeric, 3) AS v, count(*) AS c FROM events "
        "WHERE created_at > NOW() - INTERVAL '7 days' AND anomaly_score IS NOT NULL GROUP BY type, 2 ORDER BY type, 3 DESC") or []}
    totals = {r["type"]: r["n"] for r in rows or []}
    for r in rows or []:
        m = modal.get(r["type"])
        ms = (m["c"] / totals[r["type"]]) if m else 0
        print(f"  {r['type']:26s} {r['n']:7d} {r['distinct_vals']:6d} | {fmt(r['p10'])} {fmt(r['p50'])} {fmt(r['p90'])} {fmt(r['p99'])} | mode {m['v'] if m else '-'} x{ms:.2f} | {r['hi']:.3f}")

    print("\nSENTIMENT on events (headline, social_signal), 7 days")
    rows = await db.query(
        "SELECT type, count(*) FILTER (WHERE sentiment IS NOT NULL) AS n, count(DISTINCT round(sentiment::numeric, 2)) AS distinct_vals, "
        "avg(sentiment) AS mean, stddev(sentiment) AS sd, "
        "count(*) FILTER (WHERE abs(sentiment) < 0.05)::float / NULLIF(count(*) FILTER (WHERE sentiment IS NOT NULL), 0) AS near_zero, "
        "count(*) FILTER (WHERE abs(sentiment) >= 0.9)::float / NULLIF(count(*) FILTER (WHERE sentiment IS NOT NULL), 0) AS saturated "
        "FROM events WHERE created_at > NOW() - INTERVAL '7 days' AND type IN ('headline','social_signal') GROUP BY 1")
    for r in rows or []:
        print(f"  {r['type']:14s} n={r['n']} distinct={r['distinct_vals']} mean={fmt(r['mean'])} sd={fmt(r['sd'])} near zero={fmt(r['near_zero'])} saturated={fmt(r['saturated'])}")

    print("\nNEWS: FinBERT tone tags vs the language-model score (stock_news_llm, non-stale), 7 days")
    rows = await db.query(
        "SELECT score, count(*) AS n FROM stock_news_llm WHERE created_at > NOW() - INTERVAL '7 days' AND model NOT LIKE '%#stale' GROUP BY 1 ORDER BY 1")
    print("  llm score histogram:", [(r["score"], r["n"]) for r in rows or []])
    rows = await db.query(
        "SELECT event_type, count(*) AS n FROM stock_news_llm WHERE created_at > NOW() - INTERVAL '7 days' AND model NOT LIKE '%#stale' GROUP BY 1 ORDER BY 2 DESC LIMIT 8")
    print("  llm event types:", [(r["event_type"], r["n"]) for r in rows or []])


asyncio.run(main())

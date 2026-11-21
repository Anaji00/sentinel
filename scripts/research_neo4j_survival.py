"""Read-only: do the graph's discovered edges appear in the edge-survival
replicated hash, and are the 14 customer_links pairs missing from the graph
present in some other form? Run:
    MSYS_NO_PATHCONV=1 docker exec -i -w /app sentinel-api_gateway python - < scripts/research_neo4j_survival.py
"""
import asyncio
import json
import time

from shared.db import get_neo4j, get_redis, get_timescale


def _ids(a: str, b: str, method: str) -> list:
    return [f"{method}:{a.upper()}:{b.upper()}", f"{method}:{b.upper()}:{a.upper()}"]


async def main() -> None:
    neo = await get_neo4j()
    raw = (await get_redis()).raw
    rep = {k.decode() if isinstance(k, bytes) else k for k in await raw.hkeys("sentinel:edges:replicated")}
    coh = {k.decode() if isinstance(k, bytes) else k for k in await raw.hkeys("sentinel:edges:cohort")}
    held = {k.decode() if isinstance(k, bytes) else k for k in await raw.hkeys("sentinel:edges:held")}
    print("replicated", len(rep), "cohort", len(coh), "held", len(held))
    by_method = {}
    for k in rep:
        by_method[k.split(":")[0]] = by_method.get(k.split(":")[0], 0) + 1
    print("replicated by method", by_method)
    by_c = {}
    for k in coh:
        by_c[k.split(":")[0]] = by_c.get(k.split(":")[0], 0) + 1
    print("cohort by method", by_c)

    # sample verdict
    sample_key = next(iter(rep), None)
    if sample_key:
        blob = await raw.hget("sentinel:edges:replicated", sample_key)
        print("verdict sample", sample_key, (blob.decode() if isinstance(blob, bytes) else blob)[:200])

    for rel, method in (("STATISTICALLY_CORRELATED_WITH", "pearson"), ("GRANGER_CAUSES", "granger"), ("PEER_OF", "pearson")):
        rows = await neo.query(
            f"MATCH (a)-[r:{rel}]->(b) RETURN a.name AS a, b.name AS b, r.created_at AS c, "
            "coalesce(r.source,'') AS s, r.p_value AS p LIMIT 3000"
        )
        n = len(rows)
        in_rep = in_coh = in_held = 0
        for r in rows:
            ids = _ids(r["a"], r["b"], method) if method == "granger" else _ids(r["a"], r["b"], method)
            if any(i in rep for i in ids):
                in_rep += 1
            if any(i in coh for i in ids):
                in_coh += 1
            if any(i in held for i in ids):
                in_held += 1
        print(rel, "edges", n, "in replicated", in_rep, "in cohort", in_coh, "still held", in_held,
              "in none of cohort/replicated", sum(1 for r in rows if not any(i in rep or i in coh for i in _ids(r['a'], r['b'], method))))

    # the 14 table pairs missing from the graph
    db = await get_timescale()
    pairs = [("AAOI", "MSFT"), ("AAOI", "ORCL"), ("ADSK", "INGM"), ("ADSK", "SNX"), ("AEIS", "AMAT"), ("AEIS", "LRCX"),
             ("ALNY", "REGN"), ("AMGN", "CAH"), ("AMKR", "AAPL"), ("AMKR", "QCOM"), ("AMT", "MTNOY"), ("ANET", "META"),
             ("ANET", "MSFT"), ("AVGO", "AAPL")]
    for a, b in pairs[:14]:
        rows = await neo.query(
            "MATCH (x:Company {name:$a}) OPTIONAL MATCH (y:Company {name:$b}) "
            "OPTIONAL MATCH (x)-[r]-(y) RETURN x IS NOT NULL AS has_a, y IS NOT NULL AS has_b, collect(type(r)) AS rels",
            {"a": a, "b": b})
        print(a, b, rows[:1])
    q = await db.query("SELECT supplier, customer_ticker, filed, method FROM customer_links "
                       "WHERE (supplier,customer_ticker) IN (('AVGO','AAPL'),('ANET','META'),('AAOI','MSFT'))")
    print("table rows", [(r['supplier'], r['customer_ticker'], str(r['filed']), r['method']) for r in q])
    await neo.close()


asyncio.run(main())

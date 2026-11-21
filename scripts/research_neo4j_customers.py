"""Read-only: compare Neo4j SUPPLIER_TO edges with customer_links, and read the
edge-survival Redis hashes' sizes. Run:
    MSYS_NO_PATHCONV=1 docker exec -i -w /app sentinel-api_gateway python - < scripts/research_neo4j_customers.py
"""
import asyncio
import json

from shared.db import get_neo4j, get_redis, get_timescale


async def main() -> None:
    neo = await get_neo4j()
    db = await get_timescale()

    edges = await neo.query(
        "MATCH (a)-[r:SUPPLIER_TO]->(b) RETURN a.name AS a, b.name AS b, r.source AS s, "
        "r.revenue_pct AS pct, r.filed AS filed, r.method AS m, r.created_at AS c LIMIT 500"
    )
    n_edges = len(edges)
    from_10k = [e for e in edges if e["s"] != "domain_bridge_prior"]
    print("neo4j SUPPLIER_TO", n_edges, "non-bridge", len(from_10k),
          "with revenue_pct", sum(1 for e in from_10k if e["pct"] is not None),
          "with filed", sum(1 for e in from_10k if e["filed"]),
          "method values", sorted({str(e["m"]) for e in from_10k}))

    rows = await db.query(
        "SELECT supplier, customer_ticker, customer_name, pct, filed, method FROM customer_links")
    print("customer_links rows", len(rows))
    tick = [r for r in rows if r["customer_ticker"]]
    print("  with ticker", len(tick), "distinct pairs", len({(r["supplier"], r["customer_ticker"]) for r in tick}))
    print("  by method", {m: sum(1 for r in rows if r["method"] == m) for m in {r["method"] for r in rows}})
    print("  filed range", min(r["filed"] for r in rows) if rows else None, max(r["filed"] for r in rows) if rows else None)

    table_pairs = {(r["supplier"], r["customer_ticker"]) for r in tick}
    graph_pairs = {(e["a"], e["b"]) for e in from_10k}
    print("pairs in table not in graph", len(table_pairs - graph_pairs))
    print("pairs in graph not in table (the name gate removed or never in table)", len(graph_pairs - table_pairs))
    print("  sample graph-not-table", sorted(graph_pairs - table_pairs)[:25])
    print("  sample table-not-graph", sorted(table_pairs - graph_pairs)[:15])

    # Is each graph-not-table pair at least present under the reverse direction, or by name only?
    name_only = [r for r in rows if not r["customer_ticker"]]
    print("rows with no resolved ticker (name only)", len(name_only))
    print("  sample", [(r["supplier"], r["customer_name"]) for r in name_only[:10]])

    raw = (await get_redis()).raw
    for key in ("sentinel:edges:cohort", "sentinel:edges:held", "sentinel:edges:replicated"):
        try:
            print(key, await raw.hlen(key))
        except Exception as e:
            print(key, "ERR", type(e).__name__)
    await neo.close()


asyncio.run(main())

"""Read-only: graph tickers with a market edge (PEER_OF, GRANGER_CAUSES, STATISTICALLY_CORRELATED_WITH,
SYMPATHY_MOVER, COMPETES_WITH) that have no daily bar in the last 30 days or never had one.
    MSYS_NO_PATHCONV=1 docker exec -i -w /app sentinel-api_gateway python - < scripts/research_neo4j_delisted.py
"""
import asyncio

from shared.db import get_neo4j, get_timescale


async def main() -> None:
    neo = await get_neo4j()
    db = await get_timescale()
    rows = await db.query("SELECT ticker AS s, max(day) AS last FROM daily_bars GROUP BY ticker")
    last = {r["s"].upper(): r["last"] for r in rows}
    newest = max(last.values())
    print("symbols with bars", len(last), "newest bar", newest)
    for rel in ("PEER_OF", "GRANGER_CAUSES", "STATISTICALLY_CORRELATED_WITH", "SYMPATHY_MOVER", "COMPETES_WITH"):
        e = await neo.query(f"MATCH (a)-[r:{rel}]->(b) RETURN a.name AS a, b.name AS b")
        stale = nobars = 0
        names = set()
        for r in e:
            bad = False
            for n in (r["a"], r["b"]):
                if n is None:
                    continue
                d = last.get(n.upper())
                if d is None:
                    nobars += 1; names.add(n); bad = True; break
                if (newest - d).days > 30:
                    stale += 1; names.add(n); bad = True; break
        print(rel, "edges", len(e), "with an endpoint never in daily_bars", nobars, "or last bar >30d old", stale,
              "sample", sorted(names)[:12])
    await neo.close()

asyncio.run(main())

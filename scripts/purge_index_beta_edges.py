"""Remove STATISTICALLY_CORRELATED_WITH edges that touch a broad-market index instrument (the market factor, not a link between companies).

Counts first and prints a sample; deletes only with --apply. Matches by the node's id or name property equal to an index anchor from
shared/utils/residual_corr.py; no other edge type (Granger lead-lag, peers, exposures) is touched. Run inside sentinel-api_gateway:
python /tmp/pibe.py [--apply]
"""
import asyncio
import sys

from shared.db import get_neo4j
from shared.utils.residual_corr import MARKET_INDEX_ANCHORS

ANCHORS = sorted(MARKET_INDEX_ANCHORS)
MATCH = ("MATCH (a)-[r:STATISTICALLY_CORRELATED_WITH]-(b) "
         "WHERE any(k IN ['id', 'ticker', 'name', 'symbol'] WHERE toUpper(toString(a[k])) IN $anchors)")


async def main(apply: bool) -> None:
    g = await get_neo4j()
    rows = await g.query(MATCH + " RETURN count(DISTINCT r) AS n", {"anchors": ANCHORS})
    total = int(rows[0]["n"]) if rows else 0
    print("index-anchored STATISTICALLY_CORRELATED_WITH edges:", total)
    sample = await g.query(MATCH + " RETURN coalesce(a.id, a.ticker, a.name) AS a, coalesce(b.id, b.ticker, b.name) AS b LIMIT 8",
                           {"anchors": ANCHORS})
    print("sample:", [(r["a"], r["b"]) for r in sample])
    other = await g.query("MATCH ()-[r:STATISTICALLY_CORRELATED_WITH]-() RETURN count(DISTINCT r) AS n")
    print("all STATISTICALLY_CORRELATED_WITH edges:", other[0]["n"] if other else None)
    if apply and total:
        await g.query(MATCH + " DELETE r", {"anchors": ANCHORS})
        left = await g.query(MATCH + " RETURN count(DISTINCT r) AS n", {"anchors": ANCHORS})
        print("deleted; index-anchored edges left:", left[0]["n"] if left else None)


asyncio.run(main("--apply" in sys.argv))

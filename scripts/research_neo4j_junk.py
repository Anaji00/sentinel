"""Read-only: nodes whose name is not an instrument or a real-world name.
    MSYS_NO_PATHCONV=1 docker exec -i -w /app sentinel-api_gateway python - < scripts/research_neo4j_junk.py
"""
import asyncio
from shared.db import get_neo4j

Q = [
 ("company price-like names", "MATCH (c:Company) WHERE c.name =~ '^[$0-9][0-9.,$MKB%]*$' RETURN c.name AS n, count{ (c)--() } AS d ORDER BY d DESC LIMIT 15"),
 ("company price-like count", "MATCH (c:Company) WHERE c.name =~ '^[$0-9][0-9.,$MKB%]*$' RETURN count(*) AS n, sum(count{ (c)--() }) AS edges"),
 ("entity numeric/price", "MATCH (c:Entity) WHERE c.name =~ '^[$0-9][0-9.,$MKB%]*$' RETURN count(*) AS n"),
 ("company foreign-suffix names", "MATCH (c:Company) WHERE c.name =~ '.*\.(SZ|SS|HK|KQ|KS|BR|TO|L|T|PA|DE|AX)$' RETURN count(*) AS n, sum(count{ (c)--() }) AS edges"),
 ("company with hub-only edges (only COMPETES_WITH)", "MATCH (c:Company) WHERE NOT (c)-[:HOLDS_STAKE_IN|INSIDER_OF|OPERATES_IN|REGISTERED_IN]-() RETURN count(*) AS n"),
 ("junk-ish entity nodes sample", "MATCH (e:Entity) WHERE e.name =~ '.*[ ].*' AND size(e.name) > 25 RETURN count(*) AS n"),
 ("entity sources/event-like names", "MATCH (e:Entity) WHERE e.name IN ['market_anomaly','market_structure','crypto_transfer','finnhub_equities','watched_wallet_transfer','suspect_wallet','Headline','Network','Global'] RETURN e.name AS n, count{ (e)--() } AS d"),
 ("AI/EPS nodes any label", "MATCH (e) WHERE e.name IN ['AI','EPS','AI stocks','Global Sentiment Analysis'] RETURN labels(e)[0] AS l, e.name AS n, count{ (e)--() } AS d"),
]
async def main():
    neo = await get_neo4j()
    for name, q in Q:
        try:
            print(name, (await neo.query(q))[:15])
        except Exception as e:
            print(name, "FAILED", str(e)[:150])
    await neo.close()
asyncio.run(main())

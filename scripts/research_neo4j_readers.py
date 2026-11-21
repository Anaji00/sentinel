"""Read-only: do the graph readers' label and property assumptions match what is
stored, and how long do the heavier reader queries take at current size? Run:
    MSYS_NO_PATHCONV=1 docker exec -i -w /app sentinel-api_gateway python - < scripts/research_neo4j_readers.py
"""
import asyncio
import time

from shared.db import get_neo4j

CASES = [
    ("wargamer :Entity{id}-[*1..3]-:Entity (NVDA)", "MATCH (a:Entity {id: 'NVDA'})-[r*1..3]-(b:Entity) RETURN count(*) AS n LIMIT 1"),
    ("wargamer same on Company NVDA", "MATCH (a:Company {name: 'NVDA'})-[r]-(b) RETURN count(*) AS n"),
    ("macro :Entity{type:'instrument'}", "MATCH (e:Entity {type: 'instrument'}) RETURN count(*) AS n"),
    ("any node type='instrument'", "MATCH (e) WHERE e.type = 'instrument' RETURN count(*) AS n"),
    ("any node type='Company'", "MATCH (e) WHERE e.type = 'Company' RETURN count(*) AS n"),
    ("any node type IN ('EQUITY','instrument')", "MATCH (e) WHERE e.type IN ['EQUITY','instrument'] RETURN count(*) AS n"),
    ("stat_discovery structural pairs (:Entity)-[..]->(:Entity)", "MATCH (a:Entity)-[r:MEMBER_OF|CUSTOMER_OF|SUPPLIER_TO|COMPETES_WITH|SYMPATHY_MOVER|COMMODITY_EXPOSURE|FX_EXPOSURE_TO|OPERATES_IN|RELATED_TO]->(b:Entity) RETURN count(*) AS n"),
    ("same pairs across all labels", "MATCH (a)-[r:MEMBER_OF|CUSTOMER_OF|SUPPLIER_TO|COMPETES_WITH|SYMPATHY_MOVER|COMMODITY_EXPOSURE|FX_EXPOSURE_TO|OPERATES_IN|RELATED_TO]->(b) RETURN count(*) AS n"),
    ("centrality :Entity{id:'NVDA'} degree", "MATCH (e:Entity {id: 'NVDA'})-[r]-(n) RETURN count(r) AS degree"),
    ("centrality unlabeled name='NVDA' degree", "MATCH (e {name: 'NVDA'})-[r]-(n) RETURN count(r) AS degree"),
    ("context FLAGGED_AS edges", "MATCH ()-[r:FLAGGED_AS]->() RETURN count(r) AS n"),
    ("context FLAGGED_BY edges", "MATCH ()-[r:FLAGGED_BY]->() RETURN count(r) AS n"),
    ("quant MEMBER_OF edges", "MATCH ()-[r:MEMBER_OF]->() RETURN count(r) AS n"),
    ("quant CUSTOMER_OF|SUPPLIES|PURCHASES_FROM", "MATCH ()-[r:CUSTOMER_OF|SUPPLIES|PURCHASES_FROM]->() RETURN count(r) AS n"),
    ("timed: context_builder rel query (NVDA,AAPL)", "MATCH (v) WHERE v.name IN ['NVDA','AAPL'] OR v.mmsi IN ['NVDA','AAPL'] OR v.id IN ['NVDA','AAPL'] MATCH (v)-[r]-(n) RETURN count(*) AS n"),
    ("timed: graph.py hop1 lookup (NVDA) LIMIT 60", "MATCH (n) WHERE toLower(n.id) = toLower('NVDA') OR toLower(n.name) = toLower('NVDA') OR toLower(n.ticker) = toLower('NVDA') MATCH (n)-[r]-(c) RETURN count(*) AS n"),
    ("hop2 path count from NVDA (sum of neighbours' degree)", "MATCH (n:Company {name:'NVDA'})-[]-(m) RETURN count(m) AS d1, sum(count{ (m)--() }) AS paths2"),
    ("hop2 path count from BLACKROCK-held MU", "MATCH (n:Company {name:'MU'})-[]-(m) RETURN count(m) AS d1, sum(count{ (m)--() }) AS paths2"),
    ("hop2 path count from a vessel's region hub (Singapore Approach)", "MATCH (n:Region {name:'SINGAPORE APPROACH'})-[]-(m) RETURN count(m) AS d1, sum(count{ (m)--() }) AS paths2"),
    ("graph.py entity list: degree ranking all edges", "MATCH (n)-[r]-() WITH n, count(r) AS degree RETURN count(*) AS nodes_with_edges, max(degree) AS mx"),
]


async def main() -> None:
    neo = await get_neo4j()
    for name, cy in CASES:
        t0 = time.time()
        try:
            rows = await asyncio.wait_for(neo.query(cy), timeout=90)
            print(f"{time.time()-t0:6.1f}s  {name}: {rows[:1]}")
        except Exception as e:
            print(f"{time.time()-t0:6.1f}s  {name}: FAILED {type(e).__name__}: {str(e)[:150]}")
    await neo.close()


asyncio.run(main())

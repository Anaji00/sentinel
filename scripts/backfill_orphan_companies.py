"""Companies in the graph that nothing links to.

Measured on the running graph 2026-09-21, after the retired cyber domain was
removed:

    Company nodes            5,513
    with no edge at all        657     11.9%
      ticker-shaped            649
      carrying a sector        135

They are real listings -- TIP, EVTL, AQST, ASTL -- that entered the graph from
a headline or a peer list and never acquired a relationship. A node with no
edges contributes nothing to any graph query; it is a name in a store.

The enrichment path already knows how to fix one: `fetch_and_cache_reference_data`
writes the sector and index membership, and `fetch_and_cache_peers` writes
COMPETES_WITH. Neither has ever been run against the backlog, only against
symbols arriving live. This walks the backlog.

    python scripts/backfill_orphan_companies.py --limit 50 --dry-run
    python scripts/backfill_orphan_companies.py

Rate-limited deliberately. Finnhub answers 429 under load and the free tier is
60 calls a minute; this makes two calls per symbol, so the default pace leaves
room for the live collectors that share the quota.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger("backfill.orphans")

# Two Finnhub calls per symbol against a 60/minute free tier, shared with the
# live collectors. 1.5s between symbols is 40 calls a minute, which leaves the
# collectors half the quota.
DEFAULT_PACE_SEC = 1.5

ORPHANS = """
MATCH (n:Company)
WHERE COUNT { (n)--() } = 0
  AND coalesce(n.primary_domain, '') <> 'cyber'
  AND n.name =~ '^[A-Za-z]{1,5}$'
RETURN toUpper(n.name) AS symbol
ORDER BY symbol
LIMIT $limit
"""


async def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--limit", type=int, default=1000)
    p.add_argument("--pace", type=float, default=DEFAULT_PACE_SEC)
    p.add_argument("--dry-run", action="store_true",
                   help="list what would be fetched and change nothing")
    args = p.parse_args()

    from shared.db import get_redis, get_neo4j
    from shared.kafka import SentinelProducer
    from services.enrichment.graph_writer import GraphWriter
    from services.enrichment.ref_data import (
        fetch_and_cache_peers,
        fetch_and_cache_reference_data,
    )

    redis_client = await get_redis()
    neo4j = await get_neo4j()

    rows = await neo4j.query(ORPHANS, {"limit": args.limit})
    symbols = [r["symbol"] for r in (rows or [])]
    logger.info("%s orphan companies to enrich.", len(symbols))
    if args.dry_run:
        for s in symbols[:40]:
            logger.info("  would enrich %s", s)
        if len(symbols) > 40:
            logger.info("  ... and %s more", len(symbols) - 40)
        return 0
    if not symbols:
        return 0

    # Through the governed path, not straight into Neo4j.
    #
    # GraphWriter publishes ONTOLOGY_PROPOSALS and the supervisor commits them,
    # which is the only sanctioned way into the graph -- a backfill writing
    # directly would bypass predicate validation and label resolution, and this
    # audit has already recorded what unvalidated labels cost.
    producer = SentinelProducer()
    await producer.start()
    graph_writer = GraphWriter(producer)

    import aiohttp

    enriched = peered = failed = 0
    async with aiohttp.ClientSession() as session:
        for index, symbol in enumerate(symbols, 1):
            try:
                ref = await fetch_and_cache_reference_data(
                    redis_client, symbol, session=session, graph_writer=graph_writer
                )
                peers = await fetch_and_cache_peers(
                    redis_client, symbol, session=session, graph_writer=graph_writer
                )
                if ref:
                    enriched += 1
                if peers:
                    peered += 1
            except Exception as e:
                # Counted, not fatal: one delisted symbol must not end the run.
                failed += 1
                logger.warning("  %s failed: %s", symbol, e)
            if index % 25 == 0:
                logger.info("  %s/%s -- %s enriched, %s with peers, %s failed",
                            index, len(symbols), enriched, peered, failed)
            await asyncio.sleep(args.pace)

    # `close()` is the flush point -- it calls the batch logger's flush and
    # stops the underlying producer, which drains. There is no `producer.flush`
    # on SentinelProducer; the `flush` a few lines away in that module belongs
    # to the metrics buffer, and calling it here raised AttributeError *after*
    # eight symbols had been fetched, so the run did the work and then died
    # before delivering it.
    await producer.close()

    logger.info("Done: %s enriched, %s gained peers, %s failed, of %s. "
                "Edges appear once the supervisor commits the proposals.",
                enriched, peered, failed, len(symbols))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

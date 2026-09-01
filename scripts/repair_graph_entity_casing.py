"""
scripts/repair_graph_entity_casing.py

Repairs entity ids that were upper-cased before they reached canonicalisation.

The graph supervisor canonicalises every node id it writes, and has since
`graph_node_id` was introduced. The knowledge-graph engine, which *produces* the
proposals the supervisor consumes, went on calling `.upper()` first.

That alone would have been harmless: WALLET lower-cases, so the rule would have
undone it. What made it permanent is the label. These nodes are classified
`Entity`, which maps to UNKNOWN, and UNKNOWN returns the string exactly as given
-- the correct answer when we cannot say what kind of identifier we hold, and
the reason the mangling survived. A generic label plus a producer that
pre-mangles is the whole defect; neither half does it alone.

Measured on the live graph before this ran:

    0X... (upper-cased)   139,047 nodes
    0x... (as written)      6,366 nodes
    of the upper-cased, with a lower-case twin:  2,193

So the same wallet existed twice, and any traversal starting from one half could
not see the other's relationships.

Two populations, deliberately counted separately rather than repaired as one:

  * 2,193 have a lower-case twin already in the graph. Renaming them would
    collide, so their relationships are transferred into the twin and the
    duplicate is dropped (apoc.refactor.mergeNodes, which rewires rather than
    copying).

  * The rest have no twin and need only to be spelled correctly. A rename is far
    cheaper than a merge, and doing all 139,047 through the merge path would
    have cost hours for no benefit -- the scope is chosen by which nodes
    actually differ, not by which ones share a type.

Idempotent: both phases select on the corruption itself, so a second run finds
nothing. Batched, so it can be interrupted and resumed.

    python scripts/repair_graph_entity_casing.py --dry-run
    python scripts/repair_graph_entity_casing.py
    python scripts/repair_graph_entity_casing.py --batch 2000
"""

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("repair_graph_entity_casing")

# Hex addresses are the population this corrupts: they are the only ids whose
# canonical form is lower case *and* whose upper-cased form is still a plausible
# identifier, so nothing else silently survives the mangling.
CORRUPT_PREFIX = "0X"

COUNT_TOTAL = """
MATCH (u:Entity) WHERE u.name STARTS WITH $prefix
RETURN count(*) AS n
"""

COUNT_TWINNED = """
MATCH (u:Entity) WHERE u.name STARTS WITH $prefix
MATCH (l:Entity {name: toLower(u.name)})
RETURN count(*) AS n
"""

# Relationships move to the surviving node; the duplicate is removed. Properties
# resolve to the existing (correctly spelled) node's values on conflict.
#
# Small batches, each its own request, and neither of the two obvious
# alternatives -- both were tried against this database and both failed:
#
#   * one transaction with LIMIT 2000 exceeded Neo4j's 716 MiB transaction
#     ceiling, rolled back, and repaired nothing;
#   * apoc.periodic.iterate over all 139,047 commits per batch and so fixes the
#     memory problem, but holds a single Bolt request open for the whole run.
#     It took the database down: the driver raised ServiceUnavailable and the
#     container restarted under it.
#
# So the batching lives here rather than in the server: many short requests, one
# transaction each, with the loop free to stop between any two of them.
MERGE_BATCH = """
MATCH (u:Entity) WHERE u.name STARTS WITH $prefix
MATCH (l:Entity {name: toLower(u.name)})
WITH u, l LIMIT $batch
CALL apoc.refactor.mergeNodes([l, u], {properties: 'discard', mergeRels: true})
YIELD node
RETURN count(node) AS n
"""

# No twin: the node simply has the wrong spelling. `id` is carried along because
# both properties are written and read as the identity.
RENAME_BATCH = """
MATCH (u:Entity) WHERE u.name STARTS WITH $prefix
AND NOT EXISTS { MATCH (l:Entity {name: toLower(u.name)}) }
WITH u LIMIT $batch
SET u.name = toLower(u.name),
    u.id   = toLower(coalesce(u.id, u.name))
RETURN count(u) AS n
"""


# Merges are heavy enough that this is the ceiling regardless of --batch.
MERGE_BATCH_CAP = 50


async def _run(session, query: str, **params):
    result = await session.run(query, **params)
    record = await result.single()
    return dict(record) if record else {}


async def _drain(session, query: str, batch: int, verb: str, pause: float) -> int:
    """Repeats one batch until it stops finding work.

    The pause is not politeness. This database serves the live agent swarm while
    the repair runs, and a tight loop of write transactions is what took it down
    the first time.
    """
    done = 0
    while True:
        n = (await _run(session, query, prefix=CORRUPT_PREFIX, batch=batch)).get("n", 0)
        if not n:
            return done
        done += n
        logger.info("%s %s node(s) (%s total)", verb, n, done)
        if pause:
            await asyncio.sleep(pause)


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="report the scope and change nothing")
    parser.add_argument("--batch", type=int, default=500, help="nodes per committed batch (default 500; merges capped at 50)")
    parser.add_argument("--pause", type=float, default=0.1, help="seconds between batches, to leave the live swarm room (default 0.1)")
    args = parser.parse_args()

    from neo4j import AsyncGraphDatabase

    uri = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD")
    if not password:
        logger.error("NEO4J_PASSWORD is not set.")
        return 2

    driver = AsyncGraphDatabase.driver(uri, auth=(user, password))
    try:
        async with driver.session() as session:
            total = (await _run(session, COUNT_TOTAL, prefix=CORRUPT_PREFIX)).get("n", 0)
            twinned = (await _run(session, COUNT_TWINNED, prefix=CORRUPT_PREFIX)).get("n", 0)
            logger.info(
                "Upper-cased entity ids: %s (%s with a lower-case twin, %s to rename)",
                total, twinned, total - twinned,
            )
            if not total:
                logger.info("Nothing to repair.")
                return 0
            if args.dry_run:
                logger.info("Dry run: no changes written.")
                return 0

            # Merges rewire relationships and are far heavier per node than a
            # rename, so they get a much smaller batch.
            merged_total = await _drain(
                session, MERGE_BATCH, min(args.batch, MERGE_BATCH_CAP), "Merged", args.pause
            )
            renamed_total = await _drain(
                session, RENAME_BATCH, args.batch, "Renamed", args.pause
            )

            remaining = (await _run(session, COUNT_TOTAL, prefix=CORRUPT_PREFIX)).get("n", 0)
            logger.info(
                "Done. merged=%s renamed=%s remaining=%s", merged_total, renamed_total, remaining
            )
            return 0 if remaining == 0 else 1
    finally:
        await driver.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

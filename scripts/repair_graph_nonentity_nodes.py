"""
scripts/repair_graph_nonentity_nodes.py

Removes graph nodes that are not entities, and the relationships built on them.

Measured on the live graph:

    (:Entity {name: "GLOBAL CONTEXT"})   -- a section heading from a prompt
    (:Entity {name: "0.80"})             -- a confidence score
    10 MACRO_CORRELATED relationships joining them to real companies

An LLM was asked to name the entities in a passage and returned the passage's
own scaffolding: the heading above the text and a number printed inside it. The
graph writer took both as subjects and created nodes, and the macro engine then
drew correlation edges to them. Nothing rejected either one, because nothing
asks whether a proposed entity name could be an entity at all.

The cost is not the two junk nodes. It is that every traversal through them is
wrong: "0.80" acquires a degree, so centrality -- which the correlation tier
reads directly -- counts a decimal number as a hub, and a MACRO_CORRELATED edge
from a real company to a section heading is a correlation the platform will
report and cannot justify.

Two populations:

  * Structurally impossible names -- a bare number, a lone punctuation mark, a
    string that is only digits and separators. These cannot name a company under
    any spelling and are removed outright with their relationships.

  * Prompt scaffolding -- headings the reasoning tier emits into its own
    context ("GLOBAL CONTEXT", "EXECUTIVE SUMMARY", "KEY FINDINGS"). These are
    real English phrases, so they are matched exactly rather than by shape.

Idempotent: both phases select on the corruption, so a second run finds nothing.

    python scripts/repair_graph_nonentity_nodes.py --dry-run
    python scripts/repair_graph_nonentity_nodes.py
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
logger = logging.getLogger("repair_graph_nonentity_nodes")

# Headings the reasoning and macro tiers print into their own prompts, which the
# extractor then read back out as named entities. Matched exactly and
# case-insensitively: these are ordinary phrases, and a shape rule that caught
# them would catch real company names too.
SCAFFOLD_NAMES = [
    "GLOBAL CONTEXT",
    "MARKET CONTEXT",
    "EXECUTIVE SUMMARY",
    "KEY FINDINGS",
    "ANALYSIS",
    "SUMMARY",
    "CONTEXT",
    "BACKGROUND",
    "RECOMMENDATION",
    "RECOMMENDATIONS",
    "CONCLUSION",
    "ASSESSMENT",
    "OVERVIEW",
    "UNKNOWN",
    "N/A",
    "NONE",
    "NULL",
]

# A name that is only digits, separators and whitespace. "0.80", "1,299",
# "2024-01-01", "- -". Anchored, so a real name containing a number
# ("3M", "7-Eleven", "AS1299 Telecom") is untouched.
NUMERIC_NAME_PATTERN = r"^[\s\d.,:;%+\-/()]*$"

COUNT_NUMERIC = """
MATCH (n:Entity)
WHERE n.name IS NOT NULL AND n.name =~ $pattern
RETURN count(n) AS n
"""

LIST_NUMERIC = """
MATCH (n:Entity)
WHERE n.name IS NOT NULL AND n.name =~ $pattern
RETURN n.name AS name, size([(n)--() | 1]) AS degree
ORDER BY degree DESC
LIMIT $limit
"""

DELETE_NUMERIC = """
MATCH (n:Entity)
WHERE n.name IS NOT NULL AND n.name =~ $pattern
WITH n LIMIT $batch
DETACH DELETE n
RETURN count(*) AS n
"""

COUNT_SCAFFOLD = """
MATCH (n:Entity)
WHERE n.name IS NOT NULL AND toUpper(trim(n.name)) IN $names
RETURN count(n) AS n
"""

LIST_SCAFFOLD = """
MATCH (n:Entity)
WHERE n.name IS NOT NULL AND toUpper(trim(n.name)) IN $names
RETURN n.name AS name, size([(n)--() | 1]) AS degree
ORDER BY degree DESC
LIMIT $limit
"""

DELETE_SCAFFOLD = """
MATCH (n:Entity)
WHERE n.name IS NOT NULL AND toUpper(trim(n.name)) IN $names
WITH n LIMIT $batch
DETACH DELETE n
RETURN count(*) AS n
"""

# Edges whose endpoints were removed above are gone with them. This finds edges
# that survived because both endpoints are real but the relationship was built
# from a degenerate correlation -- a correlation of a series with itself.
COUNT_SELF_EDGES = """
MATCH (a:Entity)-[r:MACRO_CORRELATED]->(b:Entity)
WHERE a = b
RETURN count(r) AS n
"""

DELETE_SELF_EDGES = """
MATCH (a:Entity)-[r:MACRO_CORRELATED]->(b:Entity)
WHERE a = b
WITH r LIMIT $batch
DELETE r
RETURN count(*) AS n
"""


async def _run(session, query: str, **params):
    result = await session.run(query, **params)
    record = await result.single()
    return dict(record) if record else {}


async def _fetch(session, query: str, **params):
    result = await session.run(query, **params)
    return [dict(record) async for record in result]


async def _drain(session, query: str, verb: str, batch: int, pause: float, **params) -> int:
    """Repeats one batch until it stops finding work.

    The pause leaves room for the live swarm, which reads this database while
    the repair runs.
    """
    done = 0
    while True:
        n = (await _run(session, query, batch=batch, **params)).get("n", 0)
        if not n:
            return done
        done += n
        logger.info("%s %s node(s)/edge(s) (%s total)", verb, n, done)
        if pause:
            await asyncio.sleep(pause)


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="report the scope and change nothing")
    parser.add_argument("--batch", type=int, default=200, help="nodes per committed batch (default 200)")
    parser.add_argument("--pause", type=float, default=0.1, help="seconds between batches (default 0.1)")
    parser.add_argument("--sample", type=int, default=20, help="how many examples to print (default 20)")
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
            numeric = (await _run(session, COUNT_NUMERIC, pattern=NUMERIC_NAME_PATTERN)).get("n", 0)
            scaffold = (await _run(session, COUNT_SCAFFOLD, names=SCAFFOLD_NAMES)).get("n", 0)
            self_edges = (await _run(session, COUNT_SELF_EDGES)).get("n", 0)
            logger.info(
                "Non-entity nodes: %s numeric, %s prompt scaffolding; %s self-correlation edges",
                numeric, scaffold, self_edges,
            )

            # Printed before anything is removed. A deletion driven by a shape
            # rule has to be inspectable, or a rule that is slightly too broad
            # takes real companies with it silently.
            for label, query, params in (
                ("numeric", LIST_NUMERIC, {"pattern": NUMERIC_NAME_PATTERN}),
                ("scaffolding", LIST_SCAFFOLD, {"names": SCAFFOLD_NAMES}),
            ):
                for row in await _fetch(session, query, limit=args.sample, **params):
                    logger.info("  %s: %r (degree %s)", label, row["name"], row["degree"])

            if not (numeric or scaffold or self_edges):
                logger.info("Nothing to repair.")
                return 0
            if args.dry_run:
                logger.info("Dry run: no changes written.")
                return 0

            removed_numeric = await _drain(
                session, DELETE_NUMERIC, "Removed numeric", args.batch, args.pause,
                pattern=NUMERIC_NAME_PATTERN,
            )
            removed_scaffold = await _drain(
                session, DELETE_SCAFFOLD, "Removed scaffolding", args.batch, args.pause,
                names=SCAFFOLD_NAMES,
            )
            removed_edges = await _drain(
                session, DELETE_SELF_EDGES, "Removed self-correlation", args.batch, args.pause,
            )

            remaining = (
                (await _run(session, COUNT_NUMERIC, pattern=NUMERIC_NAME_PATTERN)).get("n", 0)
                + (await _run(session, COUNT_SCAFFOLD, names=SCAFFOLD_NAMES)).get("n", 0)
                + (await _run(session, COUNT_SELF_EDGES)).get("n", 0)
            )
            logger.info(
                "Done. numeric=%s scaffolding=%s self_edges=%s remaining=%s",
                removed_numeric, removed_scaffold, removed_edges, remaining,
            )
            return 0 if remaining == 0 else 1
    finally:
        await driver.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

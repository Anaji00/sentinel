"""Give 254,542 Ethereum addresses the label their producer always asked for.

`services/enrichment/enrichers/crypto.py` proposes `target_label: "Wallet"` for
every transfer counterparty. `Wallet` was missing from ALLOWED_NODE_LABELS, so
the supervisor rewrote it to `Entity` -- and that one omission is almost the
whole of the "84% of this graph is untyped" finding:

    :Entity total                    257,689
    of which 0x... addresses         254,542   (98.8%)
    genuinely untyped everything else  3,147

The code side is fixed (`Wallet` is now allowlisted, and tests pin it). This
script repairs the nodes already written.

WHY THIS IS NOT JUST `SET n:Wallet`
-----------------------------------
Adding a label is non-destructive and would be the safer-looking half-measure,
but several read paths take `head(labels(n))` or `labels(n)[0]` as the node's
type -- services/api_gateway/routes/graph.py does it in four places. A node
carrying both `Entity` and `Wallet` would report whichever came first, which is
not something the query controls. So the label is swapped atomically per batch
rather than added.

Batched because this touches a quarter of a million nodes: one transaction that
size risks the heap, and a partial run is safe to resume -- the selection is
"still labelled Entity and looks like an address", so anything already converted
is simply not selected again.

USAGE
    python scripts/relabel_wallet_nodes.py            # count only, no writes
    python scripts/relabel_wallet_nodes.py --apply    # convert, in batches
"""

from __future__ import annotations

import argparse
import os
import sys

try:
    from neo4j import GraphDatabase
except ImportError:  # pragma: no cover
    print("neo4j driver not installed", file=sys.stderr)
    raise


BATCH = 5000

# An Ethereum address and nothing else: `0x` followed by exactly 40 hex digits.
#
# Deliberately stricter than "starts with 0x". A prefix test would also catch a
# transaction hash (66 chars) or a truncated fragment, and mislabelling those as
# wallets would be the same class of mistake as calling a flag code a company.
COUNT = """
MATCH (n:Entity)
WHERE n.name =~ '0x[0-9a-fA-F]{40}'
RETURN count(*) AS n
"""

# What is NOT selected, reported alongside, because a number that only counts
# what it is about to change cannot show what it is leaving behind.
RESIDUE = """
MATCH (n:Entity)
WHERE n.name STARTS WITH '0x' AND NOT n.name =~ '0x[0-9a-fA-F]{40}'
RETURN size(n.name) AS len, count(*) AS n
ORDER BY n DESC LIMIT 5
"""

# Addresses that already exist as :Wallet are left alone.
#
# Once the uniqueness constraints are in place this is not optional. The same
# address can exist as both `:Entity` and `:Wallet` -- which is exactly what a
# link proposal naming `target_label` and not `source_label` produced -- and
# relabelling the Entity copy then violates `uniq_wallet_name` and aborts the
# whole batch, not just the offending row. Those pairs are duplicates and
# belong to the merge script; this one only promotes addresses that have no
# typed counterpart.
RELABEL = """
MATCH (n:Entity)
WHERE n.name =~ '0x[0-9a-fA-F]{40}'
  AND NOT EXISTS { MATCH (w:Wallet {name: n.name}) }
WITH n LIMIT $batch
SET n:Wallet
REMOVE n:Entity
RETURN count(n) AS converted
"""

# Reported alongside, because a count that describes only what it converted
# cannot show what it had to leave behind.
COLLISIONS = """
MATCH (n:Entity)
WHERE n.name =~ '0x[0-9a-fA-F]{40}'
  AND EXISTS { MATCH (w:Wallet {name: n.name}) }
RETURN count(*) AS n
"""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="perform the relabel; without it nothing is written")
    ap.add_argument("--batch", type=int, default=BATCH)
    ap.add_argument("--uri", default=os.getenv("NEO4J_URI", "bolt://localhost:7687"))
    ap.add_argument("--user", default=os.getenv("NEO4J_USER", "neo4j"))
    args = ap.parse_args()

    password = os.getenv("NEO4J_PASSWORD")
    if not password:
        print("NEO4J_PASSWORD is not set.", file=sys.stderr)
        return 2

    driver = GraphDatabase.driver(args.uri, auth=(args.user, password))
    try:
        with driver.session() as session:
            total = session.run(COUNT).single()["n"]
            print(f"{total:,} :Entity nodes are Ethereum addresses.")

            residue = [dict(r) for r in session.run(RESIDUE)]
            if residue:
                print("\nNot selected -- 0x-prefixed but not 40 hex digits:")
                for row in residue:
                    print(f"  length {row['len']:>4}: {row['n']:,}")
                print("  (transaction hashes and fragments; left as Entity "
                      "rather than guessed at)")

            collisions = session.run(COLLISIONS).single()["n"]
            if collisions:
                print()
                print(f"{collisions:,} of those already exist as :Wallet: duplicates, not untyped nodes.")
                print("  Left for scripts/merge_split_graph_nodes.py, which merges them")
                print("  rather than failing a uniqueness constraint.")

            if not args.apply:
                print(f"\nDry run. Re-run with --apply to convert {total:,} nodes.")
                return 0

            done = 0
            while True:
                converted = session.run(
                    RELABEL, batch=args.batch
                ).single()["converted"]
                if not converted:
                    break
                done += converted
                print(f"  {done:,} / {total:,}")
            print(f"\nConverted {done:,} nodes to :Wallet.")

            remaining = session.run(
                "MATCH (n:Entity) RETURN count(*) AS n"
            ).single()["n"]
            print(f":Entity now holds {remaining:,} nodes.")
    finally:
        driver.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

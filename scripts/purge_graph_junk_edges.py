"""Removes four classes of junk edges found by the 18 Nov Neo4j audit, after saving every edge it will delete to a JSON-lines backup.

  1. PEER_OF edges touching a broad-market index anchor (the market factor, not a link between companies).
  2. Self-loops: an edge from a node to itself (114 SYMPATHY_MOVER such as NVDA to NVDA).
  3. SYMPATHY_MOVER edges whose endpoint is a price-shaped string such as '$325.18' (281 endpoints; futures and crypto symbols such as GC=F or BTCUSD are real instruments and stay).
  4. The five SUPPLIER_TO pairs removed from the table by the 12 Nov name gate: ELF-TGT, FCFS-ONON, LII-ONON, REGN-ONON, RPM-ONON (either direction).
Dry run unless --apply. Counts and a sample per class are printed first. Backup: --backup PATH (default /tmp/graph_junk_edges_backup.jsonl; copy it out of the
container). Nothing else is touched. Run: docker exec -i -w /app sentinel-api_gateway python - --apply --backup /tmp/b.jsonl < scripts/purge_graph_junk_edges.py
"""
import argparse
import asyncio
import json
import sys

sys.path.insert(0, "/app")
from shared.db import get_neo4j  # noqa: E402
from shared.utils.residual_corr import MARKET_INDEX_ANCHORS  # noqa: E402

ANCHORS = sorted(MARKET_INDEX_ANCHORS)
KEYS = "['id', 'ticker', 'name', 'symbol']"
NAME = "coalesce(a.id, a.ticker, a.name, a.symbol)"
NAME_B = "coalesce(b.id, b.ticker, b.name, b.symbol)"
PAIRS = [["ELF", "TGT"], ["FCFS", "ONON"], ["LII", "ONON"], ["REGN", "ONON"], ["RPM", "ONON"]]

CLASSES = {
    "peer_index": ("MATCH (a)-[r:PEER_OF]-(b) WHERE any(k IN " + KEYS + " WHERE toUpper(toString(a[k])) IN $anchors)", {"anchors": ANCHORS}),
    "self_loop": ("MATCH (a)-[r]->(a)", {}),
    "sympathy_non_ticker": ("MATCH (a)-[r:SYMPATHY_MOVER]-(b) WHERE toString(" + NAME + ") =~ '^[$]?[0-9][0-9.,]*$' "
                            "OR toString(" + NAME_B + ") =~ '^[$]?[0-9][0-9.,]*$'", {}),
    "supplier_name_gate": ("MATCH (a)-[r:SUPPLIER_TO]-(b) WHERE any(p IN $pairs WHERE (toUpper(toString(" + NAME + ")) = p[0] AND toUpper(toString(" + NAME_B + ")) = p[1]) "
                           "OR (toUpper(toString(" + NAME + ")) = p[1] AND toUpper(toString(" + NAME_B + ")) = p[0]))", {"pairs": PAIRS}),
}


async def main(apply: bool, backup: str) -> None:
    g = await get_neo4j()
    total = 0
    with open(backup, "w", encoding="utf-8") as fh:
        for label, (match, params) in CLASSES.items():
            rows = await g.query(match + " RETURN DISTINCT elementId(r) AS id, type(r) AS t, " + NAME + " AS a, " + NAME_B.replace("b.", "b.") + " AS b, "
                                 "properties(r) AS p", params) if label != "self_loop" else await g.query(
                match + " RETURN DISTINCT elementId(r) AS id, type(r) AS t, " + NAME + " AS a, " + NAME + " AS b, properties(r) AS p", params)
            print(f"{label}: {len(rows or [])} edges; sample {[(r['a'], r['b']) for r in (rows or [])[:5]]}")
            for r in rows or []:
                fh.write(json.dumps({"class": label, **{k: r[k] for k in ("id", "t", "a", "b", "p")}}, default=str) + "\n")
            total += len(rows or [])
    print("total to delete:", total, "| backup:", backup)
    if not apply:
        return
    for label, (match, params) in CLASSES.items():
        await g.query(match + " DELETE r", params)
        left = await g.query(match + " RETURN count(DISTINCT r) AS n", params)
        print(f"deleted {label}; left {left[0]['n'] if left else None}")


ap = argparse.ArgumentParser()
ap.add_argument("--apply", action="store_true")
ap.add_argument("--backup", default="/tmp/graph_junk_edges_backup.jsonl")
a = ap.parse_args()
asyncio.run(main(a.apply, a.backup))

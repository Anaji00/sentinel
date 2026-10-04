"""Seed region-to-instrument edges, so a place names what it would move.

Measured on 3 October: the graph's 33 Region nodes connected only to the
vessels and aircraft located in them, and no edge joined a region to anything
that trades. A geopolitical scenario about the Strait of Hormuz therefore had
no path from its region to crude. These edges come from the same table the
enrichers use to tag events (shared/utils/osint_bridges.py), through the
governed proposal path:

    Region  -[COMMODITY_EXPOSURE]->  Commodity     STRAIT OF HORMUZ -> CL=F
    Company -[HAS_EXPOSURE_IN]->     Region        TSM -> TAIWAN STRAIT

    python scripts/seed_region_exposures.py --dry-run
    python scripts/seed_region_exposures.py
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


async def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    from shared.utils.osint_bridges import region_exposure_edges

    edges = region_exposure_edges()
    for region, sym, target in edges:
        print(f"  {region.upper():22} {sym:6} {target}")
    print(f"{len(edges)} edge(s)")
    if args.dry_run:
        return 0

    from shared.kafka import SentinelProducer
    from services.enrichment.graph_writer import GraphWriter

    producer = SentinelProducer()
    await producer.start()
    writer = GraphWriter(producer)
    props = {"confidence": 0.8, "source": "osint_bridges"}
    for region, sym, target in edges:
        if target == "Commodity":
            await writer.link_entities(
                region.upper(), "COMMODITY_EXPOSURE", sym, properties=dict(props),
                source_label="Region", target_label="Commodity",
            )
        else:
            await writer.link_entities(
                sym, "HAS_EXPOSURE_IN", region.upper(), properties=dict(props),
                source_label="Company", target_label="Region",
            )
    # close() is the flush point; SentinelProducer has no flush().
    await producer.close()
    print("proposed; the supervisor commits them")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

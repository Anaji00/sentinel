"""Print the proven cells of the live reaction study (read-only)."""
import asyncio
import json

from shared.db import get_redis


def walk(node, out):
    if isinstance(node, dict):
        if "family" in node and "verdict" in node:
            out.append(node)
        for v in node.values():
            walk(v, out)
    elif isinstance(node, list):
        for v in node:
            walk(v, out)


async def main() -> None:
    r = await get_redis()
    study = json.loads(await r.raw.get("sentinel:signals:study"))
    found: list = []
    walk(study, found)
    for c in sorted(found, key=lambda c: (c["family"], str(c.get("horizon")))):
        if c["verdict"] == "proven":
            print(c["family"], c.get("horizon"), c.get("units"), c.get("mean_aligned"), c.get("t"))
    print("keys of a cell:", sorted(found[0].keys()) if found else [])


asyncio.run(main())

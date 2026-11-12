"""The stored grade of every correlation producer: does it precede bigger moves than chance? Run in a service container."""

import asyncio
import json

from shared.db import get_redis
from shared.utils.signal_store import PRODUCER_GRADES_KEY


async def main() -> None:
    r = await get_redis()
    blob = await r.raw.get(PRODUCER_GRADES_KEY)
    g = json.loads(blob if isinstance(blob, str) else blob.decode()) if blob else {}
    print("keys:", sorted(g))
    for k in ("correlation_families_5d", "correlations_5d", "agents_5d", "edges_5d"):
        rows = g.get(k) or []
        print(f"\n{k}: {len(rows)} groups")
        for x in sorted(rows, key=lambda x: -int(x.get("n") or 0))[:14]:
            print("  ", str(x.get("group"))[:52].ljust(52), "n=", str(x.get("n")).rjust(5), x.get("verdict"), {k2: v for k2, v in x.items() if k2 not in ("group", "n", "verdict")})


asyncio.run(main())

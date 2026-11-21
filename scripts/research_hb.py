"""Read-only: dump per-component heartbeat age and suppressed-failure counters, plus Redis key-family sizes."""
import asyncio
import json
from collections import Counter

from shared.db import get_redis


async def main() -> None:
    r = await get_redis()
    raw = getattr(r, "raw", r)
    print("== heartbeats")
    async for k in raw.scan_iter(match="sentinel:heartbeat:*", count=200):
        v = await raw.get(k)
        ttl = await raw.ttl(k)
        try:
            d = json.loads(v)
        except Exception:
            continue
        sup = d.get("suppressed") or {}
        name = k.decode() if isinstance(k, bytes) else k
        print(name.split(":")[-1], "ttl", ttl, "sites", sup.get("sites"), "total", sup.get("total"),
              json.dumps(sup.get("counts"))[:400])
    print("== redis info")
    info = await raw.info("memory")
    print({x: info.get(x) for x in ("used_memory_human", "maxmemory_human", "mem_fragmentation_ratio", "maxmemory_policy")})
    print("dbsize", await raw.dbsize())
    fam: Counter = Counter()
    nottl: Counter = Counter()
    sizes: Counter = Counter()
    n = 0
    async for k in raw.scan_iter(count=1000):
        n += 1
        ks = k.decode() if isinstance(k, bytes) else k
        parts = ks.split(":")
        f = ":".join(parts[:3]) if len(parts) > 2 else ks
        fam[f] += 1
        if n % 7 == 0:
            ttl = await raw.ttl(k)
            if ttl == -1:
                nottl[f] += 1
            try:
                sizes[f] += await raw.memory_usage(k) or 0
            except Exception:
                pass
        if n > 60000:
            break
    print("keys scanned", n)
    for f, c in fam.most_common(25):
        print(f, c, "sampled_nottl", nottl[f], "sampled_bytes", sizes[f])


asyncio.run(main())

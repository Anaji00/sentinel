"""Which call sites used the hosted models each day (calls and repeated prompts), from the usage meter. Run in a service container."""

import asyncio

from shared.db import get_redis


async def main() -> None:
    r = await get_redis()
    async for key in r.raw.scan_iter(match="sentinel:hosted_llm:dup:*"):
        k = key.decode() if isinstance(key, bytes) else key
        data = await r.raw.hgetall(key)
        rows = sorted(((v.decode() if isinstance(v, bytes) else v, f.decode() if isinstance(f, bytes) else f) for f, v in data.items()), key=lambda x: -int(x[0]))
        print(k, "|", ", ".join(f"{f}={v}" for v, f in rows[:14]))


asyncio.run(main())

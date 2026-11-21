import asyncio
from shared.db import get_redis
async def main():
    raw = (await get_redis()).raw
    d = await raw.hgetall("sentinel:edges:survival")
    print({(k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v) for k, v in d.items()})
asyncio.run(main())

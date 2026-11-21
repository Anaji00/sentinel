import asyncio
import json

from shared.db import get_redis
from shared.utils.signal_store import FACTOR_REPORT_KEY


async def main() -> None:
    r = await get_redis()
    rep = json.loads(await r.raw.get(FACTOR_REPORT_KEY))
    print("top keys:", sorted(rep))
    h = rep.get("horizons") or {}
    print("horizons:", list(h))
    for name, body in list(h.items())[:1]:
        print(name, "keys:", sorted(body) if isinstance(body, dict) else type(body))
        for k, v in body.items():
            print("  ", k, "->", (str(v)[:260]))
            if k in ("models", "reports"):
                break


asyncio.run(main())

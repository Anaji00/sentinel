"""Wait until the factor model's report as_of moves past a given date, or time out. Run inside sentinel-api_gateway:
python /tmp/wma.py 2026-11-06 10800"""
import asyncio
import json
import sys
import time

from shared.db import get_redis
from shared.utils.signal_store import FACTOR_REPORT_KEY


async def main(old: str, limit_s: int) -> None:
    r = await get_redis()
    end = time.time() + limit_s
    while time.time() < end:
        rep = json.loads(await r.raw.get(FACTOR_REPORT_KEY) or "{}")
        cur = str(rep.get("as_of"))
        if cur > old:
            print("as_of moved:", old, "->", cur, "| generated_at", rep.get("generated_at"), "| scored", rep.get("scored"),
                  "| validated", rep.get("validated"), "| tradable", rep.get("tradable"))
            return
        await asyncio.sleep(120)
    print("timeout; as_of still", old)


asyncio.run(main(sys.argv[1], int(sys.argv[2])))

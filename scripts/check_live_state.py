"""Read-only check of the 11 Nov deployment: the factor-engine heartbeat, the watchdog and trader flags, the shadow-signal set,
the wargamer cap counter and the book's frozen weights."""
import asyncio
import json
from datetime import datetime, timezone

from shared.db import get_redis
from shared.utils import engine_liveness as el


async def main() -> None:
    raw = (await get_redis()).raw
    hb = await raw.get(f"sentinel:heartbeat:{el.COMPONENT}")
    age = el.heartbeat_age(hb)
    print("factor-engine heartbeat age:", None if age is None else round(age, 1), "| meta:", json.loads(hb).get("metadata") if hb else None)
    print("halt flag:", await raw.get(el.HALT_KEY), "| seen:", await raw.get(el.SEEN_KEY))
    print("shadow signals:", await raw.zcard("sentinel:signals:shadow"))
    day = datetime.now(timezone.utc).strftime("%Y%m%d")
    print("wargamer calls today:", await raw.get(f"sentinel:wargamer:daily:adversarial_wargamer_v2_hosted:{day}"))
    book = json.loads(await raw.get("sentinel:auto_trade:book"))
    print("book keys:", sorted(book), "| risk_weights:", len(book.get("risk_weights") or {}), "| vetoes:", book.get("vetoes"))


asyncio.run(main())

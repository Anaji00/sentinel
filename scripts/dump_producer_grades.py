"""Print what the producer-grade gate says about each correlation producer. Run inside a service container."""

import asyncio
import json

from shared.db import get_redis
from shared.utils.producer_grades import is_uninformative, uninformative_rules
from shared.utils.signal_store import PRODUCER_GRADES_KEY

NAMES = [
    "Cross-Domain Semantic Convergence",
    "Quantitative Equity Peer Cointegration & Decoupling",
    "Hawkes Cross-Domain Excitation",
]


async def main() -> None:
    r = await get_redis()
    blob = await r.raw.get(PRODUCER_GRADES_KEY)
    if not blob:
        print("no producer grades stored")
        return
    grades = json.loads(blob if isinstance(blob, str) else blob.decode("utf-8"))
    retired = uninformative_rules(grades)
    print("retired rules:", len(retired))
    for n in NAMES:
        print(n, "->", "uninformative" if is_uninformative(n, retired) else "not capped")
    keys = [k for k in (grades if isinstance(grades, dict) else {}) if "emantic" in str(k) or "QUANT" in str(k).upper()]
    print("grade keys matching:", keys[:10])


asyncio.run(main())

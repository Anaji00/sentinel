"""Print stored dynamic rules whose name contains the given text (read-only)."""
import asyncio
import json
import sys

from shared.db import get_redis


async def main(text: str) -> None:
    r = await get_redis()
    stored = await r.raw.hgetall("sentinel:correlation:dynamic_rules")
    n = 0
    for k, v in stored.items():
        rule = json.loads(v)
        if text.lower() in str(rule.get("rule_name", "")).lower():
            n += 1
            print(json.dumps(rule, indent=1)[:1800])
            if n >= 1:
                break
    print("rules stored:", len(stored))


asyncio.run(main(sys.argv[1]))

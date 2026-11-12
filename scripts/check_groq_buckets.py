"""Counted requests and tokens per Groq step for the last two UTC days. Run inside a service container."""

import asyncio

from shared.db import get_redis
from shared.utils import hosted_llm as hl


async def main() -> None:
    r = await get_redis()
    for step in ("groq/openai/gpt-oss-120b", "groq/openai/gpt-oss-20b", "groq/qwen/qwen3.8-27b"):
        for day in ("20261111", "20261112"):
            t = await r.raw.get(hl.BUCKET_KEY.format(step=step, unit="t", window=day))
            q = await r.raw.get(hl.BUCKET_KEY.format(step=step, unit="r", window=day))
            print(step, day, "tokens", t, "requests", q)


asyncio.run(main())

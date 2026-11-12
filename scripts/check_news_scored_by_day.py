"""Headlines scored by the news model per day, and the request count that implies (20 a batch). Run in a service container."""

import asyncio
import os

from shared.db import get_timescale


async def main() -> None:
    db = await get_timescale()
    m = os.getenv("NEWS_LLM_MODEL", "")
    rows = await db.query(
        "SELECT created_at::date AS d, count(*) AS n, count(*) FILTER (WHERE model LIKE '%#stale') AS stale "
        "FROM stock_news_llm WHERE created_at > NOW() - INTERVAL '4 days' GROUP BY 1 ORDER BY 1")
    for r in rows or []:
        print(r["d"], "scored rows", r["n"], "| marked stale", r["stale"], "| model", m)
    rows = await db.query("SELECT model, count(*) AS n, min(created_at) AS first FROM stock_news_llm GROUP BY 1 ORDER BY 3 DESC LIMIT 5")
    for r in rows or []:
        print(r["model"], r["n"], r["first"])


asyncio.run(main())

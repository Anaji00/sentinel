"""Share of the news model's scored headlines, by the stock's market cap, over the last two full days. Run in a service container."""

import asyncio
import os

from shared.db import get_redis, get_timescale
from shared.utils import market_cap as mc


async def main() -> None:
    r, db = await get_redis(), await get_timescale()
    raw = await r.raw.hgetall(mc.CAPS_KEY)
    caps = {(k.decode() if isinstance(k, bytes) else k).upper(): float(v.decode() if isinstance(v, bytes) else v) for k, v in raw.items()}
    print("cap table names:", len(caps))
    rows = await db.query(
        "SELECT ticker, count(*) AS n FROM stock_news_llm WHERE created_at >= date_trunc('day', NOW()) - INTERVAL '2 days' "
        "AND created_at < date_trunc('day', NOW()) AND model = $1 GROUP BY 1", os.getenv("NEWS_LLM_MODEL", ""))
    tot = sum(r["n"] for r in rows)
    bands = {"under $1bn": 0, "$1bn to $10bn": 0, "$10bn and over": 0, "no cap on file": 0}
    for r in rows:
        c = caps.get(str(r["ticker"]).upper())
        k = "no cap on file" if c is None else "under $1bn" if c < 1e9 else "$1bn to $10bn" if c < 1e10 else "$10bn and over"
        bands[k] += r["n"]
    print("scored rows, two full days:", tot, "| tickers:", len(rows))
    for k, v in bands.items():
        print(f"  {k:16s} {v:6d}  {v / tot:5.1%}")
    top = sorted(rows, key=lambda x: -x["n"])[:8]
    print("busiest tickers:", [(t["ticker"], t["n"]) for t in top])


asyncio.run(main())

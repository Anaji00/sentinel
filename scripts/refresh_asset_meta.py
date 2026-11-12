"""Rewrite the cached Alpaca asset list now (atomic SET), so a new field appears without waiting for the model run."""

import asyncio
import os

import aiohttp

from shared.db import get_redis
from shared.utils.asset_universe import exchange_of, load_asset_meta, refresh_asset_meta


async def main() -> None:
    r = await get_redis()
    headers = {"APCA-API-KEY-ID": os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID") or "",
               "APCA-API-SECRET-KEY": os.getenv("ALPACA_SECRET_KEY") or os.getenv("ALPACA_API_SECRET") or os.getenv("APCA_API_SECRET_KEY") or ""}
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=90)) as s:
        n = await refresh_asset_meta(r.raw, s, headers)
    meta = await load_asset_meta(r.raw)
    print("cached", n, "| EADSY ->", exchange_of("EADSY", meta), "| AAPL ->", exchange_of("AAPL", meta), "| USAR short ok:", meta.get("USAR", [0, None])[1])


asyncio.run(main())

"""For the study's no-bars queue: which exchange each name trades on, and whether Alpaca's free data serves it daily bars.
Read-only; run inside sentinel-tradfi (it has the Alpaca keys, which are never printed)."""

import asyncio
import collections
import json
import os
import urllib.request

from shared.db import get_redis
from shared.utils.signal_store import NEEDS_DAILY_KEY

KEY = os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID")
SECRET = os.getenv("ALPACA_SECRET_KEY") or os.getenv("ALPACA_API_SECRET") or os.getenv("APCA_API_SECRET_KEY")
HEAD = {"APCA-API-KEY-ID": KEY or "", "APCA-API-SECRET-KEY": SECRET or ""}


def get(url: str) -> dict:
    with urllib.request.urlopen(urllib.request.Request(url, headers=HEAD), timeout=60) as r:
        return json.loads(r.read())


async def main() -> None:
    r = await get_redis()
    names = sorted(str(m.decode() if isinstance(m, bytes) else m).upper() for m in await r.raw.smembers(NEEDS_DAILY_KEY))
    print("queue", len(names))
    assets = get("https://paper-api.alpaca.markets/v2/assets?status=active&asset_class=us_equity")
    by = {a["symbol"].upper(): a for a in assets}
    ex = collections.Counter(by[n]["exchange"] if n in by else "NOT IN ACTIVE LIST" for n in names)
    print("exchange of queued names:", dict(ex))
    served = {"sip": set(), "iex": set()}
    for feed in ("sip", "iex"):
        for i in range(0, len(names), 100):
            chunk = names[i:i + 100]
            url = ("https://data.alpaca.markets/v2/stocks/bars?symbols=" + ",".join(chunk)
                   + f"&timeframe=1Day&start=2025-06-01&limit=10000&feed={feed}&adjustment=all")
            try:
                data = get(url).get("bars") or {}
            except Exception as exc:
                print(feed, "chunk", i, "failed:", str(exc)[:120])
                continue
            served[feed] |= {k.upper() for k, v in data.items() if v}
    print("names with daily bars since 2025-06 -> sip:", len(served["sip"]), "| iex:", len(served["iex"]))
    otc = [n for n in names if n in by and by[n]["exchange"] == "OTC"]
    listed = [n for n in names if n in by and by[n]["exchange"] != "OTC"]
    print("OTC queued:", len(otc), "served by sip:", len([n for n in otc if n in served["sip"]]))
    print("exchange-listed queued:", len(listed), "served by sip:", len([n for n in listed if n in served["sip"]]))
    print("exchange-listed but NOT served:", [n for n in listed if n not in served["sip"]][:40])
    print("OTC served sample:", [n for n in otc if n in served["sip"]][:15])


asyncio.run(main())

"""Live check of the held-back and model-quota data paths, and that both HTTP routes exist and require a login. Run in api_gateway."""

import asyncio
import json
import urllib.error
import urllib.request

from shared.db import get_redis
from shared.utils import held_back as hb
from shared.utils import model_quota as mq
from shared.utils.signal_families import FAMILY_DESCRIPTIONS
from shared.utils.signal_store import SHADOW_KEY


async def main() -> None:
    r = await get_redis()
    models = await mq.snapshot(r.raw)
    print("QUOTA", json.dumps(models[:3], indent=1)[:1400])
    items = await r.raw.zrevrange(SHADOW_KEY, 0, -1)
    print("shadow entries:", len(items))
    tickers = {}
    for it in items[:400]:
        e = json.loads(it if isinstance(it, str) else it.decode())
        tickers[e["instrument"]] = tickers.get(e["instrument"], 0) + 1
    top = max(tickers, key=tickers.get) if tickers else None
    print("most held-back ticker:", top, tickers.get(top))
    if top:
        print("HELD", json.dumps(hb.for_ticker(items, top, FAMILY_DESCRIPTIONS, 2), indent=1)[:1500])
    for path in ("/api/v1/health/model-quota", "/api/v1/signals/held-back/AAPL"):
        try:
            code = urllib.request.urlopen("http://localhost:8000" + path, timeout=10).status
        except urllib.error.HTTPError as e:
            code = e.code
        print(path, "->", code)


asyncio.run(main())

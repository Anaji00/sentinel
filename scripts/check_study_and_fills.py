"""The signal study's untradeable and needs-daily fields, and the auto-trader's fill status by day. Run inside a service container."""

import asyncio
import json

from shared.db import get_redis, get_timescale
from shared.utils.signal_store import NEEDS_DAILY_KEY, STUDY_KEY


async def main() -> None:
    r = await get_redis()
    blob = await r.raw.get(STUDY_KEY)
    study = json.loads(blob if isinstance(blob, str) else blob.decode("utf-8")) if blob else {}
    print("study keys:", sorted(study)[:20] if isinstance(study, dict) else type(study))
    for k in ("generated_at", "as_of", "instruments_untradeable", "instruments_without_bars", "instruments_priceable"):
        if isinstance(study, dict) and k in study:
            print(k, "=", study[k])
    print("needs_daily names:", await r.raw.scard(NEEDS_DAILY_KEY))
    db = await get_timescale()
    rows = await db.query(
        "SELECT day, status, COALESCE(fill_status,'-') AS fill, count(*) AS n FROM auto_trade_orders "
        "WHERE day >= CURRENT_DATE - 2 GROUP BY 1,2,3 ORDER BY 1 DESC, 2, 3")
    for x in rows or []:
        print(x["day"], x["status"], x["fill"], x["n"])
    gm = await db.query("SELECT day, status, fill_status, filled_qty, fill_price FROM auto_trade_orders "
                        "WHERE symbol = 'GMAB' ORDER BY day DESC LIMIT 3")
    for x in gm or []:
        print("GMAB", dict(x))


asyncio.run(main())

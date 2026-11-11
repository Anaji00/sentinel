"""Print the last two days of auto-trader orders. Run inside a service container."""

import asyncio

from shared.db import get_timescale


async def main() -> None:
    db = await get_timescale() if asyncio.iscoroutinefunction(get_timescale) else get_timescale()
    rows = await db.query(
        "SELECT day, symbol, side, qty, status, ref_price, reason, error, client_order_id "
        "FROM auto_trade_orders WHERE day >= CURRENT_DATE - 2 ORDER BY day DESC, symbol"
    )
    for r in rows or []:
        print(r["day"], r["symbol"], r["side"], r["qty"], r["status"], r["ref_price"],
              (r["reason"] or "")[:40], (r["error"] or "")[:400])
    print("rows:", len(rows or []))


asyncio.run(main())

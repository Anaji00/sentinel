"""Read-only: auto-trader orders since 1 Nov that are not recorded as filled, against the paper account's own answer, plus per-ticker drift
between the broker's positions and the net of what the trader ordered. Places no order and writes nothing. Run inside a service container:

    MSYS_NO_PATHCONV=1 docker exec -w /app sentinel-agents-heavy python scripts/reconcile_trader_orders.py
"""

import asyncio
import json
from datetime import date
from typing import Dict

from services.agents.auto_trader import BOOK_KEY
from shared.broker.alpaca import AlpacaBroker
from shared.db import get_redis, get_timescale

SINCE = date(2026, 11, 1)
ALL_TIME = date(2000, 1, 1)   # drift is measured against every order the trader ever sent: the book was built before 1 Nov
_UNRESOLVED_SQL = """
    SELECT id, day, symbol, side, qty, order_id, status, COALESCE(fill_status, '-') AS fill_status, filled_qty
    FROM auto_trade_orders
    WHERE day >= $1::date AND order_id IS NOT NULL AND COALESCE(fill_status, '-') NOT IN ('filled', 'unfilled')
    ORDER BY day, symbol
"""
_NET_SQL = """
    SELECT symbol,
           sum(CASE WHEN lower(side) = 'buy' THEN qty ELSE -qty END) AS ordered_net,
           sum(CASE WHEN lower(side) = 'buy' THEN COALESCE(filled_qty, 0) ELSE -COALESCE(filled_qty, 0) END) AS filled_net
    FROM auto_trade_orders WHERE day >= $1::date AND order_id IS NOT NULL GROUP BY symbol
"""


async def main() -> None:
    db, r, broker = await get_timescale(), await get_redis(), AlpacaBroker(paper=True)
    assert broker.paper, "paper account only"
    raw = r.raw
    blob = await raw.get(BOOK_KEY)
    book = json.loads(blob if isinstance(blob, str) else blob.decode()) if blob else {}
    side_of: Dict[str, str] = {**{t: "long" for t in book.get("long", [])}, **{t: "short" for t in book.get("short", [])}}

    print(f"== orders since {SINCE} not recorded filled/unfilled (broker's answer, read-only)")
    for x in await db.query(_UNRESOLVED_SQL, SINCE) or []:
        o = await broker.get_order_status(str(x["order_id"]))
        broker_view = "no answer" if o is None else f"{getattr(o.status, 'value', o.status)} filled {o.filled_qty} @ {o.filled_avg_price}"
        print(f"{x['day']} {x['symbol']:<6} {x['side']:<4} qty {x['qty']:<8g} db status {x['status']}/{x['fill_status']:<10} broker: {broker_view}")
        await asyncio.sleep(0.3)

    held = {p.symbol.upper(): float(p.qty) for p in await broker.get_positions()}
    nets = {str(x["symbol"]).upper(): x for x in await db.query(_NET_SQL, ALL_TIME) or []}
    print("\n== per-ticker drift: broker position vs net ordered and net filled over all recorded orders (a position held by hand or before the trader differs)")
    print(f"{'ticker':<7}{'book':<7}{'broker':>10}{'db_filled':>11}{'ordered':>10}{'broker-filled':>15}{'broker-ordered':>16}")
    drift = 0
    for t in sorted(set(held) | set(nets) | set(side_of)):
        n = nets.get(t, {})
        filled, ordered, have = (round(float(n.get("filled_net") or 0), 4), round(float(n.get("ordered_net") or 0), 4), round(held.get(t, 0.0), 4))
        if abs(have - filled) > 1e-3 or abs(have - ordered) > 1e-3 or (t in side_of) != (abs(have) > 0):
            drift += 1
            print(f"{t:<7}{side_of.get(t, '-'):<7}{have:>10g}{filled:>11g}{ordered:>10g}{round(have - filled, 4):>15g}{round(have - ordered, 4):>16g}")
    print(f"\n{drift} tickers differ of {len(set(held) | set(nets) | set(side_of))} seen")


asyncio.run(main())

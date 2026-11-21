"""Fire path of the price alerts: insert alerts created two hours ago, evaluate, report, clean up. Run inside sentinel-api_gateway."""
import asyncio

from services.api_gateway.routes.alerts import _evaluate
from shared.db import get_timescale

OWNER = "check:fire-path"


async def main() -> None:
    db = await get_timescale()
    await db.execute("DELETE FROM price_alerts WHERE owner = $1", OWNER)
    for direction, level in (("above", 100.0), ("above", 100000.0), ("below", 100000.0), ("below", 1.0)):
        await db.execute("INSERT INTO price_alerts (owner, ticker, direction, level, created_at, checked_at) "
                         "VALUES ($1, 'NVDA', $2, $3, NOW() - INTERVAL '2 hours', NOW() - INTERVAL '2 hours')", OWNER, direction, level)
    await _evaluate(db, OWNER)
    for r in await db.query("SELECT direction, level, triggered_at IS NOT NULL AS fired, triggered_price FROM price_alerts "
                            "WHERE owner = $1 ORDER BY id", OWNER):
        print(dict(r))
    await db.execute("DELETE FROM price_alerts WHERE owner = $1", OWNER)


asyncio.run(main())

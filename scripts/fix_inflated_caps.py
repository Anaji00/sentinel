"""One-off: apply market_cap's cap rules (implausible ratio to the vendor figure, liquidity bound) to the live cap table now; the job applies them
on its next run after a restart. Prints what changed. Run inside sentinel-api_gateway."""
import asyncio

from shared.db import get_redis, get_timescale
from shared.utils import market_cap as mc

ADV_SQL = "SELECT ticker, avg(close * volume) AS adv FROM daily_bars WHERE day > now() - interval '30 days' AND volume > 0 GROUP BY ticker"


async def main() -> None:
    r = await get_redis()
    db = await get_timescale()
    raw = r.raw
    table = {(k.decode() if isinstance(k, bytes) else str(k)): float(v) for k, v in (await raw.hgetall(mc.CAPS_KEY)).items()}
    names = list(table)
    vendor = dict(zip(names, [mc.parse_market_cap(v) for v in await raw.mget([mc.market_cap_key(n) for n in names])]))
    adv = {str(x["ticker"]).upper(): float(x["adv"]) for x in await db.query(ADV_SQL) or [] if x["adv"]}
    changed, removed = [], []
    for n in names:
        fixed = mc.best_cap(table[n], vendor.get(n))
        if fixed is not None and not mc.plausible_against_liquidity(fixed, adv.get(n)):
            removed.append((n, table[n]))
        elif fixed is not None and fixed != table[n]:
            changed.append((n, table[n], fixed))
    for n, _, fixed in changed:
        await raw.hset(mc.CAPS_KEY, n, repr(fixed))
    for n, _ in removed:
        await raw.hdel(mc.CAPS_KEY, n)
    print("lowered", len(changed), "| removed as implausible", len(removed), "| left", len(names) - len(removed))
    print("removed names at $10bn or more:", sorted(n for n, c in removed if c >= 1e10))
    print("removed names at $1bn to $10bn:", sum(1 for _, c in removed if 1e9 <= c < 1e10))


asyncio.run(main())

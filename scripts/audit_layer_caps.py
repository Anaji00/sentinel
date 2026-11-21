"""Read-only: sanity-check the stored market caps (Redis hash sentinel:marketcap:best) against close*known shares for mega caps and against ADV.
Run: docker exec -i -w /app sentinel-api_gateway python - < scripts/audit_layer_caps.py"""
import asyncio, sys
sys.path.insert(0, "/app")
from shared.db import get_redis, get_timescale
from shared.utils.market_cap import CAPS_KEY

CHECK = ["AAPL", "MSFT", "NVDA", "GOOGL", "GOOG", "AMZN", "META", "TSLA", "BRK.B", "BRK-B", "AVGO", "JPM", "LLY", "V", "XOM", "WMT", "NFLX", "ORCL", "COST", "BAC"]


async def main() -> None:
    r = await get_redis()
    raw = getattr(r, "raw", r)
    tbl = await raw.hgetall(CAPS_KEY) or {}
    caps = {(k.decode() if isinstance(k, bytes) else k): float(v.decode() if isinstance(v, bytes) else v) for k, v in tbl.items()}
    print("names with a cap:", len(caps), "ttl_s:", await raw.ttl(CAPS_KEY))
    for t in CHECK:
        print(t, f"{caps.get(t, float('nan')) / 1e9:,.0f}bn" if t in caps else "missing")
    db = await get_timescale()
    q = await db.query("SELECT count(DISTINCT ticker) n FROM daily_bars WHERE day > CURRENT_DATE - 5 AND close > 0 AND volume > 0")
    n_px = int(q[0]["n"])
    print("priced names last 5d:", n_px, "share with cap:", round(len(caps) / n_px, 3))
    big = sorted(caps.items(), key=lambda kv: -kv[1])[:12]
    print("top12:", [(k, round(v / 1e9)) for k, v in big])
    small = sum(1 for v in caps.values() if v < 1e9)
    print("below 1bn:", small, "of", len(caps))
    adv = await db.query("SELECT ticker, avg(close*volume) adv FROM daily_bars WHERE day > CURRENT_DATE - 30 AND volume>0 GROUP BY ticker")
    advd = {a["ticker"]: float(a["adv"]) for a in adv}
    bad = [(t, round(c / 1e9, 1), round(advd[t] / 1e6, 2)) for t, c in caps.items() if t in advd and advd[t] > 0 and c / advd[t] > 20000]
    print("cap/ADV > 20000 (implausible):", len(bad), sorted(bad, key=lambda x: -x[1])[:10])

asyncio.run(main())

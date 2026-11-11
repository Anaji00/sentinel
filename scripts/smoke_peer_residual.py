"""Smoke test of stage two of the live P1 rule against the real database (read-only): validity of the SQL and its latency."""
import asyncio
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from shared.db import get_timescale
from services.agents import peer_residual_scan as ps


async def main() -> None:
    db = await get_timescale()
    now_ny = datetime.now(ZoneInfo("America/New_York")).replace(hour=11, minute=0, second=0, microsecond=0)
    members = ["AMD", "AVGO", "MRVL", "TSM", "MU", "QCOM", "INTC", "ARM"]
    cands = [{"ticker": t, "reference": "industry", "members": [m for m in members if m != t], "r": -0.043, "ref": 0.077,
              "beta": 1.0, "breadth": 1.0, "zd": -4.0, "news": True} for t in ("NVDA", "AAPL", "MSFT")]
    t0 = time.time()
    names = sorted({c["ticker"] for c in cands} | {m for c in cands for m in c["members"]})
    rows = await db.query(ps.SLOT_SQL, names, now_ny.date(), now_ny.time())
    have = sum(1 for r in rows if r["px"] is not None)
    print(f"slot rows {len(rows)} with a price {have} in {time.time() - t0:.1f}s for {len(names)} tickers")
    t1 = time.time()
    hits = await ps.session_hits(db, cands, now_ny)
    print(f"session_hits ran in {time.time() - t1:.1f}s, hits {len(hits)}")


asyncio.run(main())

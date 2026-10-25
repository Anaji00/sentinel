"""
shared/utils/sec_rate.py

One pace for every bulk SEC loop in a process.

SEC's fair-access policy allows ten requests a second per client. The filings
collector runs several bulk loops -- 8-K history, company profiles, company
facts, XBRL frames -- and each paced only itself: together they reached about
thirteen a second on 8 October 2026, and SEC slowed every response to ~40 s
(a 5 MB file at ~120 KB/s) and began dropping connections. The live pollers
stay outside this, well inside the headroom it leaves.
"""
import asyncio
import time
from typing import Optional

BULK_REQUESTS_PER_SEC = 6.0

_lock: Optional[asyncio.Lock] = None
_next_at = 0.0


async def sec_slot() -> None:
    """Wait for the next shared slot; call before each bulk SEC request."""
    global _lock, _next_at
    if _lock is None:
        _lock = asyncio.Lock()
    async with _lock:
        now = time.monotonic()
        if _next_at > now:
            await asyncio.sleep(_next_at - now)
        _next_at = max(now, _next_at) + 1.0 / BULK_REQUESTS_PER_SEC


__all__ = ["BULK_REQUESTS_PER_SEC", "sec_slot"]

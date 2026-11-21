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
import logging
import os
import time
from typing import Any, Awaitable, Callable, Optional

from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("sec_rate")

BULK_REQUESTS_PER_SEC = 6.0
# Across every process on the host (review F9, 18 Nov): SEC's limit is ten a second per client, not per process. Each process still paces itself at
# BULK_REQUESTS_PER_SEC; this shared clock, kept in Redis, caps the sum. Set SEC_SHARED_SLOT=0 to turn it off; if Redis fails the local pace alone applies (counted).
GLOBAL_REQUESTS_PER_SEC = 8.0
SHARED_KEY = "sentinel:sec_rate:next_ms"
_SHARED_LUA = (
    "local t = redis.call('TIME'); local now = t[1] * 1000 + math.floor(t[2] / 1000); "
    "local nxt = tonumber(redis.call('GET', KEYS[1]) or '0'); if nxt < now then nxt = now end; "
    "redis.call('SET', KEYS[1], nxt + tonumber(ARGV[1]), 'PX', 10000); return nxt - now"
)

_lock: Optional[asyncio.Lock] = None
_next_at = 0.0


async def _redis_raw() -> Any:
    from shared.db import get_redis
    return getattr(await get_redis(), "raw", None)


_raw_provider: Callable[[], Awaitable[Any]] = _redis_raw


def shared_enabled() -> bool:
    return os.environ.get("SEC_SHARED_SLOT", "1") != "0"


async def _shared_wait() -> None:
    """Reserve the next slot on the host-wide clock and sleep until it; the reservation is one atomic Lua call, so two processes never get the same slot."""
    try:
        raw = await asyncio.wait_for(_raw_provider(), 2.0)
        wait_ms = float(await asyncio.wait_for(raw.eval(_SHARED_LUA, 1, SHARED_KEY, int(1000 / GLOBAL_REQUESTS_PER_SEC)), 2.0))
    except Exception as exc:
        swallowed("sec_rate.shared_slot", exc, logger)
        return
    if wait_ms > 0:
        await asyncio.sleep(min(wait_ms, 5000.0) / 1000.0)


async def sec_slot() -> None:
    """Wait for the next slot; call before each SEC request. Paces this process, then the whole host."""
    await _local_slot()
    if shared_enabled():
        await _shared_wait()


async def _local_slot() -> None:
    global _lock, _next_at
    if _lock is None:
        _lock = asyncio.Lock()
    async with _lock:
        now = time.monotonic()
        if _next_at > now:
            await asyncio.sleep(_next_at - now)
        _next_at = max(now, _next_at) + 1.0 / BULK_REQUESTS_PER_SEC


__all__ = ["BULK_REQUESTS_PER_SEC", "GLOBAL_REQUESTS_PER_SEC", "sec_slot"]

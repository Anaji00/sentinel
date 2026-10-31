"""
shared/utils/response_cache.py

Stale-while-revalidate for slow read endpoints.

Measured on 30 October: /attribution/signals took 13.2 s, recomputing 90 days
of outcomes on every visit to Performance; /backtest/results 3.7 s; the civic
feed 2-3 s; conflict exposure 1.9 s. None of them changes minute to minute.

A fresh copy is served from Redis. A stale one is still served at once while
one background task, behind a lock, refreshes it, so a reader never waits for
the recomputation and concurrent readers never all recompute together. Only
an expired or missing copy is computed inline.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Awaitable, Callable, Dict, Literal, Optional, Set

from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("sentinel.response_cache")

PREFIX = "sentinel:response_cache:"
LOCK_SUFFIX = ":refreshing"

State = Literal["fresh", "stale", "expired"]

# Background refreshes in flight, kept so the event loop does not collect them.
_TASKS: Set[asyncio.Task] = set()


def freshness(stored_at: Optional[float], now: float, ttl: float, max_stale: float) -> State:
    """Fresh within `ttl` seconds, stale up to `max_stale`, expired after (or never stored)."""
    if stored_at is None:
        return "expired"
    age = now - stored_at
    if age < ttl:
        return "fresh"
    if age < max_stale:
        return "stale"
    return "expired"


def cache_key(name: str, **params: Any) -> str:
    """A stable key from a route name and its query parameters."""
    parts = [f"{k}={params[k]}" for k in sorted(params) if params[k] is not None]
    return PREFIX + name + (":" + "&".join(parts) if parts else "")


def _jsonable(o: Any) -> Any:
    """What FastAPI would have sent: ISO dates, models as dicts, else text."""
    if hasattr(o, "isoformat"):
        return o.isoformat()
    if hasattr(o, "model_dump"):
        return o.model_dump(mode="json")
    return str(o)


async def _store(raw: Any, key: str, value: Any, max_stale: float) -> None:
    payload = json.dumps({"at": time.time(), "v": value}, default=_jsonable)
    await raw.set(key, payload, ex=int(max_stale))


async def _refresh(raw: Any, key: str, compute: Callable[[], Awaitable[Any]], max_stale: float) -> None:
    try:
        await _store(raw, key, await compute(), max_stale)
    except Exception as e:
        # The stale copy keeps being served; the failure is counted, not hidden.
        swallowed("utils.response_cache.refresh", e, logger, variant=key.removeprefix(PREFIX).split(":")[0])
    finally:
        try:
            await raw.delete(key + LOCK_SUFFIX)
        except Exception as e:
            swallowed("utils.response_cache.unlock", e, logger)


async def cached_json(
    raw: Any,
    key: str,
    compute: Callable[[], Awaitable[Any]],
    *,
    ttl: float,
    max_stale: float,
    lock_s: int = 300,
) -> Any:
    """The cached value for `key`, refreshing it per the rules in the module doc.

    `raw` is a redis.asyncio client; None means no cache, so `compute` runs
    inline. `compute` must return something JSON-serialisable.
    """
    if raw is None:
        return await compute()
    entry: Optional[Dict[str, Any]] = None
    try:
        blob = await raw.get(key)
        if blob:
            entry = json.loads(blob)
    except Exception as e:
        swallowed("utils.response_cache.read", e, logger)
    state = freshness(entry.get("at") if entry else None, time.time(), ttl, max_stale)
    if state == "fresh" and entry is not None:
        return entry["v"]
    if state == "stale" and entry is not None:
        try:
            if await raw.set(key + LOCK_SUFFIX, "1", nx=True, ex=lock_s):
                task = asyncio.create_task(_refresh(raw, key, compute, max_stale))
                _TASKS.add(task)
                task.add_done_callback(_TASKS.discard)
        except Exception as e:
            swallowed("utils.response_cache.lock", e, logger)
        return entry["v"]
    value = await compute()
    try:
        await _store(raw, key, value, max_stale)
    except Exception as e:
        swallowed("utils.response_cache.write", e, logger)
    return value

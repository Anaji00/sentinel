"""Stale-while-revalidate for slow read endpoints.

Measured on 30 October: /attribution/signals took 13.2 s on every visit to
Performance, /backtest/results 3.7 s, the civic feed 2-3 s.
"""

import asyncio
import datetime as dt
import json
import time

from shared.utils import response_cache as rc


def test_freshness_windows():
    assert rc.freshness(None, 100, 10, 50) == "expired"
    assert rc.freshness(95, 100, 10, 50) == "fresh"
    assert rc.freshness(80, 100, 10, 50) == "stale"
    assert rc.freshness(40, 100, 10, 50) == "expired"


def test_keys_are_stable_and_skip_absent_params():
    assert rc.cache_key("civic_feed", state=None, days=7) == "sentinel:response_cache:civic_feed:days=7"
    assert rc.cache_key("civic_feed", days=7, state="CA") == rc.cache_key("civic_feed", state="CA", days=7)
    assert rc.cache_key("backtest_results") == "sentinel:response_cache:backtest_results"


class _Raw:
    def __init__(self):
        self.kv = {}

    async def get(self, k):
        return self.kv.get(k)

    async def set(self, k, v, ex=None, nx=False):
        if nx and k in self.kv:
            return False
        self.kv[k] = v
        return True

    async def delete(self, k):
        self.kv.pop(k, None)


def _run(coro):
    return asyncio.run(coro)


def test_a_miss_computes_and_stores_then_a_hit_does_not_compute():
    raw, calls = _Raw(), []

    async def compute():
        calls.append(1)
        return {"n": len(calls)}

    async def go():
        a = await rc.cached_json(raw, "k", compute, ttl=60, max_stale=600)
        b = await rc.cached_json(raw, "k", compute, ttl=60, max_stale=600)
        return a, b

    assert _run(go()) == ({"n": 1}, {"n": 1})
    assert len(calls) == 1


def test_stale_is_served_at_once_and_refreshed_once_in_the_background():
    raw, calls = _Raw(), []
    raw.kv["k"] = json.dumps({"at": time.time() - 120, "v": "old"})

    async def compute():
        calls.append(1)
        await asyncio.sleep(0.01)
        return "new"

    async def go():
        first = await rc.cached_json(raw, "k", compute, ttl=60, max_stale=600)
        second = await rc.cached_json(raw, "k", compute, ttl=60, max_stale=600)  # lock held: no 2nd refresh
        await asyncio.sleep(0.05)
        third = await rc.cached_json(raw, "k", compute, ttl=60, max_stale=600)
        return first, second, third

    assert _run(go()) == ("old", "old", "new")
    assert len(calls) == 1
    assert "k:refreshing" not in raw.kv


def test_a_failed_refresh_keeps_serving_the_stale_copy():
    raw = _Raw()
    raw.kv["k"] = json.dumps({"at": time.time() - 120, "v": "old"})

    async def compute():
        raise RuntimeError("database away")

    async def go():
        first = await rc.cached_json(raw, "k", compute, ttl=60, max_stale=600)
        await asyncio.sleep(0.02)
        return first, await rc.cached_json(raw, "k", compute, ttl=60, max_stale=600)

    assert _run(go()) == ("old", "old")


def test_without_redis_it_just_computes():
    async def compute():
        return 7
    assert _run(rc.cached_json(None, "k", compute, ttl=1, max_stale=2)) == 7


def test_dates_are_stored_as_iso_like_fastapi_sends_them():
    raw = _Raw()

    async def compute():
        return {"at": dt.datetime(2026, 10, 30, 12, 0, tzinfo=dt.timezone.utc)}

    _run(rc.cached_json(raw, "k", compute, ttl=60, max_stale=600))
    assert json.loads(raw.kv["k"])["v"]["at"] == "2026-10-30T12:00:00+00:00"

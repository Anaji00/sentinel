"""The recent-event window must not be materialised whole on every lookup.

get_recent issued a single ZRANGE across the entire 48-hour set -- 144,212
members, 98.7 MB measured live -- fetched and JSON-parsed in full so Python
could filter it down and return at most 50 rows. It runs once per correlation
clause, per rule, per event.

Redis builds the whole reply in the client output buffer before sending, so each
call pushed used_memory from 114 MB to 273-287 MB against a 419 MB ceiling.
Measured: a spike every ~55 seconds, evicted_keys rising ~2,300 each time,
roughly 47 evictions a second sustained.

What that evicted is the damage. events:recent_window carries no TTL, so under
volatile-lru it is not evictable; the only candidates are the small TTL'd keys,
which is where the anomaly baselines live. 331 tickers passed through the
financial scorers in 24 hours and 4 still had a stored mean and variance -- and
a normaliser with no history scores its first observation 0, so the financial
z-scores were computed against a baseline eviction kept wiping.

The scan still reads every member: the caller ranks by anomaly score and takes
the top N, so stopping early would silently change which rows come back.
"""

import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.correlation.event_store import (  # noqa: E402
    RECENT_WINDOW_SCAN_BATCH, EventStore,
)


class _Raw:
    """A zset that honours BYSCORE with LIMIT, and records each reply size."""

    def __init__(self, members):
        self.members = members          # list of (payload, score), newest last
        self.reply_sizes = []

    async def zrange(self, key, start, end, desc=False, byscore=False,
                     offset=None, num=None, **kw):
        rows = sorted(self.members, key=lambda m: m[1], reverse=desc)
        rows = [m for m in rows if m[1] >= float(end)]
        if offset is not None:
            rows = rows[offset: offset + (num or len(rows))]
        self.reply_sizes.append(len(rows))
        return [r[0] for r in rows]


class _Redis:
    def __init__(self, raw):
        self.raw = raw


def _store(n_events):
    import time
    now = time.time()
    members = []
    for i in range(n_events):
        payload = json.dumps({
            "event_id": f"e{i}",
            "type": "equity_block",
            "domain": "equity",
            # Ascending anomaly, so the highest scores are the OLDEST events.
            # A scan that stopped early would miss them entirely.
            "anomaly_score": round(i / max(1, n_events - 1), 6),
            "tags": ["equity"],
            "region": None,
            "headline": "x" * 160,
            "summary": "y" * 200,
        })
        members.append((payload, now - (n_events - i)))
    store = EventStore.__new__(EventStore)
    store.cache_key = "events:recent_window"
    store._redis = _Redis(_Raw(members))
    return store


def test_no_single_reply_exceeds_the_batch_size():
    store = _store(5000)
    asyncio.run(store.get_recent(None, hours=48, limit=50))
    assert max(store._redis.raw.reply_sizes) <= RECENT_WINDOW_SCAN_BATCH, (
        f"a reply of {max(store._redis.raw.reply_sizes)} members was built at once"
    )


def test_it_takes_more_than_one_round_trip_for_a_large_window():
    """Otherwise the batching is not actually happening."""
    store = _store(5000)
    asyncio.run(store.get_recent(None, hours=48, limit=50))
    assert len(store._redis.raw.reply_sizes) > 1


def test_the_highest_scoring_events_are_returned_even_though_they_are_oldest():
    """The regression an early exit would introduce.

    Anomaly ascends with age here, so the top 50 by score are the 50 oldest
    members. A scan that stopped once it had 50 rows would return the 50
    newest instead and look entirely plausible.
    """
    store = _store(5000)
    rows = asyncio.run(store.get_recent(None, hours=48, limit=50))
    assert len(rows) == 50
    assert rows[0]["event_id"] == "e4999"
    assert rows[0]["anomaly_score"] == 1.0
    assert rows[-1]["anomaly_score"] > rows[0]["anomaly_score"] - 0.02


def test_the_whole_window_is_still_scanned():
    store = _store(5000)
    asyncio.run(store.get_recent(None, hours=48, limit=50))
    assert sum(store._redis.raw.reply_sizes) >= 5000


def test_filters_still_apply():
    store = _store(1000)
    rows = asyncio.run(store.get_recent(None, hours=48, min_anomaly=0.99, limit=500))
    assert rows and all(r["anomaly_score"] >= 0.99 for r in rows)

    store = _store(1000)
    rows = asyncio.run(store.get_recent(["options_flow"], hours=48, limit=50))
    assert rows == []

    store = _store(1000)
    rows = asyncio.run(store.get_recent(None, hours=48, tags=["nothing"], limit=50))
    assert rows == []


def test_an_empty_window_returns_empty_without_looping_forever():
    store = _store(0)
    assert asyncio.run(store.get_recent(None, hours=48, limit=50)) == []

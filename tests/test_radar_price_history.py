"""
tests/test_radar_price_history.py

"Hurst/GARCH Regime: Baseline Initialization", on every ticker, forever.

RadarAgent read its price history from one place: the Redis list
sentinel:candles:1h:{ticker}. That list is a hot cache appended once per
hour-bucket rollover, so it holds one entry per hour of continuous uptime for
that ticker -- measured across live equities, one or two. Twenty were required
before Hurst or GARCH would be computed, so the regime was never computed at
all, and every prompt reaching the model carried the placeholder instead of a
description of how the instrument had been behaving.

The history existed the whole time. tradfi_bars_1h held 68 hourly bars for the
same names, 70,009 bars across 306 tickers in total -- collected, aggregated
into a continuous aggregate, and never read.

The ordering was wrong too: lrange returns newest-first and simple_returns()
reads consecutive pairs as (previous, current), so every return the old path
produced had its sign inverted.
"""

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _source() -> str:
    return (ROOT / "services" / "agents" / "radar_agent.py").read_text(encoding="utf-8")


class _FakeRedisRaw:
    def __init__(self, entries):
        self._entries = entries

    async def lrange(self, key, start, end):
        return self._entries


class _FakeRedis:
    def __init__(self, entries):
        self.raw = _FakeRedisRaw(entries)


class _FakeDb:
    def __init__(self, rows):
        self._rows = rows
        self.queries = []

    async def query(self, sql, *args):
        self.queries.append((sql, args))
        return self._rows


class _Agent:
    """RadarAgent's history method, lifted onto a bare object.

    Constructing the real agent pulls in Kafka, Redis and an LLM client; the
    method under test needs none of them.
    """

    MIN_REGIME_BARS = 20

    def __init__(self, redis, db):
        self.redis = redis
        self.db = db
        import logging

        self.logger = logging.getLogger("test.radar")

    @property
    def _fetch_close_history(self):
        from services.agents.radar_agent import RadarAgent

        return RadarAgent._fetch_close_history.__get__(self, _Agent)


def _redis_bars(closes):
    """Newest-first, the way lrange returns them."""
    return [json.dumps({"close": c}) for c in reversed(closes)]


# -- the cache path -----------------------------------------------------------

@pytest.mark.anyio
async def test_a_warm_cache_is_used_without_touching_the_database():
    """A ticker seen for a day costs one Redis read, as before."""
    closes = [100.0 + i for i in range(25)]
    db = _FakeDb(rows=[])
    agent = _Agent(_FakeRedis(_redis_bars(closes)), db)

    got, _ = await agent._fetch_close_history("NVDA")

    assert got == closes
    assert db.queries == [], "the durable series was queried despite a warm cache"


@pytest.mark.anyio
async def test_closes_come_back_oldest_first():
    """simple_returns() reads (previous, current); newest-first inverts every
    return's sign."""
    closes = [10.0, 11.0, 12.0]
    agent = _Agent(_FakeRedis(_redis_bars(closes)), _FakeDb(rows=[]))

    assert (await agent._fetch_close_history("NVDA"))[0] == [10.0, 11.0, 12.0]


# -- the fallback that was missing --------------------------------------------

@pytest.mark.anyio
async def test_a_cold_cache_falls_back_to_the_durable_bars():
    """The measured failure: two cached bars against sixty-eight stored ones."""
    db = _FakeDb(rows=[{"close": 100.0 + i} for i in range(68)])
    agent = _Agent(_FakeRedis(_redis_bars([101.0, 102.0])), db)

    got, _ = await agent._fetch_close_history("NVDA")

    assert len(got) == 68
    assert len(db.queries) == 1
    assert "tradfi_bars_1h" in db.queries[0][0]


@pytest.mark.anyio
async def test_the_fallback_reaches_the_regime_threshold():
    """The whole point: twenty bars, so Hurst and GARCH actually run."""
    db = _FakeDb(rows=[{"close": 100.0 + i} for i in range(68)])
    agent = _Agent(_FakeRedis([]), db)

    assert len((await agent._fetch_close_history("NVDA"))[0]) >= _Agent.MIN_REGIME_BARS


@pytest.mark.anyio
async def test_the_durable_query_is_chronological():
    db = _FakeDb(rows=[{"close": 1.0}])
    agent = _Agent(_FakeRedis([]), db)
    await agent._fetch_close_history("NVDA")

    sql = db.queries[0][0]
    assert "ORDER BY bucket_time DESC LIMIT" in sql, "the newest bars are not selected"
    assert "ORDER BY bucket_time ASC" in sql, "the result is not re-sorted chronologically"


# -- failure is never fabricated ----------------------------------------------

@pytest.mark.anyio
async def test_a_shorter_durable_series_does_not_replace_a_longer_cache():
    """Falling back must not lose history."""
    closes = [100.0 + i for i in range(19)]
    db = _FakeDb(rows=[{"close": 5.0}])
    agent = _Agent(_FakeRedis(_redis_bars(closes)), db)

    assert (await agent._fetch_close_history("NVDA"))[0] == closes


@pytest.mark.anyio
async def test_no_database_means_whatever_the_cache_had():
    agent = _Agent(_FakeRedis(_redis_bars([101.0, 102.0])), None)
    assert (await agent._fetch_close_history("NVDA"))[0] == [101.0, 102.0]


@pytest.mark.anyio
async def test_a_failing_database_does_not_break_evaluation():
    """An unavailable series must degrade to "no regime", never to an error:
    the anomaly is still worth evaluating without it."""

    class _Boom:
        async def query(self, *a):
            raise RuntimeError("timescale down")

    agent = _Agent(_FakeRedis(_redis_bars([101.0])), _Boom())
    assert (await agent._fetch_close_history("NVDA"))[0] == [101.0]


@pytest.mark.anyio
async def test_malformed_cache_entries_are_skipped_not_fatal():
    entries = ["not json", json.dumps({"close": None}), json.dumps({"close": 42.0})]
    agent = _Agent(_FakeRedis(entries), _FakeDb(rows=[]))

    assert (await agent._fetch_close_history("NVDA"))[0] == [42.0]


# -- the threshold is named, not buried ---------------------------------------

def test_the_regime_threshold_is_a_named_constant():
    source = _source()
    assert "MIN_REGIME_BARS = 20" in source
    assert "len(closes) >= 20" not in source, "the threshold is inlined again"


# -- a regime must say what it was measured over ------------------------------

@pytest.mark.anyio
async def test_the_timeframe_is_returned_with_the_series():
    """Volatility over 5-minute bars is not volatility over hourly ones."""
    db = _FakeDb(rows=[{"close": 100.0 + i} for i in range(40)])
    agent = _Agent(_FakeRedis([]), db)

    closes, label = await agent._fetch_close_history("MSFT")
    assert len(closes) == 40
    assert label in ("1h", "15m", "5m")


def test_finer_timeframes_are_tried_when_the_hourly_series_is_thin():
    """Measured: MSFT held 8 hourly bars against the 20 a regime needs, while
    the 15-minute aggregate held 21 and the 5-minute 40."""
    source = _source()
    for table in ("tradfi_bars_1h", "tradfi_bars_15m", "tradfi_bars_5m"):
        assert table in source


def test_the_regime_string_states_its_timeframe():
    source = _source()
    assert "[over {len(closes)} {tf_label} bars]" in source


def test_insufficient_history_says_how_much_is_missing():
    """"Baseline Initialization" told a reader nothing about why."""
    source = _source()
    assert "insufficient history" in source
    # The assignment, not the phrase. Both this file and radar_agent.py quote
    # the old string in prose to explain what was wrong, and asserting on the
    # bare substring fails on the explanation rather than on the code -- which
    # it did, twice, before this was written properly.
    assert 'regime_str = "Hurst/GARCH Regime: Baseline Initialization"' not in source
    assert "{self.MIN_REGIME_BARS} needed" in source

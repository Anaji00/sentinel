"""The financial advisory must not depend on a cache that cannot hold its input.

_process_trading_advisory refuses to run on fewer than five bars, and
_fetch_prices read only the Redis candle cache. Measured across 572 tickers with
an hourly candle list: not one had five bars. The deepest was four.

The candle lists carry a TTL, which makes them evictable, and Redis was evicting
roughly forty-seven keys a second to make room for a structure that had no TTL
and so could not be evicted at all. With that stopped the cache accumulates --
but it still needs five hours of uninterrupted uptime to reach five hourly bars,
and a restart returns it to zero.

tradfi_bars_1h held 42 to 46 bars for those same tickers throughout, durably,
and nothing read it. So the entire financial advisory path had never produced an
output, on a cache structurally unable to hold enough history to let it.
"""

import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.agents.quant_trading_engine import (  # noqa: E402
    DURABLE_BAR_LIMIT, MIN_ADVISORY_BARS, QuantTradingEngine,
)


class _Raw:
    def __init__(self, bars):
        self.bars = bars

    async def lrange(self, key, a, b):
        return [json.dumps(x) for x in self.bars]


class _Redis:
    def __init__(self, bars):
        self.raw = _Raw(bars)


class _Db:
    def __init__(self, rows, fail=False):
        self.rows, self.fail, self.queries = rows, fail, []

    async def query(self, sql, *params):
        self.queries.append((sql, params))
        if self.fail:
            raise RuntimeError("connection reset")
        return self.rows


def _engine(cache_bars, db_rows, db_fail=False):
    e = QuantTradingEngine.__new__(QuantTradingEngine)
    e.redis = _Redis(cache_bars)
    e.db = _Db(db_rows, db_fail)
    import logging
    e.logger = logging.getLogger("test")
    return e


def _bar(c):
    return {"close": c, "high": c + 1, "low": c - 1}


def _row(c):
    return {"close": c, "high": c + 1, "low": c - 1}


def test_a_full_cache_is_used_without_touching_the_database():
    e = _engine([_bar(i) for i in range(10)], [])
    closes, _h, _l = asyncio.run(e._fetch_prices("AAPL"))
    assert len(closes) == 10
    assert e.db.queries == [], "the database was queried despite a sufficient cache"


def test_a_short_cache_falls_back_to_the_database():
    """The live case: four bars cached, forty-six in the table."""
    e = _engine([_bar(i) for i in range(4)], [_row(i) for i in range(46)])
    closes, _h, _l = asyncio.run(e._fetch_prices("AAPL"))
    assert len(closes) == 46
    assert e.db.queries, "no durable lookup was attempted"


def test_the_fallback_clears_the_advisory_minimum():
    e = _engine([_bar(i) for i in range(4)], [_row(i) for i in range(46)])
    closes, _h, _l = asyncio.run(e._fetch_prices("AAPL"))
    assert len(closes) >= MIN_ADVISORY_BARS


def test_the_durable_rows_come_back_oldest_first():
    """The cache is oldest-first and the indicators assume that ordering; the
    query is newest-first so it can LIMIT, and must be reversed."""
    e = _engine([], [_row(c) for c in (50, 40, 30, 20, 10)])
    closes, _h, _l = asyncio.run(e._fetch_prices("AAPL"))
    assert closes == [10, 20, 30, 40, 50]


def test_the_durable_query_is_bounded():
    e = _engine([], [_row(i) for i in range(3)])
    asyncio.run(e._fetch_prices("AAPL"))
    sql, params = e.db.queries[0]
    assert "LIMIT" in sql
    assert DURABLE_BAR_LIMIT in params


def test_the_ticker_is_matched_uppercase():
    e = _engine([], [_row(1)])
    asyncio.run(e._fetch_prices("aapl"))
    assert "AAPL" in e.db.queries[0][1]


def test_a_database_failure_leaves_the_cache_result_intact():
    """Degrade to what the cache had; never raise into the message loop."""
    e = _engine([_bar(i) for i in range(3)], [], db_fail=True)
    closes, _h, _l = asyncio.run(e._fetch_prices("AAPL"))
    assert len(closes) == 3


def test_nothing_anywhere_returns_empty_rather_than_raising():
    e = _engine([], [])
    closes, highs, lows = asyncio.run(e._fetch_prices("AAPL"))
    assert closes == [] and highs == [] and lows == []


def test_high_and_low_default_to_close_when_absent():
    e = _engine([], [{"close": 10.0, "high": None, "low": None}])
    closes, highs, lows = asyncio.run(e._fetch_prices("AAPL"))
    assert closes == [10.0] and highs == [10.0] and lows == [10.0]


def test_the_advisory_uses_the_same_minimum_constant():
    source = (ROOT / "services/agents/quant_trading_engine.py").read_text(encoding="utf-8")
    assert "if len(closes) < MIN_ADVISORY_BARS:" in source

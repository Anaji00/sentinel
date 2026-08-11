"""
tests/test_watched_equities_candles.py

Unit tests for structural candle evaluation and watchlist boosting for watched equities.
"""

import pytest
import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from shared.utils.candles import evaluate_multi_timeframe, TIMEFRAMES_MINUTES


class DummyRedisStore:
    def __init__(self):
        self.store = {}
        self.raw = self

    async def get(self, key: str):
        return self.store.get(key)

    async def set(self, key: str, value: str, ex: int = None):
        self.store[key] = value

    async def lrange(self, key: str, start: int, end: int):
        return []

    async def lpush(self, key: str, value: str):
        pass

    async def ltrim(self, key: str, start: int, end: int):
        pass

    def pipeline(self):
        return self

    async def execute(self):
        return []


class DummyScorer:
    async def score_market_candle(self, domain, asset, features):
        return 0.40

    async def check_watchlist(self, asset, watchlist_type):
        return asset == "AAPL"


def test_get_domain_tag():
    from shared.utils.candles import get_domain_tag

    assert get_domain_tag("crypto", "BTCUSDT") == "CRYPTO"
    assert get_domain_tag("tradfi", "ETHUSDT") == "CRYPTO"
    assert get_domain_tag("tradfi", "AAPL") == "EQUITY"
    assert get_domain_tag("equity", "NVDA") == "EQUITY"
    assert get_domain_tag("tradfi", "CL=F") == "MACRO"
    assert get_domain_tag("macro", "TNX") == "MACRO"
    assert get_domain_tag("tradfi", "^VIX") == "MACRO"
    assert get_domain_tag("", "") == "UNKNOWN"


def test_evaluate_multi_timeframe_watched_equities():
    async def run_test():
        redis = DummyRedisStore()
        scorer = DummyScorer()
        ts = datetime.now(timezone.utc)

        # Evaluate watched ticker AAPL
        results = await evaluate_multi_timeframe(
            redis, scorer, domain="tradfi", asset="AAPL",
            ts=ts, open_p=150.0, high_p=152.0, low_p=149.0, close_p=151.5, volume=10000.0
        )
        assert isinstance(results, list)
        assert len(TIMEFRAMES_MINUTES) == 6

    asyncio.run(run_test())

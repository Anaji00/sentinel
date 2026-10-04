"""
tests/test_minute_bar_flush_race.py

The minute-bar flush iterated a buffer the websocket kept writing to.

`OHLCVAggregator.flush` walked `self.buffer.items()` across
`await producer.send(...)`. A trade for a ticker new that minute raised
"dictionary changed size during iteration" -- three times in seventeen minutes
of a regular session on 1 October -- and the exception skipped the Redis
pipeline and the clear(). Bars already sent stayed buffered and went out again
next minute merged with a second minute of trades. Without a crash, trades that
arrived during the awaits were wiped unflushed by the clear().
"""

import asyncio
import importlib.util
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

TRADFI = ROOT / "services" / "collector-tradfi" / "main.py"


def _module():
    spec = importlib.util.spec_from_file_location("tradfi_main_for_flush_test", TRADFI)
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except SystemExit:
        pass
    return module


def _aggregator(on_send=None):
    m = _module()
    m._collector_metrics = MagicMock()
    producer = MagicMock()
    sent = []

    async def send(topic, payload, key=None):
        sent.append(key)
        if on_send:
            on_send(agg)

    producer.send = AsyncMock(side_effect=send)
    pipe = MagicMock()
    pipe.execute = AsyncMock()
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=pipe)
    ctx.__aexit__ = AsyncMock(return_value=False)
    redis = MagicMock()
    redis.raw.pipeline = MagicMock(return_value=ctx)
    agg = m.OHLCVAggregator(producer, redis)
    return agg, sent, pipe


def test_a_trade_for_a_new_ticker_during_the_flush_does_not_crash_it():
    def new_ticker_arrives(agg):
        agg.add_trade("NEWT", 10.0, 100)

    agg, sent, pipe = _aggregator(on_send=new_ticker_arrives)
    agg.add_trade("AAPL", 200.0, 50)
    agg.add_trade("MSFT", 400.0, 25)
    asyncio.run(agg.flush())
    assert sorted(sent) == ["AAPL", "MSFT"]
    pipe.execute.assert_awaited_once()


def test_a_trade_that_arrives_mid_flush_waits_for_the_next_bar():
    """Not wiped by the end-of-flush clear, and not merged into a sent bar."""
    def arrives(agg):
        agg.add_trade("AAPL", 201.0, 7)

    agg, sent, _ = _aggregator(on_send=arrives)
    agg.add_trade("AAPL", 200.0, 50)
    asyncio.run(agg.flush())
    assert agg.buffer["AAPL"]["V"] == 7
    assert agg.buffer["AAPL"]["O"] == 201.0


def test_a_sent_bar_is_not_sent_again():
    agg, sent, _ = _aggregator()
    agg.add_trade("AAPL", 200.0, 50)
    asyncio.run(agg.flush())
    asyncio.run(agg.flush())
    assert sent == ["AAPL"]

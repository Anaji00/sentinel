"""Live minute bars line up with vendor bars, keep their range, and say their session.

Flush-time stamps (09:29:14.385) never met the vendor's 09:29:00 bar for the
same minute, so both rows were kept and every rollup summed the minute twice;
vendor bars folded in as one trade lost their high and low; and bars arriving
without a session were all called REGULAR, at 06:59 and 22:01 ET alike.
"""

import asyncio
import importlib.util
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.market_session import session_label_at  # noqa: E402

TRADFI = ROOT / "services" / "collector-tradfi" / "main.py"


def _module():
    spec = importlib.util.spec_from_file_location("tradfi_main_for_alignment_test", TRADFI)
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except SystemExit:
        pass
    return module


M = _module()


def test_a_bar_is_stamped_at_the_minute_it_covers() -> None:
    flushed = datetime(2026, 10, 14, 13, 30, 0, 500000, tzinfo=timezone.utc)
    assert M.minute_bar_start(flushed) == datetime(2026, 10, 14, 13, 29, tzinfo=timezone.utc)
    assert abs(M.seconds_to_next_minute(datetime(2026, 10, 14, 13, 29, 14, 385000, tzinfo=timezone.utc)) - 45.615) < 1e-9


def _agg():
    M._collector_metrics = MagicMock()
    M.get_market_session = MagicMock(return_value=("REGULAR", ""))
    producer = MagicMock()
    sent = []

    async def send(topic, payload, key=None):
        sent.append(payload)

    producer.send = AsyncMock(side_effect=send)
    pipe = MagicMock()
    pipe.execute = AsyncMock()
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=pipe)
    ctx.__aexit__ = AsyncMock(return_value=False)
    redis = MagicMock()
    redis.raw.pipeline = MagicMock(return_value=ctx)
    return M.OHLCVAggregator(producer, redis), sent


def test_vendor_bars_keep_their_range_and_the_flush_stamps_the_bar_start() -> None:
    agg, sent = _agg()
    agg.add_bar("AMD", 570.0, 572.5, 569.0, 571.0, 1000)
    agg.add_bar("AMD", 571.0, 571.2, 568.5, 569.0, 500)
    start = datetime(2026, 10, 14, 13, 29, tzinfo=timezone.utc)
    asyncio.run(agg.flush(bar_start=start))
    bar = sent[0]["raw_payload"]
    assert (bar["open"], bar["high"], bar["low"], bar["close"], bar["volume"]) == (570.0, 572.5, 568.5, 569.0, 1500)
    assert sent[0]["occurred_at"] in (start, start.isoformat())


def test_a_bar_with_an_impossible_range_is_only_a_print_at_its_close() -> None:
    agg, _ = _agg()
    agg.add_bar("X", 10.0, 9.0, 11.0, 10.0, 5)     # high below low
    agg.add_bar("Y", None, 12.0, 10.0, 11.0, 5)    # no open
    assert agg.buffer["X"]["H"] == agg.buffer["X"]["L"] == 10.0
    assert agg.buffer["Y"]["O"] == agg.buffer["Y"]["H"] == 11.0


def test_sessions_come_from_the_bar_time() -> None:
    utc = lambda h, m: datetime(2026, 10, 14, h, m, tzinfo=timezone.utc)  # noqa: E731  (EDT = UTC-4)
    assert session_label_at("AAPL", utc(10, 59)) == "PRE_MARKET"      # 06:59 ET
    assert session_label_at("AAPL", utc(14, 0)) == "REGULAR"
    assert session_label_at("AAPL", utc(21, 30)) == "AFTER_HOURS"
    assert session_label_at("AAPL", utc(2, 1)) == "CLOSED"            # 22:01 ET the day before
    assert session_label_at("BTCUSD", utc(2, 1)) == "REGULAR"
    assert session_label_at("GC=F", utc(2, 1)) == "REGULAR"


def test_minute_history_loads_each_name_once_and_retries_a_failed_chunk(monkeypatch) -> None:
    import types
    calls, marked = [], set()

    async def fake_backfill(db, chunk, timeframe, lookback_days):
        calls.append(list(chunk))
        return {"errors": ["boom"]} if "BAD" in chunk else {"errors": []}

    fake_mod = types.ModuleType("historical_backfill")
    fake_mod.backfill_bars = fake_backfill
    monkeypatch.setitem(sys.modules, "historical_backfill", fake_mod)

    async def no_refresh(*a, **k):
        return None

    monkeypatch.setattr(M, "refresh_rollups", no_refresh)
    monkeypatch.setattr(M, "MINUTE_HISTORY_CHUNK", 2)

    class Raw:
        async def smembers(self, key):
            return {b"AAA"} | {x.encode() for x in marked}

        async def sadd(self, key, *names):
            marked.update(names)

    asyncio.run(M._load_minute_history(None, Raw(), ["AAA", "BBB", "BAD", "CCC"]))
    assert calls == [["BBB", "BAD"], ["CCC"]]          # AAA was loaded before
    assert marked == {"CCC"}                           # the failed chunk retries next pass


def test_a_rollup_refresh_shorter_than_two_buckets_is_left_to_the_policy() -> None:
    """The settle pass opens its window at 08:00 UTC; at 09:15 the hourly view's
    window was 08:00-08:15 and Timescale refused it, 28 times on 30 Oct 2026."""
    from datetime import timedelta
    calls = []

    class Db:
        async def execute_without_transaction(self, sql, params, timeout=None):
            calls.append(sql.split("'")[1])

    start = datetime.now(timezone.utc) - timedelta(minutes=75)
    asyncio.run(M.refresh_rollups(Db(), start, views=("tradfi_bars_5m", "tradfi_bars_15m", "tradfi_bars_30m",
                                                    "tradfi_bars_1h")))
    assert calls == ["tradfi_bars_5m", "tradfi_bars_15m"]

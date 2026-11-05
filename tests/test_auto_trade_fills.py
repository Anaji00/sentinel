"""Order outcomes are recorded after the session, read-only, and a failure parks rather than loops."""

import asyncio
import pathlib
import sys
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.agents import auto_trader as tr  # noqa: E402
from shared.utils import auto_trade as at  # noqa: E402

NY = ZoneInfo("America/New_York")


def test_outcomes_and_slippage_signs() -> None:
    assert at.fill_outcome(10, 10) == "filled"
    assert at.fill_outcome(10, 4) == "partial"
    assert at.fill_outcome(10, 0) == "unfilled"
    assert round(at.slippage_bps("buy", 101, 100), 6) == 100.0
    assert round(at.slippage_bps("sell", 99, 100), 6) == 100.0
    assert round(at.slippage_bps("sell", 101, 100), 6) == -100.0
    assert at.slippage_bps("buy", None, 100) is None and at.slippage_bps("buy", 100, None) is None


class _DB:
    def __init__(self) -> None:
        self.calls = []
        self.asked = None

    async def query(self, sql, *args):
        self.asked = args[0]
        return [{"id": 1, "order_id": "a", "qty": 2.0}, {"id": 2, "order_id": "b", "qty": 2.0}]

    async def execute(self, sql, *args):
        self.calls.append((sql, args))


class _Broker:
    paper = True

    async def get_order_status(self, order_id):
        return SimpleNamespace(filled_qty=2.0, filled_avg_price=5.0) if order_id == "a" else None

    async def submit_order(self, *a, **k):
        raise AssertionError("recording fills must never place an order")


def _run(now):
    db = _DB()
    trader = tr.AutoTrader(db, None, broker=_Broker())
    tr.FILL_GAP_SEC = 0
    return db, asyncio.run(trader.record_fills(now))


def test_records_read_only_and_parks_the_unreadable() -> None:
    db, stored = _run(datetime(2026, 11, 5, 17, 0, tzinfo=NY))
    assert stored == 1 and str(db.asked) == "2026-11-05"
    assert db.calls[0][1] == (1, 2.0, 5.0, "filled")
    assert db.calls[1][0] == tr._FILL_MISS_SQL and db.calls[1][1] == (2, at.FILL_MAX_CHECKS)


def test_today_waits_for_the_close() -> None:
    db, _ = _run(datetime(2026, 11, 5, 11, 0, tzinfo=NY))
    assert str(db.asked) == "2026-11-04"

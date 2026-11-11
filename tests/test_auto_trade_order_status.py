"""A lost response is not a rejection, and the same order cannot be sent twice in a day."""

import asyncio
import pathlib
import sys
from datetime import date
from types import SimpleNamespace

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.agents import auto_trader as tr  # noqa: E402
from shared.utils import auto_trade as at  # noqa: E402

DAY = date(2026, 11, 6)
PLAN = at.Plan("AAA", 2.0, "buy", "add")


class _DB:
    def __init__(self):
        self.logged = []

    async def execute(self, sql, *args):
        self.logged.append(args)

    async def query(self, sql, *args):
        return []


class _Broker:
    paper = True

    def __init__(self, error=None, found=None):
        self.error, self.found, self.ids = error, found, []

    async def submit_order(self, *a, client_order_id=None, **k):
        self.ids.append(client_order_id)
        if self.error:
            raise self.error
        return SimpleNamespace(order_id="ok")

    async def get_order_by_client_id(self, cid):
        return self.found


def _send(broker):
    db = _DB()
    ok = asyncio.run(tr.AutoTrader(db, None, broker=broker)._send(DAY, PLAN, None))
    return ok, db.logged[-1]


def test_the_id_is_stable_for_the_same_order() -> None:
    a = at.client_order_id("b1_extremes", DAY, "AAA", "buy", "add")
    assert a == at.client_order_id("b1_extremes", DAY, "AAA", "buy", "add") and a != at.client_order_id("b1_extremes", DAY, "AAA", "buy", "trim")


def test_failures_are_classified() -> None:
    assert at.order_failure_status(TimeoutError()) == "unknown"
    assert at.order_failure_status(RuntimeError("Alpaca submit_order failed (403): insufficient qty")) == "rejected"
    assert at.order_failure_status(RuntimeError('Alpaca submit_order failed (422): {"message":"client_order_id must be unique"}')) == "duplicate"


def test_a_timeout_found_at_the_venue_is_submitted() -> None:
    broker = _Broker(TimeoutError(), SimpleNamespace(order_id="live"))
    ok, row = _send(broker)
    assert ok and row[6] == "live" and row[7] == "submitted" and row[-1] == broker.ids[0]


def test_a_timeout_not_found_is_unknown_not_rejected() -> None:
    ok, row = _send(_Broker(TimeoutError(), None))
    assert not ok and row[6] is None and row[7] == "unknown"


def test_a_real_refusal_is_rejected_and_a_duplicate_is_not_a_failure() -> None:
    ok, row = _send(_Broker(RuntimeError("Alpaca submit_order failed (403): no")))
    assert not ok and row[7] == "rejected"
    ok, row = _send(_Broker(RuntimeError('Alpaca submit_order failed (422): client_order_id must be unique')))
    assert ok and row[7] == "duplicate" and row[6] is None

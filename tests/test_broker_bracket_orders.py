"""Pins the bracket-order contract that live execution depends on.

A protective stop and a profit target are legs of a bracket, not fields on a
plain order. The route sent limit_price together with stop_price, which Alpaca
rejects outright (422 "limit orders require no stop price"), so every protected
order failed against the live venue while passing on the simulator. Worse,
target_price was echoed back in the API response as though it were part of the
executed order although no take-profit leg was ever transmitted -- a filled
position had no exit.
"""
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.broker.alpaca import AlpacaBroker  # noqa: E402
from shared.broker.base import OrderSide, OrderType  # noqa: E402


class _CapturedPayload(Exception):
    """Aborts the request once the payload is built, so no network call happens."""

    def __init__(self, payload):
        self.payload = payload


@pytest.fixture
def capture(monkeypatch):
    """Intercepts the outbound payload without contacting the venue."""
    def _run(**kwargs):
        broker = AlpacaBroker(api_key="k", secret_key="s", paper=True)

        class FakeSession:
            def __init__(self, *a, **kw): pass
            async def __aenter__(self): return self
            async def __aexit__(self, *a): return False
            def post(self, url, json=None, timeout=None):
                raise _CapturedPayload(json)

        monkeypatch.setattr("shared.broker.alpaca.aiohttp.ClientSession", FakeSession)

        import asyncio
        try:
            # asyncio.run, not get_event_loop: by the time the full suite reaches
            # this test another module has already closed the thread's loop.
            asyncio.run(broker.submit_order(**kwargs))
        except _CapturedPayload as e:
            return e.payload
        raise AssertionError("payload was never built")
    return _run


BASE = dict(symbol="NVDA", qty=45.0, side=OrderSide.BUY,
            order_type=OrderType.LIMIT, limit_price=220.0)


def test_stop_and_target_become_a_bracket(capture):
    p = capture(**BASE, stop_price=210.0, take_profit_price=245.0)
    assert p["order_class"] == "bracket"
    assert p["take_profit"] == {"limit_price": "245.0"}
    assert p["stop_loss"] == {"stop_price": "210.0"}


def test_bracket_never_sends_a_bare_stop_price(capture):
    """The exact shape the venue rejected with 422."""
    p = capture(**BASE, stop_price=210.0, take_profit_price=245.0)
    assert "stop_price" not in p, "a bare stop_price alongside limit_price is a 422"


def test_stop_without_target_stays_a_plain_order(capture):
    p = capture(**BASE, stop_price=210.0)
    assert "order_class" not in p
    assert p["stop_price"] == "210.0"


def test_bracket_quantity_is_whole_shares(capture):
    """Alpaca rejects bracket and OCO orders on fractional quantities."""
    p = capture(**{**BASE, "qty": 45.45}, stop_price=210.0, take_profit_price=245.0)
    assert p["qty"] == "45"


def test_sub_share_bracket_is_refused_not_silently_rounded_to_zero():
    import asyncio
    broker = AlpacaBroker(api_key="k", secret_key="s", paper=True)
    with pytest.raises(ValueError, match="at least 1 whole share"):
        asyncio.run(broker.submit_order(
            symbol="BRK.A", qty=0.4, side=OrderSide.BUY,
            order_type=OrderType.LIMIT, limit_price=700000.0,
            stop_price=690000.0, take_profit_price=720000.0))


def test_plain_order_quantity_is_untouched(capture):
    """Fractional shares remain valid when no bracket is involved."""
    p = capture(**{**BASE, "qty": 45.45})
    assert p["qty"] == "45.45"

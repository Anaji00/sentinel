"""A quote is one print, not a minute bar spanning the day.

Finnhub's quote carries the day's open, high and low; published as an
OHLCV_MINUTE_BAR they gave CL=F a 153.37-158.10 "minute" on 15 Oct 2026, and
every 5m candle built on those rows spanned the whole session.
"""

import asyncio
import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

MACRO = ROOT / "services" / "collector-macro" / "main.py"


def _module():
    spec = importlib.util.spec_from_file_location("macro_main_for_print_test", MACRO)
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except SystemExit:
        pass
    return module


class _Resp:
    status = 200

    async def json(self):
        return {"c": 158.06, "o": 153.365, "h": 158.1, "l": 153.365, "pc": 152.0}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class _Session:
    def get(self, url, timeout=None):
        return _Resp()


def test_a_quote_is_a_print_with_the_day_range_kept_aside() -> None:
    m = _module()
    m.FINNHUB_API_KEY = "test"
    q = asyncio.run(m.fetch_finnhub_quote(_Session(), "CL=F"))
    assert q["open"] == q["high"] == q["low"] == q["close"] == 158.06
    assert (q["day_open"], q["day_high"], q["day_low"]) == (153.365, 158.1, 153.365)


def test_a_real_future_is_its_last_finished_minute_and_a_stale_one_is_nothing() -> None:
    m = _module()
    chart = {"chart": {"result": [{"timestamp": [1000, 1060, 1120],
                                   "indicators": {"quote": [{"open": [96.5, 96.6, 96.7], "high": [96.7, 96.8, 96.9],
                                                             "low": [96.4, 96.5, 96.6], "close": [96.6, 96.7, 96.8],
                                                             "volume": [1200, 900, 50]}]}}]}}
    bar = m.last_complete_bar(chart, now_ts=1150)
    assert bar == {"close": 96.7, "open": 96.6, "high": 96.8, "low": 96.5, "volume": 900.0, "bar_time": 1060}
    assert m.last_complete_bar(chart, now_ts=1060 + 31 * 60) == {}      # market closed
    assert m.last_complete_bar({"chart": {"result": []}}, now_ts=0) == {}


def test_real_futures_are_off_unless_switched_on(monkeypatch) -> None:
    """Off by default: on before the proxy history moves, the first real crude
    print reads as a -37% move. Switched on in .env once it has moved."""
    # Set, not deleted: the module loads .env on import, which never
    # overrides a variable already in the environment.
    monkeypatch.setenv("MACRO_REAL_FUTURES", "0")
    assert _module().REAL_FUTURES is False
    monkeypatch.setenv("MACRO_REAL_FUTURES", "1")
    assert _module().REAL_FUTURES is True

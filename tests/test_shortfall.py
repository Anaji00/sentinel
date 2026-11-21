"""Quant report T9: shortfall measurement (shared/utils/shortfall.py). Measurement only."""
from datetime import date, datetime, time, timezone

from shared.utils import shortfall as sf

DAY = date(2026, 11, 18)


def _bar(h, m, close, vol):                       # New York time in November is UTC-5
    return (datetime(2026, 11, 18, h + 5, m, tzinfo=timezone.utc), close, vol)


BARS = [_bar(9, 30, 100.0, 1000), _bar(9, 40, 100.5, 1000), _bar(10, 30, 101.0, 3000), _bar(15, 59, 102.0, 1000)]


def test_cost_is_positive_when_the_fill_is_worse_for_the_side() -> None:
    assert sf.bps(101.0, 100.0, "buy") == 100.0
    assert sf.bps(101.0, 100.0, "sell") == -100.0
    assert sf.bps(99.0, 100.0, "sell") == 100.0
    assert sf.bps(0, 100.0, "buy") is None and sf.bps(100.0, None, "buy") is None and sf.bps(100.0, 100.0, "hold") is None


def test_reference_prices_and_vwap() -> None:
    assert sf.price_at(BARS, DAY, time(9, 40)) == 100.5
    assert sf.price_at(BARS, DAY, time(10, 35)) == 101.0
    assert sf.price_at(BARS, DAY, time(9, 20)) is None                          # nothing at or before it
    assert sf.price_at([_bar(9, 30, 100.0, 1)], DAY, time(10, 30)) is None      # the last bar is over ten minutes old
    assert abs(sf.day_vwap(BARS, DAY) - (100 * 1000 + 100.5 * 1000 + 101 * 3000 + 102 * 1000) / 6000) < 1e-9


def test_rows_and_summary() -> None:
    rows = sf.rows_for([{"symbol": "AAA", "side": "buy", "reason": "rebuild", "filled_qty": 10, "fill_price": 101.0},
                        {"symbol": "ZZZ", "side": "sell", "reason": "trim", "filled_qty": 5, "fill_price": 50.0}],
                       {"AAA": BARS}, {"AAA": 100.0}, DAY)
    assert rows[0]["shortfall_prior_close_bps"] == 100.0 and rows[0]["shortfall_ref_1030_bps"] == 0.0
    assert rows[1]["shortfall_vwap_bps"] is None
    s = sf.summarize(rows)
    assert s["orders"] == 2 and s["prior_close"]["n"] == 1 and s["prior_close"]["weighted_mean_bps"] == 100.0 and s["vwap"]["n"] == 1

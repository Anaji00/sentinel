"""
tests/test_financial_correctness.py

Financial figures that were wrong by construction.

Three defects, all silent -- each produced a number, none raised:

  1. periods_per_year() lower-cased the timeframe before comparing. "1M" is a
     month and "1m" is a minute, so a monthly series annualized as if sampled
     every minute: 98,280 periods per year instead of 12, an 8,190x error that
     inflates a Sharpe ratio by about 90x. "1M" is live -- the radar route
     accepts it and reads it from tradfi_bars_1mth. shared/utils/candles.py
     already guarded this; quant_calc never did.

  2. QuantTradingEngine computed Sharpe two ways from the same hourly series.
     One call passed the correct factor for PRICE_TIMEFRAME; the other took the
     default trading_days=252 -- the daily equity convention -- understating the
     result by sqrt(1638/252) = 2.55x for equities and sqrt(8760/252) = 5.9x for
     crypto, and publishing it as quality_metrics.sharpe_ratio.

  3. `np.diff(closes) / closes[:-1]` divides by the previous close with no
     guard, and the collectors substitute 0.0 for a bar with no close price. One
     malformed bar produced inf or nan, which propagated through volatility,
     Sharpe, VaR and Kelly without any error.
"""

import math
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.quant_calc import (  # noqa: E402
    classify_asset_class,
    periods_per_year,
    sharpe_ratio,
    simple_returns,
)


# ── annualization ────────────────────────────────────────────────────────────

def test_a_month_is_not_a_minute():
    assert periods_per_year("1M") == 12.0
    assert periods_per_year("1m") > 90_000


def test_every_monthly_spelling_agrees():
    for spelling in ("1M", "1mo", "1MO", "1month", "monthly"):
        assert periods_per_year(spelling) == 12.0, spelling


def test_the_radar_route_still_accepts_1M():
    """If this timeframe is dropped the guard above stops being load-bearing."""
    src = (ROOT / "services/api_gateway/routes/radar.py").read_text(encoding="utf-8")
    assert '"1M"' in src


def test_intraday_is_annualized_on_its_own_calendar():
    """Crypto trades continuously; equities do not."""
    assert periods_per_year("1h", "crypto") > periods_per_year("1h", "equity")
    assert periods_per_year("1d", "crypto") == 365.0
    assert periods_per_year("1d", "equity") == 252.0


@pytest.mark.parametrize("symbol,expected", [
    ("BCHUSDT", "crypto"), ("BTC-USD", "crypto"), ("ETH", "crypto"),
    ("NVDA", "equity"), ("BRK.B", "equity"), ("EURUSD", "fx"),
])
def test_asset_class_drives_the_calendar(symbol, expected):
    assert classify_asset_class(symbol) == expected


# ── Sharpe on the frequency actually sampled ─────────────────────────────────

def test_the_engine_never_annualizes_hourly_bars_as_daily():
    src = (ROOT / "services/agents/quant_trading_engine.py").read_text(encoding="utf-8")
    code = re.sub(r"#.*$", "", src, flags=re.M)
    assert "quant_calc.sharpe_ratio(returns)" not in code, (
        "a bare call takes the 252 default on a PRICE_TIMEFRAME series"
    )
    for call in re.findall(r"quant_calc\.sharpe_ratio\([^)]*\)", code):
        assert "trading_days=" in call, f"unqualified annualization: {call}"


def test_the_two_conventions_differ_enough_to_matter():
    """2.55x for equities, 5.9x for crypto -- the figures the docstring cites."""
    returns = [0.001, -0.002, 0.003, 0.0005, -0.001] * 8
    daily = sharpe_ratio(returns, annualize=True, trading_days=252)
    hourly = sharpe_ratio(returns, annualize=True, trading_days=periods_per_year("1h", "equity"))
    assert abs(hourly) > abs(daily) * 2


# ── returns that cannot be produced are not invented ─────────────────────────

def test_a_zero_base_price_yields_no_return_rather_than_infinity():
    out = simple_returns([0.0, 50.0, 55.0])
    assert out == [pytest.approx(0.1)]
    assert all(math.isfinite(x) for x in out)


def test_a_series_of_zeros_produces_nothing():
    assert simple_returns([0.0, 0.0, 0.0]) == []


def test_ordinary_returns_are_unchanged():
    assert simple_returns([100, 101, 102]) == [pytest.approx(0.01), pytest.approx(0.00990099)]


def test_junk_and_short_series_do_not_raise():
    for series in ([], [100], [100, "x", 102], [None, 5]):
        assert simple_returns(series) == [] or all(math.isfinite(v) for v in simple_returns(series))


def test_no_unguarded_return_idiom_remains():
    """The pattern that started this: np.diff(closes) / closes[:-1]."""
    offenders = []
    for path in list((ROOT / "services").rglob("*.py")) + list((ROOT / "shared").rglob("*.py")):
        if "__pycache__" in str(path):
            continue
        code = path.read_text(encoding="utf-8")
        # Docstrings as well as comments: the replacement helper documents the
        # very idiom it exists to remove, and matching prose is not a defect.
        triple = chr(34) * 3
        code = re.sub(triple + '.*?' + triple, '', code, flags=re.S)
        code = re.sub(r"#.*$", "", code, flags=re.M)
        if re.search(r"np\.diff\(\s*\w+\s*\)\s*/\s*\w+\[:-1\]", code):
            offenders.append(str(path.relative_to(ROOT)))
    assert not offenders, f"unguarded return division in: {offenders}"


# ── paired series must stay paired ───────────────────────────────────────────

def test_amihud_inputs_are_built_together():
    """amihud_illiquidity answers 0.0 on a length mismatch, losing the metric."""
    src = (ROOT / "services/enrichment/enrichers/crypto.py").read_text(encoding="utf-8")
    block = src[src.index("returns, valid_notionals = [], []"):]
    assert "returns.append" in block[:600] and "valid_notionals.append" in block[:600]


def test_the_backtest_benchmark_survives_a_zero_open():
    src = (ROOT / "services/reasoning/strategy_backtester.py").read_text(encoding="utf-8")
    assert "if closes and closes[0] > 0 else 0.0" in src

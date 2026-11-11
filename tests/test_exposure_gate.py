"""A macro-themed alert on a stock needs a measured exposure to that theme (shared/utils/exposure_gate.py)."""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import exposure_gate as eg  # noqa: E402

TABLE = eg.build_table([
    {"ticker": "XOM", "macro": "CL=F", "partial_corr": 0.41},
    {"ticker": "AAPL", "macro": "US10Y", "partial_corr": -0.12},
    {"ticker": "KRE", "macro": "US2Y", "partial_corr": -0.33},
    {"ticker": "BAD", "macro": "CL=F", "partial_corr": float("nan")},
])


def test_a_stock_with_a_measured_oil_exposure_passes_and_nvda_does_not() -> None:
    assert eg.passes("Oil Price Spike Leads to Energy Stock Selloff", "XOM", TABLE)[0] is True
    ok, why = eg.passes("Oil Price Spike Leads to Energy Stock Selloff", "NVDA", TABLE)
    assert ok is False and "NVDA" in why and "oil" in why


def test_the_exposure_must_be_to_the_named_theme_and_strong_enough() -> None:
    assert eg.passes("Yield Drop", "XOM", TABLE)[0] is False               # exposed to oil, not yields
    assert eg.passes("Yield Drop", "AAPL", TABLE)[0] is False              # 0.12 is under the bar
    assert eg.passes("Yield Drop", "KRE", TABLE)[0] is True                # sign does not matter, size does


def test_instruments_and_unthemed_names_pass_untouched() -> None:
    assert eg.passes("Oil Price Spike", "CL=F", TABLE)[0] is True
    assert eg.passes("Oil Price Spike", "BTCUSDT", TABLE)[0] is True
    assert eg.passes("Options Print Convergence", "NVDA", TABLE)[0] is True


def test_unusable_rows_are_left_out_of_the_table() -> None:
    assert "BAD" not in eg.build_table([{"ticker": "BAD", "macro": "CL=F", "partial_corr": float("nan")}])
    assert eg.build_table([{"ticker": "X", "macro": "CL=F"}]) == {}


def test_the_correlation_engine_gates_before_it_dedups_and_counts_the_drop() -> None:
    src = (ROOT / "services/correlation/main.py").read_text(encoding="utf-8")
    assert src.index("exposure_gate.passes(") < src.index("alert_dedup.admit(")
    assert "correlation.macro_exposure_gate" in src and "correlation.exposure_table" in src

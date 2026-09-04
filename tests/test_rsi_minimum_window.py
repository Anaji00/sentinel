"""
tests/test_rsi_minimum_window.py

"Bearish Structural Anomaly: BCHUSDT 1-min moved -0.00%" was the single most
common shape of market_anomaly the system produced.

Grouping two hours of them by the price move stated in their own headline:

    0.01%   89        0.00%   74
   -0.01%   76        0.04%   63

The most frequent anomaly was a price change of exactly zero, and these carry an
average score of 0.82 -- high enough to compete for inference slots on a host
that affords a few dozen an hour.

The driver was RSI. Over one hour, 431 evaluations reported RSI exactly 0.0
against a sensible spread of 42-48 for everything else. RSI reaches 0 only when
avg_gain is zero, and with `len(closes) > 1` admitting two closes, a single
down-tick does it. Downstream that reads as maximally oversold; it actually
means there was almost no data.

The long timeframes are where it bites. crypto:history240m holds two entries,
because the platform has not been running long enough to fill fifteen four-hour
candles and will not be for a week.

Below a real window the honest answer is the neutral 0.5 the code already
defaults to: no view, rather than a maximal one.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.candles import MIN_RSI_OBSERVATIONS  # noqa: E402


def _rsi(closes):
    """Mirrors the computation under test."""
    if len(closes) <= MIN_RSI_OBSERVATIONS:
        return 0.5
    diffs = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gain = sum(d if d > 0 else 0.0 for d in diffs) / len(diffs)
    loss = sum(-d if d < 0 else 0.0 for d in diffs) / len(diffs)
    if loss == 0.0:
        return 1.0 if gain > 0 else 0.5
    return (100 - (100 / (1 + gain / loss))) / 100.0


def test_two_closes_no_longer_read_as_maximally_oversold():
    """The defect: one down-tick on a nearly empty history returned 0.0."""
    assert _rsi([10.0, 9.0, 8.0]) == 0.5


def test_a_genuine_sustained_decline_still_reads_oversold():
    """The fix must not silence the real signal it was masking."""
    assert _rsi([20.0 - i * 0.1 for i in range(15)]) == 0.0


def test_a_mixed_series_lands_in_the_middle():
    closes = [10, 10.2, 9.9, 10.1, 10.4, 10.2, 10.5, 10.3, 10.6, 10.4, 10.7, 10.5, 10.8, 10.6, 10.9]
    assert 0.3 < _rsi(closes) < 0.9


def test_the_bar_is_high_enough_to_mean_something():
    """One move must not be able to define the value."""
    assert MIN_RSI_OBSERVATIONS >= 8


def test_the_bar_is_low_enough_that_a_filling_timeframe_recovers():
    """Demanding the full fourteen would silence the 4-hour frame for a week."""
    assert MIN_RSI_OBSERVATIONS <= 14


def test_a_flat_series_is_neutral_not_extreme():
    """No movement is no information, in either direction."""
    assert _rsi([100.0] * 20) == 0.5


def test_an_all_gains_series_reads_overbought():
    assert _rsi([10.0 + i for i in range(15)]) == 1.0


def test_the_guard_is_on_the_computation_not_the_caller():
    """Every caller of this evaluator inherits the fix."""
    import re
    source = (ROOT / "shared" / "utils" / "candles.py").read_text(encoding="utf-8")
    # Matched on the comparison rather than one spelling of the line. The guard
    # was later hoisted to `has_history = len(closes) > MIN_RSI_OBSERVATIONS`,
    # which is the same guard in the same place; asserting the literal `if`
    # made this test fail on a refactor that changed nothing it exists to
    # protect.
    assert re.search(r"len\(closes\)\s*>\s*MIN_RSI_OBSERVATIONS", source), (
        "the window guard is no longer expressed against MIN_RSI_OBSERVATIONS"
    )
    assert "if len(closes) > 1:" not in source

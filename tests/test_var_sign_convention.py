"""VaR is a loss, not a magnitude.

var_historical returned abs(cutoff). When the cutoff return is negative the two
agree, which is why this survived -- but when the worst 5% of outcomes is still
a gain, abs() reports that gain as a loss. A series returning +1% every period
came back with a 95% VaR of 0.01, describing a loss that cannot occur.

var_parametric on the same input returns max(0.0, var) and is correct, so the
two implementations disagreed on the same series. The historical one is the
non-parametric path the quant engine uses, and it feeds position sizing.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.utils.quant_calc import (  # noqa: E402
    cvar_historical, var_historical, var_parametric,
)

GAINS = [0.01] * 12
LOSSES = [-0.02] * 12
MIXED = [0.01, -0.02, 0.03, -0.01, 0.005, -0.03, 0.02, -0.015, 0.01, -0.005, 0.012, -0.008]


def test_a_series_that_never_loses_has_no_value_at_risk():
    """The defect: this returned 0.01, the size of the smallest gain."""
    assert var_historical(GAINS) == 0.0
    assert cvar_historical(GAINS) == 0.0


def test_the_two_implementations_agree_on_a_riskless_series():
    assert var_historical(GAINS) == var_parametric(GAINS)


def test_the_two_implementations_agree_on_a_uniformly_losing_series():
    assert var_historical(LOSSES) == var_parametric(LOSSES) == 0.02


def test_a_loss_is_reported_as_a_positive_number():
    """The contract the callers assume: VaR is a positive loss magnitude."""
    assert var_historical(MIXED) > 0
    assert cvar_historical(MIXED) > 0


def test_the_tail_is_never_less_severe_than_the_threshold():
    for series in (MIXED, LOSSES, GAINS):
        assert cvar_historical(series) >= var_historical(series), series[:3]


def test_var_scales_with_position_value():
    one = var_historical(MIXED, position_value=1.0)
    hundred = var_historical(MIXED, position_value=100.0)
    assert abs(hundred - one * 100.0) < 0.01


def test_a_short_series_declines_to_answer():
    """Nine points cannot support a 95% tail estimate."""
    assert var_historical([-0.05] * 9) == 0.0
    assert cvar_historical([-0.05] * 9) == 0.0


def test_higher_confidence_never_reports_less_risk():
    assert var_historical(MIXED, confidence=0.99) >= var_historical(MIXED, confidence=0.90)

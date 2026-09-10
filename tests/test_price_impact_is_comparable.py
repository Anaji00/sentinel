"""Kyle's lambda is in price per share, so it cannot be thresholded absolutely.

The regression itself is correctly specified and correctly named. Its units are
the problem: impact of trading Q shares is lambda*Q dollars, so lambda scales
with the square of the instrument's price for a fixed fractional impact. Two
names with an identical real illiquidity -- a $1M order moving each 50 basis
points -- carry slopes of 0.00125 and 0.0000005.

`microstructure_stop_distance` thresholded that raw slope at 1.0 and 2.0 and
used the result to decide how tight a stop is. Those numbers are unreachable for
any realistically-priced instrument, so the illiquidity term had never once
fired; and had they been reachable, the same threshold would have meant opposite
things at opposite ends of the universe.
"""
import numpy as np
import pytest

from shared.utils.quant_calc import (
    IMPACT_BPS_ILLIQUID,
    IMPACT_BPS_THIN,
    kyle_impact_bps,
    kyle_lambda,
    microstructure_stop_distance,
)


def _series(price, impact_frac_per_million, n=40, seed=3):
    """A synthetic book with a known fractional impact per $1M of flow."""
    rng = np.random.default_rng(seed)
    lam = impact_frac_per_million * price ** 2 / 1e6
    sv = list(rng.normal(0, 1000, n))
    return [lam * v for v in sv], sv


def test_the_raw_slope_ranks_by_price_level():
    """This is the defect, stated as a measurement."""
    dp_hi, sv_hi = _series(500.0, 0.005)
    dp_lo, sv_lo = _series(5.0, 0.005)
    lam_hi = kyle_lambda(dp_hi, sv_hi)
    lam_lo = kyle_lambda(dp_lo, sv_lo)
    # Identical real illiquidity, four orders of magnitude apart.
    assert lam_hi > lam_lo * 1000


def test_the_comparable_form_does_not():
    dp_hi, sv_hi = _series(500.0, 0.005)
    dp_lo, sv_lo = _series(5.0, 0.005)
    bps_hi = kyle_impact_bps(dp_hi, sv_hi, reference_price=500.0)
    bps_lo = kyle_impact_bps(dp_lo, sv_lo, reference_price=5.0)
    assert bps_hi == pytest.approx(50.0, rel=0.1)
    assert bps_lo == pytest.approx(50.0, rel=0.1)


def test_the_old_thresholds_were_unreachable():
    """A raw lambda above 1.0 implies an absurd instrument."""
    for price in (5.0, 50.0, 500.0):
        # What fractional impact per $1M would a lambda of 1.0 correspond to?
        implied = 1.0 * 1e6 / price ** 2
        assert implied > 1.0, (
            f"at ${price} a lambda of 1.0 means a {implied:.0%} move per $1M -- "
            "which is why the stop guard's illiquidity branch never fired."
        )


def test_a_liquid_name_does_not_tighten_the_stop():
    assert microstructure_stop_distance(atr=2.5, ofi=0.1, impact_bps=5.0) == 1.5


def test_a_thin_name_does():
    thin = microstructure_stop_distance(atr=2.5, ofi=0.1, impact_bps=IMPACT_BPS_THIN + 1)
    illiquid = microstructure_stop_distance(atr=2.5, ofi=0.1, impact_bps=IMPACT_BPS_ILLIQUID + 1)
    assert illiquid < thin < 1.5


def test_impact_is_never_negative():
    """Buying does not push a price down; a negative slope reads as zero."""
    rng = np.random.default_rng(5)
    sv = list(rng.normal(0, 1000, 40))
    dp = [-0.001 * v for v in sv]           # deliberately inverted
    assert kyle_lambda(dp, sv) == 0.0
    assert kyle_impact_bps(dp, sv, reference_price=100.0) == 0.0


def test_a_missing_price_is_not_an_infinite_impact():
    dp, sv = _series(100.0, 0.005)
    assert kyle_impact_bps(dp, sv, reference_price=0.0) == 0.0
    assert kyle_impact_bps(dp, sv, reference_price=-1.0) == 0.0

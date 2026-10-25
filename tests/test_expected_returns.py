"""How far, not only which way: expected returns, volatility, sizing."""

import math
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import expected_returns as er  # noqa: E402


def _walk(n, daily_vol, seed=1):
    import random
    rng = random.Random(seed)
    px = [100.0]
    for _ in range(n):
        px.append(px[-1] * math.exp(rng.gauss(0, daily_vol)))
    return px


def test_alpha_scales_with_skill_volatility_and_score() -> None:
    z_top = er.z_of(0.95)
    assert abs(z_top - 1.645) < 0.01 and er.z_of(0.5) == 0.0
    a = er.alpha(0.06, 0.40, z_top, 21)
    assert abs(a - 0.06 * 0.40 * math.sqrt(21 / 252) * z_top) < 1e-12 and a > 0
    assert er.calibrated(a, "monthly", (1.0, 0.0)) == a
    line = {"slope": 2.0, "intercept": -0.01}
    assert abs(er.calibrated(a, "monthly", line) - (2.0 * a - 0.005)) < 1e-12
    assert er.alpha(0.06, 0.40, -z_top, 21) == -a
    assert er.with_events(a, [100.0]) == a + 0.01


def test_the_volatility_forecast_adds_the_earnings_jump_and_leans_to_implied() -> None:
    px = _walk(300, 0.02)                                      # about 32% a year
    har = er.har_vol(px)
    assert 0.25 < har < 0.40
    plain = er.forecast_vol(px, None, False)
    with_jump = er.forecast_vol(px, None, True, 0.08, 21)
    assert with_jump > plain * 1.3                              # an 8% earnings move inside a month
    leaned = er.forecast_vol(px, 0.80, False)
    assert plain < leaned < 0.80                                # pulled toward implied less its premium


def test_probability_and_half_kelly_size() -> None:
    assert abs(er.prob_beats(0.0, 0.3, 21) - 0.5) < 1e-9
    assert er.prob_beats(0.03, 0.3, 21) > 0.6
    w = er.kelly_stock(0.03, 0.30, 21, cost=0.002)
    assert 0 < w <= 0.05
    assert er.kelly_stock(0.001, 0.30, 21, cost=0.002) == 0.0  # no edge after costs, no position


def test_calibration_recovers_a_known_slope_and_offset() -> None:
    import random
    rng = random.Random(3)
    raw = [rng.gauss(0, 0.02) for _ in range(5000)]
    real = [2.0 * a - 0.01 + rng.gauss(0, 0.05) for a in raw]
    cal = er.calibration(raw, real)
    assert abs(cal["slope"] - 2.0) < 0.15 and abs(cal["intercept"] + 0.01) < 0.003
    assert cal["deciles_bps"][0][1] < cal["deciles_bps"][-1][1]


def test_the_measured_map_saturates_long_tails_and_keeps_short_ones() -> None:
    # Monthly defaults: a raw alpha far past the top bucket is held at what the
    # top 1% realised (+232.5 bps), plus half the shortfall to SPY taken back.
    top = er.calibrated(0.0435, "monthly")
    assert abs(top - (0.02325 + 0.5 * 0.0111)) < 1e-9
    assert top < 1.809 * 0.0435 - 0.5 * 0.0111                # well under the line's +732
    assert er.calibrated(-0.06, "monthly") < -0.14            # the short tail is not shrunk
    xs = [er.calibrated(x / 1e4, "monthly") for x in range(-600, 400, 10)]
    assert all(b >= a for a, b in zip(xs, xs[1:]))            # monotone
    pts = er.monotone([[0, 5, 1], [1, 3, 1], [2, 4, 1], [3, 10, 1]])
    assert [y for _x, y in pts] == [4.0, 4.0, 4.0, 10.0]


def test_the_lab_measures_knots_over_fixed_percentile_buckets() -> None:
    import random
    rng = random.Random(5)
    raw = [rng.gauss(0, 0.02) for _ in range(5000)]
    cal = er.calibration(raw, [a + rng.gauss(0, 0.01) for a in raw])
    assert len(cal["knots_bps"]) == len(er.KNOT_EDGES) - 1
    assert sum(k[2] for k in cal["knots_bps"]) == 5000
    assert abs(er.calibrated(0.01, "monthly", cal) - (0.01 + 0.5 * cal["intercept"])) < 0.002

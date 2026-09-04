"""
tests/test_volatility_regime.py

The radar's adaptive threshold had never adapted.

    Calculates dynamic alpha and Z-Score threshold based on VIX state.
    Scales Z_threshold dynamically with VIX: Z_th = 3.0 * (VIX / 20.0)

It read `sentinel:macro:vix`, which nothing has ever written -- VIX appears in
no collector on this platform, and a scan of the macro tier finds no VIX, TNX or
DXY at all. The read fell through to a hardcoded 20.0, so the threshold resolved
to 3.0 * (20/20) = 3.0 and alpha to 0.10, on every call, in every market
condition, since the function was written.

This is the same shape as the invented Hawkes excitation table and the macro
yield defaults: a chosen number wearing the clothes of a measurement, and
harder to see than either because the surrounding code describes the adaptation
it is not performing.

Realised volatility of the index the platform already records is not VIX -- VIX
is implied and forward-looking, this is neither -- but it moves with what the
threshold cares about, and it can be measured rather than assumed. QQQ carries
530 bars with 342 distinct closes in a day on this deployment.
"""

import math
import random
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.volatility import (  # noqa: E402
    ASSUMED_VOL_WHEN_UNMEASURED, MIN_VOLATILITY_BARS, REALISED_VOL_KEY,
    realised_volatility, simple_returns, threshold_for,
)


def _series(sigma: float, n: int = 80, seed: int = 7):
    rng = random.Random(seed)
    out = [100.0]
    for _ in range(n):
        out.append(out[-1] * (1 + rng.gauss(0, sigma)))
    return out


# -- the threshold now moves ---------------------------------------------------

def test_a_calm_session_tightens_the_threshold():
    alpha, z = threshold_for(realised_volatility(_series(0.0004)))
    assert z < 3.0 and alpha < 0.10


def test_a_violent_session_widens_it():
    alpha, z = threshold_for(realised_volatility(_series(0.004)))
    assert z > 3.0 and alpha > 0.10


def test_calm_and_violent_do_not_produce_the_same_answer():
    """The defect in one line: every market produced 3.0 and 0.10."""
    assert threshold_for(realised_volatility(_series(0.0004))) != \
           threshold_for(realised_volatility(_series(0.004)))


def test_the_threshold_stays_bounded():
    """An unbounded threshold turns one violent session into a detector that
    reports nothing for a week."""
    for vol in (0.0, 1.0, 40.0, 500.0, 10_000.0):
        alpha, z = threshold_for(vol)
        assert 2.5 <= z <= 5.0
        assert 0.0 < alpha <= 0.20


# -- refusing to guess ---------------------------------------------------------

def test_too_few_bars_is_not_a_volatility_of_zero():
    assert realised_volatility([100, 101, 102]) is None


def test_a_flat_series_is_refused_rather_than_called_calm():
    """Zero variance would read as extraordinary calm. It is an absence of
    data, and the frozen macro quotes are what happens when the two are
    conflated."""
    assert realised_volatility([100.0] * 80) is None


def test_unmeasured_falls_back_to_the_old_constants():
    """No worse than the previous behaviour, and now labelled as the assumption
    it always was."""
    assert threshold_for(None) == (0.10, 3.0)
    assert ASSUMED_VOL_WHEN_UNMEASURED == 20.0


def test_malformed_closes_are_skipped_not_fatal():
    series = _series(0.002) + [None, "abc", -5.0, float("nan")]
    assert realised_volatility(series) is not None


def test_the_bar_count_bar_is_meaningful():
    assert MIN_VOLATILITY_BARS >= 20


# -- naming and wiring ---------------------------------------------------------

def test_it_is_not_published_as_vix():
    """Calling realised volatility VIX would be the mislabelling this module
    exists to correct."""
    assert "vix" not in REALISED_VOL_KEY.lower()


def test_the_radar_reads_the_measured_key():
    source = (ROOT / "services" / "collector-radar" / "regime.py").read_text(encoding="utf-8")
    assert "REALISED_VOL_KEY" in source
    assert 'raw_vix = await self.redis.raw.get("sentinel:macro:vix")' not in source


def test_something_actually_publishes_it():
    """A measurement nothing writes is exactly the defect being fixed."""
    enrichment = (ROOT / "services" / "enrichment" / "main.py").read_text(encoding="utf-8")
    assert 'name="realised-volatility"' in enrichment
    assert "REALISED_VOL_KEY" in enrichment


def test_returns_run_forward_in_time():
    """Bars come back newest-first from the query; reversing them is what makes
    the returns mean what they say."""
    enrichment = (ROOT / "services" / "enrichment" / "main.py").read_text(encoding="utf-8")
    assert "list(reversed(closes))" in enrichment


def test_returns_are_computed_on_prices_not_levels():
    assert simple_returns([100.0, 110.0, 121.0]) == [0.1, 0.1]


def test_annualisation_lands_on_the_expected_scale():
    """The existing thresholds were tuned against VIX-scale numbers, so a
    typical index session has to land near them rather than near 0.2."""
    vol = realised_volatility(_series(0.0008))
    assert 5.0 < vol < 60.0, f"annualised vol {vol} is not on a VIX-like scale"

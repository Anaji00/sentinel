"""
tests/test_input_quality_filters.py

Two domains were producing volume rather than signal, and both were measurable
on the running system before anything was changed.

Crypto transfers: 39,262 events in six hours, all scored 0.534. A $0 USDC
movement, a $5 movement and a $291,300 movement carried identical scores under
an identical siren. The size function was correct and unreachable -- a watched
counterparty set `anomaly = max(size_score, 0.45)`, and the log scale only
exceeds 0.45 above $1M, so every transfer below a million dollars collapsed onto
one number. The anomaly score's correlation with trade size was -0.002.

Aviation dark gaps: 9,056 events, 8,064 of them at exactly 1.00 -- one maximally
anomalous aircraft every 2.7 seconds. The score was min(1.0, 0.60 + gap/10),
which reaches the ceiling at four hours. ADS-B is received by ground stations,
so coverage decides whether a position is heard; over open ocean a multi-hour
gap is what every flight on the route does. The absolute threshold was measuring
the ocean.
"""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.enrichment.anomaly_scorer import lift_score  # noqa: E402
from services.enrichment.enrichers.crypto import (  # noqa: E402
    _ALERT_FLOOR_USD, _NOTIONAL_FLOOR_USD, _SUSPECT_LIFT_WEIGHT, _notional_score,
)
from services.enrichment.aviation_gap_detector import (  # noqa: E402
    _MIN_GAP_SAMPLES, _NOTABLE_PERCENTILE, _SCORE_CEILING, _SCORE_FLOOR,
    _percentile_rank,
)


def _suspect_score(notional: float) -> float:
    """What a watched-wallet transfer of this size now scores."""
    return round(lift_score(_notional_score(notional), _SUSPECT_LIFT_WEIGHT), 4)


# -- crypto: size has to survive provenance ------------------------------------

def test_suspect_transfers_are_no_longer_all_the_same_score():
    """The defect verbatim: $0 and $291.3K both arrived at 0.53."""
    assert _suspect_score(0) != _suspect_score(291_300)


def test_size_ordering_survives_the_suspect_lift():
    sizes = [12_000, 291_300, 2_500_000, 50_000_000]
    scores = [_suspect_score(n) for n in sizes]
    assert scores == sorted(scores), "a larger transfer must not score lower"
    assert len(set(scores)) == len(scores), "sizes must be distinguishable"


def test_the_lift_raises_rather_than_replaces():
    """max() discarded the size signal; a headroom lift keeps it."""
    small, large = _notional_score(12_000), _notional_score(2_500_000)
    assert _suspect_score(2_500_000) > _suspect_score(12_000)
    assert _suspect_score(12_000) > small
    assert _suspect_score(2_500_000) > large


def test_a_lifted_score_stays_in_range():
    for n in (0, 1, 10_000, 10**6, 10**9, 10**12):
        assert 0.0 <= _suspect_score(n) <= 1.0


# -- crypto: the notional floor ------------------------------------------------

def test_the_alert_floor_is_the_size_floor_not_a_new_number():
    """Reusing the threshold the log scale already declares avoids a second
    magic constant, and it is the same claim: below this, size says nothing."""
    assert _ALERT_FLOOR_USD == _NOTIONAL_FLOOR_USD


def test_dust_contributes_no_size_signal():
    """Everything the floor excludes scored identically anyway."""
    for n in (0.0, 5.0, 50.0, 505.0, 1_800.0, 9_999.0):
        assert _notional_score(n) == 0.0


def test_a_transfer_above_the_floor_does_carry_signal():
    assert _notional_score(_ALERT_FLOOR_USD * 10) > 0.0


# -- aviation: the null model --------------------------------------------------

def test_a_gap_at_its_regions_median_is_not_notable():
    """The whole point. Four hours of silence over the ocean is what the ocean
    does, and an absolute cutoff called it maximal 8,064 times."""
    oceanic = [3.0 + (i % 40) * 0.1 for i in range(200)]
    assert _percentile_rank(oceanic, 4.0) < _NOTABLE_PERCENTILE


def test_a_gap_in_the_tail_of_its_region_is_notable():
    oceanic = [3.0 + (i % 40) * 0.1 for i in range(200)]
    assert _percentile_rank(oceanic, 12.0) >= _NOTABLE_PERCENTILE


def test_the_same_gap_means_different_things_in_different_regions():
    """A four-hour silence is unremarkable oceanic traffic and extraordinary
    over a monitored strait. One number cannot express both."""
    oceanic = [3.0 + (i % 40) * 0.1 for i in range(200)]
    monitored = [0.05 + (i % 20) * 0.01 for i in range(200)]
    assert _percentile_rank(oceanic, 4.0) < _NOTABLE_PERCENTILE
    assert _percentile_rank(monitored, 4.0) >= _NOTABLE_PERCENTILE


def test_an_empty_distribution_ranks_nothing():
    assert _percentile_rank([], 9.0) == 0.0


def test_the_score_band_leaves_the_ceiling_reachable_by_something_else():
    """An aircraft losing ADS-B is a prompt to look, not a certainty, and 1.00
    should mean more than 'it is over water'."""
    assert 0.0 < _SCORE_FLOOR < _SCORE_CEILING < 1.0


def test_the_sample_bar_is_high_enough_to_mean_something():
    assert _MIN_GAP_SAMPLES >= 30

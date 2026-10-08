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

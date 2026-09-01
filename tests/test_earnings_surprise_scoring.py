"""
tests/test_earnings_surprise_scoring.py

Every earnings surprise scored exactly 0.300, whatever happened.

Traced live after the close, when reports began landing:

    EARNINGS BEAT | RGS  | EPS: 1.04   vs Est 0.1717 (+505.7%)  -> 0.300
    EARNINGS BEAT | PANW | EPS: 1.02   vs Est 0.9959  (+2.4%)   -> 0.300
    EARNINGS MISS | MMED | EPS: 0.0    vs Est 0.0952 (-100.0%)  -> 0.300
    EARNINGS MISS | NIO  | EPS: -0.269 vs Est -0.2124 (-26.6%)  -> 0.300

The scorer computes a z-score against a per-ticker EMA of past surprises, which
is the right question -- how unusual is this, for this issuer. The defect was in
the seed:

    ema_mean = float(raw_mean) if raw_mean else abs_surprise

With no history the mean defaulted to the observation itself, so z was exactly
zero and the max(0.3, ...) floor reported 0.300. Issuers report quarterly and
the EMA expires in four weeks, so a second observation never arrives: every
surprise scored 0.300, permanently.

No history means no z-score, not a z-score of zero. On first sight the size of
the surprise is the only thing actually known, so that is what is scored, and
the EMA still records it for next time.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.enrichment.enrichers.tradfi import (  # noqa: E402
    SURPRISE_MAGNITUDE_SCALE, _surprise_magnitude_score,
)


def test_the_live_cases_are_no_longer_identical():
    """All four scored 0.300 within one window."""
    scores = [_surprise_magnitude_score(p) for p in (2.4, 21.1, 26.6, 100.0, 505.7)]
    assert len(set(scores)) == len(scores)


def test_a_larger_surprise_scores_higher():
    assert _surprise_magnitude_score(505.7) > _surprise_magnitude_score(2.4)


def test_the_curve_is_monotonic():
    observed = [0.5, 2.4, 21.1, 26.6, 100.0, 505.7]
    scores = [_surprise_magnitude_score(p) for p in observed]
    assert scores == sorted(scores)


def test_a_tiny_surprise_is_not_notable():
    """A 2.4% beat is a rounding error on consensus, and 0.300 overstated it."""
    assert _surprise_magnitude_score(2.4) < 0.10


def test_a_hundred_percent_miss_is_notable():
    assert _surprise_magnitude_score(100.0) > 0.80


def test_the_sign_does_not_change_the_magnitude():
    """A miss and a beat of the same size are equally surprising; direction is
    carried in the tags and the headline."""
    assert _surprise_magnitude_score(-26.6) == _surprise_magnitude_score(26.6)


def test_malformed_input_does_not_raise():
    for value in (None, "", "abc", [], {}):
        assert 0.0 <= _surprise_magnitude_score(value) <= 1.0


def test_the_scale_is_documented_and_sane():
    """Fifty percent from consensus is already extraordinary."""
    assert SURPRISE_MAGNITUDE_SCALE > 0
    assert _surprise_magnitude_score(SURPRISE_MAGNITUDE_SCALE) > 0.6


def test_no_history_is_not_a_zero_z_score():
    """The seed is the whole defect: comparing the first observation against
    itself yields z=0, which the floor then reports as unremarkable."""
    source = (ROOT / "services" / "enrichment" / "enrichers" / "tradfi.py").read_text(encoding="utf-8")
    assert "ema_mean = float(raw_mean) if raw_mean else abs_surprise" not in source
    assert "has_history = raw_mean is not None" in source


def test_the_first_sight_path_is_distinguished_from_a_failure():
    """A missing baseline is an expected state, not an error, and conflating
    the two is how it stayed invisible."""
    source = (ROOT / "services" / "enrichment" / "enrichers" / "tradfi.py").read_text(encoding="utf-8")
    assert "except _NoEarningsHistory:" in source

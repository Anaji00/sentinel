"""
tests/test_anomaly_score_calibration.py

378 of ~400 equity blocks scored exactly 1.000, before any adjustment.

Both detector paths mapped an unbounded magnitude through a hand-picked sigmoid:

    RRCF:     score = 1.0 / (1.0 + exp(-0.5 * (avg_codisp - 4.0)))
    fallback: score = 1.0 / (1.0 + exp(-0.8 * (max_z    - 2.5)))

rrcf IS installed in the enrichment image, so the RRCF curve is the one that
runs in production -- a fact worth stating because the first attempt at this
fix corrected only the fallback, on the strength of a "rrcf not installed"
message emitted by a local interpreter rather than by the container. The
ceiling did not move.

Both constants were written before the system had ever run, and against real
traffic the curve is exhausted almost immediately: z=8 already scores 0.988 and
anything past z=15 rounds to exactly 1.0000. Live equity blocks piled 45% of
the population into the top decile. A detector whose output is 1.0 for nearly
half its input is not ranking that half, and everything downstream inherits it
-- radar multiplies the score by 5 for its z-score, correlation derives
confidence from it, and every ranking in the product is built on it.

The score is now the empirical position of the current observation within the
detector's own recent history. "0.9" means "more extreme than 90% of what this
detector has lately seen" -- which is how an analyst reads it anyway -- and it
spreads across the range by construction, whatever the units of the underlying
features happen to be.
"""

import math
import random
import sys
from collections import Counter
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest  # noqa: E402

from shared.utils.streaming_detectors import (  # noqa: E402
    FALLBACK_MAX_SCORE,
    FALLBACK_MIN_HISTORY,
    RRCFDetector,
)


def _point(magnitude):
    return np.array([magnitude, magnitude * 0.5, 0.0, 0.0, 0.0], dtype=np.float32)


def _run(count, seed=1):
    """Realistic traffic: mostly ordinary, occasionally much larger."""
    detector = RRCFDetector()
    rng = random.Random(seed)
    return [detector.insert(_point(rng.lognormvariate(0, 1))) for _ in range(count)]


# -- the failure, stated -------------------------------------------------------

def test_the_old_curve_saturated():
    """Kept as arithmetic so the reason for the change stays legible."""
    old = lambda z: 1.0 / (1.0 + math.exp(-0.8 * (z - 2.5)))
    assert round(old(8), 4) >= 0.98, "the curve was exhausted by z=8"
    assert round(old(15), 4) == 1.0, "everything past z=15 collapsed to one value"
    assert round(old(15), 4) == round(old(30), 4), "extremes were indistinguishable"


def test_scores_no_longer_pile_at_the_ceiling():
    scores = _run(600)[FALLBACK_MIN_HISTORY:]
    at_ceiling = sum(1 for s in scores if s >= 0.999)
    assert at_ceiling == 0, f"{at_ceiling} scores still at the ceiling"


def test_the_range_is_actually_used():
    """A ranking that uses three deciles is ranking into three buckets."""
    scores = _run(600)[FALLBACK_MIN_HISTORY:]
    deciles = {min(9, int(s * 10)) for s in scores}
    assert len(deciles) >= 8, f"only {len(deciles)} deciles populated"


def test_no_single_decile_dominates():
    """45% of live equity blocks landed in the top decile alone."""
    scores = _run(600)[FALLBACK_MIN_HISTORY:]
    counts = Counter(min(9, int(s * 10)) for s in scores)
    biggest = max(counts.values()) / len(scores)
    assert biggest < 0.35, f"one decile holds {biggest:.0%} of the population"


# -- it must still detect ------------------------------------------------------

def test_a_genuine_outlier_scores_near_the_top():
    """Spreading the range is worthless if the detector stops detecting."""
    detector = RRCFDetector()
    rng = random.Random(7)
    for _ in range(300):
        detector.insert(_point(rng.gauss(10.0, 0.5)))

    assert detector.insert(_point(500.0)) > 0.9


def test_an_ordinary_observation_does_not():
    detector = RRCFDetector()
    rng = random.Random(7)
    for _ in range(300):
        detector.insert(_point(rng.gauss(10.0, 0.5)))

    assert detector.insert(_point(10.1)) < 0.9


# -- bounds --------------------------------------------------------------------

def test_the_score_never_reaches_one():
    """The most extreme thing seen so far is still only that. Reporting
    certainty leaves nothing to say when something worse arrives."""
    detector = RRCFDetector()
    for i in range(400):
        detector.insert(_point(float(i) ** 2))          # relentlessly escalating
    assert detector.insert(_point(1e12)) <= FALLBACK_MAX_SCORE


@pytest.mark.parametrize("score", _run(400)[FALLBACK_MIN_HISTORY:])
def test_every_score_is_a_valid_probability(score):
    assert 0.0 <= score <= FALLBACK_MAX_SCORE


def test_warmup_does_not_invent_a_percentile():
    """A percentile from four observations is not a percentile."""
    detector = RRCFDetector()
    for _ in range(5):
        detector.insert(_point(1.0))
    assert len(detector._z_history) < FALLBACK_MIN_HISTORY


def test_degenerate_input_does_not_break_scoring():
    """An unchanging feed has zero variance; it must not divide by it."""
    detector = RRCFDetector()
    for _ in range(200):
        score = detector.insert(_point(5.0))
        assert 0.0 <= score <= FALLBACK_MAX_SCORE


# -- both detector paths, one meaning ------------------------------------------

def test_both_scoring_paths_use_the_same_positional_rule():
    """The first version of this fix corrected only the fallback path.

    rrcf is installed in the enrichment image, so _insert_fallback never runs
    there and the change had no effect on production at all -- the ceiling
    stayed at 41.8%. The RRCF path carried its own hand-picked sigmoid,
    1/(1+exp(-0.5*(avg_codisp-4.0))), with the same failure. Two curves meant
    two meanings for the same number; one method means one.
    """
    source = (ROOT / "shared/utils/streaming_detectors.py").read_text(encoding="utf-8")
    assert source.count("self._positional_score(") == 2, "a scoring path bypasses the shared rule"
    assert "def _positional_score" in source


def test_neither_path_keeps_a_bare_saturating_sigmoid():
    """The curves survive only as warmup priors, never as the final score."""
    source = (ROOT / "shared/utils/streaming_detectors.py").read_text(encoding="utf-8")
    assert "score = 1.0 / (1.0 + math.exp(-0.5 * (avg_codisp - 4.0)))" not in source
    assert "score = 1.0 / (1.0 + math.exp(-0.8 * (max_z - 2.5)))" not in source


def test_history_exists_regardless_of_which_detector_is_available():
    """The RRCF path needs the history buffer too; it was originally created
    only in the fallback branch."""
    detector = RRCFDetector()
    assert hasattr(detector, "_z_history")


# -- boosts must not re-create the ceiling the detector just lost --------------

def test_boosts_lift_headroom_rather_than_adding():
    """`min(1.0, a + b)` has no notion of how much room is left.

    A 0.85 score and a 0.99 score, given the same two boosts, both became
    exactly 1.0 and stopped being distinguishable -- which put a third of
    market_anomaly events in the top decile even after the detector itself was
    recalibrated. The tradfi enricher was converted to a headroom lift for this
    reason; the crypto paths were not.
    """
    from services.enrichment.anomaly_scorer import lift_score

    low = lift_score(lift_score(0.85, 0.15), 0.20, 0.15)
    high = lift_score(lift_score(0.99, 0.15), 0.20, 0.15)

    assert low < high, "two different scores collapsed to the same value"
    assert low < 1.0 and high < 1.0


def test_no_crypto_path_still_adds_and_clamps():
    source = (ROOT / "services/enrichment/enrichers/crypto.py").read_text(encoding="utf-8")
    assert "min(1.0, anomaly +" not in source, "an additive clamp remains"


def test_boosts_share_one_allowance():
    """Otherwise each boost takes a share of what the last one left, and a
    sequence of them still approaches the ceiling."""
    from services.enrichment.anomaly_scorer import lift_score, MAX_TOTAL_LIFT_SHARE

    score, spent = 0.60, 0.0
    for weight in (0.15, 0.20, 0.15, 0.20):
        score = lift_score(score, weight, spent)
        spent += weight
    assert score < 0.60 + (1.0 - 0.60) * MAX_TOTAL_LIFT_SHARE + 1e-6

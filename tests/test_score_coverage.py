'''Gap 2: "I don't know" is not a value any detector can return.

Repeatedly in this audit, failure was indistinguishable from a confident
negative. The RRCF forest returned 0.0 -- the lowest possible anomaly -- when
every tree in it had thrown. A z-score pinned at its reporting cap looked like a
measurement rather than a collapsed denominator. An earnings surprise was scored
against a baseline seeded with itself and reported the floor. Each was fixed
where it was found; the general property was missing.

The general form lives in `_positional_score`, which produces a score two ways:

    if len(self._z_history) < FALLBACK_MIN_HISTORY:
        score = warmup_curve(raw)        # a guessed sigmoid, almost no history
    else:
        score = below / len(self._z_history)   # a percentile over 64+ samples

Both return a float in [0,1] and nothing downstream could tell them apart, so a
cold detector's 0.4 ranked beside a warm one's 0.4 -- and an inference slot,
the scarcest thing this platform has, could be spent on the weaker of the two.

Coverage is recorded as state rather than returned, so no caller signature
changes and a reader that does not ask is unaffected.
'''
import numpy as np
import pytest

from shared.utils.streaming_detectors import FALLBACK_MIN_HISTORY, RRCFDetector


def _drive(n, seed=0):
    rng = np.random.default_rng(seed)
    d = RRCFDetector()
    for _ in range(n):
        d.insert(np.array(rng.normal(0, 1, 5)))
    return d


def test_a_detector_that_has_scored_nothing_claims_no_coverage():
    d = RRCFDetector()
    c = d.coverage()
    assert c["fraction"] == 0.0
    assert c["basis"] == "none"


def test_a_cold_detector_says_it_is_cold():
    c = _drive(10).coverage()
    assert c["basis"] == "warmup_curve"
    assert 0.0 < c["fraction"] < 1.0


def test_a_warm_detector_says_its_score_is_a_percentile():
    c = _drive(FALLBACK_MIN_HISTORY + 40).coverage()
    assert c["basis"] == "percentile"
    assert c["fraction"] == 1.0


def test_coverage_rises_monotonically_with_history():
    seen = [_drive(n, seed=3).coverage()["fraction"] for n in (5, 20, 50, FALLBACK_MIN_HISTORY + 10)]
    assert seen == sorted(seen)
    assert seen[0] < seen[-1]


def test_the_two_bases_are_distinguishable_at_the_same_score():
    """The whole point: equal numbers, unequal claims."""
    cold = _drive(8, seed=1).coverage()
    warm = _drive(FALLBACK_MIN_HISTORY + 60, seed=1).coverage()
    assert cold["basis"] != warm["basis"]
    assert cold["fraction"] < warm["fraction"]


def test_coverage_reports_the_sample_count_it_was_computed_from():
    c = _drive(30, seed=5).coverage()
    assert c["target_samples"] == FALLBACK_MIN_HISTORY
    assert 0 < c["samples"] <= 30
    assert c["fraction"] == pytest.approx(c["samples"] / c["target_samples"], abs=1e-3)


def test_coverage_never_exceeds_one():
    c = _drive(FALLBACK_MIN_HISTORY * 4, seed=7).coverage()
    assert c["fraction"] == 1.0


def test_the_scorer_carries_coverage_to_its_consumers():
    """Recorded on the detector is not enough; it has to reach a reader."""
    import pathlib

    src = (
        pathlib.Path(__file__).resolve().parents[1]
        / "services" / "enrichment" / "anomaly_scorer.py"
    ).read_text(encoding="utf-8")
    assert src.count('"coverage"') >= 2, (
        "coverage is computed and not attached to the score dicts, which is the "
        "shape this audit has found nine times"
    )
    assert "detector.coverage()" in src

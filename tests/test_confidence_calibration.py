"""Heuristic confidence scores, mapped onto the rate they actually confirm at.

A correlation is published with a confidence built from three hand-chosen
weights -- 0.45 on the trigger's anomaly, 0.30 on breadth of evidence, 0.25 on
whether it spans domains. Those weights rank clusters sensibly. They do not
make the number a probability, and it was being published as one and read as
one by the tier reconciliation downstream.

The important properties are the refusals: below a sample floor it declines to
fit, when every outcome agrees it declines to fit, and it never reorders two
clusters the engine ranked.
"""
import pathlib
import random
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.confidence_calibration import (  # noqa: E402
    MIN_CALIBRATION_SAMPLES,
    apply,
    fit,
)


def _overconfident(n=1200, scale=0.55, seed=7):
    """A heuristic that ranks correctly and is systematically overconfident."""
    rng = random.Random(seed)
    pairs = []
    for _ in range(n):
        raw = rng.uniform(0.3, 0.95)
        pairs.append((raw, 1 if rng.random() < scale * raw else 0))
    return pairs


def test_it_recovers_the_true_rate_from_an_overconfident_score():
    pairs = _overconfident()
    model = fit(pairs)
    assert model is not None
    # A published 0.8 confirms at about 0.44 in this population.
    assert abs(apply(model, 0.80) - 0.44) < 0.06
    assert abs(apply(model, 0.60) - 0.33) < 0.06


def test_it_refuses_to_fit_below_the_sample_floor():
    """A mapping from nine outcomes is worse than none: it looks like evidence."""
    assert fit(_overconfident(n=MIN_CALIBRATION_SAMPLES - 1)) is None
    assert fit([]) is None


def test_it_refuses_to_fit_when_every_outcome_agrees():
    """Such a mapping describes the sample, not the relationship."""
    all_true = [(0.1 * i % 1.0, 1) for i in range(MIN_CALIBRATION_SAMPLES + 50)]
    assert fit(all_true) is None


def test_the_ranking_is_never_inverted():
    """Isotonic is monotone by construction; a cluster ranked higher by the
    engine must never be published below one it ranked under."""
    model = fit(_overconfident())
    scores = [0.30, 0.45, 0.60, 0.75, 0.90]
    mapped = [apply(model, s) for s in scores]
    assert mapped == sorted(mapped)


def test_an_uncalibrated_deployment_publishes_the_raw_score_unchanged():
    for raw in (0.0, 0.37, 0.85, 1.0):
        assert apply(None, raw) == raw


def test_junk_input_is_returned_rather_than_raised():
    assert apply(None, "not a number") == "not a number"


@pytest.mark.anyio
async def test_calibrate_reports_whether_it_actually_calibrated():
    """The flag matters: a correction doing nothing must be visible."""
    from shared.utils import confidence_calibration as cc

    cc._cached_model, cc._cached_at, cc._cached_n = None, 0.0, 0
    out = await cc.calibrate(None, 0.8)
    assert out["confidence"] == 0.8
    assert out["raw_confidence"] == 0.8
    assert out["calibrated"] is False


def test_the_engine_publishes_both_numbers():
    """Seeing only the corrected value gives a reader no way to notice the
    correction is inert."""
    src = (ROOT / "services" / "correlation" / "main.py").read_text(encoding="utf-8")
    assert "calibrate_confidence(" in src
    assert '"raw_confidence"' in src
    assert '"confidence_calibrated"' in src


def test_outcomes_are_recorded_when_a_scenario_resolves():
    src = (ROOT / "services" / "reasoning" / "scenario_tracker.py").read_text(encoding="utf-8")
    assert "_record_correlation_confidence_outcome" in src
    assert "record_outcome(" in src

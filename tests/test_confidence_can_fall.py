"""Refuting a scenario must be able to lower confidence in it.

overall_confidence was max(posteriors). Posteriors are normalised to sum to 1,
so that is a relative measure -- how far the leading hypothesis is ahead of its
rivals -- with a hard floor of 1/n: 33 for three hypotheses, 50 for two.

The tracker reads the same number as an absolute claim, confirming above 65 and
denying at or below 25. Against a floor of 33 the deny branch could never fire,
and the database bore that out exactly: of 672 scenarios, 213 were confirmed and
0 were ever denied, with the lowest confidence ever recorded sitting at 35.

The failure was not merely that refutation was weak. Deny hits against every
hypothesis moved confidence by nothing whatsoever -- normalisation divides a
shared penalty straight back out -- and deny hits against the leader alone
*raised* it, because the leader's mass simply moved to the runner-up.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.reasoning.calibration_harness import (  # noqa: E402
    DENY_MASS_WEIGHT, DynamicBayesianCalibrator as Calibrator, MIN_EVIDENTIAL_SUPPORT,
)

# The tracker's thresholds, restated so this file fails if they drift apart.
CONFIRM_THRESHOLD = 65
DENY_THRESHOLD = 25

HYPOTHESES = [
    {"label": "H1", "probability": 45},
    {"label": "H2", "probability": 35},
    {"label": "H3", "probability": 20},
]


def _confidence(watch=None, deny=None) -> int:
    _h, confidence, _notes = Calibrator.recalibrate_hypotheses(
        HYPOTHESES, watch or {}, deny or {}
    )
    return confidence


def test_the_tracker_thresholds_still_match_this_file():
    source = (ROOT / "services/reasoning/scenario_tracker.py").read_text(encoding="utf-8")
    assert f"CONFIRM_THRESHOLD = {CONFIRM_THRESHOLD}" in source
    assert f"DENY_THRESHOLD = {DENY_THRESHOLD}" in source


def test_refuting_every_hypothesis_reaches_denied():
    """The case that never once fired in 672 live scenarios."""
    confidence = _confidence(deny={0: ["a"], 1: ["b"], 2: ["c"]})
    assert confidence <= DENY_THRESHOLD, (
        f"every hypothesis was refuted and confidence is still {confidence}, "
        f"above the deny threshold of {DENY_THRESHOLD}"
    )


def test_refuting_everything_is_not_a_no_op():
    """It used to return exactly the unrefuted value, whatever the evidence."""
    assert _confidence(deny={0: ["a"], 1: ["b"], 2: ["c"]}) < _confidence()


def test_refuting_the_leader_does_not_raise_confidence():
    """It rose from 45 to 57: the leader's mass moved to the runner-up and the
    scenario read as *more* certain for having its best explanation refuted."""
    assert _confidence(deny={0: ["a", "a", "a"]}) <= _confidence()


def test_more_refutation_never_reads_as_more_confidence():
    """Monotonicity across breadth of refutation."""
    one = _confidence(deny={0: ["a"]})
    two = _confidence(deny={0: ["a"], 1: ["b"]})
    three = _confidence(deny={0: ["a"], 1: ["b"], 2: ["c"]})
    assert one >= two >= three, (one, two, three)


def test_confirmation_is_untouched():
    """The 213 existing confirmations must not be disturbed.

    Nothing here may lower confidence when no deny signal fired; the support
    term is exactly 1.0 in that case.
    """
    assert _confidence() == 45
    assert _confidence(watch={0: ["w", "w"]}) >= CONFIRM_THRESHOLD


def test_total_refutation_stops_short_of_certainty():
    """Signals are matched heuristically, so 0 would assert more than we know."""
    confidence = _confidence(deny={0: ["a"] * 9, 1: ["b"] * 9, 2: ["c"] * 9})
    assert confidence > 0
    assert MIN_EVIDENTIAL_SUPPORT > 0 and DENY_MASS_WEIGHT <= 1.0


def test_the_note_says_why_confidence_fell():
    """A confidence that drops without saying why cannot be reviewed."""
    _h, _c, notes = Calibrator.recalibrate_hypotheses(
        HYPOTHESES, {}, {0: ["a"], 1: ["b"], 2: ["c"]}
    )
    assert "refuted" in notes.lower(), notes

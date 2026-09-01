"""
tests/test_inference_admission.py

Which twenty of a hundred and fifty thousand, and why.

This host affords roughly twenty model inferences an hour against an input of
about a hundred and fifty thousand events. `try_acquire` had taken a `score`
parameter since it was written and never read it, so admission was decided by
one thing: which caller happened to arrive while the slot was free.

That makes timing the selection criterion for everything the system chooses to
think about. Asked which twenty events were analysed and why, the honest answer
was "whichever arrived at the right moment" -- which is not a defensible answer
for a surveillance platform, however well every individual component works.

A candidate now has to beat what this process has lately been seeing. The bar
is a percentile of recent scores rather than a fixed threshold, for the same
reason the detectors are: only the deployment knows what ordinary looks like.
"""

import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest  # noqa: E402

from shared.utils.inference_budget import (  # noqa: E402
    ADMISSION_MIN_HISTORY,
    ADMISSION_PERCENTILE,
    MAX_HOLDBACK_SEC,
    InferenceBudget,
)


def _warmed(ordinary=0.5, n=200):
    """A budget whose bar has seen a steady stream of ordinary traffic."""
    budget = InferenceBudget(None, "test-model")
    for i in range(n):
        budget._passes_admission_bar(ordinary + (i % 7) * 0.01)
    return budget


# -- the selection ------------------------------------------------------------

def test_a_weak_candidate_is_held_back():
    assert _warmed()._passes_admission_bar(0.05) is False


def test_a_strong_candidate_is_admitted():
    assert _warmed()._passes_admission_bar(0.99) is True


def test_the_bar_tracks_the_stream_rather_than_a_constant():
    """0.6 is unremarkable in a busy stream and exceptional in a quiet one.

    A fixed threshold would call it the same thing in both.
    """
    busy = _warmed(ordinary=0.85)
    quiet = _warmed(ordinary=0.15)

    assert busy._passes_admission_bar(0.60) is False
    assert quiet._passes_admission_bar(0.60) is True


# -- the rails that stop it starving -----------------------------------------

def test_an_unscored_caller_is_never_refused():
    """A caller that does not score its work must not be silently starved by a
    selection rule it never participated in."""
    assert _warmed()._passes_admission_bar(None) is True


def test_a_malformed_score_is_not_treated_as_low():
    assert _warmed()._passes_admission_bar("not a number") is True


def test_admission_is_open_until_there_is_history_to_judge_against():
    """A bar computed from a handful of samples mostly encodes their order."""
    budget = InferenceBudget(None, "test-model")
    for _ in range(ADMISSION_MIN_HISTORY - 1):
        assert budget._passes_admission_bar(0.01) is True


def test_a_long_holdback_eventually_admits_anything():
    """An idle slot helps nobody, and a rule that can refuse forever is worse
    than no rule."""
    budget = _warmed()
    assert budget._passes_admission_bar(0.05) is False

    budget._last_admit = time.monotonic() - (MAX_HOLDBACK_SEC + 1)
    assert budget._passes_admission_bar(0.05) is True


# -- the parameter is actually supplied --------------------------------------

def test_the_agent_passes_a_score_when_claiming_a_slot():
    """The bar is inert unless callers fill the parameter -- which none did for
    the entire life of the method."""
    source = (ROOT / "services/agents/base.py").read_text(encoding="utf-8")
    assert "score=_message_score(message)" in source


@pytest.mark.parametrize(
    "message,expected",
    [
        ({"anomaly_score": 0.93}, 0.93),
        ({"severity": 4}, 0.8),                      # authored 1-5, normalised
        ({"trigger": {"anomaly_score": 0.71}}, 0.71),
        ({"headline": "no score here"}, None),
        ({"anomaly_score": "bad"}, None),
    ],
)
def test_the_score_is_read_from_whatever_the_producer_supplied(message, expected):
    from services.agents.base import _message_score

    assert _message_score(message) == expected


def test_the_percentile_is_configurable():
    source = (ROOT / "shared/utils/inference_budget.py").read_text(encoding="utf-8")
    assert "INFERENCE_ADMISSION_PERCENTILE" in source
    assert 0.0 < ADMISSION_PERCENTILE < 1.0

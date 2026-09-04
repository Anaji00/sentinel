"""A scenario that cannot be refuted must not claim the confidence of one that can.

Two asymmetries, both measured on the full 676-scenario corpus once the
double-encoded half became readable:

  148 of 676 (21.9%) carry no deny signal on any hypothesis. They can be
  confirmed by watch hits and never refuted by anything, which is half the
  reason 672 scenarios produced 213 confirmations and zero denials.

  117 of 676 (17.3%) have every hypothesis watching for an identical set of
  observables. The tracker applies watch hits per hypothesis, so a hit raises
  all of them together and separates none -- the Bayesian update is
  uninformative whichever signal fires.

_discriminates_between_hypotheses does not catch the second: it compares the
deny set and the watch set as one signature, so identical watch signals pass
whenever the deny signals differ at all.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.reasoning.scenario_generator import (  # noqa: E402
    INDISCRIMINATE_WATCH_CEILING, UNREFUTABLE_CEILING,
    _discriminates_between_hypotheses, _shares_one_watch_set, _supported_confidence,
)


class _Signal:
    def __init__(self, entity, observable, comparator="above", threshold=5.0, unit="%"):
        self.entity = entity
        self.observable = observable
        self.comparator = comparator
        self.threshold = threshold
        self.unit = unit


class _Hyp:
    def __init__(self, probability, watch=(), deny=()):
        self.probability = probability
        self.watch_signals = list(watch)
        self.deny_signals = list(deny)


class _Draft:
    def __init__(self, hypotheses, confidence=95):
        self.hypotheses = hypotheses
        self.confidence_overall = confidence
        self.confidence_rationale = (
            "The options flow on TSLA and the block trade on SNDK land inside the "
            "same session, which is what ties them together rather than coincidence."
        )


def _w(name):
    return _Signal("TSLA", name)


def _d(name):
    return _Signal("TSLA", name, comparator="occurs", threshold=None, unit=None)


def test_a_draft_with_no_deny_signals_is_capped():
    draft = _Draft([
        _Hyp(50, watch=[_w("spot volume")]),
        _Hyp(30, watch=[_w("open interest")]),
        _Hyp(20, watch=[_w("short interest")]),
    ])
    assert _supported_confidence(draft) <= UNREFUTABLE_CEILING


def test_the_cap_sits_below_the_confirm_threshold():
    """Otherwise a one-directional claim could still assert itself as proven."""
    tracker = (ROOT / "services/reasoning/scenario_tracker.py").read_text(encoding="utf-8")
    assert "CONFIRM_THRESHOLD = 65" in tracker
    assert UNREFUTABLE_CEILING < 65


def test_a_refutable_draft_is_not_capped_by_that_rule():
    draft = _Draft([
        _Hyp(50, watch=[_w("spot volume")], deny=[_d("no volume change")]),
        _Hyp(30, watch=[_w("open interest")], deny=[_d("open interest flat")]),
        _Hyp(20, watch=[_w("short interest")], deny=[_d("short interest flat")]),
    ])
    assert _supported_confidence(draft) > UNREFUTABLE_CEILING


def test_identical_watch_sets_are_capped():
    shared = [_w("spot volume")]
    draft = _Draft([
        _Hyp(50, watch=list(shared), deny=[_d("a")]),
        _Hyp(30, watch=list(shared), deny=[_d("b")]),
        _Hyp(20, watch=list(shared), deny=[_d("c")]),
    ])
    assert _shares_one_watch_set(draft.hypotheses)
    assert _supported_confidence(draft) <= INDISCRIMINATE_WATCH_CEILING


def test_that_case_slips_past_the_existing_guard():
    """The reason a second check was needed rather than a stricter first one."""
    shared = [_w("spot volume")]
    draft = _Draft([
        _Hyp(50, watch=list(shared), deny=[_d("a")]),
        _Hyp(30, watch=list(shared), deny=[_d("b")]),
        _Hyp(20, watch=list(shared), deny=[_d("c")]),
    ])
    assert _discriminates_between_hypotheses(draft), (
        "the existing guard now rejects this; the ceiling would be unreachable"
    )


def test_distinct_watch_sets_are_not_capped():
    draft = _Draft([
        _Hyp(50, watch=[_w("spot volume")], deny=[_d("a")]),
        _Hyp(30, watch=[_w("open interest")], deny=[_d("b")]),
        _Hyp(20, watch=[_w("short interest")], deny=[_d("c")]),
    ])
    assert not _shares_one_watch_set(draft.hypotheses)
    assert _supported_confidence(draft) > INDISCRIMINATE_WATCH_CEILING


def test_no_watch_signals_anywhere_is_not_double_counted():
    """That is the unfalsifiable case, which already carries a lower ceiling."""
    draft = _Draft([_Hyp(50, deny=[_d("a")]), _Hyp(30, deny=[_d("b")]), _Hyp(20, deny=[_d("c")])])
    assert not _shares_one_watch_set(draft.hypotheses)


def test_a_single_hypothesis_is_not_an_indiscriminate_watch_set():
    draft = _Draft([_Hyp(100, watch=[_w("spot volume")], deny=[_d("a")])])
    assert not _shares_one_watch_set(draft.hypotheses)


def test_nothing_here_raises_a_number_the_model_did_not_claim():
    """Every ceiling caps; none invents confidence."""
    draft = _Draft([
        _Hyp(50, watch=[_w("spot volume")], deny=[_d("a")]),
        _Hyp(30, watch=[_w("open interest")], deny=[_d("b")]),
        _Hyp(20, watch=[_w("short interest")], deny=[_d("c")]),
    ], confidence=12)
    assert _supported_confidence(draft) == 12

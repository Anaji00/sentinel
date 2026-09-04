"""Governs when the red team runs, and whether its verdict is accepted.

Scenario synthesis is two full inferences -- on this host roughly eight minutes
each -- and the second ran unconditionally, halving how many scenarios could be
produced at all. It earns that cost on a draft that is uncertain or internally
inconsistent; on one already confident, clearly ranked and argued it mostly
returns a rephrasing.

The critique's output was also accepted unconditionally, so a pass that dropped
the hypotheses or returned an empty headline silently destroyed a good scenario
and the failure looked like a success.
"""
import pathlib
import sys
import types

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.reasoning.scenario_generator import (  # noqa: E402
    CLEAR_LEAD_MARGIN,
    CRITIQUE_CONFIDENCE_CEILING,
    MIN_ARGUED_RATIONALE_CHARS,
    _draft_needs_critique,
    _is_at_least_as_complete,
)

ARGUED = "x" * (MIN_ARGUED_RATIONALE_CHARS + 20)


def draft(confidence=90, probs=(70, 30), rationale=ARGUED):
    return types.SimpleNamespace(
        headline="Export controls tighten",
        confidence_overall=confidence,
        confidence_rationale=rationale,
        hypotheses=[types.SimpleNamespace(probability=p) for p in probs],
    )


def test_a_strong_draft_skips_the_red_team():
    """Confident, clearly ranked and argued -- the critique would rephrase it."""
    assert _draft_needs_critique(draft()) is False


def test_an_unconfident_draft_is_challenged():
    assert _draft_needs_critique(draft(confidence=40)) is True


def test_evenly_split_hypotheses_are_challenged():
    """The model is genuinely torn, which is what a red team is for."""
    margin = CLEAR_LEAD_MARGIN - 5
    assert _draft_needs_critique(draft(probs=(50, 50 - margin))) is True


def test_an_unargued_rationale_is_challenged():
    """A confidence figure with no reasoning behind it is a claim."""
    assert _draft_needs_critique(draft(rationale="High confidence.")) is True


def test_a_single_hypothesis_is_challenged():
    """Nothing to weigh against anything."""
    assert _draft_needs_critique(draft(probs=(100,))) is True


def test_the_ceiling_is_the_boundary_not_a_trigger():
    """A draft exactly at the ceiling counts as committed, not as suspect."""
    assert _draft_needs_critique(draft(confidence=CRITIQUE_CONFIDENCE_CEILING)) is False
    assert _draft_needs_critique(draft(confidence=CRITIQUE_CONFIDENCE_CEILING - 1)) is True


def test_no_draft_means_nothing_to_critique():
    assert _draft_needs_critique(None) is False


# ── accepting or refusing the critique ───────────────────────────────────────

def test_a_critique_that_keeps_the_structure_is_accepted():
    assert _is_at_least_as_complete(draft(), draft()) is True


def test_a_critique_that_deletes_the_hypotheses_is_refused():
    """A red team that removes the analysis has not improved it."""
    stripped = draft()
    stripped.hypotheses = []
    assert _is_at_least_as_complete(stripped, draft()) is False


def test_a_critique_with_an_empty_headline_is_refused():
    blank = draft()
    blank.headline = "   "
    assert _is_at_least_as_complete(blank, draft()) is False


def test_pruning_one_weak_hypothesis_is_allowed():
    """Removing a weak branch is legitimate editing, not degradation."""
    incumbent = draft(probs=(50, 30, 20))
    pruned = draft(probs=(60, 40))
    assert _is_at_least_as_complete(pruned, incumbent) is True


def test_collapsing_three_hypotheses_to_one_is_refused():
    incumbent = draft(probs=(50, 30, 20))
    collapsed = draft(probs=(100,))
    assert _is_at_least_as_complete(collapsed, incumbent) is False


def test_a_missing_critique_result_is_refused():
    assert _is_at_least_as_complete(None, draft()) is False


def test_both_exits_from_synthesis_map_through_one_function():
    """The skip path and the critique path must produce the same shape."""
    src = (ROOT / "services/reasoning/scenario_generator.py").read_text(encoding="utf-8")
    # Counted on the call, not on `return` preceding it. The skip path now
    # assigns first so it can apply the grounding gate before returning, which
    # is still one function producing both shapes -- the property this test
    # exists to hold.
    assert src.count("self._to_scenario(cluster, output)") == 2
    assert src.count("scenario = Scenario(") == 1, "two places build a Scenario and can drift"

"""Bulletin conviction must obey the same contract as prediction conviction.

AgentPrediction.conviction gained a validator after a model filled the bare
float field with 55.0 and every consumer read it as 0-1. AgentBulletin carried
the identical field, with the identical "# 0.0 - 1.0" comment, no validator, and
the same models feeding it -- macro's tail_risk_conviction is a bare float the
model writes and it reaches publish_bulletin unchanged.

Bulletins are fused by the consensus engine using Subjective Logic, where an
out-of-range conviction does not merely overweight an opinion, it breaks the
algebra. For a bearish bulletin at conviction 85:

    r = (1.0 - 85.0) * evidence_count * 0.3   ->  negative
    b = r / (r + s + W)                       ->  -0.419

Belief, disbelief and uncertainty are masses in [0,1] that sum to 1, so a
negative belief is not a wrong answer, it is not an answer at all.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.agents.base import AgentBulletin, AgentPrediction  # noqa: E402


def _bulletin(conviction):
    return AgentBulletin(agent_name="a", bulletin_type="thesis", conviction=conviction)


def _prediction(conviction):
    return AgentPrediction(agent_name="a", ticker="X", direction="up",
                           conviction=conviction, entry_price=1.0)


def test_a_percentage_is_normalised():
    assert _bulletin(85.0).conviction == 0.85
    assert _bulletin(55).conviction == 0.55


def test_a_probability_is_left_alone():
    assert _bulletin(0.85).conviction == 0.85
    assert _bulletin(0.0).conviction == 0.0
    assert _bulletin(1.0).conviction == 1.0


def test_conviction_never_leaves_the_unit_interval():
    """The property the fusion algebra depends on."""
    for value in (-3, 0, 0.5, 1, 55, 100, 250, 1e9):
        assert 0.0 <= _bulletin(value).conviction <= 1.0, value


def test_the_two_models_agree():
    """They diverged once; the point of sharing the helper is that they cannot
    again."""
    for value in (0.0, 0.42, 1.0, 1.5, 55, 85.0, 100, 250, -3):
        assert _bulletin(value).conviction == _prediction(value).conviction, value


def test_the_fusion_masses_stay_valid_for_any_conviction():
    """Driven through the real SubjectiveOpinion rather than asserted."""
    from services.agents.consensus_engine import SubjectiveOpinion

    for value in (0.0, 0.5, 1.0, 55, 85.0, 250):
        for direction in ("up", "down", "neutral"):
            b = _bulletin(value)
            b.expected_direction = direction
            opinion = SubjectiveOpinion.from_bulletin(b, weight=0.5)
            for name, mass in (("belief", opinion.belief),
                               ("disbelief", opinion.disbelief),
                               ("uncertainty", opinion.uncertainty)):
                assert 0.0 <= mass <= 1.0, f"{name}={mass} for conviction={value} {direction}"
            total = opinion.belief + opinion.disbelief + opinion.uncertainty
            assert abs(total - 1.0) < 1e-4, f"masses sum to {total}"


def test_a_non_numeric_conviction_is_still_rejected():
    """Normalising is for recoverable numbers, not for nonsense."""
    import pytest
    with pytest.raises(Exception):
        _bulletin("very confident")

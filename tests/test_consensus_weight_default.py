"""An unevaluated agent must not outrank a measured one.

AgentScorecard.consensus_weight defaulted to 1.0 while update_scorecard sets it
to max(0.1, 1.0 - brier_score). The starting Brier is 0.5, which that formula
turns into 0.5 -- so an agent that had never resolved a prediction carried the
weight of a flawless one, and halved the instant it resolved its first.

The consensus engine multiplies this by 10 to get an evidence count for
Subjective Logic fusion, so an unproven agent moved the fused opinion twice as
hard as one measured at the very Brier it starts from.

Not a theoretical exposure: Redis holds no agent scorecards at all, because
until the entity-appearance scorer landed nothing could resolve a prediction.
Every agent in the swarm is weighted through this default today.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.agents.base import (  # noqa: E402
    MIN_CONSENSUS_WEIGHT, UNPROVEN_BRIER, AgentScorecard, consensus_weight_for,
)


def test_the_default_agrees_with_the_formula():
    """The drift itself. These two were 1.0 and 0.5 for the same Brier."""
    card = AgentScorecard(agent_name="never_evaluated")
    assert card.consensus_weight == consensus_weight_for(card.brier_score)


def test_an_unproven_agent_does_not_outrank_a_perfect_one():
    unproven = AgentScorecard(agent_name="a").consensus_weight
    perfect = consensus_weight_for(0.0)
    assert unproven < perfect


def test_resolving_a_prediction_at_the_starting_brier_changes_nothing():
    """It used to halve the agent's weight, purely for having been measured."""
    unproven = AgentScorecard(agent_name="a").consensus_weight
    assert consensus_weight_for(UNPROVEN_BRIER) == unproven


def test_better_calibration_earns_more_weight():
    assert consensus_weight_for(0.1) > consensus_weight_for(0.4) > consensus_weight_for(0.8)


def test_a_reliably_wrong_agent_is_not_silenced():
    """Being consistently wrong is information; zero would discard it."""
    assert consensus_weight_for(1.0) == MIN_CONSENSUS_WEIGHT
    assert consensus_weight_for(5.0) == MIN_CONSENSUS_WEIGHT
    assert MIN_CONSENSUS_WEIGHT > 0


def test_the_calibration_path_uses_the_same_function():
    """Otherwise the default and the written value drift apart again."""
    source = (ROOT / "services/agents/base.py").read_text(encoding="utf-8")
    # Assignments only. A log line that formats the value is not one, and
    # matching it made this test fail on the correct code.
    import re
    executable = [
        line for line in source.splitlines()
        if re.match(r"\s*card\.consensus_weight\s*=[^=]", line)
        and not line.strip().startswith("#")
    ]
    assert executable, "update_scorecard no longer assigns a consensus weight"
    for line in executable:
        assert "consensus_weight_for(" in line, line


def test_the_engine_falls_back_to_this_default():
    """The consensus engine constructs a bare AgentScorecard for any agent with
    no stored card, which is every agent right now."""
    engine = (ROOT / "services/agents/consensus_engine.py").read_text(encoding="utf-8")
    assert "AgentScorecard(agent_name=agent_name)).consensus_weight" in engine

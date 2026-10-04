"""
tests/test_scorecard_survives_an_outage.py

Every agent scorecard expired during the outage.

Scorecards lived only in Redis with a 7-day TTL, and the platform was down for
nine: /agents/swarm returned no scorecards and every consensus weight fell back
to the unproven default. agent_predictions holds the same history durably, and
the wargamer's 91 graded predictions there had never reached its scorecard.
"""

import asyncio
import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.agents.prediction_scoring import (
    SCORECARD_TTL_SEC, AgentScorecard, PredictionScoringMixin, fold_outcome,
)


class _Agent(PredictionScoringMixin):
    name = "adversarial_wargamer"

    def _scorecard_key(self, strategy=None, regime=None):
        return f"sentinel:agents:scorecard:{self.name}"


def _agent(exists, rows):
    a = _Agent()
    a.logger = MagicMock()
    a.redis = MagicMock()
    a.redis.raw.exists = AsyncMock(return_value=exists)
    a.redis.raw.set = AsyncMock()
    a.db = MagicMock()
    a.db.query = AsyncMock(return_value=rows)
    return a


def test_the_ttl_outlasts_an_outage():
    assert SCORECARD_TTL_SEC >= 30 * 86400


def test_a_missing_scorecard_is_rebuilt_from_history():
    rows = [{"confidence": 0.8, "outcome_correct": True},
            {"confidence": 0.9, "outcome_correct": False},
            {"confidence": 0.0, "outcome_correct": False}]
    a = _agent(False, rows)
    assert asyncio.run(a.rebuild_scorecard_if_missing()) == 3
    key, blob = a.redis.raw.set.await_args.args
    card = json.loads(blob)
    assert key == "sentinel:agents:scorecard:adversarial_wargamer"
    assert card["predictions_made"] == 3 and card["predictions_correct"] == 1
    expected = ((0.8 - 1) ** 2 + 0.9 ** 2 + 0.5 ** 2) / 3
    assert abs(card["brier_score"] - expected) < 1e-9
    assert a.redis.raw.set.await_args.kwargs["ex"] == SCORECARD_TTL_SEC


def test_an_existing_scorecard_is_left_alone():
    a = _agent(True, [{"confidence": 0.8, "outcome_correct": True}])
    assert asyncio.run(a.rebuild_scorecard_if_missing()) is None
    a.db.query.assert_not_awaited()


def test_the_live_update_and_the_rebuild_share_one_fold():
    card = AgentScorecard(agent_name="x")
    fold_outcome(card, True, 0.8)
    fold_outcome(card, False, 0.6)
    assert card.predictions_made == 2
    assert abs(card.brier_score - ((0.2 ** 2) + (0.6 ** 2)) / 2) < 1e-9
    src = (ROOT / "services" / "agents" / "prediction_scoring.py").read_text(encoding="utf-8")
    apply = src.split("    async def _apply_outcome(")[1].split("    async def ")[0]
    assert "fold_outcome(card, prediction_correct, conviction)" in apply
    assert "ex=604800" not in src


def test_the_rebuild_runs_at_startup():
    src = (ROOT / "services" / "agents" / "base.py").read_text(encoding="utf-8")
    run = src.split("    async def run(self):")[1]
    assert run.index("rebuild_scorecard_if_missing()") < run.index("_resolve_predictions_loop()")

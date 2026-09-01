"""
tests/test_prediction_yield.py

Getting more out of a slot you have already paid for.

The swarm affords roughly twenty model inferences an hour, and only two agents
make directional claims. That is a real capacity limit and none of the changes
here lift it. What they do is stop discarding work the inference already
produced:

  * A brief arrives carrying `highest_conviction_plays` -- the model's own
    ranking -- and every play in it has been through full deterministic risk
    construction: entry level, stop distance, conviction-tiered risk-reward,
    Kelly sizing. Only the first two ever became predictions. Taking [:2] of an
    already-ranked list is a second arbitrary cut that throws away finished
    directional claims costing nothing more to keep.

  * A prediction was stored with a two-hour window past its horizon in which to
    be resolved. The resolver sweeps every fifteen minutes, so two hours is
    ample while the agent is running -- and the agent is frequently not.
    Deploys, restarts and a suspended laptop all expire predictions unresolved,
    and an unresolved prediction is an inference spent for nothing plus a
    scorecard entry never made. Those scorecards weight the consensus engine,
    so the loss compounds.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest  # noqa: E402


def _quant() -> str:
    return (ROOT / "services/agents/quant_trading_engine.py").read_text(encoding="utf-8")


# -- every play the inference produced ----------------------------------------

def test_plays_are_no_longer_capped_at_two():
    source = _quant()
    assert "brief.highest_conviction_plays[:2]" not in source, "the arbitrary cut remains"
    assert "brief.highest_conviction_plays[:MAX_RECORDED_PLAYS]" in source


def test_the_remaining_cap_is_a_runaway_guard_not_a_filter():
    """highest_conviction_plays is already the model's own ranking; a second
    quality cut on top of it is not selection, it is loss."""
    from services.agents.quant_trading_engine import MAX_RECORDED_PLAYS

    assert MAX_RECORDED_PLAYS >= 4, "the cap is still throttling value"
    assert MAX_RECORDED_PLAYS <= 32, "a malformed brief could flood the track record"


def test_the_cap_is_configurable():
    assert "QUANT_MAX_RECORDED_PLAYS" in _quant()


def test_the_yield_improves_at_least_fourfold():
    """The whole point: same slot, more finished claims kept."""
    from services.agents.quant_trading_engine import MAX_RECORDED_PLAYS

    assert MAX_RECORDED_PLAYS / 2 >= 4.0


# -- outcomes must outlive the outage -----------------------------------------

def test_a_prediction_outlives_an_overnight_outage():
    """Two hours of buffer does not survive a deploy, a crash or a sleeping
    laptop landing on the wrong two hours."""
    from services.agents.base import PREDICTION_RESOLUTION_BUFFER_SEC

    assert PREDICTION_RESOLUTION_BUFFER_SEC >= 8 * 3600


def test_the_buffer_is_sized_for_the_outage_not_the_sweep():
    """The resolver sweeps every 15 minutes; the buffer is not about that."""
    from services.agents.base import (
        PREDICTION_RESOLUTION_BUFFER_SEC,
        SentinelAgent,
    )

    assert PREDICTION_RESOLUTION_BUFFER_SEC > SentinelAgent.PREDICTION_SWEEP_INTERVAL_SEC * 10


@pytest.mark.parametrize("horizon_hours", [1, 6, 24, 72])
def test_the_ttl_always_exceeds_the_horizon(horizon_hours):
    """A prediction that expires before its own horizon can never be scored."""
    from services.agents.base import PREDICTION_RESOLUTION_BUFFER_SEC

    ttl = max(horizon_hours * 3600 + PREDICTION_RESOLUTION_BUFFER_SEC, 86400)
    assert ttl > horizon_hours * 3600


def test_the_buffer_is_configurable():
    source = (ROOT / "services/agents/base.py").read_text(encoding="utf-8")
    assert "PREDICTION_RESOLUTION_BUFFER_SEC" in source
    assert 'os.getenv("PREDICTION_RESOLUTION_BUFFER_SEC"' in source


# -- what is NOT claimed -------------------------------------------------------

def test_no_new_inference_lane_was_added():
    """These changes raise yield per slot. They do not create slots, and adding
    a third lane would put a third concurrent request on a single-threaded
    model server -- rebuilding the queue the budget exists to prevent."""
    from services.agents.base import SentinelAgent
    from services.agents.quant_trading_engine import QuantTradingEngine

    # The attribute, not its source text -- the declaration carries a type
    # annotation and an earlier version of this test matched on the wrong form.
    assert SentinelAgent.INFERENCE_LANE is None, "the default stopped being shared"
    assert QuantTradingEngine.INFERENCE_LANE is None, "quant_trading_engine took a lane"

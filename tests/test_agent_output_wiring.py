"""
tests/test_agent_output_wiring.py

"No bulletin has ever completed end to end" had two causes, and only one of
them was capacity.

The swarm affords roughly twenty model decisions an hour, so it is genuinely
capacity-bound -- but that explanation was hiding a second, cheaper fault.
RadarAgent appeared in neither the publish_bulletin nor the record_prediction
call graph, and RadarAgent is the one agent that reliably wins an inference
slot, because it holds a reserved lane. The agents that do publish bulletins
are precisely the ones that rarely get a slot at all.

So the working agent was never wired to the output, and the wired agents never
worked. Attributing the whole silence to capacity would have bought hardware to
fix a missing function call.

Predictions remain capacity-bound and that is not papered over here: only
adversarial_wargamer and quant_trading_engine make directional claims, and both
compete on the shared budget. RadarAgent deliberately does not record one --
see below.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest  # noqa: E402


def _radar() -> str:
    return (ROOT / "services/agents/radar_agent.py").read_text(encoding="utf-8")


# -- the missing call ---------------------------------------------------------

def test_an_escalation_publishes_a_bulletin():
    """The agent that wins slots must reach the output the swarm reads."""
    source = _radar()
    assert "await self.publish_bulletin(" in source


def test_the_bulletin_carries_the_evidence_behind_it():
    """A thesis a reader cannot check is not worth publishing."""
    source = _radar()
    block = source[source.index("await self.publish_bulletin("):]
    block = block[: block.index("ttl_seconds")]
    for field in ('"z_score"', '"notional_usd"', '"regime"', '"rationale"'):
        assert field in block, f"the bulletin omits {field}"


def test_the_bulletin_names_the_instrument():
    source = _radar()
    block = source[source.index("await self.publish_bulletin("):]
    assert "ticker=ticker" in block[:600]


# -- what it deliberately does NOT do -----------------------------------------

def test_an_escalation_does_not_record_a_directional_prediction():
    """RadarDecision is {investigate, rationale}.

    It says an instrument deserves attention, not which way it will move.
    Recording a direction would invent a claim the agent never made -- and the
    resolver would then score a track record against that invention, which is
    the exact failure this codebase has spent its time removing.
    """
    source = _radar()
    assert "record_prediction(" not in source


def test_the_reason_for_that_is_written_down():
    """A future reader will otherwise 'fix' the omission."""
    source = _radar()
    assert "Deliberately NOT a prediction" in source


# -- conviction is derived, not asserted --------------------------------------

def test_conviction_comes_from_a_measurement():
    """publish_bulletin defaults conviction to 0.5. A constant tells a reader
    nothing; the z-score at least says how unusual the flow was."""
    source = _radar()
    block = source[source.index("await self.publish_bulletin("):]
    assert "conviction=min(1.0, max(0.0, z_score / 10.0))" in block[:600]


@pytest.mark.parametrize("z,expected", [(0.0, 0.0), (4.49, 0.449), (10.0, 1.0), (25.0, 1.0)])
def test_conviction_is_bounded(z, expected):
    assert min(1.0, max(0.0, z / 10.0)) == pytest.approx(expected)


# -- the honest half of the original claim ------------------------------------

def test_predictions_are_still_only_made_by_directional_agents():
    """Documented so the capacity limit is not mistaken for another wiring gap.

    If a third agent starts recording predictions, this test should be updated
    deliberately rather than silently.
    """
    callers = {
        path.name
        for path in (ROOT / "services/agents").glob("*.py")
        if "record_prediction(" in path.read_text(encoding="utf-8")
        and "async def record_prediction" not in path.read_text(encoding="utf-8")
    }
    assert callers == {"adversarial_wargamer.py", "quant_trading_engine.py"}


# -- a bulletin nobody can attribute is outside every comparison ---------------

def test_the_intel_bulletin_names_its_subject():
    """The first bulletin this system ever produced end to end had
    primary_entity_id and ticker both null.

    The consensus engine fuses bulletins by entity, so an unattributed one
    cannot be corroborated or contradicted by anything -- it reaches the swarm
    and then sits outside every comparison the swarm exists to make.
    """
    source = (ROOT / "services/agents/knowledge_graph_engine.py").read_text(encoding="utf-8")
    block = source[source.index("asyncio.create_task(self.publish_bulletin("):]
    block = block[: block.index("ttl_seconds")]
    assert 'primary_entity_id=message.get("primary_entity_id")' in block
    assert 'primary_entity_name=message.get("primary_entity_name")' in block

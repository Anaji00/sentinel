"""Keeps the knowledge-graph engine reasoning about intelligence, not plumbing.

The engine shares the ontology topic with the GraphSupervisor. Supervisor
messages are commands -- merge this node, link these two -- and carry no prose.
Having no headline, they fell through to a fallback that fabricated one
("Intelligence update for entity X") and, having no anomaly_score, defaulted to
0.50, which clears the 0.20 skip threshold. Every command therefore bought a
full LLM round trip to reason about a placeholder sentence.

Measured before the fix: 338 of 335 recently processed messages were that
synthesized headline, while the consumer sat 144,000 messages behind and losing
ground at roughly 250 a minute.
"""
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.agents.knowledge_graph_engine import _SUPERVISOR_GRAPH_ACTIONS  # noqa: E402


def _engine():
    """The handler under test, with its LLM and IO dependencies left unset.

    Any message that reaches inference would raise on them, which is what makes
    "returns None without touching them" a meaningful assertion.
    """
    from services.agents.knowledge_graph_engine import KnowledgeGraphEngine
    return KnowledgeGraphEngine.__new__(KnowledgeGraphEngine)


@pytest.mark.anyio
@pytest.mark.parametrize("action", sorted(_SUPERVISOR_GRAPH_ACTIONS))
async def test_supervisor_commands_are_skipped_without_inference(action):
    engine = _engine()
    message = {
        "entity_id": "271044408",
        "action": action,
        "data": {"label": "Vessel", "relation_type": "REGISTERED_IN", "target_id": "LR"},
    }
    assert await engine.handle(message) is None, f"{action} reached the reasoning path"


@pytest.mark.anyio
async def test_a_message_with_no_prose_is_not_given_a_fabricated_one():
    """Synthesizing a headline from an id hands the model a sentence with no
    information in it; anything returned is invention, not extraction."""
    engine = _engine()
    assert await engine.handle({"entity_id": "AAPL"}) is None
    assert await engine.handle({"primary_entity": "NVDA"}) is None
    assert await engine.handle({"ticker": "TSM"}) is None


@pytest.mark.anyio
async def test_low_anomaly_intelligence_is_still_skipped():
    """The existing cheap filter must survive the change."""
    engine = _engine()
    msg = {"headline": "Something mildly interesting happened", "anomaly_score": 0.05}
    assert await engine.handle(msg) is None


def test_the_action_list_matches_what_the_supervisor_branches_on():
    """A command the supervisor handles but this list omits goes back to burning
    inference; one that is listed but unhandled silently drops real work."""
    import re
    src = (ROOT / "services/agents/supervisor.py").read_text(encoding="utf-8")
    handled = set()
    for m in re.finditer(r'action\s*(?:==|in)\s*\(?((?:"[A-Z_]+"(?:,\s*)?)+)\)?', src):
        handled.update(re.findall(r'"([A-Z_]+)"', m.group(1)))
    assert handled, "could not read the supervisor's action vocabulary"
    missing = handled - _SUPERVISOR_GRAPH_ACTIONS
    assert not missing, f"supervisor handles {missing}, which would still reach inference"

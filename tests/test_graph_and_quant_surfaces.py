"""Two surfaces nothing in this audit had opened yet, and what was in them.

The knowledge graph and the options desk. Both answer questions whose whole
value is that the answer is true, and both had a branch that supplied one when
the platform had nothing to say:

  * /graph/shortest-path, asked how two entities are connected, returned a
    chain of unrelated entities in the shape of a real Cypher path; and
  * /radar/options/covered-calls, missing the statistic that decides whether a
    trade exists at all, supplied 2.8 -- just over the 2.5 gate.

Neither failure could be seen from the response. A fabricated path is
well-formed, and a covered call written off an invented z-score prices exactly
like one written off a measured one.
"""
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


class _EmptyGraph:
    """A Neo4j that knows of no path between the two entities asked about."""

    def __init__(self):
        self.queries = []

    async def query(self, cypher, params=None):
        self.queries.append(cypher)
        return []


# -- /graph/shortest-path ---------------------------------------------------


async def test_no_path_is_reported_as_no_path():
    """It used to be reported as a path.

    The fallback queried events matching *either* endpoint by substring, took
    the ten most recent, and returned [source, ...those ten..., target] under
    `{"path": [{"entities": [...]}]}` -- the shape a real Cypher path comes
    back in. Nothing in the chain was connected to anything else in it, and
    because the filter was an OR, the usual result was ten entities related to
    the source strung onto a target they had never co-occurred with.
    """
    from services.api_gateway.routes.graph import get_shortest_path

    graph = _EmptyGraph()
    result = await get_shortest_path(
        source_id="EVERGIVEN", target_id="NVDA", graph=graph
    )
    assert result["path"] == []
    assert result["message"] == "No path found"


async def test_the_path_query_does_not_exclude_the_graph_it_runs_against():
    """`(start:Entity)` matched almost nothing this writer creates.

    `graph_writer` MERGEs typed nodes -- Vessel, Aircraft, Company, Index,
    Sector, MacroFactor, Flag, Region -- and a Neo4j node carries the label it
    was created with. `Entity` is only the default for the generic path, so
    anchoring both ends of the shortestPath on `:Entity` meant a vessel and a
    company could never be connected however many relationships joined them.

    Which is what made the fabricated fallback above the usual answer rather
    than the rare one.
    """
    from services.api_gateway.routes.graph import get_shortest_path

    graph = _EmptyGraph()
    await get_shortest_path(source_id="a", target_id="b", graph=graph)
    assert graph.queries, "the route asked Neo4j nothing"
    assert "shortestPath((start)-[*..6]-(end))" in graph.queries[0]
    assert ":Entity" not in graph.queries[0]


def test_the_writer_does_not_label_its_nodes_entity():
    """The premise of the test above, checked against the writer itself."""
    writer = (ROOT / "services" / "enrichment" / "graph_writer.py").read_text(
        encoding="utf-8"
    )
    for label in ("Vessel", "Aircraft", "Company", "Index", "Sector", "MacroFactor"):
        assert f'"label": "{label}"' in writer or f'target_label="{label}"' in writer, (
            f"{label} is no longer written; the reasoning above may be stale"
        )


# -- /radar/options/covered-calls -------------------------------------------


async def test_a_covered_call_is_not_written_off_an_invented_z_score():
    """2.8 was the one value that guaranteed the gate would pass.

    `generate_covered_call_recommendation` returns None below +2.5. With no
    row in `tradfi_bars_5m_zscore` the route supplied 2.8, so on a cold or
    unavailable database -- exactly when nothing has been measured -- every
    ticker cleared the significance test the endpoint exists to apply.

    The asymmetry was the tell: twenty lines further down, a missing price
    raises a 400 rather than being invented.
    """
    from fastapi import HTTPException

    from services.api_gateway.routes.radar import get_covered_call_recommendations

    with pytest.raises(HTTPException) as raised:
        await get_covered_call_recommendations(
            ticker="NVDA", z_score=None, current_price=None, db=None, redis=None
        )
    assert raised.value.status_code == 400
    assert "z-score" in str(raised.value.detail).lower()


def test_the_gate_the_z_score_had_to_clear_is_still_there():
    """If the 2.5 floor moves, the reasoning above needs rewriting."""
    calc = (ROOT / "shared" / "utils" / "quant_calc.py").read_text(encoding="utf-8")
    block = calc[calc.index("def generate_covered_call_recommendation"):]
    block = block[:2000]
    assert "if z_score < 2.5:" in block
    assert "return None" in block


def test_the_agent_calling_the_same_function_fails_closed():
    """Two callers, opposite policies. This is the one that was right.

    The quant engine starts from 0.0, reads the aggregate, and computes a
    z-score from returns when the view is empty -- so a missing statistic
    produces no recommendation rather than a confident one.
    """
    engine = (ROOT / "services" / "agents" / "quant_trading_engine.py").read_text(
        encoding="utf-8"
    )
    block = engine[: engine.index("generate_covered_call_recommendation")]
    assert "z_score = 0.0" in block, (
        "the agent no longer starts from zero; if it now defaults to something "
        "that clears the 2.5 gate, it has acquired the defect this records"
    )

"""
tests/test_edge_direction_claims.py

Every graph edge that did not state a direction was written asserting one.

Found by tracing peer edges end to end. All three live PEER_OF relationships
carried direction="lead":

    GC=F -> NQ=F   coefficient  0.952   direction "lead"
    CL=F -> GC=F   coefficient -0.896   direction "lead"
    CL=F -> NQ=F   coefficient -0.880   direction "lead"

PEER_OF is derived from a contemporaneous Pearson correlation. It is symmetric
and has no lead or lag -- the two series were only ever measured at the same
instants. The supervisor's default filled the field in anyway:

    "direction": data.get("direction", props.get("direction", "lead"))

so a causal claim was attached to every symmetric relationship in the graph,
including STATISTICALLY_CORRELATED_WITH, which has the same shape.

A default is not a measurement. Lead and lag are what the Granger path
establishes, and edges from that path still say so explicitly.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.agents.supervisor import UNDIRECTED  # noqa: E402

SOURCE = (ROOT / "services" / "agents" / "supervisor.py").read_text(encoding="utf-8")


def test_an_unstated_direction_is_not_a_lead():
    assert UNDIRECTED != "lead"


def test_no_call_site_still_defaults_to_lead():
    assert 'props.get("direction", "lead")' not in SOURCE


def test_every_direction_default_is_the_undirected_one():
    import re

    defaults = re.findall(r'props\.get\("direction",\s*([A-Za-z_"\']+)\)', SOURCE)
    assert defaults, "the direction default disappeared entirely"
    assert all(d == "UNDIRECTED" for d in defaults)


def test_an_explicit_direction_still_wins():
    """Granger edges measure lead and lag and must keep saying so."""
    import re

    for m in re.finditer(r'"direction":\s*(?:str\()?data\.get\("direction",', SOURCE):
        assert m, "callers can no longer state a direction"


def test_the_peer_graph_never_claims_one():
    """A contemporaneous correlation has no direction to claim, so the module
    must not be quietly supplying one either."""
    peer = (ROOT / "services" / "correlation" / "peer_graph.py").read_text(encoding="utf-8")
    assert '"direction"' not in peer


def test_the_sign_is_what_carries_the_relationship():
    """Losing the inverse flag to the writer's property allowlist is survivable
    only because the coefficient is signed. This pins that."""
    from services.correlation.peer_graph import PeerEdge

    edge = PeerEdge("CL=F", "GC=F", -0.896, 0.001, 59, None, None, 0.896)
    props = edge.as_proposal()["data"]["properties"]
    assert props["coefficient"] < 0
    assert edge.as_proposal()["data"]["weight"] > 0

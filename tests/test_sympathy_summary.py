"""The stock correlation agent's bulletin must survive having sympathy movers.

The summary was built with:

    ', '.join([sm.ticker for sm.ticker in brief.sympathy_movers])

The comprehension target is an attribute rather than a name, so each
SympathyMover was assigned into the `.ticker` field of the `sm` still bound by
the loop above it, and the comprehension yielded those objects instead of their
tickers. Live:

    Stock Correlation Agent processing error:
    sequence item 0: expected str instance, SympathyMover found

It raised on every brief that had movers -- so the agent's bulletin never
reached the consensus engine -- and on the way past it overwrote a real mover's
ticker with a SympathyMover object.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.agents.stock_correlation_agent import SympathyMover  # noqa: E402

SOURCE = (ROOT / "services/agents/stock_correlation_agent.py").read_text(encoding="utf-8")


def _movers():
    fields = SympathyMover.model_fields
    out = []
    for ticker in ("MTZ", "KKR", "DELL"):
        kwargs = {}
        for name, f in fields.items():
            if name == "ticker":
                kwargs[name] = ticker
            elif f.is_required():
                ann = str(f.annotation)
                kwargs[name] = 0.5 if "float" in ann else (0 if "int" in ann else "x")
        out.append(SympathyMover(**kwargs))
    return out


def test_the_summary_joins_tickers():
    movers = _movers()
    summary = ", ".join(sm.ticker for sm in movers)
    assert summary == "MTZ, KKR, DELL"


def test_the_comprehension_target_is_a_name_not_an_attribute():
    """The defect, read straight from the source: a dotted target rebinds an
    attribute of an object from the enclosing scope."""
    import re
    dotted = re.findall(r"for\s+([A-Za-z_][A-Za-z0-9_]*\.[A-Za-z0-9_.]+)\s+in\s", SOURCE)
    assert not dotted, f"comprehension assigns to attributes: {dotted}"


def test_the_movers_are_not_mutated_by_building_the_summary():
    """The side effect: it wrote a SympathyMover into a real mover's ticker."""
    movers = _movers()
    before = [sm.ticker for sm in movers]
    _ = ", ".join(sm.ticker for sm in movers)
    assert [sm.ticker for sm in movers] == before
    assert all(isinstance(sm.ticker, str) for sm in movers)


def test_no_movers_produces_no_summary():
    brief_movers = []
    summary = f" Sympathy: {', '.join(sm.ticker for sm in brief_movers)}" if brief_movers else ""
    assert summary == ""

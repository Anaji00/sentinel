"""
tests/test_no_constant_graph_nodes.py

Every high-scoring headline merged the same graph node.

The news enricher proposed MERGE_ONTOLOGY_NODE for the constant entity
"High_Impact_News", with no edge to the headline or anything it named, so each
write overwrote the previous headline's confidence on a node nothing could
reach. Its label, NewsEvent, is not canonical, so the live graph held two such
nodes -- NewsEvent and Entity -- both at degree 0, rewritten about five times an
hour through the supervisor.
"""

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _proposals_with_literal_entity(path: Path):
    """Dict literals carrying a node action and a hard-coded entity_id."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Dict):
            continue
        keys = {k.value: v for k, v in zip(node.keys, node.values) if isinstance(k, ast.Constant)}
        action = keys.get("action")
        entity = keys.get("entity_id")
        if (
            isinstance(action, ast.Constant) and action.value == "MERGE_ONTOLOGY_NODE"
            and isinstance(entity, ast.Name)
        ):
            found.append((node.lineno, entity.id))
    return found


def test_the_news_enricher_proposes_no_constant_node():
    src = (ROOT / "services" / "enrichment" / "enrichers" / "news.py").read_text(encoding="utf-8")
    code = "\n".join(l for l in src.splitlines() if not l.lstrip().startswith("#"))
    assert "High_Impact_News" not in code
    assert '"label": "NewsEvent"' not in code


def test_no_enricher_merges_a_node_named_by_a_constant():
    offenders = []
    for path in (ROOT / "services" / "enrichment").rglob("*.py"):
        src = path.read_text(encoding="utf-8")
        tree = ast.parse(src)
        constants = {
            t.id for n in ast.walk(tree) if isinstance(n, ast.Assign)
            for t in n.targets if isinstance(t, ast.Name)
            and isinstance(n.value, ast.Constant) and isinstance(n.value.value, str)
        }
        for line, name in _proposals_with_literal_entity(path):
            if name in constants:
                offenders.append(f"{path.relative_to(ROOT)}:{line} entity_id={name}")
    assert offenders == []

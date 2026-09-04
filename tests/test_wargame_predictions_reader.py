"""agent_predictions must have a reader.

Nothing anywhere in the codebase read this table. That is precisely why five of
its six columns could sit at their defaults across all 30 rows without anything
raising: prediction_id was the literal "wargame_sim", confidence was 0.0 with a
standard deviation of 0.000, and correlation_id was empty, so no prediction
could be joined back to the cluster that caused it.

A write-only table cannot report that what it holds is wrong. The field mapping
is fixed; this is what makes the fix observable.
"""

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

ROUTE = ROOT / "services/api_gateway/routes/agents.py"
SOURCE = ROUTE.read_text(encoding="utf-8")


def test_the_table_is_read_somewhere():
    hits = []
    for path in (ROOT / "services").rglob("*.py"):
        text = path.read_text(encoding="utf-8", errors="ignore")
        if "agent_predictions" not in text:
            continue
        for line in text.splitlines():
            if "agent_predictions" in line and "FROM agent_predictions" in line:
                hits.append(str(path.relative_to(ROOT)))
    assert hits, "no SELECT reads agent_predictions anywhere"


def test_the_response_exposes_the_predictions():
    assert '"wargame_predictions": wargame_predictions,' in SOURCE


def test_the_uuid_is_validated_before_it_is_cast():
    """Rows written before the mapping fix hold an empty correlation_id, and
    casting an empty string to uuid raises rather than returning null."""
    assert "correlation_id ~ " in SOURCE and "::uuid" in SOURCE
    guard = SOURCE[SOURCE.index("WITH recent AS"):SOURCE.index("LEFT JOIN correlations")]
    assert "CASE WHEN" in guard, "the cast is not guarded by a pattern check"


def test_the_query_is_bounded():
    """This runs on an operator dashboard request."""
    query = SOURCE[SOURCE.index("WITH recent AS"):SOURCE.index("ORDER BY r.occurred_at")]
    assert "LIMIT" in query


def test_the_probability_is_not_relabelled_as_confidence():
    """The stored number is the wargamer's cascade probability. Calling it
    confidence in the response would quietly change what it claims."""
    assert '"cascade_probability": row.get("confidence")' in SOURCE
    assert '"confidence": row.get("confidence")' not in SOURCE


def test_a_failure_returns_empty_rather_than_breaking_the_dashboard():
    tree = ast.parse(SOURCE)
    fn = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.AsyncFunctionDef) and n.name == "get_swarm_intelligence"
    )
    handlers = [n for n in ast.walk(fn) if isinstance(n, ast.Try)]
    assert handlers, "the new query is not guarded"
    assert "wargame_predictions = []" in SOURCE


def test_the_route_actually_receives_a_database():
    tree = ast.parse(SOURCE)
    fn = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.AsyncFunctionDef) and n.name == "get_swarm_intelligence"
    )
    args = [a.arg for a in fn.args.args + fn.args.kwonlyargs]
    assert "db" in args, f"no db dependency on the route: {args}"

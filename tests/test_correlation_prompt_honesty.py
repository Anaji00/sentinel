"""The correlation agent must not assert a relationship it has not measured.

Three fabrications sat in one path:

  coalesce(r.coefficient, r.weight, 1.0)  an unmeasured edge became a PERFECT
                                          correlation
  coalesce(r.p_value, 0.05)               and one significant at exactly 5%
  empty case                              "Live continuous correlation matrix
                                          active" -- asserting statistics
                                          exist, with none supplied

and the task beneath them read "explain WHY the empirically measured
correlation between X and Y exists", then "ground your explanation directly in
the empirical statistics provided above". With no data the model was told the
statistics were live, that a correlation existed, to explain it, and to ground
the explanation in what it had not been given. It can only confabulate.
"""

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

SOURCE = (ROOT / "services/agents/stock_correlation_agent.py").read_text(encoding="utf-8")


def test_the_query_no_longer_invents_a_perfect_coefficient():
    assert "coalesce(r.coefficient, r.weight, 1.0)" not in SOURCE
    assert "coalesce(r.coefficient, r.weight)" in SOURCE


def test_the_query_no_longer_invents_significance():
    assert "coalesce(r.p_value, 0.05)" not in SOURCE
    assert "r.p_value AS p_val" in SOURCE


def test_an_unmeasured_coefficient_is_labelled_as_such():
    assert "Coef: unmeasured" in SOURCE


def _string_constants():
    """Strings the module actually contains, parsed rather than grepped.

    A text search matches the phrase inside the comment explaining why it was
    removed, which is the prose worth keeping.
    """
    return [
        n.value for n in ast.walk(ast.parse(SOURCE))
        if isinstance(n, ast.Constant) and isinstance(n.value, str)
    ]


def test_the_empty_case_says_nothing_was_measured():
    joined = chr(10).join(_string_constants())
    assert "Live continuous correlation matrix active" not in joined
    assert "Grounded Empirical Statistics: NONE" in joined


def test_the_task_no_longer_asserts_a_measured_correlation():
    assert "WHY the empirically measured correlation" not in SOURCE


def test_the_task_varies_with_the_evidence():
    assert "{mechanism_task}" in SOURCE
    assert "{grounding_task}" in SOURCE
    assert "Do not assert that a correlation exists" in SOURCE


def test_both_branches_define_both_tasks():
    """A branch that leaves one undefined raises inside the message loop."""
    tree = ast.parse(SOURCE)
    for node in ast.walk(tree):
        if not isinstance(node, ast.If):
            continue
        names = {
            t.id
            for stmt in node.body if isinstance(stmt, ast.Assign)
            for t in stmt.targets if isinstance(t, ast.Name)
        }
        if "mechanism_task" not in names:
            continue
        else_names = {
            t.id
            for stmt in node.orelse if isinstance(stmt, ast.Assign)
            for t in stmt.targets if isinstance(t, ast.Name)
        }
        assert {"empirical_block", "mechanism_task", "grounding_task"} <= names
        assert {"empirical_block", "mechanism_task", "grounding_task"} <= else_names
        return
    raise AssertionError("the evidence-dependent branch was not found")


def test_the_module_still_parses_and_defines_the_agent():
    tree = ast.parse(SOURCE)
    classes = {n.name for n in ast.walk(tree) if isinstance(n, ast.ClassDef)}
    assert "StockCorrelationAgent" in classes or any("Correlation" in c for c in classes)

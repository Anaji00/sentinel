"""
tests/test_consensus_publishing.py

One report, one send, and a lookup that does not write.

Measured on the live topic, 250 consecutive messages from offset 65100:

    distinct report_ids   129
    republished           121   (48.4%)

`analyze()` sent the report to Topics.CONSENSUS_REPORTS itself, and `handle()`
returns the same report -- which the agent framework sends to
`self.output_topic`, which this agent declares as Topics.CONSENSUS_REPORTS. Two
sends of one report inside the one second its id is built from.

The framework path is the one to keep: it also broadcasts the result to Redis
for the live UI, which a direct send skips. The scheduled path is the exception,
because `_scheduled_review_loop` logs what `run_scheduled_review` returns rather
than publishing it -- which is why the direct send existed at all.

Two read-only lookups, `get_consensus_for_ticker` and `get_contradictions`,
also called `analyze()` and so published a full report to answer a question
about one ticker.
"""

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

ENGINE = ROOT / "services" / "agents" / "consensus_engine.py"


def _tree():
    return ast.parse(ENGINE.read_text(encoding="utf-8"))


def _method(name: str):
    for node in ast.walk(_tree()):
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)) and node.name == name:
            return node
    raise AssertionError(f"{name} is not defined")


def _analyze_calls(node) -> list:
    """Every `self.analyze(...)` inside a method, with its publish argument."""
    out = []
    for sub in ast.walk(node):
        if not isinstance(sub, ast.Call):
            continue
        func = sub.func
        if isinstance(func, ast.Attribute) and func.attr == "analyze":
            publish = None
            for kw in sub.keywords:
                if kw.arg == "publish":
                    publish = getattr(kw.value, "value", None)
            out.append(publish)
    return out


def test_analyze_does_not_publish_unless_asked():
    """An analysis function with a send in it is a read that writes."""
    node = _method("analyze")
    args = [a.arg for a in node.args.args]
    assert "publish" in args, "publishing must be something the caller chooses"
    defaults = {
        a.arg: getattr(d, "value", None)
        for a, d in zip(node.args.args[-len(node.args.defaults):], node.args.defaults)
    }
    assert defaults.get("publish") is False, "and the default must be not to"


def test_the_handler_lets_the_framework_publish():
    """Returning the report AND sending it is how 121 of 250 were duplicates.

    The framework send also broadcasts to Redis for the live UI; a direct send
    from inside analyze() skips that.
    """
    assert _analyze_calls(_method("handle")) == [None], (
        "handle() must call analyze() without publishing, and return the report"
    )


def test_the_scheduled_review_publishes_because_nothing_else_will():
    assert _analyze_calls(_method("run_scheduled_review")) == [True], (
        "_scheduled_review_loop logs the return rather than sending it"
    )


def test_a_lookup_does_not_publish_a_report():
    for name in ("get_consensus_for_ticker", "get_contradictions"):
        assert _analyze_calls(_method(name)) == [None], (
            f"{name} answers a question; it must not emit a full report to do it"
        )


def test_the_send_is_guarded_by_the_flag():
    """Read from the source, because the guard is the whole fix."""
    code = ENGINE.read_text(encoding="utf-8")
    assert "if publish and producer and (" in code, (
        "the unconditional send is what published every report twice"
    )

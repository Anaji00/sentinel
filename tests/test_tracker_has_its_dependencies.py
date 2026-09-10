"""Three closed mechanisms, all inert, none of them saying so.

`ScenarioTracker.__init__` takes `(db, producer=None, redis=None)` and
`services/reasoning/main.py` constructed it with two arguments. `self._redis`
was therefore None for the life of the service, and everything needing it
returned early and silently:

  * the calibration outcome write closed in Phase 4.12
  * the open-questions offer that gap 5 turns on
  * the resolved-history backfill that gap 4 turns on

Beneath that, two sites read `self.redis` -- no underscore, an attribute this
class has never defined -- so they raised AttributeError into a handler logging
at DEBUG, which this deployment does not print. Either defect alone was enough;
together they made "562 resolved scenarios, 0 calibration samples" a fact about
the wiring rather than about the history.

Neither is visible from the code. `check_all` ran, the sweep logged normally,
and the two additions executed and did nothing.
"""
import ast
import inspect
import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
TRACKER_SRC = (ROOT / "services" / "reasoning" / "scenario_tracker.py").read_text(encoding="utf-8")
MAIN_SRC = (ROOT / "services" / "reasoning" / "main.py").read_text(encoding="utf-8")


def test_the_tracker_is_constructed_with_every_dependency_it_declares():
    import services.reasoning.scenario_tracker as st

    params = [p for p in inspect.signature(st.ScenarioTracker.__init__).parameters if p != "self"]

    call = None
    for node in ast.walk(ast.parse(MAIN_SRC)):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id == "ScenarioTracker"):
            call = node
    assert call is not None, "ScenarioTracker is no longer constructed in main"

    supplied = len(call.args) + len(call.keywords)
    assert supplied >= len(params), (
        f"ScenarioTracker declares {params} and is built with {supplied} argument(s). "
        "A dependency defaulting to None makes every path that needs it return "
        "early and silently."
    )


def test_nothing_reaches_for_an_attribute_the_class_never_sets():
    """`self.redis` is not `self._redis`, and the difference was invisible."""
    body = "\n".join(
        l for l in TRACKER_SRC.splitlines() if not l.strip().startswith("#")
    )
    assert "self.redis" not in body, (
        "an attribute this class does not define; every call raises "
        "AttributeError into a DEBUG-level handler"
    )


def test_the_redis_handle_has_a_fallback_where_it_is_used():
    """One site already did this correctly and two did not."""
    assert TRACKER_SRC.count("self._redis or (await get_redis())") >= 3


@pytest.mark.parametrize(
    "method", ["_offer_open_questions", "_record_correlation_confidence_outcome"]
)
def test_the_paths_that_need_redis_still_guard_against_its_absence(method):
    """Fixing the wiring must not turn a silent skip into a crash."""
    import services.reasoning.scenario_tracker as st

    fn = getattr(st.ScenarioTracker, method, None)
    assert fn is not None
    src = inspect.getsource(fn)
    assert "None" in src or "try" in src

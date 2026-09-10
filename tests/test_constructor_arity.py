"""Is this reachable *with the arguments it needs*?

`check_reachability.py` answers whether code sits on an execution path. It
cannot answer the question that made three mechanisms closed in this audit run
inert in a single pass:

    class ScenarioTracker:
        def __init__(self, db, producer=None, redis=None): ...

    tracker = ScenarioTracker(db, tracker_producer)     # two arguments

`self._redis` was None for the life of the service, so the Phase 4.12
calibration write, the open-questions offer and the resolved-history backfill
all returned early and silently. The call is valid Python; pyflakes sees a
correct call; the reachability check sees a reachable class; and the unit tests
pass because they construct the class themselves, with everything it wants.

This runs in the suite rather than as a script you remember to invoke. That is
the other half of the lesson: the undefined-name sweep has caught its class
three times and lives in the suite, and I still crash-looped a service by
deploying between writing a line and running the tests. A guard you have to
remember is one you skip exactly when you are moving fast.
"""
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from check_constructor_arity import scan  # noqa: E402

# Call sites that omit a None-defaulting dependency for a stated reason.
#
# Both are Binance streams, which carry their subscription in the URL and need
# no post-connect frame. The two sockets that do need one pass `on_connect`.
ALLOWED = {
    ("services", "collector-crypto", "main.py", 196),
    ("services", "collector-crypto", "main.py", 330),
}


def _key(finding: str):
    """(parts..., line) for a finding, so the separator is not part of the match."""
    location = finding.split(":", 2)
    path = pathlib.PurePath(location[0].replace("\\", "/"))
    return tuple(path.parts) + (int(location[1]),)


def test_every_dependency_a_class_reads_is_supplied_where_it_is_built():
    findings = [f for f in scan() if _key(f) not in ALLOWED]
    assert not findings, (
        "constructor call(s) omitting a dependency the class reads:\n  "
        + "\n  ".join(findings)
        + "\n\nA parameter defaulting to None makes every path that needs it return "
          "early and silently, while the unit tests keep passing."
    )


def test_the_check_would_have_caught_the_defect_that_motivated_it():
    """Driven against the original shape, not asserted about."""
    import ast
    import textwrap

    import check_constructor_arity as mod

    src = textwrap.dedent(
        """
        class Tracker:
            def __init__(self, db, producer=None, redis=None):
                self._db = db
                self._producer = producer
                self._redis = redis

            async def sweep(self):
                if self._redis is None:
                    return
                await self._redis.get("x")

        t = Tracker(db, producer)
        """
    )
    tree = ast.parse(src)
    cls = next(n for n in ast.walk(tree) if isinstance(n, ast.ClassDef))
    init = mod._init_of(cls)

    params = dict(mod._params(init))
    assert params["redis"] is True, "a None default must be recognised as absence"
    assert params["db"] is False, "a required parameter is not a None default"

    attr = mod._attribute_for(init, "redis")
    assert attr == "_redis"
    assert mod._reads_attribute(cls, attr, init), (
        "the class reads self._redis outside __init__, which is what makes the "
        "omission consequential"
    )


def test_a_tuning_default_is_not_reported_as_a_missing_dependency():
    """61 findings became 2 by drawing this line; it has to hold."""
    import ast
    import textwrap

    import check_constructor_arity as mod

    src = textwrap.dedent(
        """
        class Socket:
            def __init__(self, url, max_backoff=30.0, queue_size=100):
                self.url = url
                self.max_backoff = max_backoff
                self.queue_size = queue_size

            def wait(self):
                return self.max_backoff + self.queue_size
        """
    )
    cls = next(n for n in ast.walk(ast.parse(src)) if isinstance(n, ast.ClassDef))
    params = dict(mod._params(mod._init_of(cls)))
    assert params["max_backoff"] is False
    assert params["queue_size"] is False


def test_a_parameter_stored_and_never_read_is_not_reported():
    import ast
    import textwrap

    import check_constructor_arity as mod

    src = textwrap.dedent(
        """
        class Thing:
            def __init__(self, a, unused=None):
                self.a = a
                self._unused = unused
        """
    )
    cls = next(n for n in ast.walk(ast.parse(src)) if isinstance(n, ast.ClassDef))
    init = mod._init_of(cls)
    assert not mod._reads_attribute(cls, "_unused", init)

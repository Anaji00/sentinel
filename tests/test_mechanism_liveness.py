"""
tests/test_mechanism_liveness.py

Has this mechanism ever actually run?

Across 631 audit findings the most common shape by a wide margin was a mechanism
that was built correctly, wired correctly, deployed, and had never once
executed: the admission bar that never refused, the focus set that never reached
tradfi, the volume/open-interest chain that received nothing, Black-Litterman
with one caller that was a unit test, a vector index nobody had pruned. None was
visible from reading the code, because the code was right every time.

The registry asks that question continuously. These tests hold the two
properties that decide whether it is worth having: that a declared mechanism
which never fires is *visible*, and that firing one is cheap enough to put on a
hot path.
"""

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import shared.utils.liveness as liveness


class _FakePipe:
    def __init__(self, store):
        self.store = store
        self.ops = []

    def hsetnx(self, key, field, value):
        self.ops.append(("hsetnx", key, field, value))

    def hset(self, key, field, value):
        self.ops.append(("hset", key, field, value))

    def hincrby(self, key, field, n):
        self.ops.append(("hincrby", key, field, n))

    async def execute(self):
        for op in self.ops:
            kind, key, field, value = op
            bucket = self.store.setdefault(key, {})
            if kind == "hsetnx":
                bucket.setdefault(field, str(value))
            elif kind == "hset":
                bucket[field] = str(value)
            elif kind == "hincrby":
                bucket[field] = str(int(bucket.get(field, 0)) + int(value))
        self.ops.clear()
        return []


class _FakeRedis:
    def __init__(self):
        self.store = {}

    def pipeline(self):
        return _FakePipe(self.store)

    async def hgetall(self, key):
        return dict(self.store.get(key, {}))


def _reset():
    liveness._pending.clear()
    liveness._declared.clear()
    liveness._seen_new.clear()


def test_a_declared_mechanism_that_never_fires_is_visible():
    """The whole point. Silence must be a row, not an absence."""
    _reset()
    r = _FakeRedis()
    liveness.declare("demo.never", "a thing that never happens")
    liveness.declare("demo.always", "a thing that does happen")
    liveness.fired("demo.always", 5)
    asyncio.run(liveness.flush(r))

    rows = asyncio.run(liveness.report(r))
    by_name = {row["mechanism"]: row for row in rows}
    assert by_name["demo.never"]["never"] is True
    assert by_name["demo.never"]["times"] == 0
    assert by_name["demo.always"]["never"] is False
    assert by_name["demo.always"]["times"] == 5

    never = asyncio.run(liveness.never_fired(r))
    assert [n["mechanism"] for n in never] == ["demo.never"]


def test_never_fired_sorts_first():
    """The answer to "what has never run" must not need scrolling."""
    _reset()
    r = _FakeRedis()
    for i in range(5):
        liveness.declare(f"demo.busy{i}")
        liveness.fired(f"demo.busy{i}")
    liveness.declare("demo.silent")
    asyncio.run(liveness.flush(r))

    rows = asyncio.run(liveness.report(r))
    assert rows[0]["mechanism"] == "demo.silent"


def test_a_count_survives_a_restart():
    """`MetricsCollector` resets on restart and answers "since the last deploy".

    The question here is "ever", so the count lives in Redis. A mechanism that
    last ran in August and has not run since is exactly what this should show,
    and a process-local counter cannot.
    """
    _reset()
    r = _FakeRedis()
    liveness.declare("demo.persisted")
    liveness.fired("demo.persisted", 3)
    asyncio.run(liveness.flush(r))

    _reset()  # a new process
    liveness.declare("demo.persisted")
    liveness.fired("demo.persisted", 2)
    asyncio.run(liveness.flush(r))

    rows = {row["mechanism"]: row for row in asyncio.run(liveness.report(r))}
    assert rows["demo.persisted"]["times"] == 5


def test_declaring_does_not_erase_an_existing_count():
    """HSETNX, not HSET. A redeploy must not zero the history."""
    _reset()
    r = _FakeRedis()
    liveness.declare("demo.kept")
    liveness.fired("demo.kept", 7)
    asyncio.run(liveness.flush(r))

    _reset()
    liveness.declare("demo.kept", "redeployed with a better description")
    asyncio.run(liveness.flush(r))

    rows = {row["mechanism"]: row for row in asyncio.run(liveness.report(r))}
    assert rows["demo.kept"]["times"] == 7


def test_firing_touches_no_io():
    """Cheap enough for a path that runs forty thousand times a minute."""
    _reset()
    liveness.declare("demo.hot")
    for _ in range(10000):
        liveness.fired("demo.hot")
    # One pending entry, not ten thousand, and no client was ever needed.
    assert liveness.pending_count() == 1
    assert liveness._pending["demo.hot"] == 10000


def test_a_failed_flush_keeps_the_counts():
    """Dropping a firing makes a live mechanism look dead."""
    _reset()

    class _Broken:
        def pipeline(self):
            raise RuntimeError("redis is down")

    liveness.declare("demo.retained")
    liveness.fired("demo.retained", 4)
    written = asyncio.run(liveness.flush(_Broken()))
    assert written == 0
    assert liveness._pending["demo.retained"] == 4


def test_no_client_is_survivable():
    _reset()
    liveness.fired("demo.x")
    assert asyncio.run(liveness.flush(None)) == 0
    assert asyncio.run(liveness.report(None)) == []


def test_the_flush_rides_on_the_heartbeat():
    """A registry each service had to remember to drain would be the very
    defect it detects."""
    src = (ROOT / "shared" / "utils" / "heartbeat.py").read_text(encoding="utf-8")
    assert "liveness_flush" in src
    loop = src.index("async def start_heartbeat_task")
    assert "liveness_flush(redis_client)" in src[loop:]


def test_the_registry_instruments_what_the_audit_found_dead():
    """Built and instrumented nothing would be the joke writing itself.

    Asserted against the sources rather than the process registry, because the
    tests above deliberately clear that registry and a module already imported
    does not re-run its module-level `declare`.
    """
    sites = {
        "shared/utils/inference_budget.py": [
            "inference.admission.holdback", "inference.budget.claimed",
        ],
        "services/correlation/soft_correlator.py": ["correlation.vectors.pruned"],
        "services/collector-tradfi/main.py": ["options.open_interest.resolved"],
    }
    for path, names in sites.items():
        src = (ROOT / path).read_text(encoding="utf-8")
        for name in names:
            assert f'_declare("{name}"' in src, f"{name} not declared in {path}"
            assert f'_fired("{name}"' in src, f"{name} declared but never fired in {path}"

"""
tests/test_batcher_survives_a_shed.py

A shed flush wedged the batcher for good.

InferenceShed is a BaseException so that the `except Exception` handlers around
inference calls cannot swallow it. `InferenceBatcher._flush` had exactly such a
handler and nothing else, so a shed escaped it with every future unresolved and
`_inflight` left True -- and `_flush()` returns early while `_inflight` is set,
so no batch could ever start again. `safe_create_task` counts a shed as the
budget working and logs nothing.

Live, 1 October: radar answered ten candidates at 17:30:37, the re-armed flush
was shed in the same second, and radar then sat at processed=87, STALLED 789s,
consumer lag climbing past the Kafka retention edge.
"""

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.agents.base import InferenceBatcher
from shared.utils.inference_budget import InferenceShed


def _batcher(flush_fn, max_items=2):
    return InferenceBatcher(
        name="test", flush_fn=flush_fn, max_items=max_items,
        max_wait_sec=0.5, max_stall_sec=10,
    )


def test_a_shed_batch_is_retried_and_answered():
    calls = []

    async def flush(items):
        calls.append([k for k, _ in items])
        if len(calls) == 1:
            raise InferenceShed("radar_agent", "qwen3:0.6b")
        return {k: f"answer-{k}" for k, _ in items}

    async def run():
        b = _batcher(flush)
        got = await asyncio.gather(b.submit("A", 1), b.submit("B", 2))
        return b, got

    b, got = asyncio.run(run())
    assert got == ["answer-A", "answer-B"]
    assert calls == [["A", "B"], ["A", "B"]]
    assert b.shed_retries == 1


def test_a_shed_does_not_leave_the_batcher_in_flight():
    """The wedge itself: `_inflight` stuck True blocks every later flush."""
    state = {"n": 0}

    async def flush(items):
        state["n"] += 1
        if state["n"] == 1:
            raise InferenceShed("radar_agent", "qwen3:0.6b")
        return {k: k for k, _ in items}

    async def run():
        b = _batcher(flush)
        first = await asyncio.gather(b.submit("A", 1), b.submit("B", 2))
        second = await asyncio.gather(b.submit("C", 3), b.submit("D", 4))
        return b, first, second

    b, first, second = asyncio.run(run())
    assert first == ["A", "B"]
    assert second == ["C", "D"]
    assert b._inflight is False


def test_fill_statistics_count_only_flushes_that_ran():
    """A shed asked nothing, so it is not a flush of any size."""
    state = {"n": 0}

    async def flush(items):
        state["n"] += 1
        if state["n"] == 1:
            raise InferenceShed("radar_agent", "qwen3:0.6b")
        return {k: k for k, _ in items}

    async def run():
        b = _batcher(flush)
        await asyncio.gather(b.submit("A", 1), b.submit("B", 2))
        return b.snapshot()

    snap = asyncio.run(run())
    assert snap["flushes"] == 1
    assert snap["items"] == 2
    assert snap["avg_fill"] == 2.0
    assert snap["shed_retries"] == 1


def test_an_ordinary_failure_still_answers_no_decision():
    """Unchanged: a genuine error resolves every caller to None at once."""
    async def flush(items):
        raise RuntimeError("model returned garbage")

    async def run():
        b = _batcher(flush)
        return await asyncio.gather(b.submit("A", 1), b.submit("B", 2))

    assert asyncio.run(run()) == [None, None]


def test_a_flush_never_exceeds_its_ceiling():
    """The queue fills while a call is in flight; the next flush took all of
    it -- 23 for radar against a ceiling of 10, 4 for extraction against 3."""
    sizes = []
    gate = asyncio.Event()

    async def flush(items):
        sizes.append(len(items))
        if len(sizes) == 1:
            await gate.wait()
        return {k: k for k, _ in items}

    async def run():
        b = _batcher(flush, max_items=2)
        first = [asyncio.ensure_future(b.submit(k, k)) for k in ("A", "B")]
        await asyncio.sleep(0.05)
        rest = [asyncio.ensure_future(b.submit(k, k)) for k in ("C", "D", "E", "F", "G")]
        await asyncio.sleep(0.05)
        gate.set()
        return await asyncio.gather(*first, *rest)

    got = asyncio.run(run())
    assert got == ["A", "B", "C", "D", "E", "F", "G"]
    assert max(sizes) <= 2
    assert sum(sizes) == 7

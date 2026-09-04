"""Keeps a slow model from freezing the consumer that feeds it.

The consume loop used to `await asyncio.gather(*batch)` before committing and
re-polling. One measured inference took 482 seconds, so a single slow message
stopped the loop entirely: no polling, no commits, lag growing while the process
sat idle holding one message. Agents that never call a model were stalled by
agents that did.

Dispatches now run detached and are accounted for as they finish. No agent's own
logic changed -- the same handler runs, with the same model, on the same
message -- only who waits for it.
"""
import asyncio
import pathlib
import re
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

BASE = (ROOT / "services/agents/base.py").read_text(encoding="utf-8")


def test_the_batch_is_no_longer_awaited_before_committing():
    """The specific line that froze the loop."""
    assert "await asyncio.gather(*tasks" not in BASE, (
        "the consume loop still blocks on the whole batch before committing"
    )


def test_dispatches_are_tracked_and_bounded():
    """Detached work must not become unbounded memory when a model is slow."""
    assert "MAX_INFLIGHT_DISPATCHES" in BASE
    cap = int(re.search(r"MAX_INFLIGHT_DISPATCHES\s*=\s*(\d+)", BASE).group(1))
    assert 0 < cap <= 1024, f"in-flight cap {cap} is not a sane bound"
    assert "self._inflight.add(task)" in BASE
    assert "FIRST_COMPLETED" in BASE, "waits for the whole set instead of the first free slot"


def test_completion_accounting_retrieves_the_exception():
    """An un-retrieved task exception is swallowed into a warning at GC time,
    which would hide every real dispatch failure."""
    assert "task.exception()" in BASE


def test_shed_is_still_distinguished_from_failure_in_the_callback():
    account = BASE[BASE.index("def _account_for"):BASE.index("async def _dispatch")]
    assert "InferenceShed" in account, "shed messages would be dead-lettered"
    assert "_send_dlq" in account, "genuine failures would no longer reach the DLQ"
    shed_branch = account[account.index("InferenceShed"):account.index("_send_dlq")]
    assert "return" in shed_branch, "a shed falls through into the DLQ path"


def test_in_flight_work_is_drained_before_the_session_closes():
    """Dispatches outlive the loop now; closing the HTTP session under a running
    inference would fail it noisily for no reason."""
    run_fn = BASE[BASE.index("heartbeat_task = safe_create_task"):BASE.index("async def _consume_loop")]
    drain_at = run_fn.index("asyncio.wait(inflight")
    close_at = run_fn.index("self._session.close()")
    assert drain_at < close_at, "the session is closed before in-flight work is drained"


@pytest.mark.anyio
async def test_a_slow_task_does_not_delay_the_next_iteration():
    """Reproduces the shape of the bug with the real primitives.

    Awaiting the batch makes the loop take as long as its slowest member;
    detaching makes it take as long as its fastest path.
    """
    async def slow():
        await asyncio.sleep(0.4)

    async def fast():
        return "ok"

    # Old shape: the loop waits for everything.
    t0 = asyncio.get_event_loop().time()
    await asyncio.gather(slow(), fast(), return_exceptions=True)
    blocking = asyncio.get_event_loop().time() - t0

    # New shape: work is registered and the loop moves on.
    t0 = asyncio.get_event_loop().time()
    inflight = set()
    for coro in (slow(), fast()):
        task = asyncio.create_task(coro)
        inflight.add(task)
        task.add_done_callback(inflight.discard)
    detached = asyncio.get_event_loop().time() - t0

    assert detached < blocking / 4, f"detached path still blocked ({detached:.3f}s vs {blocking:.3f}s)"
    await asyncio.gather(*inflight, return_exceptions=True)

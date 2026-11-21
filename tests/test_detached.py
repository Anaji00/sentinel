import asyncio

import pytest

from shared.utils import detached


def test_a_caller_that_runs_out_of_time_does_not_cancel_the_work_and_the_next_one_gets_the_result():
    started = []

    async def slow():
        started.append(1)
        await asyncio.sleep(0.3)
        return {"v": 42}

    async def scenario():
        with pytest.raises(detached.Warming):
            await detached.run("k1", slow, 0.05)                 # gives up while the task carries on
        assert detached.running("k1")
        first = await detached.run("k1", slow, 1.0)              # joins the same task
        return first

    assert asyncio.run(scenario()) == {"v": 42}
    assert len(started) == 1, "a second caller must not start a second computation"


def test_concurrent_callers_share_one_task_and_a_finished_one_is_not_reused():
    calls = []

    async def work():
        calls.append(1)
        await asyncio.sleep(0.05)
        return len(calls)

    async def scenario():
        a, b = await asyncio.gather(detached.run("k2", work, 1.0), detached.run("k2", work, 1.0))
        c = await detached.run("k2", work, 1.0)                  # the first finished, so this starts a new one
        return a, b, c

    a, b, c = asyncio.run(scenario())
    assert a == b == 1 and c == 2


def test_a_failing_task_reaches_the_waiting_caller_and_is_forgotten():
    async def boom():
        raise RuntimeError("db down")

    async def scenario():
        with pytest.raises(RuntimeError):
            await detached.run("k3", boom, 1.0)
        await asyncio.sleep(0)
        return detached.running("k3")

    assert asyncio.run(scenario()) is False


def test_a_task_that_fails_with_timeout_error_raises_it_not_warming():
    # Review F10 (18 Nov): asyncpg's command_timeout raises the builtin TimeoutError inside the task; the caller must see that, not "still computing".
    import asyncio
    from shared.utils import detached

    async def boom():
        raise TimeoutError("command_timeout")

    async def go():
        try:
            await detached.run("f10:boom", boom, wait_s=5.0)
        except detached.Warming:
            return "warming"
        except TimeoutError:
            return "timeout"
        return "returned"

    assert asyncio.run(go()) == "timeout"

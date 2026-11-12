"""The slow-callback summary that finds the jobs holding the event loop (shared/utils/loop_watch.py)."""

import asyncio
import logging
import pathlib
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import loop_watch as lw  # noqa: E402

MSG = ("Executing <Task pending name='Task-5' coro=<NewsRefit.run() running at /app/services/agents/news_model_refit.py:210> "
       "wait_for=<Future pending cb=[Task.task_wakeup()]> created at /app/x.py:1> took 0.412 seconds")


def test_a_slow_callback_message_gives_its_job_place_and_seconds() -> None:
    assert lw.parse(MSG) == ("NewsRefit.run @ /app/services/agents/news_model_refit.py:210", 0.412)
    assert lw.parse("something else took 3 seconds") is None and lw.parse("Executing x") is None


def test_the_summary_ranks_by_total_time_and_can_reset() -> None:
    h = lw.SlowCallbackLog()
    for text, secs in (("A.run", 0.1), ("A.run", 0.3), ("B.run", 0.5)):
        h.emit(logging.LogRecord("asyncio", logging.WARNING, "", 0, f"Executing <Task coro=<{text}() running at f.py:1>> took {secs} seconds", (), None))
    lines = h.summary()
    assert lines[0].startswith("B.run @ f.py:1: 1 times, worst 0.50s") and lines[1].startswith("A.run @ f.py:1: 2 times, worst 0.30s, total 0.4s")
    assert h.summary(reset=True) and h.summary() == []


def test_the_threshold_comes_from_the_environment_and_off_means_none() -> None:
    assert lw.threshold_from_env({"LOOP_SLOW_CALLBACK_MS": "50"}) == 50.0
    for bad in ({}, {"LOOP_SLOW_CALLBACK_MS": ""}, {"LOOP_SLOW_CALLBACK_MS": "x"}, {"LOOP_SLOW_CALLBACK_MS": "0"}):
        assert lw.threshold_from_env(bad) is None


def test_installing_it_catches_a_real_blocking_call() -> None:
    async def scenario() -> list:
        h = lw.install(asyncio.get_running_loop(), 20, logging.getLogger("t"), interval_sec=3600)

        async def blocker() -> None:
            time.sleep(0.15)                     # the kind of call that has to move to a thread

        await asyncio.create_task(blocker())
        await asyncio.sleep(0.01)
        logging.getLogger("asyncio").removeHandler(h)
        return h.summary()

    out = asyncio.run(scenario())
    assert out and "blocker" in out[0]

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
        h = lw.install(asyncio.get_running_loop(), 20, logging.getLogger("t"), interval_sec=3600, debug=True)

        async def blocker() -> None:
            time.sleep(0.15)                     # the kind of call that has to move to a thread

        await asyncio.create_task(blocker())
        await asyncio.sleep(0.01)
        logging.getLogger("asyncio").removeHandler(h)
        return h.summary()

    out = asyncio.run(scenario())
    assert out and "blocker" in out[0]


def test_the_sampler_names_the_function_that_blocks_the_loop() -> None:
    import threading

    def heavy_synchronous_work() -> None:
        time.sleep(0.4)

    async def scenario() -> list:
        s = lw.BlockedLoopSampler(asyncio.get_running_loop(), threading.get_ident(), threshold_ms=30, sample_ms=5, tick_ms=5)
        s.start()
        await asyncio.sleep(0.05)
        heavy_synchronous_work()                   # holds the loop for 0.4 s
        await asyncio.sleep(0.05)
        s.stop()
        return s.summary()

    out = asyncio.run(scenario())
    assert out and "heavy_synchronous_work" in out[0] or any("heavy_synchronous_work" in line for line in out)


def test_the_soft_correlator_imports_its_heavy_libraries_off_the_event_loop() -> None:
    src = (ROOT / "services/correlation/soft_correlator.py").read_text(encoding="utf-8")
    i = src.index("async def _load(self)")
    body = src[i:i + 2500]
    assert "run_in_executor(None, _heavy_imports)" in body
    assert "\n                import sentence_transformers\n" not in body          # no import statement runs on the loop


def test_debug_mode_is_off_by_default_because_it_inflates_what_it_measures() -> None:
    async def scenario() -> bool:
        lw.install(asyncio.get_running_loop(), 50, logging.getLogger("t"), interval_sec=3600)
        return asyncio.get_running_loop().get_debug()

    assert asyncio.run(scenario()) is False

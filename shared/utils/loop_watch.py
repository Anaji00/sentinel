"""Which jobs block the event loop, and for how long.

asyncio's debug mode already logs every callback that holds the loop longer than `slow_callback_duration`, naming the task. This
turns those log lines into a ranked summary (job, where it was running, count, worst and total seconds), so the jobs that need a
worker thread are found by measurement. Switched on with LOOP_SLOW_CALLBACK_MS; off by default, since debug mode costs CPU.
"""
from __future__ import annotations

import asyncio
import collections
import logging
import re
import sys
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

_TOOK = re.compile(r"took ([0-9.]+) seconds")
_CORO = re.compile(r"coro=<(.+?)\(\)")
_AT = re.compile(r"(?:running|defined) at ([^\s>]+)")


def parse(message: str) -> Optional[Tuple[str, float]]:
    """(job, seconds) from an asyncio slow-callback message; None for any other line."""
    t = _TOOK.search(message)
    if not t or "Executing" not in message:
        return None
    coro = _CORO.search(message)
    at = _AT.search(message)
    name = coro.group(1) if coro else "callback"
    if at:
        name = f"{name} @ {at.group(1)}"
    return name, float(t.group(1))


class SlowCallbackLog(logging.Handler):
    """Collects slow-callback records from the asyncio logger."""

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self._lock = threading.Lock()
        self.stats: Dict[str, List[float]] = {}          # job -> [count, worst seconds, total seconds]
        self.sampler: Optional["BlockedLoopSampler"] = None

    def emit(self, record: logging.LogRecord) -> None:
        got = parse(record.getMessage())
        if got is None:
            return
        job, secs = got
        with self._lock:
            row = self.stats.setdefault(job, [0.0, 0.0, 0.0])
            row[0] += 1
            row[1] = max(row[1], secs)
            row[2] += secs

    def summary(self, top: int = 8, reset: bool = False) -> List[str]:
        """The worst jobs by total seconds held, one line each."""
        with self._lock:
            rows = sorted(self.stats.items(), key=lambda kv: -kv[1][2])[:top]
            out = [f"{job}: {int(r[0])} times, worst {r[1]:.2f}s, total {r[2]:.1f}s" for job, r in rows]
            if reset:
                self.stats = {}
        return out


_SKIP = ("asyncio/", "utils/loop_watch.py", "threading.py")


class BlockedLoopSampler(threading.Thread):
    """Records the exact line the event loop is stuck on while it is blocked.

    The loop schedules a tick every `tick_ms`. A thread watches the time since the last tick; once it exceeds `threshold_ms` it
    samples the loop thread's stack every `sample_ms`, so a call that blocks for a second is seen about 1000/sample_ms times at its
    own line. Names from asyncio's slow-callback log say which task was held; this says where."""

    def __init__(self, loop: asyncio.AbstractEventLoop, loop_thread_id: int, threshold_ms: float, sample_ms: float = 10.0, tick_ms: float = 10.0) -> None:
        super().__init__(name="loop-watch-sampler", daemon=True)
        self._loop, self._tid = loop, loop_thread_id
        self._threshold, self._sample, self._tick = threshold_ms / 1000.0, sample_ms / 1000.0, tick_ms / 1000.0
        self._last = time.monotonic()
        self._stop_flag = threading.Event()
        self._lock = threading.Lock()
        self.counts: "collections.Counter[str]" = collections.Counter()
        self.sample_s = sample_ms / 1000.0

    def _beat(self) -> None:
        self._last = time.monotonic()
        if not self._stop_flag.is_set():
            self._loop.call_later(self._tick, self._beat)

    def start(self) -> None:                                           # type: ignore[override]
        self._loop.call_soon(self._beat)
        super().start()

    def stop(self) -> None:
        self._stop_flag.set()

    def run(self) -> None:
        while not self._stop_flag.is_set():
            time.sleep(self._sample)
            if time.monotonic() - self._last < self._threshold:
                continue
            frame = sys._current_frames().get(self._tid)
            parts: List[str] = []
            while frame is not None and len(parts) < 3:
                code = frame.f_code
                path = code.co_filename.replace("\\", "/")
                if not any(k in path for k in _SKIP):
                    parts.append(f"{path.rsplit('/app/', 1)[-1]}:{frame.f_lineno} {code.co_name}")
                frame = frame.f_back
            if parts:
                with self._lock:
                    self.counts[" <- ".join(parts)] += 1

    def summary(self, top: int = 8, reset: bool = False) -> List[str]:
        with self._lock:
            rows = self.counts.most_common(top)
            out = [f"{where}: {n * self.sample_s:.1f}s" for where, n in rows]
            if reset:
                self.counts = collections.Counter()
        return out


def install(loop: asyncio.AbstractEventLoop, threshold_ms: float, logger: logging.Logger, interval_sec: float = 600.0, debug: bool = False) -> SlowCallbackLog:
    """Watch for a blocked loop: the stack sampler always, asyncio's own slow-callback log only with `debug`.

    Debug mode records the creation stack of every callback, which cost 509 of 600 seconds on the agents loop (12 Nov measurement), so
    it inflates what it measures and is off by default; the sampler costs one thread waking every 10 ms."""
    handler = SlowCallbackLog()
    logging.getLogger("asyncio").addHandler(handler)
    if debug:
        loop.set_debug(True)
        loop.slow_callback_duration = max(0.001, float(threshold_ms) / 1000.0)
    sampler = BlockedLoopSampler(loop, threading.get_ident(), threshold_ms)
    sampler.start()
    handler.sampler = sampler

    async def report() -> None:
        while True:
            await asyncio.sleep(interval_sec)
            lines = handler.summary(reset=True)
            if lines:
                logger.warning("Event loop held over %.0f ms in the last %.0f s: %s", threshold_ms, interval_sec, " | ".join(lines))
            where = sampler.summary(reset=True)
            if where:
                logger.warning("Where the loop was blocked (seconds, newest frame first): %s", " | ".join(where))

    loop.create_task(report(), name="loop_watch_report")
    return handler


def threshold_from_env(env: Dict[str, Any]) -> Optional[float]:
    """LOOP_SLOW_CALLBACK_MS as a positive number, else None (off)."""
    try:
        v = float(str(env.get("LOOP_SLOW_CALLBACK_MS") or "").strip())
    except ValueError:
        return None
    return v if v > 0 else None


__all__ = ["BlockedLoopSampler", "SlowCallbackLog", "install", "parse", "threshold_from_env"]

"""Which jobs block the event loop, and for how long.

asyncio's debug mode already logs every callback that holds the loop longer than `slow_callback_duration`, naming the task. This
turns those log lines into a ranked summary (job, where it was running, count, worst and total seconds), so the jobs that need a
worker thread are found by measurement. Switched on with LOOP_SLOW_CALLBACK_MS; off by default, since debug mode costs CPU.
"""
from __future__ import annotations

import asyncio
import logging
import re
import threading
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


def install(loop: asyncio.AbstractEventLoop, threshold_ms: float, logger: logging.Logger, interval_sec: float = 600.0) -> SlowCallbackLog:
    """Turn on slow-callback detection at `threshold_ms` and log a ranked summary every `interval_sec`."""
    handler = SlowCallbackLog()
    logging.getLogger("asyncio").addHandler(handler)
    loop.set_debug(True)
    loop.slow_callback_duration = max(0.001, float(threshold_ms) / 1000.0)

    async def report() -> None:
        while True:
            await asyncio.sleep(interval_sec)
            lines = handler.summary(reset=True)
            if lines:
                logger.warning("Event loop held over %.0f ms in the last %.0f s: %s", threshold_ms, interval_sec, " | ".join(lines))

    loop.create_task(report(), name="loop_watch_report")
    return handler


def threshold_from_env(env: Dict[str, Any]) -> Optional[float]:
    """LOOP_SLOW_CALLBACK_MS as a positive number, else None (off)."""
    try:
        v = float(str(env.get("LOOP_SLOW_CALLBACK_MS") or "").strip())
    except ValueError:
        return None
    return v if v > 0 else None


__all__ = ["SlowCallbackLog", "install", "parse", "threshold_from_env"]

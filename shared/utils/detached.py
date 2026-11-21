"""Run a slow computation detached from the request that asked for it, so a client that gives up does not cancel it and the next client does not start another.

The breadth window query takes about 43 s on a cold cache (pg_stat_statements, 13 Nov 2026). Computed inline, a viewer that timed out cancelled it, nothing was
cached, and every following request began another 43 s query. Here the first caller starts one task per key, every caller (that one and later ones) waits up to
`wait_s` for it, and a caller that runs out of time gets `Warming` while the task carries on and fills the cache. Process-local, single event loop. Failures of the task
are counted with `swallowed` and surface to waiting callers as the original exception.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable, Dict

from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("detached")
_RUNNING: Dict[str, "asyncio.Task[Any]"] = {}


class Warming(Exception):
    """The computation is still running; the caller should retry shortly."""


def _finished(key: str, task: "asyncio.Task[Any]") -> None:
    if _RUNNING.get(key) is task:
        _RUNNING.pop(key, None)
    if not task.cancelled() and task.exception() is not None:
        swallowed("utils.detached.task", task.exception(), logger, detail=key)


async def run(key: str, start: Callable[[], Awaitable[Any]], wait_s: float) -> Any:
    task = _RUNNING.get(key)
    if task is None or task.done():
        task = asyncio.ensure_future(start())
        _RUNNING[key] = task
        task.add_done_callback(lambda t, k=key: _finished(k, t))
    # asyncio.wait never raises the task's own exception and never cancels the task, so a task that itself fails with TimeoutError (asyncpg's
    # command_timeout) is told apart from "still running" by task.done(), not by the type of the exception (review F10, 18 Nov).
    await asyncio.wait({task}, timeout=wait_s)
    if not task.done():
        raise Warming(key)
    return task.result()


def running(key: str) -> bool:
    task = _RUNNING.get(key)
    return task is not None and not task.done()


__all__ = ["Warming", "run", "running"]

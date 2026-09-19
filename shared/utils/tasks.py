"""
shared/utils/tasks.py

Safe async task creation utility.
Ensures fire-and-forget tasks have their exceptions properly logged 
instead of being silently swallowed by asyncio.
"""

import asyncio
import logging
from typing import Optional, Set

logger = logging.getLogger("sentinel.tasks")

# Keep strong references to running tasks to prevent GC from destroying them
_background_tasks: Set[asyncio.Task] = set()

# Sheds counted per task family, so the INFO line below reports a running
# total rather than one message in a thousand with no denominator.
_shed_counts: dict = {}


def safe_create_task(
    coro,
    *,
    name: Optional[str] = None,
    logger_override: Optional[logging.Logger] = None,
) -> asyncio.Task:
    """
    Create an asyncio task with automatic exception logging and reference tracking.
    
    This replaces bare `asyncio.create_task()` calls throughout the codebase.
    Without this, exceptions in fire-and-forget tasks are silently swallowed
    (Python only emits a 'Task exception was never retrieved' warning).
    
    Usage:
        from shared.utils.tasks import safe_create_task
        safe_create_task(some_coroutine(), name="descriptive-name")
    """
    task = asyncio.create_task(coro, name=name)
    _background_tasks.add(task)
    
    log = logger_override or logger
    
    def _on_done(t: asyncio.Task):
        _background_tasks.discard(t)
        if t.cancelled():
            return
        exc = t.exception()
        if exc is None:
            return

        task_name = t.get_name() or "unnamed"

        # A shed is the budget working, not a task failing.
        #
        # `SentinelAgent._on_dispatch_done` already knows this -- it counts
        # sheds and logs every thousandth at INFO -- but any agent work started
        # through safe_create_task lands here instead, where every exception was
        # an ERROR with a traceback. Measured in the running deployment: 51
        # ERROR lines in ten minutes reading "Background task 'Task-435414'
        # failed with InferenceShed: inference budget declined work for
        # knowledge_graph_engine", which is the budget doing exactly what it
        # exists to do.
        #
        # It costs more than tidiness: an operator scanning for errors finds a
        # steady stream of them describing normal operation, which is how a real
        # one gets lost.
        if type(exc).__name__ == "InferenceShed":
            _shed_counts[task_name.split("-")[0]] = (
                _shed_counts.get(task_name.split("-")[0], 0) + 1
            )
            total = _shed_counts[task_name.split("-")[0]]
            if total % 1000 == 1:
                log.info(
                    "Inference budget has declined %d background task(s) so far; "
                    "latest: %s",
                    total, exc,
                )
            return

        log.error(
            f"Background task '{task_name}' failed with {type(exc).__name__}: {exc}",
            exc_info=exc,
        )
    
    task.add_done_callback(_on_done)
    return task

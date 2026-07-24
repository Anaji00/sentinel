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
        if exc is not None:
            task_name = t.get_name() or "unnamed"
            log.error(
                f"Background task '{task_name}' failed with {type(exc).__name__}: {exc}",
                exc_info=exc,
            )
    
    task.add_done_callback(_on_done)
    return task

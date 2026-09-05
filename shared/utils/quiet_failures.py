"""Counting the failures that are deliberately not raised.

Across `services/` and `shared/` there are 989 exception handlers. 116 swallow
silently with a bare `pass` -- 89 of them the exact form
`except Exception: pass` -- and 285 log only at DEBUG. The deployment runs at
LOG_LEVEL=INFO, set in .env and hardcoded in the services' own
`basicConfig(level=logging.INFO)`, and the running containers emit no DEBUG
lines at all. So roughly 401 of 989 handlers, about 41%, are guaranteed to
produce nothing observable when they fire.

That is the mechanism behind most of the defects this audit found. The backtest
cache write that could not create a task on a worker thread, the graph backfill
that never ran, the 13F deduplication, the counterparty degree that stayed zero
because three Redis commands were missing from a test double, the movement
score that fell back to its base on every filing -- each reported success and
did nothing, because the handler that knew otherwise was speaking at a level
nobody was listening to.

The answer is not to raise every one of them to WARNING: most are genuinely
recoverable and the log would become unreadable, which is its own way of
hiding things. What is missing is a *count*. A handler that fires twice a week
is noise; the same handler firing four hundred times an hour is a broken
subsystem reporting success, and only the second one needs a human.

    from shared.utils.quiet_failures import swallowed

    try:
        ...
    except Exception as e:
        swallowed("enrichment.tradfi_bars_persist", e, logger)

The counters live in-process and are exposed through `snapshot()`, so a health
endpoint or a heartbeat can surface them without any handler having to decide
in advance how loud it should be.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from typing import Dict, Optional

_LOCK = threading.Lock()
_COUNTS: Dict[str, int] = {}
_FIRST_SEEN: Dict[str, float] = {}
_LAST_SEEN: Dict[str, float] = {}
_LAST_REPORTED: Dict[str, float] = {}

# How often one site may escalate to WARNING, however often it fires. A broken
# subsystem should say so; it should not say so four hundred times an hour.
ESCALATION_INTERVAL_SEC = float(os.getenv("QUIET_FAILURE_REPORT_SEC", "300"))

# Firing counts at which a site is escalated regardless of the interval, so the
# first occurrence and an obvious runaway are both visible immediately.
ESCALATION_COUNTS = (1, 10, 100, 1000)


def swallowed(
    site: str,
    exc: BaseException,
    logger: Optional[logging.Logger] = None,
    detail: str = "",
) -> int:
    """Record a deliberately-not-raised failure. Returns the count for this site.

    Always counted. Logged at DEBUG every time, and escalated to WARNING on the
    first occurrence, at powers of ten, and at most once per interval after
    that -- so a rare recoverable failure stays quiet and a persistent one
    becomes impossible to miss without anyone having to predict which it would
    be.
    """
    now = time.time()
    with _LOCK:
        count = _COUNTS.get(site, 0) + 1
        _COUNTS[site] = count
        _FIRST_SEEN.setdefault(site, now)
        _LAST_SEEN[site] = now
        last_reported = _LAST_REPORTED.get(site, 0.0)
        escalate = count in ESCALATION_COUNTS or (now - last_reported) >= ESCALATION_INTERVAL_SEC
        if escalate:
            _LAST_REPORTED[site] = now

    log = logger or logging.getLogger("sentinel.quiet")
    suffix = f" ({detail})" if detail else ""
    if escalate:
        log.warning(
            "Suppressed failure at %s has now fired %s time(s): %s: %s%s",
            site, count, type(exc).__name__, exc, suffix,
        )
    else:
        log.debug("Suppressed failure at %s (%s): %s%s", site, count, exc, suffix)
    return count


def snapshot() -> Dict[str, Dict[str, float]]:
    """Every site that has swallowed a failure, with counts and timings."""
    with _LOCK:
        return {
            site: {
                "count": _COUNTS[site],
                "first_seen": _FIRST_SEEN.get(site, 0.0),
                "last_seen": _LAST_SEEN.get(site, 0.0),
            }
            for site in sorted(_COUNTS, key=lambda k: -_COUNTS[k])
        }


def reset() -> None:
    """Clear the counters. For tests."""
    with _LOCK:
        _COUNTS.clear()
        _FIRST_SEEN.clear()
        _LAST_SEEN.clear()
        _LAST_REPORTED.clear()

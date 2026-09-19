"""
shared/utils/liveness.py

Has this mechanism ever actually run?

Across 631 audit findings the single most common shape was a mechanism that was
built correctly, wired correctly, deployed, and had never once executed. The
admission bar that had never refused a candidate. The focus set that never
reached the financial domain. The volume/open-interest chain wired end to end
and receiving nothing. Black-Litterman with exactly one caller, a unit test. The
reasoning lane nobody claimed. `score_adjustments` present in the schema, the
writer and the endpoint, and written by almost nothing. Nine collectors
incrementing a counter that stayed at zero. A vector index nobody had pruned.

None of those were visible from reading the code, because the code was right
every time. Each was found by asking the running system how often it had done
the thing it exists for, and each took an afternoon to find individually.

This is that question, asked continuously and cheaply.

    from shared.utils.liveness import declare, fired

    declare("inference.admission.holdback",
            "the admission bar refused a below-bar candidate")
    ...
    fired("inference.admission.holdback")

`declare` is the half that matters and the half that is easy to forget. Without
it a silent mechanism is indistinguishable from one nobody instrumented, which
is the state this module exists to end: a declared mechanism with a count of
zero is a *finding*, while an undeclared one is merely unknown.

Cost. `fired` is a dict increment and a set membership test. Nothing touches
Redis on the hot path: the first firing in a process marks the name dirty, and a
flush -- called from the heartbeat each service already runs -- writes the
accumulated counts in one pipeline. A mechanism that fires forty thousand times
a minute costs forty thousand integer increments and one round trip.

Durability. Counts live in Redis rather than in process memory, because the
lesson of `MetricsCollector._COUNTER_METRICS` is that an in-process counter
answers "since the last deploy" while the question here is "ever". A mechanism
that last ran in August and has not run since is exactly what this should
surface, and a counter that resets on restart cannot.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("shared.liveness")

# One hash of counts, one of last-seen epochs, one of descriptions.
#
# Three keys rather than one packed value per field, because the report reads
# all of them together and the hot path writes only the first two. Splitting
# them keeps HINCRBY usable, which is what makes a concurrent flush from several
# services safe without a lock.
COUNT_KEY = "sentinel:liveness:count"
LAST_KEY = "sentinel:liveness:last"
DESC_KEY = "sentinel:liveness:describes"

# Deliberately no TTL.
#
# "Never fired" is a claim about all of history, and it cannot be made from a
# key that expires. This is a small hash -- one field per declared mechanism,
# a few hundred at most -- and it is the one structure on this platform whose
# whole value is that it outlives everything else.

# Pending counts, flushed on the next heartbeat.
_pending: Dict[str, int] = {}

# Names this process has declared, so declaration is idempotent and free.
_declared: Dict[str, str] = {}

# Names already written to Redis at least once by this process. A first firing
# is worth a flush promptly; the thousandth can wait for the next one.
_seen_new: set = set()


def declare(name: str, description: str = "") -> None:
    """Register a mechanism, so that silence from it is legible.

    Idempotent and cheap: call it at import time next to the thing it names.
    A declared mechanism that has never fired is the finding this module
    exists to produce, and an undeclared one cannot produce it.
    """
    if not name:
        return
    _declared[str(name)] = str(description or "")


def fired(name: str, count: int = 1) -> None:
    """Record that this mechanism just did its job.

    Cheap enough for a hot path: a dict increment. Nothing is written to Redis
    here -- see `flush`.
    """
    if not name or count <= 0:
        return
    key = str(name)
    _pending[key] = _pending.get(key, 0) + int(count)
    if key not in _seen_new:
        _seen_new.add(key)


def pending_count() -> int:
    """How many mechanisms are waiting to be flushed. For tests and health."""
    return len(_pending)


async def flush(redis_client: Any) -> int:
    """Write accumulated counts and declarations. Returns mechanisms written.

    Safe to call from several services at once: counts use HINCRBY, and the
    declaration uses HSETNX so a service with a stale description cannot
    overwrite a better one.

    Never raises. Liveness accounting that takes down the thing it measures
    would be a poor trade.
    """
    if redis_client is None:
        return 0
    raw = getattr(redis_client, "raw", redis_client)

    counts = dict(_pending)
    _pending.clear()
    declarations = dict(_declared)

    if not counts and not declarations:
        return 0

    now = int(time.time())
    try:
        pipe = raw.pipeline()
        for mechanism, description in declarations.items():
            # The count field is created at zero if absent and left alone if
            # present. That is what makes "declared and never fired" a state
            # the report can see rather than an absence it has to infer.
            pipe.hsetnx(COUNT_KEY, mechanism, 0)
            if description:
                pipe.hset(DESC_KEY, mechanism, description)
        for mechanism, n in counts.items():
            pipe.hincrby(COUNT_KEY, mechanism, n)
            pipe.hset(LAST_KEY, mechanism, now)
        await pipe.execute()
    except Exception as exc:
        # Put the counts back rather than lose them: the question is "ever",
        # and dropping a firing makes a live mechanism look dead.
        for mechanism, n in counts.items():
            _pending[mechanism] = _pending.get(mechanism, 0) + n
        # Counted rather than whispered at DEBUG, which this deployment does
        # not emit: a liveness registry that fails invisibly is the defect it
        # exists to detect, wearing its own clothes.
        swallowed("utils.liveness.flush", exc, logger)
        return 0
    return len(counts) + len(declarations)


async def report(redis_client: Any) -> List[Dict[str, Any]]:
    """Every declared mechanism, with how often it has run and when it last did.

    Sorted so the answer to "what has never run" is at the top, because that is
    the question worth asking daily.
    """
    if redis_client is None:
        return []
    raw = getattr(redis_client, "raw", redis_client)
    try:
        counts = await raw.hgetall(COUNT_KEY)
        lasts = await raw.hgetall(LAST_KEY)
        describes = await raw.hgetall(DESC_KEY)
    except Exception as exc:
        swallowed("utils.liveness.report", exc, logger)
        return []

    def _txt(value: Any) -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8", "replace")
        return str(value)

    now = int(time.time())
    rows: List[Dict[str, Any]] = []
    for field, value in (counts or {}).items():
        name = _txt(field)
        try:
            total = int(_txt(value))
        except (TypeError, ValueError):
            total = 0
        last_raw = (lasts or {}).get(field)
        last = None
        if last_raw is not None:
            try:
                last = int(_txt(last_raw))
            except (TypeError, ValueError):
                last = None
        rows.append({
            "mechanism": name,
            "describes": _txt((describes or {}).get(field, b"")) or "",
            "times": total,
            "last_epoch": last,
            "silent_seconds": (now - last) if last else None,
            "never": total == 0,
        })

    # Never-fired first, then longest-silent, then by name. A mechanism that has
    # run once and then stopped is the second most interesting row here.
    rows.sort(key=lambda r: (
        not r["never"],
        -(r["silent_seconds"] or 0),
        r["mechanism"],
    ))
    return rows


async def never_fired(redis_client: Any) -> List[Dict[str, Any]]:
    """Declared mechanisms with a lifetime count of zero."""
    return [row for row in await report(redis_client) if row["never"]]


__all__ = [
    "COUNT_KEY",
    "DESC_KEY",
    "LAST_KEY",
    "declare",
    "fired",
    "flush",
    "never_fired",
    "pending_count",
    "report",
]

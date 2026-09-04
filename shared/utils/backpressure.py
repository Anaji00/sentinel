"""
shared/utils/backpressure.py

Telling the producer that the consumer is drowning.

The platform sheds. When the reasoning service cannot get an inference slot it
drops the cluster; when an agent is behind, it skips stale events. Shedding is a
real defence and it is the *last* one in the chain -- by the time it fires, the
correlation layer has already built the cluster, embedded it, persisted it and
published it, and every one of those costs was paid for work that was then
thrown away.

Nothing upstream ever learned. The correlation service produced at full rate
into a consumer it could observe was saturated: 10,354 clusters in a day against
a reasoning tier that manages about thirty-six an hour.

This is the missing signal. A saturated consumer raises a flag; producers read
it and slow the work that only exists to feed that consumer. It is advisory and
never blocking:

  - A producer that ignores it behaves exactly as it does today.
  - A stale flag expires on its own, so a crashed consumer cannot wedge the
    pipeline shut.
  - It throttles *optional* work only. Ingest, persistence and anything a person
    is waiting on must not be paced by a downstream model server.

Advisory rather than a queue depth
----------------------------------
Kafka already knows the queue depth, and consumer lag is the honest measure of
it. What lag cannot say is *why*: a consumer behind because it is shedding on
budget is in a different state from one behind because it restarted, and only
the consumer knows which. So the consumer declares its own state and the
producers react to the declaration.
"""

import logging
import time
from typing import Any, Optional

logger = logging.getLogger("shared.backpressure")

# One key per saturated consumer group. Presence is the signal; the value
# carries why, for a human reading the keyspace.
_PRESSURE_KEY = "sentinel:backpressure:{consumer}"

# How long a declaration stands without being renewed.
#
# Short by design. A consumer that stops declaring -- because it recovered, or
# because it died -- must stop throttling its producers within a minute or two,
# or a crash becomes a permanent slowdown of the whole platform.
PRESSURE_TTL_SEC = 90

# How much a producer stretches its optional work while a consumer is saturated.
# Not unbounded: the point is to stop paying for work that will be discarded,
# not to stop producing.
THROTTLE_MULTIPLIER = 4.0
MAX_THROTTLED_INTERVAL_SEC = 300


async def declare_pressure(
    redis_client: Any,
    consumer: str,
    reason: str = "",
    ttl_sec: int = PRESSURE_TTL_SEC,
) -> bool:
    """A consumer stating that it cannot keep up.

    Called from the shed path, where the consumer has just decided to discard
    work -- which is the moment it knows, and the moment nothing upstream did.
    """
    if not redis_client or not consumer:
        return False
    try:
        raw = getattr(redis_client, "raw", redis_client)
        await raw.set(
            _PRESSURE_KEY.format(consumer=consumer),
            f"{int(time.time())}:{reason[:200]}",
            ex=int(ttl_sec),
        )
        return True
    except Exception as e:
        logger.debug("Could not declare backpressure for %s: %s", consumer, e)
        return False


async def clear_pressure(redis_client: Any, consumer: str) -> bool:
    """A consumer stating that it has caught up.

    Optional -- the TTL handles the common case -- but a consumer that recovers
    cleanly should say so rather than leaving its producers throttled for the
    remainder of the window.
    """
    if not redis_client or not consumer:
        return False
    try:
        raw = getattr(redis_client, "raw", redis_client)
        await raw.delete(_PRESSURE_KEY.format(consumer=consumer))
        return True
    except Exception:
        return False


async def is_under_pressure(redis_client: Any, consumer: str) -> bool:
    """Whether a named consumer is currently declaring saturation.

    Answers False when it cannot tell. A Redis problem must not invent
    backpressure that nobody declared -- the failure mode of a false positive
    here is the whole platform quietly slowing down for no reason.
    """
    if not redis_client or not consumer:
        return False
    try:
        raw = getattr(redis_client, "raw", redis_client)
        return bool(await raw.exists(_PRESSURE_KEY.format(consumer=consumer)))
    except Exception:
        return False

# A producer-side interval helper was written here and never called: the one
# producer that yields to backpressure is event-driven rather than polling, so
# it consults is_under_pressure directly. Removed rather than left as surface
# nobody uses -- this audit spent a great deal of time on mechanisms that were
# built, correct and unreachable, and adding to that set knowingly would be odd.

#!/usr/bin/env python
"""Container healthcheck that asks whether a service is *working*, not running.

Twenty-two of the thirty running containers had no healthcheck at all -- every
collector, both agent tiers, correlation, enrichment, reasoning, ingress, the
frontend, alert-manager, dlq-worker and telemetry-worker. Docker's only question
of the application was whether the process still existed.

That is why a wedged consumer was invisible: `sentinel-agents-fast` reported
`Up 11 hours` while its radar loop sat with every Kafka offset frozen and 21,000
messages of lag, because the process was alive and nothing asked it anything
harder.

The signal needed already existed and was already being written. Every service's
heartbeat loop publishes, once a minute:

    sentinel:heartbeat:{service}  -> {processed, window_rate, stalled_seconds,
                                      error_rate, consumer_lag, lag_growing}

with a TTL, so staleness is self-evident. `stalled_seconds` is exactly the
question a liveness probe wants to ask. Every component of the answer was
present except the question.

Exit codes are Docker's: 0 healthy, 1 unhealthy.

Deliberately lenient in two places. A service that has not published a heartbeat
*yet* is healthy -- startup is not a fault, and failing during it would restart
the container into a loop. And a stall is only unhealthy past
`HEALTHCHECK_MAX_STALL_SEC`, which is far longer than any legitimate quiet
period, because a quiet market is not a broken consumer.
"""
import asyncio
import json
import os
import sys
import time

sys.path.insert(0, "/app")

SERVICE = os.getenv("SENTINEL_SERVICE", "").strip()

# How long a service may process nothing before the container is unhealthy.
# Generous: the inference tier legitimately spends minutes inside one model
# call, and an overnight market is genuinely quiet.
MAX_STALL_SEC = float(os.getenv("HEALTHCHECK_MAX_STALL_SEC", "1800"))

# How long after the last heartbeat the record is treated as gone. The
# heartbeat loop runs every 60s; three missed ones is a stopped loop.
MAX_HEARTBEAT_AGE_SEC = float(os.getenv("HEALTHCHECK_MAX_AGE_SEC", "240"))


async def _check() -> tuple[bool, str]:
    if not SERVICE:
        # Nothing to look up. A container that does not name itself cannot be
        # judged, and guessing would be worse than passing.
        return True, "SENTINEL_SERVICE unset; nothing to check"

    from shared.db import get_redis

    redis = await get_redis()
    raw = getattr(redis, "raw", redis)

    payload = None
    for key in (f"sentinel:heartbeat:{SERVICE}", f"sentinel:agents:health:{SERVICE}"):
        value = await raw.get(key)
        if value:
            try:
                payload = json.loads(value.decode() if isinstance(value, bytes) else value)
                break
            except (ValueError, TypeError):
                continue

    if not payload:
        # Either still starting, or the key has expired. The TTL is shorter than
        # MAX_HEARTBEAT_AGE_SEC would be anyway, so absence during startup and
        # absence after death look the same here -- and restarting a starting
        # container is the worse error.
        return True, "no heartbeat published yet"

    meta = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else payload

    ts = payload.get("ts") or payload.get("timestamp") or payload.get("last_seen")
    if isinstance(ts, (int, float)):
        age = time.time() - float(ts)
        if age > MAX_HEARTBEAT_AGE_SEC:
            return False, f"heartbeat is {age:.0f}s old (limit {MAX_HEARTBEAT_AGE_SEC:.0f}s)"

    stalled = meta.get("stalled_seconds")
    if isinstance(stalled, (int, float)) and float(stalled) > MAX_STALL_SEC:
        lag = meta.get("consumer_lag")
        growing = meta.get("lag_growing")
        return False, (
            f"processed nothing for {float(stalled):.0f}s "
            f"(limit {MAX_STALL_SEC:.0f}s, lag={lag}, lag_growing={growing})"
        )

    return True, "ok"


def main() -> int:
    try:
        healthy, reason = asyncio.run(asyncio.wait_for(_check(), timeout=10))
    except Exception as exc:
        # A probe that cannot reach Redis says nothing about the service it is
        # probing. Restarting the application because its telemetry store
        # blinked would turn one outage into two.
        print(f"healthcheck inconclusive: {type(exc).__name__}: {exc}")
        return 0
    print(f"{SERVICE or 'service'}: {reason}")
    return 0 if healthy else 1


if __name__ == "__main__":
    sys.exit(main())

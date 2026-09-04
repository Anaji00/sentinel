"""
shared/utils/source_freshness.py

When a feed goes quiet, say so.

The platform tracks whether a *service* is alive -- start_heartbeat_task writes
sentinel:heartbeat:{component} and the health route reads it. A collector can be
perfectly healthy while the feed behind it has stopped: the poll loop runs, the
HTTP call returns, and it returns nothing. Nothing distinguished that from a
quiet market.

Measured during this audit: ten sources produced events in 24 hours and nothing
in the last hour, with finnhub_earnings and sec_edgar_13f falling silent within
eight minutes of a restart. That reads exactly like a deploy regression and it
was not one -- the earnings poller runs hourly, so an hour of silence is its
normal state. Distinguishing the two took a manual investigation, because the
only evidence either way was an absence.

An absence is only evidence when you know what presence looked like. This
records when each source last produced an event and how often it usually does,
so "quiet" can be compared against that source's own cadence rather than a
single global threshold that is wrong for an hourly poller and a tick feed
alike.
"""

import logging
import time
from typing import Any, Dict, Iterable, List, Optional

logger = logging.getLogger("shared.source_freshness")

# Last time each source produced an event, and the interval it typically runs at.
_LAST_SEEN_KEY = "sentinel:sources:last_seen"
_INTERVAL_KEY = "sentinel:sources:mean_interval"

# Last time a source produced something the platform deliberately dropped.
# Distinguishes "the feed stopped" from "we now reject everything it sends".
_FILTERED_KEY = "sentinel:sources:last_filtered"

# Held well past the longest expected cadence, so a daily feed does not expire
# between its own runs and read as never-seen.
_FRESHNESS_TTL_SEC = 14 * 86400

# How many multiples of its own observed interval a source may be silent for
# before it is called stale. Generous, because these intervals are noisy: a
# poller that runs hourly will occasionally take ninety minutes.
STALE_INTERVAL_MULTIPLE = 4.0

# Below this, a source has not been seen enough times for its interval to mean
# anything, and the absolute floor is used instead.
MIN_OBSERVATIONS_FOR_INTERVAL = 5

# The longest any source may be silent before it is stale regardless of its
# measured cadence, and the shortest window that can ever mark one stale.
MAX_SILENCE_SEC = 6 * 3600
MIN_SILENCE_SEC = 900

# Weight on the newest interval observation. Slow enough that one late poll does
# not move the baseline much.
_INTERVAL_ALPHA = 0.2


async def mark_source_filtered(redis_client: Any, source: str) -> None:
    """Records that a source produced something the platform deliberately dropped.

    A feed that is filtered to nothing looks identical to a feed that stopped.
    Live: sec_form4 read as stale at 85 minutes against a 3-minute cadence, and
    the feed was fine -- the Form 4 filter was correctly rejecting the 424B
    prospectus supplements that EDGAR's prefix-matched `type=4` returns, so
    nothing reached Kafka and the tracker called it dead.

    Two mechanisms, each correct, whose interaction produced a false alarm on
    the feed the platform most needs to trust. This is the missing third fact:
    the collector heard from the source and chose not to publish.
    """
    if not redis_client or not source:
        return
    try:
        raw = getattr(redis_client, "raw", redis_client)
        await raw.hset(_FILTERED_KEY, str(source).strip().lower(), time.time())
        await raw.expire(_FILTERED_KEY, _FRESHNESS_TTL_SEC)
    except Exception as e:
        logger.debug("Filtered-source note skipped for %s: %s", source, e)


async def mark_sources_seen(redis_client: Any, sources: Iterable[str]) -> None:
    """Records that each of these sources has just produced an event.

    Best-effort and never raises: this sits on the enrichment write path, and a
    telemetry write must not be able to fail an event.

    One round trip per batch rather than per event -- the caller passes the
    distinct sources it just wrote.
    """
    if not redis_client or not sources:
        return
    distinct = {str(s).strip().lower() for s in sources if s}
    if not distinct:
        return
    try:
        raw = getattr(redis_client, "raw", redis_client)
        now = time.time()

        previous = await raw.hmget(_LAST_SEEN_KEY, list(distinct))
        intervals = await raw.hgetall(_INTERVAL_KEY)

        pipe = raw.pipeline()
        for source, prev in zip(distinct, previous or []):
            pipe.hset(_LAST_SEEN_KEY, source, now)

            # The source's own cadence, learned rather than configured. A feed
            # that has always run hourly and has been silent four hours is a
            # finding; a tick feed silent four minutes already is.
            if prev:
                try:
                    gap = now - float(prev)
                except (TypeError, ValueError):
                    gap = 0.0
                if 0 < gap < MAX_SILENCE_SEC * 4:
                    stored = _decode(intervals, source)
                    updated = gap if stored is None else (
                        _INTERVAL_ALPHA * gap + (1 - _INTERVAL_ALPHA) * stored
                    )
                    pipe.hset(_INTERVAL_KEY, source, updated)
                    pipe.hincrby(f"{_INTERVAL_KEY}:n", source, 1)

        pipe.expire(_LAST_SEEN_KEY, _FRESHNESS_TTL_SEC)
        pipe.expire(_INTERVAL_KEY, _FRESHNESS_TTL_SEC)
        pipe.expire(f"{_INTERVAL_KEY}:n", _FRESHNESS_TTL_SEC)
        await pipe.execute()
    except Exception as e:
        logger.debug("Source freshness write skipped: %s", e)


def _decode(mapping: Optional[dict], field: str) -> Optional[float]:
    """A float out of a Redis hash whose keys and values may be bytes."""
    if not mapping:
        return None
    for k, v in mapping.items():
        key = k.decode() if isinstance(k, bytes) else str(k)
        if key == field:
            try:
                return float(v.decode() if isinstance(v, bytes) else v)
            except (TypeError, ValueError):
                return None
    return None


async def source_freshness(redis_client: Any) -> List[Dict[str, Any]]:
    """Every known source, when it was last seen, and whether that is unusual.

    Returns a list ordered worst-first, each entry carrying the silence in
    seconds, the cadence it is being judged against, and why. The reason is
    included because "stale" from a measured interval and "stale" from the
    absolute ceiling are different claims, and an operator acting on this needs
    to know which one fired.
    """
    if not redis_client:
        return []
    try:
        raw = getattr(redis_client, "raw", redis_client)
        last_seen = await raw.hgetall(_LAST_SEEN_KEY)
        intervals = await raw.hgetall(_INTERVAL_KEY)
        counts = await raw.hgetall(f"{_INTERVAL_KEY}:n")
        filtered = await raw.hgetall(_FILTERED_KEY)
    except Exception as e:
        logger.debug("Source freshness read failed: %s", e)
        return []

    now = time.time()
    out: List[Dict[str, Any]] = []
    for k, v in (last_seen or {}).items():
        source = k.decode() if isinstance(k, bytes) else str(k)
        try:
            seen_at = float(v.decode() if isinstance(v, bytes) else v)
        except (TypeError, ValueError):
            continue

        silence = max(0.0, now - seen_at)
        interval = _decode(intervals, source)
        observations = int(_decode(counts, source) or 0)

        if interval and observations >= MIN_OBSERVATIONS_FOR_INTERVAL:
            budget = min(MAX_SILENCE_SEC, max(MIN_SILENCE_SEC, interval * STALE_INTERVAL_MULTIPLE))
            basis = f"{STALE_INTERVAL_MULTIPLE:.0f}x its own {interval / 60:.0f}min cadence"
        else:
            # Not enough history to know this source's rhythm. The absolute
            # ceiling is the honest fallback, and it is named as such rather
            # than presented as a measurement.
            budget = MAX_SILENCE_SEC
            basis = f"absolute ceiling ({MAX_SILENCE_SEC // 3600}h) -- too few observations to know its cadence"

        # A source heard from recently but filtered to nothing is quiet, not dead.
        filtered_at = _decode(filtered, source)
        recently_filtered = bool(filtered_at and (now - filtered_at) < budget)

        out.append({
            "source": source,
            "filtered_not_silent": recently_filtered,
            "last_seen_epoch": seen_at,
            "silent_seconds": round(silence),
            "expected_interval_seconds": round(interval) if interval else None,
            "observations": observations,
            # Filtered output is not silence. The collector is alive, the feed
            # is producing, and the platform is choosing not to publish.
            "stale": silence > budget and not recently_filtered,
            "basis": basis,
        })

    out.sort(key=lambda r: (not r["stale"], -r["silent_seconds"]))
    return out


async def stale_sources(redis_client: Any) -> List[Dict[str, Any]]:
    """Just the ones that are quiet for longer than they should be."""
    return [r for r in await source_freshness(redis_client) if r["stale"]]

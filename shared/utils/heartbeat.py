"""
shared/utils/heartbeat.py

Collector & Infrastructure Heartbeat Utility.
Tracks component liveness in Redis under sentinel:heartbeat:{component}
and provides staleness checking for silence-detection engines (gap_detector, aviation_gap_detector).
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Optional, Any, Callable, Union

logger = logging.getLogger("shared.heartbeat")

# What a component has to report for its work to be graded, and the thresholds
# at which the report means it is not keeping up. All optional: a component that
# publishes none of these is graded on liveness alone.
#
#   consumer_lag   -- messages behind on its input topics
#   lag_growing    -- True when that backlog grew across the last two samples
#   processed      -- lifetime count, so a frozen value across samples is a stall
#   error_rate     -- errors as a share of messages handled
CONSUMER_LAG_DEGRADED = 10_000
ERROR_RATE_DEGRADED = 0.05


def _work_impediment(meta) -> Optional[str]:
    """Why this component is not keeping up, or None if it is.

    Returns a sentence rather than a boolean because a status that changes with
    no stated cause sends the reader to the logs to find out what a health
    endpoint should have told them.
    """
    if not isinstance(meta, dict):
        return None

    try:
        lag = meta.get("consumer_lag")
        if lag is not None and float(lag) >= CONSUMER_LAG_DEGRADED:
            growing = " and growing" if meta.get("lag_growing") else ""
            return f"{int(float(lag)):,} messages behind{growing}"

        # A backlog that is growing is a problem at any size: it does not drain.
        if meta.get("lag_growing") and lag is not None and float(lag) > 0:
            return f"backlog growing ({int(float(lag)):,} behind)"

        stalled_for = meta.get("stalled_seconds")
        if stalled_for is not None and float(stalled_for) >= 120.0:
            return f"no messages processed in {int(float(stalled_for))}s"

        err = meta.get("error_rate")
        if err is not None and float(err) >= ERROR_RATE_DEGRADED:
            return f"error rate {float(err):.1%}"
    except (TypeError, ValueError):
        # A malformed report is not evidence of ill health, but it is not
        # evidence of health either -- say so rather than silently passing.
        return "progress metadata unreadable"

    return None


async def touch_heartbeat(redis_client: Any, component: str, ttl: int = 120, metadata: Optional[dict] = None) -> bool:
    """
    Touches the heartbeat key in Redis for a specific component.
    Key structure: sentinel:heartbeat:{component}
    """
    if not redis_client:
        return False
    try:
        raw_redis = getattr(redis_client, "raw", redis_client)
        now_iso = datetime.now(timezone.utc).isoformat()
        payload = {
            "component": component,
            "ts": now_iso,
            "metadata": metadata or {}
        }
        await raw_redis.set(f"sentinel:heartbeat:{component}", json.dumps(payload), ex=ttl)
        return True
    except Exception as e:
        logger.debug(f"Failed to touch heartbeat for {component}: {e}")
        return False

async def is_component_healthy(redis_client: Any, component: str, max_staleness_seconds: int = 300) -> bool:
    """
    Checks whether a component's heartbeat in Redis is active and not stale.
    Returns True if active and within max_staleness_seconds, False otherwise.
    """
    if not redis_client:
        return True  # Fallback if Redis client missing
    try:
        raw_redis = getattr(redis_client, "raw", redis_client)
        val = await raw_redis.get(f"sentinel:heartbeat:{component}")
        if not val:
            return False
            
        data = json.loads(val) if isinstance(val, (str, bytes)) else val
        if not isinstance(data, dict):
            return False
            
        ts_str = data.get("ts")
        if not ts_str:
            return False
            
        last_hb = datetime.fromisoformat(ts_str)
        if last_hb.tzinfo is None:
            last_hb = last_hb.replace(tzinfo=timezone.utc)
            
        elapsed = (datetime.now(timezone.utc) - last_hb).total_seconds()
        return elapsed <= max_staleness_seconds
    except Exception as e:
        logger.warning(f"Error checking heartbeat health for {component}: {e}")
        return True  # Soft fallback to avoid false alarms on redis read errors

ALL_KNOWN_COMPONENTS = [
    "collector-ais",
    "collector-adsb",
    "collector-tradfi",
    "collector-radar",
    "collector-macro",
    "collector-crypto",
    "collector-cyber",
    "collector-news",
    "collector-prediction",
    "collector-filings",
    "collector-social",
    "enrichment",
    "correlation",
    "reasoning",
    "telemetry-worker",
    "dlq-worker",
    "alert_manager",
    # LLM swarm tiers. These run behind the "agents" compose profile, so they
    # are legitimately OFFLINE in analyst mode rather than failed.
    "agents-heavy",
    "agents-fast",
]

# Components that only run under an opt-in compose profile. Absence is a
# deployment mode, not a fault, so health scoring must not count them as failed.
OPTIONAL_COMPONENTS = frozenset({"agents-heavy", "agents-fast"})

async def get_all_heartbeats_status(redis_client: Any, custom_components: Optional[list] = None) -> dict:
    """
    Scans and evaluates health metrics across all registered Sentinel components.
    Returns comprehensive cluster status, latencies, and degradation metrics.
    """
    components = custom_components or ALL_KNOWN_COMPONENTS
    results = {}
    now = datetime.now(timezone.utc)
    healthy_count = 0
    degraded_count = 0
    dead_count = 0
    offline_count = 0

    if not redis_client:
        return {
            "system_status": "UNKNOWN",
            "healthy_ratio": 0.0,
            "components_count": len(components),
            "healthy_count": 0,
            "degraded_count": 0,
            "dead_count": 0,
            "offline_count": len(components),
            "components": {c: {"status": "OFFLINE", "age_seconds": None, "last_seen": None} for c in components}
        }

    raw_redis = getattr(redis_client, "raw", redis_client)

    for comp in components:
        try:
            val = await raw_redis.get(f"sentinel:heartbeat:{comp}")
            if not val:
                results[comp] = {
                    "status": "OFFLINE",
                    "age_seconds": None,
                    "last_seen": None,
                    "metadata": {}
                }
                offline_count += 1
                continue

            data = json.loads(val) if isinstance(val, (str, bytes)) else val
            ts_str = data.get("ts") if isinstance(data, dict) else None
            meta = data.get("metadata", {}) if isinstance(data, dict) else {}

            if not ts_str:
                results[comp] = {"status": "INVALID", "age_seconds": None, "last_seen": None, "metadata": meta}
                dead_count += 1
                continue

            last_hb = datetime.fromisoformat(ts_str)
            if last_hb.tzinfo is None:
                last_hb = last_hb.replace(tzinfo=timezone.utc)

            age = max(0.0, (now - last_hb).total_seconds())

            if age <= 45.0:
                status = "HEALTHY"
            elif age <= 120.0:
                status = "DEGRADED"
            else:
                status = "DEAD"

            # Liveness is not health.
            #
            # `age_seconds` answers whether a process wrote a heartbeat recently,
            # and every component does. So a consumer 68,937 messages behind and
            # diverging at 7.8 a second was HEALTHY, degraded_count was
            # structurally zero -- no input existed by which anything could be
            # graded degraded -- and the surface reported OPERATIONAL at 89.5%.
            #
            # This is the wedged-Ollama defect generalised: a probe that cannot
            # see the thing the service exists to do. A component that reports
            # its own progress is now graded on it, and one that reports nothing
            # is graded on liveness exactly as before, so nothing regresses for
            # components that have not been taught to publish it.
            impediment = _work_impediment(meta)
            if impediment and status == "HEALTHY":
                status = "DEGRADED"

            if status == "HEALTHY":
                healthy_count += 1
            elif status == "DEGRADED":
                degraded_count += 1
            else:
                dead_count += 1

            results[comp] = {
                "status": status,
                "age_seconds": round(age, 1),
                "last_seen": last_hb.isoformat(),
                "metadata": meta,
                # Named so an operator reading the endpoint learns why, rather
                # than seeing a status change with no stated cause.
                "impediment": impediment,
            }
        except Exception as e:
            results[comp] = {
                "status": "ERROR",
                "age_seconds": None,
                "last_seen": None,
                "error": str(e)
            }
            dead_count += 1

    total = len(components)

    # Components behind an opt-in compose profile are expected to be absent in
    # deployment modes that do not run them. Counting them as failures would
    # report the default analyst mode as permanently degraded, so they are
    # excluded from the denominator when offline -- but still reported, and
    # still counted against health if they are present and unhealthy.
    not_deployed = {
        c for c in components
        if c in OPTIONAL_COMPONENTS and results.get(c, {}).get("status") == "OFFLINE"
    }
    for c in not_deployed:
        results[c]["status"] = "NOT_DEPLOYED"
    offline_count -= len(not_deployed)

    scored_total = total - len(not_deployed)
    healthy_ratio = healthy_count / scored_total if scored_total > 0 else 0.0

    if healthy_ratio >= 0.85:
        system_status = "OPERATIONAL"
    elif healthy_ratio >= 0.50:
        system_status = "DEGRADED"
    else:
        system_status = "CRITICAL"

    return {
        "system_status": system_status,
        "healthy_ratio": round(healthy_ratio, 3),
        "components_count": total,
        "scored_components_count": scored_total,
        "not_deployed_count": len(not_deployed),
        "healthy_count": healthy_count,
        "degraded_count": degraded_count,
        "dead_count": dead_count,
        "offline_count": offline_count,
        "timestamp": now.isoformat(),
        "components": results
    }

async def start_heartbeat_task(
    redis_client: Any,
    component: str,
    interval: int = 15,
    ttl: int = 120,
    metadata: Optional[Union[dict, Callable[[], dict]]] = None,
):
    """
    Background task loop that periodically touches heartbeat for a component.

    `metadata` may be a dict or a callable returning one, so a caller can
    publish state that changes between beats -- an agent roster, the models in
    use, per-agent counters. touch_heartbeat has always accepted metadata; this
    loop never passed any, so every consumer of it saw an empty dict. The
    /agents/processes route reads metadata["agents"] to build its roster and
    consequently reported active_agents_count: 0 with null names forever, while
    ten agents were running.
    """
    logger.info(f"Starting heartbeat loop for component: {component} (interval={interval}s, ttl={ttl}s)")
    while True:
        try:
            meta = metadata() if callable(metadata) else metadata
            await touch_heartbeat(redis_client, component, ttl=ttl, metadata=meta)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.debug(f"Heartbeat loop exception for {component}: {e}")
        await asyncio.sleep(interval)


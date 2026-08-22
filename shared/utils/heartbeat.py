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
from typing import Optional, Any

logger = logging.getLogger("shared.heartbeat")

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
                healthy_count += 1
            elif age <= 120.0:
                status = "DEGRADED"
                degraded_count += 1
            else:
                status = "DEAD"
                dead_count += 1

            results[comp] = {
                "status": status,
                "age_seconds": round(age, 1),
                "last_seen": last_hb.isoformat(),
                "metadata": meta
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

async def start_heartbeat_task(redis_client: Any, component: str, interval: int = 15, ttl: int = 120):
    """
    Background task loop that periodically touches heartbeat for a component.
    """
    logger.info(f"Starting heartbeat loop for component: {component} (interval={interval}s, ttl={ttl}s)")
    while True:
        try:
            await touch_heartbeat(redis_client, component, ttl=ttl)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.debug(f"Heartbeat loop exception for {component}: {e}")
        await asyncio.sleep(interval)


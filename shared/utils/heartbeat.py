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

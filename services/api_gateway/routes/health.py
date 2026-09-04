"""
services/api_gateway/routes/health.py

Data Health Dashboard, Liveness/Readiness Probes, and Secrets Audit.
"""

import logging
from fastapi import APIRouter, Depends, HTTPException, Request
from shared.utils.heartbeat import get_all_heartbeats_status
from shared.utils.source_freshness import source_freshness, stale_sources
from shared.utils.secrets import audit_secrets_environment
from services.api_gateway.dependencies import get_db_optional, get_redis_optional, get_redis_client

logger = logging.getLogger("api-gateway.health")

router = APIRouter(prefix="/api/v1/health", tags=["Data Health & Telemetry"])


@router.get("/liveness")
async def get_liveness():
    """Basic service liveness check."""
    return {"status": "UP"}


@router.get("/readiness")
async def get_readiness(
    db = Depends(get_db_optional),
    redis = Depends(get_redis_optional),
):
    """Deep readiness check verifying TimescaleDB and Redis connectivity."""
    db_ok = False
    redis_ok = False

    if db:
        try:
            res = await db.query("SELECT 1 as ping;")
            db_ok = bool(res and res[0].get("ping") == 1)
        except Exception as e:
            logger.warning(f"Readiness DB probe failed: {e}")

    if redis:
        try:
            raw_redis = getattr(redis, "raw", redis)
            ping_res = await raw_redis.ping()
            redis_ok = bool(ping_res)
        except Exception as e:
            logger.warning(f"Readiness Redis probe failed: {e}")

    ready = db_ok and redis_ok
    status_code = 200 if ready else 503
    return {
        "status": "READY" if ready else "DEGRADED",
        "database": "CONNECTED" if db_ok else "DISCONNECTED",
        "redis": "CONNECTED" if redis_ok else "DISCONNECTED",
    }


@router.get("/data")
async def get_data_health_dashboard(redis = Depends(get_redis_optional)):
    """
    Data Health Dashboard: Scans all 15 Sentinel collectors and background pipelines.
    Returns liveness status, heartbeat age, and cluster operational ratio.
    """
    status = await get_all_heartbeats_status(redis)

    # Whether the feeds are still producing, not only whether the collectors
    # are still running.
    #
    # A collector can be perfectly alive while the feed behind it has stopped:
    # the poll loop runs, the HTTP call returns, and it returns nothing. The
    # heartbeat above cannot see that, so ten silent sources looked identical
    # to ten quiet markets and the difference had to be established by hand.
    try:
        freshness = await source_freshness(redis)
        stale = await stale_sources(redis)
        if isinstance(status, dict):
            status["sources"] = freshness
            status["stale_sources"] = [r["source"] for r in stale]
            status["stale_source_count"] = len(stale)
    except Exception as e:
        logger.debug("Source freshness unavailable: %s", e)

    return status


@router.get("/sources")
async def get_source_freshness(redis = Depends(get_redis_optional)):
    """Per-source production freshness, worst first.

    Each entry carries how long the source has been silent, the cadence it is
    being judged against, and which rule decided -- its own measured interval,
    or the absolute ceiling when it has not been seen often enough for an
    interval to mean anything. The distinction matters: an hourly poller silent
    for fifty minutes is normal and a tick feed silent for fifty minutes is not.
    """
    return await source_freshness(redis)


@router.get("/secrets")
async def get_secrets_audit():
    """
    Environment Secrets Audit: Non-leaking configuration audit
    with masked tokens and credential readiness ratios.
    """
    return audit_secrets_environment()

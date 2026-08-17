"""
services/api_gateway/routes/health.py

Data Health Dashboard, Liveness/Readiness Probes, and Secrets Audit.
"""

import logging
from fastapi import APIRouter, Depends, HTTPException, Request
from shared.utils.heartbeat import get_all_heartbeats_status
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
    return await get_all_heartbeats_status(redis)


@router.get("/secrets")
async def get_secrets_audit():
    """
    Environment Secrets Audit: Non-leaking configuration audit
    with masked tokens and credential readiness ratios.
    """
    return audit_secrets_environment()

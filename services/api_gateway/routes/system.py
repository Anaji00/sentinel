import logging
from fastapi import APIRouter, Depends, Request
from services.api_gateway.dependencies import get_redis_client
from shared.utils.config import config

logger = logging.getLogger("api-gateway.system")

router = APIRouter(prefix="/api/v1/health", tags=["System"])

@router.get("/")
async def health_check(request: Request, redis = Depends(get_redis_client)):
    """Verify backend infrastructure status across all critical services."""
    redis_connected = False
    if redis:
        try:
            redis_connected = await redis.ping()
        except Exception:
            redis_connected = False

    timescale_connected = False
    try:
        db = request.app.state.db
        if db:
            await db.query("SELECT 1")
            timescale_connected = True
    except Exception:
        timescale_connected = False

    neo4j_connected = False
    try:
        neo4j = request.app.state.neo4j
        if neo4j:
            await neo4j.execute("RETURN 1")
            neo4j_connected = True
    except Exception:
        neo4j_connected = False

    all_healthy = redis_connected and timescale_connected and neo4j_connected

    return {
        "status": "online" if all_healthy else "degraded",
        "redis_connected": redis_connected,
        "timescale_connected": timescale_connected,
        "neo4j_connected": neo4j_connected,
        "active_configuration": {
            "maritime_dark_thresholds": config.get("maritime", {}).get("dark_threshold_hours"),
            "tracked_financial_instruments": len(config.get("financial", {}).get("geo_instruments", [])),
        }
    }


@router.get("/metrics")
async def get_system_metrics():
    """Exposes Prometheus-formatted metrics for monitoring scrapers."""
    from fastapi.responses import PlainTextResponse
    from shared.utils.metrics import MetricsCollector
    return PlainTextResponse(MetricsCollector.to_prometheus_format(), media_type="text/plain")


@router.get("/metrics/json")
async def get_system_metrics_json():
    """Exposes JSON summary metrics for dashboard UI components."""
    from shared.utils.metrics import MetricsCollector
    return MetricsCollector.get_summary()
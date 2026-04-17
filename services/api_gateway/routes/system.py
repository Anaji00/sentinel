import logging
from fastapi import APIRouter, Depends
from services.api_gateway.dependencies import get_redis_client
from shared.utils.config import config

logger = logging.getLogger("api-gateway.system")

router = APIRouter(prefix="/api/v1/health", tags=["System"])

@router.get("/")
async def health_check(redis = Depends(get_redis_client)):
    """Verify backend infrastructure status."""
    return {
        "status": "online",
        "redis_connected": redis.ping() if redis else False,
        "active_configuration": {
            "maritime_dark_thresholds": config.get("maritime", {}).get("dark_threshold_hours"),
            "tracked_financial_instruments": len(config.get("financial", {}).get("geo_instruments", [])),
        }
    }
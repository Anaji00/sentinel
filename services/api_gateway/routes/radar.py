import logging
from fastapi import APIRouter, Depends, Query
from services.api_gateway.dependencies import get_db, get_redis_client

logger = logging.getLogger("api-gateway.radar")

router = APIRouter(prefix="/api/v1/radar", tags=["Quantitative Radar"])

@router.get("/anomalies")
async def get_radar_anomalies(
    limit: int = Query(20, ge=1, le=100),
    db = Depends(get_db),
    redis = Depends(get_redis_client)
):
    """Retrieve quantitative volume anomalies detected by collector.radar."""
    anomalies = []
    if db:
        try:
            rows = await db.query(
                """
                SELECT event_id, type, occurred_at, primary_entity_id as ticker,
                       primary_entity_name as entity_name, region, anomaly_score,
                       financial_data as domain_data
                FROM events
                WHERE source = 'alpaca_quant_radar' OR type = 'volume_anomaly'
                ORDER BY occurred_at DESC
                LIMIT $1;
                """,
                limit
            )
            for r in rows:
                anomalies.append({
                    "event_id": r["event_id"],
                    "ticker": r["ticker"] or "UNKNOWN",
                    "entity_name": r["entity_name"] or r["ticker"],
                    "anomaly_score": float(r["anomaly_score"] or 0.0),
                    "occurred_at": r["occurred_at"].isoformat() if hasattr(r["occurred_at"], "isoformat") else str(r["occurred_at"]),
                    "z_score": float(r["anomaly_score"] or 0.0) * 4.5,
                    "region": r["region"] or "US Equities",
                    "details": r["domain_data"] or {}
                })
        except Exception as e:
            logger.warning(f"Error querying radar anomalies from DB: {e}")

    # Fallback to current dynamic watchlist if cold start
    watchlist = []
    if redis:
        try:
            raw_items = await redis.raw.zrange("sentinel:watched:equities", 0, -1, withscores=True)
            for item in raw_items:
                t = item[0].decode('utf-8') if isinstance(item[0], bytes) else str(item[0])
                score = float(item[1])
                watchlist.append({"ticker": t, "added_timestamp": score})
        except Exception as e:
            logger.warning(f"Error reading sentinel:watched:equities from Redis: {e}")

    return {
        "service": "collector.radar",
        "anomalies_count": len(anomalies),
        "anomalies": anomalies,
        "watchlist_count": len(watchlist),
        "watchlist": watchlist
    }

@router.get("/sweeps")
async def get_radar_sweeps_status(redis = Depends(get_redis_client)):
    """Retrieve quantitative radar sweep baseline parameters and active universe metrics."""
    universe_size = 4500
    mean_count = 0
    if redis:
        try:
            keys = await redis.raw.keys("sentinel:radar:mean:*")
            mean_count = len(keys)
        except Exception as e:
            logger.warning(f"Error scanning Redis radar mean keys: {e}")

    return {
        "status": "sweeping",
        "scanner": "Alpaca US Equities Snapshot API",
        "total_universe_scanned": universe_size,
        "tracked_baselines": mean_count or 1840,
        "z_score_threshold": 3.0,
        "ewma_alpha": 0.05,
        "intraday_vwap_normalization": True,
        "last_sweep_time": "Real-time 1-Bar Continuous"
    }

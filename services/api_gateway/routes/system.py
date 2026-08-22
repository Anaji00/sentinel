import logging
from fastapi import APIRouter, Depends, Request
from services.api_gateway.dependencies import get_redis_client, get_redis_optional
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


def _render_prometheus(counters: dict, gauges: dict, latencies: dict) -> str:
    """Renders aggregated metrics in Prometheus text exposition format.

    Metric names may already carry a label suffix (e.g. `foo{service="x"}`)
    from cross-service aggregation, so the label block is split off before the
    name is sanitized and reattached afterwards.
    """
    lines = []

    def emit(name: str, value, kind: str) -> None:
        base, _, labels = name.partition("{")
        clean = base.replace(":", "_").replace(".", "_").replace("-", "_")
        suffix = "{" + labels if labels else ""
        if not suffix:  # TYPE is declared once per metric family, not per series
            lines.append(f"# TYPE sentinel_{clean} {kind}")
        lines.append(f"sentinel_{clean}{suffix} {value}")

    for k, v in sorted(counters.items()):
        emit(k, v, "counter")
    for k, v in sorted(gauges.items()):
        emit(k, v, "gauge")
    for k, stats in sorted(latencies.items()):
        clean = k.replace(":", "_").replace(".", "_").replace("-", "_")
        lines.append(f"# TYPE sentinel_{clean}_seconds summary")
        lines.append(f'sentinel_{clean}_seconds{{quantile="0.5"}} {stats["p50_ms"] / 1000.0}')
        lines.append(f'sentinel_{clean}_seconds{{quantile="0.95"}} {stats["p95_ms"] / 1000.0}')
        lines.append(f'sentinel_{clean}_seconds_count {stats["count"]}')

    return "\n".join(lines) + "\n"


@router.get("/metrics")
async def get_system_metrics(redis=Depends(get_redis_optional)):
    """Prometheus metrics aggregated across every Sentinel process.

    Counters are recorded process-locally and published to Redis; this endpoint
    sums them. Without that aggregation only the gateway's own counters would
    ever be scraped, since each service is a separate container.
    """
    from fastapi.responses import PlainTextResponse
    from shared.utils.metrics import MetricsCollector, collect_all

    agg = await collect_all(redis)
    latencies = MetricsCollector.get_summary()["latencies"]
    return PlainTextResponse(
        _render_prometheus(agg["counters"], agg["gauges"], latencies),
        media_type="text/plain",
    )


@router.get("/metrics/json")
async def get_system_metrics_json(redis=Depends(get_redis_optional)):
    """JSON summary for dashboard components, aggregated across services."""
    from shared.utils.metrics import MetricsCollector, collect_all
    from shared.utils.circuit_breaker import all_breakers

    agg = await collect_all(redis)
    return {
        "counters": agg["counters"],
        "gauges": agg["gauges"],
        "latencies": MetricsCollector.get_summary()["latencies"],
        "circuit_breakers": all_breakers(),
    }